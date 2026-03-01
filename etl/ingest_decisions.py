"""
判決 JSON 匯入腳本（Schema v4：decisions 為 citation graph 唯一節點）

流程：
1. 掃描資料夾內的所有 JSON 檔案
2. 解析每個判決的 8 個欄位
3. 正規化 jcase_norm, decision_date
4. Upsert 到 decisions 表（含識別欄位 + 文書內容）；若有 placeholder 則升級
5. 從 clean_text 抽取 citations，寫入 citations 表
"""
import json
import os
import sys
from pathlib import Path
from typing import Optional, Dict, List
import re

import psycopg
from court_parser import parse_court_from_folder, to_generic_root_norm
from citation_parser import extract_citations
from text_cleaner import clean_judgment_text


# =========================
# DB 連線配置
# =========================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "citations",
    "user": "postgres",
    "password": "postgres"
}


def get_db_connection():
    conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return psycopg.connect(conn_str)


def log_error(conn, folder_name: str, file_name: str, error_type: str, error_msg: str):
    """記錄匯入錯誤到 ingest_error_log（error_type: 'A'=JSON讀取, 'B'=判決匯入, 'D'=Citation寫入）"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ingest_error_log (folder_name, file_name, error_type, error_msg)
                VALUES (%s, %s, %s, %s)
            """, (folder_name, file_name, error_type, error_msg))
        conn.commit()
    except Exception as e:
        print(f"警告：無法寫入 ingest_error_log - {e}")
        conn.rollback()


# =========================
# 法院資訊處理
# =========================
def upsert_court_unit(conn, court_info: Dict) -> int:
    """Insert court_units 若不存在，回傳 court_unit_id。"""
    generic_root = to_generic_root_norm(court_info["unit_norm"], court_info["level"])
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO court_units (unit_norm, root_norm, level, county, district)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (unit_norm) DO NOTHING
        """, (
            court_info["unit_norm"],
            generic_root,
            court_info["level"],
            court_info.get("county"),
            court_info.get("district"),
        ))
        cur.execute(
            "SELECT id FROM court_units WHERE unit_norm = %s",
            (court_info["unit_norm"],)
        )
        court_unit_id = cur.fetchone()[0]
        conn.commit()
        return court_unit_id


# =========================
# 正規化函式
# =========================
def normalize_jcase(jcase: str) -> str:
    return jcase.replace("臺", "台")


def parse_decision_date(jdate: str) -> Optional[str]:
    try:
        if len(jdate) != 8:
            return None
        return f"{jdate[:4]}-{jdate[4:6]}-{jdate[6:8]}"
    except Exception:
        return None


# =========================
# 判決匯入（Schema v4）
# =========================
def _extract_doc_type(jfull: str) -> Optional[str]:
    """從 JFULL 開頭推斷 doc_type（判決 / 裁定 / 憲判字 / 宣判筆錄）"""
    if not jfull:
        return None
    header = jfull[:200]
    if '憲判字' in header:
        return '憲判字'
    if re.search(r"宣\s*示\s*判\s*決(?:\s*筆\s*錄)?", header):
        return '宣判筆錄'
    if '裁定' in header:
        return '裁定'
    if '判決' in header:
        return '判決'
    return None


def ingest_decision(conn, court_unit_id: int, root_norm: str, unit_norm: str,
                    case_type: Optional[str], json_data: Dict) -> tuple:
    """
    Upsert 單一判決到 decisions 表（v4：decisions 同時承擔識別 + 文書內容）

    邏輯：
    1. 先找可升級的 placeholder（jid IS NULL，同 unit_norm/jyear/jcase_norm/jno）
       → 優先挑同 doc_type，其次 NULL doc_type，最後任意
    2. 找到 → UPDATE 填入 jid + 文書內容 + 升級 case_type
    3. 找不到 → INSERT，ON CONFLICT(jid) DO UPDATE

    Returns:
        (True, decision_id) 成功，(False, error_msg) 失敗
    """
    try:
        jid = json_data.get("JID")
        jyear = int(json_data.get("JYEAR"))
        jcase = json_data.get("JCASE")
        jno = int(json_data.get("JNO"))
        jdate = json_data.get("JDATE")
        jtitle = json_data.get("JTITLE")
        jfull = json_data.get("JFULL")
        jpdf = json_data.get("JPDF")

        if not jid:
            return False, "JID 欄位缺失，無法匯入 decisions"

        jcase_norm = normalize_jcase(jcase)
        decision_date = parse_decision_date(jdate)
        clean_text = clean_judgment_text(jfull) if jfull else None
        doc_type = _extract_doc_type(jfull)

        with conn.cursor() as cur:
            # 先找可升級的 placeholder
            cur.execute("""
                SELECT id, doc_type, case_type FROM decisions
                WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                  AND jid IS NULL
                ORDER BY
                  CASE WHEN case_type = %s THEN 0
                       WHEN case_type IS NULL THEN 1
                       ELSE 2 END,
                  CASE WHEN doc_type = %s THEN 0
                       WHEN doc_type IS NULL THEN 1
                       ELSE 2 END,
                  id
                LIMIT 1
            """, (unit_norm, jyear, jcase_norm, jno, case_type, doc_type))
            placeholder = cur.fetchone()

        if placeholder:
            ph_id, ph_doc_type, ph_case_type = placeholder

            # jid 已存在時（例如重複匯入同一資料夾），直接回傳現有 row
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM decisions WHERE jid = %s", (jid,))
                existing = cur.fetchone()
            if existing:
                conn.commit()
                return True, existing[0]

            # conservative case_type upgrade
            new_case_type = case_type
            if case_type is None:
                new_case_type = ph_case_type
            elif ph_case_type is not None and ph_case_type != case_type:
                print(f"  衝突：placeholder id={ph_id} case_type={ph_case_type!r} vs {case_type!r}，用新值")

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE decisions
                    SET jid=%s, doc_type=%s, case_type=%s, court_unit_id=%s,
                        decision_date=%s, title=%s, clean_text=%s,
                        pdf_url=%s, updated_at=now()
                    WHERE id=%s
                    RETURNING id
                """, (jid, doc_type, new_case_type, court_unit_id,
                      decision_date, jtitle, clean_text,
                      jpdf, ph_id))
                row = cur.fetchone()
            conn.commit()
            return True, row[0]

        else:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO decisions (
                        unit_norm, root_norm, case_type, jyear, jcase_norm, jno,
                        court_unit_id, jid, doc_type, decision_date, title,
                        clean_text, pdf_url
                    )
                    VALUES (
                        %(unit_norm)s, %(root_norm)s, %(case_type)s, %(jyear)s,
                        %(jcase_norm)s, %(jno)s, %(court_unit_id)s, %(jid)s,
                        %(doc_type)s, %(decision_date)s, %(title)s,
                        %(clean_text)s, %(pdf_url)s
                    )
                    ON CONFLICT (jid) WHERE jid IS NOT NULL DO UPDATE
                        SET doc_type      = EXCLUDED.doc_type,
                            case_type     = EXCLUDED.case_type,
                            court_unit_id = EXCLUDED.court_unit_id,
                            decision_date = EXCLUDED.decision_date,
                            title         = EXCLUDED.title,
                            clean_text    = EXCLUDED.clean_text,
                            pdf_url       = EXCLUDED.pdf_url,
                            updated_at    = now()
                    RETURNING id
                """, {
                    "unit_norm":     unit_norm,
                    "root_norm":     root_norm,
                    "case_type":     case_type,
                    "jyear":         jyear,
                    "jcase_norm":    jcase_norm,
                    "jno":           jno,
                    "court_unit_id": court_unit_id,
                    "jid":           jid,
                    "doc_type":      doc_type,
                    "decision_date": decision_date,
                    "title":         jtitle,
                    "clean_text":    clean_text,
                    "pdf_url":       jpdf,
                })
                row = cur.fetchone()
            conn.commit()
            return True, row[0]

    except Exception as e:
        msg = str(e)
        print(f"錯誤：匯入失敗 - {msg}")
        conn.rollback()
        return False, msg


# =========================
# Citation 處理（Schema v4）
# =========================
def _insert_placeholder(conn, court: str, jyear: int, jcase_norm: str, jno: int,
                         doc_type: Optional[str], case_type: Optional[str]) -> Optional[int]:
    """INSERT new placeholder，ON CONFLICT DO UPDATE 確保冪等，回傳 id"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO decisions (unit_norm, root_norm, case_type, jyear, jcase_norm, jno, doc_type)
                VALUES (%(court)s, %(court)s, %(case_type)s, %(jyear)s, %(jcase_norm)s, %(jno)s, %(doc_type)s)
                ON CONFLICT (unit_norm, jyear, jcase_norm, jno,
                             COALESCE(case_type,''), COALESCE(doc_type,''))
                  WHERE jid IS NULL
                  DO UPDATE SET updated_at = now()
                RETURNING id
            """, {"court": court, "case_type": case_type,
                  "jyear": jyear, "jcase_norm": jcase_norm,
                  "jno": jno, "doc_type": doc_type})
            row = cur.fetchone()
            conn.commit()
            return row[0]
    except Exception as e:
        print(f"錯誤：_insert_placeholder 失敗 - {e}")
        conn.rollback()
        return None


def upsert_target_placeholder(conn, court: str, jyear: int, jcase_norm: str, jno: int,
                               target_doc_type: Optional[str] = None,
                               target_case_type: Optional[str] = None,
                               source_case_type: Optional[str] = None) -> Optional[int]:
    """
    在 decisions 表 upsert target placeholder（jid IS NULL），回傳 decision_id

    ct = target_case_type or source_case_type（source fallback）
    pool = 同字號且 case_type == ct 的既有 placeholder（精確匹配）

    doc_type 升級邏輯（在 pool 內）：
      判決  → pool 有判例 → 回傳判例；有判決 → 回傳；有 NULL doc_type → 升級為判決
      裁定  → pool 有裁定 → 回傳；有 NULL doc_type → 升級為裁定
      判例  → 有判例 → 回傳；升級判決/NULL → 判例
      None/其他 → pool 有明確 doc_type → 掛引用數最多的；只有 NULL doc_type → 掛 NULL；找不到 → INSERT
      找不到可用的 → INSERT new placeholder（case_type=ct）

    ct=None（來源資料夾無後綴）時印警告；pool 找 case_type IS NULL 的既有 placeholder。
    """
    if len(jcase_norm) > 50:
        print(f"  跳過：jcase_norm 過長（{len(jcase_norm)} 字）：{jcase_norm[:60]!r}")
        return None

    try:
        ct = target_case_type or source_case_type  # source fallback

        # 先查是否已有完整 decisions row（jid IS NOT NULL）
        # 須同時比對 case_type 與 doc_type，避免判決/裁定並存時回傳錯誤目標
        # target_doc_type=None 時接受任意 doc_type（OR %s IS NULL = TRUE）
        if ct is not None:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM decisions
                    WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                      AND jid IS NOT NULL AND case_type = %s
                      AND (doc_type = %s OR %s IS NULL)
                    LIMIT 1
                """, (court, jyear, jcase_norm, jno, ct,
                      target_doc_type, target_doc_type))
                full_row = cur.fetchone()
            if full_row:
                conn.commit()
                return full_row[0]

        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, doc_type, case_type FROM decisions
                WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                  AND jid IS NULL
                ORDER BY id
            """, (court, jyear, jcase_norm, jno))
            all_ph = cur.fetchall()  # [(id, doc_type, case_type), ...]
        if ct is None:
            print(f"  警告：無法確定 case_type（court={court}, {jyear}年{jcase_norm}字{jno}號）")
        pool = [r for r in all_ph if r[2] == ct]  # exact case_type match（含 ct=None 時找 case_type IS NULL 的既有 placeholder）

        chosen_id = None

        if target_doc_type in ('判決', '裁定'):
            # 判例與判決不可並存；新來判決若已有判例，直接回傳判例
            prec_ph = next((r for r in pool if r[1] == '判例'), None)
            same_ph = next((r for r in pool if r[1] == target_doc_type), None)
            null_ph = next((r for r in pool if r[1] is None), None)
            if target_doc_type == '判決' and prec_ph:
                chosen_id = prec_ph[0]
            elif same_ph:
                chosen_id = same_ph[0]
            elif null_ph:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE decisions SET doc_type=%s, updated_at=now() WHERE id=%s",
                        (target_doc_type, null_ph[0])
                    )
                chosen_id = null_ph[0]

        elif target_doc_type == '判例':
            prec_ph = next((r for r in pool if r[1] == '判例'), None)
            judg_ph = next((r for r in pool if r[1] == '判決'), None)
            null_ph = next((r for r in pool if r[1] is None), None)
            if prec_ph:
                chosen_id = prec_ph[0]
            else:
                upgrade_src = judg_ph or null_ph
                if upgrade_src:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE decisions SET doc_type='判例', updated_at=now() WHERE id=%s",
                            (upgrade_src[0],)
                        )
                    chosen_id = upgrade_src[0]

        else:
            # target_doc_type is None 或其他未知值
            # 有明確 doc_type 的 placeholder → 掛引用數最多的（避免掛到不相關節點）
            # 只有 NULL doc_type 的 placeholder → 掛 NULL（語意一致：都不知道 doc_type）
            explicit_phs = [r for r in pool if r[1] is not None]
            if explicit_phs:
                explicit_ids = [r[0] for r in explicit_phs]
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT target_id, COUNT(*) AS cnt
                        FROM citations
                        WHERE target_id = ANY(%s)
                        GROUP BY target_id
                        ORDER BY cnt DESC
                        LIMIT 1
                    """, (explicit_ids,))
                    row = cur.fetchone()
                chosen_id = row[0] if row else explicit_phs[0][0]
            else:
                null_ph = next((r for r in pool if r[1] is None), None)
                if null_ph:
                    chosen_id = null_ph[0]

        if chosen_id is not None:
            conn.commit()
            return chosen_id

        # 沒有可用的 → INSERT new placeholder（用 ct = effective_ct，含 source fallback）
        conn.commit()
        return _insert_placeholder(conn, court, jyear, jcase_norm, jno,
                                   target_doc_type, ct)

    except Exception as e:
        print(f"錯誤：upsert_target_placeholder 失敗 - {e}")
        conn.rollback()
        return None


# citation_parser 輸出的 auth_type（英文）→ authorities 表的 doc_type（中文）+ root_norm
_AUTH_TYPE_TO_DOC_TYPE = {
    "resolution":     "決議",
    "grand_interp":   "釋字",
    "conference":     "法律座談會",
    "agency_opinion": "研審小組意見",
}


def _authority_root_norm(auth_type: str, ref_key: str) -> str:
    if auth_type == "resolution":
        return "最高法院"
    if auth_type in ("grand_interp", "agency_opinion"):
        return "司法院"
    if auth_type == "conference":
        prefix = ref_key.split("|")[0]
        if "行政法院" in prefix:
            return "高等行政法院"
        if "高等法院" in prefix:
            return "高等法院"
        return "司法院"
    return "司法院"


def upsert_authority(conn, auth_type: str, ref_key: str, display: Optional[str] = None) -> Optional[int]:
    """在 authorities 表 upsert 非裁判性引用，回傳 authority id"""
    doc_type  = _AUTH_TYPE_TO_DOC_TYPE.get(auth_type, auth_type)
    root_norm = _authority_root_norm(auth_type, ref_key)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO authorities (doc_type, root_norm, ref_key, display)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (doc_type, ref_key) DO UPDATE
                  SET display = COALESCE(EXCLUDED.display, authorities.display)
                RETURNING id
            """, (doc_type, root_norm, ref_key, display))
            row = cur.fetchone()
            if row:
                auth_id = row[0]
            else:
                cur.execute(
                    "SELECT id FROM authorities WHERE doc_type = %s AND ref_key = %s",
                    (doc_type, ref_key)
                )
                auth_id = cur.fetchone()[0]
            conn.commit()
            return auth_id
    except Exception as e:
        print(f"錯誤：upsert authority 失敗 - {e}")
        conn.rollback()
        return None


def ingest_citations(conn, source_id: int, clean_text: str,
                     court_root_norm: str = None,
                     source_self_key: Optional[tuple] = None,
                     source_case_type: Optional[str] = None) -> tuple:
    """
    從 clean_text 抽取所有引用，寫入 citations 表。

    source_id：來源判決的 decisions.id
    citations.source_id / target_id 均指向 decisions.id（Schema v4）

    Returns:
        (成功寫入/更新的 citation 數量, 錯誤訊息清單)
    """
    raw_citations = extract_citations(clean_text, court_root_norm=court_root_norm,
                                      self_key=source_self_key)
    inserted = 0
    errors = []

    # Phase 1：解析所有 target ID
    resolved = []
    for c in raw_citations:
        ctype = c.get("citation_type", "decision")
        if ctype == "authority":
            auth_id = upsert_authority(conn, c["auth_type"], c["auth_key"], c.get("display"))
            if auth_id is not None:
                resolved.append((ctype, auth_id, c))
        else:  # "decision"
            target_id = upsert_target_placeholder(
                conn,
                c["court"], c["jyear"], c["jcase_norm"], c["jno"],
                target_doc_type=c.get("doc_type"),
                target_case_type=c.get("target_case_type"),
                source_case_type=source_case_type,
            )
            if target_id is not None:
                resolved.append((ctype, target_id, c))

    # Phase 2：增量 upsert citations
    try:
        current_ids: set = set()
        with conn.cursor() as cur:
            for ctype, target, c in resolved:
                ms = c.get("match_start")
                tct = c.get("target_case_type")   # target_case_type
                tdt = c.get("doc_type")            # target_doc_type

                if ctype == "authority":
                    if ms is not None:
                        cur.execute("""
                            INSERT INTO citations
                              (source_id, target_authority_id, raw_match, match_start, match_end, snippet)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (source_id, target_authority_id, match_start) DO UPDATE
                              SET snippet   = EXCLUDED.snippet,
                                  match_end = EXCLUDED.match_end,
                                  raw_match = EXCLUDED.raw_match
                            RETURNING id
                        """, (source_id, target, c["raw_match"], ms, c["match_end"], c["snippet"]))
                    else:
                        cur.execute("""
                            INSERT INTO citations
                              (source_id, target_authority_id, raw_match, match_start, match_end, snippet)
                            VALUES (%s, %s, %s, NULL, NULL, %s)
                            ON CONFLICT (source_id, target_authority_id, raw_match)
                              WHERE match_start IS NULL DO UPDATE
                              SET snippet = EXCLUDED.snippet
                            RETURNING id
                        """, (source_id, target, c["raw_match"], c["snippet"]))
                else:  # decision
                    if ms is not None:
                        cur.execute("""
                            INSERT INTO citations
                              (source_id, target_id, raw_match, match_start, match_end, snippet,
                               target_case_type, target_doc_type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (source_id, target_id, match_start) DO UPDATE
                              SET snippet          = EXCLUDED.snippet,
                                  match_end        = EXCLUDED.match_end,
                                  raw_match        = EXCLUDED.raw_match,
                                  target_case_type = EXCLUDED.target_case_type,
                                  target_doc_type  = EXCLUDED.target_doc_type
                            RETURNING id
                        """, (source_id, target, c["raw_match"], ms, c["match_end"], c["snippet"],
                              tct, tdt))
                    else:
                        cur.execute("""
                            INSERT INTO citations
                              (source_id, target_id, raw_match, match_start, match_end, snippet,
                               target_case_type, target_doc_type)
                            VALUES (%s, %s, %s, NULL, NULL, %s, %s, %s)
                            ON CONFLICT (source_id, target_id, raw_match)
                              WHERE match_start IS NULL DO UPDATE
                              SET snippet          = EXCLUDED.snippet,
                                  target_case_type = EXCLUDED.target_case_type,
                                  target_doc_type  = EXCLUDED.target_doc_type
                            RETURNING id
                        """, (source_id, target, c["raw_match"], c["snippet"], tct, tdt))

                row = cur.fetchone()
                if row:
                    current_ids.add(row[0])
                    inserted += 1

            # stale cleanup：刪除本次未出現的舊引用邊
            if current_ids:
                cur.execute(
                    "DELETE FROM citations WHERE source_id = %s AND id != ALL(%s)",
                    (source_id, list(current_ids))
                )
            else:
                cur.execute("DELETE FROM citations WHERE source_id = %s", (source_id,))

        conn.commit()
    except Exception as e:
        msg = f"source_id={source_id} transaction failed: {e}"
        print(f"錯誤：citation 交易失敗 - {msg}")
        errors.append(msg)
        conn.rollback()

    return inserted, errors


# =========================
# 主程式
# =========================
def main(folder_path: str):
    """掃描資料夾，匯入所有判決"""
    folder = Path(folder_path)
    if not folder.exists():
        print(f"錯誤：資料夾不存在 - {folder_path}")
        return

    folder_name = f"{folder.parent.name}/{folder.name}"
    court_info = parse_court_from_folder(folder.name)
    if not court_info:
        print(f"錯誤：無法解析法院 - {folder_name}")
        return

    print(f"法院：{court_info['unit_norm']} (level={court_info['level']})")

    conn = get_db_connection()
    print("✓ DB 連線成功")

    court_unit_id = upsert_court_unit(conn, court_info)
    print(f"✓ Court unit ID: {court_unit_id}")

    json_files = list(folder.glob("*.json"))
    print(f"找到 {len(json_files)} 個 JSON 檔案")

    success_count = 0
    fail_count = 0

    for json_file in json_files:
        # A 類：JSON 讀取失敗
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                json_data = json.load(f)
        except Exception as e:
            msg = str(e)
            print(f"錯誤(A)：無法讀取 {json_file.name} - {msg}")
            log_error(conn, folder_name, json_file.name, "A", msg)
            fail_count += 1
            continue

        # B 類：判決匯入失敗
        ok, result = ingest_decision(
            conn, court_unit_id,
            to_generic_root_norm(court_info["unit_norm"], court_info["level"]),
            court_info["unit_norm"],
            court_info.get("case_type"),
            json_data
        )
        if not ok:
            fail_count += 1
            log_error(conn, folder_name, json_file.name, "B", result or "ingest_decision failed")
        else:
            success_count += 1
            decision_id = result

            # 憲法法庭判決不作為來源，跳過 citation 抽取
            if court_info["unit_norm"] == "憲法法庭":
                continue

            jfull = json_data.get("JFULL", "") or ""
            if jfull and decision_id:
                clean_text = clean_judgment_text(jfull)
                _self_jcase = normalize_jcase(json_data.get("JCASE", ""))
                _self_key = (
                    court_info["court_root_norm"].replace('臺', '台'),
                    int(json_data.get("JYEAR")),
                    _self_jcase,
                    int(json_data.get("JNO")),
                )
                n, cite_errors = ingest_citations(
                    conn, decision_id, clean_text,
                    court_root_norm=court_info["court_root_norm"],
                    source_self_key=_self_key,
                    source_case_type=court_info.get("case_type"),
                )
                if n > 0:
                    print(f"  ↳ {json_file.name}: 寫入 {n} 筆 citation")
                for ce in cite_errors:
                    log_error(conn, folder_name, json_file.name, "D", ce)

        if (success_count + fail_count) % 100 == 0:
            print(f"  進度：{success_count + fail_count}/{len(json_files)}")

    # 計算本次寫入的 citation 總數
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM citations c
            JOIN decisions d ON d.id = c.source_id
            WHERE d.court_unit_id = %s
        """, (court_unit_id,))
        total_citations = cur.fetchone()[0]

    # 寫入 ingest_log
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingest_log (folder_name, decision_count, citation_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (folder_name) DO UPDATE
                SET ingested_at     = now(),
                    decision_count  = EXCLUDED.decision_count,
                    citation_count  = EXCLUDED.citation_count
        """, (folder_name, success_count, total_citations))
    conn.commit()

    conn.close()
    print(f"\n完成！成功 {success_count} 筆，失敗 {fail_count} 筆")
    print(f"✓ 已寫入 ingest_log：{folder_name}")


def main_batch(base_dir: str, keyword: str = ""):
    """批次匯入：掃描 base_dir 下所有符合 keyword 的資料夾"""
    base = Path(base_dir)
    keywords = [k for k in keyword.split(",") if k]
    folders = [f for f in base.iterdir() if f.is_dir() and all(k in f.name for k in keywords)]
    label = f"'{keyword}'" if keywords else "（全部）"
    print(f"找到 {len(folders)} 個資料夾符合 {label}")
    for folder in sorted(folders):
        print(f"\n{'='*40}")
        print(f"處理：{folder.name}")
        main(str(folder))

    # 印出本次 batch 的錯誤摘要
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT error_type, COUNT(*) AS cnt
            FROM ingest_error_log
            WHERE resolved = false
            GROUP BY error_type
            ORDER BY error_type
        """)
        error_rows = cur.fetchall()
    conn.close()

    if error_rows:
        total_errors = sum(cnt for _, cnt in error_rows)
        detail = ", ".join(f"{t}類 {cnt} 筆" for t, cnt in error_rows)
        print(f"\n⚠ 本次 batch 共有 {total_errors} 筆未解決錯誤（{detail}）")
        print(f"  執行以下指令重跑：")
        print(f"  python etl/ingest_decisions.py --retry {base_dir}")
    else:
        print("\n✓ 本次 batch 無錯誤")


def main_retry(base_dir: str):
    """重跑所有未解決的錯誤（ingest_error_log.resolved = false）"""
    from collections import defaultdict

    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT folder_name, file_name, error_type
            FROM ingest_error_log
            WHERE resolved = false
            ORDER BY folder_name, file_name
        """)
        rows = cur.fetchall()

    if not rows:
        print("沒有未解決的錯誤。")
        conn.close()
        return

    print(f"找到 {len(rows)} 筆未解決錯誤")

    by_folder = defaultdict(list)
    for folder_name, file_name, error_type in rows:
        by_folder[folder_name].append((file_name, error_type))

    for folder_name, files in sorted(by_folder.items()):
        folder_path = Path(base_dir) / folder_name
        if not folder_path.exists():
            print(f"警告：資料夾不存在，跳過 - {folder_path}")
            continue

        court_info = parse_court_from_folder(folder_path.name)
        if not court_info:
            print(f"錯誤：無法解析法院 - {folder_name}")
            continue

        court_unit_id = upsert_court_unit(conn, court_info)
        print(f"\n{'='*40}")
        print(f"重跑：{folder_name}（{len(files)} 筆錯誤）")

        for file_name, error_type in files:
            json_file = folder_path / file_name
            if not json_file.exists():
                print(f"  警告：檔案不存在 - {file_name}")
                continue

            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    json_data = json.load(f)
            except Exception as e:
                print(f"  A 仍失敗：{file_name} - {e}")
                continue

            ok, result = ingest_decision(
                conn, court_unit_id,
                to_generic_root_norm(court_info["unit_norm"], court_info["level"]),
                court_info["unit_norm"],
                court_info.get("case_type"),
                json_data
            )
            if not ok:
                print(f"  B 仍失敗：{file_name} - {result}")
                continue

            decision_id = result

            jfull = json_data.get("JFULL", "") or ""
            if jfull and decision_id:
                clean_text = clean_judgment_text(jfull)
                _self_key_retry = (
                    court_info["court_root_norm"].replace('臺', '台'),
                    int(json_data.get("JYEAR")),
                    normalize_jcase(json_data.get("JCASE", "")),
                    int(json_data.get("JNO")),
                )
                n, cite_errors = ingest_citations(
                    conn, decision_id, clean_text,
                    court_root_norm=court_info["court_root_norm"],
                    source_self_key=_self_key_retry
                )
                if cite_errors:
                    print(f"  D 仍有錯：{file_name} - {cite_errors[0]}")
                    continue

            # 成功：標記為 resolved
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ingest_error_log
                    SET resolved = true, resolved_at = now()
                    WHERE folder_name = %s AND file_name = %s AND resolved = false
                """, (folder_name, file_name))
            conn.commit()
            print(f"  ✓ 已修復：{file_name}")

    conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方式：")
        print("  單一資料夾：python etl/ingest_decisions.py <資料夾路徑>")
        print("  批次匯入：  python etl/ingest_decisions.py --batch <基底目錄>")
        print("  重跑錯誤：  python etl/ingest_decisions.py --retry <基底目錄>")
        sys.exit(1)

    if sys.argv[1] == "--batch":
        if len(sys.argv) < 3:
            print("錯誤：--batch 需要指定基底目錄")
            sys.exit(1)
        keyword = sys.argv[3] if len(sys.argv) > 3 else ""
        main_batch(sys.argv[2], keyword)
    elif sys.argv[1] == "--retry":
        if len(sys.argv) < 3:
            print("錯誤：--retry 需要指定基底目錄")
            sys.exit(1)
        main_retry(sys.argv[2])
    else:
        main(sys.argv[1])
