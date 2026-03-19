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
from typing import Optional, Dict
import re

import psycopg
from dotenv import load_dotenv
from court_parser import parse_court_from_folder, to_generic_root_norm
from citation_parser_next import extract_citations_next
from text_cleaner import clean_judgment_text

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

def get_db_connection():
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(database_url)


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
    """從 JFULL 開頭推斷 doc_type（判決 / 裁定 / 憲判字 / 宣判筆錄 / 調解筆錄 / 和解筆錄）"""
    if not jfull:
        return None
    header = re.sub(r'[ \t\u3000]+', '', jfull[:200])  # 去除空白以處理全形空格排版
    if '憲判字' in header:
        return '憲判字'
    if '宣示判決' in header:
        return '宣判筆錄'
    if '調解筆錄' in header:
        return '調解筆錄'
    if '和解筆錄' in header:
        return '和解筆錄'
    if '裁定' in header:
        return '裁定'
    if '判決' in header:
        return '判決'
    # header 無「裁定」字眼但實質上是裁定的文書類型
    if '支付命令' in header or '保護令' in header:
        return '裁定'
    if '補償決定書' in header:
        return '補償決定書'
    return None


def ingest_decision(conn, court_unit_id: int, root_norm: str, unit_norm: str,
                    case_type: Optional[str], json_data: Dict) -> tuple:
    """
    Upsert 單一判決到 decisions 表（v4：decisions 同時承擔識別 + 文書內容）

    邏輯：
    1. jid 已存在 → 直接回傳（冪等，重複 ingest 防護）
    2. 找可升級的 placeholder（jid IS NULL，同 unit_norm/jyear/jcase_norm/jno）
       → 嚴格匹配：case_type 與 doc_type 都必須完全相同（IS NOT DISTINCT FROM）
       → 不同 case_type 或不同 doc_type（含 NULL vs 非NULL）不匹配
    3. 找到 → UPDATE 填入 jid + 文書內容（case_type/doc_type 不變，因為已嚴格匹配）
    4. 找不到 → INSERT 新 row

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

        # jid 已存在時直接回傳（冪等）；後續 UPDATE/INSERT 均假設 jid 尚未存在
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM decisions WHERE jid = %s", (jid,))
            existing = cur.fetchone()
        if existing:
            conn.commit()
            return True, existing[0]

        with conn.cursor() as cur:
            # 找可升級的 placeholder — 嚴格匹配 case_type + doc_type
            # 不同 case_type 或不同 doc_type（含 NULL vs 非NULL）一律不匹配，改走 INSERT
            cur.execute("""
                SELECT id, doc_type, case_type FROM decisions
                WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                  AND jid IS NULL
                  AND case_type IS NOT DISTINCT FROM %s
                  AND doc_type IS NOT DISTINCT FROM %s
                ORDER BY id
                LIMIT 1
            """, (unit_norm, jyear, jcase_norm, jno, case_type, doc_type))
            placeholder = cur.fetchone()

        if placeholder:
            ph_id = placeholder[0]

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE decisions
                    SET jid=%s, doc_type=%s, case_type=%s, court_unit_id=%s,
                        decision_date=%s, title=%s, clean_text=%s,
                        pdf_url=%s, updated_at=now()
                    WHERE id=%s
                    RETURNING id
                """, (jid, doc_type, case_type, court_unit_id,
                      decision_date, jtitle, clean_text,
                      jpdf, ph_id))
                row = cur.fetchone()
            ref = f"{jyear}年{jcase_norm}字第{jno}號"
            print(f"  ↑ 升級 placeholder id={ph_id} → {unit_norm} {ref}（ct={case_type!r} dt={doc_type!r} jid={jid}）")
            conn.commit()
            return True, row[0]

        else:
            # 除錯：檢查是否有不匹配的 placeholder 被跳過
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, case_type, doc_type FROM decisions
                    WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                      AND jid IS NULL
                """, (unit_norm, jyear, jcase_norm, jno))
                skipped = cur.fetchall()
            if skipped:
                ref = f"{jyear}年{jcase_norm}字第{jno}號"
                for s_id, s_ct, s_dt in skipped:
                    print(f"  ⚠ 跳過 placeholder id={s_id}（ct={s_ct!r} dt={s_dt!r}）"
                          f"→ 新建 {ref}（ct={case_type!r} dt={doc_type!r}）")

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
def _insert_placeholder(conn, unit_norm: str, jyear: int, jcase_norm: str, jno: int,
                         doc_type: Optional[str], case_type: Optional[str],
                         root_norm: Optional[str] = None) -> Optional[int]:
    """INSERT new placeholder，ON CONFLICT DO UPDATE 確保冪等，回傳 id"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO decisions (unit_norm, root_norm, case_type, jyear, jcase_norm, jno, doc_type)
                VALUES (%(court)s, %(root_norm)s, %(case_type)s, %(jyear)s, %(jcase_norm)s, %(jno)s, %(doc_type)s)
                ON CONFLICT (unit_norm, jyear, jcase_norm, jno,
                             COALESCE(case_type,''), COALESCE(doc_type,''))
                  WHERE jid IS NULL
                  DO UPDATE SET updated_at = now()
                RETURNING id
            """, {"court": unit_norm, "root_norm": root_norm or unit_norm, "case_type": case_type,
                  "jyear": jyear, "jcase_norm": jcase_norm,
                  "jno": jno, "doc_type": doc_type})
            row = cur.fetchone()
            conn.commit()
            return row[0]
    except Exception as e:
        print(f"錯誤：_insert_placeholder 失敗 - {e}")
        conn.rollback()
        return None


_RESOLVABLE_DOC_TYPES = {'判決', '裁定', '憲判字'}


def _infer_level(unit_norm: str) -> int:
    """從 unit_norm 推導 court level，供 to_generic_root_norm 使用"""
    if '憲法法庭' in unit_norm:                                  return 0
    if '最高' in unit_norm:                                       return 1
    if '智慧財產' in unit_norm:                                   return 2
    if '高等行政法院' in unit_norm and '地方庭' in unit_norm:     return 3
    if '高等' in unit_norm:                                       return 2
    if '少年' in unit_norm or '家事' in unit_norm:                return 3
    if '簡易庭' in unit_norm:                                     return 4
    return 3  # 地方法院


def upsert_target_placeholder(conn, court: str, jyear: int, jcase_norm: str, jno: int,
                               target_doc_type: Optional[str] = None,
                               target_case_type: Optional[str] = None,
                               source_case_type: Optional[str] = None) -> Optional[int]:
    """
    在 decisions 表 upsert target placeholder（jid IS NULL），回傳 decision_id

    本體 doc_type（resolve_doc_type）與 citation metadata 分開：
      - resolve_doc_type = target_doc_type if target_doc_type in {判決,裁定,憲判字} else None
      - 判例/裁判/None 都視為 unresolved，不決定本體 doc_type

    resolve_doc_type in {判決,裁定,憲判字}：
      1. 查 jid IS NOT NULL + case_type + doc_type → 找到就回傳
      2. 查 pool（jid IS NULL, 同 case_type）找同 doc_type placeholder → 回傳
      3. 找不到 → INSERT doc_type=resolve_doc_type placeholder

    resolve_doc_type is None（判例/裁判/None）：
      1. 查 pool（jid IS NULL, 同 case_type）找 doc_type IS NULL placeholder → 回傳
      2. 找不到 → INSERT doc_type=NULL placeholder
      不查完整 row，不碰 explicit placeholder

    ct=None（來源資料夾無後綴）時印警告。
    """
    if len(jcase_norm) > 50:
        print(f"  跳過：jcase_norm 過長（{len(jcase_norm)} 字）：{jcase_norm[:60]!r}")
        return None

    # 正規化法院名：台→臺，與 court_parser 一致，確保 placeholder 能被 source ingest 正確找到
    court = court.replace('台', '臺')
    # 推導 root_norm（通用層級分類），與 court_parser.to_generic_root_norm 一致
    root_norm = to_generic_root_norm(court, level=_infer_level(court))

    try:
        ct = target_case_type or source_case_type  # source fallback
        resolve_doc_type = target_doc_type if target_doc_type in _RESOLVABLE_DOC_TYPES else None

        if ct is None:
            print(f"  警告：無法確定 case_type（court={court}, {jyear}年{jcase_norm}字{jno}號）")

        if resolve_doc_type is not None:
            # 有明確本體類型：先查完整 row（jid IS NOT NULL）
            if ct is not None:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id FROM decisions
                        WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                          AND jid IS NOT NULL AND case_type = %s AND doc_type = %s
                        LIMIT 1
                    """, (court, jyear, jcase_norm, jno, ct, resolve_doc_type))
                    full_row = cur.fetchone()
                if full_row:
                    conn.commit()
                    return full_row[0]

            # 查 placeholder pool
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM decisions
                    WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                      AND jid IS NULL AND case_type IS NOT DISTINCT FROM %s
                      AND doc_type = %s
                    LIMIT 1
                """, (court, jyear, jcase_norm, jno, ct, resolve_doc_type))
                ph = cur.fetchone()
            if ph:
                conn.commit()
                return ph[0]

            # 找不到 → INSERT
            return _insert_placeholder(conn, court, jyear, jcase_norm, jno, resolve_doc_type, ct, root_norm)

        else:
            # 判例/裁判/None：只找/建 doc_type IS NULL placeholder
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM decisions
                    WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                      AND jid IS NULL AND case_type IS NOT DISTINCT FROM %s
                      AND doc_type IS NULL
                    LIMIT 1
                """, (court, jyear, jcase_norm, jno, ct))
                ph = cur.fetchone()
            if ph:
                conn.commit()
                return ph[0]

            # 找不到 → INSERT doc_type=NULL placeholder
            return _insert_placeholder(conn, court, jyear, jcase_norm, jno, None, ct, root_norm)

    except Exception as e:
        print(f"錯誤：upsert_target_placeholder 失敗 - {e}")
        conn.rollback()
        return None


# citation_parser 輸出的 auth_type（英文）→ authorities 表的 doc_type（中文）+ root_norm
_AUTH_TYPE_TO_DOC_TYPE = {
    "resolution":       "決議",
    "grand_interp":     "釋字",
    "conference":       "法律座談會",
    "agency_opinion":   "研審小組意見",
    "admin_resolution": "聯席會議決議",
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
    raw_citations = [c.to_dict() for c in extract_citations_next(
        clean_text, court_root_norm=court_root_norm, self_key=source_self_key)]
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

    # 清理沒有任何 citation 指向的 placeholder
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM decisions
            WHERE jid IS NULL
              AND id NOT IN (
                  SELECT DISTINCT target_id FROM citations WHERE target_id IS NOT NULL
              )
            RETURNING id
        """)
        deleted_ids = cur.fetchall()
    conn.commit()
    if deleted_ids:
        print(f"✓ 清理 {len(deleted_ids)} 筆孤兒 placeholder")

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


def main_regen(unit_norm_filter: str = ""):
    """
    用 DB 裡現有的 clean_text 重跑 citation 抽取（不重讀 JSON）。
    主要用途：更新 parser 邏輯後，刷新 citation 並自動清理孤兒 placeholder。

    unit_norm_filter：若指定，只重跑該法院的 source 判決（模糊比對 unit_norm）。
    """
    conn = get_db_connection()

    with conn.cursor() as cur:
        sql = """
            SELECT d.id, d.clean_text, d.root_norm, d.unit_norm,
                   d.jyear, d.jcase_norm, d.jno, d.case_type
            FROM decisions d
            WHERE d.jid IS NOT NULL AND d.clean_text IS NOT NULL
        """
        if unit_norm_filter:
            cur.execute(sql + " AND d.unit_norm LIKE %s ORDER BY d.id",
                        (f"%{unit_norm_filter}%",))
        else:
            cur.execute(sql + " ORDER BY d.id")
        rows = cur.fetchall()

    total = len(rows)
    print(f"重跑 {total} 筆 source 判決的 citation 抽取...")

    for i, (decision_id, clean_text, root_norm, unit_norm,
            jyear, jcase_norm, jno, case_type) in enumerate(rows, 1):
        self_key = (
            (root_norm or unit_norm).replace('臺', '台'),
            jyear, jcase_norm, jno,
        )
        _, errors = ingest_citations(
            conn, decision_id, clean_text,
            court_root_norm=root_norm,
            source_self_key=self_key,
            source_case_type=case_type,
        )
        for e in errors:
            print(f"  ⚠ id={decision_id}: {e}")
        if i % 200 == 0:
            print(f"  進度：{i}/{total}")

    # 清理孤兒 placeholder（stale cleanup 已刪掉指向它們的 citation row）
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM decisions
            WHERE jid IS NULL
              AND id NOT IN (
                  SELECT DISTINCT target_id FROM citations WHERE target_id IS NOT NULL
              )
            RETURNING id
        """)
        deleted_ids = cur.fetchall()
    conn.commit()
    if deleted_ids:
        print(f"✓ 清理 {len(deleted_ids)} 筆孤兒 placeholder")

    conn.close()
    print(f"\n完成！重跑 {total} 筆")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方式：")
        print("  單一資料夾：python etl/ingest_decisions.py <資料夾路徑>")
        print("  批次匯入：  python etl/ingest_decisions.py --batch <基底目錄>")
        print("  重跑錯誤：  python etl/ingest_decisions.py --retry <基底目錄>")
        print("  重跑 parser：python etl/ingest_decisions.py --regen [法院關鍵字]")
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
    elif sys.argv[1] == "--regen":
        unit_norm_filter = sys.argv[2] if len(sys.argv) > 2 else ""
        main_regen(unit_norm_filter)
    else:
        main(sys.argv[1])
