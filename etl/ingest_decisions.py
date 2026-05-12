"""
判決匯入與引用落表的 ETL orchestrator。

負責把司法院判決 JSON 匯入資料庫，並串接 citation_parser，
將判決本文中的引用關係轉成 citation graph 所需的節點與邊。

主要資料流：
1. 讀取判決 JSON，解析法院資訊與判決欄位。
2. 清理與正規化資料，例如 court/unit、jcase_norm、decision_date、clean_text。
3. Upsert source 判決到 decisions；若先前已有 placeholder，則將其升級為完整判決。
4. 呼叫 citation_parser 從 clean_text 抽取結構化引用。
5. 將引用目標落到 decisions placeholder 或 authorities，並寫入 citations。
6. 視需要重算 citation count，維持 citation graph 的一致性。

此檔案關心的是 ETL 流程、冪等寫入、placeholder 升級、錯誤記錄與資料一致性；
引用是否成立、如何排除 false positive、snippet 怎麼切，則由 citation_parser.py 負責。

支援的執行模式：
- 單一資料夾匯入
"""
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict
import re
import psycopg
from dotenv import load_dotenv
from court_parser import parse_court_from_folder, to_generic_root_norm
from citation_parser import CitationResult, extract_citations_next
from text_cleaner import clean_judgment_text

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

def get_db_connection():
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(database_url)


def log_error(conn, folder_name: str, file_name: str, error_type: str, error_msg: str):
    """記錄匯入錯誤到 ingest_error_log。"""
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
    unit_norm = court_info["unit_norm"]
    generic_root = to_generic_root_norm(unit_norm)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM court_units WHERE unit_norm = %s",
            (unit_norm,)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute("""
            INSERT INTO court_units (unit_norm, root_norm, level, county, district)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            unit_norm,
            generic_root,
            court_info["level"],
            court_info["county"],
            court_info["district"],
        ))
        court_unit_id = cur.fetchone()[0]
        conn.commit()
        return court_unit_id


# =========================
# 正規化函式
# =========================
@dataclass(frozen=True)
class PreparedSourceDecision:
    jid: Optional[str]
    jyear: int
    jcase_norm: str
    jno: int
    decision_date: Optional[str]
    title: Optional[str]
    clean_text: Optional[str]
    doc_type: Optional[str]
    pdf_url: Optional[str]


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
# 判決匯入（Schema）
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


def prepare_source_decision(json_data: Dict) -> tuple:
    """將單份 source 判決 JSON 整理為可重用的 normalization 結果。"""
    try:
        jcase = json_data.get("JCASE")
        jfull = json_data.get("JFULL")
        prepared = PreparedSourceDecision(
            jid=json_data.get("JID"),
            jyear=int(json_data.get("JYEAR")),
            jcase_norm=normalize_jcase(jcase),
            jno=int(json_data.get("JNO")),
            decision_date=parse_decision_date(json_data.get("JDATE")),
            title=json_data.get("JTITLE"),
            clean_text=clean_judgment_text(jfull) if jfull else None,
            doc_type=_extract_doc_type(jfull),
            pdf_url=json_data.get("JPDF"),
        )
        return True, prepared
    except Exception as e:
        return False, str(e)


def ingest_decision(conn, court_unit_id: int, root_norm: str, unit_norm: str,
                    case_type: Optional[str], prepared: PreparedSourceDecision) -> tuple:
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
        jid = prepared.jid
        jyear = prepared.jyear
        jcase_norm = prepared.jcase_norm
        jno = prepared.jno
        decision_date = prepared.decision_date
        jtitle = prepared.title
        clean_text = prepared.clean_text
        doc_type = prepared.doc_type
        jpdf = prepared.pdf_url

        if not jid:
            return False, "JID 欄位缺失，無法匯入 decisions"

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
                LIMIT 1
            """, (unit_norm, jyear, jcase_norm, jno, case_type, doc_type))
            placeholder = cur.fetchone()

        if placeholder:
            ph_id = placeholder[0]

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE decisions
                    SET jid=%s, court_unit_id=%s,
                        decision_date=%s, title=%s, clean_text=%s,
                        pdf_url=%s, updated_at=now()
                    WHERE id=%s
                    RETURNING id
                """, (jid, court_unit_id,
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
                    ON CONFLICT (jid) WHERE jid IS NOT NULL DO NOTHING
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
            if row is None:
                # 並發 insert 被 unique constraint 擋下，另一個 process 搶先插入
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM decisions WHERE jid = %s", (jid,))
                    row = cur.fetchone()
            _set_canonical_id(conn, row[0], unit_norm, jyear, jcase_norm, jno, case_type)
            conn.commit()
            return True, row[0]

    except Exception as e:
        msg = str(e)
        print(f"錯誤：匯入失敗 - {msg}")
        conn.rollback()
        return False, msg


# =========================
# Citation 處理
# =========================
def _insert_placeholder(conn, unit_norm: str, jyear: int, jcase_norm: str, jno: int,
                         doc_type: Optional[str], case_type: Optional[str],
                         root_norm: Optional[str] = None) -> Optional[int]:
    """INSERT new placeholder，ON CONFLICT DO UPDATE 確保冪等，回傳 id。
    新插入的 row 會自動設 canonical_id（_set_canonical_id 冪等，重複呼叫無副作用）。"""
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
        ph_id = row[0]
        _set_canonical_id(conn, ph_id, unit_norm, jyear, jcase_norm, jno, case_type)
        conn.commit()
        return ph_id
    except Exception as e:
        print(f"錯誤：_insert_placeholder 失敗 - {e}")
        conn.rollback()
        return None


_RESOLVABLE_DOC_TYPES = {'判決', '裁定', '憲判字'}


def _set_canonical_id(conn, new_id: int, unit_norm: str, jyear: int,
                      jcase_norm: str, jno: int,
                      case_type: Optional[str] = None) -> None:
    """新 row 設 canonical_id：同字號＋同 case_type 已有 canonical → 指向它；否則自身為 canonical。
    canonical_id 已設定時不修改（冪等）。"""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE decisions
            SET canonical_id = COALESCE(
                (SELECT canonical_id FROM decisions
                 WHERE unit_norm = %s AND jyear = %s AND jcase_norm = %s AND jno = %s
                   AND case_type IS NOT DISTINCT FROM %s
                   AND canonical_id IS NOT NULL
                 ORDER BY id LIMIT 1),
                %s
            )
            WHERE id = %s AND canonical_id IS NULL
        """, (unit_norm, jyear, jcase_norm, jno, case_type, new_id, new_id))


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
    root_norm = to_generic_root_norm(court)

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

            # 找不到 → INSERT（_insert_placeholder 內部會設 canonical_id）
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


def _get_decision_canonical_id(conn, target_id: int) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT canonical_id FROM decisions WHERE id = %s",
            (target_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _require_citation_offsets(citation: CitationResult) -> tuple[int, int]:
    """所有 citation 都必須帶可用的 clean_text offset。"""
    match_start = citation.match_start
    match_end = citation.match_end
    if match_start is None or match_end is None:
        raw_match = citation.raw_match or "<unknown>"
        raise ValueError(f"citation 缺少 match_start/match_end: {raw_match}")
    return int(match_start), int(match_end)


def ingest_citations(conn, source_id: int, clean_text: str,
                     source_self_key: Optional[tuple] = None,
                     source_case_type: Optional[str] = None) -> tuple:
    """
    從 clean_text 抽取所有引用，寫入 citations 表。

    source_id：來源判決的 decisions.id
    citations.source_id / target_id 均指向 decisions.id（Schema v4）

    Returns:
        (True, 成功寫入/更新的 citation 數量, 錯誤訊息清單)
        (False, 成功寫入/更新的 citation 數量, 錯誤訊息清單)
    """
    inserted = 0
    errors = []
    try:
        parsed_citations = extract_citations_next(
            clean_text,
            self_key=source_self_key,
        )

        # Phase 1：解析所有 target ID
        resolved: list[tuple[str, int, CitationResult]] = []
        for c in parsed_citations:
            _require_citation_offsets(c)
            ctype = c.citation_type
            if ctype == "authority":
                auth_id = upsert_authority(conn, c.auth_type, c.auth_key, c.display)
                if auth_id is not None:
                    resolved.append((ctype, auth_id, c))
            else:  # "decision"
                target_id = upsert_target_placeholder(
                    conn,
                    c.court, c.jyear, c.jcase_norm, c.jno,
                    target_doc_type=c.doc_type,
                    target_case_type=c.target_case_type,
                    source_case_type=source_case_type,
                )
                if target_id is not None:
                    resolved.append((ctype, target_id, c))

        # Phase 2：增量 upsert citations
        current_ids: set = set()
        with conn.cursor() as cur:
            for ctype, target, c in resolved:
                ms, me = _require_citation_offsets(c)
                tct = c.target_case_type   # target_case_type
                tdt = c.doc_type           # target_doc_type

                if ctype == "authority":
                    cur.execute("""
                        INSERT INTO citations
                          (source_id, target_authority_id, raw_match, match_start, match_end, snippet)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id, target_authority_id, match_start) DO UPDATE
                          SET snippet   = EXCLUDED.snippet,
                              match_end = EXCLUDED.match_end,
                              raw_match = EXCLUDED.raw_match
                        RETURNING id
                    """, (source_id, target, c.raw_match, ms, me, c.snippet))
                else:  # decision
                    target_canonical_id = _get_decision_canonical_id(conn, target)
                    cur.execute("""
                        INSERT INTO citations
                          (source_id, target_id, target_canonical_id, raw_match, match_start, match_end, snippet,
                           target_case_type, target_doc_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id, target_id, match_start) DO UPDATE
                          SET snippet          = EXCLUDED.snippet,
                              match_end        = EXCLUDED.match_end,
                              raw_match        = EXCLUDED.raw_match,
                              target_canonical_id = EXCLUDED.target_canonical_id,
                              target_case_type = EXCLUDED.target_case_type,
                              target_doc_type  = EXCLUDED.target_doc_type
                        RETURNING id
                    """, (source_id, target, target_canonical_id, c.raw_match, ms, me, c.snippet,
                          tct, tdt))

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
        return True, inserted, errors
    except Exception as e:
        msg = f"source_id={source_id} transaction failed: {e}"
        print(f"錯誤：citation 交易失敗 - {msg}")
        errors.append(msg)
        conn.rollback()
        return False, inserted, errors


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
        # 讀取 JSON
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                json_data = json.load(f)
        except Exception as e:
            msg = str(e)
            print(f"錯誤(A)：無法讀取 {json_file.name} - {msg}")
            log_error(conn, folder_name, json_file.name, "json_read", msg)
            fail_count += 1
            continue

        # 處理 source 判決
        ok, prepared_or_error = prepare_source_decision(json_data)
        if not ok:
            fail_count += 1
            msg = prepared_or_error or "prepare_source_decision failed"
            log_error(conn, folder_name, json_file.name, "source_decision", msg)
            continue
        prepared = prepared_or_error

        ok, result = ingest_decision(
            conn, court_unit_id,
            to_generic_root_norm(court_info["unit_norm"]),
            court_info["unit_norm"],
            court_info["case_type"],
            prepared=prepared,
        )
        if not ok:
            fail_count += 1
            log_error(conn, folder_name, json_file.name, "source_decision", result or "ingest_decision failed")
            continue

        success_count += 1
        decision_id = result

        # 處理 citation
        # 憲法法庭判決不作為來源，跳過 citation 處理
        if court_info["unit_norm"] == "憲法法庭":
            continue

        if prepared.clean_text and decision_id:
            _self_key = (
                court_info["court_root_norm"].replace('臺', '台'),
                prepared.jyear,
                prepared.jcase_norm,
                prepared.jno,
            )
            ok, n, cite_errors = ingest_citations(
                conn, decision_id, prepared.clean_text,
                source_self_key=_self_key,
                source_case_type=court_info["case_type"],
            )
            if n > 0:
                print(f"  ↳ {json_file.name}: 寫入 {n} 筆 citation")
            for ce in cite_errors:
                log_error(conn, folder_name, json_file.name, "citation", ce)

        if (success_count + fail_count) % 100 == 0:
            print(f"  進度：{success_count + fail_count}/{len(json_files)}")

    # 寫入 ingest_log
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingest_log (folder_name, decision_count)
            VALUES (%s, %s)
            ON CONFLICT (folder_name) DO UPDATE
                SET ingested_at     = now(),
                    decision_count  = EXCLUDED.decision_count
        """, (folder_name, success_count))
    conn.commit()

    conn.close()
    print(f"\n完成！成功 {success_count} 筆，失敗 {fail_count} 筆")
    print(f"✓ 已寫入 ingest_log：{folder_name}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方式：")
        print("  單一資料夾：python etl/ingest_decisions.py <資料夾路徑>")
        sys.exit(1)

    main(sys.argv[1])
