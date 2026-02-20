"""
判決 JSON 匯入腳本（Week 1：高等法院 → 最高法院引用）

流程：
1. 掃描資料夾內的所有 JSON 檔案
2. 解析每個判決的 8 個欄位
3. 正規化 jcase_norm, decision_date
4. Upsert 到 decisions 表（自然鍵去重）
"""
import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List

import psycopg
from court_parser import parse_court_from_folder
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
    """取得 DB 連線"""
    conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return psycopg.connect(conn_str)


def log_error(conn, folder_name: str, file_name: str, error_type: str, error_msg: str):
    """
    記錄匯入錯誤到 ingest_error_log

    error_type: 'A'=JSON讀取失敗, 'B'=判決匯入失敗, 'D'=Citation寫入失敗
    """
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
    """
    Insert court_units 若不存在，回傳 court_unit_id。
    已存在則不更新（避免覆蓋手動修正的資料）。

    Args:
        conn: DB 連線
        court_info: parse_court_from_folder() 的回傳值

    Returns:
        court_unit_id (int)
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO court_units (unit_norm, root_norm, level, county, district)
            VALUES (%(unit_norm)s, %(root_norm)s, %(level)s, %(county)s, %(district)s)
            ON CONFLICT (unit_norm) DO NOTHING
        """, court_info)
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
    """
    正規化 JCASE（字別）

    範例：
        "台上" → "台上"
        "臺上" → "台上"（統一用台）
        "重訴" → "重訴"
    """
    normalized = jcase.replace("臺", "台")
    return normalized


def parse_decision_date(jdate: str) -> Optional[str]:
    """
    解析 JDATE（西元年格式）轉成 ISO 日期

    Args:
        jdate: "20251120"

    Returns:
        "2025-11-20" 或 None（解析失敗）
    """
    try:
        # 提示：JDATE 格式是 YYYYMMDD
        # 例如：20251120 → 2025-11-20
        if len(jdate) != 8:
            return None

        year = jdate[:4]
        month = jdate[4:6]
        day = jdate[6:8]
        return f"{year}-{month}-{day}"

    except Exception as e:
        print(f"警告：無法解析日期 {jdate} - {e}")
        return None


# =========================
# 判決匯入
# =========================
def ingest_decision(conn, court_unit_id: int, court_root_norm: str, unit_norm: str, case_type: Optional[str], json_data: Dict) -> bool:
    """
    Upsert 單一判決到 decisions 表

    Args:
        conn: DB 連線
        court_unit_id: court_units.id
        court_root_norm: 聚合層級，例如「臺灣高等法院」（顯示/篩選用）
        unit_norm: 具體分院名稱，例如「臺灣高等法院臺南分院」（自然鍵）
        case_type: 案件類型（民事/刑事/行政/憲法），或 None
        json_data: 判決 JSON（8 個欄位）

    Returns:
        (True, None) 成功，(False, error_msg) 失敗
    """
    try:
        # 提取欄位
        jid = json_data.get("JID")
        jyear = int(json_data.get("JYEAR"))
        jcase = json_data.get("JCASE")
        jno = int(json_data.get("JNO"))
        jdate = json_data.get("JDATE")
        jtitle = json_data.get("JTITLE")
        jfull = json_data.get("JFULL")
        jpdf = json_data.get("JPDF")

        # 正規化
        jcase_norm = normalize_jcase(jcase)
        decision_date = parse_decision_date(jdate)
        clean_text = clean_judgment_text(jfull) if jfull else None

        # Upsert（自然鍵：unit_norm + jyear + jcase_norm + jno）
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO decisions (
                    unit_norm, court_root_norm, jyear, jcase_norm, jno,
                    jid, court_unit_id, decision_date, title, full_text, clean_text, pdf_url, raw,
                    case_type
                )
                VALUES (
                    %(unit_norm)s, %(court_root_norm)s, %(jyear)s, %(jcase_norm)s, %(jno)s,
                    %(jid)s, %(court_unit_id)s, %(decision_date)s, %(title)s, %(full_text)s, %(clean_text)s, %(pdf_url)s, %(raw)s,
                    %(case_type)s
                )
                ON CONFLICT (unit_norm, jyear, jcase_norm, jno) DO UPDATE
                    SET court_root_norm = EXCLUDED.court_root_norm,
                        jid = EXCLUDED.jid,
                        court_unit_id = EXCLUDED.court_unit_id,
                        decision_date = EXCLUDED.decision_date,
                        title = EXCLUDED.title,
                        full_text = EXCLUDED.full_text,
                        clean_text = EXCLUDED.clean_text,
                        pdf_url = EXCLUDED.pdf_url,
                        raw = EXCLUDED.raw,
                        case_type = EXCLUDED.case_type,
                        updated_at = now()
            """, {
                "unit_norm": unit_norm,
                "court_root_norm": court_root_norm,
                "jyear": jyear,
                "jcase_norm": jcase_norm,
                "jno": jno,
                "jid": jid,
                "court_unit_id": court_unit_id,
                "decision_date": decision_date,
                "title": jtitle,
                "full_text": jfull,
                "clean_text": clean_text,
                "pdf_url": jpdf,
                "raw": json.dumps(json_data, ensure_ascii=False),
                "case_type": case_type,
            })
            conn.commit()
            return True, None

    except Exception as e:
        msg = str(e)
        print(f"錯誤：匯入失敗 - {msg}")
        conn.rollback()
        return False, msg


# =========================
# Citation 處理
# =========================
def upsert_decision(conn, court: str, jyear: int, jcase_norm: str, jno: int) -> Optional[int]:
    """
    在 decisions 表 upsert target 判決 placeholder（僅自然鍵欄位）
    回傳 target decision_id

    Args:
        conn: DB 連線
        court: 目標法院 unit_norm（'最高法院' / '最高行政法院' / '憲法法庭'）
        jyear, jcase_norm, jno: 從 citation 抽取的自然鍵

    Returns:
        target decision id，失敗回傳 None
    """
    # 正常案號字別最多約 10 字，超過 50 字幾乎必為 parser 誤抓，直接跳過
    if len(jcase_norm) > 50:
        print(f"  跳過：jcase_norm 過長（{len(jcase_norm)} 字），可能為 parser 誤抓：{jcase_norm[:60]!r}")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO decisions (unit_norm, court_root_norm, jyear, jcase_norm, jno)
                VALUES (%(court)s, %(court)s, %(jyear)s, %(jcase_norm)s, %(jno)s)
                ON CONFLICT (unit_norm, jyear, jcase_norm, jno) DO NOTHING
                RETURNING id
            """, {"court": court, "jyear": jyear, "jcase_norm": jcase_norm, "jno": jno})
            row = cur.fetchone()
            if row:
                target_id = row[0]
            else:
                # 已存在，查出 id
                cur.execute("""
                    SELECT id FROM decisions
                    WHERE unit_norm = %(court)s
                      AND jyear = %(jyear)s
                      AND jcase_norm = %(jcase_norm)s
                      AND jno = %(jno)s
                """, {"court": court, "jyear": jyear, "jcase_norm": jcase_norm, "jno": jno})
                target_id = cur.fetchone()[0]
            conn.commit()
            return target_id
    except Exception as e:
        print(f"錯誤：upsert decision 失敗 - {e}")
        conn.rollback()
        return None


def upsert_authority(conn, auth_type: str, auth_key: str, display: Optional[str] = None) -> Optional[int]:
    """
    在 authorities 表 upsert 非裁判性引用（會議決議、釋字、法律座談會等），回傳 authority id

    Args:
        conn: DB 連線
        auth_type: 類型（'resolution' / 'grand_interp' / 'conference' / ...）
        auth_key: 自然鍵（如 '民事庭|77|9'、'釋字|144'）
        display: 顯示用完整名稱（可選；已存在時不覆蓋原有值）

    Returns:
        authority id，失敗回傳 None
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO authorities (auth_type, auth_key, display)
                VALUES (%s, %s, %s)
                ON CONFLICT (auth_type, auth_key) DO UPDATE
                  SET display = COALESCE(EXCLUDED.display, authorities.display)
                RETURNING id
            """, (auth_type, auth_key, display))
            row = cur.fetchone()
            if row:
                auth_id = row[0]
            else:
                cur.execute(
                    "SELECT id FROM authorities WHERE auth_type = %s AND auth_key = %s",
                    (auth_type, auth_key)
                )
                auth_id = cur.fetchone()[0]
            conn.commit()
            return auth_id
    except Exception as e:
        print(f"錯誤：upsert authority 失敗 - {e}")
        conn.rollback()
        return None


def ingest_citations(conn, source_id: int, clean_text: str, court_root_norm: str = None, source_self_key: Optional[tuple] = None) -> tuple:
    """
    從 clean_text 抽取所有引用，寫入 citations 表。
    採增量 upsert：同一 (source, target, match_start) 衝突時 UPDATE snippet，
    本次未再出現的舊引用邊最後統一刪除（stale cleanup）。
    match_start / match_end 對應 clean_text 的字元位置。

    Args:
        conn: DB 連線
        source_id: 來源判決的 decisions.id
        clean_text: clean_judgment_text() 處理後的全文
        court_root_norm: 來源判決的法院 root_norm，用於解析「本院」引用

    Returns:
        (成功寫入/更新的 citation 數量, 錯誤訊息清單)
    """
    raw_citations = extract_citations(clean_text, court_root_norm=court_root_norm, self_key=source_self_key)
    inserted = 0
    errors = []

    # Phase 1：解析所有 target ID（各自在自己的 transaction 完成）
    resolved = []
    for c in raw_citations:
        ctype = c.get("citation_type", "decision")
        if ctype == "authority":
            auth_id = upsert_authority(conn, c["auth_type"], c["auth_key"], c.get("display"))
            if auth_id is not None:
                resolved.append((ctype, auth_id, c))
        else:  # "decision"
            target_id = upsert_decision(conn, c["court"], c["jyear"], c["jcase_norm"], c["jno"])
            if target_id is not None:
                resolved.append((ctype, target_id, c))

    # Phase 2：增量 upsert，同一 transaction
    # match_start IS NOT NULL → ON CONFLICT (source, target, match_start) DO UPDATE
    # match_start IS NULL     → ON CONFLICT (source, target, raw_match) WHERE match_start IS NULL DO UPDATE
    #                           （依賴 citations_null_match_*_uniq partial unique index）
    # 最後刪除本次未出現的 stale 引用邊
    try:
        current_ids: set = set()
        with conn.cursor() as cur:
            for ctype, target, c in resolved:
                ms = c.get("match_start")

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
                    doc_type = c.get("doc_type")
                    if ms is not None:
                        cur.execute("""
                            INSERT INTO citations
                              (source_id, target_id, raw_match, match_start, match_end, snippet, doc_type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (source_id, target_id, match_start) DO UPDATE
                              SET snippet   = EXCLUDED.snippet,
                                  match_end = EXCLUDED.match_end,
                                  raw_match = EXCLUDED.raw_match,
                                  doc_type  = EXCLUDED.doc_type
                            RETURNING id
                        """, (source_id, target, c["raw_match"], ms, c["match_end"], c["snippet"], doc_type))
                    else:
                        cur.execute("""
                            INSERT INTO citations
                              (source_id, target_id, raw_match, match_start, match_end, snippet, doc_type)
                            VALUES (%s, %s, %s, NULL, NULL, %s, %s)
                            ON CONFLICT (source_id, target_id, raw_match)
                              WHERE match_start IS NULL DO UPDATE
                              SET snippet   = EXCLUDED.snippet,
                                  doc_type  = EXCLUDED.doc_type
                            RETURNING id
                        """, (source_id, target, c["raw_match"], c["snippet"], doc_type))

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
    """
    掃描資料夾，匯入所有判決

    Args:
        folder_path: 例如 "/Users/rachel/Downloads/202511/臺灣高等法院民事"
    """
    folder = Path(folder_path)
    if not folder.exists():
        print(f"錯誤：資料夾不存在 - {folder_path}")
        return

    # 解析法院資訊
    # ingest_log key 格式：「月份批次/資料夾名稱」（如 202511/臺灣高等法院民事）
    folder_name = f"{folder.parent.name}/{folder.name}"
    court_info = parse_court_from_folder(folder.name)
    if not court_info:
        print(f"錯誤：無法解析法院 - {folder_name}")
        return

    print(f"法院：{court_info['unit_norm']} (level={court_info['level']})")

    # 連線 DB
    conn = get_db_connection()
    print("✓ DB 連線成功")

    # Upsert court_unit
    court_unit_id = upsert_court_unit(conn, court_info)
    print(f"✓ Court unit ID: {court_unit_id}")

    # 掃描 JSON 檔案
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
        ok, err_msg = ingest_decision(conn, court_unit_id, court_info["root_norm"], court_info["unit_norm"], court_info.get("case_type"), json_data)
        if not ok:
            fail_count += 1
            log_error(conn, folder_name, json_file.name, "B", err_msg or "ingest_decision failed")
        else:
            success_count += 1

            # 同步抽取 citations（用 clean_text，offset 對應 clean_text）
            # 憲法法庭判決不作為來源，跳過 citation 抽取
            if court_info["unit_norm"] == "憲法法庭":
                continue

            jfull = json_data.get("JFULL", "") or ""
            if jfull:
                clean_text = clean_judgment_text(jfull)
                source_id_row = None
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id FROM decisions
                        WHERE unit_norm = %(unit_norm)s
                          AND jyear = %(jyear)s
                          AND jcase_norm = %(jcase_norm)s
                          AND jno = %(jno)s
                    """, {
                        "unit_norm": court_info["unit_norm"],
                        "jyear": int(json_data.get("JYEAR")),
                        "jcase_norm": normalize_jcase(json_data.get("JCASE", "")),
                        "jno": int(json_data.get("JNO")),
                    })
                    row = cur.fetchone()
                    if row:
                        source_id_row = row[0]

                if source_id_row:
                    _self_jcase = normalize_jcase(json_data.get("JCASE", ""))
                    _self_key = (
                        court_info["root_norm"].replace('臺', '台'),  # root_norm = court_root_norm，與 extract_citations 內 current_court 一致
                        int(json_data.get("JYEAR")),
                        _self_jcase,
                        int(json_data.get("JNO")),
                    )
                    n, cite_errors = ingest_citations(conn, source_id_row, clean_text, court_root_norm=court_info["root_norm"], source_self_key=_self_key)
                    if n > 0:
                        print(f"  ↳ {json_file.name}: 寫入 {n} 筆 citation")
                    # D 類：citation 寫入失敗
                    for ce in cite_errors:
                        log_error(conn, folder_name, json_file.name, "D", ce)

        # 每 100 筆顯示進度
        if (success_count + fail_count) % 100 == 0:
            print(f"  進度：{success_count + fail_count}/{len(json_files)}")

    # 計算本次寫入的 citation 總數
    total_citations = 0
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
    """
    批次匯入：掃描 base_dir 下所有符合 keyword 的資料夾

    Args:
        base_dir: 例如 "/Users/rachel/Downloads/202511"
        keyword: 逗號分隔關鍵字（AND 條件）；留空則匯入全部資料夾

    範例：
        python etl/ingest_decisions.py --batch /Users/rachel/Downloads/202511
        python etl/ingest_decisions.py --batch /Users/rachel/Downloads/202511 高等法院,刑事
    """
    base = Path(base_dir)
    keywords = [k for k in keyword.split(",") if k]  # 空字串 → 不過濾
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
    """
    重跑所有未解決的錯誤（ingest_error_log.resolved = false）

    Args:
        base_dir: 原始資料的根目錄，例如 "/Users/rachel/Downloads"
                  完整路徑 = base_dir / folder_name / file_name

    範例：
        python etl/ingest_decisions.py --retry /Users/rachel/Downloads
    """
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

            ok, err_msg = ingest_decision(conn, court_unit_id, court_info["root_norm"], court_info["unit_norm"], court_info.get("case_type"), json_data)
            if not ok:
                print(f"  B 仍失敗：{file_name} - {err_msg}")
                continue

            # citation 補跑（A/B/D 都補）
            jfull = json_data.get("JFULL", "") or ""
            if jfull:
                clean_text = clean_judgment_text(jfull)
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id FROM decisions
                        WHERE unit_norm = %(unit_norm)s
                          AND jyear = %(jyear)s
                          AND jcase_norm = %(jcase_norm)s
                          AND jno = %(jno)s
                    """, {
                        "unit_norm": court_info["unit_norm"],
                        "jyear": int(json_data.get("JYEAR")),
                        "jcase_norm": normalize_jcase(json_data.get("JCASE", "")),
                        "jno": int(json_data.get("JNO")),
                    })
                    row = cur.fetchone()
                if row:
                    _self_key_retry = (
                        court_info["root_norm"].replace('臺', '台'),
                        int(json_data.get("JYEAR")),
                        normalize_jcase(json_data.get("JCASE", "")),
                        int(json_data.get("JNO")),
                    )
                    n, cite_errors = ingest_citations(conn, row[0], clean_text, court_root_norm=court_info["root_norm"], source_self_key=_self_key_retry)
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
