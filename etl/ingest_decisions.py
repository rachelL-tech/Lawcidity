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


# =========================
# 法院資訊處理
# =========================
def upsert_court_unit(conn, court_info: Dict) -> int:
    """
    Upsert court_units 表，回傳 court_unit_id

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
            ON CONFLICT (unit_norm) DO UPDATE
                SET root_norm = EXCLUDED.root_norm,
                    level = EXCLUDED.level,
                    county = EXCLUDED.county,
                    district = EXCLUDED.district,
                    updated_at = now()
            RETURNING id
        """, court_info)
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
def ingest_decision(conn, court_unit_id: int, court_root_norm: str, json_data: Dict) -> bool:
    """
    Upsert 單一判決到 decisions 表

    Args:
        conn: DB 連線
        court_unit_id: court_units.id
        court_root_norm: 例如「臺灣高等法院」
        json_data: 判決 JSON（8 個欄位）

    Returns:
        True 成功，False 失敗
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

        # Upsert
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO decisions (
                    court_root_norm, jyear, jcase_norm, jno,
                    jid, court_unit_id, decision_date, title, full_text, clean_text, pdf_url, raw
                )
                VALUES (
                    %(court_root_norm)s, %(jyear)s, %(jcase_norm)s, %(jno)s,
                    %(jid)s, %(court_unit_id)s, %(decision_date)s, %(title)s, %(full_text)s, %(clean_text)s, %(pdf_url)s, %(raw)s
                )
                ON CONFLICT (court_root_norm, jyear, jcase_norm, jno) DO UPDATE
                    SET jid = EXCLUDED.jid,
                        court_unit_id = EXCLUDED.court_unit_id,
                        decision_date = EXCLUDED.decision_date,
                        title = EXCLUDED.title,
                        full_text = EXCLUDED.full_text,
                        clean_text = EXCLUDED.clean_text,
                        pdf_url = EXCLUDED.pdf_url,
                        raw = EXCLUDED.raw,
                        updated_at = now()
            """, {
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
                "raw": json.dumps(json_data, ensure_ascii=False)
            })
            conn.commit()
            return True

    except Exception as e:
        print(f"錯誤：匯入失敗 - {e}")
        conn.rollback()
        return False


# =========================
# Citation 處理
# =========================
def upsert_target_placeholder(conn, jyear: int, jcase_norm: str, jno: int) -> Optional[int]:
    """
    在 decisions 表 upsert 最高法院 placeholder（僅自然鍵欄位）
    回傳 target decision_id

    Args:
        conn: DB 連線
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
                INSERT INTO decisions (court_root_norm, jyear, jcase_norm, jno)
                VALUES ('最高法院', %(jyear)s, %(jcase_norm)s, %(jno)s)
                ON CONFLICT (court_root_norm, jyear, jcase_norm, jno) DO NOTHING
                RETURNING id
            """, {"jyear": jyear, "jcase_norm": jcase_norm, "jno": jno})
            row = cur.fetchone()
            if row:
                target_id = row[0]
            else:
                # 已存在，查出 id
                cur.execute("""
                    SELECT id FROM decisions
                    WHERE court_root_norm = '最高法院'
                      AND jyear = %(jyear)s
                      AND jcase_norm = %(jcase_norm)s
                      AND jno = %(jno)s
                """, {"jyear": jyear, "jcase_norm": jcase_norm, "jno": jno})
                target_id = cur.fetchone()[0]
            conn.commit()
            return target_id
    except Exception as e:
        print(f"錯誤：upsert target placeholder 失敗 - {e}")
        conn.rollback()
        return None


def ingest_citations(conn, source_id: int, clean_text: str) -> int:
    """
    從 clean_text 抽取所有最高法院引用，寫入 citations 表
    match_start / match_end 對應 clean_text 的字元位置

    Args:
        conn: DB 連線
        source_id: 來源判決的 decisions.id
        clean_text: clean_judgment_text() 處理後的全文

    Returns:
        成功寫入的 citation 數量
    """
    citations = extract_citations(clean_text)
    inserted = 0

    for c in citations:
        target_id = upsert_target_placeholder(
            conn, c["jyear"], c["jcase_norm"], c["jno"]
        )
        if target_id is None:
            continue

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO citations (source_id, target_id, raw_match, match_start, match_end, snippet)
                    VALUES (%(source_id)s, %(target_id)s, %(raw_match)s, %(match_start)s, %(match_end)s, %(snippet)s)
                    ON CONFLICT (source_id, target_id, match_start) DO NOTHING
                """, {
                    "source_id": source_id,
                    "target_id": target_id,
                    "raw_match": c["raw_match"],
                    "match_start": c["match_start"],
                    "match_end": c["match_end"],
                    "snippet": c["snippet"],
                })
                conn.commit()
                inserted += 1
        except Exception as e:
            print(f"錯誤：寫入 citation 失敗 - {e}")
            conn.rollback()

    return inserted


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
    folder_name = folder.name
    court_info = parse_court_from_folder(folder_name)
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
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                json_data = json.load(f)

            if ingest_decision(conn, court_unit_id, court_info["root_norm"], json_data):
                success_count += 1

                # 同步抽取 citations（用 clean_text，offset 對應 clean_text）
                jfull = json_data.get("JFULL", "") or ""
                if jfull:
                    clean_text = clean_judgment_text(jfull)
                    source_id_row = None
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT id FROM decisions
                            WHERE court_root_norm = %(root_norm)s
                              AND jyear = %(jyear)s
                              AND jcase_norm = %(jcase_norm)s
                              AND jno = %(jno)s
                        """, {
                            "root_norm": court_info["root_norm"],
                            "jyear": int(json_data.get("JYEAR")),
                            "jcase_norm": normalize_jcase(json_data.get("JCASE", "")),
                            "jno": int(json_data.get("JNO")),
                        })
                        row = cur.fetchone()
                        if row:
                            source_id_row = row[0]

                    if source_id_row:
                        n = ingest_citations(conn, source_id_row, clean_text)
                        if n > 0:
                            print(f"  ↳ {json_file.name}: 寫入 {n} 筆 citation")
            else:
                fail_count += 1

            # 每 100 筆顯示進度
            if (success_count + fail_count) % 100 == 0:
                print(f"  進度：{success_count + fail_count}/{len(json_files)}")

        except Exception as e:
            print(f"錯誤：無法讀取 {json_file.name} - {e}")
            fail_count += 1

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


def main_batch(base_dir: str, keyword: str = "高等法院,民事"):
    """
    批次匯入：掃描 base_dir 下所有符合 keyword 的資料夾

    Args:
        base_dir: 例如 "/Users/rachel/Downloads/202511"
        keyword: 資料夾名稱關鍵字，預設 "高等法院民事"

    範例：
        python etl/ingest_decisions.py --batch /Users/rachel/Downloads/202511
        python etl/ingest_decisions.py --batch /Users/rachel/Downloads/202511 高等法院,刑事
    """
    base = Path(base_dir)
    folders = [f for f in base.iterdir() if f.is_dir() and all(k in f.name for k in keyword.split(","))]
    print(f"找到 {len(folders)} 個資料夾符合 '{keyword}'")
    for folder in sorted(folders):
        print(f"\n{'='*40}")
        print(f"處理：{folder.name}")
        main(str(folder))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方式：")
        print("  單一資料夾：python etl/ingest_decisions.py <資料夾路徑>")
        print("  批次匯入：  python etl/ingest_decisions.py --batch <基底目錄>")
        sys.exit(1)

    if sys.argv[1] == "--batch":
        if len(sys.argv) < 3:
            print("錯誤：--batch 需要指定基底目錄")
            sys.exit(1)
        keyword = sys.argv[3] if len(sys.argv) > 3 else "高等法院,民事"
        main_batch(sys.argv[2], keyword)
    else:
        main(sys.argv[1])
