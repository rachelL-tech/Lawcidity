"""
compare_parsers.py — 比較 extract_citations（舊）與 extract_citations_next（新）的輸出差異

「舊」基準：主目錄 /Users/rachel/Desktop/claude/final-project/etl/citation_parser.py
「新」版本：本 worktree 的 etl/citation_parser.py

使用方式：
  python etl/compare_parsers.py /Users/rachel/Downloads/202512          # 全量
  python etl/compare_parsers.py /Users/rachel/Downloads/202512 --limit 500
  python etl/compare_parsers.py /Users/rachel/Downloads/202512 --folder 臺灣高等法院民事
  python etl/compare_parsers.py /Users/rachel/Downloads/202512 --limit 200 --show-examples
  python etl/compare_parsers.py /Users/rachel/Downloads/202512 --out ~/diff.jsonl
"""
import sys
import json
import random
import argparse
import importlib.util
from pathlib import Path
from collections import defaultdict
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 主目錄（老基準）
MAIN_ROOT = Path("/Users/rachel/Desktop/claude/final-project")
_main_etl = str(MAIN_ROOT / "etl")
if _main_etl not in sys.path:
    sys.path.insert(0, _main_etl)   # court_parser 等 dependency 從這裡找

def _load_old_extract_citations():
    """從主目錄載入未修改的 citation_parser.extract_citations。"""
    old_path = MAIN_ROOT / "etl" / "citation_parser.py"
    spec = importlib.util.spec_from_file_location("citation_parser_main", old_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["citation_parser_main"] = mod
    spec.loader.exec_module(mod)
    return mod.extract_citations

from etl.court_parser import parse_court_from_folder
from etl.text_cleaner import clean_judgment_text
from etl.citation_parser import extract_citations_next

extract_citations = _load_old_extract_citations()


# ─── 自然鍵 ────────────────────────────────────────────────────────────────────

def _decision_key(c) -> tuple:
    """決策引用的自然鍵：(court, jyear, jcase_norm, jno)"""
    if isinstance(c, dict):
        return (c["court"], c["jyear"], c["jcase_norm"], c["jno"])
    return (c.court, c.jyear, c.jcase_norm, c.jno)


def _authority_key(c) -> tuple:
    """authority 的自然鍵：(auth_type, auth_key)"""
    if isinstance(c, dict):
        return (c["auth_type"], c["auth_key"])
    return (c.auth_type, c.auth_key)


def _cite_key(c) -> tuple:
    ctype = c["citation_type"] if isinstance(c, dict) else c.citation_type
    if ctype == "decision":
        return ("decision",) + _decision_key(c)
    return ("authority",) + _authority_key(c)


def _snippet(c) -> str:
    if isinstance(c, dict):
        return c.get("snippet", "")
    return c.snippet or ""


# ─── 單檔比較 ───────────────────────────────────────────────────────────────────

def compare_file(
    json_path: Path,
    court_info: dict,
) -> Optional[dict]:
    """回傳 {added, removed, snippet_changed, old_count, new_count} 或 None（跳過）"""
    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None

    jfull = data.get("JFULL") or ""
    if not jfull:
        return None

    clean_text = clean_judgment_text(jfull)

    root_norm = court_info.get("court_root_norm", "")
    unit_norm = court_info.get("unit_norm", "")
    jcase_norm = str(data.get("JCASE", "")).replace("臺", "台")
    self_key = (
        root_norm.replace("臺", "台"),
        int(data.get("JYEAR", 0)),
        jcase_norm,
        int(data.get("JNO", 0)),
    )

    old_results = extract_citations(
        clean_text,
        court_root_norm=root_norm,
        self_key=self_key,
    )
    new_results = extract_citations_next(
        clean_text,
        court_root_norm=root_norm,
        self_key=self_key,
    )

    old_map = {_cite_key(c): _snippet(c) for c in old_results}
    new_map = {_cite_key(c): _snippet(c) for c in new_results}

    old_keys = set(old_map)
    new_keys = set(new_map)

    added   = new_keys - old_keys
    removed = old_keys - new_keys
    common  = old_keys & new_keys
    snippet_changed = {k for k in common if old_map[k] != new_map[k]}

    return {
        "file":    json_path.name,
        "folder":  json_path.parent.name,
        "old_count": len(old_keys),
        "new_count": len(new_keys),
        "added":   added,
        "removed": removed,
        "snippet_changed": snippet_changed,
        "old_map": old_map,
        "new_map": new_map,
    }


# ─── 主程式 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("base_dir")
    parser.add_argument("--limit",        type=int, default=0,  help="隨機取樣 N 筆（0=全量）")
    parser.add_argument("--folder",       default="",           help="只跑指定資料夾（部分字串匹配）")
    parser.add_argument("--show-examples",action="store_true",  help="印出 added/removed/snippet_changed 範例")
    parser.add_argument("--out",          default="",           help="結果輸出到 JSONL 檔案路徑（只寫有差異的檔案）")
    parser.add_argument("--seed",         type=int, default=42)
    args = parser.parse_args()

    base = Path(args.base_dir)
    folders = [f for f in base.iterdir() if f.is_dir()]
    if args.folder:
        folders = [f for f in folders if args.folder in f.name]

    # 收集所有 JSON 路徑（含法院 info）
    all_files: list[tuple[Path, dict]] = []
    for folder in folders:
        info = parse_court_from_folder(folder.name)
        if not info:
            continue
        for jp in folder.glob("*.json"):
            all_files.append((jp, info))

    print(f"總計 {len(all_files)} 個 JSON 檔案（{len(folders)} 個資料夾）")

    if args.limit and args.limit < len(all_files):
        rng = random.Random(args.seed)
        all_files = rng.sample(all_files, args.limit)
        print(f"取樣 {args.limit} 筆（seed={args.seed}）")

    # 統計
    totals = defaultdict(int)
    examples: dict[str, list] = {"added": [], "removed": [], "snippet_changed": []}
    out_fh = open(args.out, "w", encoding="utf-8") if args.out else None

    for i, (jp, info) in enumerate(all_files):
        if (i + 1) % 500 == 0:
            print(f"  進度 {i+1}/{len(all_files)} ...")

        result = compare_file(jp, info)
        if result is None:
            totals["skipped"] += 1
            continue

        totals["files"] += 1
        totals["old_total"] += result["old_count"]
        totals["new_total"] += result["new_count"]
        totals["added"]   += len(result["added"])
        totals["removed"] += len(result["removed"])
        totals["snippet_changed"] += len(result["snippet_changed"])

        if result["added"]:   totals["files_with_added"] += 1
        if result["removed"]: totals["files_with_removed"] += 1

        # JSONL 輸出：每筆 diff 一行（per-citation）
        if out_fh:
            base_info = {"folder": result["folder"], "file": result["file"]}
            for k in result["added"]:
                out_fh.write(json.dumps({
                    **base_info,
                    "change_type": "added",
                    "key": list(k),
                    "new_snippet": result["new_map"][k],
                }, ensure_ascii=False) + "\n")
            for k in result["removed"]:
                out_fh.write(json.dumps({
                    **base_info,
                    "change_type": "removed",
                    "key": list(k),
                    "old_snippet": result["old_map"][k],
                }, ensure_ascii=False) + "\n")
            for k in result["snippet_changed"]:
                out_fh.write(json.dumps({
                    **base_info,
                    "change_type": "snippet_changed",
                    "key": list(k),
                    "old_snippet": result["old_map"][k],
                    "new_snippet": result["new_map"][k],
                }, ensure_ascii=False) + "\n")

        if args.show_examples:
            for key in list(result["added"])[:2]:
                examples["added"].append({
                    "file": f'{result["folder"]}/{result["file"]}',
                    "key": key,
                    "new_snippet": result["new_map"][key][:120],
                })
            for key in list(result["removed"])[:2]:
                examples["removed"].append({
                    "file": f'{result["folder"]}/{result["file"]}',
                    "key": key,
                    "old_snippet": result["old_map"][key][:120],
                })
            for key in list(result["snippet_changed"])[:1]:
                examples["snippet_changed"].append({
                    "file": f'{result["folder"]}/{result["file"]}',
                    "key": key,
                    "old": result["old_map"][key][:120],
                    "new": result["new_map"][key][:120],
                })

    # ─── 結果輸出 ──────────────────────────────────────────────────────────────
    print()
    print("═" * 60)
    print("比較結果")
    print("═" * 60)
    print(f"掃描檔案：{totals['files']}（跳過 {totals['skipped']}）")
    print(f"舊版 citations 總計：{totals['old_total']}")
    print(f"新版 citations 總計：{totals['new_total']}")
    delta = totals['new_total'] - totals['old_total']
    print(f"淨變化：{delta:+d}")
    print()
    print(f"新版新增（added）：   {totals['added']:5d}  "
          f"（影響 {totals['files_with_added']} 個檔案）")
    print(f"新版移除（removed）： {totals['removed']:5d}  "
          f"（影響 {totals['files_with_removed']} 個檔案）")
    print(f"snippet 變更：       {totals['snippet_changed']:5d}")
    print()

    if out_fh:
        out_fh.close()
        print(f"結果已寫入：{args.out}")

    if totals["old_total"] > 0:
        add_pct    = totals["added"]           / totals["old_total"] * 100
        remove_pct = totals["removed"]         / totals["old_total"] * 100
        change_pct = totals["snippet_changed"] / totals["old_total"] * 100
        print(f"added   佔舊版 {add_pct:.2f}%")
        print(f"removed 佔舊版 {remove_pct:.2f}%")
        print(f"snippet 佔舊版 {change_pct:.2f}%")

    if args.show_examples:
        for category in ("added", "removed", "snippet_changed"):
            if not examples[category]:
                continue
            print()
            print(f"─── {category} 範例（最多 10 筆）───")
            for ex in examples[category][:10]:
                print(f'  [{ex["file"]}]')
                print(f'  key: {ex["key"]}')
                if "new_snippet" in ex:
                    print(f'  new: {ex["new_snippet"]!r}')
                if "old_snippet" in ex:
                    print(f'  old: {ex["old_snippet"]!r}')
                if "old" in ex:
                    print(f'  old: {ex["old"]!r}')
                    print(f'  new: {ex["new"]!r}')
                print()


if __name__ == "__main__":
    main()
