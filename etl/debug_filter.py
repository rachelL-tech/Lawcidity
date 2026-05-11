"""
debug_filter.py — 輸出每筆 candidate 的 filter path（完整 rule 判定過程）

使用方式：
  python etl/debug_filter.py /Users/rachel/Downloads/202512 --limit 500
  python etl/debug_filter.py /Users/rachel/Downloads/202512 --folder 臺灣高等法院民事 --limit 100
  python etl/debug_filter.py /Users/rachel/Downloads/202512 --file TCHM,114,金上訴,2240,20251230,1.json
  python etl/debug_filter.py /Users/rachel/Downloads/202512 --limit 500 --out ~/debug.jsonl
"""
import sys
import json
import random
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "etl"))

from etl.citation_parser import (
    find_all_candidates,
    make_filter_context,
    _RULES,
    find_snippet_start,
    find_snippet_end,
    _post_sentence,
    _accept_window,
    _prev_heading_pos,
    _PRIOR_CASE_RE,
    _EVIDENCE_CITE_RE,
    ACCEPT_RE,
    PARTY_CLAIM_RE,
)
from etl.court_parser import parse_court_from_folder
from etl.text_cleaner import clean_judgment_text


def _label(c):
    if c.citation_type == "decision":
        return "%s %s年%s字第%s號" % (c.court, c.jyear, c.jcase_norm, c.jno)
    return "%s %s" % (c.auth_type, c.auth_key)


def _rule_name(fn):
    # _r007_after_zhengben → R007
    return fn.__name__.split("_")[1].upper()


def _find_reject_pattern(c, ctx, reject_rule):
    """回傳觸發 reject 的具體 RE 命中字串（或 accept guard 的 window 摘要）。"""
    clean = ctx.clean_text
    if reject_rule == "R003_prior_case":
        after = _post_sentence(clean, c.match_end)
        m = _PRIOR_CASE_RE.search(after)
        return m.group() if m else None
    if reject_rule == "R011_evidence_cite":
        after = _post_sentence(clean, c.match_end)
        m = _EVIDENCE_CITE_RE.search(after)
        return m.group() if m else None
    if reject_rule in ("R002_ben_yuan_missing_intent",
                       "R009_district_court_missing_intent",
                       "R010_authority_missing_intent"):
        window = _accept_window(clean, c.match_start, c.match_end)
        return "(no ACCEPT_RE; window=%d chars)" % len(window)
    if reject_rule == "R005a_party_context":
        heading_pos = _prev_heading_pos(clean, c.match_start)
        section_before = clean[heading_pos: c.match_start]
        m = PARTY_CLAIM_RE.search(section_before)
        return m.group() if m else None
    return None


def debug_file(json_path, court_info):
    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None

    jfull = data.get("JFULL") or ""
    if not jfull:
        return None

    clean = clean_judgment_text(jfull)
    jcase_norm = str(data.get("JCASE", "")).replace("臺", "台")
    self_key = (
        court_info.get("court_root_norm", "").replace("臺", "台"),
        int(data.get("JYEAR", 0)),
        jcase_norm,
        int(data.get("JNO", 0)),
    )

    cands = find_all_candidates(clean, self_key[0])
    if not cands:
        return None

    ctx = make_filter_context(clean, self_key=self_key)

    rows = []
    for c in cands:
        path = []
        final = "ACCEPT"
        reject_rule = None
        for rule in _RULES:
            reason = rule(c, ctx)
            rname = _rule_name(rule)
            if reason is not None:
                path.append("%s:REJECT(%s)" % (rname, reason))
                final = "REJECT"
                reject_rule = reason
                break
            else:
                path.append("%s:pass" % rname)

        ss = find_snippet_start(clean, c.match_start)
        se = find_snippet_end(clean, c.match_start, c.match_end)
        snippet = clean[ss:se]

        reject_pattern = _find_reject_pattern(c, ctx, reject_rule) if reject_rule else None

        rows.append({
            "file": json_path.name,
            "folder": json_path.parent.name,
            "result": final,
            "reject_rule": reject_rule,
            "reject_pattern": reject_pattern,
            "label": _label(c),
            "type": c.citation_type,
            "path": " ".join(path),
            "match_start": c.match_start,
            "snippet_len": se - ss,
            "snippet": snippet,
        })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base_dir")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--folder", default="")
    ap.add_argument("--file", default="", help="指定單一檔名（在所有資料夾中搜尋）")
    ap.add_argument("--out", default="", help="JSONL 輸出路徑")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--reject-only", action="store_true", help="只顯示被 reject 的")
    ap.add_argument("--accept-only", action="store_true", help="只顯示被 accept 的")
    args = ap.parse_args()

    base = Path(args.base_dir)
    folders = [f for f in base.iterdir() if f.is_dir()]
    if args.folder:
        folders = [f for f in folders if args.folder in f.name]

    all_files = []
    for folder in folders:
        info = parse_court_from_folder(folder.name)
        if not info:
            continue
        if args.file:
            fp = folder / args.file
            if fp.exists():
                all_files.append((fp, info))
        else:
            for jp in folder.glob("*.json"):
                all_files.append((jp, info))

    print("檔案數：%d" % len(all_files))

    if args.limit and args.limit < len(all_files):
        rng = random.Random(args.seed)
        all_files = rng.sample(all_files, args.limit)
        print("取樣 %d 筆（seed=%d）" % (args.limit, args.seed))

    out_fh = open(args.out, "w", encoding="utf-8") if args.out else None
    stats = {"files": 0, "accept": 0, "reject": 0}
    reject_counts = {}

    for i, (jp, info) in enumerate(all_files):
        if (i + 1) % 200 == 0:
            print("  進度 %d/%d ..." % (i + 1, len(all_files)))

        rows = debug_file(jp, info)
        if rows is None:
            continue
        stats["files"] += 1

        for row in rows:
            is_reject = row["result"] == "REJECT"
            if args.reject_only and not is_reject:
                continue
            if args.accept_only and is_reject:
                continue

            if is_reject:
                stats["reject"] += 1
                reject_counts[row["reject_rule"]] = reject_counts.get(row["reject_rule"], 0) + 1
            else:
                stats["accept"] += 1

            if out_fh:
                out_fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            else:
                print("[%s] %s" % (row["result"], row["label"]))
                print("  file: %s/%s" % (row["folder"], row["file"]))
                print("  path: %s" % row["path"])
                if row["reject_pattern"]:
                    print("  pattern: %s" % row["reject_pattern"])
                print("  snippet(%d): %s" % (row["snippet_len"], repr(row["snippet"][:300])))
                print()

    # 統計
    print("=" * 60)
    print("掃描 %d 檔案" % stats["files"])
    print("ACCEPT: %d  REJECT: %d" % (stats["accept"], stats["reject"]))
    if reject_counts:
        print()
        print("Reject 分布：")
        for rule, cnt in sorted(reject_counts.items(), key=lambda x: -x[1]):
            print("  %-40s %d" % (rule, cnt))

    if out_fh:
        out_fh.close()
        print("輸出：%s" % args.out)


if __name__ == "__main__":
    main()
