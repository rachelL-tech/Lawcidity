import hashlib
import json
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

@dataclass
class EmbedSpan:
    selected_issues: list[str]
    voyage_latency: float # ms

@dataclass
class AnnSpan:
    issue: str
    top_sim: float
    median_sim: float
    min_sim: float

@dataclass
class AggregationSpan:
    issue: str
    targets: list = field(default_factory=list)
    concentration: float = 0.0 # no.1 的 chunk_count ÷ 所有 chunk_count 總和

# 1. 1 リクエストにつき 1 トレース。
# 2. session_id（将来対応）：抽出(/analyze) と 生成(/analyze/generate) は別々の
# リクエストであり、それぞれ 1 トレースになる。ユーザー全体の流れを見たい場合、
# 両者を 1 つのトレースに詰め込むことではなく、共通の session_id を
# 付与し、観測レイヤーで group 化することに選択。
# 3.トレードオフ（drop-off）：
# ユーザーは抽出後に離脱し生成まで進まないことが多い。
# 「分離 + session 紐付け」なら抽出側のトレースは単体で完結する。一方「1 トレースで　
# 2 リクエストを貫く」設計にすると、離脱時に後半が永久に欠けた不完全なトレースが残る。
# したがって分離して記録。
@dataclass
class RetrievalTrace:
    id: str
    raw_query: str
    created_at: str = ""
    gemini_latency: float | None = None # ms
    embed: EmbedSpan | None = None
    ann: list[AnnSpan] = field(default_factory=list)
    aggregation: list[AggregationSpan] = field(default_factory=list)

# trace を jsonl に追記して永続化する（in-memory と違い reload / 再起動で消えない）。
_TRACES_PATH = Path(__file__).resolve().parents[1] / "traces.jsonl"


def new_trace(raw_query):
    # id は created_at の hash：reload でカウンタがリセットされても衝突しない。
    created_at = datetime.now(timezone.utc).isoformat()
    trace_id = hashlib.md5(created_at.encode("utf-8")).hexdigest()[:8]
    return RetrievalTrace(id=trace_id, raw_query=raw_query, created_at=created_at)


def persist_trace(trace: "RetrievalTrace") -> None:
    """完成した trace を 1 行の JSON として jsonl に追記する。"""
    with _TRACES_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(trace), ensure_ascii=False) + "\n")


def get_recent(n):
    """jsonl の末尾 n 件を dict のリストで返す。"""
    if not _TRACES_PATH.exists():
        return []
    with _TRACES_PATH.open(encoding="utf-8") as f:
        lines = f.readlines()
    return [json.loads(line) for line in lines[-n:]]

def build_ann_span(issue, rows) -> AnnSpan:
    sims = sorted(1 - float(r["distance"]) for r in rows)
    if not sims:
        return AnnSpan(issue=issue, top_sim=0.0, median_sim=0.0, min_sim=0.0)
    return AnnSpan(
        issue=issue,
        top_sim=round(sims[-1], 4),
        median_sim=round(statistics.median(sims), 4),
        min_sim=round(sims[0], 4),
    )

def build_aggregation_span(issue, hits) -> AggregationSpan:
    if not hits:
        return AggregationSpan(issue=issue, targets=[], concentration=0.0)
    
    groups = defaultdict(list)
    for h in hits:
        key = (h["type"], h["id"])
        groups[key].append(h)

    targets = []
    for members in groups.values():
        targets.append({
            "type": members[0]["type"],
            "display": members[0]["display"],
            "chunk_count": len(members),
            "total_citation_count": members[0]["total_citation_count"],
            # 該 target 被召回的每段 chunk_text（先放全文，驗證「同 target 文字是否相似」的假說）
            "chunk_texts": [m["chunk_text"] for m in members],
        })

    targets.sort(key=lambda t: t["chunk_count"], reverse=True)
    
    total = len(hits)

    concentration = targets[0]["chunk_count"] / total
    return AggregationSpan(issue=issue, targets=targets, concentration=round(concentration, 2))