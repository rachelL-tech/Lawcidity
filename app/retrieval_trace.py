import statistics
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

@dataclass
class EmbedSpan:
    selected_issues: list[str]
    voyage_latency: float # ms

@dataclass
class KnnSpan:
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
    id: int
    raw_query: str
    created_at: str = ""
    gemini_latency: float | None = None # ms
    embed: EmbedSpan | None = None
    knn: list[KnnSpan] = field(default_factory=list)
    aggregation: list[AggregationSpan] = field(default_factory=list)

query_id = 0
_traces = deque(maxlen=100)

def new_trace(raw_query):
    global query_id
    query_id += 1
    trace = RetrievalTrace(
        id=query_id,
        raw_query=raw_query,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    _traces.append(trace)
    return trace
    
def get_recent(n):
    return list(_traces)[-n:]

def build_knn_span(issue, rows) -> KnnSpan:
    sims = sorted(1 - float(r["distance"]) for r in rows)
    if not sims:
        return KnnSpan(issue=issue, top_sim=0.0, median_sim=0.0, min_sim=0.0)
    return KnnSpan(
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
        })

    targets.sort(key=lambda t: t["chunk_count"], reverse=True)
    
    total = len(hits)

    concentration = targets[0]["chunk_count"] / total
    return AggregationSpan(issue=issue, targets=targets, concentration=round(concentration, 2))