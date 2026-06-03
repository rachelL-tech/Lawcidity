"""
観測用デバッグ endpoint。

- GET /debug/traces — 直近の retrieval trace（traces.jsonl）の JSON

trace は jsonl に永続化されるため reload / 再起動でも消えない。
UI は frontend の TracesPage（/traces）がこの JSON を fetch して描画する。
"""
from fastapi import APIRouter

from app.retrieval_trace import get_recent

router = APIRouter(tags=["debug"])

DEFAULT_TRACE_LIMIT = 20


@router.get("/debug/traces")
def list_traces(n: int = DEFAULT_TRACE_LIMIT):
    """直近 n 件の retrieval trace を返す。"""
    return get_recent(n)
