import time

from fastapi import APIRouter
from app.db import get_conn
from app.search_backend import get_search_backend

router = APIRouter(tags=["common"])


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/ready")
def ready():
    db_ok = False
    os_ok = False

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                db_ok = True
    except Exception:
        pass

    search_backend = get_search_backend()
    if search_backend == "opensearch":
        try:
            from app.opensearch_service import _get_opensearch_client
            client = _get_opensearch_client()
            info = client.info()
            os_ok = bool(info)
        except Exception:
            pass

    return {
        "status": "ok",
        "db": db_ok,
        "opensearch": os_ok,
        "search_backend": search_backend,
    }


_WARMUP_INTERVAL_SECONDS = 240  # 略短於資料庫的閒置休眠時間，同一實例內不重複暖機
_last_warmup_at: float | None = None


@router.get("/warmup")
def warmup():
    """
    頁面載入時由前端在背景呼叫：啟動 function、喚醒資料庫，並把判決全文與引用片段讀進快取，
    讓訪客送出第一次搜尋時不必等冷啟動。只在 PostgreSQL 搜尋後端（小資料集）啟用。
    """
    global _last_warmup_at
    if get_search_backend() != "postgres":
        return {"status": "ok", "warmed": False}

    now = time.monotonic()
    if _last_warmup_at is not None and now - _last_warmup_at < _WARMUP_INTERVAL_SECONDS:
        return {"status": "ok", "warmed": False}
    _last_warmup_at = now

    try:
        with get_conn() as conn:
            conn.execute("SELECT sum(length(clean_text)) FROM decisions").fetchone()
            conn.execute("SELECT sum(length(snippet)) FROM citations").fetchone()
    except Exception:
        _last_warmup_at = None
        return {"status": "ok", "warmed": False}
    return {"status": "ok", "warmed": True}
