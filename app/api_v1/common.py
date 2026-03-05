from fastapi import APIRouter
from app.db import get_conn
from etl.law_names import LAW_NAMES

router = APIRouter()


@router.get("/laws")
def get_laws(q: str = ""):
    """法律名稱 autocomplete，從 1028 筆白名單篩選。"""
    q = q.strip()
    if not q:
        return {"laws": LAW_NAMES[:50]}
    matched = [name for name in LAW_NAMES if q in name]
    return {"laws": matched[:20]}


@router.get("/health")
def health():
    db_ok = False
    os_ok = False

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                db_ok = True
    except Exception:
        pass

    try:
        from app.search_service import _get_opensearch_client
        client = _get_opensearch_client()
        info = client.info()
        os_ok = bool(info)
    except Exception:
        pass

    return {"status": "ok", "db": db_ok, "opensearch": os_ok}
