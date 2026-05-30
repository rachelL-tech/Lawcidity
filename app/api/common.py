from fastapi import APIRouter
from app.db import get_conn

router = APIRouter()


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

    try:
        from app.opensearch_service import _get_opensearch_client
        client = _get_opensearch_client()
        info = client.info()
        os_ok = bool(info)
    except Exception:
        pass

    return {"status": "ok", "db": db_ok, "opensearch": os_ok}
