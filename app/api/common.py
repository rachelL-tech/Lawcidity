from fastapi import APIRouter
from app.db import get_conn
from etl.law_names import LAW_NAMES, LAW_ALIASES

router = APIRouter()

# 建一份「簡稱 → 全名」查找表，autocomplete 時簡稱也能命中
_ALIAS_TO_CANONICAL = {alias: full for alias, full in LAW_ALIASES.items()}

# 預先把 LAW_NAMES 按字數小到大排序
_SORTED_LAWS = sorted(LAW_NAMES, key=len)


@router.get("/laws")
def get_laws(q: str = ""):
    """法律名稱 autocomplete，以 q 為開頭篩選白名單。簡稱也會推薦對應全名。"""
    q = q.strip()
    if not q:
        return {"laws": _SORTED_LAWS[:50]}
    # 從白名單找以 q 開頭的法律
    matched = [name for name in _SORTED_LAWS if name.startswith(q)]
    # 從簡稱找以 q 開頭的，推薦其對應全名（去重）
    seen = set(matched)
    for alias, canonical in _ALIAS_TO_CANONICAL.items():
        if alias.startswith(q) and canonical not in seen:
                matched.append(canonical)
                seen.add(canonical)
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
        from app.opensearch_service import _get_opensearch_client
        client = _get_opensearch_client()
        info = client.info()
        os_ok = bool(info)
    except Exception:
        pass

    return {"status": "ok", "db": db_ok, "opensearch": os_ok}
