from fastapi import APIRouter
from app.api_v1 import common, search, citations

router = APIRouter(prefix="/api/v1")
router.include_router(common.router)
router.include_router(search.router)
router.include_router(citations.router)
