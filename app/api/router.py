from fastapi import APIRouter
from app.api import common, search, citations, decisions

router = APIRouter(prefix="/api")
router.include_router(common.router)
router.include_router(search.router)
router.include_router(citations.router)
router.include_router(decisions.router)
