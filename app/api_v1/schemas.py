from __future__ import annotations
from pydantic import BaseModel, model_validator
from typing import Literal


# ── Shared ────────────────────────────────────────────────────────────────────

class StatuteFilter(BaseModel):
    law: str
    article: str | None = None
    sub_ref: str | None = None


# ── POST /search ───────────────────────────────────────────────────────────────

class SearchRequest(BaseModel):
    keywords: list[str] = []
    statutes: list[StatuteFilter] = []
    exclude_keywords: list[str] = []
    exclude_statutes: list[StatuteFilter] = []
    case_types: list[str] = []
    doc_types: list[str] = []          # 篩選 target 的文書類型（判決/裁定/…）
    court_levels: list[int] = []       # 篩選 target 的法院層級（0-4）
    sort: Literal["relevance", "matched_citation_count", "total_citation_count"] = "relevance"
    page: int = 1
    page_size: int = 20

    @model_validator(mode="after")
    def at_least_one(self) -> SearchRequest:
        if not self.keywords and not self.statutes:
            raise ValueError("keywords 和 statutes 至少填一個")
        return self


class SearchResultItem(BaseModel):
    target_id: int | None
    authority_id: int | None
    court: str          # root_norm
    court_level: int | None
    jyear: int | None
    jcase_norm: str | None
    jno: int | None
    case_ref: str
    doc_type: str | None
    total_citation_count: int # 不受搜尋條件限制，歷史上被引用的總次數
    matched_citation_count: int # 符合搜尋條件的 source 數
    score: float


class SearchContext(BaseModel):
    keywords: list[str]
    statutes: list[StatuteFilter]
    exclude_keywords: list[str]
    exclude_statutes: list[StatuteFilter]


class SearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    source_count: int
    source_ids: list[int]               # 供 rerank 用，避免重打 OpenSearch
    results: list[SearchResultItem]
    search_context: SearchContext


class RerankRequest(BaseModel):
    source_ids: list[int]
    keywords: list[str] = []
    statutes: list[StatuteFilter] = []
    doc_types: list[str] = []
    court_levels: list[int] = []
    sort: Literal["relevance", "matched_citation_count", "total_citation_count"] = "relevance"
    page: int = 1
    page_size: int = 20


# ── GET /decisions/{id}/citations ──────────────────────────────────────────────

class CitationSource(BaseModel):
    citation_id: int
    source_id: int
    source_court: str       # unit_norm (簡易庭截到地方法院)
    source_court_level: int | None
    case_ref: str
    doc_type: str | None
    decision_date: str | None
    snippet: str | None
    match_start: int | None
    match_end: int | None
    raw_match: str
    statutes: list[dict]
    is_matched: bool
    score: float


class CitationTargetInfo(BaseModel):
    id: int
    target_type: str        # "decision" | "authority"
    court: str              # root_norm
    case_ref: str           # 案號 or authority display
    doc_type: str | None


class CitationsResponse(BaseModel):
    target: CitationTargetInfo
    total: int
    matched_total: int
    sources: list[CitationSource]


# ── GET /decisions/{id} ────────────────────────────────────────────────────────

class DecisionStatute(BaseModel):
    law: str
    article: str
    sub_ref: str
    count: int


class DecisionDetail(BaseModel):
    id: int
    court: str              # unit_norm
    court_root: str         # root_norm
    court_level: int | None
    case_ref: str
    doc_type: str | None
    decision_date: str | None
    title: str | None
    clean_text: str | None
    total_citation_count: int
    statutes: list[DecisionStatute]
