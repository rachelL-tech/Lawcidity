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


# ── POST /search/semantic ──────────────────────────────────────────────────────

class SemanticSearchRequest(BaseModel):
    query: str
    case_type: str | None = None   # 民事/刑事/行政（可選）
    k: int = 200                   # knn 召回 chunk 數
    page: int = 1
    page_size: int = 20


class SemanticTarget(BaseModel):
    target_id: int | None
    authority_id: int | None
    case_ref: str
    court: str
    doc_type: str | None


class SemanticSourceItem(BaseModel):
    source_id: int
    case_ref: str
    court: str
    doc_type: str | None
    decision_date: str | None
    score: float
    chunk_count: int
    cited_targets: list[SemanticTarget]


class SemanticSearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[SemanticSourceItem]


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


# ── POST /search/rag ──────────────────────────────────────────────────────────

class RagStatuteFilter(BaseModel):
    law: str
    article: str


class RagSearchRequest(BaseModel):
    query: str
    case_type: str | None = None
    statutes: list[RagStatuteFilter] = []
    boost: float = 0.15
    authority_boost: float = 0.05
    top: int = 20


class RagResultTarget(BaseModel):
    id: int
    display_title: str
    root_norm: str
    total_citation_count: int


class RagResultItem(BaseModel):
    type: str                   # "citation" | "supreme" | "supreme+citation"
    decision_id: int
    root_norm: str
    display_title: str
    doc_type: str | None
    decision_date: str | None
    case_type: str | None
    score: float
    sim: float
    statute_hit: bool
    chunk_count: int
    chunk_types: list[str]
    best_chunk_text: str
    targets: list[RagResultTarget]


class RagSearchResponse(BaseModel):
    total: int
    results: list[RagResultItem]


# ── POST /analyze ────────────────────────────────────────────────────────────

class AnalyzeStatute(BaseModel):
    law: str
    article: str


class AnalyzeRequest(BaseModel):
    text: str


class AnalyzeResponse(BaseModel):
    issues: list[str]
    statutes: list[AnalyzeStatute]


# ── POST /analyze/generate ───────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    query: str
    issues: list[str] = []
    statutes: list[AnalyzeStatute] = []
    case_type: str | None = None
    top: int = 10


class GenerateResponse(BaseModel):
    analysis: str
    rag_results: list[RagResultItem]
