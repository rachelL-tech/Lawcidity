from __future__ import annotations
from pydantic import BaseModel, ConfigDict, model_validator
from typing import Literal


# ── Shared ────────────────────────────────────────────────────────────────────

class StatuteFilter(BaseModel):
    law: str
    article: str | None = None
    sub_ref: str | None = None


# ── POST /search ───────────────────────────────────────────────────────────────

class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    keywords: list[str] = []
    statutes: list[StatuteFilter] = []
    exclude_keywords: list[str] = []
    exclude_statutes: list[StatuteFilter] = []
    case_types: list[str] = []
    sort: Literal["relevance"] = "relevance"
    page: int = 1
    page_size: int = 20

    @model_validator(mode="after")
    def keywords_required(self) -> SearchRequest:
        if not any(keyword.strip() for keyword in self.keywords):
            raise ValueError("keywords 至少填一個")
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
    total_citation_count: int # 不受搜尋條件限制，歷史上引用此 target 的 distinct source 數
    ranked_source_ids: list[int] = []


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
    search_cache_key: str | None = None
    results: list[SearchResultItem]
    search_context: SearchContext


class RerankRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    search_cache_key: str | None = None
    keywords: list[str] = []
    statutes: list[StatuteFilter] = []
    exclude_keywords: list[str] = []
    exclude_statutes: list[StatuteFilter] = []
    case_types: list[str] = []
    doc_types: list[str] = []
    court_levels: list[int] = []
    sort: Literal["relevance", "total_citation_count"] = "relevance"
    page: int = 1
    page_size: int = 20

    @model_validator(mode="after")
    def keywords_required(self) -> RerankRequest:
        if not any(keyword.strip() for keyword in self.keywords):
            raise ValueError("keywords 至少填一個")
        return self


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
    raw_match: str
    statutes: list[dict]


class CitationTargetInfo(BaseModel):
    id: int
    target_type: str        # "decision" | "authority"
    court: str              # root_norm
    case_ref: str           # 案號 or authority display
    doc_type: str | None


class CitationsResponse(BaseModel):
    target: CitationTargetInfo
    matched_total: int
    others_total: int
    matched_sources: list[CitationSource] | None = None
    others_sources: list[CitationSource] | None = None


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
    total_citation_count: int  # 歷史上引用此 target 的 distinct source 數
    statutes: list[DecisionStatute]


class RagResultTarget(BaseModel):
    id: int
    display_title: str
    root_norm: str
    total_citation_count: int
    target_type: str = "decision"   # "decision" | "authority"


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
