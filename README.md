# Lawcidity

Lawcidity helps lawyers find authoritative court holdings faster, and an AI-assisted flow that clarifies legal intent before analization.

**🔗 [Page](https://lawcidity.rachel-create.com/)** — try keyword search or RAG-based retrieval on real court data.

> Try these searches:
> - **Keyword search**: keyword「行車紀錄器」「車禍」＋ statute「刑法」「284」 — see citation-linked rankings
> - **RAG search**: 「如果我騎車，對方碰瓷，但沒有行車記錄器，該怎麼主張無過失？」 → 確認爭點、法條 → 生成 AI 分析

---

## Demo

### Keyword Search

(1) Enter free-text keywords (e.g. "車禍", "行車記錄器") and optional statute filters with autocomplete — type a law name ("刑法") and article number ("284").  
![](frontend/public/gif/keyword-1-input.gif)

(2) Sort results by **relevance** or **citation count**; filter by **documentation type** and **court level**.  
![](frontend/public/gif/keyword-2-sort-filter.gif)

(3) Click a target to see citation snippets from sources — split into snippets that **match** the search criteria, and snippets that cite the same target but **fall outside** the criteria.  
![](frontend/public/gif/keyword-3-snippets.gif)

(4) Click a snippet's source title to open the full decision, with a **jump-to-snippet** button.  
![](frontend/public/gif/keyword-4-decision.gif)

### RAG Search

(1) Describe a case in natural language → click **AI Analyze** to extract candidate legal issues and statutes → confirm before submitting.  
![](frontend/public/gif/rag-1-analyze.gif)

(2) The analysis page shows **confirmed search parameters** on the left, **Gemini-generated analysis** per issue on the upper right, and **reference sources** on the lower right.  
![](frontend/public/gif/rag-2-analysis-page.gif)

(3) Click an **orange block** (source) to open the decision detail page; click a **gray block** (target) to see how many times it has been cited.  
![](frontend/public/gif/rag-3-blocks.gif)

---

## Features

- **Keyword search**
  - Citation-linked target ranking from keyword and statute queries
  - Filters for statute, case type, document type, and court level
  - Citation expansion and full decision drill-down

- **RAG-based search**
  - Gemini-assisted issue and statute extraction from factual queries
  - Dual-path retrieval with pgvector semantic search and statute-guided matching
  - AI-generated legal analysis with supporting decisions

- **Decision detail page**
  - Full decision text with citation snippet highlighting and jump-to-snippet navigation

---

## Architecture

![Architecture](frontend/public/Architecture.png)

| Layer | Technology |
|---|---|
| Frontend | React 19, Tailwind CSS 4 |
| Application | FastAPI |
| Keyword search | OpenSearch (2-gram ngram analyzer) |
| Semantic search | pgvector (ivfflat) |
| Storage | PostgreSQL |
| AI services | Gemini Flash, Voyage API (voyage-law-2) |
| Infrastructure | AWS EC2, RDS, ALB, nginx |

**Data source:** [Judicial Yuan Open Data Platform](https://opendata.judicial.gov.tw/) — public court decisions, 2025-01 to 2026-01.

**Key technology decisions:**
- **OpenSearch** over PostgreSQL GIN: 27× faster on cited-decision retrieval, less than a third of the index storage
- **pgvector** over a managed vector DB: co-located with relational data, avoids extra service dependency
- **Voyage API (voyage-law-2)** over local embedding: legal-domain model with better recall; freed local machine for parallel development; 6–7 chunks/s vs 3.3 chunks/s local

**Project structure:**

```
lawcidity/
├── app/                  # FastAPI application (API routes, RAG service, search service)
├── etl/                  # Data pipeline (citation parser, statute extractor, chunk builders)
├── scripts/              # Embedding, OpenSearch sync, evaluation
├── tests/
│   ├── test_citation_parser.py   # Citation extraction regression tests
│   ├── test_snippet.py           # Snippet boundary tests
│   ├── test_search_service.py    # SQL builder unit tests
│   └── test_doc_type_normalize.py
├── frontend/             # React + Vite
│   └── src/
│       ├── pages/        # HomePage, DemoPage, KeywordResultsPage, AiResultsPage, DecisionPage
│       └── components/   # SearchForm, AiSearchForm, ModeToggle, LawCombobox, ResultCard, ...
├── sql/                  # Schema migrations
└── docker-compose.yml    # Local dev stack
```

---

## Data Model

### PostgreSQL ER Diagram
![PostgreSQL ER Diagram](frontend/public/er-diagram-overview.png)
For a detailed version, see [er-diagram-detail.png](frontend/public/er-diagram-detail.png).

**Core tables:**

| Table | Rows | Description |
|---|---|---|
| `decisions` | 1.4M | Court decisions with normalized metadata |
| `citations` | 552K | Source → target citation links with snippet positions |
| `chunks` | 575K | Embedding chunks (citation-context + supreme reasoning) |
| `decision_reason_statutes` | 5.2M | Statute references extracted from decision reasoning |
| `citation_snippet_statutes` | 298K | Statute references within citation snippets |
| `authorities` | 1.6K | Authoritative decisions, resolutions, interpretations |

### PostgreSQL-to-OpenSearch Index Flow
![PostgreSQL-to-OpenSearch Index Flow](frontend/public/index-flow.png)

**Core Indexs:**

---

## Why This Problem Is Hard

Building a usable legal search system means handling messy data at scale, where no single retrieval method works for every search goal.

- **Unstable document structure** — court decisions vary in format across judges, case types, and writing styles. Section markers, citation signals, and document type labels are all inconsistent, making reliable parsing difficult.
- **Ambiguous references** — a case number is not always a legal citation — it may refer to an attachment, a procedural history, or a summary of another ruling. The core challenge is classification and context-sensitive filtering, not just extraction.
- **Scale and infrastructure** — over a million decisions require indexed retrieval, pre-computed fields, and deployment-aware infrastructure choices across both keyword search and vector search paths.
- **Dual retrieval paths** — running two retrieval paths means maintaining two independent pipelines with different scoring logic, ranking strategies, and result presentation formats — and neither path's raw output can be shown directly to users.

---

## Technical Decisions

### Unstable document structure

Court decisions do not follow a single stable format. Their structure varies across judges, case types, and writing styles, and section boundaries may appear as numbered clauses, sub-clauses, outline markers, or plain numerals. Legal references may appear in citation chains, inside brackets, with different trailing signals such as "意旨參照", "可參", or "理由書闡釋略以", and with inconsistent document type labels such as 判決, 裁定, 裁判, 理由, or 憲判字.

**Decision:** I defined expected outputs for these unstable patterns and used iterative validation to make the normalization layer more predictable and more usable for downstream retrieval.

### Ambiguous references

A case number is not always a legal authority citation. It may instead refer to an attachment in the record, a prior procedural history, or a summary of another ruling. The core problem is not only extraction, but classification and context-sensitive filtering.

**Decision:** I refactored the citation parser into smaller, traceable functions so each rule could be inspected, tested, and debugged against real failure cases. This made the pipeline easier to reason about and reduced the risk of reintroducing bugs that had already been observed in real cases.

### Scale and infrastructure

With over a million decisions, both retrieval paths faced volume and technology challenges.

#### Scale

For keyword search, a naive PostgreSQL sequential scan was far too slow. I benchmarked PostgreSQL GIN against an unindexed ILIKE scan, then compared GIN and OpenSearch on the same cited-decision pool (latency, result volume, storage size). OpenSearch was roughly 27× faster and used less than a third of the index storage, so it became the primary retrieval path.

For semantic search, embedding hundreds of thousands of chunks required managing volume and cost. I designed citation-context chunks to extract high-value passages near citation references rather than embedding full documents, reducing chunk volume and embedding cost. Supreme court reasoning was chunked as complete passages since the full text carries high value as RAG generation input. Text-level deduplication (via md5 hashing) ensured identical chunks were only embedded once, and Voyage API batch processing reached roughly 6–7 chunks/s — compared to about 3.3 chunks/s with a local MLX model on the same machine.

#### Infrastructure

For keyword search, ik tokenization underperformed on legal vocabulary because segmentation was not predictable enough for this domain. I moved toward a 2-gram plus match_phrase strategy instead of relying on IK tokenization alone.

For semantic search, I compared multiple embedding candidates across several rounds (BAAI/bge-m3, Qwen3-Embedding-0.6B, Qwen3-Embedding-4B, Gemini embedding, voyage-multilingual-2, voyage-law-2, voyage-4-large). The evaluation looked not only at Recall@5, but also at score gap between related and unrelated snippets, so the decision reflected both retrieval quality and ranking stability. Vector index type also depended on the deployment target's memory constraints — HNSW locally for faster recall, ivfflat on RDS where RAM was limited. The production embedding backend moved toward Voyage API because local generation interfered with parallel development.

### Dual retrieval paths

Running two retrieval paths means maintaining two independent pipelines with different scoring logic, ranking strategies, and result presentation formats. Neither path's raw retrieval output can be presented directly to users, and the two paths serve different user intents — keyword search users expect to browse citation-linked rankings and find answers from context themselves, while RAG users expect a ready-made analysis generated from retrieved evidence.

**Decision:** I separated the two paths into distinct result experiences. The keyword result page presents source-target citation pairs ranked by hit rate and citation count, with filters for court level and document type. The RAG result page presents AI-generated analysis guided by confirmed issues and statutes, with supporting decisions as references.

For keyword ranking, the original approach scored each target by running PostgreSQL ILIKE across all citation snippets — a binary hit per keyword term, scanning every matching source. This was exhaustive but scaled linearly with corpus size (roughly 18 seconds for broad queries). I moved scoring to a pre-indexed source-target window index in OpenSearch, where each keyword's proximity to the citation position determines its weight (snippet context = 3.0, post-citation 100 chars = 1.5, 200 chars = 0.5, pre-citation 60 chars = 0.2). Source retrieval is capped at 50,000, which bounds query time regardless of corpus growth. The trade-off: targets cited only by sources ranked below the cap may appear with incomplete citation counts or be absent entirely — but query time dropped from roughly 18 seconds to roughly 3 seconds, and the proximity-weighted signal produces more relevant top-K rankings than binary keyword matching.

For RAG retrieval, a single semantic search path misses results that are topically relevant but not close enough in embedding space, especially when the user has specific statutes in mind. I used a dual-path retrieval design: Path A runs HNSW approximate nearest-neighbor search for the top 50 semantically similar chunks, while Path B finds chunks whose associated citations or decisions match the user's confirmed statutes and computes vector distances via brute-force scan. The two paths are merged by chunk ID, and each chunk is scored as cosine similarity plus a statute-match boost. Chunks are not arbitrary text splits — they are either citation-context passages (text surrounding a citation reference in lower-court decisions) or supreme-reasoning passages (complete reasoning sections from Supreme Court decisions), so each chunk carries a citation link back to the target it discusses. The user-facing flow has two stages: Gemini first extracts candidate issues and statutes from a factual description, the user confirms which to keep, and then the confirmed inputs drive both retrieval paths and the final generated analysis.

---

## Development Journey

Eight weeks of iterative development, from raw court documents to a working search product.

| Phase | Period | Key work |
|---|---|---|
| **1. Parsing & normalization** | Feb 12–24 | Citation parser (state machine), statute extraction, false positive filtering, schema v1→v4 |
| **2. Keyword search** | Feb 25 – Mar 3 | OpenSearch vs PostgreSQL GIN benchmark, IK → 2-gram ngram migration, citation expansion scoring |
| **3. API & frontend** | Mar 5–13 | REST API with SQL aggregation, React search UI with filters, Docker + EC2 deployment |
| **4. Parser refactor** | Mar 14–21 | Rewrote citation parser into traceable functions, tightened false positive rules, sibling citation dedup |
| **5. Semantic search & RAG** | Mar 22–27 | Multi-round embedding evaluation, citation-context + supreme reasoning chunks, dual-path retrieval, Gemini AI analysis |
| **6. Optimization & deploy** | Mar 26–30 | RAG merge layer optimization, chunk dedup, issue-based embedding search, HTTPS production deployment |

---

## Contribution

This project was built with AI-assisted development, but the problem framing, evaluation design, and technical decisions were my responsibility.

- **I led:** Problem framing, expected outputs, evaluation setup, search architecture decisions, parser refactoring direction, and final technical trade-offs.
- **AI-assisted implementation:** AI tools supported parts of normalization logic, scripts, and UI scaffolding during prototyping and implementation.
- **I validated and converged:** I verified outputs, debugged failure cases, and made the final decisions based on retrieval quality, system constraints, and product usability.

---

## Future Work

- Redesign chunk boundaries — explore semantic chunking or LLM-based chunking to better separate factual context from legal reasoning
- Validate whether rewriting queries into more precise legal language improves retrieval recall
