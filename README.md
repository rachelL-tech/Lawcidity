# Lawcidity

Lawcidity helps lawyers find authoritative court holdings faster, and an AI-assisted flow that clarifies legal intent before analization.

**🔗 [Live Demo](https://your-domain.com)** — try keyword search or RAG-based retrieval on real court data.

> Try these searches:
> - **Keyword search**: keyword「行車記錄器」「車禍」＋ statute「刑法」「284」 — see citation-linked rankings
> - **RAG search**: 「如果我騎車，對方碰瓷，但沒有行車記錄器，該怎麼主張無過失？」 → 確認爭點、法條 → 生成 AI 分析

---

## Demo

<!-- TODO: Replace with actual screenshots/GIFs -->

| Keyword Search |
|---|
| ![keyword search](docs/screenshots/keyword-search.png) |

| RAG-based Search |
|---|
| ![rag search](docs/screenshots/rag-search.png) |

| Decision Detail |
|---|
| ![decision detail](docs/screenshots/decision-detail.png) |

---

## Features

- **Keyword search**
  - Statute autocomplete with law name validation, article number, and sub-reference input
  - Statute filtering and case type filtering
  - Citation expansion: finds sources containing query keywords, then surfaces the targets they cite — when multiple sources point to the same target, that signals a stable, authoritative holding
  - Results ranked by hit rate and citation count, with document type and court level filters
  - Click through to full decision detail

- **RAG-based search**
  - Input a factual description → Gemini extracts candidate issues and statutes → user selects which to confirm
  - AI-generated analysis with supporting decisions as references
  - Click through to full decision detail

- **Decision detail page**
  - Full decision text with citation snippet highlighting

---

## Architecture

<!-- TODO: Replace with actual architecture diagram -->
![Architecture](docs/screenshots/architecture.png)

| Layer | Technology |
|---|---|
| Frontend | React 19, Vite 7, Tailwind CSS 4 |
| Application | FastAPI |
| Keyword search | OpenSearch (2-gram ngram analyzer) |
| Semantic search | pgvector (HNSW / ivfflat) |
| Storage | PostgreSQL |
| AI services | Gemini Flash, Voyage API (voyage-law-2) |
| Infrastructure | AWS EC2, RDS, nginx |

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

Running two retrieval paths means maintaining two independent pipelines with different scoring logic, ranking strategies, and result presentation formats. Neither path's raw retrieval output can be presented directly to users — keyword results require citation-expansion ranking with authority signals, while RAG results must generate coherent, logically structured responses based on retrieved chunks and the user's confirmed issues and statutes.

**Decision:** I separated the two paths into distinct result experiences. The keyword result page presents source-target citation pairs ranked by hit rate and citation count, with filters for court level and document type. The RAG result page presents AI-generated analysis guided by confirmed issues and statutes, with supporting decisions as references.

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

## ER Diagram

<!-- TODO: Replace with actual ER diagram -->
![ER Diagram](docs/screenshots/er-diagram.png)

**Core tables:**

| Table | Rows | Description |
|---|---|---|
| `decisions` | 1.4M | Court decisions with normalized metadata |
| `citations` | 552K | Source → target citation links with snippet positions |
| `chunks` | 575K | Embedding chunks (citation-context + supreme reasoning) |
| `decision_reason_statutes` | 5.2M | Statute references extracted from decision reasoning |
| `citation_snippet_statutes` | 298K | Statute references within citation snippets |
| `authorities` | 1.6K | Authoritative decisions, resolutions, interpretations |

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
