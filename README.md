# Lawcidity

A citation-based legal search engine that helps lawyers find authoritative court holdings faster, with an AI-assisted flow that clarifies legal intent before analysis.

**🔗 [Live Demo](https://lawcidity.rachel-create.com/)**

> Try these searches:
> - **Keyword search**: keyword「行車紀錄器」「車禍」＋ statute「刑法」「284」
> - **RAG search**: 「如果我騎車，對方碰瓷，但沒有行車記錄器，該怎麼主張無過失？」

## Why Citations?

During a legal internship, I noticed that similar cases consistently cite the same precedents as their legal basis. My hypothesis: **the more a decision is cited, the more likely it represents a stable, authoritative holding** — because no matter how relevant a legal opinion seems, if it's a minority view, judges won't adopt it.

I extracted and counted citation relationships from court decisions, and confirmed the pattern: under specific keyword and statute combinations, certain targets are cited far more frequently than others. This became the core retrieval strategy — find all decisions containing a keyword or statute (sources), identify the targets they commonly cite, and rank by citation count.

---

## Features

### Keyword Search

(1) Enter keywords like "車禍" or "行車紀錄器". You can also optionally add a statute using autocomplete (e.g. "刑法" + "284") or filter by case type (e.g. "刑事").

![](frontend/public/gif/keyword-1-input.gif)

(2) Sort by relevance or citation count; filter by documentation type and court level.

![](frontend/public/gif/keyword-2-sort-filter.gif)

(3) Click a target to see matched and unmatched citation snippets, then drill into the full decision with jump-to-snippet.

![](frontend/public/gif/keyword-3-snippets-and-decisions.gif)

### RAG Search

(1) Describe a case in natural language → Gemini extracts legal issues and statutes → confirm before submitting.

![](frontend/public/gif/rag-1-analyze.gif)

(2) Browse Gemini-generated analysis per issue with supporting decisions; click a source (orange) to open the full decision or a target (gray) to see citation counts.

![](frontend/public/gif/rag-2-analysis-page.gif)

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

---

## Data Model

**Data source:** [Judicial Yuan Open Data Platform](https://opendata.judicial.gov.tw/) — public court decisions, 2025-01 to 2026-01.

PostgreSQL: **17 GB** on RDS. OpenSearch: **3.2 GB** on EC2.

### PostgreSQL ER Diagram
![PostgreSQL ER Diagram](frontend/public/er-diagram-overview.png)
For a detailed version, see [er-diagram-detail.png](frontend/public/er-diagram-detail.png).

**Core tables:**

| Table | Rows | Description |
|---|---|---|
| `decisions` | 1.4M | Source and target court decisions with normalized metadata |
| `citations` | 552K | Source → target citation relationships with full-text positions |
| `chunks` | 575K | Embedding chunks (citation-context + supreme reasoning) |
| `decision_reason_statutes` | 6.6M | Statute references extracted from decision reasoning sections |
| `citation_snippet_statutes` | 458K | Statute references within citation snippets |
| `authorities` | 1.6K | Other cited legal documents that are not court decisions, stored separately from `decisions` |

### PostgreSQL-to-OpenSearch Index Flow
![PostgreSQL-to-OpenSearch Index Flow](frontend/public/index-flow.png)

**Core indexes:**

| Index | Documents | Store size | Description |
|---|---:|---:|---|
| `decisions_v3` | 3.0M | 2.8 GB | Main decision index for keyword retrieval |
| `source_target_windows_v2` | 997K | 456 MB | Source-target pairs with citation snippets for ranking cited targets |

---

## Technical Decisions

### Citation parsing

A case number in a court decision is not always a legal citation — it may refer to an attachment, a procedural history, or a summary of another ruling. Citations do not always appear with clear signals like「最高法院…判決意旨參照」; often only a bare case number is mentioned, so the extraction regex must be broad enough to catch them — which inevitably pulls in many false positives.

**Why this is hard:** There is no universal rule for identifying false positives. Even within the court's own reasoning section, a case number may appear as evidence on file (「有該裁定在卷可參」), as prior case history (「判決上訴駁回確定」), or as a party's argument being summarized rather than the court's own citation. If these false positives are not filtered early, they propagate into the citation index and degrade both retrieval quality and query speed downstream.

<!-- placeholder: 截圖 — raw decision JSON 片段，
     標出一個 true citation 和一個 false positive，
     展示兩者在文本中看起來幾乎一樣 -->

**Decision:** The citation parser was decomposed into small, individually testable functions. Each extraction and filtering rule was validated against real failure cases — the test suite currently covers 27+ edge cases from production data, including evidence reference filtering, procedural history detection, and party-section vs court-reasoning distinction.

### Keyword search: retrieval and ranking

**Starting point.** The original pipeline ran a PostgreSQL ILIKE scan over the full clean_text of every decision to recall sources, then scored each target by scanning every citation row's snippet with PG ILIKE. A broad query like「詐欺」took ~73 seconds.

**Moving source recall to OpenSearch.** PostgreSQL GIN was the natural first candidate, but benchmarking showed OpenSearch was 27× faster on cited-decision retrieval with less than a third of the index storage. Tokenization was another early decision: IK segmentation was not predictable enough on legal vocabulary, so I adopted a 2-gram ngram + match_phrase strategy instead.

At this point, source recall (Stage 1) was fast, but the bottleneck shifted: scoring still happened in PostgreSQL, running ILIKE against every citation snippet for every recalled source. When source count was high, the number of (source, target) pairs exploded, and the per-row ILIKE scan became the dominant cost.

**Building a source-target window index.** Since OpenSearch was already fast at keyword hit detection, I built a new `source_target_windows` index that stored each (source, target) pair with the citation snippet text. This added a second stage to the pipeline: after Stage 1 source recall, Stage 2 scans the source-target index in OpenSearch to flag keyword and statute hits per pair, Python aggregates the flags into target scores, and PostgreSQL only provides metadata. The per-row PG ILIKE scan was eliminated. Stage 2 scrolls pairs up to a cap of 40,000 hits to bound query time.

<!-- placeholder: 前後架構對比圖，或一張表格比較
     舊 pipeline (OpenSearch recall → PG ILIKE score) vs
     新 pipeline (Stage 1 OpenSearch recall → Stage 2 OpenSearch hit flag → Python score → PG metadata) -->

**Optimizing ranking quality and speed.** With the two-stage pipeline in place, new problems emerged at scale:

*Noisy second-stage hits.* Initially, the Stage 2 query set no `minimum_should_match` — any (source, target) pair that matched the source ID filter was returned, even if it never hit any keyword or statute window. For「損害賠償」, 83% of the 50,000 hits scored zero. Setting `minimum_should_match=1` fixed the noise, but strict matching produced zero results for rare terms where citation snippets did not contain the search keyword. The solution was a strict-then-fallback blend: run strict first, and if results are insufficient, supplement with a relaxed pass.

*Single-keyword aggregation path.* For single-keyword queries with no statute filter, pair-level scores are all 1.0 — every matching pair hits the keyword once, so scoring individual pairs is meaningless. The pipeline instead uses a target-level aggregation in OpenSearch: count how many distinct sources cite each target, then rank by matched citation count, total citation count, and court level. This is also more accurate than the pair-scroll path, since the composite aggregation pages through all pairs without the 40K cap.

*Streaming aggregation.* The original second-stage path collected all hits into a Python list, sorted them, then aggregated into targets. Replacing it with a streaming approach — aggregating into target state while scrolling — cut both latency and peak memory without changing results.

**Reducing downstream latency.** After the initial search, reranking and citation expansion also had significant latency:

*Rerank cache.* The first version only cached the Stage 1 source IDs (all source IDs matching the search query). Every rerank request — filtering by document type, switching sort order (relevance, citation count) — still re-ran the full Stage 2 pipeline. The fix was caching the complete target rankings alongside source IDs after the first search, so subsequent reranks become in-memory filter/sort/paginate operations.

*Citations: dropping global score ranking.* Citation preview originally required scoring every citation across all matched sources to determine display order — this was the single largest latency cost in citation expansion. Since the search stage already produces the top 5 ranked source IDs per target (selected from aggregated pair scores), the citation preview can reuse them directly instead of recomputing. For each of the 5 sources, it picks one representative citation by keyword and statute hit flags — no global scoring needed. This dropped citation expansion from ~3 seconds to ~0.8 seconds.

*SQL-level optimizations.* Additional improvements included query shape changes (DISTINCT ON restructuring, denormalization, index upgrades) across citation preview and rerank queries.

<!-- placeholder: benchmark 表格 -->

| Operation | Before | After |
|---|---|---|
| Keyword search (「詐欺」) | ~73s | 2–4s |
| Rerank | ~1.27s | ~0.04s (cache hit: <1ms) |
| Citation expansion | 13–16s | ~0.8–1.0s |

### RAG search: retrieval and generation

**User flow.** The user describes a legal situation in natural language. Gemini extracts candidate legal issues and statutes, the user confirms which to keep, and the confirmed inputs drive both retrieval and AI-generated analysis.

**Dual-path retrieval.** A single semantic search misses results that are topically relevant but not close enough in embedding space — especially when the user has specific statutes in mind. To address this, retrieval runs two paths:

- **Path A (semantic):** pgvector ivfflat approximate nearest-neighbor search for the top 50 chunks most similar to the user's query embedding.
- **Path B (statute-guided):** finds chunks whose associated citations or decisions reference the user's confirmed statutes, then computes vector distances for all matching chunks without filtering any out.

The two paths are merged by chunk ID. Each chunk is scored as cosine similarity plus a statute-match boost.

<!-- placeholder: scoring 公式截圖 -->

**Chunk design.** Chunks are not arbitrary text splits — they are structured around two types of high-value legal text:

- **Citation-context chunks:** text surrounding a citation reference in lower-court decisions. Each chunk is anchored to a specific citation and carries a link to the cited target. Boundaries are defined by the citation's snippet position.
- **Supreme-reasoning chunks:** complete reasoning sections from Supreme Court decisions. These chunks have no specific citation target — they represent the court's own legal reasoning. Boundaries follow section markers in the decision text, with a maximum chunk length of 2,000 characters.

Text-level deduplication (via md5 hashing) ensures identical chunks are only embedded once.

**Embedding selection.** Evaluated across multiple rounds: BAAI bge-m3, Qwen3-Embedding (0.6B / 4B), Gemini embedding, voyage-multilingual-2, voyage-law-2, voyage-4-large. The evaluation measured both Recall@5 and score gap between related and unrelated snippets, so the decision reflected retrieval quality and ranking stability. voyage-law-2 was selected for its legal-domain specialization and superior score separation.

<!-- placeholder: embedding 評估比較表（Recall@5 + score gap） -->

**Path B SQL optimization.** Path B was initially the dominant latency source within RAG retrieval:

| Chunk type | Before | After |
|---|---|---|
| citation_context | ~4,700ms | ~20ms |
| supreme_reasoning | ~2,100ms | ~50ms |

The key optimizations: for citation_context, restructuring the query to let statute filtering happen before the vector scan (preventing the planner from going through the vector index first and returning empty). For supreme_reasoning, switching from a large decision_id universe join to an EXISTS-first approach since the chunk universe is much smaller. Additional SQL-level changes included CTE restructuring and selectivity-aware join ordering.

After optimization, SQL is no longer the bottleneck in the RAG pipeline — the dominant costs are now Gemini generation and Voyage embedding.

---

## Development Journey

Nine weeks of iterative development, from raw court documents to a working search product.

| Phase | Period | Key work |
|---|---|---|
| **1. Parsing & normalization** | Feb 12–24 | Citation parser (state machine), statute extraction, false positive filtering, schema v1→v4 |
| **2. Keyword search** | Feb 25 – Mar 3 | OpenSearch vs PostgreSQL GIN benchmark, IK → 2-gram ngram migration, citation expansion scoring |
| **3. API & frontend** | Mar 5–13 | REST API with SQL aggregation, React search UI with filters, Docker + EC2 deployment |
| **4. Parser refactor** | Mar 14–21 | Rewrote citation parser into traceable functions, tightened false positive rules, sibling citation dedup |
| **5. Semantic search & RAG** | Mar 22–27 | Multi-round embedding evaluation, citation-context + supreme reasoning chunks, dual-path retrieval, Gemini AI analysis |
| **6. Optimization & deploy** | Mar 26–30 | RAG merge layer optimization, chunk dedup, issue-based embedding search, HTTPS production deployment |
| **7. Search & retrieval optimization** | Apr 7–12 | Source-target window scoring, strict/fallback ranking, hot-term target aggregation, streaming aggregation, rerank cache, citation preview optimization, Path B SQL optimization |

---

## Future Work

- Redesign chunk boundaries — explore semantic chunking or LLM-based chunking to better separate factual context from legal reasoning
- Validate whether rewriting queries into more precise legal language improves retrieval recall
