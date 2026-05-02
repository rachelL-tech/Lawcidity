# Lawcidity

**PageRank for Taiwan court decisions.** A citation-based search engine that surfaces authoritative precedents and holdings. Supports keyword search (OpenSearch) and semantic search (RAG).

**🔗 [Demo](https://lawcidity.rachel-create.com/)**

> Try these searches:
> - **Keyword search**: keyword「殺人」(homicide)「無罪」(not guilty) ＋ statute「刑法」(Criminal Code)「271」
> - **RAG search**: 「如果我騎車，對方碰瓷，但沒有行車記錄器，該怎麼主張無過失？」
>   *(If I'm riding a scooter and the other party stages a collision, but I have no dashcam, how do I argue I wasn't at fault?)*

## TL;DR

- **Problem**: Traditional legal search has two blind spots:
  1. **Positional blindness** — full-text search checks whether a keyword appears *anywhere* in the document, but ignores *where*. In court decisions, different sections carry different weight (court's own reasoning > party's arguments). Full-text search treats them equally.
  2. **Lexical gap** — the same legal concept can be phrased in many ways. If the lawyer doesn't pick the exact keyword the court used, relevant decisions are missed.

- **Approach**:
  1. **Citation-based ranking** — use citations as importance signals: extract what each decision (source) cites (targets) and the surrounding text (snippets), then rank by citation frequency.
  2. **RAG-based semantic search** — vectorize queries and citation-anchored chunks, enabling retrieval by meaning rather than exact keyword match.

- **Stack**: FastAPI / PostgreSQL / OpenSearch / pgvector / Gemini / Voyage / React / AWS

## What You Get

**Keyword search**
- Input: keyword(s) + optional statute + case type
- Output: ranked list of precedents (targets) by citation count — expand any precedent to see how source decisions quote it (snippets), then drill into a source decision's full text.

**Semantic search (RAG)**
- Input: natural language description of a legal situation
- Output: Gemini-extracted issues and statutes → AI-generated analysis per issue, with decisions ranked by semantic similarity to citation-anchored chunks.

---

## How a Court Decision Becomes Data

![Mark Terms](frontend/public/mark_terms.png)

| Term | Meaning |
|---|---|
| **decision** | A court decision (判決 *judgment* / 裁定 *ruling*) — the node in the citation graph |
| **authority** | A non-decision legal source (司法院釋字 *Constitutional Interpretation*, 決議 *resolution*, etc.) — also a node, only appears as a target |
| **source** | A decision that cites another |
| **target** | A decision or authority being cited |
| **citation** | A source → target reference, with the surrounding text (snippet) |
| **statute** | A law article (e.g. 民法第184條 *Civil Code Art. 184*) referenced in the text |
| **chunk** | A text segment anchored to a citation position in a decision, used as the retrieval unit for semantic search |
| **embedding** | A vector representation of a chunk, enabling similarity-based retrieval |

### ETL pipeline

![Flowchart](frontend/public/flow_chart.png)

---

## Why Citations?

![Citation Concept](frontend/public/citation_concept.png)

Legal citations work like academic citations — and in theory, like **PageRank**: a decision cited by many others is likely authoritative — and citation count is a reasonable signal of that authority.

While working as a legal intern, I found that **snippets from different sources pointing to the same target are nearly identical.** Each high-citation target doesn't just happen to be popular — it establishes one or a few specific legal rules, and every source quotes it in the same way.

![](frontend/public/why_citations_snippet.png)

For example, running a keyword search for「車禍」(traffic accident) in Lawcidity: the most-cited target appears across dozens of sources, and every snippet discusses the definition of「突發狀況」(sudden circumstances). The second most-cited target's snippets all address「逃逸」(fleeing the scene). Each top target consistently maps to one or a few closely related legal questions — not vague thematic similarity, but the same holding cited in the same way.

**Citation count is a semantic signal, not just a popularity signal.** A highly-cited target under a specific keyword isn't just frequently mentioned — it's the established answer to the legal question that keyword raises.

- Full-text search finds decisions that *mention* the same words.
- Citation ranking finds decisions that *settle* the legal question.

The retrieval strategy follows: find all decisions matching the keyword or statute (**sources**), identify what they commonly cite (**targets**), rank by citation count. A target ranks higher when its snippets contain the search keyword — meaning the source cited it specifically while discussing that legal question, not for an unrelated reason.

The same citation structure also powers semantic search. Citation snippets concentrate the most legally relevant text in a decision — the court's own reasoning at the moment of citation. Using these snippets as anchors for text chunks means the embedded segments are inherently high-signal, making them better retrieval targets for vector search than arbitrary splits of the full document.

---

## Features

### Keyword Search

![Keyword Search](frontend/public/keyword_search_diagram.png)

(1) Enter keywords like「車禍」(traffic accident) or「行車紀錄器」(dashcam). You can also optionally add a statute using autocomplete (e.g.「刑法」*Criminal Code* +「284」) or filter by case type (e.g.「刑事」*criminal*).

![](frontend/public/gif/keyword-1-input.gif)

(2) Sort by relevance or citation count; filter by documentation type and court level.

![](frontend/public/gif/keyword-2-sort-filter.gif)

(3) Click a target to see matched and unmatched citation snippets, then drill into the full decision with jump-to-snippet.

![](frontend/public/gif/keyword-3-snippets-and-decisions.gif)

### RAG Search

![RAG Search](frontend/public/RAG_search_diagram.png)

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

### Data source
[Judicial Yuan Open Data Platform](https://opendata.judicial.gov.tw/) — public court decisions, 2025-01 to 2026-01.

For a sample raw decision file, see [data/PCDV,113,訴,2272,20250210,1.json](data/PCDV,113,訴,2272,20250210,1.json).

PostgreSQL: **17 GB** on RDS. OpenSearch: **3.2 GB** on EC2.

### PostgreSQL ER Diagram
![PostgreSQL ER Diagram](frontend/public/er-diagram.png)

**Core tables:**

| Table | Rows | Description |
|---|---|---|
| `decisions` | 1.4M | Source and target court decisions with normalized metadata |
| `citations` | 552K | Source → target citation relationships with full-text positions |
| `chunks` | 575K | Embedding chunks anchored to citation references |
| `decision_reason_statutes` | 6.6M | Statute references extracted from decision reasoning sections |
| `citation_snippet_statutes` | 458K | Statute references within citation snippets |
| `authorities` | 1.6K | Other cited legal documents that are not court decisions, stored separately from `decisions` |

### OpenSearch Indexes and Documents Shapes
![OpenSearch Index](frontend/public/opensearch_index_documents.png)

**Core indexes:**

| Index | Documents | Store size | Description |
|---|---:|---:|---|
| `decisions_v3` | 3.0M | 2.8 GB | Main decision index for keyword retrieval |
| `source_target_windows_v2` | 997K | 456 MB | Source-target pairs with citation snippets for ranking cited targets |

---

## Technical Decisions

### Citation parsing

![Raw JSON vs Parsed](frontend/public/raw_vs_parsed.png)

Court decisions are delivered as raw JSON — the full text is dense and unstructured, with case numbers appearing at unpredictable positions. The raw data must be cleaned and parsed before it can be stored in structured tables.

![True vs False](frontend/public/true_vs_false.png)

A case number in a court decision is not always a legal citation — it may appear as a reference to an attachment, procedural history, or a party's argument. The extraction regex must be broad enough to catch bare case numbers (which rarely come with explicit signals like「最高法院…判決意旨參照」), which inevitably pulls in false positives. These are difficult to distinguish even within the court's own reasoning: the same format may indicate evidence on file (「有該裁定在卷可參」), prior case history, or the court's actual citation. Left unfiltered, they propagate into the citation index and degrade retrieval quality downstream.

The parser addresses this by decomposing extraction and filtering into small, individually testable functions — the test suite covers 27+ edge cases from production data, including evidence reference filtering, procedural history detection, and party-section vs court-reasoning distinction.

### Keyword search: retrieval and ranking

![retrieval](frontend/public/retrieval.png)

**Why two stages.** The original pipeline did everything in PostgreSQL: an ILIKE scan over every decision's `clean_text` to recall sources, then another ILIKE scan over each recalled source's citation snippets to score targets. A broad query like「詐欺」(fraud) took ~73 seconds.

**How the retrieval pipeline works.** A query can include multiple keywords and statute references; each is treated as a separate clause (referred to as query clauses throughout). The pipeline has two OpenSearch stages:

- **Stage 1 — source recall.** Finds all decisions that contain the query clauses — these become the source set for Stage 2. The query and index share the same 2-gram ngram tokenization; `match_phrase` requires the resulting ngrams to appear contiguously, rather than scattered across the document. Returns a set of source IDs.
- **Stage 2 — target recall.** Within the Stage 1 source set, filters (source, target) pairs by scanning each citation snippet against the query clauses — a matching snippet means the source cited that target while discussing the relevant legal issue. Each target is tagged with the MSM level at which it first qualified (`reached_at_msm`), which becomes the primary criterion for downstream ranking.

*Stage 1 — moving source recall to OpenSearch.* 

Stage 1's job is straightforward: find every decision that mentions the search terms. PostgreSQL GIN was the natural first candidate, but OpenSearch was 27× faster on cited-decision retrieval with less than a third of the index size — so source recall moved to OpenSearch.

The remaining question was tokenization. OpenSearch's built-in IK segmenter was designed for modern Chinese text, but legal vocabulary is archaic and specialized — its segmentation was unpredictable on court decisions. The solution was 2-gram ngram + `match_phrase`: index every document as overlapping 2-character windows, and require those windows to appear in order. This gives phrase-level precision without relying on a domain-specific segmenter.

At this point, the backend received the Stage 1 source IDs and passed them to PostgreSQL, which ran an ILIKE scan over each source's citation snippets to check for keyword hits. Per-source match scores were aggregated per target — a target cited by more matching sources ranked higher. This worked while source counts were moderate, but broad queries that returned tens of thousands of sources made the per-source snippet scan the dominant cost.

*Stage 2 — moving snippet scoring to OpenSearch.* 

The fix was `source_target_windows_v2` — one document per (source, target) pair, pre-loaded with the citation snippet and statute list — so keyword and statute matching on snippets could move from PostgreSQL to OpenSearch. PostgreSQL was left responsible only for final metadata lookup.

For candidate target collection, Stage 2 uses a step-down MSM ladder: MSM (minimum_should_match) controls how many of the query's N clauses must appear in a citation snippet for a source-target pair to qualify. Starting at MSM=N, Stage 2 collects all qualifying targets, then steps down to N−1, N−2, and so on, stopping once 200 candidates are in the pool (e.g., a 3-clause query tries MSM=3 first, then MSM=2, then MSM=1). Each target is tagged with the highest MSM level at which it first qualified (`reached_at_msm`). A target that enters at MSM=N was cited by sources discussing the exact legal question the user raised; one that enters at a lower MSM appeared in a less directly relevant citation context. The tag preserves this signal for the ranking step.

![ranking](frontend/public/ranking.png)

**How the ranking works.** Targets are sorted first by `reached_at_msm` — a target with a clause-matching snippet at msm=N always ranks above one that first appeared at msm=N−1. Within the same level, `matched_citation_count` is the tie-breaker — the number of distinct Stage 1 sources that cite this target, computed from PostgreSQL.

**Reducing downstream latency.** After the initial search, reranking and citation expansion also had significant latency:

*Rerank cache.* The first version only cached the Stage 1 source IDs (all source IDs matching the search query). Every rerank request — filtering by document type, switching sort order (relevance, citation count) — still re-ran the full Stage 2 pipeline. The fix was caching the complete target rankings alongside source IDs after the first search, so subsequent reranks become in-memory filter/sort/paginate operations.

*Citations: dropping global score ranking.* Cut citation expansion from ~3s to ~0.8s.

Previously, every citation across all matched sources was scored to determine display order. The fix: Stage 2 already collects up to 5 representative source IDs per target — citation preview reuses them directly to pick one representative citation per source, with no global scoring needed.

*SQL-level optimizations.* Additional improvements included query shape changes (DISTINCT ON restructuring, denormalization, index upgrades) across citation preview and rerank queries.

| Operation | Before | After |
|---|---|---|
| Keyword search (「詐欺」*fraud*) | ~73s | 2–4s |
| Rerank | ~1.27s | ~0.04s (cache hit: <1ms) |
| Citation expansion | 13–16s | ~0.8–1.0s |

### RAG search: retrieval and generation

**User flow.** The user describes a legal situation in natural language. Gemini extracts candidate legal issues and statutes, the user confirms which to keep, and the confirmed inputs drive both retrieval and AI-generated analysis.

**Retrieval.** The user's query is embedded via Voyage API (voyage-law-2), then searched against pgvector using IVFFlat approximate nearest-neighbor search, returning the top 50 chunks ranked by cosine similarity. Results are aggregated to the decision level — each decision's score is determined by its best-matching chunk.

**Chunk design.** Each chunk is anchored to a citation reference — not an arbitrary text split. This ensures the embedded text is the court's own reasoning at the moment of citation, which is inherently high-signal for retrieval.

- **Center**: the citation position in the decision
- **Boundaries**: expand outward to the nearest section markers (㈠㈡㈢, ⒈⒉⒊, 一二三、, etc.), falling back to sentence endings (。) if the span exceeds 2,000 characters
- **Hard limits**: cannot extend before the reasoning section header or past the closing dateline
- **Overlap**: adjacent citation chunks are merged; identical chunks are deduplicated via md5 hashing

![Chunk Design](frontend/public/chunk_design.png)

**Embedding selection.** Evaluated across three rounds covering BAAI bge-m3, Qwen3-Embedding (0.6B / 4B), Gemini embedding, voyage-multilingual-2, voyage-law-2, and voyage-4-large. Each round used the same test set: 6 target decisions (civil, criminal, administrative, IP), with related citation snippets and 20 unrelated snippets as negatives. The two metrics were `avg gap` (mean score of related snippets minus mean score of unrelated snippets — measures ranking stability) and `Recall@5` (fraction of related snippets appearing in the top 5 results).

| Model | avg gap | min gap | Recall@5 |
|---|---:|---:|---:|
| bge-m3 | 0.212 | 0.080 | 0.826 |
| Qwen3-Embedding-0.6B (512d) | 0.341 | 0.177 | 0.938 |
| voyage-multilingual-2 | 0.386 | 0.287 | 0.938 |
| voyage-4-large | 0.351 | 0.230 | 0.938 |
| **voyage-law-2** | **0.404** | **0.241** | 0.882 |

voyage-law-2 led on avg gap (+18% over Qwen3, +15% over voyage-4-large) and won on all 6 targets individually. Its Recall@5 is slightly lower (0.882 vs 0.938), but the larger gap means its ranking is more stable — related snippets score further above unrelated ones even when not all land in the top 5. voyage-law-2 was selected for its legal-domain specialization and superior score separation.

---

## Development Journey

Nine weeks of iterative development, from raw court documents to a working search product.

| Phase | Period | Key work |
|---|---|---|
| **1. Parsing & normalization** | Feb 12–24 | Citation parser (state machine), statute extraction, false positive filtering, schema v1→v4 |
| **2. Keyword search** | Feb 25 – Mar 3 | OpenSearch vs PostgreSQL GIN benchmark, IK → 2-gram ngram migration, citation expansion scoring |
| **3. API & frontend** | Mar 5–13 | REST API with SQL aggregation, React search UI with filters, Docker + EC2 deployment |
| **4. Parser refactor** | Mar 14–21 | Rewrote citation parser into traceable functions, tightened false positive rules, sibling citation dedup |
| **5. Semantic search & RAG** | Mar 22–27 | Multi-round embedding evaluation, citation-context chunks, semantic retrieval, Gemini AI analysis |
| **6. Optimization & deploy** | Mar 26–30 | Chunk dedup, HTTPS production deployment |
| **7. Search & retrieval optimization** | Apr 7–19 | Source-target window index, step-down msm ladder, single ranking path across query shapes, rerank cache, citation preview optimization |

---

## Future Work

- Redesign chunk boundaries — explore semantic chunking or LLM-based chunking to better separate factual context from legal reasoning
- Validate whether rewriting queries into more precise legal language improves retrieval recall
