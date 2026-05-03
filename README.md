# Lawcidity

[繁體中文](README.zh-TW.md)

**A Taiwan court decision retrieval system built around citation relationships.**

> From keywords and citation relationships to semantic understanding, Lawcidity helps users find the court views that truly carry precedential value.

**[Demo](https://lawcidity.rachel-create.com/)**

> Try these searches:
> - **Keyword search**: keywords `殺人` (`homicide`), `無罪` (`not guilty`) + statute `刑法` (`Criminal Code`), `271`
> - **RAG search**: `如果我騎機車，對方碰瓷，但我沒有行車記錄器，該怎麼主張自己無過失？` ("If I was riding a scooter, the other party staged an accident, and I had no dashcam, how can I argue that I was not at fault?")

---

## Project Snapshot

| Item | Details |
|---|---|
| **Core idea** | Use **citation relationships** between decisions as the ranking signal, inspired by the core intuition behind PageRank, to surface legal views that courts repeatedly rely on |
| **Search modes** | Keyword search (OpenSearch) + semantic search (RAG) |
| **Data scale** | `1.4M` decisions, `552K` citations, `575K` chunks |
| **Technical focus** | citation parsing, citation-based ranking, citation-anchored chunking |
| **Performance result** | Keyword search improved from about `73s` to `2-4s` |
| **Tech stack** | FastAPI / PostgreSQL / OpenSearch / pgvector / Gemini / Voyage / React / AWS |

---

## What Problem Is This Project Solving?

Traditional legal search has two recurring blind spots:

### 1. Ignoring positional signal
Full-text search only checks whether a keyword appears in a document, not where it appears.

But not every part of a court decision carries the same weight:
- the court's own legal reasoning
- the parties' arguments
- procedural background
- factual or evidentiary descriptions

Full-text search only checks whether the keyword appears, without considering the context in which it appears. In legal research, the same keyword hit is usually more valuable when it appears in the court's own reasoning.

### 2. Vocabulary mismatch
The same legal concept is often expressed in different ways. For example:
- `詐欺` (`fraud`) vs `詐騙` (`scam`)
- `資遣` (`layoff`) vs `終止勞動契約` (`termination of an employment contract`)

If the user does not happen to use the same wording preferred by courts, a traditional keyword search can easily miss relevant decisions.

---

## How Lawcidity Approaches It

| Mode | How it works | What it solves |
|---|---|---|
| **Keyword search** | First recall source decisions from full text, then use each source's citation snippets to find the targets they collectively point to, and rank those targets by citation strength | It does not just find decisions that mention the same words; it finds the decisions courts most often rely on when discussing that legal issue |
| **Semantic search (RAG)** | Vectorize the user's query and citation-anchored chunks, then retrieve by semantic similarity | Reduces dependence on exact keyword overlap and instead finds similar cases through legal meaning |

---

## Project Highlights

- **Citation relationships are the core ranking signal.**  
  The system moves beyond "which documents mention this term" to "which decisions courts actually use to resolve this legal issue."

- **It focuses on high-signal legal text.**  
  By anchoring on citation positions, it targets the places where courts actually enter legal reasoning, which improves recall quality and semantic retrieval quality.

- **It works on a real, large-scale court dataset.**  
  The project handles public court decisions at scale, including citation parsing, false-positive filtering, OpenSearch index design, and performance optimization.

- **The performance gains are concrete.**  
  Keyword search dropped from about `73 seconds` to `2-4 seconds`, and reranking can fall below `1 ms` on cache hits.

---

## Feature Demo

### Keyword Search

![Keyword Search](frontend/public/keyword_search_diagram.png)

**What you can do:**
- search with keywords such as `車禍` (`traffic accident`) and `行車紀錄器` (`dashcam`)
- add statute filters such as `刑法` (`Criminal Code`) + `284`
- further filter by case type, court level, and document type
- inspect the context in which a target is cited by different sources
- open the full source decision directly

![](frontend/public/gif/keyword-1-input.gif)

![](frontend/public/gif/keyword-2-sort-filter.gif)

![](frontend/public/gif/keyword-3-snippets-and-decisions.gif)

### RAG Search

![RAG Search](frontend/public/RAG_search_diagram.png)

**What you can do:**
- describe the facts of a case in natural language
- let Gemini extract candidate legal issues and statutes
- confirm them and get issue-by-issue analysis grounded in court decisions

![](frontend/public/gif/rag-1-analyze.gif)

![](frontend/public/gif/rag-2-analysis-page.gif)

---

## Why Rank by Citation Relationships?

![Citation Concept](frontend/public/citation_concept.png)

Legal citations work a lot like academic citations. Structurally, they are also close to the intuition behind **PageRank**:

> If a decision is cited frequently by other decisions, it usually carries real weight in practice.

During my internship at a law firm, I noticed that citation snippets from different **source decisions** pointing to the same **target decision** often contain highly similar language. That suggests a highly cited target is not merely being mentioned by chance. Rather, it has articulated one or more concrete legal positions, and different courts quote it in similar ways when handling similar issues.

For example, when searching for `車禍` (`traffic accident`):
- the most-cited target is repeatedly cited in snippets about "sudden situations"
- the second most-cited target is cited in snippets centered on "fleeing the scene"

This shows that courts repeatedly rely on the same legal view when dealing with the same legal issue.

![](frontend/public/why_citations_snippet.png)

**So citation count reflects more than popularity. It also reflects the stable line of case law courts have formed around a particular issue.**

- Full-text search asks: which decisions **mention** the same term?
- Citation ranking asks: which decisions are actually used by courts to resolve legal disputes related to that term?

---

## Terminology and Data Units

![Mark Terms](frontend/public/mark_terms.png)

In practice, the system first uses case numbers mentioned in a decision to locate citation candidates, then uses surrounding context to determine whether they are true citations.

| Term | Meaning |
|---|---|
| **decision** | A court decision, including both judgments and rulings; acts as a node in the citation graph |
| **authority** | A non-decision legal authority, such as Constitutional Court interpretations or judicial resolutions; also treated as a node |
| **source** | A decision that cites another decision or legal authority |
| **target** | The decision or legal authority being cited |
| **citation** | One citation record in which a source cites a target |
| **citation snippet** | The local legal reasoning text around a citation, used to show the concrete context in which a target is cited |
| **statute** | A statute mentioned in the full decision text or in a citation snippet, such as Civil Code Article 184 |
| **chunk** | A text segment cut around a citation position and used as the retrieval unit for semantic search |
| **embedding** | The vector representation of a chunk, used for semantic similarity search |

---

## Search Performance and Optimization Results

| Operation | Before | After |
|---|---|---|
| Keyword search (`詐欺` / `fraud`) | ~73s | 2-4s |
| Reranking | ~1.27s | ~0.04s (`<1 ms` on cache hit) |
| Citation expansion | 13-16s | ~0.8-1.0s |

---

## System Architecture

![Architecture](frontend/public/Architecture.png)

| Layer | Technology |
|---|---|
| Frontend | React 19, Tailwind CSS 4 |
| Backend | FastAPI |
| Keyword search | OpenSearch (2-gram ngram analyzer) |
| Semantic search | pgvector (ivfflat) |
| Database | PostgreSQL |
| AI services | Gemini Flash, Voyage API (`voyage-law-2`) |
| Deployment | AWS EC2, RDS, ALB, nginx |

---

## Data Source and Data Model

### Data Source
[Judicial Yuan Open Data Platform](https://opendata.judicial.gov.tw/)  
Contains public Taiwan court decisions from January 2025 through January 2026.

Raw decision JSON example (original filename preserved):  
[data/PCDV,113,訴,2272,20250210,1.json](data/PCDV,113,訴,2272,20250210,1.json)

### Data Size

PostgreSQL: **17 GB** (RDS)  
OpenSearch: **3.2 GB** (EC2)

### ETL Flow

![Flowchart](frontend/public/flow_chart.png)

### PostgreSQL ER Diagram

![PostgreSQL ER Diagram](frontend/public/er-diagram.png)

### Core Tables

| Table | Rows | Description |
|---|---|---|
| `decisions` | 1.4M | Normalized court decision records, including both source and target decisions |
| `citations` | 552K | Citation records from source to target, including citation snippets and citation positions in the full text |
| `chunks` | 575K | Text segments anchored on citation positions, with embeddings, used for semantic search |
| `decision_reason_statutes` | 6.6M | Statute references extracted from full decision texts |
| `citation_snippet_statutes` | 458K | Statute references extracted from citation snippets |
| `authorities` | 1.6K | Non-decision legal authorities such as Constitutional Court interpretations and judicial resolutions |

### OpenSearch Indexes and Document Structure

![OpenSearch Index](frontend/public/opensearch_index_documents.png)

| Index | Documents | Size | Description |
|---|---:|---:|---|
| `decisions_v3` | 3.0M | 2.8 GB | Full-text keyword index used to first recall matching source IDs |
| `source_target_windows_v2` | 997K | 456 MB | Source-target pair documents with citation snippets, used to identify highly relevant citation snippets among recalled sources and then recover the targets they collectively point to |

---

## Key Technical Decisions

### 1. Citation Parsing

![Raw JSON vs Parsed](frontend/public/raw_vs_parsed.png)

**Cleaning and parsing**  
Court decisions from the Judicial Yuan come as raw JSON. The full text is inconsistently formatted and mixed with whitespace and unstructured content, so it cannot be queried directly.

![True vs False](frontend/public/true_vs_false.png)

**The real difficulty**  
Case numbers appearing in a decision do not automatically represent legal citations. They may instead refer to:
- evidentiary references
- procedural history
- historical case records
- case numbers mentioned in the parties' arguments

Only when the court cites a prior decision as part of its own reasoning should that case number count as a true citation.

For example, all of the following strings may be captured as citation candidates, but only some are true legal citations:

- `按最高法院 112 年度台上字第 1234 號判決意旨……` ("According to the holding of Supreme Court Decision 112-Tai-Shang-1234 ...")
- `本件前經最高法院 112 年度台上字第 1234 號判決發回更審` ("This case was previously remanded by Supreme Court Decision 112-Tai-Shang-1234")
- `有臺灣高等法院 111 年度上字第 567 號裁定在卷可參` ("The Taiwan High Court Ruling 111-Shang-567 is in the record for reference")

All three contain case numbers, but only the first is the court relying on an existing legal view. The latter two are merely procedural history or references to materials in the record.

**Approach**
- add contextual filtering rules
- separate extraction and filtering logic into smaller functions so each part can be tested and adjusted independently

At a high level, the workflow is:

```text
1. Use a permissive regex to extract possible case-number candidates from the decision text
2. Inspect the surrounding context and section position of each candidate
3. Use that context to filter out procedural history, evidentiary references, party arguments, and other non-citation usages
4. Treat the remaining candidates as citations
5. Cut a legal reasoning snippet around each accepted citation
```

**Result**  
The current pytest cases cover more than 27 edge cases drawn from real decisions, including:
- filtering exhibits in the record
- detecting procedural-history references
- distinguishing party-argument sections from the court's own reasoning sections

---

### 2. Keyword Search: Retrieval and Ranking

![retrieval](frontend/public/retrieval.png)

### Why split it into two stages?

The earliest pipeline ran entirely in PostgreSQL:
1. scan each decision's `clean_text` with `ILIKE` to recall sources
2. scan each recalled source's `citation snippets`
3. compute a snippet match score for each target
4. rank targets by the total score

That works at smaller scale, but with broad queries such as `詐欺` (`fraud`), the first stage recalls a very large number of sources, and snippet scanning becomes the main bottleneck.

### Stage 1: Source recall

**Approach**  
First find the source IDs whose full text matches the search conditions.

**Why not PostgreSQL GIN?**  
In measurement, OpenSearch source recall was:
- about **27 times faster**
- less than **one third** of the index size

**Chinese retrieval strategy**  
The commonly used IK tokenizer in OpenSearch is mainly designed for simplified Chinese. Court decisions, however, contain a large amount of specialized legal vocabulary that is not covered by the tokenizer's dictionary, which made tokenization unstable.

The final choice was:

**2-gram ngram + `match_phrase`**

That means:
- each document is split into overlapping 2-character pieces
- `match_phrase` requires those pieces to appear contiguously and in order

This keeps keywords from being scattered across unrelated parts of a document while preserving near phrase-level precision.

### Stage 2: Target recall

**Approach**  
Within the set of sources recalled by Stage 1, inspect their citation snippets, keep the snippets that match the query conditions, and then count which targets those snippets collectively point to.

**Why move this stage to OpenSearch too?**  
In the earlier version, once Stage 1 returned source IDs, PostgreSQL had to scan the citation snippets one source at a time. Once the recalled source count reached the tens of thousands, performance also degraded sharply.

To solve this, I built the `source_target_windows_v2` index:

- each document represents one `(source, target)` pair
- all citation snippets and statute data for that pair are pre-stored together
- keyword and statute matching on citation snippets can therefore be handled directly inside OpenSearch

PostgreSQL is left with only the final metadata lookup and aggregation.

*MSM ladder: layered collection of matched snippets*

Target collection in Stage 2 uses a step-down MSM ladder.

MSM (`minimum_should_match`) controls how many query clauses a source-target pair must match in its citation snippets before it qualifies.

A query clause can be:
- a keyword, such as `過失` (`negligence`) or `車禍` (`traffic accident`)
- a statute condition, such as Criminal Code Article 284 or Civil Code Article 185

For example, if a query contains 3 clauses, the system tries:

1. MSM = 3
2. MSM = 2
3. MSM = 1

The pipeline starts at the strictest level, MSM = N, and progressively relaxes the condition until the candidate pool reaches 200 targets.

Each target records the MSM level at which it first entered the pool, stored as `reached_at_msm`.

This means:
- if a target is recalled at the highest MSM level, it is more likely to match the user's search conditions precisely
- if it only appears at a lower MSM level, its citation context is more loosely related to the query

### How are targets ranked?

![ranking](frontend/public/ranking.png)

Target ranking is mainly based on two signals:

1. **`reached_at_msm`**  
   Targets first recalled at a higher MSM level are ranked first.

2. **`matched_citation_count`**  
   Within the same MSM level, targets are secondarily ranked by the number of distinct source decisions that point to them.

In other words, Lawcidity is not just finding decisions that mention the searched keywords or statutes. It is asking:

> Within court reasoning that is relevant to the user's search conditions, which targets are repeatedly relied on by the largest number of courts?

### How is follow-up interaction latency reduced?

**Ranking cache**  
The first version cached only the Stage 1 source IDs. That meant users had to rerun Stage 2 whenever they:
- changed the sort order
- moved to another page
- added filters

The later version caches the full target ranking order after the initial search, so subsequent interactions can be handled in memory.

**Faster citation expansion**  
The earlier version rescored every matching snippet from every source in order to decide snippet order. The later version has OpenSearch return 5 representative source IDs during Stage 2, and PostgreSQL only scans those 5 sources to pick a representative citation.

This reduced citation expansion time from about `3 seconds` to about `0.8 seconds`.

**SQL-level optimizations**  
Other changes included:
- refactoring `DISTINCT ON`
- denormalization
- adding indexes for high-frequency query columns
- improving the query shapes for citation preview and reranking

---

### 3. RAG Search: Retrieval and Generation

### RAG Flow
Users begin by describing their legal problem in natural language. Gemini first extracts candidate legal issues and relevant statutes. After the user confirms them, the system proceeds through the rest of the RAG pipeline:

- **Query understanding**  
  Structure the user's input into explicit legal issues and statute conditions, which then serve as structured input for generated analysis.

- **R — Retrieval**  
  Convert the user's query into an embedding, retrieve the most semantically similar citation-anchored chunks from pgvector, and aggregate them at the decision level.

- **A — Augmentation**  
  Package the retrieved chunks, source decision metadata, and related target references into the prompt as context for downstream analysis.

- **G — Generation**  
  Gemini generates issue-by-issue analysis grounded in real court decisions returned by retrieval.

### Retrieval
- convert the query into an embedding through the Voyage API (`voyage-law-2`)
- run approximate search through PostgreSQL / pgvector with an IVFFlat index
- return the top 50 most similar chunks by cosine similarity
- aggregate the results to the decision level, using the highest-scoring chunk to represent the decision's score

### Chunk Design

Each chunk is anchored on a citation position in the decision, rather than cut randomly from the full text. That ensures the text sent for embedding comes from places where the court is entering substantive legal reasoning and where the signal is strongest.

- **center point**: the citation position in the decision
- **boundaries**: expand outward from the citation position to the nearest structural markers such as `㈠㈡㈢`, `⒈⒉⒊`, or `一二三` (Chinese numeral headings)
- **overlength handling**: if the span exceeds 2,000 characters, fall back to sentence boundaries marked by `。` (the Chinese full stop)
- **hard limits**: do not extend before the heading of the reasoning section, and do not extend past the date line at the end of the document
- **overlap handling**: if neighboring citation-based chunks overlap, merge them; if two chunks are identical, deduplicate them with MD5 to avoid redundant embedding work

![Chunk Design](frontend/public/chunk_design.png)

### Embedding Model Selection

I ran three rounds of embedding model evaluation covering:

- `BAAI bge-m3`
- `Qwen3-Embedding (0.6B / 4B)`
- `Gemini embedding`
- `voyage-multilingual-2`
- `voyage-law-2`
- `voyage-4-large`

Each round used the same evaluation set:
- 6 target decisions covering civil, criminal, administrative, and IP cases
- the citation snippets that originally pointed to each target as the positive examples
- 20 unrelated snippets added as negative examples

### Evaluation Metrics

- **`avg gap`**: the average score of relevant snippets minus the average score of irrelevant snippets, used to measure how consistently the model separates the two groups
- **`Recall@5`**: the proportion of relevant snippets that appear in the top 5 results, used to measure how well the model ranks relevant snippets near the top

| Model | avg gap | min gap | Recall@5 |
|---|---:|---:|---:|
| bge-m3 | 0.212 | 0.080 | 0.826 |
| Qwen3-Embedding-0.6B (512d) | 0.341 | 0.177 | 0.938 |
| voyage-multilingual-2 | 0.386 | 0.287 | 0.938 |
| voyage-4-large | 0.351 | 0.230 | 0.938 |
| **voyage-law-2** | **0.404** | **0.241** | 0.882 |

**Final choice: `voyage-law-2`**

The main reason is that it performed best on **avg gap**, which means it was the most consistent at separating relevant from irrelevant snippets.

- compared with `Qwen3-Embedding-0.6B`, its avg gap was about **18% higher**
- compared with `voyage-4-large`, its avg gap was about **15% higher**

Although its `Recall@5` is slightly lower than some other models, it creates a clearer score gap between relevant and irrelevant snippets, which makes it less likely that irrelevant passages will creep into the highest-scoring results.

---

## Development Journey

Seven weeks of iterative development, starting from raw Judicial Yuan JSON and gradually turning it into a usable search product.

| Phase | Time | Main work |
|---|---|---|
| **1. Parsing and normalization** | Feb 12-24 | Built the citation parser (state machine), statute extraction, and false-positive filtering; iterated the schema from v1 to v4 |
| **2. Keyword search** | Feb 25-Mar 3 | Compared OpenSearch with PostgreSQL GIN, replaced IK tokenization with 2-gram ngram, and built target ranking based on citation snippet matches |
| **3. API and frontend** | Mar 5-13 | Built REST APIs and SQL aggregation, the React search interface and filtering UI, and completed Docker + EC2 deployment |
| **4. Parser refactor** | Mar 14-21 | Refactored the citation parser into traceable, testable small functions and tightened false-positive filtering rules |
| **5. Semantic search and RAG** | Mar 22-27 | Ran multiple rounds of embedding evaluation, designed citation-anchored chunks, and integrated pgvector retrieval with Gemini analysis |
| **6. Optimization and deployment** | Mar 26-30 | Completed chunk deduplication, production HTTPS deployment, and baseline performance tuning |
| **7. Search and retrieval optimization** | Apr 7-19 | Built `source_target_windows_v2`, introduced step-down MSM recall, and reduced follow-up query latency through caching |

---

## Future Work

- redesign chunk boundaries, including semantic segmentation or LLM-assisted segmentation, so factual narratives, party arguments, and the court's own legal reasoning can be separated more cleanly
- test whether using an LLM to rewrite user queries into more precise legal issues and practitioner-style terminology can improve retrieval recall and relevance
