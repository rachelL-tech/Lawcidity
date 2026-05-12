# Lawcidity

[日本語](README.md) | [English](README.en.md)

[![Frontend](https://img.shields.io/badge/frontend-React%2019-61DAFB?style=flat-square&logo=react&logoColor=0b0f19)](./frontend)
[![Backend](https://img.shields.io/badge/backend-FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)](./app)
[![Search](https://img.shields.io/badge/search-OpenSearch-005EB8?style=flat-square&logo=opensearch&logoColor=white)](#)
[![Vector](https://img.shields.io/badge/vector-pgvector-336791?style=flat-square&logo=postgresql&logoColor=white)](#)
[![AI](https://img.shields.io/badge/AI-Gemini%20%2B%20Voyage-FF6F00?style=flat-square)](#)
[![Data](https://img.shields.io/badge/data-1.4M%20decisions-6A1B9A?style=flat-square)](#)

**基於「引用關係」的台灣法院判決檢索系統。**

從關鍵字到語意理解，Lawcidity 協助使用者找出真正具有參考價值的法院見解。

**Demo:** [lawcidity.rachel-create.com](https://lawcidity.rachel-create.com/)

**可以先試試這組搜尋**
- **關鍵字搜尋**：關鍵字「殺人」「無罪」＋ 法條「刑法」「271」
- **RAG搜尋**：「如果我騎機車，對方碰瓷，但我沒有行車記錄器，該怎麼主張自己無過失？」

---

## 專案速覽

| 項目 | 內容 |
|---|---|
| **核心想法** | 用判決間的 **引用關係** 作為排序依據，借鑑 PageRank 核心概念，找出真正被法院反覆援引的法律見解 |
| **搜尋模式** | 關鍵字搜尋（OpenSearch）＋ 語意搜尋（RAG） |
| **資料規模** | `1.4M` 判決、`552K` citations、`575K` chunks |
| **技術重點** | citation parsing、citation-based ranking、citation-anchored chunking |
| **效能成果** | 關鍵字搜尋從約 `73s` 降到 `2–4s` |
| **技術選用** | FastAPI / PostgreSQL / OpenSearch / pgvector / Gemini / Voyage / React / AWS |

---

## 這個專案在解決什麼問題？

傳統法律檢索常有兩個核心盲點：

### 1. 忽略位置訊號
全文搜尋只判斷關鍵字是否出現在判決書中，卻不區分它出現在哪個脈絡。

但在判決書裡，不同段落的參考價值並不相同，例如：
- 法院自己的法律論述
- 當事人一方的主張
- 程序背景
- 事實背景
- 證據記載

對律師而言，法律主張必須建立在法院可接受的法律論述上。因此，同樣是命中關鍵字，若關鍵字出現在法院自己的法律說理段落，通常會比出現在事實背景或當事人主張中，更具有參考價值。

Lawcidity 因此特別關注法院引用前案的位置。因為法院通常是在處理法律爭點、展開自己的法律說理時，才會引用前案。

### 2. 用詞落差
同一個法律概念常常有不同說法，例如：
- 「詐欺」vs「詐騙」
- 「資遣」vs「終止勞動契約」

如果使用者輸入的詞，剛好不是法院慣用語，傳統關鍵字搜尋就容易漏掉實質相關的判決。

---

## Lawcidity 的解法

| 模式 | 怎麼做 | 解決什麼問題 |
|---|---|---|
| **關鍵字搜尋** | 在法院引用前案的片段中比對關鍵字，並依相關度與引用次數排序前案 | 不只找「全文有命中關鍵字」的判決，而是找出法院在「特定法律爭點下反覆援引」的重要判決 |
| **語意搜尋（RAG）** | 比對「使用者問題」與「引用周邊的法律論述」，找出「語意」上相近的判決 | 降低對精確關鍵字的依賴，找出「說法不同、爭點相同」的法院法律論述 |

---

## 專案特色

- **處理 140 萬筆公開判決資料。**  
  專案以公開判決文資料為對象，進行引用關係抽取、誤判內容排除與 OpenSearch 索引設計。

- **以引用關係作為排名機制的核心。**  
  不是只找出「提到同樣詞彙」的判決，而是優先找出「法院在相同爭點中反覆援引的判決」。

- **不只追求準確，也優化查詢速度。**  
  關鍵字搜尋從約 `73 秒` 降到 `2–4 秒`，重新排名時若命中快取，也可低於 `1 ms`。

---

## 功能展示

### 關鍵字搜尋

![Keyword Search](frontend/public/keyword_search_diagram.png)

**功能：**
- 輸入「車禍」、「行車紀錄器」等關鍵字
- 搭配法條條件搜尋，例如「刑法」＋「284」
- 依案件類型、法院層級、文書類型進一步篩選
- 直接查看 target 被不同 source 引用時的上下文脈絡
- 直接查看 source 原文

![](frontend/public/gif/keyword-1-input.gif)

![](frontend/public/gif/keyword-2-sort-filter.gif)

![](frontend/public/gif/keyword-3-snippets-and-decisions.gif)

### RAG 搜尋

![RAG Search](frontend/public/RAG_search_diagram.png)

**功能：**
- 用自然語言描述案件事實
- 讓 Gemini 先擷取可能涉及的法律爭點與法條
- 確認後送出搜尋，取得分爭點的分析與對應判決依據

![](frontend/public/gif/rag-1-analyze.gif)

![](frontend/public/gif/rag-2-analysis-page.gif)

---

## 為什麼「引用關係」值得當排名依據？

![Citation Concept](frontend/public/citation_concept.png)

引用關係如圖：一份後來作成的判決會引用一份前案，而法院引用前案時周圍的上下文，就是後面要看的 `citation snippet`。

Lawcidity 不只是找哪些判決「出現過」使用者的查詢，而是找：

> 在這個查詢下，哪些前案實際被法院反覆引用，後來的法院又是如何把這些前案整理成法律見解來使用。

法院在寫判決理由時，常會引用之前的判決。先看這張圖：

![Mark Terms](frontend/public/mark_terms.png)

這是一份真實判決的示意圖，裡面有三個最重要的東西：

- 最上面這份正在寫理由的判決，我稱它為 `source`，也就是引用方
- 橙色螢光筆標出的案號，例如「最高法院105年台上字第1374號」，我稱它為 `target`，也就是被引用的前案
- 藍色框起來的那一小段文字，我稱它為 `citation snippet`，也就是一個判決中最精華、最重要的片段

`citation snippet` 特別重要，因為法院通常會在這裡做兩件事：

- 把前案的法律見解濃縮成幾句話
- 接著說明這個見解怎麼套用到這次案件的事實

所以對律師來說，`target` 和 `citation snippet` 都重要：

- `target` 告訴我們：這個見解出自哪個前案
- `citation snippet` 告訴我們：後來的法院怎麼把前案濃縮成法律見解，又怎麼把它放進個案事實中

Lawcidity 的排序，就是利用這個結構來做。

系統不是把所有命中查詢的判決混在一起，而是先找出真正命中查詢條件的 `citation snippets`，再看這些片段共同指向哪些 `target`。如果很多不同的後來判決，在處理相近問題時，都收斂到同一個前案，那這個前案就應該優先排在前面。

換句話說，結果列表先回答的是：

> 在這個查詢下，法院實務最常收斂到哪些前案？

而點開結果後看到的 `citation snippets`，回答的則是：

> 後來的法院通常怎麼把這些前案濃縮成法律見解，並套用到具體個案裡？

例如在 Lawcidity 搜尋「車禍」時，把排名第一的結果展開後，可以看到很多後來判決都在討論「突發狀況」怎麼定義；而另一個前案底下，則多在討論「逃逸」的定義。

![](frontend/public/why_citations_snippet.png)

這時候你看到的，就不只是哪些結果命中了查詢，而是法院在不同問題上，如何反覆整理、使用既有見解。

因此，透過定位引用關係，我們可以同時抓到兩種線索：

- 一個判決中最精華、最重要的片段
- 在這個查詢下，被不同法院反覆援引的前案

而 Lawcidity 就是把這兩種線索一起作為排序依據。

---

## 術語與資料單位

以下整理本專案中會反覆出現的核心術語與資料單位。

| 術語 | 說明 |
|---|---|
| **decision** | 法院裁判，包含判決與裁定；在引用關係中作為節點 |
| **authority** | 非裁判類的法律文件，例如司法院釋字、決議等；也是節點 |
| **source** | 引用其他裁判或法律文件的裁判 |
| **target** | 被引用的裁判或法律文件 |
| **citation** | source 中一次引用 target 的紀錄 |
| **citation snippet** | 每次 citation 周圍的法律論述片段，用來呈現 target 被引用的具體脈絡 |
| **statute** | decision 全文或 citation snippet 中提到的法條，例如民法第184條 |
| **chunk** | 以引用位置為錨點切出的文字塊，作為語意搜尋的檢索單位 |
| **embedding** | chunk 的向量表示，用來進行語意相似度檢索 |

---

## 搜尋效能、優化結果

| 操作 | 優化前 | 優化後 |
|---|---|---|
| 關鍵字搜尋（「詐欺」） | ~73s | 2–4s |
| 重新排名 | ~1.27s | ~0.04s（快取命中：<1ms） |
| 引用展開 | 13–16s | ~0.8–1.0s |

---

## 系統架構

![Architecture](frontend/public/Architecture.png)

| 層次 | 技術 |
|---|---|
| 前端 | React 19, Tailwind CSS 4 |
| 後端 | FastAPI |
| 關鍵字搜尋 | OpenSearch（2-gram ngram analyzer） |
| 語意搜尋 | pgvector（ivfflat） |
| 資料庫 | PostgreSQL |
| AI 服務 | Gemini Flash, Voyage API（voyage-law-2） |
| 部署 | AWS EC2, RDS, ALB, nginx |

---

## 資料來源與資料模型

### 資料來源
[司法院開放資料平台](https://opendata.judicial.gov.tw/)  
收錄 2025 年 1 月到 2026 年 1 月的公開法院判決。

原始判決 JSON 範例：  
[data/PCDV,113,訴,2272,20250210,1.json](data/PCDV,113,訴,2272,20250210,1.json)

### 資料規模

PostgreSQL：**17 GB**（RDS）  
OpenSearch：**3.2 GB**（EC2）

### ETL 流程

![Flowchart](frontend/public/flow_chart.png)

### PostgreSQL ER 圖

![PostgreSQL ER Diagram](frontend/public/er-diagram.png)

### 核心資料表

| 資料表 | 筆數 | 說明 |
|---|---|---|
| `decisions` | 1.4M | 經正規化處理的法院裁判資料，包含 source 與 target decisions |
| `citations` | 552K | source 對 target 的引用紀錄，包含 citation snippets 與 citation 在全文中的位置 |
| `chunks` | 575K | 以 citation 位置為錨點切出的文字片段，包含 embeddings，用於語意搜尋 |
| `decision_reason_statutes` | 6.6M | 從裁判全文中擷取出的法條引用 |
| `citation_snippet_statutes` | 458K | 從 citation snippets 中擷取出的法條引用 |
| `authorities` | 1.6K | 非法院裁判的法律權威文件，例如司法院釋字、決議等 |

### OpenSearch 索引與文件結構

![OpenSearch Index](frontend/public/opensearch_index_documents.png)

| 索引 | 文件數 | 大小 | 說明 |
|---|---:|---:|---|
| `decisions_v3` | 3.0M | 2.8 GB | 全文關鍵字檢索索引，用來先召回符合搜尋條件的 source IDs |
| `source_target_windows_v2` | 997K | 456 MB | 含 citation snippets 的 source-target 配對，再從這些 source 的 citation snippets 中找出高相關引用片段，最後召回其共同指向的 target |

---

## 關鍵技術決策

## 1. Citation 解析

![Raw JSON vs Parsed](frontend/public/raw_vs_parsed.png)

**清洗、解析**  
司法院提供的法院判決是原始 JSON，全文格式不固定，也夾雜空白與非結構化內容，不能直接拿來做查詢。

![True vs False](frontend/public/true_vs_false.png)

**真正的難點**  
判決書中的案號，不一定代表法律見解引用。它可能是：
- 證據參照
- 程序經過
- 歷史案件紀錄
- 當事人主張中提到的案號

只有當法院在自己的說理中引用某個判決作為論述依據時，它才應該被視為真正的 citation。

例如，下列文字中的案號都可能被抓成 citation 候選，但只有部分屬於真正的法律見解引用：

- 「按最高法院 112 年度台上字第 1234 號判決意旨……」
- 「本件前經最高法院 112 年度台上字第 1234 號判決發回更審」
- 「有臺灣高等法院 111 年度上字第 567 號裁定在卷可參」

它們都包含案號，但只有第一種是法院援引既有見解；後兩種則只是程序歷史或卷內資料記載。

**做法**
- 加入上下文判斷規則過濾
- 將「擷取」與「過濾」拆成多個小型函式，方便獨立測試與調整

實作上，流程大致是：

```text
1. 使用較寬鬆的 regex，在判決全文中抓出可能的案號候選
2. 檢查候選周圍的前後文與段落位置
3. 根據前後文與段落位置，排除程序歷史、卷內證據、當事人主張等非真正引用的語境
4. 剩下者才視為 citation
5. 再依 citation 位置切出法律論述片段（citation snippet）
```

**結果**  
目前的 pytest 測試案例涵蓋 27 個以上來自真實判決資料的 edge cases，包括：
- 卷內證物過濾
- 程序歷史偵測
- 當事人主張段落與法院論述段落的區分...等等

---

## 2. 關鍵字搜尋：檢索與排名

![retrieval](frontend/public/retrieval.png)

### 為什麼拆成兩階段？

最早的 pipeline 全都在 PostgreSQL 中完成：
1. 對每份判決的 `clean_text` 做 `ILIKE` 掃描，召回 source
2. 再逐一掃描這些 source 的 `citation snippets`
3. 計算每個 target 的 snippets 命中分數
4. 最後依總分排序

這在資料量較小時可行，但像「詐欺」這種廣泛查詢，會一次召回大量 source，導致 snippets 掃描成為主要瓶頸。

### Stage 1：source 召回

**做法**  
先從判決全文中找出符合搜尋條件的 source IDs。

**為什麼不用 PostgreSQL GIN？**  
因為實測中，OpenSearch 的 source recall：
- 查詢速度約快 **27 倍**
- 索引體積不到原本的 **三分之一**

**中文檢索策略**  
OpenSearch 常用的 IK 分詞器主要針對簡體中文設計，但判決中有大量**詞庫未收錄**的專業法律用語，分詞結果不穩定。

最後改採：

**2-gram ngram + `match_phrase`**

也就是：
- 文件會被切成重疊的 2 字元片段
- `match_phrase` 會要求這些片段依照順序連續出現

這樣可以避免關鍵字被拆散到文件不同位置，同時保留接近詞組比對的精確度。

### Stage 2：target 召回

**做法**  
以 Stage 1 召回的 source 為範圍，檢查各自的 citation snippets，篩選出命中搜尋條件的 snippets，再統計它們共同指向哪些 target。

**為什麼也搬到 OpenSearch？**  
早期版本中，Stage 1 回傳 source IDs 後，由 PostgreSQL 逐一掃描 citation snippets。當 source 數量達到數萬筆時，效能也會明顯變差。

為了解決這個問題，我建立了 `source_target_windows_v2` index：

- 每一筆文件代表一組 `(source, target)` 配對
- 預先存放該配對下的所有 citation snippets 與法條資訊
- 讓 citation snippet 的關鍵字與法條比對也能在 OpenSearch 內完成

PostgreSQL 只負責最後的 metadata 查詢與統計。

*MSM ladder：分層收集命中 snippets*

第二階段的 target 收集採用逐步下調的 MSM ladder。

MSM（`minimum_should_match`）用來控制一筆 source-target 配對，至少要在 citation snippets 中命中多少個 query clauses，才會被視為符合條件。

這裡的 query clause 可以是：
- 關鍵字（過失、車禍）
- 法條條件（刑法第284條、民法第185條）

例如，一個查詢包含 3 個 query clauses 時，系統會依序嘗試：

1. MSM = 3
2. MSM = 2
3. MSM = 1

流程會先從最嚴格的 MSM=N 開始收集 target candidates，再逐步放寬條件，直到候選池累積到 200 個 target 為止。

每個 target 都會記錄自己第一次進入候選池時的 MSM 層級，也就是 `reached_at_msm`。

這代表：
- 若某個 target 在最高 MSM 層級就被召回，表示它越可能精確對應使用者的搜尋條件
- 若它只在較低 MSM 層級才出現，代表引用脈絡與搜尋條件的關聯較弱

### 排名怎麼做？

![ranking](frontend/public/ranking.png)

Target ranking 主要依據兩個訊號：

1. **`reached_at_msm`**  
   優先排序第一次在較高 MSM 層級被召回的 target。

2. **`matched_citation_count`**  
   在同一個 MSM 層級內，再依據該 target 不重複 source 總數當次要排序依據。

換句話說，Lawcidity 找的不是單純「全文中提到搜尋關鍵字或法條」的判決，而是：

> 在與使用者搜尋條件相關的核心法律論述（即 citation snippets）中，哪些 target 被法院反覆引用。

這比忽略位置訊號的全文搜尋，更接近律師真正想找的答案。

### 如何降低後續互動延遲？

搜尋結果頁上使用者主要做兩件事：
- 在結果列表上 **改排序、換頁、加篩選**
- 點開結果卡片 **讀各個 source 的 citation snippet**

兩條路徑都針對「初次搜尋後盡量不重做重活」做了優化。

**結果列表互動：快取整份排名**  
第一版只快取 Stage 1 的 source IDs，使用者只要換排序依據、換頁或加篩選條件，
就得重跑 Stage 2。後來改成在首次搜尋後直接快取完整的 target ranking 順序，
讓後續列表操作可以在記憶體中完成。

**點開結果卡片：兩階段載入**  
展開結果卡片時會顯示的 snippet 分成「初次載入」跟「查看更多」兩階段。

- **初次載入**：用 Stage 2 OpenSearch 回傳的 `preview source IDs` 當候選，
  PostgreSQL 從每個 source 對應的 citation 裡選分數最高的一筆，再補上判決資訊。
  這一段把初次展開時間從約 `3 秒` 降到約 `0.8 秒`。
- **查看更多**：Stage 2 為每個 target 只先取 5 筆 preview source ID。
  當實際引用該 target 的 source 超過 5 筆，UI 會出現「命中 N 筆」但實際只顯示 5 筆 snippet 的差距。
  考慮過送出搜尋時就補滿，但對熱門 keyword（例如損害、112k source）會多花約 `+3 秒`，
  因此改成只在命中數 > 顯示數時才出現「查看更多」按鈕，
  讓使用者想看更多時，才另外送一支 api，
  讓 PostgreSQL 一條 SQL 回傳下一頁 5 筆 snippet。
  額外時間成本只在使用者實際點擊查看更多的 target 才發生。

**預先計算 UI 顯示值**  
案號、引用數等 UI 顯示值在 ETL 階段就算好，搜尋時不重算。

**調整索引**  
依 `WHERE` / `JOIN` / `ORDER BY` 最常出現的查詢模式重建複合索引。

---

## 3. RAG 搜尋：檢索與生成

### RAG 流程
使用者先以自然語言描述法律問題。系統會先用 Gemini 擷取候選法律爭點與涉及的法條；等使用者確認後，再進入後續的 RAG pipeline：

  - **Query understanding**
    先將使用者輸入整理成較明確的法律爭點與法條條件，作為後續生成分析的結構化輸入。

  - **R — Retrieval**
    將使用者 query 轉成 embedding，從 pgvector 召回語意最相近的 citation-anchored chunks，並彙總到判決層級。

  - **A — Augmentation**
    將召回的 chunks、source 判決資訊與相關 target references 一起組裝進 prompt，作為後續生成分析的上下文。

  - **G — Generation**
    Gemini 根據檢索結果生成爭點分析，並附上對應的真實判決依據。

### 檢索
- 先透過 Voyage API（`voyage-law-2`）將 query 轉成 embedding
- 再使用 PostgreSQL / pgvector 的 IVFFlat index 做近似搜尋
- 召回依餘弦相似度排序、回傳最相似的前 50 個 chunks
- 最後再彙總到判決層級，以最高分 chunk 代表該判決的分數

### Chunk 設計

每個 chunk 都以判決中的 citation 位置為錨點，而不是隨機切分全文。這樣可以確保拿去 embeddings 的是法院進入法律論述、具高參考價值的文本段落。

- **中心點**：判決中的 citation 位置
- **邊界**：從 citation 位置向外擴展到最近的段落標記（㈠㈡㈢、⒈⒉⒊、一二三、等）
- **超長處理**：如果範圍超過 2,000 字，則退回以句號（。）作為切分邊界
- **硬性限制**：不得延伸到理由段落標題之前，也不得超過文末日期行
- **重疊處理**：相鄰 citation 產生的 chunks 若範圍重疊，會合併；完全相同者以 MD5 hash 去重，避免重複向量化的成本

![Chunk Design](frontend/public/chunk_design.png)

### Embedding 模型選擇

這裡總共做了三輪 Embedding 模型評估，涵蓋：

- `BAAI bge-m3`
- `Qwen3-Embedding（0.6B / 4B）`
- `Gemini embedding`
- `voyage-multilingual-2`
- `voyage-law-2`
- `voyage-4-large`

每輪都使用同一組測試資料：
- 6 個 target 判決（涵蓋民事、刑事、行政、智財）
- 每個 target 搭配原本指向它的 citation snippets 作為正確答案
- 再加入 20 個不相關 snippets 作為錯誤答案

### 評估指標

- **`avg gap`**：相關 snippets 平均分數減去不相關 snippets 平均分數，用來衡量模型是否能穩定拉開距離
- **`Recall@5`**：相關 snippets 出現在前 5 名結果中的比例，用來衡量模型能否把相關片段排進前段結果

| 模型 | avg gap | min gap | Recall@5 |
|---|---:|---:|---:|
| bge-m3 | 0.212 | 0.080 | 0.826 |
| Qwen3-Embedding-0.6B (512d) | 0.341 | 0.177 | 0.938 |
| voyage-multilingual-2 | 0.386 | 0.287 | 0.938 |
| voyage-4-large | 0.351 | 0.230 | 0.938 |
| **voyage-law-2** | **0.404** | **0.241** | 0.882 |

**最終選擇：`voyage-law-2`**

原因是它在 **avg gap** 上表現最好，代表它最能穩定區分相關與不相關 snippets。

- 相較於 `Qwen3-Embedding-0.6B`，avg gap 約高出 **18%**
- 相較於 `voyage-4-large`，avg gap 約高出 **15%**

雖然它的 `Recall@5` 略低於部分模型，但它更能把相關 snippets 和不相關 snippets 的分數拉開，也因此更不容易讓不相關片段混入高分結果。

---

## 開發歷程

七週迭代開發，從司法院原始 JSON 資料出發，逐步做成可實際使用的搜尋產品。

| 階段 | 時間 | 主要工作 |
|---|---|---|
| **1. 解析與正規化** | 2月12–24日 | 建立 Citation 解析器（狀態機）、法條抽取、false positive 過濾，並完成 schema v1 → v4 的資料表設計迭代 |
| **2. 關鍵字搜尋** | 2月25日–3月3日 | 比較 OpenSearch 與 PostgreSQL GIN 的效能，將中文檢索策略從 IK 分詞器改為 2-gram ngram，並建立以 citation snippet 分數排名 target 的機制 |
| **3. API 與前端** | 3月5–13日 | REST API 與 SQL 彙總、React 搜尋介面與篩選，並完成 Docker + EC2 部署 |
| **4. 解析器重構** | 3月14–21日 | 把 Citation 解析器重構為可追蹤、可測試的小型函式，收緊 false positive 過濾規則 |
| **5. 語意搜尋與 RAG** | 3月22–27日 | 進行多輪 embedding 評估，設計以 citation 為錨點的 chunks，並整合 pgvector 語意檢索與 Gemini AI 分析 |
| **6. 優化與部署** | 3月26–30日 | 完成 chunk 去重、正式環境 HTTPS 部署與基礎效能調整 |
| **7. 搜尋與檢索優化** | 4月7–19日 | 建立 `source_target_windows_v2` 索引，導入逐步下調的 MSM 梯度召回，並透過快取降低後續查詢延遲 |

---

## 未來方向

- 重新設計 chunk 邊界，評估 LLM 輔助切割，並依事實敘述、當事人主張與法院法律見解等脈絡，更明確地切分 chunks
- 驗證將使用者 query 改寫成法律實務中使用的用語後，是否能提升檢索召回率與結果相關性
- 為　LLM 呼叫補上 timeout 與 retry 控制，提升整體檢索 pipeline 的穩定性
- 強化 logging 與 tracing，讓 RAG 各階段的瓶頸或失敗點更容易被追蹤與診斷
