# Lawcidity

[繁體中文](README.zh-TW.md) | [English](README.en.md)

[![Frontend](https://img.shields.io/badge/frontend-React%2019-61DAFB?style=flat-square&logo=react&logoColor=0b0f19)](./frontend)
[![Backend](https://img.shields.io/badge/backend-FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)](./app)
[![Search](https://img.shields.io/badge/search-OpenSearch-005EB8?style=flat-square&logo=opensearch&logoColor=white)](#)
[![Vector](https://img.shields.io/badge/vector-pgvector-336791?style=flat-square&logo=postgresql&logoColor=white)](#)
[![AI](https://img.shields.io/badge/AI-Gemini%20%2B%20Voyage-FF6F00?style=flat-square)](#)
[![Data](https://img.shields.io/badge/data-1.4M%20decisions-6A1B9A?style=flat-square)](#)

**「引用関係」に基づく台湾の裁判検索システム。**

キーワード検索、引用関係、そして意味理解まで。Lawcidity は、実務上本当に参照価値のある裁判所の見解を見つける手助けをします。

**Demo:** [lawcidity.rachel-create.com](https://lawcidity.rachel-create.com/)

**試せる検索例**
- **キーワード検索**: キーワード `殺人`（殺人）, `無罪`（無罪） + 条文 `刑法`（刑法）, `271`
- **RAG検索**: `如果我騎機車，對方碰瓷，但我沒有行車記錄器，該怎麼主張自己無過失？`（「私がスクーターに乗っていて、相手が当たり屋だったが、ドライブレコーダーがない場合、どうすれば自分に過失がないと主張できるか」）

---

## プロジェクト概要

| 項目 | 内容 |
|---|---|
| **核となる発想** | 判決同士の **引用関係** をランキングの主軸にし、PageRank の発想を応用して、裁判所が繰り返し参照する法的見解を浮かび上がらせる |
| **検索モード** | キーワード検索（OpenSearch）+ セマンティック検索（RAG） |
| **データ規模** | `1.4M` 件の判決、`552K` 件の引用、`575K` チャンク |
| **技術上の焦点** | 引用解析、引用ベースのランキング、引用位置をアンカーにしたチャンク分割 |
| **性能改善** | キーワード検索を約 `73s` から `2-4s` に短縮 |
| **技術スタック** | FastAPI / PostgreSQL / OpenSearch / pgvector / Gemini / Voyage / React / AWS |

---

## このプロジェクトが解こうとしている問題

従来の法律検索には、主に二つの盲点があります。

### 1. 位置情報を無視
全文検索は、キーワードが文書中に出現するかどうかしか見ず、どこに出現したかを見ません。

しかし、判決文の中では段落ごとの重要度は同じではありません。
- 裁判所自身の法的理由付け
- 当事者の主張
- 手続の経過
- 証拠や事実の記載

全文検索はキーワードが出てくるかどうかだけを見て、その前後の文脈を見ません。法律実務では、同じキーワードのヒットでも、裁判所自身の法的理由付けに現れる方が通常は参考価値が高くなります。

### 2. 用語のずれ
同じ法的概念でも、表現の仕方が複数あります。たとえば、
- `詐欺`（詐欺） vs `詐騙`（だまし）
- `資遣`（整理解雇・レイオフ） vs `終止勞動契約`（労働契約の終了）

利用者が入力した語が、判決で一般的に用いられる表現と一致しない場合、従来のキーワード検索では関連判決を取りこぼしやすくなります。

---

## Lawcidity のアプローチ

| モード | 仕組み | 解決すること |
|---|---|---|
| **キーワード検索** | まず全文から関連する判決を拾い、その判決群が共通して引用している判決を集め、関連度と引用の多さで並べる | 単に「同じ語を含む判決」を探すのではなく、「その法律争点を論じる際に裁判所が最もよく引用する判決」を探す |
| **セマンティック検索（RAG）** | 利用者のクエリと、引用付近から切り出した本文断片をベクトル化し、意味的に近いチャンクを取得する | 厳密なキーワード一致への依存を下げ、法的意味の近い事例を見つける |

---

## プロジェクトの見どころ

- **引用関係をランキングの中心に据えている。**  
  「どの文書がこの語を含むか」から一歩進み、「どの判決がこの法的問題を処理するために実際に裁判所で使われているか」を見に行きます。

- **情報量の多い法的テキストに焦点を当てている。**  
  引用位置を起点にすることで、裁判所が実際に法的理由付けに入る箇所を捉え、検索の再現率とベクトル検索の質を高めています。

- **実際の大規模裁判データを扱っている。**  
  引用解析、誤検出の除去、OpenSearch のインデックス設計、性能最適化まで含めて、公開されている実データを処理しています。

- **性能改善が明確に出ている。**  
  キーワード検索は約 `73 秒` から `2-4 秒` に短縮され、再ランキングはキャッシュヒット時に `1 ms` 未満まで下がります。

---

## 機能デモ

### キーワード検索

![Keyword Search](frontend/public/keyword_search_diagram.png)

**できること:**
- `車禍`（交通事故）や `行車紀錄器`（ドライブレコーダー）などのキーワードで検索
- `刑法`（刑法）+ `284` のように条文条件を追加
- 事件類型、裁判所レベル、文書種別で絞り込み
- ある被引用判決が異なる引用元判決からどのような文脈で引用されているかを確認
- 引用元判決の原文をそのまま開く

![](frontend/public/gif/keyword-1-input.gif)

![](frontend/public/gif/keyword-2-sort-filter.gif)

![](frontend/public/gif/keyword-3-snippets-and-decisions.gif)

### RAG 検索

![RAG Search](frontend/public/RAG_search_diagram.png)

**できること:**
- 事案の事実関係を自然言語で入力
- Gemini に候補となる法律争点と条文を抽出させる
- 内容を確認したうえで、争点ごとの分析とそれを支える判決を得る

![](frontend/public/gif/rag-1-analyze.gif)

![](frontend/public/gif/rag-2-analysis-page.gif)

---

## なぜ「引用関係」でランキングするのか

![Citation Concept](frontend/public/citation_concept.png)

法律上の引用は学術論文の引用によく似ています。データ構造の観点から見ると、**PageRank** の直感にもかなり近いものです。

> ある判決が他の判決から頻繁に引用されているなら、実務上一定の重みを持っている可能性が高い。

法律事務所でのインターン中、異なる **引用元判決** から同じ **被引用判決** に向かう引用の抜粋に、非常によく似た文言が現れることに気づきました。これは、引用数の多い判決が単に偶然多く言及されているのではなく、その判決自体が一つまたは複数の明確な法的見解を定立しており、類似した問題を扱う裁判所が似た形でそれを引用していることを示しています。

たとえば `車禍`（交通事故）で検索すると、
- 最も多く引用される被引用判決は、「突発状況」に関する抜粋で繰り返し現れる
- 二番目に多い被引用判決は、「逃逸」に関する抜粋に集中している

これは、裁判所が同じ法的問題を扱うとき、同じ法的見解を繰り返し援用していることを示しています。

![](frontend/public/why_citations_snippet.png)

**したがって、引用回数は単なる人気ではなく、特定の法的問題について裁判所が形成してきた安定した実務見解も反映します。**

- 全文検索が問うのは: 同じ語を **言及している** 判決はどれか
- 引用ランキングが問うのは: その語に関連する法的争点を処理するために、裁判所が実際に使っている判決はどれか

---

## 用語とデータ単位

![Mark Terms](frontend/public/mark_terms.png)

実装上は、まず判決文中の案号を手がかりに `citation` 候補を特定し、その前後の文脈から真の `citation` かどうかを判定します。

| 用語 | 説明 |
|---|---|
| **decision** | 判決・裁定を含む裁判所の裁判文書。引用関係のグラフではノードとして扱う |
| **authority** | 司法院釈字や決議など、裁判ではない法的権威資料。これもノードとして扱う |
| **source** | 他の裁判や法的権威資料を引用する裁判 |
| **target** | 引用される裁判または法的権威資料 |
| **citation** | `source` が `target` を一度引用した記録 |
| **citation snippet** | 各 `citation` の周辺にある法的理由付けの断片で、`target` がどの文脈で引用されたかを示す |
| **statute** | 判決全文または citation snippet に現れる条文。たとえば民法第184条 |
| **chunk** | `citation` の位置を起点に切り出されたテキスト単位で、セマンティック検索の検索単位になる |
| **embedding** | `chunk` のベクトル表現で、意味的類似検索に用いる |

---

## 検索性能と最適化結果

| 操作 | 改善前 | 改善後 |
|---|---|---|
| キーワード検索（`詐欺` / fraud） | ~73s | 2-4s |
| 再ランキング | ~1.27s | ~0.04s（キャッシュヒット時は `<1 ms`） |
| 引用展開 | 13-16s | ~0.8-1.0s |

---

## アーキテクチャ

![Architecture](frontend/public/Architecture.png)

| 層 | 技術 |
|---|---|
| フロントエンド | React 19, Tailwind CSS 4 |
| バックエンド | FastAPI |
| キーワード検索 | OpenSearch（2-gram ngram analyzer） |
| セマンティック検索 | pgvector（ivfflat） |
| データベース | PostgreSQL |
| AI サービス | Gemini Flash, Voyage API（`voyage-law-2`） |
| デプロイ | AWS EC2, RDS, ALB, nginx |

---

## データソースとデータモデル

### データソース
[司法院オープンデータプラットフォーム](https://opendata.judicial.gov.tw/)  
2025 年 1 月から 2026 年 1 月までの公開裁判データを収録しています。

生データ JSON の例（ファイル名は元データのまま保持）:  
[data/PCDV,113,訴,2272,20250210,1.json](data/PCDV,113,訴,2272,20250210,1.json)

### データ規模

PostgreSQL: **17 GB**（RDS）  
OpenSearch: **3.2 GB**（EC2）

### ETL フロー

![Flowchart](frontend/public/flow_chart.png)

### PostgreSQL ER 図

![PostgreSQL ER Diagram](frontend/public/er-diagram.png)

### 主要テーブル

| テーブル | 件数 | 説明 |
|---|---|---|
| `decisions` | 1.4M | 正規化済みの裁判データ。`source` と `target` の両方を含む |
| `citations` | 552K | `source` から `target` への引用記録。citation snippets と全文中の位置情報を含む |
| `chunks` | 575K | `citation` 位置を起点に切り出したテキスト片。`embedding` を持ち、セマンティック検索に使用 |
| `decision_reason_statutes` | 6.6M | 判決全文から抽出した条文引用 |
| `citation_snippet_statutes` | 458K | citation snippets から抽出した条文引用 |
| `authorities` | 1.6K | 司法院釈字や決議など、裁判ではない法的権威資料 |

### OpenSearch インデックスと文書構造

![OpenSearch Index](frontend/public/opensearch_index_documents.png)

| インデックス | 文書数 | サイズ | 説明 |
|---|---:|---:|---|
| `decisions_v3` | 3.0M | 2.8 GB | 全文キーワード検索用のインデックス。まず条件に合う source IDs を取得するために使う |
| `source_target_windows_v2` | 997K | 456 MB | citation snippets を持つ source-target ペア文書。取得した source 群から関連度の高い citation snippets を見つけ、その先の target を取り出すために使う |

---

## 主要な技術判断

### 1. 引用解析

![Raw JSON vs Parsed](frontend/public/raw_vs_parsed.png)

**クリーニングと解析**  
司法院が提供する裁判データは生の JSON で、全文の形式が一定せず、空白や非構造化内容も混ざっています。そのままでは検索に使えません。

![True vs False](frontend/public/true_vs_false.png)

**本当の難しさ**  
判決文に現れる案号は、必ずしも法的引用を意味しません。たとえば、
- 証拠への参照
- 手続の経過
- 過去事件の記録
- 当事者の主張の中に出てくる案号

などである可能性があります。

裁判所が自らの法的理由付けの中で、先行判決を論拠として引用している場合に限って、それを真の `citation` とみなすべきです。

たとえば、次の文字列はいずれも `citation` 候補として拾われ得ますが、真の法的引用とは限りません。

- `按最高法院 112 年度台上字第 1234 號判決意旨……`（「最高法院 112 年度台上字第 1234 號判決の趣旨によれば……」）
- `本件前經最高法院 112 年度台上字第 1234 號判決發回更審`（「本件は以前、最高法院 112 年度台上字第 1234 號判決により差戻しとなった」）
- `有臺灣高等法院 111 年度上字第 567 號裁定在卷可參`（「臺灣高等法院 111 年度上字第 567 號裁定が記録にあり参照できる」）

三つとも案号を含みますが、裁判所が既存の法的見解を援用しているのは最初の例だけで、後の二つは手続の経過や記録資料への言及にすぎません。

**アプローチ**
- 前後文を見るためのルールを追加してフィルタする
- 抽出とフィルタのロジックを小さな関数に分け、個別にテスト・調整できるようにする

大まかな流れは次のとおりです。

```text
1. 緩めの regex を使って、判決全文から案号候補を抽出する
2. 各候補の前後文と段落位置を調べる
3. その文脈に基づいて、手続の経過、証拠参照、当事者の主張など、真の引用ではない用法を除外する
4. 残ったものを `citation` とみなす
5. 有効と判定された `citation` の周囲から法的理由付けの `citation snippet` を切り出す
```

**結果**  
現在の pytest テストケースは、実データから取った 27 件以上の edge cases をカバーしており、たとえば次のようなものがあります。
- 記録中の証拠物の除外
- 手続経過の検出
- 当事者主張の段落と裁判所自身の理由付け段落の区別

---

### 2. キーワード検索: 候補取得と順位付け

![retrieval](frontend/public/retrieval.png)

### なぜ二段階に分けるのか

初期のパイプラインはすべて PostgreSQL 上で処理していました。
1. 各判決の `clean_text` を `ILIKE` で走査して source を取得
2. 取得した source ごとに `citation snippets` を走査
3. 各 target の snippet ヒットスコアを計算
4. 合計スコアで target を並べる

この方法はデータ量が少ないうちは動きますが、`詐欺`（fraud）のような広いクエリでは、第一段階で非常に多くの source がヒットし、snippet 走査がボトルネックになります。

### Stage 1: source の取得

**アプローチ**  
まず、全文が検索条件に合う source IDs を見つけます。

**なぜ PostgreSQL GIN ではないのか**  
測定では、OpenSearch での source 取得は
- 約 **27 倍** 高速
- インデックスサイズは **3 分の 1 未満**

でした。

**中国語テキストの検索戦略**  
OpenSearch でよく使われる IK tokenizer は主に簡体字中国語向けです。一方、裁判文には tokenizer の辞書に載っていない専門的な法律語彙が大量に含まれ、分かち書きが安定しませんでした。

最終的に選んだのは、

**2-gram ngram + `match_phrase`**

です。つまり、
- 文書を重なり合う 2 文字単位に分割し
- `match_phrase` でそれらが順序どおり連続して現れることを要求する

ことで、キーワードが文書の離れた場所に散ってヒットしてしまうのを防ぎつつ、フレーズに近い精度を維持しています。

### Stage 2: target の収集

**アプローチ**  
Stage 1 で取得した source 群を対象に、それぞれの citation snippets を見て、検索条件にヒットした snippets だけを残し、それらが共通して指している target を数えます。

**なぜこの段階も OpenSearch に移したのか**  
初期版では、Stage 1 が source IDs を返した後、PostgreSQL が source ごとに citation snippets を順番に走査していました。取得される source 数が数万件規模になると、ここでも性能が大きく悪化しました。

そこで `source_target_windows_v2` インデックスを作りました。

- 各文書は 1 組の `(source, target)` ペアを表す
- そのペアに属する `citation snippets` と条文データをまとめて保持する
- `citation snippets` に対するキーワード条件や条文条件の照合を OpenSearch 内で処理できる

ようにしています。

PostgreSQL は最後の metadata 取得と集計だけを担当します。

*MSM ladder: ヒットした `citation snippets` を段階的に集める仕組み*

Stage 2 の target 収集では、step-down 形式の MSM ladder を使います。

MSM（`minimum_should_match`）は、ある source-target ペアが citation snippets の中で何個のクエリ句を満たせば条件適合とみなすかを制御します。

クエリ句には、
- `過失`（過失）や `車禍`（交通事故）のようなキーワード
- 刑法第 284 条や民法第 185 条のような条文条件

があります。

たとえばクエリ句が 3 個あるなら、システムは

1. MSM = 3
2. MSM = 2
3. MSM = 1

の順に試します。

最も厳しい MSM = N から始め、条件を徐々に緩めながら、候補プールが 200 個の target に達するまで集めます。

各 target には、最初にプールへ入った MSM レベルを `reached_at_msm` として記録します。

これはつまり、
- 最も高い MSM レベルで取得された target ほど、利用者の検索条件により正確に対応している可能性が高い
- より低い MSM レベルで初めて現れる target は、引用文脈との関連が相対的に弱い

ということです。

### target はどう並べるのか

![ranking](frontend/public/ranking.png)

target の順位付けは主に次の二つの手掛かりに基づきます。

1. **`reached_at_msm`**  
   より高い MSM レベルで最初に取得された target を優先する

2. **`matched_citation_count`**  
   同じ MSM レベル内では、その target を指している異なる source の数で二次的に並べる

つまり Lawcidity が見ているのは、単に検索キーワードや条文を含む判決ではなく、

> 利用者の検索条件に関連する裁判所の理由付けの中で、どの target が最も多くの裁判所に繰り返し参照されているか

という点です。

### その後の操作の待ち時間をどう下げたか

**ランキングキャッシュ**  
最初の版では Stage 1 の source IDs しかキャッシュしていなかったため、利用者が
- 並び替え条件を変える
- ページを変える
- 絞り込みを追加する

たびに Stage 2 を再実行する必要がありました。

後の版では、最初の検索時点で target の順位付け全体をキャッシュし、その後の操作はメモリ上で処理できるようにしました。

**引用展開の高速化**  
- まず OpenSearch が返した `preview source IDs` だけを候補にし、その `source` に紐づく `citation` の中から score が最も高い 1 件を選んでから判決情報を補うようにした
- それ以外の引用は、`source` ごとに `citation` を 1 件だけ先に取り出してから判決情報を結合するようにし、不要な join と sort を減らした

これにより、引用展開は約 `3 秒` から約 `0.8 秒` に短縮されました。

**UI 表示用の値を事前計算**  
- UI 表示用の事件番号や引用数をあらかじめ計算して持たせ、検索時の都度計算を避けた

**インデックス調整**  
- `WHERE` / `JOIN` / `ORDER BY` でよく使う条件に合わせて複合インデックスを張り直した

---

### 3. RAG 検索: 検索と生成

### RAG の流れ
利用者はまず自然言語で法的問題を記述します。Gemini が候補となる法律争点と関連条文を抽出し、利用者が確認した後、残りの RAG パイプラインに進みます。

- **クエリ理解**  
  利用者入力を明示的な法律争点と条文条件に整理し、後続の生成分析に渡す構造化入力にする

- **R — Retrieval（検索）**  
  利用者クエリを `embedding` に変換し、pgvector から意味的に近い citation-anchored `chunks` を取得し、判決単位へ集約する

- **A — Augmentation（文脈補強）**  
  取得した `chunks`、`source` 判決の `metadata`、関連する `target references` をプロンプトにまとめ、後続の分析の文脈として使う

- **G — Generation（生成）**  
  Gemini が retrieval で得た実際の判決を根拠に、争点ごとの分析を生成する

### 検索
- Voyage API（`voyage-law-2`）でクエリを `embedding` に変換
- PostgreSQL / pgvector の IVFFlat index で近似検索を実行
- 余弦類似度で最も近い上位 50 chunks を返す
- 判決単位に集約し、最も高得点の chunk をその判決の代表スコアとする

### Chunk 設計

各 `chunk` は全文を機械的に切るのではなく、判決中の `citation` 位置を基点に切り出します。これにより、`embedding` に送られるのは、裁判所が実質的な法的理由付けに入る箇所であり、情報量の多いテキストになります。

- **中心点**: 判決中の citation 位置
- **境界**: citation 位置から、最も近い構造マーカー `㈠㈡㈢`、`⒈⒉⒊`、`一二三` などまで広げる
- **長すぎる場合**: 範囲が 2,000 文字を超えるときは、`。` を文境界として切り直す
- **ハード制約**: 理由欄の見出しより前には伸ばさず、文末の日付行も越えない
- **重複処理**: 隣接する `citation` 由来の `chunks` が重なれば統合し、完全に同一なら MD5 で重複除去して冗長な `embedding` を避ける

![Chunk Design](frontend/public/chunk_design.png)

### Embedding モデル選定

`embedding` モデルは 3 ラウンド評価し、次を比較しました。

- `BAAI bge-m3`
- `Qwen3-Embedding (0.6B / 4B)`
- `Gemini embedding`
- `voyage-multilingual-2`
- `voyage-law-2`
- `voyage-4-large`

各ラウンドでは同じ評価セットを使いました。
- 民事・刑事・行政・知財を含む 6 件の target 判決
- 各 target をもともと指していた citation snippets を正例として使用
- 無関係な snippets 20 件を負例として追加

### 評価指標

- **`avg gap`**: 関連する抜粋の平均スコアから無関係な抜粋の平均スコアを引いた値。関連・無関係の分離をどれだけ安定して行えるかを見る
- **`Recall@5`**: 関連する抜粋が上位 5 件に入る割合。関連断片を上位に押し上げられるかを見る

| Model | avg gap | min gap | Recall@5 |
|---|---:|---:|---:|
| bge-m3 | 0.212 | 0.080 | 0.826 |
| Qwen3-Embedding-0.6B (512d) | 0.341 | 0.177 | 0.938 |
| voyage-multilingual-2 | 0.386 | 0.287 | 0.938 |
| voyage-4-large | 0.351 | 0.230 | 0.938 |
| **voyage-law-2** | **0.404** | **0.241** | 0.882 |

**最終選択: `voyage-law-2`**

主な理由は、**avg gap** が最も高く、関連する抜粋と無関係な抜粋を最も安定して分離できたからです。

- `Qwen3-Embedding-0.6B` と比べて avg gap は約 **18%** 高い
- `voyage-4-large` と比べても約 **15%** 高い

`Recall@5` は一部モデルよりやや低いものの、関連する抜粋と無関係な抜粋のスコア差をより明確に作れるため、無関係な断片が高得点結果に紛れ込みにくくなります。

---

## 開発の流れ

7 週間にわたるアジャイル開発で、司法院の生の JSON から出発し、実際に使える検索プロダクトへと段階的に仕上げました。

| フェーズ | 期間 | 主な作業 |
|---|---|---|
| **1. 解析と正規化** | 2 月 12-24 日 | 引用パーサ（状態機械）、条文抽出、false positive 除去を作り、schema を v1 から v4 へ |
| **2. キーワード検索** | 2 月 25 日-3 月 3 日 | OpenSearch と PostgreSQL GIN を比較し、IK tokenizer を 2-gram ngram に置き換え、citation snippet の検索条件への一致度にもとづく順位付けを導入 |
| **3. API とフロントエンド** | 3 月 5-13 日 | REST API と SQL 集約、React の検索 UI とフィルタ UI を実装し、Docker + EC2 デプロイを完了 |
| **4. パーサ再設計** | 3 月 14-21 日 | 引用パーサを追跡可能でテストしやすい小さな関数群に再構成し、false positive 除去ルールを強化 |
| **5. セマンティック検索と RAG** | 3 月 22-27 日 | 埋め込み評価を複数回実施し、citation 起点の chunks を設計し、pgvector retrieval と Gemini 分析を統合 |
| **6. 最適化とデプロイ** | 3 月 26-30 日 | chunk の重複除去、本番 HTTPS デプロイ、基礎的な性能調整を実施 |
| **7. 検索・取得の最適化** | 4 月 7-19 日 | `source_target_windows_v2` を構築し、step-down MSM recall を導入し、キャッシュで後続操作の待ち時間を削減 |

---

## 今後の課題

- chunk の境界設計を見直し、意味分割や LLM 補助分割を含めて、事実記述・当事者主張・裁判所自身の法的理由付けをよりきれいに分ける
- 利用者クエリをより精密な法律争点や実務用語に LLM で書き換えることで、検索再現率と関連性が改善するか検証する
