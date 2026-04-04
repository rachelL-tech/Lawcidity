import { Link, useParams } from "react-router-dom";

function Section({ id, children, className = "" }) {
  return (
    <section id={id} className={`mb-20 ${className}`}>
      {children}
    </section>
  );
}

function SectionHeading({ children }) {
  return (
    <h2 className="font-display text-3xl text-text-primary mb-6 border-b border-divider pb-3">
      {children}
    </h2>
  );
}

function SubHeading({ children }) {
  return (
    <h3 className="font-display text-xl text-text-primary mt-8 mb-3">
      {children}
    </h3>
  );
}

function Paragraph({ children }) {
  return (
    <p className="text-text-secondary font-body leading-relaxed mb-4">
      {children}
    </p>
  );
}

function ContribGroup({ title, children }) {
  return (
    <div className="bg-card-bg border border-brand-border/50 rounded-xl p-6">
      <h3 className="font-display text-lg text-brand mb-2">{title}</h3>
      <p className="text-sm text-text-secondary font-body leading-relaxed">
        {children}
      </p>
    </div>
  );
}

function FlowStep({ label, isLast }) {
  return (
    <div className="flex items-center gap-3">
      <div className="bg-brand-light border border-brand-border rounded-lg px-4 py-2 text-sm font-body text-text-primary whitespace-nowrap">
        {label}
      </div>
      {!isLast && (
        <span className="text-brand-border text-lg select-none">&rarr;</span>
      )}
    </div>
  );
}

function ArchBullet({ label, value }) {
  return (
    <li className="flex gap-2 text-sm">
      <span className="font-semibold text-text-primary min-w-[140px]">
        {label}
      </span>
      <span className="text-text-secondary">{value}</span>
    </li>
  );
}

export default function OverviewPage() {
  const { lang = "en" } = useParams();

  return (
    <div className="font-body text-text-primary">
      {/* Page Hero */}
      <div className="max-w-3xl mx-auto px-6 pt-20 pb-12 text-center animate-fade-in-up">
        <h1 className="font-display text-4xl md:text-5xl leading-tight mb-4">
          Inside Lawcidity
        </h1>
        <p className="text-lg text-text-secondary mb-4">
          Built on real Taiwanese court decisions, with separate paths for
          keyword search and AI-assisted search.
        </p>
        <p className="text-sm text-text-secondary/70 max-w-xl mx-auto">
          This page explains the data source, parsing constraints, retrieval
          architecture, and model decisions behind the demo.
        </p>

        {/* Metadata row */}
        <div className="flex flex-wrap justify-center gap-x-8 gap-y-3 mt-8 py-5 border-y border-divider text-sm text-text-secondary">
          <span>
            <span className="text-brand">&sect;</span> Taiwan-qualified lawyer
          </span>
          <span>
            <span className="text-brand">&#9783;</span> Public court decisions,
            2025-01 to 2026-01
          </span>
          <span>
            <span className="text-brand">&#9638;</span> 12GB PostgreSQL corpus
            including indexes
          </span>
        </div>
      </div>

      {/* Table of Contents */}
      <nav className="max-w-3xl mx-auto px-6 mb-16 animate-fade-in-up" style={{ animationDelay: "100ms" }}>
        <div className="bg-card-bg border border-brand-border/50 rounded-xl p-6">
          <h3 className="font-display text-lg mb-3 text-text-primary">
            Contents
          </h3>
          <ol className="grid md:grid-cols-2 gap-1 text-sm text-text-secondary list-decimal list-inside">
            {[
              ["overview", "Overview"],
              ["problem", "Problem"],
              ["what-i-built", "What I Built"],
              ["technical-decisions", "Technical Decisions"],
              ["architecture", "Architecture"],
              ["product-capabilities", "Product Capabilities"],
              ["my-contribution", "My Contribution"],
              ["results", "Results and Next Steps"],
            ].map(([id, label]) => (
              <li key={id}>
                <a
                  href={`#${id}`}
                  className="hover:text-brand transition-colors"
                >
                  {label}
                </a>
              </li>
            ))}
          </ol>
        </div>
      </nav>

      {/* Content */}
      <div className="max-w-3xl mx-auto px-6 animate-fade-in-up" style={{ animationDelay: "200ms" }}>
        {/* 1. Overview */}
        <Section id="overview">
          <SectionHeading>Overview</SectionHeading>
          <Paragraph>
            Lawcidity is an AI-assisted legal search project built on public
            Taiwanese court decisions. It explores how unstructured legal texts
            can be transformed into a usable search product through parsing,
            retrieval design, and evaluation-driven technical decisions.
          </Paragraph>
          <Paragraph>
            In this project, the AI-assisted search mode is implemented as a
            RAG-based retrieval workflow.
          </Paragraph>
          <div className="bg-brand-light/40 rounded-lg p-4 mt-4 text-sm">
            <p className="mb-1">
              <span className="font-semibold">Data source:</span>{" "}
              <a
                href="https://opendata.judicial.gov.tw/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-brand underline"
              >
                Judicial Yuan Open Data Platform
              </a>
            </p>
            <p>
              <span className="font-semibold">Core focus:</span> parsing,
              retrieval, embeddings, and product integration
            </p>
          </div>
        </Section>

        {/* 2. Problem */}
        <Section id="problem">
          <SectionHeading>Problem</SectionHeading>
          <Paragraph>
            Legal search becomes difficult when the source material is
            structurally inconsistent, citation-heavy, and context-dependent.
          </Paragraph>

          <SubHeading>Parsing and normalization</SubHeading>
          <Paragraph>
            Court decisions do not follow a single stable format. Their structure
            varies across judges, case types, and writing styles, and section
            boundaries may appear as numbered clauses, sub-clauses, outline
            markers, or plain numerals.
          </Paragraph>
          <Paragraph>
            Legal references are also not expressed in one uniform pattern. The
            same kind of citation may appear in a citation chain, inside
            brackets, with different trailing signals such as
            &ldquo;意旨參照&rdquo;, &ldquo;可參&rdquo;, or
            &ldquo;理由書闡釋略以&rdquo;, and with inconsistent document type
            labels such as 判決, 裁定, 裁判, 理由, or 憲判字.
          </Paragraph>
          <Paragraph>
            A case number is not always a legal authority citation. It may
            instead refer to an attachment in the record, a prior procedural
            history, or a summary of another ruling. This means the core problem
            is not only extraction, but classification and context-sensitive
            filtering.
          </Paragraph>

          <SubHeading>Retrieval architecture</SubHeading>
          <Paragraph>
            Retrieval in this project could not rely on a single search method.
            Lexical retrieval and semantic retrieval addressed different search
            goals, and each introduced its own technical constraints.
          </Paragraph>

          <h4 className="font-body font-semibold text-text-primary mt-6 mb-2">
            Lexical retrieval
          </h4>
          <Paragraph>
            Lexical retrieval was primarily aimed at higher-level legal reasoning
            search, such as doctrines, holdings, and statute-linked views. But
            citation snippets are relatively short and often contain limited
            factual context. Their ranking signals also depend heavily on whether
            the snippet explicitly contains the query keywords or statute
            references, which creates built-in limits for recall and ranking
            quality.
          </Paragraph>
          <Paragraph>
            Retrieval infrastructure also became a practical problem. A naive
            PostgreSQL ILIKE scan was too slow for a usable search experience,
            while indexed retrieval still required trade-offs between latency,
            index size, and recall behavior.
          </Paragraph>

          <h4 className="font-body font-semibold text-text-primary mt-6 mb-2">
            Semantic retrieval
          </h4>
          <Paragraph>
            Semantic retrieval was better suited for fact-pattern search, where
            relevant cases may not share the same wording as the query. Because
            it works on larger chunks and ranks by similarity rather than literal
            term overlap, it can capture broader factual context than keyword
            matching alone.
          </Paragraph>
          <Paragraph>
            But this also introduced a different kind of retrieval problem.
            Embedding quality depended not only on model choice, but also on
            chunk design, evaluation method, and how the system handled texts
            that were legally relevant but not close enough in vector distance to
            survive a pure semantic ranking.
          </Paragraph>
        </Section>

        {/* 3. What I Built */}
        <Section id="what-i-built">
          <SectionHeading>What I Built</SectionHeading>
          <Paragraph>
            Lawcidity turns raw court decisions into a search system with
            separate keyword and RAG-based retrieval paths.
          </Paragraph>
          <ul className="list-disc list-inside text-sm text-text-secondary space-y-2 mb-8 ml-2">
            <li>
              <span className="font-semibold text-text-primary">
                Parsing &amp; normalization:
              </span>{" "}
              clean and structure unstable legal references, document types, and
              citation metadata
            </li>
            <li>
              <span className="font-semibold text-text-primary">
                Lexical retrieval:
              </span>{" "}
              retrieve citation-aware result pools for keyword and
              statute-driven legal reasoning search
            </li>
            <li>
              <span className="font-semibold text-text-primary">
                Semantic retrieval:
              </span>{" "}
              retrieve fact-pattern-adjacent chunks through RAG-oriented semantic
              search
            </li>
            <li>
              <span className="font-semibold text-text-primary">
                Result assembly:
              </span>{" "}
              aggregate retrieval outputs into source-level results with
              supporting targets and metadata
            </li>
            <li>
              <span className="font-semibold text-text-primary">
                Frontend presentation:
              </span>{" "}
              expose keyword search and AI-assisted search through separate
              user-facing flows
            </li>
          </ul>

          {/* Flow diagram */}
          <div className="bg-card-bg border border-brand-border/50 rounded-xl p-8 overflow-x-auto">
            <div className="flex flex-wrap items-center justify-center gap-y-3 gap-x-0 min-w-0">
              {[
                "Court decisions",
                "Parsing & normalization",
                "Indexing",
                "Keyword retrieval",
                "RAG-based retrieval",
                "Frontend demo",
              ].map((label, i, arr) => (
                <FlowStep
                  key={i}
                  label={label}
                  isLast={i === arr.length - 1}
                />
              ))}
            </div>
            <p className="text-xs text-text-secondary/60 text-center mt-5">
              A high-level pipeline from court decisions to keyword retrieval,
              RAG-based retrieval, and result presentation.
            </p>
          </div>
        </Section>

        {/* 4. Technical Decisions */}
        <Section id="technical-decisions">
          <SectionHeading>Technical Decisions</SectionHeading>
          <Paragraph>
            The project was shaped by technical trade-offs across parsing,
            retrieval, model selection, and product integration.
          </Paragraph>

          <SubHeading>Data Normalization</SubHeading>
          <Paragraph>
            Normalization in this project was not simple field cleaning. Court
            decisions express references inconsistently, with variations in
            section markers, citation signals, and document type labels such as
            判決, 裁定, 裁判, 理由, and 憲判字. I defined expected outputs for
            these unstable patterns and used iterative validation to make the
            normalization layer more predictable and more usable for downstream
            retrieval.
          </Paragraph>

          <SubHeading>Parser Refactor for Debuggability</SubHeading>
          <Paragraph>
            The parser had to handle citation chains, bracketed references,
            shifting paragraph boundaries, and snippet start and end decisions
            that depended on local context. I refactored the citation parser into
            smaller, traceable functions so each rule could be inspected, tested,
            and debugged against real failure cases. This made the pipeline easier
            to reason about and reduced the risk of reintroducing bugs that had
            already been observed in real cases.
          </Paragraph>

          <SubHeading>Lexical Retrieval Benchmarking</SubHeading>
          <Paragraph>
            I treated lexical retrieval as an infrastructure decision, not just a
            query implementation detail. First, I confirmed that indexed
            retrieval was necessary by comparing PostgreSQL GIN against an
            unindexed ILIKE sequential scan, which was far too slow for a usable
            search experience. I then compared PostgreSQL GIN and OpenSearch on
            the same cited-decision pool, looking at latency, returned result
            volume, and storage size. OpenSearch was faster and smaller, so it
            became the primary retrieval path. However, the decision did not end
            there: after choosing OpenSearch, I found that ik tokenization
            underperformed on legal vocabulary because segmentation was not
            predictable enough for this domain. To recover recall while keeping
            OpenSearch as the main search layer, I moved toward a 2-gram plus
            match_phrase strategy instead of relying on IK tokenization alone.
          </Paragraph>

          <SubHeading>Semantic Retrieval Benchmarking</SubHeading>

          <h4 className="font-body font-semibold text-text-primary mt-6 mb-2">
            Model evaluation and selection
          </h4>
          <Paragraph>
            I did not treat semantic retrieval as a single-model choice. I
            compared multiple embedding candidates across several rounds,
            including BAAI/bge-m3, Qwen3-Embedding-0.6B, Qwen3-Embedding-4B,
            Gemini embedding, voyage-multilingual-2, voyage-law-2, and
            voyage-4-large. The evaluation looked not only at Recall@5, but also
            at score gap between related and unrelated snippets, so the decision
            reflected both retrieval quality and ranking stability.
          </Paragraph>

          <h4 className="font-body font-semibold text-text-primary mt-6 mb-2">
            Deployment constraints
          </h4>
          <Paragraph>
            I also designed the embedding pipeline as a systems problem. Bulk
            embedding and query-time embedding had different operational
            constraints, and local models had to be evaluated not only for
            quality, but also for whether they could coexist with everyday
            development work. Qwen remained a viable local option, but local
            embedding generation on the Mac was slower than Voyage API and
            consumed enough machine resources that it interfered with parallel
            development. For that reason, the production embedding backend
            ultimately moved toward Voyage rather than staying on a purely local
            workflow.
          </Paragraph>

          <h4 className="font-body font-semibold text-text-primary mt-6 mb-2">
            Chunk design evolution
          </h4>
          <Paragraph>
            The retrieval pipeline itself also evolved. Citation-context chunks
            remained important, but I later expanded the chunk set to include
            supreme reasoning chunks because some high-value Supreme Court and
            Supreme Administrative Court passages were worth retrieving even
            without citation snippets. To reduce semantic misses, the RAG flow
            also used Gemini to extract issues and statutes, then combined pure
            semantic retrieval with a statute-matching auxiliary retrieval path
            before final scoring and reranking.
          </Paragraph>
        </Section>

        {/* 5. Architecture */}
        <Section id="architecture">
          <SectionHeading>Architecture</SectionHeading>
          <Paragraph>
            The architecture combines a React frontend, nginx reverse proxy,
            FastAPI application layer, OpenSearch for keyword retrieval,
            PostgreSQL with pgvector for structured storage and vector support,
            and external APIs for generation and embeddings.
          </Paragraph>
          <ul className="space-y-2 mt-6 mb-4">
            <ArchBullet label="Frontend" value="React" />
            <ArchBullet label="Proxy layer" value="nginx" />
            <ArchBullet label="Application layer" value="FastAPI" />
            <ArchBullet label="Search" value="OpenSearch" />
            <ArchBullet label="Storage" value="PostgreSQL + pgvector" />
            <ArchBullet label="External AI services" value="Gemini, Voyage" />
            <ArchBullet label="Infrastructure" value="AWS EC2 and RDS" />
          </ul>
        </Section>

        {/* 6. Product Capabilities */}
        <Section id="product-capabilities">
          <SectionHeading>Product Capabilities</SectionHeading>
          <Paragraph>
            A retrieval pipeline is only useful if it connects to a product layer
            that someone can actually use. This section explains how the
            technical capabilities became two distinct search flows, and why they
            were designed this way.
          </Paragraph>

          <SubHeading>Keyword search</SubHeading>
          <Paragraph>
            Keyword search was designed around citation-linked retrieval rather
            than plain document matching. The system finds sources containing the
            query keywords or statute references, then surfaces which targets
            those sources cite. When multiple sources point to the same target,
            that signals a stable, authoritative legal view &mdash; the kind of
            holding that courts tend to follow. This citation-expansion design
            was chosen because the primary value of keyword search in this domain
            is not just finding documents that mention a term, but helping users
            discover the upstream authority behind those documents.
          </Paragraph>
          <Paragraph>
            Ranking uses hit rate and citation count, with filters for document
            type and court level. These were chosen because they map to how legal
            professionals already evaluate relevance, rather than requiring users
            to learn a new ranking model.
          </Paragraph>

          <SubHeading>RAG-based search</SubHeading>
          <Paragraph>
            RAG-based search was built for a different retrieval goal:
            fact-pattern search, where relevant cases may not share the same
            vocabulary as the query. This required a fundamentally different
            design from keyword search, which is why the two paths are separated
            rather than merged.
          </Paragraph>
          <Paragraph>
            The chunk design is built around citation proximity as a value
            signal. For source decisions, the system extracts chunks near
            citation references rather than embedding full documents &mdash; text
            around citations tends to carry high-value legal reasoning, so this
            pre-filters less relevant context and reduces both embedding and
            retrieval cost. For high-value supreme court decisions, full texts
            are suitable as RAG generation inputs, so they are chunked as
            complete reasoning passages rather than citation-scoped excerpts.
          </Paragraph>
          <Paragraph>
            The product flow has two stages. In the first stage, Gemini extracts
            candidate issues and statutes from the user&rsquo;s factual query,
            and the user selects which ones to confirm. In the second stage, the
            user&rsquo;s original factual query is vectorized for semantic
            retrieval, while the confirmed issues and statutes guide the
            generation side of the RAG flow. On the other hand, the statute path
            acts as an auxiliary retrieval route &mdash; not replacing semantic
            retrieval, but recovering legally relevant chunks that may be missed
            by similarity ranking alone. This was added after observing that pure
            semantic retrieval missed cases where the legal relevance was
            statute-driven rather than fact-pattern-driven.
          </Paragraph>
        </Section>

        {/* 7. My Contribution */}
        <Section id="my-contribution">
          <SectionHeading>My Contribution</SectionHeading>
          <Paragraph>
            This project was built with AI-assisted development, but the problem
            framing, evaluation design, and technical decisions were my
            responsibility.
          </Paragraph>
          <div className="grid md:grid-cols-3 gap-5 mt-6">
            <ContribGroup title="I led">
              Problem framing, expected outputs, evaluation setup, search
              architecture decisions, parser refactoring direction, and final
              technical trade-offs.
            </ContribGroup>
            <ContribGroup title="AI-assisted implementation">
              AI tools supported parts of normalization logic, scripts, and UI
              scaffolding during prototyping and implementation.
            </ContribGroup>
            <ContribGroup title="I validated and converged">
              I verified outputs, debugged failure cases, and made the final
              decisions based on retrieval quality, system constraints, and
              product usability.
            </ContribGroup>
          </div>
        </Section>

        {/* 8. Results and Next Steps */}
        <Section id="results">
          <SectionHeading>Results and Next Steps</SectionHeading>

          <SubHeading>Current results</SubHeading>
          <ul className="list-disc list-inside text-sm text-text-secondary space-y-2 ml-2 mb-6">
            <li>
              A usable demo that connects keyword search and AI-assisted search
            </li>
            <li>A refactored parser structure with better debuggability</li>
            <li>
              Retrieval benchmarking to support search architecture decisions
            </li>
            <li>
              Embedding evaluation work tied to deployment constraints
            </li>
          </ul>

          <SubHeading>Next steps</SubHeading>
          <ul className="list-disc list-inside text-sm text-text-secondary space-y-2 ml-2">
            <li>
              Improve retrieval recall by refining how legal issues are
              represented in the embedding layer
            </li>
            <li>
              Redesign chunk boundaries to better separate factual context from
              legal reasoning
            </li>
            <li>
              Expand evaluation coverage to catch retrieval gaps across more case
              types
            </li>
            <li>
              Build a more repeatable benchmarking workflow for ongoing retrieval
              quality checks
            </li>
          </ul>
        </Section>

        {/* Final CTA */}
        <Section
          id="explore"
          className="text-center border-t border-divider pt-16"
        >
          <h2 className="font-display text-3xl mb-3">Explore the Project</h2>
          <p className="text-text-secondary mb-8">
            Read the homepage summary, inspect the source, or try the working
            demo.
          </p>
          <div className="flex flex-wrap justify-center gap-4">
            <Link
              to={`/${lang}/demo`}
              className="bg-brand text-white font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-dark transition-colors"
            >
              Try Demo
            </Link>
            <a
              href="https://github.com/rachelL-tech/Lawcidity"
              target="_blank"
              rel="noopener noreferrer"
              className="border border-brand text-brand font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-light transition-colors"
            >
              View on GitHub
            </a>
            <Link
              to={`/${lang}`}
              className="border border-brand-border text-text-secondary font-semibold text-sm px-6 py-3 rounded-lg hover:border-brand/40 transition-colors"
            >
              Back to Homepage
            </Link>
          </div>
        </Section>
      </div>
    </div>
  );
}
