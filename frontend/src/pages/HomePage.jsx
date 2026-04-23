import { useState } from "react";
import { Link } from "react-router-dom";

function Section({ children, className = "", delay = 0 }) {
  return (
    <section
      className={`animate-fade-in-up ${className}`}
      style={{ animationDelay: `${delay}ms` }}
    >
      {children}
    </section>
  );
}

function TrustBadge({ icon, text }) {
  return (
    <div className="flex items-center gap-2 text-sm text-text-secondary font-body">
      <span className="text-brand">{icon}</span>
      <span>{text}</span>
    </div>
  );
}

function DemoCarousel({ steps, accent }) {
  const [current, setCurrent] = useState(0);
  const total = steps.length;

  const activeDot =
    accent === "brand"
      ? "bg-brand text-white"
      : "bg-amber-500 text-white";
  const inactiveDot =
    "bg-transparent border border-text-secondary/30 text-text-secondary/50 hover:border-text-secondary/60";
  const arrowBase =
    "absolute top-1/2 -translate-y-1/2 z-10 w-9 h-9 rounded-full flex items-center justify-center shadow-md transition-opacity bg-white/90 border border-divider text-text-primary";

  return (
    <div>
      {/* GIF with arrows */}
      <div className="relative">
        <button
          onClick={() => setCurrent((c) => c - 1)}
          disabled={current === 0}
          className={`${arrowBase} left-3 ${current === 0 ? "opacity-0 pointer-events-none" : "opacity-80 hover:opacity-100"}`}
          aria-label="Previous"
        >
          ‹
        </button>

        <img
          key={current}
          src={steps[current].gif}
          alt={`Step ${current + 1}`}
          className="w-full rounded-xl border border-divider"
        />

        <button
          onClick={() => setCurrent((c) => c + 1)}
          disabled={current === total - 1}
          className={`${arrowBase} right-3 ${current === total - 1 ? "opacity-0 pointer-events-none" : "opacity-80 hover:opacity-100"}`}
          aria-label="Next"
        >
          ›
        </button>
      </div>

      {/* Description */}
      <p className="mt-4 text-sm text-text-secondary leading-relaxed">
        {steps[current].desc}
      </p>

      {/* Step indicators */}
      <div className="flex items-center justify-center gap-2 mt-4">
        {steps.map((_, i) => (
          <button
            key={i}
            onClick={() => setCurrent(i)}
            className={`w-7 h-7 rounded-full text-xs font-bold transition-colors ${
              i === current ? activeDot : inactiveDot
            }`}
          >
            {i + 1}
          </button>
        ))}
      </div>
    </div>
  );
}

export default function PortfolioHomePage() {
  const keywordSteps = [
    {
      desc: 'Enter keywords like "車禍" or "行車紀錄器". You can also optionally add a statute using autocomplete (e.g. "刑法" + "284") or filter by case type (e.g. "刑事").',
      gif: "/gif/keyword-1-input.gif",
    },
    {
      desc: "Sort by relevance or citation count; filter by documentation type and court level.",
      gif: "/gif/keyword-2-sort-filter.gif",
    },
    {
      desc: "Click a target to see matched and unmatched citation snippets, then drill into the full decision with jump-to-snippet.",
      gif: "/gif/keyword-3-snippets-and-decisions.gif",
    },
  ];

  const ragSteps = [
    {
      desc: "Describe a case in natural language → AI extracts legal issues and statutes → confirm before submitting.",
      gif: "/gif/rag-1-analyze.gif",
    },
    {
      desc: "Browse Gemini-generated analysis per issue with supporting decisions; click a source (orange) to open the full decision or a target (gray) to see citation counts.",
      gif: "/gif/rag-2-analysis-page.gif",
    },
  ];

  return (
    <div className="font-body text-text-primary">
      {/* Hero */}
      <Section className="max-w-3xl mx-auto px-6 pt-20 pb-16 text-center">
        <p className="text-sm font-semibold tracking-widest uppercase text-brand mb-6">
          Legal Search Project
        </p>
        <h1 className="font-display text-4xl md:text-5xl leading-tight text-text-primary mb-5">
          Lawcidity, a legal search project using Taiwanese court decisions.
        </h1>
        <p className="text-lg text-text-secondary mb-8">
          Built on real court decisions, with separate paths for keyword search and AI-assisted (RAG) retrieval.
        </p>
        <div className="flex flex-wrap justify-center gap-4">
          <Link
            to="/demo"
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
        </div>
      </Section>

      {/* Trust strip */}
      <Section className="max-w-3xl mx-auto px-6 pb-16" delay={100}>
        <div className="flex flex-wrap justify-center gap-x-8 gap-y-3 py-5 border-y border-divider">
          <TrustBadge icon="&sect;" text="Taiwan-qualified lawyer" />
          <TrustBadge icon="&#9783;" text="Public court decisions, 2025-01 to 2026-01" />
          <TrustBadge icon="&#9638;" text="1.4M decisions, 15GB across PostgreSQL and OpenSearch" />
        </div>
      </Section>

      {/* What I Built — demo carousel */}
      <Section className="max-w-4xl mx-auto px-6 pb-20" delay={200}>
        <h2 className="font-display text-3xl text-center mb-12">
          Features
        </h2>

        <div className="grid md:grid-cols-2 gap-10">
          {/* Keyword Search */}
          <div>
            <h3 className="font-display text-lg mb-5 flex items-center justify-center gap-2 text-center">
              <span className="bg-brand text-white text-xs font-semibold px-2 py-1 rounded-md tracking-wide">
                KEYWORD
              </span>
              Lexical Search
            </h3>
            <DemoCarousel steps={keywordSteps} accent="brand" />
          </div>

          {/* RAG Search */}
          <div>
            <h3 className="font-display text-lg mb-5 flex items-center justify-center gap-2 text-center">
              <span className="bg-amber-500 text-white text-xs font-semibold px-2 py-1 rounded-md tracking-wide">
                AI (RAG)
              </span>
              Semantic Search
            </h3>
            <DemoCarousel steps={ragSteps} accent="amber" />
          </div>
        </div>
      </Section>

      {/* Final CTA */}
      <Section className="max-w-3xl mx-auto px-6 pb-24 text-center" delay={300}>
        <h2 className="font-display text-3xl mb-3">
          Explore the Project
        </h2>
        <p className="text-text-secondary mb-8">Read the full case study, inspect the architecture, or try the working demo.</p>
        <div className="flex flex-wrap justify-center gap-4">
          <Link
            to="/demo"
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
        </div>
      </Section>
    </div>
  );
}
