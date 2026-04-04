import { useTranslation } from "react-i18next";
import { Link, useParams } from "react-router-dom";

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

function ProblemCard({ title, body }) {
  return (
    <div className="bg-card-bg border border-brand-border/50 rounded-xl p-6 hover:border-brand/30 transition-colors">
      <h3 className="font-display text-xl text-text-primary mb-2">{title}</h3>
      <p className="text-sm text-text-secondary font-body leading-relaxed">
        {body}
      </p>
    </div>
  );
}

function DecisionCard({ number, title, body }) {
  return (
    <div className="bg-card-bg border border-brand-border/50 rounded-xl p-6 hover:border-brand/30 transition-colors group">
      <div className="flex items-start gap-4">
        <span className="text-3xl font-display text-brand/20 group-hover:text-brand/40 transition-colors leading-none">
          {number}
        </span>
        <div>
          <h3 className="font-display text-xl text-text-primary mb-2">
            {title}
          </h3>
          <p className="text-sm text-text-secondary font-body leading-relaxed">
            {body}
          </p>
        </div>
      </div>
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

export default function PortfolioHomePage() {
  const { t } = useTranslation();
  const { lang = "en" } = useParams();

  return (
    <div className="font-body text-text-primary">
      {/* Hero */}
      <Section className="max-w-3xl mx-auto px-6 pt-20 pb-16 text-center">
        <p className="text-sm font-semibold tracking-widest uppercase text-brand mb-6">
          {t("hero.eyebrow")}
        </p>
        <h1 className="font-display text-4xl md:text-5xl leading-tight text-text-primary mb-5">
          {t("hero.headline")}
        </h1>
        <p className="text-lg text-text-secondary mb-4">
          {t("hero.subheadline")}
        </p>
        <p className="text-sm text-text-secondary/70 max-w-xl mx-auto mb-10">
          {t("hero.supporting")}
        </p>
        <div className="flex flex-wrap justify-center gap-4">
          <Link
            to={`/${lang}/overview`}
            className="bg-brand text-white font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-dark transition-colors"
          >
            {t("hero.cta_overview")}
          </Link>
          <Link
            to={`/${lang}/demo`}
            className="border border-brand text-brand font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-light transition-colors"
          >
            {t("hero.cta_demo")}
          </Link>
          <a
            href="https://github.com/rachelL-tech/Lawcidity"
            target="_blank"
            rel="noopener noreferrer"
            className="border border-brand-border text-text-secondary font-semibold text-sm px-6 py-3 rounded-lg hover:border-brand/40 transition-colors"
          >
            {t("hero.cta_github")}
          </a>
        </div>
      </Section>

      {/* Trust strip */}
      <Section
        className="max-w-3xl mx-auto px-6 pb-16"
        delay={100}
      >
        <div className="flex flex-wrap justify-center gap-x-8 gap-y-3 py-5 border-y border-divider">
          <TrustBadge icon="&sect;" text={t("trust.lawyer")} />
          <TrustBadge icon="&#9783;" text={t("trust.dataset")} />
          <TrustBadge icon="&#9638;" text={t("trust.scale")} />
        </div>
      </Section>

      {/* Why this problem is hard */}
      <Section className="max-w-4xl mx-auto px-6 pb-20" delay={200}>
        <h2 className="font-display text-3xl text-center mb-3">
          {t("why_hard.heading")}
        </h2>
        <p className="text-center text-text-secondary mb-10 max-w-2xl mx-auto">
          {t("why_hard.intro")}
        </p>
        <div className="grid md:grid-cols-3 gap-5">
          <ProblemCard
            title={t("why_hard.card1_title")}
            body={t("why_hard.card1_body")}
          />
          <ProblemCard
            title={t("why_hard.card2_title")}
            body={t("why_hard.card2_body")}
          />
          <ProblemCard
            title={t("why_hard.card3_title")}
            body={t("why_hard.card3_body")}
          />
        </div>
      </Section>

      {/* What I built */}
      <Section className="max-w-4xl mx-auto px-6 pb-20" delay={300}>
        <h2 className="font-display text-3xl text-center mb-3">
          {t("what_built.heading")}
        </h2>
        <p className="text-center text-text-secondary mb-10 max-w-2xl mx-auto">
          {t("what_built.intro")}
        </p>
        <div className="bg-card-bg border border-brand-border/50 rounded-xl p-8 overflow-x-auto">
          <div className="flex flex-wrap items-center justify-center gap-y-3 gap-x-0 min-w-0">
            {[
              t("what_built.step1"),
              t("what_built.step2"),
              t("what_built.step3"),
              t("what_built.step4"),
              t("what_built.step5"),
              t("what_built.step6"),
            ].map((label, i, arr) => (
              <FlowStep key={i} label={label} isLast={i === arr.length - 1} />
            ))}
          </div>
          <p className="text-xs text-text-secondary/60 text-center mt-5">
            {t("what_built.caption")}
          </p>
        </div>
      </Section>

      {/* Key technical decisions */}
      <Section className="max-w-4xl mx-auto px-6 pb-20" delay={400}>
        <h2 className="font-display text-3xl text-center mb-3">
          {t("decisions.heading")}
        </h2>
        <p className="text-center text-text-secondary mb-10 max-w-2xl mx-auto">
          {t("decisions.intro")}
        </p>
        <div className="grid md:grid-cols-2 gap-5">
          <DecisionCard
            number="01"
            title={t("decisions.card1_title")}
            body={t("decisions.card1_body")}
          />
          <DecisionCard
            number="02"
            title={t("decisions.card2_title")}
            body={t("decisions.card2_body")}
          />
          <DecisionCard
            number="03"
            title={t("decisions.card3_title")}
            body={t("decisions.card3_body")}
          />
          <DecisionCard
            number="04"
            title={t("decisions.card4_title")}
            body={t("decisions.card4_body")}
          />
        </div>
      </Section>

      {/* My contribution */}
      <Section className="max-w-3xl mx-auto px-6 pb-20" delay={500}>
        <div className="border-l-4 border-brand bg-brand-light/40 rounded-r-xl py-8 px-8">
          <h2 className="font-display text-2xl mb-3">
            {t("contribution.heading")}
          </h2>
          <p className="text-text-secondary leading-relaxed">
            {t("contribution.body")}
          </p>
        </div>
      </Section>

      {/* Final CTA */}
      <Section className="max-w-3xl mx-auto px-6 pb-24 text-center" delay={600}>
        <h2 className="font-display text-3xl mb-3">
          {t("final_cta.heading")}
        </h2>
        <p className="text-text-secondary mb-8">{t("final_cta.body")}</p>
        <div className="flex flex-wrap justify-center gap-4">
          <Link
            to={`/${lang}/overview`}
            className="bg-brand text-white font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-dark transition-colors"
          >
            {t("final_cta.cta_overview")}
          </Link>
          <Link
            to={`/${lang}/demo`}
            className="border border-brand text-brand font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-light transition-colors"
          >
            {t("final_cta.cta_demo")}
          </Link>
          <a
            href="https://github.com/rachelL-tech/Lawcidity"
            target="_blank"
            rel="noopener noreferrer"
            className="border border-brand-border text-text-secondary font-semibold text-sm px-6 py-3 rounded-lg hover:border-brand/40 transition-colors"
          >
            {t("final_cta.cta_github")}
          </a>
        </div>
      </Section>
    </div>
  );
}
