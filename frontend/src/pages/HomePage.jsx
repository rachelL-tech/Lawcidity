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
        <p className="text-lg text-text-secondary mb-8">
          {t("hero.subheadline")}
        </p>
        <div className="flex flex-wrap justify-center gap-4">
          <Link
            to={`/${lang}/demo`}
            className="bg-brand text-white font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-dark transition-colors"
          >
            {t("hero.cta_demo")}
          </Link>
          <a
            href="https://github.com/rachelL-tech/Lawcidity"
            target="_blank"
            rel="noopener noreferrer"
            className="border border-brand text-brand font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-light transition-colors"
          >
            {t("hero.cta_github")}
          </a>
        </div>
      </Section>

      {/* Trust strip */}
      <Section className="max-w-3xl mx-auto px-6 pb-16" delay={100}>
        <div className="flex flex-wrap justify-center gap-x-8 gap-y-3 py-5 border-y border-divider">
          <TrustBadge icon="&sect;" text={t("trust.lawyer")} />
          <TrustBadge icon="&#9783;" text={t("trust.dataset")} />
          <TrustBadge icon="&#9638;" text={t("trust.scale")} />
        </div>
      </Section>

      {/* What I Built — demo videos */}
      <Section className="max-w-4xl mx-auto px-6 pb-20" delay={200}>
        <h2 className="font-display text-3xl text-center mb-3">
          {t("what_built.heading")}
        </h2>
        <p className="text-center text-text-secondary mb-10 max-w-2xl mx-auto">
          {t("what_built.intro")}
        </p>
        {/* TODO: demo videos go here */}
        <div className="bg-card-bg border border-brand-border/50 rounded-xl p-12 text-center text-text-secondary/60 text-sm">
          Demo videos coming soon
        </div>
      </Section>

      {/* Final CTA */}
      <Section className="max-w-3xl mx-auto px-6 pb-24 text-center" delay={300}>
        <h2 className="font-display text-3xl mb-3">
          {t("final_cta.heading")}
        </h2>
        <p className="text-text-secondary mb-8">{t("final_cta.body")}</p>
        <div className="flex flex-wrap justify-center gap-4">
          <Link
            to={`/${lang}/demo`}
            className="bg-brand text-white font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-dark transition-colors"
          >
            {t("final_cta.cta_demo")}
          </Link>
          <a
            href="https://github.com/rachelL-tech/Lawcidity"
            target="_blank"
            rel="noopener noreferrer"
            className="border border-brand text-brand font-semibold text-sm px-6 py-3 rounded-lg hover:bg-brand-light transition-colors"
          >
            {t("final_cta.cta_github")}
          </a>
        </div>
      </Section>
    </div>
  );
}
