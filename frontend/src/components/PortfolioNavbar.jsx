import { NavLink, useParams } from "react-router-dom";
import { useTranslation } from "react-i18next";

export default function PortfolioNavbar() {
  const { t } = useTranslation();
  const { lang = "en" } = useParams();

  const linkClass = ({ isActive }) =>
    `text-sm font-body transition-colors ${
      isActive
        ? "text-brand font-semibold"
        : "text-text-secondary hover:text-brand"
    }`;

  return (
    <nav className="sticky top-0 z-50 bg-page-bg/90 backdrop-blur-sm border-b border-divider">
      <div className="max-w-5xl mx-auto flex items-center justify-between px-6 py-4">
        <NavLink
          to={`/${lang}`}
          end
          className="flex items-center gap-2 text-brand font-bold text-lg font-body"
        >
          <span className="w-8 h-8 bg-brand-light rounded-full flex items-center justify-center text-brand text-sm">
            &oline;
          </span>
          Lawcidity
        </NavLink>

        <div className="flex items-center gap-8">
          <NavLink to={`/${lang}/overview`} className={linkClass}>
            {t("nav.overview")}
          </NavLink>
          <NavLink to={`/${lang}/demo`} className={linkClass}>
            {t("nav.demo")}
          </NavLink>
          <a
            href="https://github.com/rachelL-tech/Lawcidity"
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm font-body text-text-secondary hover:text-brand transition-colors"
          >
            {t("nav.github")}
          </a>
          <span className="text-xs text-text-secondary/60 border border-divider rounded px-2 py-1">
            EN
          </span>
        </div>
      </div>
    </nav>
  );
}
