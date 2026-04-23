import { NavLink } from "react-router-dom";

export default function PortfolioNavbar() {
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
          to="/"
          end
          className="flex items-center gap-2 text-brand font-bold text-lg font-body"
        >
          <img
            src="/favicon.png"
            alt=""
            aria-hidden="true"
            className="w-8 h-8 rounded-full object-cover"
          />
          Lawcidity 法澄
        </NavLink>

        <div className="flex items-center gap-8">
          <NavLink to="/demo" className={linkClass}>
            Demo
          </NavLink>
          <a
            href="https://github.com/rachelL-tech/Lawcidity"
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm font-body text-text-secondary hover:text-brand transition-colors"
          >
            GitHub
          </a>
        </div>
      </div>
    </nav>
  );
}
