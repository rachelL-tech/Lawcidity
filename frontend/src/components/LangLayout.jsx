import { useEffect } from "react";
import { Outlet, useParams, Navigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import PortfolioNavbar from "./PortfolioNavbar";

const SUPPORTED_LANGS = ["en"];

export default function LangLayout() {
  const { lang } = useParams();
  const { i18n } = useTranslation();

  useEffect(() => {
    if (lang && SUPPORTED_LANGS.includes(lang)) {
      i18n.changeLanguage(lang);
    }
  }, [lang, i18n]);

  if (!SUPPORTED_LANGS.includes(lang)) {
    return <Navigate to="/en" replace />;
  }

  return (
    <>
      <PortfolioNavbar />
      <Outlet />
    </>
  );
}
