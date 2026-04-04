import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import LangLayout from "./components/LangLayout";
import PortfolioHomePage from "./pages/PortfolioHomePage";
import OverviewPage from "./pages/OverviewPage";
import DemoPage from "./pages/DemoPage";
import SearchResultsPage from "./pages/SearchResultsPage";
import DecisionPage from "./pages/DecisionPage";
import AiResultsPage from "./pages/AiResultsPage";

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-page-bg">
        <Routes>
          <Route path="/" element={<Navigate to="/en" replace />} />
          <Route path="/:lang" element={<LangLayout />}>
            <Route index element={<PortfolioHomePage />} />
            <Route path="overview" element={<OverviewPage />} />
            <Route path="demo" element={<DemoPage />} />
            <Route path="search" element={<SearchResultsPage />} />
            <Route path="decisions/:id" element={<DecisionPage />} />
            <Route path="ai-results" element={<AiResultsPage />} />
          </Route>
        </Routes>
        <footer className="text-center text-sm text-gray-400 py-6 font-body">
          &copy; 2026 Lawcidity. All Rights Reserved.
        </footer>
      </div>
    </BrowserRouter>
  );
}
