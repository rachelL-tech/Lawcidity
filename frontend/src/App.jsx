import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import LangLayout from "./components/LangLayout";
import HomePage from "./pages/HomePage";
import DemoPage from "./pages/DemoPage";
import KeywordResultsPage from "./pages/KeywordResultsPage";
import DecisionPage from "./pages/DecisionPage";
import AiResultsPage from "./pages/AiResultsPage";

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-page-bg">
        <Routes>
          <Route path="/" element={<Navigate to="/en" replace />} />
          <Route path="/:lang" element={<LangLayout />}>
            <Route index element={<HomePage />} />
            <Route path="demo" element={<DemoPage />} />
            <Route path="search" element={<KeywordResultsPage />} />
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
