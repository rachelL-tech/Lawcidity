import { BrowserRouter, Routes, Route, Outlet } from "react-router-dom";
import Navbar from "./components/Navbar";
import HomePage from "./pages/HomePage";
import DemoPage from "./pages/DemoPage";
import KeywordResultsPage from "./pages/KeywordResultsPage";
import DecisionPage from "./pages/DecisionPage";
import AiResultsPage from "./pages/AiResultsPage";
import TracesPage from "./pages/TracesPage";

// Lawcidity 產品外殼（Navbar + 淺色底 + footer）
function MainLayout() {
  return (
    <div className="min-h-screen bg-page-bg">
      <Navbar />
      <Outlet />
      <footer className="text-center text-sm text-gray-400 py-6 font-body">
        &copy; 2026 Lawcidity. All Rights Reserved.
      </footer>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<MainLayout />}>
          <Route path="/" element={<HomePage />} />
          <Route path="/demo" element={<DemoPage />} />
          <Route path="/search" element={<KeywordResultsPage />} />
          <Route path="/decisions/:id" element={<DecisionPage />} />
          <Route path="/ai-results" element={<AiResultsPage />} />
        </Route>
        {/* observability dashboard：不套產品外殼，整頁 dark */}
        <Route path="/traces" element={<TracesPage />} />
      </Routes>
    </BrowserRouter>
  );
}
