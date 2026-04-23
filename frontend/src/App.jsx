import { BrowserRouter, Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import HomePage from "./pages/HomePage";
import DemoPage from "./pages/DemoPage";
import KeywordResultsPage from "./pages/KeywordResultsPage";
import DecisionPage from "./pages/DecisionPage";
import AiResultsPage from "./pages/AiResultsPage";

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-page-bg">
        <Navbar />
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/demo" element={<DemoPage />} />
          <Route path="/search" element={<KeywordResultsPage />} />
          <Route path="/decisions/:id" element={<DecisionPage />} />
          <Route path="/ai-results" element={<AiResultsPage />} />
        </Routes>
        <footer className="text-center text-sm text-gray-400 py-6 font-body">
          &copy; 2026 Lawcidity. All Rights Reserved.
        </footer>
      </div>
    </BrowserRouter>
  );
}
