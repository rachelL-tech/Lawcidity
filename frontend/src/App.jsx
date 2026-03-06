import { BrowserRouter, Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import HomePage from "./pages/HomePage";
import SearchResultsPage from "./pages/SearchResultsPage";
import DecisionPage from "./pages/DecisionPage";

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-page-bg">
        <Navbar />
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/search" element={<SearchResultsPage />} />
          <Route path="/decisions/:id" element={<DecisionPage />} />
        </Routes>
        <footer className="text-center text-sm text-gray-400 py-6">
          © 2026 Lawcidity 法澄. All Rights Reserved.
        </footer>
      </div>
    </BrowserRouter>
  );
}
