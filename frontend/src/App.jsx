import { Routes, Route } from "react-router-dom";
import Home from "./pages/Home";
import Audit from "./pages/Audit";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/audit" element={<Audit />} />
    </Routes>
  );
}
