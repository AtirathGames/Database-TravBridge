// src/App.js
import React, { useState } from "react";
import {
  BrowserRouter as Router,
  Routes,
  Route,
  Navigate,
} from "react-router-dom";

import LoginForm from "./LoginPage/LoginPage";            // Login form
import HomePage from "./Components/Home/Home";            // Home page

// TC and SOTC Chatbot history components
import TeeCeeChatbot from "./Components/TC/TeeCeeChatbot";
import SotcChatbot from "./Components/SOTC/SotcChatbot";

// TC and SOTC Stats components
import TeeCeeStats from "./Components/TC/TeeCeeStats";
import SotcStats from "./Components/SOTC/SotcStats";

import DailyBatchStats from "./DailyBatchStats";

// Protected Route Component
const ProtectedRoute = ({ children }) => {
  const isLoggedIn = localStorage.getItem("isLoggedIn") === "true";
  return isLoggedIn ? children : <Navigate to="/" replace />;
};

const App = () => {
  const [isTCIL, setIsTCIL] = useState(true);

  const toggleState = () => {
    setIsTCIL((prev) => !prev);
  };

  return (
    <Router>
      <Routes>
        {/* Public route: Login */}
        <Route path="/" element={<LoginForm />} />

        {/* Home */}
        <Route
          path="/home"
          element={
            <ProtectedRoute>
              <HomePage isTCIL={isTCIL} toggleState={toggleState} />
            </ProtectedRoute>
          }
        />

        {/* TCIL Chatbot Dashboard */}
        <Route
          path="/tcil-dashboard"
          element={
            <ProtectedRoute>
              <TeeCeeChatbot />
            </ProtectedRoute>
          }
        />

        {/* SOTC Chatbot Dashboard */}
        <Route
          path="/sotc-dashboard"
          element={
            <ProtectedRoute>
              <SotcChatbot />
            </ProtectedRoute>
          }
        />

        {/* TCIL Stats */}
        <Route
          path="/tcil-stats"
          element={
            <ProtectedRoute>
              <TeeCeeStats />
            </ProtectedRoute>
          }
        />

        {/* SOTC Stats */}
        <Route
          path="/sotc-stats"
          element={
            <ProtectedRoute>
              <SotcStats />
            </ProtectedRoute>
          }
        />

        {/* Daily Batch Stats */}
        <Route
          path="/batchstats"
          element={
            <ProtectedRoute>
              <DailyBatchStats />
            </ProtectedRoute>
          }
        />

        {/* Catch-all: if some unknown path, go to login */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Router>
  );
};

export default App;
