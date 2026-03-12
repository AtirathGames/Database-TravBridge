// src/HomePage.jsx
import React from "react";
import { useNavigate } from "react-router-dom";
import { MessagesSquare, BarChart2 } from "lucide-react";
import "./Home.css";

const ButtonWithIcon = ({ Icon, label, onClick }) => {
  return (
    <button className="home-button" onClick={onClick}>
      <Icon className="home-button-icon" size={48} />
      <span className="home-button-label">{label}</span>
    </button>
  );
};

const HomePage = ({ isTCIL, toggleState }) => {
  const navigate = useNavigate();

  const handleLogout = () => {
    localStorage.removeItem("isLoggedIn");
    navigate("/");
  };

  return (
    <div className={`home-page ${isTCIL ? "tcil" : "sotc"}`}>
      <div className="home-header-bar">
        <h1 className="home-title">TravBridge Data &amp; Log Subsystem</h1>
        <button className="logout-button" onClick={handleLogout}>
          Logout
        </button>
      </div>

      <div className="toggle-wrapper">
        <span className="toggle-label">TCIL</span>
        <div
          className={`toggle-switch ${isTCIL ? "tcil" : "sotc"}`}
          onClick={toggleState}
        >
          <div
            className={`toggle-circle ${isTCIL ? "left" : "right"}`}
          ></div>
        </div>
        <span className="toggle-label">SOTC</span>
      </div>

      <div className="home-section">
        <div className="home-grid">
          <ButtonWithIcon
            Icon={MessagesSquare}
            label="Conversation History"
            onClick={() =>
              navigate(isTCIL ? "/tcil-dashboard" : "/sotc-dashboard")
            }
          />
          <ButtonWithIcon
            Icon={BarChart2}
            label="Weekly Stats"
            onClick={() =>
              navigate(isTCIL ? "/tcil-stats" : "/sotc-stats")
            }
          />
          {/* 
          <ButtonWithIcon
            Icon={BarChart2}
            label="Daily Batch Processing Stats"
            onClick={() => navigate("/batchstats")}
          />
          */}
        </div>
      </div>
    </div>
  );
};

export default HomePage;
