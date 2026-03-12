import React from "react";
import "./SideNav.css";

const SideNav = ({ channels = [], active, onSelect }) => {
  return (
    <aside className="side-nav">
      <div className="side-nav-title">Channels</div>

      {channels.map((c) => (
        <div
          key={c.key}
          onClick={() => onSelect?.(c.key)}
          className={`side-nav-item ${active === c.key ? "active" : ""}`}
          title={c.description || c.label}
        >
          <span className="side-nav-dot" />
          <span className="side-nav-label">{c.label}</span>
        </div>
      ))}

      <div className="side-nav-footer"></div>
    </aside>
  );
};

export default SideNav;
