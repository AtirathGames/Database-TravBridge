import React from "react";
import "./SOTCSideNav.css";

const SideNav = ({ channels = [], active, onSelect }) => {
  return (
    <aside className="sotc-side-nav">
      <div className="sotc-side-nav-title">Channels</div>

      {channels.map((c) => (
        <div
          key={c.key}
          onClick={() => onSelect?.(c.key)}
          className={`sotc-side-nav-item ${active === c.key ? "active" : ""}`}
          title={c.description || c.label}
        >
          <span className="sotc-side-nav-dot" />
          <span className="sotc-side-nav-label">{c.label}</span>
        </div>
      ))}

      <div className="sotc-side-nav-footer" />
    </aside>
  );
};

export default SideNav;
