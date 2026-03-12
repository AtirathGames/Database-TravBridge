// import React from "react";

// const SideNav = ({ channels = [], active, onSelect }) => {
//   const styles = {
//     aside: {
//       width: 240,
//       backgroundColor: "#ED1C24d9", // dark brand tone
//       color: "#111827",
//       padding: "16px 12px",
//       minHeight: "100vh",
//       boxShadow: "2px 0 8px rgba(0,0,0,0.15)",
//       position: "sticky",
//       top: 0,
//     },
//     title: {
//       fontSize: 18,
//       fontWeight: 700,
//       marginBottom: 10,
//       letterSpacing: 0.3,
//       opacity: 0.95,
//     },
//     item: (isActive) => ({
//       display: "flex",
//       alignItems: "center",
//       gap: 10,
//       padding: "10px 12px",
//       margin: "6px 0",
//       borderRadius: 8,
//       cursor: "pointer",
//       backgroundColor: isActive ? "#FFFFFF" : "transparent",
//       border: isActive ? "1px solid rgba(255,255,255,0.25)" : "1px solid transparent",
//     }),
//     dot: {
//       width: 8,
//       height: 8,
//       borderRadius: "50%",
//       background: "#00b894",
//       flex: "0 0 8px",
//     },
//     label: {
//       fontSize: 14,
//       fontWeight: 600,
//     },
//     footer: {
//       position: "absolute",
//       bottom: 12,
//       left: 12,
//       right: 12,
//       fontSize: 12,
//       opacity: 0.9,
//     },
//   };

//   return (
//     <aside style={styles.aside}>
//       <div style={styles.title}>Channels</div>
//       {channels.map((c) => (
//         <div
//           key={c.key}
//           onClick={() => onSelect?.(c.key)}
//           style={styles.item(active === c.key)}
//           title={c.description || c.label}
//         >
//           <span style={styles.dot} />
//           <span style={styles.label}>{c.label}</span>
//         </div>
//       ))}

//       <div style={styles.footer}>
//       </div>
//     </aside>
//   );
// };

// export default SideNav;
