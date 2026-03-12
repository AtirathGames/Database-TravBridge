// // conversationhistory.js
// import React, { useState, Suspense, lazy } from "react";
// import { useNavigate } from "react-router-dom";

// // Dynamically map channel -> component. Add new channels here later.
// const CHANNEL_REGISTRY = {
//   "ChatBot": lazy(() => import("./Chatbothistory")),
//   "Agent-Assistant": lazy(() => import("./agentassisthistory")),
// };

// const CHANNEL_OPTIONS = [
//   { value: "ChatBot", label: "ChatBot" },
//   { value: "Agent-Assistant", label: "Agent Assist" },
// ];

// export default function ConversationHistory() {
//   const [channel, setChannel] = useState("ChatBot");
//   const navigate = useNavigate();

//   const styles = {
//     container: {
//       display: "flex",
//       flexDirection: "column",
//       gap: "20px",
//       padding: "20px",
//       fontFamily: "'Poppins', sans-serif",
//       backgroundColor: "#0056B3",
//       minHeight: "100vh",
//       color: "#f5f5f5",
//       boxSizing: "border-box",
//     },
//     button: {
//       padding: "10px 20px",
//       borderRadius: "8px",
//       border: "none",
//       backgroundColor: "#f5f5f5",
//       color: "#000",
//       cursor: "pointer",
//       transition: "all 0.2s",
//       marginRight: "10px",
//     },
//     title: {
//       fontSize: "36px",
//       fontWeight: "bold",
//       color: "#f5f5f5",
//       margin: 0,
//     },
//     headerRow: {
//       display: "flex",
//       alignItems: "center",
//       gap: "10px",
//       marginBottom: "10px",
//     },
//     filterRow: {
//       display: "flex",
//       alignItems: "center",
//       gap: "10px",
//       flexWrap: "wrap",
//     },
//     select: {
//       padding: "8px",
//       borderRadius: "8px",
//       border: "1px solid #e0e0e0",
//       backgroundColor: "#f5f5f5",
//       color: "#000",
//       minWidth: "160px",
//     },
//     card: {
//       backgroundColor: "#0a66c2",
//       padding: "12px",
//       borderRadius: "10px",
//     },
//     hr: {
//       border: "none",
//       height: "1px",
//       background: "rgba(255,255,255,0.2)",
//       margin: "10px 0 0",
//     },
//   };

//   const SelectedHistory = CHANNEL_REGISTRY[channel] || CHANNEL_REGISTRY["ChatBot"];

//   return (
//     <div style={styles.container}>
//       <div style={styles.headerRow}>
//         {/* <button style={styles.button} onClick={() => navigate("/home")}>
//           &larr; Back
//         </button> */}
//         <button style={styles.button} onClick={() => window.history.back()}>
//   ← Back
// </button>
//         <h1 style={styles.title}>Conversation Dashboard</h1>
//       </div>

//       <div style={styles.card}>
//         <div style={styles.filterRow}>
//           <label htmlFor="channel">Channel:</label>
//           <select
//             id="channel"
//             style={styles.select}
//             value={channel}
//             onChange={(e) => setChannel(e.target.value)}
//           >
//             {CHANNEL_OPTIONS.map((c) => (
//               <option key={c.value} value={c.value}>
//                 {c.label}
//               </option>
//             ))}
//           </select>
//         </div>
//         <div style={styles.hr} />
//       </div>

//       {/* The child renders all filters/search/table/modal for the chosen channel */}
//       <Suspense fallback={<div>Loading {channel} view…</div>}>
//         <SelectedHistory />
//       </Suspense>
//     </div>
//   );
// }
