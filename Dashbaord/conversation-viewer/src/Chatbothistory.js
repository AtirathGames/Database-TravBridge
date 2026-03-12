// // Chatbothistory.js
// import React, { useEffect, useState } from "react";
// import axios from "axios";

// // Axios defaults (server should ideally manage CORS).
// axios.defaults.withCredentials = false;
// axios.defaults.headers.common["Access-Control-Allow-Origin"] = "*";

// // Update BASE_URL as needed.
// const BASE_URL = "https://travbridge.atirath.com";
// const CHANNEL_KEY = "ChatBot"; // <— fixed to ChatBot

// export default function ChatbotHistory() {
//   // ---------------- STATE ----------------
//   const [startDate, setStartDate] = useState("");
//   const [endDate, setEndDate] = useState("");
//   const [isFiltered, setIsFiltered] = useState(false);

//   const [filterOpportunity, setFilterOpportunity] = useState(false);
//   const [filterUserType, setFilterUserType] = useState("all"); // "all", "registered", "guest"
//   const [filterToolOnly, setFilterToolOnly] = useState(false);

//   const [conversations, setConversations] = useState([]);
//   const [totalConvos, setTotalConvos] = useState(0);
//   const [isLoading, setIsLoading] = useState(false);

//   const [searchTerm, setSearchTerm] = useState("");
//   const [searchError, setSearchError] = useState("");
//   const [userConvoError, setUserConvoError] = useState("");

//   const [selectedConversation, setSelectedConversation] = useState(null);
//   const [viewMode, setViewMode] = useState("chat");
//   const [showModal, setShowModal] = useState(false);
//   const [conversationError, setConversationError] = useState("");

//   const [currentPage, setCurrentPage] = useState(1);
//   const pageSize = 10;

//   // ---------------- STYLES ----------------
//   const styles = {
//     // Child renders full content; parent provides header/channel picker
//     filterBox: { backgroundColor: "#0056B3", padding: "10px", borderRadius: "8px" },
//     groupTitle: { fontSize: "18px", fontWeight: 600, marginBottom: "10px", color: "#f5f5f5" },
//     select: {
//       padding: "8px",
//       borderRadius: "8px",
//       border: "1px solid #e0e0e0",
//       backgroundColor: "#f5f5f5",
//       color: "#000",
//       minWidth: "120px",
//       marginRight: "10px",
//     },
//     inputDate: {
//       padding: "8px",
//       borderRadius: "8px",
//       border: "1px solid #e0e0e0",
//       backgroundColor: "#f5f5f5",
//       color: "#000",
//       marginRight: "10px",
//     },
//     inputSearch: {
//       padding: "8px 12px",
//       borderRadius: "8px",
//       border: "1px solid #e0e0e0",
//       backgroundColor: "#f5f5f5",
//       color: "#000",
//       minWidth: "220px",
//       marginRight: "10px",
//     },
//     button: {
//       padding: "10px 20px",
//       borderRadius: "8px",
//       border: "none",
//       backgroundColor: "#0044a3",
//       color: "white",
//       cursor: "pointer",
//       transition: "all 0.2s",
//       marginRight: "10px",
//     },
//     conversationsSection: { marginTop: "20px" },
//     sectionTitle: { fontSize: "18px", fontWeight: 600, marginBottom: "10px", color: "#f5f5f5" },
//     tableHeader: {
//       display: "flex",
//       fontWeight: "bold",
//       color: "#fff",
//       marginBottom: "8px",
//       padding: "10px 15px",
//       textTransform: "uppercase",
//       borderBottom: "1px solid #ccc",
//     },
//     headerId: { flex: "1 1 15%", minWidth: "60px", textAlign: "left" },
//     headerName: { flex: "1 1 35%", minWidth: "150px", textAlign: "left" },
//     headerOpp: { flex: "1 1 15%", minWidth: "80px", textAlign: "center" },
//     headerMod: { flex: "1 1 25%", minWidth: "120px", textAlign: "left" },
//     headerBtn: { flex: "1 1 10%", textAlign: "right" },
//     rowContainer: {
//       display: "flex",
//       alignItems: "center",
//       backgroundColor: "#f8fafc",
//       color: "#1e293b",
//       borderRadius: "8px",
//       padding: "10px 15px",
//       marginBottom: "10px",
//       border: "1px solid #e2e8f0",
//     },
//     rowId: { flex: "1 1 15%", minWidth: "60px", textAlign: "left" },
//     rowName: { flex: "1 1 35%", minWidth: "150px", textAlign: "left" },
//     rowOpp: { flex: "1 1 15%", minWidth: "80px", textAlign: "center" },
//     rowMod: { flex: "1 1 25%", minWidth: "120px", textAlign: "left" },
//     rowBtn: { flex: "1 1 10%", textAlign: "right" },
//     paginationRow: { display: "flex", alignItems: "center", gap: "10px", marginTop: "10px" },
//     greenDot: { display: "inline-block", width: "10px", height: "10px", borderRadius: "50%", backgroundColor: "green" },
//     redDot: { display: "inline-block", width: "10px", height: "10px", borderRadius: "50%", backgroundColor: "red" },

//     modalOverlay: {
//       position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh",
//       backgroundColor: "rgba(0,0,0,0.5)", display: "flex", justifyContent: "center", alignItems: "center", zIndex: 9999,
//     },
//     modalContent: {
//       backgroundColor: "#fff", width: "80%", maxHeight: "80%", overflowY: "auto",
//       borderRadius: "8px", padding: "20px", position: "relative", color: "#000", display: "flex", flexDirection: "column",
//     },
//     closeButton: {
//       position: "absolute", top: "10px", right: "10px", backgroundColor: "#eee", border: "none",
//       borderRadius: "50%", width: "32px", height: "32px", cursor: "pointer", fontWeight: "bold",
//     },
//     viewToggle: { display: "flex", gap: "10px", marginBottom: "15px" },
//     toggleButton: {
//       padding: "8px 16px", borderRadius: "20px", border: "1px solid #cbd5e1", backgroundColor: "#fff", cursor: "pointer", transition: "all 0.2s",
//     },
//     activeToggle: { backgroundColor: "#0056B3", color: "#fff", borderColor: "#3b82f6" },
//     chatContainer: { display: "flex", flexDirection: "column", gap: "15px" },
//     messageBubble: { maxWidth: "80%", padding: "15px", borderRadius: "20px", margin: "5px 0" },
//     userMessage: { alignSelf: "flex-end", backgroundColor: "#0056B3", color: "white" },
//     otherMessage: { alignSelf: "flex-start", backgroundColor: "#f1f5f9", color: "#1e293b" },
//     packageGrid: {
//       display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(250px, 1fr))", gap: "20px", marginTop: "15px",
//     },
//     packageCard: { border: "1px solid #e2e8f0", borderRadius: "12px", overflow: "hidden", backgroundColor: "#fff", color: "#000" },
//     packageImage: { width: "100%", height: "150px", objectFit: "cover" },
//     packageContent: { padding: "15px" },
//     modalHeader: { position: "sticky", top: 0, backgroundColor: "#fff", zIndex: 100, paddingBottom: "10px" },
//     modalBody: { flex: 1, overflowY: "auto", paddingTop: "10px" },
//     jsonViewer: { backgroundColor: "#f8fafc", padding: "20px", borderRadius: "8px", whiteSpace: "pre-wrap", wordWrap: "break-word", overflowX: "auto" },
//     multilineContent: { whiteSpace: "pre-wrap", wordWrap: "break-word", margin: 0 },
//   };

//   // ---------------- API ----------------
//   const fetchAllConversations = async (page = 1) => {
//     setIsLoading(true);
//     try {
//       const filters = {
//         chat_channel: CHANNEL_KEY,
//         count: pageSize,
//         page,
//       };
//       if (startDate) filters.chat_started_from = startDate;
//       if (endDate) filters.chat_started_to = endDate;
//       if (filterOpportunity) filters.opportunity_id = "true";
//       if (filterToolOnly) filters.only_tool_conversations = true;
//       if (filterUserType.toLowerCase() !== "all") filters.userId = filterUserType;

//       const response = await axios.post(`${BASE_URL}/v1/get_all_conversations`, filters);
//       if (response.data.status === "success") {
//         setTotalConvos(response.data.total);
//         setCurrentPage(response.data.page);
//         setConversations(response.data.conversations || []);
//       } else {
//         setConversations([]);
//         setTotalConvos(0);
//       }
//     } catch (error) {
//       console.error("Error fetching all conversations:", error);
//       setConversations([]);
//       setTotalConvos(0);
//     } finally {
//       setIsLoading(false);
//     }
//   };

//   const fetchConversationById = async (convId) => {
//     try {
//       setViewMode("chat");
//       const payload = { conversationId: convId };
//       const response = await axios.post(`${BASE_URL}/v1/get_conversation`, payload);
//       if (response.data.status === "success") {
//         setSelectedConversation(response.data.conversation);
//         setShowModal(true);
//       } else {
//         setConversationError(response.data.message || "No conversation found.");
//       }
//     } catch (error) {
//       console.error("Error fetching conversation:", error);
//       setConversationError(error.response?.data?.detail || "Unexpected error");
//     }
//   };

//   const fetchUserConversations = async (requestedPage = 1, userId) => {
//     try {
//       setUserConvoError("");
//       const payload = { userId };
//       const response = await axios.post(`${BASE_URL}/v1/get_conversation_summaries`, payload);
//       if (response.data.status === "success") {
//         const raw = response.data.conversations || [];
//         setTotalConvos(raw.length);
//         setCurrentPage(requestedPage);
//         const startIdx = (requestedPage - 1) * pageSize;
//         setConversations(raw.slice(startIdx, startIdx + pageSize));
//       } else {
//         setUserConvoError(response.data.message || "No conversations found");
//         setConversations([]);
//       }
//     } catch (error) {
//       console.error("Error fetching user conversations:", error);
//       setUserConvoError(error.response?.data?.detail || "Unexpected error");
//       setConversations([]);
//     }
//   };

//   // ---------------- HANDLERS ----------------
//   const handleSearch = () => {
//     setSearchError("");
//     const trimmed = searchTerm.trim();
//     if (!trimmed) {
//       setSearchError("Please enter a Conversation ID or User ID.");
//       return;
//     }
//     if (trimmed.includes("@")) {
//       // Search by user ID
//       fetchUserConversations(1, trimmed);
//     } else {
//       // Search by conversation ID
//       fetchConversationById(trimmed);
//     }
//   };

//   const resetSearchFields = () => {
//     setSearchTerm("");
//     setSearchError("");
//     setConversationError("");
//     setSelectedConversation(null);
//     setShowModal(false);
//   };

//   const handleFilter = () => {
//     const hasFilters = !!(
//       startDate ||
//       endDate ||
//       filterOpportunity ||
//       filterToolOnly ||
//       filterUserType.toLowerCase() !== "all"
//     );
//     setIsFiltered(hasFilters);
//     setCurrentPage(1);
//     fetchAllConversations(1);
//   };

//   const resetFilterFields = () => {
//     setStartDate("");
//     setEndDate("");
//     setFilterOpportunity(false);
//     setFilterToolOnly(false);
//     setFilterUserType("all");
//     setIsFiltered(false);
//     setTotalConvos(0);
//     setCurrentPage(1);
//     fetchAllConversations(1);
//   };

//   const handlePageChange = (newPage) => {
//     const maxPage = Math.ceil(totalConvos / pageSize);
//     if (newPage > 0 && newPage <= maxPage) {
//       setCurrentPage(newPage);
//       fetchAllConversations(newPage);
//     }
//   };

//   const downloadExcelFile = async () => {
//     if (!startDate || !endDate) {
//       alert("Please select both From and To dates.");
//       return;
//     }
//     try {
//       const payload = { from_date: startDate, to_date: endDate };
//       const response = await axios.post(
//         `${BASE_URL}/v1/export_conversations`,
//         payload,
//         { responseType: "blob" }
//       );

//       const blob = new Blob([response.data], {
//         type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
//       });
//       const url = window.URL.createObjectURL(blob);
//       const a = document.createElement("a");
//       a.href = url;
//       a.download = `conversations_tcil_${startDate}_to_${endDate}.xlsx`;
//       document.body.appendChild(a);
//       a.click();
//       a.remove();
//     } catch (error) {
//       console.error("Download error:", error);
//       alert("Failed to export summary.");
//     }
//   };

//   // ---------------- EFFECTS ----------------
//   useEffect(() => {
//     fetchAllConversations(1);
//     // eslint-disable-next-line
//   }, []);

//   const totalPages = Math.ceil(totalConvos / pageSize);

//   // ---------------- RENDER ----------------
//   return (
//     <>
//       {/* Filters (channel fixed to ChatBot) */}
//       <div style={styles.filterBox}>
//         <h2 style={styles.groupTitle}>Filter by Date, Opportunity ID &amp; User Type</h2>
//         <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
//           <input
//             type="date"
//             style={styles.inputDate}
//             value={startDate}
//             onChange={(e) => setStartDate(e.target.value)}
//           />
//           <input
//             type="date"
//             style={styles.inputDate}
//             value={endDate}
//             onChange={(e) => setEndDate(e.target.value)}
//           />

//           <label style={{ color: "#f5f5f5", display: "flex", alignItems: "center", gap: "5px" }}>
//             <input
//               type="checkbox"
//               checked={filterOpportunity}
//               onChange={(e) => setFilterOpportunity(e.target.checked)}
//             />
//             Only with Opportunity ID
//           </label>

//           <label style={{ color: "#f5f5f5", display: "flex", alignItems: "center", gap: "5px" }}>
//             <input
//               type="checkbox"
//               checked={filterToolOnly}
//               onChange={(e) => setFilterToolOnly(e.target.checked)}
//             />
//             Only Tool Conversations
//           </label>

//           <select
//             style={styles.select}
//             value={filterUserType}
//             onChange={(e) => setFilterUserType(e.target.value)}
//           >
//             <option value="all">All Users</option>
//             <option value="registered">Registered</option>
//             <option value="guest">Guest</option>
//           </select>

//           <button style={styles.button} onClick={handleFilter}>Filter</button>
//           <button
//             style={{ ...styles.button, backgroundColor: "#f5f5f5", color: "#000" }}
//             onClick={resetFilterFields}
//           >
//             Reset Filters
//           </button>
//         </div>
//       </div>

//       {/* Export Button */}
//       {isFiltered && (
//         <div style={{ margin: "10px 0", textAlign: "right" }}>
//           <button style={styles.button} onClick={downloadExcelFile}>Export Summary to Excel</button>
//         </div>
//       )}

//       {/* Search */}
//       <div style={styles.filterBox}>
//         <h2 style={styles.groupTitle}>Search by Conversation ID or User ID</h2>
//         <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
//           <input
//             style={styles.inputSearch}
//             placeholder="Enter Conversation ID or User ID"
//             value={searchTerm}
//             onChange={(e) => setSearchTerm(e.target.value)}
//           />
//           <button style={styles.button} onClick={handleSearch}>Search</button>
//           <button
//             style={{ ...styles.button, backgroundColor: "#f5f5f5", color: "#000" }}
//             onClick={resetSearchFields}
//           >
//             Reset
//           </button>
//         </div>
//         {searchError && <div style={{ color: "red", marginTop: "10px" }}>{searchError}</div>}
//         {conversationError && <div style={{ color: "red", marginTop: "10px" }}>{conversationError}</div>}
//         {userConvoError && <div style={{ color: "red", marginTop: "10px" }}>{userConvoError}</div>}
//       </div>

//       {/* Conversations */}
//       <div style={styles.conversationsSection}>
//         <div style={styles.sectionTitle}>
//           {isFiltered ? "Filtered Conversations" : `All Conversations — ${CHANNEL_KEY}`} ({totalConvos})
//         </div>

//         <div style={styles.tableHeader}>
//           <div style={styles.headerId}>ID</div>
//           <div style={styles.headerName}>Chat Name</div>
//           <div style={styles.headerOpp}>Opportunity ID</div>
//           <div style={styles.headerMod}>Last Modified</div>
//           <div style={styles.headerBtn}></div>
//         </div>

//         {isLoading || totalConvos === 0 ? (
//           <div style={{ marginTop: "20px" }}>{isLoading ? "Loading..." : "No conversations found."}</div>
//         ) : (
//           <>
//             {conversations.map((conv, idx) => (
//               <div key={idx} style={styles.rowContainer}>
//                 <div style={styles.rowId}>{conv.conversationId}</div>
//                 <div style={styles.rowName}>{conv.chat_name}</div>
//                 <div style={styles.rowOpp}>
//                   {conv.opportunity_id && conv.opportunity_id.trim() !== "" ? (
//                     <span style={styles.greenDot} title="Opportunity ID exists"></span>
//                   ) : (
//                     <span style={styles.redDot} title="Missing Opportunity ID"></span>
//                   )}
//                 </div>
//                 <div style={styles.rowMod}>
//                   {conv.chat_modified
//                     ? new Date(`${conv.chat_modified}Z`).toLocaleString("en-US", {
//                         timeZone: "Asia/Kolkata",
//                       })
//                     : "N/A"}
//                 </div>
//                 <div style={styles.rowBtn}>
//                   <button
//                     style={{ ...styles.button, padding: "6px 12px", backgroundColor: "#0056B3", fontSize: "14px" }}
//                     onClick={() => fetchConversationById(conv.conversationId)}
//                   >
//                     View Details
//                   </button>
//                 </div>
//               </div>
//             ))}

//             {totalConvos > pageSize && (
//               <div style={styles.paginationRow}>
//                 <button
//                   disabled={currentPage === 1}
//                   onClick={() => handlePageChange(currentPage - 1)}
//                   style={styles.button}
//                 >
//                   Prev
//                 </button>
//                 <span>
//                   Page {currentPage} of {Math.ceil(totalConvos / pageSize)}
//                 </span>
//                 <button
//                   disabled={currentPage === Math.ceil(totalConvos / pageSize)}
//                   onClick={() => handlePageChange(currentPage + 1)}
//                   style={styles.button}
//                 >
//                   Next
//                 </button>
//               </div>
//             )}
//           </>
//         )}
//       </div>

//       {/* Modal */}
//       {showModal && selectedConversation && (
//         <div style={styles.modalOverlay}>
//           <div style={styles.modalContent}>
//             <button
//               style={styles.closeButton}
//               onClick={() => {
//                 setSelectedConversation(null);
//                 setShowModal(false);
//               }}
//             >
//               X
//             </button>

//             <div style={styles.modalHeader}>
//               <div style={styles.viewToggle}>
//                 <button
//                   style={{ ...styles.toggleButton, ...(viewMode === "chat" ? styles.activeToggle : {}) }}
//                   onClick={() => setViewMode("chat")}
//                 >
//                   Chat View
//                 </button>
//                 <button
//                   style={{ ...styles.toggleButton, ...(viewMode === "raw" ? styles.activeToggle : {}) }}
//                   onClick={() => setViewMode("raw")}
//                 >
//                   JSON View
//                 </button>
//               </div>
//               <h2 style={{ marginTop: 0 }}>
//                 Conversation Details:&nbsp;
//                 <span style={{ backgroundColor: "#eee", padding: "4px 8px", borderRadius: "4px", fontSize: "0.85em" }}>
//                   {selectedConversation.conversationId}
//                 </span>
//               </h2>
//             </div>

//             <div style={styles.modalBody}>
//               {viewMode === "raw" ? (
//                 <pre style={styles.jsonViewer}>
//                   {JSON.stringify(selectedConversation, null, 2)}
//                 </pre>
//               ) : (
//                 <div style={styles.chatContainer}>
//                   {selectedConversation.conversation?.map((msg, index) => {
//                     let content = msg.content;
//                     let packages = [];
//                     try {
//                       const parsed = JSON.parse(msg.content);
//                       if (parsed.message) content = parsed.message;
//                       if (msg.type === "list" && Array.isArray(parsed.message)) {
//                         packages = parsed.message;
//                       }
//                     } catch {
//                       // Fallback to raw content if parsing fails
//                     }
//                     if (msg.role === "tool") {
//                       content = "Here's the list of packages available with Thomas Cook";
//                     }
//                     return (
//                       <div
//                         key={index}
//                         style={{
//                           ...styles.messageBubble,
//                           ...(msg.role === "user" ? styles.userMessage : styles.otherMessage),
//                         }}
//                       >
//                         {packages.length > 0 ? (
//                           <div style={styles.packageGrid}>
//                             {packages.map((pkg, idx) => (
//                               <div key={idx} style={styles.packageCard}>
//                                 <img
//                                   src={pkg.tumbnail}
//                                   alt={pkg.title}
//                                   style={styles.packageImage}
//                                   onError={(e) => { e.target.src = "https://via.placeholder.com/150"; }}
//                                 />
//                                 <div style={styles.packageContent}>
//                                   <h4 style={{ margin: "0 0 8px 0" }}>{pkg.title}</h4>
//                                   <p style={{ margin: "4px 0" }}>{pkg.days}</p>
//                                   <p style={{ margin: "4px 0", fontWeight: "bold" }}>{pkg.price}</p>
//                                 </div>
//                               </div>
//                             ))}
//                           </div>
//                         ) : (
//                           <div style={styles.multilineContent}>{content}</div>
//                         )}

//                         <div
//                           style={{
//                             fontSize: "0.8em",
//                             marginTop: "8px",
//                             opacity: 0.7,
//                             textAlign: msg.role === "user" ? "right" : "left",
//                           }}
//                         >
//                           {msg.chat_time
//                             ? new Date(
//                                 msg.chat_time +
//                                   (msg.chat_time.endsWith("Z") || msg.chat_time.includes("+") ? "" : "Z")
//                               ).toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata" })
//                             : ""}
//                         </div>
//                       </div>
//                     );
//                   })}
//                 </div>
//               )}
//             </div>
//           </div>
//         </div>
//       )}
//     </>
//   );
// }
