// // TeeCeeChatbot.js
// import React, { useState, useEffect } from "react";
// import { useNavigate } from "react-router-dom";
// import axios from "axios";

// // ---------------------------------------------
// // Axios defaults (server should ideally manage CORS).
// // ---------------------------------------------
// axios.defaults.withCredentials = false;
// axios.defaults.headers.common["Access-Control-Allow-Origin"] = "*";

// // Update BASE_URL as needed.
// const BASE_URL = "https://travbridge.atirath.com";

// function TeeCeeChatbot() {
//   // ---------------- STATE VARIABLES ----------------
//   // Filter: tool type, optional date, and new filters for opportunity and user type.
//   const [toolType, setToolType] = useState("ChatBot");
//   const [startDate, setStartDate] = useState("");
//   const [endDate, setEndDate] = useState("");
//   const [isFiltered, setIsFiltered] = useState(false);


//   // NOTE: For your backend example, pass "opportunity_id":"true" (as a string),
//   // so we store a boolean here but convert to string in the API call.
//   const [filterOpportunity, setFilterOpportunity] = useState(false); // Checkbox
//   const [filterUserType, setFilterUserType] = useState("all"); // "all", "registered", "guest"
//   const [filterToolOnly, setFilterToolOnly] = useState(false);

//   // Conversation summaries from the backend (server-side pagination).
//   const [conversations, setConversations] = useState([]);
//   const [totalConvos, setTotalConvos] = useState(0);

//   // Loading indicator.
//   const [isLoading, setIsLoading] = useState(false);

//   // Search state for unified search (Conversation ID or User ID).
//   const [searchTerm, setSearchTerm] = useState("");
//   const [searchError, setSearchError] = useState("");
//   const [userConvoError, setUserConvoError] = useState("");

//   // Full conversation details for modal view.
//   const [selectedConversation, setSelectedConversation] = useState(null);
//   const [viewMode, setViewMode] = useState("chat"); // "chat" or "raw"
//   const [showModal, setShowModal] = useState(false);

//   // Error message for full conversation fetch.
//   const [conversationError, setConversationError] = useState("");

//   // Pagination: current page (page size is fixed at 10).
//   const [currentPage, setCurrentPage] = useState(1);
//   const pageSize = 10;

//   // Router navigation.
//   const navigate = useNavigate();
//   const goBack = () => navigate("/");

//   // ---------------- STYLES ----------------
//   const styles = {
//     container: {
//       display: "flex",
//       flexDirection: "column",
//       gap: "20px",
//       padding: "20px",
//       fontFamily: "'Poppins', sans-serif",
//       backgroundColor: "#333333",
//       minHeight: "100vh",
//       color: "#f5f5f5",
//       boxSizing: "border-box",
//     },
//     button: {
//       padding: "10px 20px",
//       borderRadius: "8px",
//       border: "none",
//       backgroundColor: "#ED1C24",
//       color: "white",
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
//     combinedRow: {
//       display: "flex",
//       justifyContent: "space-between",
//       alignItems: "flex-start",
//       flexWrap: "wrap",
//       gap: "20px",
//     },
//     leftGroup: {
//       flex: "1 1 60%",
//       minWidth: "320px",
//     },
//     rightGroup: {
//       flex: "1 1 35%",
//       minWidth: "320px",
//       marginTop: "56px", // To align with the Back+Title block.
//     },
//     filterBox: {
//       backgroundColor: "#333333",
//       padding: "10px",
//       borderRadius: "8px",
//     },
//     groupTitle: {
//       fontSize: "18px",
//       fontWeight: "600",
//       marginBottom: "10px",
//       color: "#f5f5f5",
//     },
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
//     // For text / checkbox / etc.
//     inputText: {
//       padding: "8px 12px",
//       borderRadius: "8px",
//       border: "1px solid #e0e0e0",
//       backgroundColor: "#f5f5f5",
//       color: "#000",
//       minWidth: "150px",
//       marginRight: "10px",
//     },
//     conversationsSection: {
//       marginTop: "20px",
//     },
//     sectionTitle: {
//       fontSize: "18px",
//       fontWeight: "600",
//       marginBottom: "10px",
//       color: "#f5f5f5",
//     },
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

//     paginationRow: {
//       display: "flex",
//       alignItems: "center",
//       gap: "10px",
//       marginTop: "10px",
//     },

//     greenDot: {
//       display: "inline-block",
//       width: "10px",
//       height: "10px",
//       borderRadius: "50%",
//       backgroundColor: "green",
//     },
//     redDot: {
//       display: "inline-block",
//       width: "10px",
//       height: "10px",
//       borderRadius: "50%",
//       backgroundColor: "red",
//     },

//     modalOverlay: {
//       position: "fixed",
//       top: 0,
//       left: 0,
//       width: "100vw",
//       height: "100vh",
//       backgroundColor: "rgba(0,0,0,0.5)",
//       display: "flex",
//       justifyContent: "center",
//       alignItems: "center",
//       zIndex: 9999,
//     },
//     modalContent: {
//       backgroundColor: "#fff",
//       width: "80%",
//       maxHeight: "80%",
//       overflowY: "auto",
//       borderRadius: "8px",
//       padding: "20px",
//       position: "relative",
//       color: "#000",
//       display: "flex",
//       flexDirection: "column",
//     },
//     closeButton: {
//       position: "absolute",
//       top: "10px",
//       right: "10px",
//       backgroundColor: "#eee",
//       border: "none",
//       borderRadius: "50%",
//       width: "32px",
//       height: "32px",
//       cursor: "pointer",
//       fontWeight: "bold",
//     },
//     viewToggle: {
//       display: "flex",
//       gap: "10px",
//       marginBottom: "15px",
//     },
//     toggleButton: {
//       padding: "8px 16px",
//       borderRadius: "20px",
//       border: "1px solid #cbd5e1",
//       backgroundColor: "#fff",
//       cursor: "pointer",
//       transition: "all 0.2s",
//     },
//     activeToggle: {
//       backgroundColor: "#333333",
//       color: "#fff",
//       borderColor: "#3b82f6",
//     },
//     chatContainer: {
//       display: "flex",
//       flexDirection: "column",
//       gap: "15px",
//     },
//     messageBubble: {
//       maxWidth: "80%",
//       padding: "15px",
//       borderRadius: "20px",
//       margin: "5px 0",
//     },
//     userMessage: {
//       alignSelf: "flex-end",
//       backgroundColor: "#333333",
//       color: "white",
//     },
//     otherMessage: {
//       alignSelf: "flex-start",
//       backgroundColor: "#f1f5f9",
//       color: "#1e293b",
//     },
//     packageGrid: {
//       display: "grid",
//       gridTemplateColumns: "repeat(auto-fill, minmax(250px, 1fr))",
//       gap: "20px",
//       marginTop: "15px",
//     },
//     packageCard: {
//       border: "1px solid #e2e8f0",
//       borderRadius: "12px",
//       overflow: "hidden",
//       backgroundColor: "#fff",
//       color: "#000",
//     },
//     packageImage: {
//       width: "100%",
//       height: "150px",
//       objectFit: "cover",
//     },
//     packageContent: {
//       padding: "15px",
//     },
//     modalHeader: {
//       position: "sticky",
//       top: 0,
//       backgroundColor: "#fff",
//       zIndex: 100,
//       paddingBottom: "10px",
//     },
//     modalBody: {
//       flex: 1,
//       overflowY: "auto",
//       paddingTop: "10px",
//     },
//     jsonViewer: {
//       backgroundColor: "#f8fafc",
//       padding: "20px",
//       borderRadius: "8px",
//       whiteSpace: "pre-wrap",
//       wordWrap: "break-word",
//       overflowX: "auto",
//     },
//     multilineContent: {
//       whiteSpace: "pre-wrap",
//       wordWrap: "break-word",
//       margin: 0,
//     },
//   };

//   // ---------------- API CALLS ----------------
//   const fetchAllConversations = async (page = 1, channel = toolType) => {
//     setIsLoading(true);
//     try {
//       const filters = {
//         chat_channel: channel,
//         count: pageSize,
//         page: page,
//       };
//       if (startDate) filters.chat_started_from = startDate;
//       if (endDate) filters.chat_started_to = endDate;

//       // If the user checked "Only with Opportunity ID", pass "true" (as string).
//       if (filterOpportunity) {
//         filters.opportunity_id = "true";
//       }
//       if (filterToolOnly) {
//         filters.only_tool_conversations = true;
//       }


//       // If user type is not "all", pass e.g. "registered" or "guest".
//       if (filterUserType.toLowerCase() !== "all") {
//         filters.userId = filterUserType;
//       }
//       const response = await axios.post(`${BASE_URL}/sotc/SOTC_get_all_conversations`, filters);
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
//       const response = await axios.post(`${BASE_URL}/sotc/SOTC_get_conversation`, payload);
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

//   // For user-specific searches (client-side slice).
//   const fetchUserConversations = async (requestedPage = 1, userId) => {
//     try {
//       setUserConvoError("");
//       const payload = { userId };
//       const response = await axios.post(`${BASE_URL}/sotc/SOTC_get_conversation_summaries`, payload);
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
//     const hasFilters = !!(startDate || endDate || filterOpportunity || filterUserType.toLowerCase() !== "all");
//     setIsFiltered(hasFilters);
//     setCurrentPage(1);
//     fetchAllConversations(1, toolType);
//   };


//   const resetFilterFields = () => {
//     setToolType("ChatBot");
//     setStartDate("");
//     setEndDate("");
//     setFilterOpportunity(false);
//     setFilterUserType("all");
//     setIsFiltered(false);
//     setTotalConvos(0);
//     setCurrentPage(1);
//     fetchAllConversations(1, "ChatBot");
//   };

//   const handlePageChange = (newPage) => {
//     const maxPage = Math.ceil(totalConvos / pageSize);
//     if (newPage > 0 && newPage <= maxPage) {
//       setCurrentPage(newPage);
//       fetchAllConversations(newPage, toolType);
//     }
//   };

//   const downloadExcelFile = async () => {
//     if (!startDate || !endDate) {
//       alert("Please select both From and To dates.");
//       return;
//     }

//     try {
//       const payload = {
//         from_date: startDate,
//         to_date: endDate,
//       };

//       const response = await axios.post(
//         "https://travbridge.atirath.com/sotc/SOTC_export_conversations",
//         payload,
//         { responseType: "blob" }
//       );

//       const blob = new Blob([response.data], {
//         type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
//       });

//       const url = window.URL.createObjectURL(blob);
//       const a = document.createElement("a");
//       a.href = url;
//       a.download = `conversations_sotc_${startDate}_to_${endDate}.xlsx`;
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
//     fetchAllConversations(1, toolType);
//     // eslint-disable-next-line
//   }, []);

//   const totalPages = Math.ceil(totalConvos / pageSize);

//   // ---------------- RENDER ----------------
//   return (
//     <div style={styles.container}>
//       {/* Combined Header and Filters */}
//       <div style={styles.combinedRow}>
//         {/* Left Group */}
//         <div style={styles.leftGroup}>
//           <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "10px" }}>
//             <button
//               style={{ ...styles.button, backgroundColor: "#f5f5f5", color: "#000" }}
//               onClick={goBack}
//             >
//               &larr; Back
//             </button>
//             <h1 style={styles.title}>Conversation Dashboard</h1>
//           </div>
//           {/* Filter Box */}
//           <div style={styles.filterBox}>
//             <h2 style={styles.groupTitle}>
//               Filter by Tool Type, Date, Opportunity ID &amp; User Type
//             </h2>
//             <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
//               <select
//                 style={styles.select}
//                 value={toolType}
//                 onChange={(e) => setToolType(e.target.value)}
//               >
//                 <option value="ChatBot">ChatBot</option>
//               </select>
//               <input
//                 type="date"
//                 style={styles.inputDate}
//                 value={startDate}
//                 onChange={e => setStartDate(e.target.value)}
//               />
//               <input
//                 type="date"
//                 style={styles.inputDate}
//                 value={endDate}
//                 onChange={e => setEndDate(e.target.value)}
//               />

//               {/* Opportunity ID checkbox */}
//               <label style={{ color: "#f5f5f5", display: "flex", alignItems: "center", gap: "5px" }}>
//                 <input
//                   type="checkbox"
//                   checked={filterOpportunity}
//                   onChange={(e) => setFilterOpportunity(e.target.checked)}
//                 />
//                 Only with Opportunity ID
//               </label>
//               {/* Only Tool Conversations checkbox */}
//               <label style={{ color: "#f5f5f5", display: "flex", alignItems: "center", gap: "5px" }}>
//                 <input
//                   type="checkbox"
//                   checked={filterToolOnly}
//                   onChange={(e) => setFilterToolOnly(e.target.checked)}
//                 />
//                 Only Tool Conversations
//               </label>

//               {/* User type dropdown */}
//               <select
//                 style={styles.select}
//                 value={filterUserType}
//                 onChange={(e) => setFilterUserType(e.target.value)}
//               >
//                 <option value="all">All Users</option>
//                 <option value="registered">Registered</option>
//                 <option value="guest">Guest</option>
//               </select>
//               <button style={styles.button} onClick={handleFilter}>
//                 Filter
//               </button>
//               <button
//                 style={{ ...styles.button, backgroundColor: "#f5f5f5", color: "#000" }}
//                 onClick={resetFilterFields}
//               >
//                 Reset Filters
//               </button>
//             </div>
//           </div>
//         </div>

//         {/* Right Group: Search */}
//         <div style={styles.rightGroup}>
//           <div style={styles.filterBox}>
//             <h2 style={styles.groupTitle}>Search by Conversation ID or User ID</h2>
//             <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
//               <input
//                 style={styles.inputSearch}
//                 placeholder="Enter Conversation ID or User ID"
//                 value={searchTerm}
//                 onChange={(e) => setSearchTerm(e.target.value)}
//               />
//               <button style={styles.button} onClick={handleSearch}>
//                 Search
//               </button>
//               <button
//                 style={{ ...styles.button, backgroundColor: "#f5f5f5", color: "#000" }}
//                 onClick={resetSearchFields}
//               >
//                 Reset
//               </button>
//             </div>
//             {searchError && (
//               <div style={{ color: "red", marginTop: "10px" }}>{searchError}</div>
//             )}
//             {conversationError && (
//               <div style={{ color: "red", marginTop: "10px" }}>{conversationError}</div>
//             )}
//             {userConvoError && (
//               <div style={{ color: "red", marginTop: "10px" }}>{userConvoError}</div>
//             )}
//           </div>
//         </div>
//       </div>

//       {/* Export Button */}
//       {isFiltered && (
//         <div style={{ marginBottom: "10px", textAlign: "right" }}>
//           <button style={styles.button} onClick={downloadExcelFile}>
//             Export Summary to Excel
//           </button>
//         </div>
//       )}

//       {/* Recent Conversations Section */}
//       <div style={styles.conversationsSection}>
//         <div style={styles.sectionTitle}>
//           {isFiltered ? "Filtered Conversations" : "All Conversations"} ({totalConvos})
//         </div>

//         {/* Table-like Header Row */}
//         <div style={styles.tableHeader}>
//           <div style={styles.headerId}>ID</div>
//           <div style={styles.headerName}>Chat Name</div>
//           <div style={styles.headerOpp}>Opportunity ID</div>
//           <div style={styles.headerMod}>Last Modified</div>
//           <div style={styles.headerBtn}></div>
//         </div>

//         {isLoading || totalConvos === 0 ? (
//           <div style={{ marginTop: "20px" }}>Loading...</div>
//         ) : (
//           <>
//             {conversations.length > 0 ? (
//               <>
//                 {conversations.map((conv, idx) => (
//                   <div key={idx} style={styles.rowContainer}>
//                     <div style={styles.rowId}>{conv.conversationId}</div>
//                     <div style={styles.rowName}>{conv.chat_name}</div>
//                     <div style={styles.rowOpp}>
//                       {conv.opportunity_id && conv.opportunity_id.trim() !== "" ? (
//                         <span style={styles.greenDot} title="Opportunity ID exists"></span>
//                       ) : (
//                         <span style={styles.redDot} title="Missing Opportunity ID"></span>
//                       )}
//                     </div>
//                     <div style={styles.rowMod}>
//                       {conv.chat_modified
//                         ? new Date(`${conv.chat_modified}Z`).toLocaleString("en-US", {
//                           timeZone: "Asia/Kolkata",
//                         })
//                         : "N/A"}
//                     </div>
//                     <div style={styles.rowBtn}>
//                       <button
//                         style={{
//                           ...styles.button,
//                           padding: "6px 12px",
//                           backgroundColor: "#333333",
//                           fontSize: "14px",
//                         }}
//                         onClick={() => fetchConversationById(conv.conversationId)}
//                       >
//                         View Details
//                       </button>
//                     </div>
//                   </div>
//                 ))}
//                 {totalConvos > pageSize && (
//                   <div style={styles.paginationRow}>
//                     <button
//                       disabled={currentPage === 1}
//                       onClick={() => handlePageChange(currentPage - 1)}
//                       style={styles.button}
//                     >
//                       Prev
//                     </button>
//                     <span>
//                       Page {currentPage} of {Math.ceil(totalConvos / pageSize)}
//                     </span>
//                     <button
//                       disabled={currentPage === Math.ceil(totalConvos / pageSize)}
//                       onClick={() => handlePageChange(currentPage + 1)}
//                       style={styles.button}
//                     >
//                       Next
//                     </button>
//                   </div>
//                 )}
//               </>
//             ) : (
//               <div style={{ marginTop: "10px" }}>No conversations found.</div>
//             )}
//           </>
//         )}
//       </div>

//       {/* Modal Overlay for Conversation Details */}
//       {showModal && selectedConversation && (
//         <div style={styles.modalOverlay}>
//           <div style={styles.modalContent}>
//             {/* Sticky Header */}
//             <div style={styles.modalHeader}>
//               <button
//                 style={styles.closeButton}
//                 onClick={() => {
//                   setSelectedConversation(null);
//                   setShowModal(false);
//                 }}
//               >
//                 X
//               </button>
//               <div style={styles.viewToggle}>
//                 <button
//                   style={{
//                     ...styles.toggleButton,
//                     ...(viewMode === "chat" ? styles.activeToggle : {}),
//                   }}
//                   onClick={() => setViewMode("chat")}
//                 >
//                   Chat View
//                 </button>
//                 <button
//                   style={{
//                     ...styles.toggleButton,
//                     ...(viewMode === "raw" ? styles.activeToggle : {}),
//                   }}
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
//                       // Fallback to raw content if parsing fails.
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
//                                   onError={(e) => {
//                                     e.target.src = "https://via.placeholder.com/150";
//                                   }}
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
//                               msg.chat_time +
//                               (msg.chat_time.endsWith("Z") || msg.chat_time.includes("+")
//                                 ? ""
//                                 : "Z")
//                             ).toLocaleTimeString("en-IN", {
//                               timeZone: "Asia/Kolkata",
//                             })
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
//     </div>
//   );
// }

// export default TeeCeeChatbot;
