// TeeCeeChatbot.js
import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";

// ---------------------------------------------
// Axios defaults (server should ideally manage CORS).
// ---------------------------------------------
axios.defaults.withCredentials = false;
axios.defaults.headers.common["Access-Control-Allow-Origin"] = "*";

// Update BASE_URL as needed.
const BASE_URL = "https://travbridge.atirath.com";

function TeeCeeChatbot() {
  // ---------------- STATE VARIABLES ----------------
  const [toolType, setToolType] = useState("ChatBot");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [isFiltered, setIsFiltered] = useState(false);

  const [filterOpportunity, setFilterOpportunity] = useState(false); // Checkbox
  const [filterUserType, setFilterUserType] = useState("all"); // "all", "registered", "guest"
  const [filterToolOnly, setFilterToolOnly] = useState(false);

  const [conversations, setConversations] = useState([]);
  const [totalConvos, setTotalConvos] = useState(0);
  const [isLoading, setIsLoading] = useState(false);

  const [searchTerm, setSearchTerm] = useState("");
  const [searchError, setSearchError] = useState("");
  const [userConvoError, setUserConvoError] = useState("");

  const [selectedConversation, setSelectedConversation] = useState(null);
  const [viewMode, setViewMode] = useState("chat"); // "chat" or "raw"
  const [showModal, setShowModal] = useState(false);
  const [conversationError, setConversationError] = useState("");

  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 10;

  const navigate = useNavigate();
  const goBack = () => navigate("/");

  // ---------------- ROLE COLORS (aligned with the screenshot palette) ----------------
  const palette = {
    primaryBlue: "#0056B3",      // reused for AI bubble
    aaSurfaceBg: "#EEF2FF",      // Agent Assist panel background
    aaSurfaceBorder: "#CBD5FF",
    customerBg: "#DCEBFF",       // Customer (left)
    agentBg: "#A9C4FF",          // Agent (right)
    aiBg: "#0056B3",             // AI (right)
    darkText: "#0f172a",
    lightText: "#ffffff",
    subtleText: "#64748b",
    bubbleBorder: "#c7d2fe",
  };

  // ---------------- STYLES ----------------
  const styles = {
    container: {
      display: "flex",
      flexDirection: "column",
      gap: "20px",
      padding: "20px",
      fontFamily: "'Poppins', sans-serif",
      backgroundColor: palette.primaryBlue,
      minHeight: "100vh",
      color: "#f5f5f5",
      boxSizing: "border-box",
    },
    button: {
      padding: "10px 20px",
      borderRadius: "8px",
      border: "none",
      backgroundColor: "#0044a3",
      color: "white",
      cursor: "pointer",
      transition: "all 0.2s",
      marginRight: "10px",
    },
    title: {
      fontSize: "36px",
      fontWeight: "bold",
      color: "#f5f5f5",
      margin: 0,
    },
    combinedRow: {
      display: "flex",
      justifyContent: "space-between",
      alignItems: "flex-start",
      flexWrap: "wrap",
      gap: "20px",
    },
    leftGroup: { flex: "1 1 60%", minWidth: "320px" },
    rightGroup: { flex: "1 1 35%", minWidth: "320px", marginTop: "56px" },
    filterBox: { backgroundColor: palette.primaryBlue, padding: "10px", borderRadius: "8px" },
    groupTitle: { fontSize: "18px", fontWeight: "600", marginBottom: "10px", color: "#f5f5f5" },
    select: {
      padding: "8px",
      borderRadius: "8px",
      border: "1px solid #e0e0e0",
      backgroundColor: "#f5f5f5",
      color: "#000",
      minWidth: "120px",
      marginRight: "10px",
    },
    inputDate: {
      padding: "8px",
      borderRadius: "8px",
      border: "1px solid #e0e0e0",
      backgroundColor: "#f5f5f5",
      color: "#000",
      marginRight: "10px",
    },
    inputSearch: {
      padding: "8px 12px",
      borderRadius: "8px",
      border: "1px solid #e0e0e0",
      backgroundColor: "#f5f5f5",
      color: "#000",
      minWidth: "220px",
      marginRight: "10px",
    },
    inputText: {
      padding: "8px 12px",
      borderRadius: "8px",
      border: "1px solid #e0e0e0",
      backgroundColor: "#f5f5f5",
      color: "#000",
      minWidth: "150px",
      marginRight: "10px",
    },
    conversationsSection: { marginTop: "20px" },
    sectionTitle: { fontSize: "18px", fontWeight: "600", marginBottom: "10px", color: "#f5f5f5" },
    tableHeader: {
      display: "flex",
      fontWeight: "bold",
      color: "#fff",
      marginBottom: "8px",
      padding: "10px 15px",
      textTransform: "uppercase",
      borderBottom: "1px solid #ccc",
    },
    headerId: { flex: "1 1 15%", minWidth: "60px", textAlign: "left" },
    headerName: { flex: "1 1 35%", minWidth: "150px", textAlign: "left" },
    headerOpp: { flex: "1 1 15%", minWidth: "80px", textAlign: "center" },
    headerMod: { flex: "1 1 25%", minWidth: "120px", textAlign: "left" },
    headerBtn: { flex: "1 1 10%", textAlign: "right" },

    rowContainer: {
      display: "flex",
      alignItems: "center",
      backgroundColor: "#f8fafc",
      color: "#1e293b",
      borderRadius: "8px",
      padding: "10px 15px",
      marginBottom: "10px",
      border: "1px solid #e2e8f0",
    },
    rowId: { flex: "1 1 15%", minWidth: "60px", textAlign: "left" },
    rowName: { flex: "1 1 35%", minWidth: "150px", textAlign: "left" },
    rowOpp: { flex: "1 1 15%", minWidth: "80px", textAlign: "center" },
    rowMod: { flex: "1 1 25%", minWidth: "120px", textAlign: "left" },
    rowBtn: { flex: "1 1 10%", textAlign: "right" },

    paginationRow: { display: "flex", alignItems: "center", gap: "10px", marginTop: "10px" },
    greenDot: { display: "inline-block", width: "10px", height: "10px", borderRadius: "50%", backgroundColor: "green" },
    redDot: { display: "inline-block", width: "10px", height: "10px", borderRadius: "50%", backgroundColor: "red" },

    modalOverlay: {
      position: "fixed",
      top: 0,
      left: 0,
      width: "100vw",
      height: "100vh",
      backgroundColor: "rgba(0,0,0,0.5)",
      display: "flex",
      justifyContent: "center",
      alignItems: "center",
      zIndex: 9999,
    },
    modalContent: {
      backgroundColor: "#fff",
      width: "80%",
      maxHeight: "80%",
      overflowY: "auto",
      borderRadius: "12px",
      padding: "20px",
      position: "relative",
      color: "#000",
      display: "flex",
      flexDirection: "column",
      boxShadow: "0 10px 30px rgba(0,0,0,0.2)",
    },
    closeButton: {
      position: "absolute",
      top: "10px",
      right: "10px",
      backgroundColor: "#eee",
      border: "none",
      borderRadius: "50%",
      width: "32px",
      height: "32px",
      cursor: "pointer",
      fontWeight: "bold",
    },
    viewToggle: { display: "flex", gap: "10px", marginBottom: "15px" },
    toggleButton: {
      padding: "8px 16px",
      borderRadius: "20px",
      border: "1px solid #cbd5e1",
      backgroundColor: "#fff",
      cursor: "pointer",
      transition: "all 0.2s",
    },
    activeToggle: { backgroundColor: palette.primaryBlue, color: "#fff", borderColor: "#3b82f6" },

    // Default (ChatBot) chat
    chatContainer: { display: "flex", flexDirection: "column", gap: "15px" },
    messageBubble: { maxWidth: "80%", padding: "15px", borderRadius: "20px", margin: "5px 0" },
    userMessage: { 
  alignSelf: "flex-end", 
  backgroundColor: palette.userBubbleBg,  // Light background
  color: palette.userBubbleText,          // Dark text
  border: `1px solid ${palette.primaryBlue}`,  // Optional: blue border for style
  borderRadius: "20px 20px 4px 20px",  // Sharp top-left (sent message style)
},
    otherMessage: { alignSelf: "flex-start", backgroundColor: "#f1f5f9", color: "#1e293b" },

    // Package cards
    packageGrid: {
      display: "grid",
      gridTemplateColumns: "repeat(auto-fill, minmax(250px, 1fr))",
      gap: "20px",
      marginTop: "15px",
    },
    packageCard: {
      border: "1px solid #e2e8f0",
      borderRadius: "12px",
      overflow: "hidden",
      backgroundColor: "#fff",
      color: "#000",
    },
    packageImage: { width: "100%", height: "150px", objectFit: "cover" },
    packageContent: { padding: "15px" },

    modalHeader: { position: "sticky", top: 0, backgroundColor: "#fff", zIndex: 100, paddingBottom: "10px" },
    modalBody: { flex: 1, overflowY: "auto", paddingTop: "10px" },
    jsonViewer: {
      backgroundColor: "#f8fafc",
      padding: "20px",
      borderRadius: "8px",
      whiteSpace: "pre-wrap",
      wordWrap: "break-word",
      overflowX: "auto",
    },
    multilineContent: { whiteSpace: "pre-wrap", wordWrap: "break-word", margin: 0 },

    // ---------- Agent Assist specific ----------
    aaSurface: {
      backgroundColor: palette.aaSurfaceBg,
      border: `1px solid ${palette.aaSurfaceBorder}`,
      borderRadius: "14px",
      padding: "16px",
    },
    aaRow: { display: "flex", alignItems: "flex-end", gap: "10px", width: "100%" },
    aaRowLeft: { justifyContent: "flex-start" },
    aaRowRight: { justifyContent: "flex-end" },
    aaAvatar: {
      width: "28px",
      height: "28px",
      borderRadius: "50%",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      fontSize: "12px",
      fontWeight: 700,
      color: "#1e293b",
      backgroundColor: "#E2E8FF",
      border: `1px solid ${palette.bubbleBorder}`,
      flex: "0 0 28px",
    },
    aaMeta: {
      fontSize: "12px",
      color: palette.subtleText,
      margin: "0 2px 4px 2px",
      display: "flex",
      alignItems: "center",
      gap: "6px",
    },
    aaBubbleBase: {
      maxWidth: "75%",
      padding: "12px 14px",
      borderRadius: "16px",
      border: `1px solid ${palette.bubbleBorder}`,
    },
    aaBubbleCustomer: {
      backgroundColor: palette.customerBg,
      color: palette.darkText,
      borderTopLeftRadius: "6px",
    },
    aaBubbleAgent: {
      backgroundColor: palette.agentBg,
      color: palette.darkText,
      borderTopRightRadius: "6px",
    },
    aaBubbleAI: {
      backgroundColor: palette.aiBg,
      color: palette.lightText,
      borderTopRightRadius: "6px",
    },
    aaTimeLeft: { fontSize: "12px", color: palette.subtleText, marginTop: "6px", textAlign: "left" },
    aaTimeRight: { fontSize: "12px", color: palette.subtleText, marginTop: "6px", textAlign: "right" },

    // header badges
    badgeRow: { display: "flex", gap: "8px", flexWrap: "wrap", alignItems: "center" },
    badge: {
      backgroundColor: "#eee",
      padding: "4px 8px",
      borderRadius: "4px",
      fontSize: "0.85em",
      color: "#111",
    },
    badgeLabel: { fontWeight: 600, marginRight: 4 },
  };

  // ---------------- API CALLS ----------------
  const fetchAllConversations = async (page = 1, channel = toolType) => {
    setIsLoading(true);
    try {
      const filters = { chat_channel: channel, count: pageSize, page };
      if (startDate) filters.chat_started_from = startDate;
      if (endDate) filters.chat_started_to = endDate;
      if (filterOpportunity) filters.opportunity_id = "true";
      if (filterToolOnly) filters.only_tool_conversations = true;
      if (filterUserType.toLowerCase() !== "all") filters.userId = filterUserType;

      const response = await axios.post(`${BASE_URL}/v1/get_all_conversations`, filters);
      if (response.data.status === "success") {
        setTotalConvos(response.data.total);
        setCurrentPage(response.data.page);
        setConversations(response.data.conversations || []);
      } else {
        setConversations([]);
        setTotalConvos(0);
      }
    } catch (error) {
      console.error("Error fetching all conversations:", error);
      setConversations([]);
      setTotalConvos(0);
    } finally {
      setIsLoading(false);
    }
  };

  const fetchConversationById = async (convId) => {
    try {
      setViewMode("chat");
      const payload = { conversationId: convId };
      const response = await axios.post(`${BASE_URL}/v1/get_conversation`, payload);
      if (response.data.status === "success") {
        setSelectedConversation(response.data.conversation);
        setShowModal(true);
      } else {
        setConversationError(response.data.message || "No conversation found.");
      }
    } catch (error) {
      console.error("Error fetching conversation:", error);
      setConversationError(error.response?.data?.detail || "Unexpected error");
    }
  };

  const fetchUserConversations = async (requestedPage = 1, userId) => {
    try {
      setUserConvoError("");
      const payload = { userId };
      const response = await axios.post(`${BASE_URL}/v1/get_conversation_summaries`, payload);
      if (response.data.status === "success") {
        const raw = response.data.conversations || [];
        setTotalConvos(raw.length);
        setCurrentPage(requestedPage);
        const startIdx = (requestedPage - 1) * pageSize;
        setConversations(raw.slice(startIdx, startIdx + pageSize));
      } else {
        setUserConvoError(response.data.message || "No conversations found");
        setConversations([]);
      }
    } catch (error) {
      console.error("Error fetching user conversations:", error);
      setUserConvoError(error.response?.data?.detail || "Unexpected error");
      setConversations([]);
    }
  };

  // ---------------- HANDLERS ----------------
  const handleSearch = () => {
    setSearchError("");
    const trimmed = searchTerm.trim();
    if (!trimmed) {
      setSearchError("Please enter a Conversation ID or User ID.");
      return;
    }
    if (trimmed.includes("@")) {
      fetchUserConversations(1, trimmed);
    } else {
      fetchConversationById(trimmed);
    }
  };

  const resetSearchFields = () => {
    setSearchTerm("");
    setSearchError("");
    setConversationError("");
    setSelectedConversation(null);
    setShowModal(false);
  };

  const handleFilter = () => {
    const hasFilters = !!(startDate || endDate || filterOpportunity || filterUserType.toLowerCase() !== "all");
    setIsFiltered(hasFilters);
    setCurrentPage(1);
    fetchAllConversations(1, toolType);
  };

  const resetFilterFields = () => {
    setToolType("ChatBot");
    setStartDate("");
    setEndDate("");
    setFilterOpportunity(false);
    setFilterUserType("all");
    setIsFiltered(false);
    setTotalConvos(0);
    setCurrentPage(1);
    fetchAllConversations(1, "ChatBot");
  };

  const handlePageChange = (newPage) => {
    const maxPage = Math.ceil(totalConvos / pageSize);
    if (newPage > 0 && newPage <= maxPage) {
      setCurrentPage(newPage);
      fetchAllConversations(newPage, toolType);
    }
  };

  const downloadExcelFile = async () => {
    if (!startDate || !endDate) {
      alert("Please select both From and To dates.");
      return;
    }
    try {
      const payload = { from_date: startDate, to_date: endDate };
      const response = await axios.post(
        "https://travbridge.atirath.com/v1/export_conversations",
        payload,
        { responseType: "blob" }
      );

      const blob = new Blob([response.data], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `conversations_tcil_${startDate}_to_${endDate}.xlsx`;
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch (error) {
      console.error("Download error:", error);
      alert("Failed to export summary.");
    }
  };

  // ---------------- EFFECTS ----------------
  useEffect(() => {
    fetchAllConversations(1, toolType);
    // eslint-disable-next-line
  }, []);

  const isAgentAssist = toolType === "Agent-Assistant";

  // ---------- Helpers ----------
  const fmtTime = (t) => {
    if (!t) return "";
    const safe = t.endsWith("Z") || t.includes("+") ? t : `${t}Z`;
    const d = new Date(safe);

    // Try native fractional seconds
    try {
      return d.toLocaleTimeString("en-IN", {
        timeZone: "Asia/Kolkata",
        hour12: false,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        fractionalSecondDigits: 3,
      });
    } catch {
      // Fallback for environments without fractionalSecondDigits
      const pad = (n, len = 2) => String(n).padStart(len, "0");
      // IST is UTC+5:30, no DST
      const istMs = d.getTime() + 5.5 * 60 * 60 * 1000;
      const ist = new Date(istMs);
      const HH = pad(ist.getUTCHours());
      const MM = pad(ist.getUTCMinutes());
      const SS = pad(ist.getUTCSeconds());
      const ms = pad(ist.getUTCMilliseconds(), 3);
      return `${HH}:${MM}:${SS}.${ms}`;
    }
  };

  // Prefer agent_id, then agentId, then userId, then user_id
  const getAgentId = (conv) => {
    const tryKeys = ["agent_id", "agentId", "userId", "user_id"];
    for (const k of tryKeys) {
      const v = conv?.[k];
      if (v !== undefined && v !== null && String(v).trim() !== "") return v;
    }
    return "N/A";
  };

  // Show only messages with query_type === "Show" (case-insensitive).
  // If query_type missing, default to show.
  const isVisibleMsg = (m) => {
    if (m?.query_type == null) return true;
    return String(m.query_type).toLowerCase() === "show";
  };

  // ---------- Agent Assist message renderer ----------
const renderAAMessageEnhanced = (msg, index, { displayContent, packages, isCustomer, isAgent, isAI }) => {
  const rowStyle = {
    ...styles.aaRow,
    ...(isCustomer ? styles.aaRowLeft : styles.aaRowRight),
  };

  const bubbleStyle = {
    ...styles.aaBubbleBase,
    ...(isCustomer ? styles.aaBubbleCustomer : isAI ? styles.aaBubbleAI : styles.aaBubbleAgent),
  };

  const avatarTxt = isCustomer ? "C" : isAI ? "AI" : "AG";
  const roleLabel = isCustomer ? "Customer" : isAI ? "Tacy (AI)" : "Agent";

  return (
    <div key={index} style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
      <div style={{
        ...styles.aaMeta,
        justifyContent: isCustomer ? "flex-start" : "flex-end",
      }}>
        <span>{roleLabel}</span>
        <span>•</span>
        <span>{fmtTime(msg.chat_time)}</span>
      </div>

      <div style={rowStyle}>
        {isCustomer && <div style={styles.aaAvatar}>{avatarTxt}</div>}

        <div style={bubbleStyle}>
          {packages.length > 0 ? (
            <div style={styles.packageGrid}>
              {packages.map((pkg, idx) => (
                <div key={idx} style={styles.packageCard}>
                  <img
                    src={pkg.thumbnail || pkg.tumbnail || "https://via.placeholder.com/150"}
                    alt={pkg.title}
                    style={styles.packageImage}
                    onError={(e) => e.target.src = "https://via.placeholder.com/150"}
                  />
                  <div style={styles.packageContent}>
                    <h4 style={{ margin: "0 0 8px 0" }}>{pkg.title}</h4>
                    <p style={{ margin: "4px 0" }}>{pkg.days}</p>
                    <p style={{ margin: "4px 0", fontWeight: "bold" }}>{pkg.price}</p>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div style={styles.multilineContent}>{displayContent}</div>
          )}
        </div>

        {!isCustomer && <div style={styles.aaAvatar}>{avatarTxt}</div>}
      </div>

      <div style={isCustomer ? styles.aaTimeLeft : styles.aaTimeRight}>
        {fmtTime(msg.chat_time)}
      </div>
    </div>
  );
};

  // ---------------- RENDER ----------------
  return (
    <div style={styles.container}>
      {/* Combined Header and Filters */}
      <div style={styles.combinedRow}>
        {/* Left Group */}
        <div style={styles.leftGroup}>
          <div style={{ display: "flex", alignItems: "center", gap: "10px", marginBottom: "10px" }}>
            <button
              style={{ ...styles.button, backgroundColor: "#f5f5f5", color: "#000" }}
              onClick={goBack}
            >
              &larr; Back
            </button>
            <h1 style={styles.title}>Conversation Dashboard</h1>
          </div>
          {/* Filter Box */}
          <div style={styles.filterBox}>
            <h2 style={styles.groupTitle}>
              Filter by Tool Type, Date, Opportunity ID &amp; User Type
            </h2>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
              <select
                style={styles.select}
                value={toolType}
                onChange={(e) => setToolType(e.target.value)}
              >
                <option value="ChatBot">ChatBot</option>
                <option value="Agent-Assistant">Agent Assist</option>
              </select>
              <input
                type="date"
                style={styles.inputDate}
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
              />
              <input
                type="date"
                style={styles.inputDate}
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
              />

              {/* Opportunity ID checkbox */}
              <label style={{ color: "#f5f5f5", display: "flex", alignItems: "center", gap: "5px" }}>
                <input
                  type="checkbox"
                  checked={filterOpportunity}
                  onChange={(e) => setFilterOpportunity(e.target.checked)}
                />
                Only with Opportunity ID
              </label>
              {/* Only Tool Conversations checkbox */}
              <label style={{ color: "#f5f5f5", display: "flex", alignItems: "center", gap: "5px" }}>
                <input
                  type="checkbox"
                  checked={filterToolOnly}
                  onChange={(e) => setFilterToolOnly(e.target.checked)}
                />
                Only Tool Conversations
              </label>

              {/* User type dropdown */}
              <select
                style={styles.select}
                value={filterUserType}
                onChange={(e) => setFilterUserType(e.target.value)}
              >
                <option value="all">All Users</option>
                <option value="registered">Registered</option>
                <option value="guest">Guest</option>
              </select>
              <button style={styles.button} onClick={handleFilter}>
                Filter
              </button>
              <button
                style={{ ...styles.button, backgroundColor: "#f5f5f5", color: "#000" }}
                onClick={resetFilterFields}
              >
                Reset Filters
              </button>
            </div>
          </div>
        </div>

        {/* Right Group: Search */}
        <div style={styles.rightGroup}>
          <div style={styles.filterBox}>
            <h2 style={styles.groupTitle}>Search by Conversation ID or User ID</h2>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
              <input
                style={styles.inputSearch}
                placeholder="Enter Conversation ID or User ID"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
              />
              <button style={styles.button} onClick={handleSearch}>
                Search
              </button>
              <button
                style={{ ...styles.button, backgroundColor: "#f5f5f5", color: "#000" }}
                onClick={resetSearchFields}
              >
                Reset
              </button>
            </div>
            {searchError && <div style={{ color: "red", marginTop: "10px" }}>{searchError}</div>}
            {conversationError && <div style={{ color: "red", marginTop: "10px" }}>{conversationError}</div>}
            {userConvoError && <div style={{ color: "red", marginTop: "10px" }}>{userConvoError}</div>}
          </div>
        </div>
      </div>

      {/* Export Button */}
      {isFiltered && (
        <div style={{ marginBottom: "10px", textAlign: "right" }}>
          <button style={styles.button} onClick={downloadExcelFile}>
            Export Summary to Excel
          </button>
        </div>
      )}

      {/* Recent Conversations Section */}
      <div style={styles.conversationsSection}>
        <div style={styles.sectionTitle}>
          {isFiltered ? "Filtered Conversations" : "All Conversations"} ({totalConvos})
        </div>

        {/* Table-like Header Row */}
        <div style={styles.tableHeader}>
          <div style={styles.headerId}>ID</div>
          <div style={styles.headerName}>Chat Name</div>
          <div style={styles.headerOpp}>Opportunity ID</div>
          <div style={styles.headerMod}>Last Modified</div>
          <div style={styles.headerBtn}></div>
        </div>

        {isLoading || totalConvos === 0 ? (
          <div style={{ marginTop: "20px" }}>Loading...</div>
        ) : (
          <>
            {conversations.length > 0 ? (
              <>
                {conversations.map((conv, idx) => (
                  <div key={idx} style={styles.rowContainer}>
                    <div style={styles.rowId}>{conv.conversationId}</div>
                    <div style={styles.rowName}>{conv.chat_name}</div>
                    <div style={styles.rowOpp}>
                      {conv.opportunity_id && conv.opportunity_id.trim() !== "" ? (
                        <span style={styles.greenDot} title="Opportunity ID exists"></span>
                      ) : (
                        <span style={styles.redDot} title="Missing Opportunity ID"></span>
                      )}
                    </div>
                    <div style={styles.rowMod}>
                      {conv.chat_modified
                        ? new Date(`${conv.chat_modified}Z`).toLocaleString("en-US", {
                            timeZone: "Asia/Kolkata",
                          })
                        : "N/A"}
                    </div>
                    <div style={styles.rowBtn}>
                      <button
                        style={{
                          ...styles.button,
                          padding: "6px 12px",
                          backgroundColor: palette.primaryBlue,
                          fontSize: "14px",
                        }}
                        onClick={() => fetchConversationById(conv.conversationId)}
                      >
                        View Details
                      </button>
                    </div>
                  </div>
                ))}
                {totalConvos > pageSize && (
                  <div style={styles.paginationRow}>
                    <button
                      disabled={currentPage === 1}
                      onClick={() => handlePageChange(currentPage - 1)}
                      style={styles.button}
                    >
                      Prev
                    </button>
                    <span>
                      Page {currentPage} of {Math.ceil(totalConvos / pageSize)}
                    </span>
                    <button
                      disabled={currentPage === Math.ceil(totalConvos / pageSize)}
                      onClick={() => handlePageChange(currentPage + 1)}
                      style={styles.button}
                    >
                      Next
                    </button>
                  </div>
                )}
              </>
            ) : (
              <div style={{ marginTop: "10px" }}>No conversations found.</div>
            )}
          </>
        )}
      </div>

      {/* Modal Overlay for Conversation Details */}
      {showModal && selectedConversation && (
        <div style={styles.modalOverlay}>
          <div style={styles.modalContent}>
            {/* Sticky Header */}
            <div style={styles.modalHeader}>
              <button
                style={styles.closeButton}
                onClick={() => {
                  setSelectedConversation(null);
                  setShowModal(false);
                }}
              >
                X
              </button>
              <div style={styles.viewToggle}>
                <button
                  style={{
                    ...styles.toggleButton,
                    ...(viewMode === "chat" ? styles.activeToggle : {}),
                  }}
                  onClick={() => setViewMode("chat")}
                >
                  Chat View
                </button>
                <button
                  style={{
                    ...styles.toggleButton,
                    ...(viewMode === "raw" ? styles.activeToggle : {}),
                  }}
                  onClick={() => setViewMode("raw")}
                >
                  JSON View
                </button>
              </div>

              <h2 style={{ marginTop: 0 }}>
                <div style={styles.badgeRow}>
                  <span style={styles.badge}>
                    <span style={styles.badgeLabel}>Conversation Details:</span>
                    {selectedConversation.conversationId}
                  </span>
                  {isAgentAssist && (
                    <span style={styles.badge}>
                      <span style={styles.badgeLabel}>Agent ID:</span>
                      {getAgentId(selectedConversation)}
                    </span>
                  )}
                </div>
              </h2>
            </div>

            <div style={styles.modalBody}>
              {viewMode === "raw" ? (
                <pre style={styles.jsonViewer}>
                  {JSON.stringify(selectedConversation, null, 2)}
                </pre>
              ) : (
                <>
                  {/* Chat View */}
                  <div
                    style={{
                      ...styles.chatContainer,
                      ...(isAgentAssist ? styles.aaSurface : {}),
                    }}
                  >
                 {(selectedConversation.conversation || [])
  .filter(msg => {
    // Keep only visible messages
    if (!isVisibleMsg(msg)) return false;
    // Skip empty content
    if (!msg.content || msg.content === "[]" || (typeof msg.content === "string" && msg.content.trim() === "")) {
      return false;
    }
    return true;
  })
  .sort((a, b) => {
    // Primary: sequence_id (when present). If missing, fall back to chat_time.
    const numOrNull = (val) => {
      const n = Number(val);
      return Number.isFinite(n) ? n : null;
    };
    const seqA = numOrNull(a.sequence_id);
    const seqB = numOrNull(b.sequence_id);

    if (seqA !== null && seqB !== null && seqA !== seqB) return seqA - seqB;
    if (seqA !== null && seqB === null) return -1; // keep sequenced items first
    if (seqA === null && seqB !== null) return 1;

    const timeA = new Date(a.chat_time).getTime();
    const timeB = new Date(b.chat_time).getTime();
    return (Number.isFinite(timeA) ? timeA : 0) - (Number.isFinite(timeB) ? timeB : 0);
  })
  .filter((msg, idx, arr) => {
    // Remove duplicate assistant messages with same sequence_id (keep only last one).
    // If sequence_id is missing/null, do not de-dupe so we don't drop messages.
    if (msg.role === "assistant") {
      const seqPresent = msg.sequence_id !== undefined && msg.sequence_id !== null && msg.sequence_id !== "";
      if (seqPresent) {
        const lastWithSameSeq = arr
          .slice(idx + 1)
          .find(m => m.role === "assistant" && m.sequence_id === msg.sequence_id);
        if (lastWithSameSeq) return false; // skip if a newer one exists later
      }
    }
    return true;
  })
  .map((msg, index) => {
    let displayContent = "";
    let packages = [];

    // Parse content safely
    try {
      let parsed = msg.content;
      if (typeof parsed === "string") {
        // Fix escaped unicode
        parsed = parsed.replace(/\\u2019/g, "’").replace(/\\u2013/g, "–").replace(/\\u2011/g, "-");
        parsed = JSON.parse(parsed);
      }
      if (parsed && typeof parsed === "object") {
        if (parsed.message) {
          displayContent = parsed.message;
        }
        if (msg.type === "list" && Array.isArray(parsed.message)) {
          packages = parsed.message;
        } else if (Array.isArray(parsed)) {
          packages = parsed;
          displayContent = "Here are the recommended packages:";
        }
      } else if (typeof parsed === "string") {
        displayContent = parsed;
      }
    } catch (e) {
      if (typeof msg.content === "string") {
        displayContent = msg.content;
      }
    }

    // Special handling
    if (msg.role === "tool" && packages.length === 0) {
      displayContent = "Here's the list of packages available with Thomas Cook";
    }

    if (!displayContent && packages.length === 0) return null;

    // Determine role for Agent-Assistant
    const effectiveRole = msg.role === "tool" ? "assistant" : msg.role;
    const isCustomer = effectiveRole === "user";
    const isAgent = effectiveRole === "agent";
    const isAI = effectiveRole === "assistant";

    if (isAgentAssist) {
      return renderAAMessageEnhanced(msg, index, { displayContent, packages, isCustomer, isAgent, isAI });
    }

    // ChatBot fallback
    return (
      <div
        key={`${msg.message_id}-${msg.sequence_id}`}
        style={{
          ...styles.messageBubble,
          ...(msg.role === "user" ? styles.userMessage : styles.otherMessage),
          backgroundColor: isAI ? palette.aiBg : undefined,
          color: isAI ? "#fff" : undefined,
        }}
      >
        {packages.length > 0 ? (
          <div style={styles.packageGrid}>
            {packages.map((pkg, idx) => (
              <div key={idx} style={styles.packageCard}>
                <img
                  src={pkg.thumbnail || pkg.tumbnail || "https://via.placeholder.com/150"}
                  alt={pkg.title}
                  style={styles.packageImage}
                  onError={(e) => e.target.src = "https://via.placeholder.com/150"}
                />
                <div style={styles.packageContent}>
                  <h4 style={{ margin: "0 0 8px 0" }}>{pkg.title || "Package"}</h4>
                  <p style={{ margin: "4px 0" }}>{pkg.days || "N/A"}</p>
                  <p style={{ margin: "4px 0", fontWeight: "bold" }}>{pkg.price || "₹XX,XXX"}</p>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div style={styles.multilineContent}>{displayContent}</div>
        )}
        <div style={{
          fontSize: "0.8em",
          marginTop: "8px",
          opacity: 0.7,
          textAlign: msg.role === "user" ? "right" : "left",
        }}>
          {fmtTime(msg.chat_time)}
        </div>
      </div>
    );
  })}
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default TeeCeeChatbot;
