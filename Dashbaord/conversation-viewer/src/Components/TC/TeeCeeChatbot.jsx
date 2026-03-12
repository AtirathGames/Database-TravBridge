// components/TC/TeeCeeChatbot.jsx
import React, { useState, useEffect } from "react";
import axios from "axios";
import "./TeeCeeChatbot.css";

// Axios defaults
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

      const response = await axios.post(
        `${BASE_URL}/v1/get_all_conversations`,
        filters
      );
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
      const response = await axios.post(
        `${BASE_URL}/v1/get_conversation`,
        payload
      );
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
      const response = await axios.post(
        `${BASE_URL}/v1/get_conversation_summaries`,
        payload
      );
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
    const hasFilters = !!(
      startDate ||
      endDate ||
      filterOpportunity ||
      filterUserType.toLowerCase() !== "all"
    );
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
    setFilterToolOnly(false);
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
        `${BASE_URL}/v1/export_conversations`,
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
    const d = new Date(t);
    if (Number.isNaN(d.getTime())) return t;

    try {
      return d.toLocaleTimeString("en-IN", {
        hour12: false,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        fractionalSecondDigits: 3,
      });
    } catch {
      const pad = (n, len = 2) => String(n).padStart(len, "0");
      const HH = pad(d.getHours());
      const MM = pad(d.getMinutes());
      const SS = pad(d.getSeconds());
      const ms = pad(d.getMilliseconds(), 3);
      return `${HH}:${MM}:${SS}.${ms}`;
    }
  };

  const fmtDateTime = (value) => {
    if (!value) return "N/A";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleString("en-US");
  };

  const getAgentId = (conv) => {
    const tryKeys = ["agent_id", "agentId", "userId", "user_id"];
    for (const k of tryKeys) {
      const v = conv?.[k];
      if (v !== undefined && v !== null && String(v).trim() !== "") return v;
    }
    return "N/A";
  };

  const formatStructuredContent = (value) => {
    if (value === null || value === undefined) return "";
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean") {
      return String(value);
    }
    if (Array.isArray(value)) {
      return value
        .map((entry) => formatStructuredContent(entry))
        .filter(Boolean)
        .join("\n");
    }
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  };

  const isVisibleMsg = (m) => {
    if (!m) return false;
    const queryType = String(m.query_type || "").toLowerCase();
    return queryType !== "hide";
  };

  const normalizeMessageContent = (msg) => {
    let displayContent = "";
    let packages = [];
    let parsed = msg?.content;

    try {
      if (typeof parsed === "string") {
        const trimmed = parsed.trim();
        if (
          trimmed.length > 0 &&
          (trimmed.startsWith("{") || trimmed.startsWith("["))
        ) {
          parsed = JSON.parse(trimmed);
        }
      }

      if (parsed && typeof parsed === "object") {
        if (Array.isArray(parsed)) {
          packages = parsed;
          displayContent = "Here are the recommended packages:";
        } else {
          // Handle nested message object
          if (parsed.message !== undefined) {
            if (typeof parsed.message === "string") {
              displayContent = parsed.message;
            } else if (typeof parsed.message === "object" && parsed.message !== null) {
              // Message is an object (Agent-Assistant structured data)
              displayContent = formatStructuredContent(parsed.message);
            }
          }
          if (Array.isArray(parsed.message) && msg?.type === "list") {
            packages = parsed.message;
          }
        }
      } else if (typeof parsed === "string") {
        displayContent = parsed;
      }
    } catch {
      if (typeof msg?.content === "string") {
        displayContent = msg.content;
      }
    }

    // Final fallback for unconverted objects
    if (!displayContent && parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      displayContent = formatStructuredContent(parsed);
    }

    if (msg?.role === "tool" && packages.length === 0) {
      displayContent =
        displayContent ||
        "Here's the list of packages available with Thomas Cook";
    }

    return {
      displayContent: String(displayContent || ""),
      packages,
    };
  };

  // ---------- Agent Assist message renderer ----------
  const renderAAMessageEnhanced = (
    msg,
    index,
    { displayContent, packages, isCustomer, isAgent, isAI }
  ) => {
    // CRITICAL: Ensure displayContent is always a string before rendering
    const safeContent = typeof displayContent === "string"
      ? displayContent
      : typeof displayContent === "object"
        ? JSON.stringify(displayContent, null, 2)
        : String(displayContent || "");

    const rowClass = [
      "aa-row",
      isCustomer ? "aa-row-left" : "aa-row-right",
    ].join(" ");

    const metaClass = [
      "aa-meta",
      isCustomer ? "aa-meta-left" : "aa-meta-right",
    ].join(" ");

    const bubbleClass = [
      "aa-bubble",
      isCustomer ? "aa-bubble-customer" : isAI ? "aa-bubble-ai" : "aa-bubble-agent",
    ].join(" ");

    const timeClass = isCustomer ? "aa-time aa-time-left" : "aa-time aa-time-right";

    const avatarTxt = isCustomer ? "C" : isAI ? "AI" : "AG";
    const roleLabel = isCustomer ? "Customer" : isAI ? "Tacy (AI)" : "Agent";

    return (
      <div key={index} className="aa-message-block">
        <div className={metaClass}>
          <span>{roleLabel}</span>
          <span>•</span>
          <span>{fmtTime(msg.chat_time)}</span>
        </div>

        <div className={rowClass}>
          {isCustomer && <div className="aa-avatar">{avatarTxt}</div>}

          <div className={bubbleClass}>
            {packages.length > 0 ? (
              <div className="tc-package-grid">
                {packages.map((pkg, idx) => (
                  <div key={idx} className="tc-package-card">
                    <img
                      src={
                        pkg.thumbnail ||
                        pkg.tumbnail ||
                        "https://via.placeholder.com/150"
                      }
                      alt={pkg.title}
                      className="tc-package-image"
                      onError={(e) =>
                        (e.target.src = "https://via.placeholder.com/150")
                      }
                    />
                    <div className="tc-package-content">
                      <h4 className="tc-package-title">
                        {pkg.title || "Package"}
                      </h4>
                      <p className="tc-package-days">
                        {pkg.days || "N/A"}
                      </p>
                      <p className="tc-package-price">
                        {pkg.price || "₹XX,XXX"}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <pre className="tc-multiline-content">{safeContent}</pre>
            )}
          </div>

          {!isCustomer && <div className="aa-avatar">{avatarTxt}</div>}
        </div>

        <div className={timeClass}>{fmtTime(msg.chat_time)}</div>
      </div>
    );
  };

  // ---------------- RENDER ----------------
  return (
    <div className="tc-chatbot-container">
      {/* Combined Header + Filters */}
      <div className="tc-header-row">
        {/* Left Group */}
        <div className="tc-left-group">
          <div className="tc-header-top">
            {/* <button
              className="tc-btn tc-btn-secondary"
              onClick={goBack}
            >
              &larr; Back
            </button> */}
            <h1 className="tc-title">Conversation Dashboard</h1>
          </div>

          <div className="tc-filter-box">
            <h2 className="tc-group-title">
              Filter by Tool Type, Date, Opportunity ID &amp; User Type
            </h2>
            <div className="tc-filter-row">
              <select
                className="tc-select"
                value={toolType}
                onChange={(e) => setToolType(e.target.value)}
              >
                <option value="ChatBot">ChatBot</option>
                <option value="Agent-Assistant">Agent Assist</option>
                <option value="VoiceBot">VoiceBot</option>
              </select>

              <input
                type="date"
                className="tc-input-date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
              />
              <input
                type="date"
                className="tc-input-date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
              />

              <label className="tc-checkbox-label">
                <input
                  type="checkbox"
                  checked={filterOpportunity}
                  onChange={(e) => setFilterOpportunity(e.target.checked)}
                />
                Only with Opportunity ID
              </label>

              <label className="tc-checkbox-label">
                <input
                  type="checkbox"
                  checked={filterToolOnly}
                  onChange={(e) => setFilterToolOnly(e.target.checked)}
                />
                Only Tool Conversations
              </label>

              <select
                className="tc-select"
                value={filterUserType}
                onChange={(e) => setFilterUserType(e.target.value)}
              >
                <option value="all">All Users</option>
                <option value="registered">Registered</option>
                <option value="guest">Guest</option>
              </select>

              <button className="tc-btn" onClick={handleFilter}>
                Filter
              </button>
              <button
                className="tc-btn tc-btn-light"
                onClick={resetFilterFields}
              >
                Reset Filters
              </button>
            </div>
          </div>
        </div>

        {/* Right Group */}
        <div className="tc-right-group">
          <div className="tc-filter-box">
            <h2 className="tc-group-title">
              Search by Conversation ID or User ID
            </h2>
            <div className="tc-filter-row">
              <input
                className="tc-input-search"
                placeholder="Enter Conversation ID or User ID"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
              />
              <button className="tc-btn" onClick={handleSearch}>
                Search
              </button>
              <button
                className="tc-btn tc-btn-light"
                onClick={resetSearchFields}
              >
                Reset
              </button>
            </div>
            {searchError && (
              <div className="tc-error-text">{searchError}</div>
            )}
            {conversationError && (
              <div className="tc-error-text">{conversationError}</div>
            )}
            {userConvoError && (
              <div className="tc-error-text">{userConvoError}</div>
            )}
          </div>
        </div>
      </div>

      {/* Export Button */}
      {isFiltered && (
        <div className="tc-export-row">
          <button className="tc-btn" onClick={downloadExcelFile}>
            Export Summary to Excel
          </button>
        </div>
      )}

      {/* Recent Conversations */}
      <div className="tc-conversations-section">
        <div className="tc-section-title">
          {isFiltered ? "Filtered Conversations" : "All Conversations"} (
          {totalConvos})
        </div>

        <div className="tc-table-header">
          <div className="tc-th tc-th-id">ID</div>
          <div className="tc-th tc-th-name">Chat Name</div>
          <div className="tc-th tc-th-opp">Opportunity ID</div>
          <div className="tc-th tc-th-mod">Last Modified</div>
          <div className="tc-th tc-th-btn"></div>
        </div>

        {isLoading || totalConvos === 0 ? (
          <div className="tc-loading">Loading...</div>
        ) : conversations.length > 0 ? (
          <>
            {conversations.map((conv, idx) => (
              <div key={idx} className="tc-row">
                <div className="tc-td tc-td-id">
                  {conv.conversationId}
                </div>
                <div className="tc-td tc-td-name">
                  {conv.chat_name}
                </div>
                <div className="tc-td tc-td-opp">
                  {conv.opportunity_id &&
                    conv.opportunity_id.trim() !== "" ? (
                    <span
                      className="tc-dot tc-dot-green"
                      title="Opportunity ID exists"
                    />
                  ) : (
                    <span
                      className="tc-dot tc-dot-red"
                      title="Missing Opportunity ID"
                    />
                  )}
                </div>
                <div className="tc-td tc-td-mod">
                  {fmtDateTime(conv.chat_modified)}
                </div>
                <div className="tc-td tc-td-btn">
                  <button
                    className="tc-btn tc-btn-small"
                    onClick={() =>
                      fetchConversationById(conv.conversationId)
                    }
                  >
                    View Details
                  </button>
                </div>
              </div>
            ))}

            {totalConvos > pageSize && (
              <div className="tc-pagination-row">
                <button
                  disabled={currentPage === 1}
                  onClick={() => handlePageChange(currentPage - 1)}
                  className="tc-btn"
                >
                  Prev
                </button>
                <span>
                  Page {currentPage} of{" "}
                  {Math.ceil(totalConvos / pageSize)}
                </span>
                <button
                  disabled={
                    currentPage === Math.ceil(totalConvos / pageSize)
                  }
                  onClick={() => handlePageChange(currentPage + 1)}
                  className="tc-btn"
                >
                  Next
                </button>
              </div>
            )}
          </>
        ) : (
          <div className="tc-no-convos">No conversations found.</div>
        )}
      </div>

      {/* Modal */}
      {showModal && selectedConversation && (
        <div className="tc-modal-overlay">
          <div className="tc-modal-content">
            <div className="tc-modal-header">
              <button
                className="tc-modal-close"
                onClick={() => {
                  setSelectedConversation(null);
                  setShowModal(false);
                }}
              >
                X
              </button>

              <div className="tc-view-toggle">
                <button
                  className={
                    viewMode === "chat"
                      ? "tc-toggle-btn tc-toggle-btn-active"
                      : "tc-toggle-btn"
                  }
                  onClick={() => setViewMode("chat")}
                >
                  Chat View
                </button>
                <button
                  className={
                    viewMode === "raw"
                      ? "tc-toggle-btn tc-toggle-btn-active"
                      : "tc-toggle-btn"
                  }
                  onClick={() => setViewMode("raw")}
                >
                  JSON View
                </button>
              </div>

              <h2 className="tc-modal-title">
                <div className="tc-badge-row">
                  <span className="tc-badge">
                    <span className="tc-badge-label">
                      Conversation Details:
                    </span>
                    {selectedConversation.conversationId}
                  </span>
                  {isAgentAssist && (
                    <span className="tc-badge">
                      <span className="tc-badge-label">
                        Agent ID:
                      </span>
                      {getAgentId(selectedConversation)}
                    </span>
                  )}
                </div>
              </h2>
            </div>

            <div className="tc-modal-body">
              {viewMode === "raw" ? (
                <pre className="tc-json-viewer">
                  {JSON.stringify(selectedConversation, null, 2)}
                </pre>
              ) : (
                <div
                  className={
                    isAgentAssist
                      ? "tc-chat-messages aa-surface"
                      : "tc-chat-messages"
                  }
                >
                  {(selectedConversation.conversation || [])
                    .filter((msg) => isVisibleMsg(msg))
                    .sort((a, b) => {
                      const seqA = parseInt(
                        a.sequence_id || "0",
                        10
                      );
                      const seqB = parseInt(
                        b.sequence_id || "0",
                        10
                      );
                      if (seqA !== seqB) return seqA - seqB;
                      return (
                        new Date(a.chat_time).getTime() -
                        new Date(b.chat_time).getTime()
                      );
                    })
                    .map((msg, index) => {
                      const { displayContent, packages } =
                        normalizeMessageContent(msg);

                      // Ensure contentToShow is always a string
                      let contentToShow = displayContent || "";
                      if (typeof contentToShow === "object") {
                        contentToShow = JSON.stringify(contentToShow, null, 2);
                      }
                      if (!contentToShow && packages.length === 0) {
                        contentToShow = "—";
                      }

                      const effectiveRole =
                        msg.role === "tool"
                          ? "assistant"
                          : msg.role;
                      const isCustomer =
                        effectiveRole === "user";
                      const isAgent =
                        effectiveRole === "agent";
                      const isAI =
                        effectiveRole === "assistant";

                      if (isAgentAssist) {
                        return renderAAMessageEnhanced(msg, index, {
                          displayContent: contentToShow,
                          packages,
                          isCustomer,
                          isAgent,
                          isAI,
                        });
                      }

                      // ChatBot view
                      const bubbleClass = [
                        "tc-msg-bubble",
                        effectiveRole === "user"
                          ? "tc-msg-user"
                          : isAI
                            ? "tc-msg-ai"
                            : "tc-msg-other",
                      ].join(" ");

                      const timeClass =
                        effectiveRole === "user"
                          ? "tc-msg-time tc-msg-time-right"
                          : "tc-msg-time tc-msg-time-left";

                      return (
                        <div
                          key={`${msg.message_id || "msg"}-${msg.sequence_id || index
                            }-${index}`}
                          className={bubbleClass}
                        >
                          {packages.length > 0 ? (
                            <div className="tc-package-grid">
                              {packages.map((pkg, idx) => (
                                <div
                                  key={idx}
                                  className="tc-package-card"
                                >
                                  <img
                                    src={
                                      pkg.thumbnail ||
                                      pkg.tumbnail ||
                                      "https://via.placeholder.com/150"
                                    }
                                    alt={
                                      pkg.title || "Package"
                                    }
                                    className="tc-package-image"
                                    onError={(e) =>
                                    (e.target.src =
                                      "https://via.placeholder.com/150")
                                    }
                                  />
                                  <div className="tc-package-content">
                                    <h4 className="tc-package-title">
                                      {pkg.title || "Package"}
                                    </h4>
                                    <p className="tc-package-days">
                                      {pkg.days || "N/A"}
                                    </p>
                                    <p className="tc-package-price">
                                      {pkg.price || "₹XX,XXX"}
                                    </p>
                                  </div>
                                </div>
                              ))}
                            </div>
                          ) : (
                            <pre className="tc-multiline-content">
                              {contentToShow}
                            </pre>
                          )}
                          <div className={timeClass}>
                            {fmtTime(msg.chat_time)}
                          </div>
                        </div>
                      );
                    })}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default TeeCeeChatbot;
