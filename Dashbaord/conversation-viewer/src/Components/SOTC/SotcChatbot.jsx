// src/Components/SOTC/SotcChatbot.jsx
import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import "./SotcChatbot.css";

// Axios defaults
axios.defaults.withCredentials = false;
axios.defaults.headers.common["Access-Control-Allow-Origin"] = "*";

// Update BASE_URL as needed.
const BASE_URL = "https://travbridge.atirath.com";

function SotcChatbot() {
  // ---------------- STATE VARIABLES ----------------
  const [toolType, setToolType] = useState("ChatBot");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [isFiltered, setIsFiltered] = useState(false);

  const [filterOpportunity, setFilterOpportunity] = useState(false);
  const [filterUserType, setFilterUserType] = useState("all");
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

  const formatTime = (value) => {
    if (!value) return "";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;

    try {
      return parsed.toLocaleTimeString("en-IN", {
        hour12: false,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        fractionalSecondDigits: 3,
      });
    } catch {
      const pad = (n, len = 2) => String(n).padStart(len, "0");
      const HH = pad(parsed.getHours());
      const MM = pad(parsed.getMinutes());
      const SS = pad(parsed.getSeconds());
      const ms = pad(parsed.getMilliseconds(), 3);
      return `${HH}:${MM}:${SS}.${ms}`;
    }
  };

  const formatDateTime = (value) => {
    if (!value) return "N/A";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleString("en-US");
  };

  // ---------------- API CALLS ----------------
  const fetchAllConversations = async (page = 1, channel = toolType) => {
    setIsLoading(true);
    try {
      const filters = {
        chat_channel: channel,
        count: pageSize,
        page,
      };
      if (startDate) filters.chat_started_from = startDate;
      if (endDate) filters.chat_started_to = endDate;
      if (filterOpportunity) filters.opportunity_id = "true";
      if (filterToolOnly) filters.only_tool_conversations = true;
      if (filterUserType.toLowerCase() !== "all") {
        filters.userId = filterUserType;
      }

      const response = await axios.post(
        `${BASE_URL}/sotc/SOTC_get_all_conversations`,
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
        `${BASE_URL}/sotc/SOTC_get_conversation`,
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
        `${BASE_URL}/sotc/SOTC_get_conversation_summaries`,
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
      const payload = {
        from_date: startDate,
        to_date: endDate,
      };

      const response = await axios.post(
        `${BASE_URL}/sotc/SOTC_export_conversations`,
        payload,
        { responseType: "blob" }
      );

      const blob = new Blob([response.data], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });

      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `conversations_sotc_${startDate}_to_${endDate}.xlsx`;
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

  const totalPages = Math.ceil(totalConvos / pageSize);

  // ---------------- HELPER FUNCTION ----------------
  const normalizeMessageContent = (msg) => {
    let displayContent = "";
    let packages = [];

    try {
      let parsed = msg?.content;

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
          if (Array.isArray(parsed.message) && msg?.type === "list") {
            packages = parsed.message;
          } else if (parsed.message !== undefined) {
            // Ensure displayContent is always a string
            displayContent = typeof parsed.message === "string"
              ? parsed.message
              : JSON.stringify(parsed.message);
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

    if (msg?.role === "tool" && packages.length === 0) {
      displayContent =
        displayContent ||
        "Here's the list of packages available with Thomas Cook";
    }

    if (Array.isArray(displayContent)) {
      displayContent = displayContent.join("\n");
    }

    return {
      displayContent: displayContent || "",
      packages,
    };
  };

  // ---------------- RENDER ----------------
  return (
    <div className="sotc-container">
      {/* Header + Filters */}
      <div className="sotc-header-row">
        {/* Left Group */}
        <div className="sotc-left-group">
          <div className="sotc-header-top">
            {/* <button
              className="sotc-btn sotc-btn-light"
              onClick={goBack}
            >
              &larr; Back
            </button> */}
            <h1 className="sotc-title">Conversation Dashboard</h1>
          </div>

          <div className="sotc-filter-box">
            <h2 className="sotc-group-title">
              Filter by Tool Type, Date, Opportunity ID &amp; User Type
            </h2>
            <div className="sotc-filter-row">
              <select
                className="sotc-select"
                value={toolType}
                onChange={(e) => setToolType(e.target.value)}
              >
                <option value="ChatBot">ChatBot</option>
                <option value="VoiceBot">VoiceBot</option>
              </select>

              <input
                type="date"
                className="sotc-input-date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
              />
              <input
                type="date"
                className="sotc-input-date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
              />

              <label className="sotc-checkbox-label">
                <input
                  type="checkbox"
                  checked={filterOpportunity}
                  onChange={(e) => setFilterOpportunity(e.target.checked)}
                />
                Only with Opportunity ID
              </label>

              <label className="sotc-checkbox-label">
                <input
                  type="checkbox"
                  checked={filterToolOnly}
                  onChange={(e) => setFilterToolOnly(e.target.checked)}
                />
                Only Tool Conversations
              </label>

              <select
                className="sotc-select"
                value={filterUserType}
                onChange={(e) => setFilterUserType(e.target.value)}
              >
                <option value="all">All Users</option>
                <option value="registered">Registered</option>
                <option value="guest">Guest</option>
              </select>

              <button className="sotc-btn" onClick={handleFilter}>
                Filter
              </button>
              <button
                className="sotc-btn sotc-btn-light"
                onClick={resetFilterFields}
              >
                Reset Filters
              </button>
            </div>
          </div>
        </div>

        {/* Right Group: Search */}
        <div className="sotc-right-group">
          <div className="sotc-filter-box">
            <h2 className="sotc-group-title">
              Search by Conversation ID or User ID
            </h2>
            <div className="sotc-filter-row">
              <input
                className="sotc-input-search"
                placeholder="Enter Conversation ID or User ID"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
              />
              <button className="sotc-btn" onClick={handleSearch}>
                Search
              </button>
              <button
                className="sotc-btn sotc-btn-light"
                onClick={resetSearchFields}
              >
                Reset
              </button>
            </div>
            {searchError && (
              <div className="sotc-error-text">{searchError}</div>
            )}
            {conversationError && (
              <div className="sotc-error-text">
                {conversationError}
              </div>
            )}
            {userConvoError && (
              <div className="sotc-error-text">{userConvoError}</div>
            )}
          </div>
        </div>
      </div>

      {/* Export Button */}
      {isFiltered && (
        <div className="sotc-export-row">
          <button className="sotc-btn" onClick={downloadExcelFile}>
            Export Summary to Excel
          </button>
        </div>
      )}

      {/* Conversations List */}
      <div className="sotc-conversations-section">
        <div className="sotc-section-title">
          {isFiltered ? "Filtered Conversations" : "All Conversations"} (
          {totalConvos})
        </div>

        <div className="sotc-table-header">
          <div className="sotc-th sotc-th-id">ID</div>
          <div className="sotc-th sotc-th-name">Chat Name</div>
          <div className="sotc-th sotc-th-opp">Opportunity ID</div>
          <div className="sotc-th sotc-th-mod">Last Modified</div>
          <div className="sotc-th sotc-th-btn"></div>
        </div>

        {isLoading || totalConvos === 0 ? (
          <div className="sotc-loading">Loading...</div>
        ) : conversations.length > 0 ? (
          <>
            {conversations.map((conv, idx) => (
              <div key={idx} className="sotc-row">
                <div className="sotc-td sotc-td-id">
                  {conv.conversationId}
                </div>
                <div className="sotc-td sotc-td-name">
                  {conv.chat_name}
                </div>
                <div className="sotc-td sotc-td-opp">
                  {conv.opportunity_id &&
                    conv.opportunity_id.trim() !== "" ? (
                    <span
                      className="sotc-dot sotc-dot-green"
                      title="Opportunity ID exists"
                    />
                  ) : (
                    <span
                      className="sotc-dot sotc-dot-red"
                      title="Missing Opportunity ID"
                    />
                  )}
                </div>
                <div className="sotc-td sotc-td-mod">
                  {formatDateTime(conv.chat_modified)}
                </div>
                <div className="sotc-td sotc-td-btn">
                  <button
                    className="sotc-btn sotc-btn-small sotc-btn-dark"
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
              <div className="sotc-pagination-row">
                <button
                  disabled={currentPage === 1}
                  onClick={() => handlePageChange(currentPage - 1)}
                  className="sotc-btn"
                >
                  Prev
                </button>
                <span>
                  Page {currentPage} of {totalPages}
                </span>
                <button
                  disabled={currentPage === totalPages}
                  onClick={() => handlePageChange(currentPage + 1)}
                  className="sotc-btn"
                >
                  Next
                </button>
              </div>
            )}
          </>
        ) : (
          <div className="sotc-no-convos">No conversations found.</div>
        )}
      </div>

      {/* Modal */}
      {showModal && selectedConversation && (
        <div className="sotc-modal-overlay">
          <div className="sotc-modal-content">
            <div className="sotc-modal-header">
              <button
                className="sotc-modal-close"
                onClick={() => {
                  setSelectedConversation(null);
                  setShowModal(false);
                }}
              >
                X
              </button>

              <div className="sotc-view-toggle">
                <button
                  className={
                    viewMode === "chat"
                      ? "sotc-toggle-btn sotc-toggle-btn-active"
                      : "sotc-toggle-btn"
                  }
                  onClick={() => setViewMode("chat")}
                >
                  Chat View
                </button>
                <button
                  className={
                    viewMode === "raw"
                      ? "sotc-toggle-btn sotc-toggle-btn-active"
                      : "sotc-toggle-btn"
                  }
                  onClick={() => setViewMode("raw")}
                >
                  JSON View
                </button>
              </div>

              <h2 className="sotc-modal-title">
                Conversation Details:&nbsp;
                <span className="sotc-badge">
                  {selectedConversation.conversationId}
                </span>
              </h2>
            </div>

            <div className="sotc-modal-body">
              {viewMode === "raw" ? (
                <pre className="sotc-json-viewer">
                  {JSON.stringify(selectedConversation, null, 2)}
                </pre>
              ) : (
                <div className="sotc-chat-messages">
                  {selectedConversation.conversation?.map(
                    (msg, index) => {
                      const { displayContent, packages } =
                        normalizeMessageContent(msg);
                      const contentToShow =
                        displayContent ||
                        (packages.length === 0 ? "—" : "");

                      const effectiveRole =
                        msg.role === "tool" ? "assistant" : msg.role;

                      const bubbleClass = [
                        "sotc-msg-bubble",
                        effectiveRole === "user"
                          ? "sotc-msg-user"
                          : "sotc-msg-other",
                      ].join(" ");

                      const timeClass =
                        effectiveRole === "user"
                          ? "sotc-msg-time sotc-msg-time-right"
                          : "sotc-msg-time sotc-msg-time-left";

                      return (
                        <div key={index} className={bubbleClass}>
                          {packages.length > 0 ? (
                            <div className="sotc-package-grid">
                              {packages.map((pkg, idx) => (
                                <div
                                  key={idx}
                                  className="sotc-package-card"
                                >
                                  <img
                                    src={
                                      pkg.thumbnail ||
                                      pkg.tumbnail ||
                                      "https://via.placeholder.com/150"
                                    }
                                    alt={pkg.title || "Package"}
                                    className="sotc-package-image"
                                    onError={(e) =>
                                    (e.target.src =
                                      "https://via.placeholder.com/150")
                                    }
                                  />
                                  <div className="sotc-package-content">
                                    <h4 className="sotc-package-title">
                                      {pkg.title || "Package"}
                                    </h4>
                                    <p className="sotc-package-days">
                                      {pkg.days || "N/A"}
                                    </p>
                                    <p className="sotc-package-price">
                                      {pkg.price || "₹XX,XXX"}
                                    </p>
                                  </div>
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div className="sotc-multiline-content">
                              {contentToShow}
                            </div>
                          )}

                          <div className={timeClass}>
                            {formatTime(msg.chat_time)}
                          </div>
                        </div>
                      );
                    }
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default SotcChatbot;
