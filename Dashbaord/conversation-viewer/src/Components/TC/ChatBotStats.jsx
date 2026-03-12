import React, { useState, useEffect, useMemo } from "react";
import axios from "axios";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  LabelList,
  LineChart,
  Line,
} from "recharts";
import "./ChatBotStats.css";

const BASE_URL = "https://travbridge.atirath.com";

/* Brand palette */
const COLORS = {
  primary: "#0044a3",
  success: "#00b894",
  warning: "#fab005",
  danger: "#ff6b6b",
  bg: "#0056B3",
  textOnBg: "#F5F5F5",
  panel: "#ffffff",
  subtle: "#6c757d",
  ink: "#111827",
  hairline: "#e5e7eb",
};

/* Default last 7 days */
const defaultDateRange = () => {
  const today = new Date();
  const past = new Date(today);
  past.setDate(today.getDate() - 6);
  return {
    from: past.toISOString().split("T")[0],
    to: today.toISOString().split("T")[0],
  };
};

const titleize = (s) =>
  (s || "—")
    .toString()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());

const roundSecs = (v) => (v == null ? "—" : Math.round(Number(v)));

const statusKey = (s) => (s || "").toLowerCase().trim().replace(/\s+/g, "_");
const prettyStatus = (s) => {
  const k = statusKey(s);
  if (k === "not_available" || k === "agent_not_available") return "Agents Unavailable";
  if (k === "requested_only" || k === "user_requested") return "User Left (Before Assign)";
  if (k === "agent_not_accepted") return "Agent Not Accepted";
  if (k === "user_left_after_assign") return "User Left (After Assign)";
  if (k === "user_left_before_assign") return "User Abandoned";
  if (k === "agent_busy") return "Agents Busy";
  return titleize(s);
};

const statusPillStyle = (status) => {
  const k = statusKey(status);
  let bg = "#e5e7eb",
    color = "#111827";
  if (k === "success") {
    bg = "#d1fae5";
    color = "#065f46";
  } else if (k === "requested_only" || k === "user_requested") {
    bg = "#dbeafe";
    color = "#1e3a8a";
  } else if (k === "not_available" || k === "agent_not_available") {
    bg = "#f3f4f6";
    color = "#374151";
  } else if (k === "agent_not_accepted") {
    bg = "#fde68a";
    color = "#92400e";
  } else if (k === "user_left_after_assign" || k === "user_left_before_assign") {
    bg = "#fef3c7";
    color = "#92400e";
  } else if (k === "agent_busy") {
    bg = "#e0e7ff";
    color = "#3730a3";
  }
  return {
    display: "inline-block",
    padding: "4px 8px",
    borderRadius: 999,
    fontSize: 12,
    fontWeight: 700,
    background: bg,
    color,
    textTransform: "none",
    whiteSpace: "nowrap",
  };
};

/* Time helpers */
const add330 = (d) => new Date(d.getTime() + 330 * 60 * 1000);
const toISTDate = (dateStr) => {
  if (!dateStr) return "—";
  const d = add330(new Date(`${dateStr}T00:00:00Z`));
  return d.toISOString().slice(0, 10);
};

/* string-based +5:30 time shifter */
const toIST_HM = (timeStr) => {
  if (!timeStr) return "—";
  const raw = String(timeStr).trim();
  const isoMatch = raw.match(/T(\d{2}):(\d{2})(?::(\d{2}))?/);
  let hh, mm;
  if (isoMatch) {
    hh = parseInt(isoMatch[1], 10);
    mm = parseInt(isoMatch[2], 10);
  } else {
    const m = raw.match(/^(\d{2}):(\d{2})(?::\d{2})?$/);
    if (!m) return "—";
    hh = parseInt(m[1], 10);
    mm = parseInt(m[2], 10);
  }
  let total = hh * 60 + mm + 330;
  total = ((total % 1440) + 1440) % 1440;
  const outH = String(Math.floor(total / 60)).padStart(2, "0");
  const outM = String(total % 60).padStart(2, "0");
  return `${outH}:${outM}`;
};

/* ────────── Simple bar funnel ────────── */
const FunnelBars = ({ title, data }) => {
  const max = data.length ? Math.max(1, data[0].value || 1) : 1;
  return (
    <div className="cb-funnel-bars">
      <div className="cb-funnel-bars-title">{title}</div>
      {data.map((d, i) => {
        const pct = max ? Math.round((d.value / max) * 100) : 0;
        return (
          <div key={i} className="cb-funnel-row">
            <div className="cb-funnel-label">{d.label}</div>
            <div className="cb-funnel-track">
              <div
                className="cb-funnel-fill"
                style={{ width: `${pct}%`, backgroundColor: d.color }}
              />
            </div>
            <div className="cb-funnel-value">{d.value}</div>
          </div>
        );
      })}
    </div>
  );
};

/* ────────── Stacked funnel ────────── */
const StepStackedFunnel = ({ title, stages }) => {
  const total = stages?.[0]?.value || 0;
  const packages = stages?.[1]?.value || 0;
  const leads = stages?.[2]?.value || 0;

  const rows = [
    { step: "Total", keep: packages, drop: Math.max(0, total - packages), base: total },
    { step: "Packages", keep: leads, drop: Math.max(0, packages - leads), base: packages },
    { step: "Leads", keep: leads, drop: 0, base: leads },
  ];

  const KeepLabel = (props) => {
    const d = rows[props.index];
    if (!d || !d.keep) return null;
    const pct = d.base ? Math.round((d.keep / d.base) * 100) : 0;
    const x = props.x + props.width - 6;
    const y = props.y + props.height / 2;
    return (
      <text x={x} y={y} textAnchor="end" dominantBaseline="central" fontSize={12} fill="#ffffff">
        {`${d.keep} (${pct}%)`}
      </text>
    );
  };
  const DropLabel = (props) => {
    const d = rows[props.index];
    if (!d || !d.drop) return null;
    const pct = d.base ? Math.round((d.drop / d.base) * 100) : 0;
    const x = props.x + 6;
    const y = props.y + props.height / 2;
    return (
      <text x={x} y={y} textAnchor="start" dominantBaseline="central" fontSize={12} fill="#374151">
        {`Drop ${d.drop} (${pct}%)`}
      </text>
    );
  };

  return (
    <div className="cb-stacked-funnel">
      <div className="cb-stacked-funnel-header">{title}</div>
      <div className="cb-stacked-funnel-body">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={rows}
            layout="vertical"
            barSize={26}
            barCategoryGap={24}
            margin={{ left: 24, right: 24, top: 10, bottom: 10 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" />
            <YAxis type="category" dataKey="step" />
            <Tooltip
              formatter={(val, name, obj) => {
                const base = obj?.payload?.base || 0;
                const pct = base ? Math.round((val / base) * 100) : 0;
                return [`${val} (${pct}%)`, name === "keep" ? "Kept" : "Dropped"];
              }}
            />
            <Legend formatter={(v) => (v === "keep" ? "Kept" : "Dropped")} />
            <Bar dataKey="drop" stackId="a" fill="#e5e7eb" radius={[6, 0, 0, 6]}>
              <LabelList content={DropLabel} />
            </Bar>
            <Bar dataKey="keep" stackId="a" fill={COLORS.success} radius={[0, 6, 6, 0]}>
              <LabelList content={KeepLabel} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

function ChatBotStats() {
  const [summaryData, setSummaryData] = useState([]);
  const [summary, setSummary] = useState({
    total: 0,
    packages: 0,
    empty: 0,
    opportunities: 0,
  });
  const [segmentData, setSegmentData] = useState([]);
  const [segmentTotals, setSegmentTotals] = useState({
    regular_total: 0,
    regular_packages: 0,
    regular_opportunities: 0,
    regular_empty: 0,
    utm_total: 0,
    utm_packages: 0,
    utm_opportunities: 0,
    utm_empty: 0,
  });

  const [liveTotals, setLiveTotals] = useState({
    total: 0,
    success: 0,
    not_available: 0,
    agent_not_accepted: 0,
    agent_busy: 0,
    requested_only: 0,
  });
  const [liveDetails, setLiveDetails] = useState([]);
  const [liveOpen, setLiveOpen] = useState(false);

  const [fromDate, setFromDate] = useState(defaultDateRange().from);
  const [toDate, setToDate] = useState(defaultDateRange().to);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [tab, setTab] = useState("trend");
  const [funnelView, setFunnelView] = useState("stacked");
  const [topDestinations, setTopDestinations] = useState([]);

  const [page, setPage] = useState(1);
  const pageSize = 10;

  const pickTime = (r) =>
    r["Request Time"] ||
    r["Assign Time"] ||
    r["Accept Time"] ||
    r["End Time"] ||
    "00:00:00";
  const toMs = (r) => {
    const date = r["Date"] || "1970-01-01";
    const tIST = toIST_HM(pickTime(r));
    const [hh, mm] = tIST.split(":").map(Number);
    const base = new Date(`${date}T00:00:00Z`).getTime();
    return base + (hh * 60 + mm) * 60 * 1000;
  };
  const sorted = useMemo(
    () => [...liveDetails].sort((a, b) => toMs(b) - toMs(a)),
    [liveDetails]
  );
  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const safePage = Math.min(page, totalPages);
  const start = (safePage - 1) * pageSize;
  const pageRows = sorted.slice(start, start + pageSize);

  const fetchStats = async () => {
    setLoading(true);
    setError("");
    try {
      const response = await axios.post(`${BASE_URL}/v1/conversation_stats`, {
        from_date: fromDate,
        to_date: toDate,
        index_name: "user_conversations",
        summary_only: true,
      });

      const res = response.data;
      const data = res?.summary || [];
      const destinations = res?.most_searched_destinations || [];
      const segments = res?.segment_summary || [];
      const live = res?.live_transfer || {};

      setTopDestinations(destinations);
      setSegmentData(segments);

      const raw = {
        total: live?.totals?.total || 0,
        success: live?.totals?.success || 0,
        not_available: live?.totals?.not_available || 0,
        agent_not_accepted: live?.totals?.agent_not_accepted || 0,
        agent_busy: live?.totals?.agent_busy || 0,
        user_left_after_assign: live?.totals?.user_left_after_assign || 0,
        user_left_before_assign: live?.totals?.user_left_before_assign || 0,
        timeout: live?.totals?.timeout || 0,
      };

      const visibleSum =
        raw.success +
        raw.not_available +
        raw.agent_not_accepted +
        raw.agent_busy;

      const requested_only = Math.max(0, (raw.total || 0) - visibleSum);

      setLiveTotals({
        total: raw.total,
        success: raw.success,
        not_available: raw.not_available,
        agent_not_accepted: raw.agent_not_accepted,
        agent_busy: raw.agent_busy,
        requested_only,
      });

      setLiveDetails(live?.details || []);

      const segTotals = segments.reduce(
        (acc, r) => {
          acc.regular_total += r.regular_total || 0;
          acc.regular_packages += r.regular_packages || 0;
          acc.regular_opportunities += r.regular_opportunities || 0;
          acc.regular_empty += r.regular_empty || 0;
          acc.utm_total += r.utm_total || 0;
          acc.utm_packages += r.utm_packages || 0;
          acc.utm_opportunities += r.utm_opportunities || 0;
          acc.utm_empty += r.utm_empty || 0;
          return acc;
        },
        {
          regular_total: 0,
          regular_packages: 0,
          regular_opportunities: 0,
          regular_empty: 0,
          utm_total: 0,
          utm_packages: 0,
          utm_opportunities: 0,
          utm_empty: 0,
        }
      );
      setSegmentTotals(segTotals);

      const parseDDMMYY = (dateStr) => {
        const [dd, mm, yy] = dateStr.split("-");
        return new Date(`20${yy}`, mm - 1, dd);
      };
      const sortedData = [...data].sort(
        (a, b) => parseDDMMYY(a.date) - parseDDMMYY(b.date)
      );
      setSummaryData(sortedData);

      const totals = sortedData.reduce(
        (acc, item) => {
          acc.total += item.total;
          acc.packages += item.packages;
          acc.empty += item.empty;
          acc.opportunities += item.opportunities;
          return acc;
        },
        { total: 0, packages: 0, empty: 0, opportunities: 0 }
      );
      setSummary(totals);
    } catch (err) {
      console.error(err);
      setError("Failed to fetch stats");
    } finally {
      setLoading(false);
    }
  };

  const downloadExcel = async () => {
    try {
      const response = await axios.post(`${BASE_URL}/v1/conversation_stats`, {
        from_date: fromDate,
        to_date: toDate,
        index_name: "user_conversations",
      });
      const resData = response.data;
      if (resData?.excel_base64) {
        const byteCharacters = atob(resData.excel_base64);
        const byteArray = new Uint8Array(
          [...byteCharacters].map((c) => c.charCodeAt(0))
        );
        const blob = new Blob([byteArray], {
          type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        });
        const link = document.createElement("a");
        link.href = window.URL.createObjectURL(blob);
        link.download = resData.filename || "conversation_stats.xlsx";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      } else {
        alert("Excel file not available in the response.");
      }
    } catch (err) {
      console.error("Download failed", err);
      alert("Failed to download Excel file.");
    }
  };

  useEffect(() => {
    fetchStats();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fromDate, toDate]);

  // Existing overall conversion (opportunities / total)
  const overallCv = useMemo(
    () =>
      summary.total
        ? ((summary.opportunities / summary.total) * 100).toFixed(2)
        : "0.00",
    [summary]
  );

  // NEW: Empty Conversations % = Empty / Total
  const emptyConvoPct = useMemo(
    () =>
      summary.total
        ? ((summary.empty / summary.total) * 100).toFixed(2)
        : "0.00",
    [summary]
  );

// Conversations → Leads % = Opportunities / Non-empty conversations
// Non-empty conversations = Total - Empty
const convToLeadsPct = useMemo(() => {
  const nonEmpty = summary.total - summary.empty;
  if (nonEmpty <= 0 || !summary.opportunities) return "0.00";
  return ((summary.opportunities / nonEmpty) * 100).toFixed(2);
}, [summary.total, summary.empty, summary.opportunities]);


  const regCv = useMemo(
    () =>
      segmentTotals.regular_total
        ? (
            (segmentTotals.regular_opportunities /
              segmentTotals.regular_total) *
            100
          ).toFixed(2)
        : "0.00",
    [segmentTotals]
  );
  const utmCv = useMemo(
    () =>
      segmentTotals.utm_total
        ? (
            (segmentTotals.utm_opportunities / segmentTotals.utm_total) *
            100
          ).toFixed(2)
        : "0.00",
    [segmentTotals]
  );

  const splitSeriesByDate = useMemo(() => {
    const map = {};
    segmentData.forEach((d) => {
      map[d.date] = {
        date: d.date,
        regular_total: d.regular_total || 0,
        utm_total: d.utm_total || 0,
      };
    });
    return Object.values(map);
  }, [segmentData]);

  const funnelOverall = useMemo(
    () => [
      { label: "Total Conversations", value: summary.total, color: COLORS.primary },
      { label: "Packages Shown", value: summary.packages, color: COLORS.success },
      { label: "Leads Generated", value: summary.opportunities, color: COLORS.danger },
    ],
    [summary]
  );

  const funnelRegular = useMemo(
    () => [
      {
        label: "Total Conversations",
        value: segmentTotals.regular_total,
        color: COLORS.primary,
      },
      {
        label: "Packages Shown",
        value: segmentTotals.regular_packages,
        color: COLORS.success,
      },
      {
        label: "Leads Generated",
        value: segmentTotals.regular_opportunities,
        color: COLORS.danger,
      },
    ],
    [segmentTotals]
  );

  const funnelUTM = useMemo(
    () => [
      {
        label: "Total Conversations",
        value: segmentTotals.utm_total,
        color: COLORS.primary,
      },
      {
        label: "Packages Shown",
        value: segmentTotals.utm_packages,
        color: COLORS.success,
      },
      {
        label: "Leads Generated",
        value: segmentTotals.utm_opportunities,
        color: COLORS.danger,
      },
    ],
    [segmentTotals]
  );

  return (
    <div className="cb-container">
      {/* Header */}
      <div className="cb-header-row">
        <button
          onClick={() => window.history.back()}
          className="cb-btn cb-btn-light"
        >
          ← Back
        </button>
        <div className="cb-header">Conversation Insights — ChatBot</div>
        <span className="cb-chip">Active</span>
      </div>

      {/* Controls */}
      <div className="cb-controls">
        <div className="cb-date-row">
          <input
            type="date"
            className="cb-date-input"
            value={fromDate}
            onChange={(e) => setFromDate(e.target.value)}
          />
          <input
            type="date"
            className="cb-date-input"
            value={toDate}
            onChange={(e) => setToDate(e.target.value)}
          />
        </div>
        <div>
          <button className="cb-btn" onClick={downloadExcel}>
            Download Excel
          </button>
        </div>
      </div>

      {/* Overview */}
      <div className="cb-section">
        <div className="cb-section-title">Overview</div>
        <div className="cb-kpi-grid">
          <div className="cb-card">
            <div className="cb-card-title">Total Conversations</div>
            <div className="cb-card-value">{summary.total}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Opportunities</div>
            <div className="cb-card-value">{summary.opportunities}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Empty Conversations</div>
            <div className="cb-card-value">{summary.empty}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Overall Conversion</div>
            <div className="cb-card-value">{overallCv}%</div>
          </div>
          {/* NEW KPI cards */}
          <div className="cb-card">
            <div className="cb-card-title">Empty Conversations %</div>
            <div className="cb-card-value">{emptyConvoPct}%</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Conversations → Leads %</div>
            <div className="cb-card-value">{convToLeadsPct}%</div>
          </div>
        </div>
      </div>

      {/* Source Split */}
      <div className="cb-section cb-section-margin">
        <div className="cb-section-title">Source Split</div>
        <div className="cb-kpi-grid">
          <div className="cb-card">
            <div className="cb-card-title">Regular — Total</div>
            <div className="cb-card-value">{segmentTotals.regular_total}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Regular — Conversion</div>
            <div className="cb-card-value">{regCv}%</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">UTM — Total</div>
            <div className="cb-card-value">{segmentTotals.utm_total}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">UTM — Conversion</div>
            <div className="cb-card-value">{utmCv}%</div>
          </div>
        </div>
      </div>

      {/* Live Agent Transfers */}
      <div className="cb-section cb-section-margin">
        <div className="cb-section-title">Live Agent Transfers</div>
        <div className="cb-kpi-grid-auto">
          <div className="cb-card">
            <div className="cb-card-title">Live — Total</div>
            <div className="cb-card-value">{liveTotals.total}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Success</div>
            <div className="cb-card-value">{liveTotals.success}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Agents Unavailable</div>
            <div className="cb-card-value">{liveTotals.not_available}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Agent Not Accepted</div>
            <div className="cb-card-value">{liveTotals.agent_not_accepted}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Agents Busy</div>
            <div className="cb-card-value">{liveTotals.agent_busy}</div>
          </div>
          <div className="cb-card">
            <div className="cb-card-title">Requested Only</div>
            <div className="cb-card-value">{liveTotals.requested_only}</div>
          </div>

          <div className="cb-card cb-live-card-flex">
            <div>
              <div className="cb-card-title">Details</div>
              <div className="cb-card-subtitle">View transfer-level rows</div>
            </div>
            <button
              className="cb-btn cb-btn-primary"
              onClick={() => {
                setPage(1);
                setLiveOpen(true);
              }}
            >
              More details
            </button>
          </div>
        </div>
      </div>

      {/* Charts + Top Destinations */}
      <div className="cb-main-grid">
        <div className="cb-charts-wrap">
          <div className="cb-charts-header">
            <div className="cb-charts-title">Charts</div>
            <div className="cb-charts-controls">
              {tab === "funnels" && (
                <div className="cb-pills">
                  <div
                    className={`cb-pill ${funnelView === "stacked" ? "active" : ""}`}
                    onClick={() => setFunnelView("stacked")}
                  >
                    Stacked
                  </div>
                  <div
                    className={`cb-pill ${
                      funnelView === "bars" ? "active" : ""
                    }`}
                    onClick={() => setFunnelView("bars")}
                  >
                    Bars
                  </div>
                </div>
              )}
              <div className="cb-pills">
                <div
                  className={`cb-pill ${tab === "trend" ? "active" : ""}`}
                  onClick={() => setTab("trend")}
                >
                  Trend
                </div>
                <div
                  className={`cb-pill ${tab === "split" ? "active" : ""}`}
                  onClick={() => setTab("split")}
                >
                  Source Split
                </div>
                <div
                  className={`cb-pill ${tab === "funnels" ? "active" : ""} cb-pill-last`}
                  onClick={() => setTab("funnels")}
                >
                  Funnels
                </div>
              </div>
            </div>
          </div>

          {loading ? (
            <div className="cb-spinner">Loading...</div>
          ) : error ? (
            <div className="cb-error-text">{error}</div>
          ) : (
            <>
              {tab === "trend" && (
                <ResponsiveContainer width="100%" height={360}>
                  <LineChart data={summaryData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Line
                      type="monotone"
                      dataKey="total"
                      stroke={COLORS.primary}
                      name="Total"
                    />
                    <Line
                      type="monotone"
                      dataKey="packages"
                      stroke={COLORS.success}
                      name="Packages Shown"
                    />
                    <Line
                      type="monotone"
                      dataKey="empty"
                      stroke={COLORS.warning}
                      name="Empty"
                    />
                    <Line
                      type="monotone"
                      dataKey="opportunities"
                      stroke={COLORS.danger}
                      name="Opportunities"
                    />
                  </LineChart>
                </ResponsiveContainer>
              )}

              {tab === "split" && (
                <ResponsiveContainer width="100%" height={360}>
                  <BarChart data={splitSeriesByDate}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar
                      dataKey="regular_total"
                      name="Regular"
                      fill={COLORS.primary}
                    />
                    <Bar
                      dataKey="utm_total"
                      name="UTM"
                      fill={COLORS.warning}
                    />
                  </BarChart>
                </ResponsiveContainer>
              )}

              {tab === "funnels" && (
                <>
                  {funnelView === "stacked" && (
                    <div className="cb-funnel-grid">
                      <StepStackedFunnel
                        title="Regular Funnel"
                        stages={funnelRegular}
                      />
                      <StepStackedFunnel
                        title="UTM Funnel"
                        stages={funnelUTM}
                      />
                      <div className="cb-funnel-full">
                        <StepStackedFunnel
                          title="Overall Funnel"
                          stages={funnelOverall}
                        />
                      </div>
                    </div>
                  )}
                  {funnelView === "bars" && (
                    <div className="cb-funnel-grid">
                      <FunnelBars title="Regular Funnel" data={funnelRegular} />
                      <FunnelBars title="UTM Funnel" data={funnelUTM} />
                      <div className="cb-funnel-full">
                        <FunnelBars
                          title="Overall Funnel"
                          data={funnelOverall}
                        />
                      </div>
                    </div>
                  )}
                </>
              )}
            </>
          )}
        </div>

        {/* Top Destinations */}
        <div className="cb-top-dest-wrapper">
          {topDestinations?.length > 0 && (
            <div className="cb-side-card">
              <div className="cb-side-card-title">Top Destinations</div>
              <ul className="cb-top-dest-list">
                {topDestinations.map((d, i) => (
                  <li key={i}>{d}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </div>

      {/* Live details modal */}
      {liveOpen && (
        <div className="cb-modal-backdrop">
          <div className="cb-modal">
            <div className="cb-modal-header">
              <div>
                <strong>Live Agent Transfers — Details</strong>
              </div>
              <button
                onClick={() => setLiveOpen(false)}
                className="cb-btn cb-btn-light"
              >
                Close
              </button>
            </div>
            <div className="cb-modal-body">
              <div className="cb-table-container">
                <table className="cb-table">
                  <thead>
                    <tr>
                      {[
                        "Date",
                        "Conversation ID",
                        "User Type",
                        "Agent ID",
                        "Attended Agents",
                        "Final Status",
                        "Request Time",
                        "Assign Time",
                        "Accept Time",
                        "End Time",
                        "Wait (sec)",
                        "Duration (sec)",
                        "End Reason",
                        "Opportunity ID",
                      ].map((h) => (
                        <th key={h} className="cb-th">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {pageRows.map((row, idx) => (
                      <tr key={start + idx}>
                        <td className="cb-td">{toISTDate(row["Date"])}</td>
                        <td className="cb-td">
                          {row["Conversation ID"] || "—"}
                        </td>
                        <td className="cb-td">
                          {titleize(row["User Type"])}
                        </td>
                        <td className="cb-td">{row["Agent ID"] || "—"}</td>
                        <td className="cb-td">
                          {row["Attended Agents"] || "—"}
                        </td>
                        <td className="cb-td">
                          <span
                            style={statusPillStyle(row["Final Status"])}
                          >
                            {prettyStatus(row["Final Status"])}
                          </span>
                        </td>
                        <td className="cb-td">
                          {toIST_HM(row["Request Time"])}
                        </td>
                        <td className="cb-td">
                          {toIST_HM(row["Assign Time"])}
                        </td>
                        <td className="cb-td">
                          {toIST_HM(row["Accept Time"])}
                        </td>
                        <td className="cb-td">
                          {toIST_HM(row["End Time"])}
                        </td>
                        <td className="cb-td">
                          {roundSecs(row["Wait (sec)"])}
                        </td>
                        <td className="cb-td">
                          {roundSecs(row["Duration (sec)"])}
                        </td>
                        <td className="cb-td">
                          {titleize(row["End Reason"])}
                        </td>
                        <td className="cb-td">
                          {row["Opportunity ID"] || "—"}
                        </td>
                      </tr>
                    ))}
                    {pageRows.length === 0 && (
                      <tr>
                        <td className="cb-td cb-td-muted" colSpan={14}>
                          No live transfer chats in this range.
                        </td>
                      </tr>
                    )}
                    <tr>
                      <td colSpan={14} className="cb-td cb-td-footer">
                        <div className="cb-pagination-footer">
                          <div className="cb-pagination-info">
                            Showing{" "}
                            {sorted.length === 0
                              ? 0
                              : start + 1}
                            –
                            {Math.min(start + pageSize, sorted.length)} of{" "}
                            {sorted.length}
                          </div>
                          <div className="cb-pagination-buttons">
                            <button
                              className="cb-btn"
                              disabled={safePage <= 1}
                              onClick={() =>
                                setPage((p) => Math.max(1, p - 1))
                              }
                            >
                              ‹ Prev
                            </button>
                            <span className="cb-page-indicator">
                              Page {safePage} / {totalPages}
                            </span>
                            <button
                              className="cb-btn"
                              disabled={safePage >= totalPages}
                              onClick={() =>
                                setPage((p) =>
                                  Math.min(totalPages, p + 1)
                                )
                              }
                            >
                              Next ›
                            </button>
                          </div>
                        </div>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default ChatBotStats;
