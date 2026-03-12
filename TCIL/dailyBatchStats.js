const fs = require("fs");
const path = require("path");

// Directory where the log files are stored
const LOG_DIR = "/home/gcp-admin/thomascook-travelplanner/Elastic Search";

// Regex to parse each log line: "YYYY-MM-DD HH:MM:SS,ms - INFO - ..."
function parseLogLine(line) {
  const regex = /^(\d{4}-\d{2}-\d{2})\s+([\d:,]+)\s+-\s+INFO\s+-\s+(.*)$/;
  const match = line.match(regex);
  if (!match) return null;

  return {
    date: match[1],  // e.g. "2025-04-04"
    time: match[2],  // e.g. "19:00:00,001"
    message: match[3],
  };
}

// Aggregates stats from an array of log messages
function extractStats(messages) {
  const stats = {
    processed: 0,
    failed: 0,
    generated: 0,
    generationFailed: 0,
    uniquePackages: 0,
  };

  messages.forEach((msg) => {
    // "Processing 873 unique packages"
    if (msg.includes("Processing") && msg.includes("unique packages")) {
      const m = msg.match(/Processing\s+(\d+)\s+unique packages/);
      if (m) stats.uniquePackages = parseInt(m[1], 10);
    }
    // "✅ Batch processing completed. Processed: 873, Failed: 0"
    if (msg.includes("✅ Batch processing completed.")) {
      const m = msg.match(/Processed:\s*(\d+),\s*Failed:\s*(\d+)/);
      if (m) {
        stats.processed = parseInt(m[1], 10);
        stats.failed = parseInt(m[2], 10);
      }
    }
    // "📊 Summary generation stats: Generated: 873, Failed: 0"
    if (msg.includes("📊 Summary generation stats:")) {
      const m = msg.match(/Generated:\s*(\d+),\s*Failed:\s*(\d+)/);
      if (m) {
        stats.generated = parseInt(m[1], 10);
        stats.generationFailed = parseInt(m[2], 10);
      }
    }
  });

  return stats;
}

/**
 * shiftToIST: 
 *   Takes a UTC date/time, builds a Date object,
 *   adds +330 minutes, then returns a local day string 'YYYY-MM-DD'.
 * @param {string} utcDate e.g. "2025-04-04"
 * @param {string} utcTime e.g. "19:00:00,001"
 * @returns {string} e.g. "2025-04-05" (the IST day)
 */
function shiftToIST(utcDate, utcTime) {
  // 1) Convert "19:00:00,001" to "19:00:00.001" for valid ISO
  const isoTime = utcTime.replace(",", ".");
  // 2) Build full ISO string e.g. "2025-04-04T19:00:00.001Z"
  const isoString = `${utcDate}T${isoTime}Z`;
  // 3) Create a date object in UTC
  const dateObj = new Date(isoString);

  // 4) Add 330 minutes (5h30m) for IST
  dateObj.setMinutes(dateObj.getMinutes() + 330);

  // 5) Now dateObj's 'UTC' date/time is effectively the local IST date/time.
  //    So we get the new day from getUTC* methods
  const year = dateObj.getUTCFullYear();
  const month = String(dateObj.getUTCMonth() + 1).padStart(2, "0");
  const day = String(dateObj.getUTCDate()).padStart(2, "0");

  return `${year}-${month}-${day}`; // e.g. "2025-04-05"
}

// Main function to gather stats for last 30 days (IST)
function getDailyBatchProcessingStats() {
  return new Promise((resolve, reject) => {
    fs.readdir(LOG_DIR, (err, files) => {
      if (err) return reject(err);

      const logFiles = files.filter((f) => f.startsWith("log_") && f.endsWith(".log"));
      const dailyLogs = {}; // e.g. { '2025-04-05': [message1, message2, ...] }

      // Process each file
      logFiles.forEach((file) => {
        const filePath = path.join(LOG_DIR, file);
        try {
          const content = fs.readFileSync(filePath, "utf8");
          const lines = content.split("\n");

          lines.forEach((line) => {
            const parsed = parseLogLine(line);
            if (!parsed) return;

            // SHIFT TO IST
            const istDateStr = shiftToIST(parsed.date, parsed.time);

            if (!dailyLogs[istDateStr]) {
              dailyLogs[istDateStr] = [];
            }
            dailyLogs[istDateStr].push(parsed.message);
          });
        } catch (e) {
          console.error(`Error reading file ${file}:`, e);
        }
      });

      // Keep only last 30 days from 'now' in IST
      // We'll do the same shifting logic for 'now' to get local date
      const now = new Date();
      // we can do the same shift approach, or simpler:
      // just interpret 'now' as UTC and shift 330. Then get the date.
      now.setMinutes(now.getMinutes() + 330);

      const cutoff = new Date(now);
      cutoff.setDate(now.getDate() - 30);

      // Build final result
      const result = Object.keys(dailyLogs)
        .filter((dateStr) => new Date(dateStr) >= cutoff)
        .map((dateStr) => ({
          date: dateStr,
          stats: extractStats(dailyLogs[dateStr]),
        }))
        // Sort descending by date
        .sort((a, b) => new Date(b.date) - new Date(a.date));

      resolve(result);
    });
  });
}

module.exports = { getDailyBatchProcessingStats };
