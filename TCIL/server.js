const express = require("express");
const cors = require("cors");
const { getDailyBatchProcessingStats } = require("./dailyBatchStats");

const app = express();
const port = 3001; // ✅ changed from 8000 to 3001

app.use(cors());
app.use(express.json());

app.post("/v1/daily_batch_stats", async (req, res) => {
  try {
    const data = await getDailyBatchProcessingStats();
    res.json({ status: "success", data });
  } catch (error) {
    console.error("Error processing batch stats:", error);
    res.status(500).json({ status: "error", message: error.message });
  }
});

app.listen(port, () => {
  console.log(`✅ Server listening on port ${port}`);
});
