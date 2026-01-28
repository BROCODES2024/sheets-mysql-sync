const express = require("express");
const mysql = require("mysql2/promise");
const bodyParser = require("body-parser");
const cors = require("cors");
require("dotenv").config();

const app = express();

/* =====================
   Middleware
===================== */
app.use(cors());
app.use(bodyParser.json());

/* =====================
   Environment
===================== */
const PORT = process.env.PORT || 3000;

/* =====================
   MySQL Connection Pool
===================== */
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: Number(process.env.DB_PORT) || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

/* =====================
   Health + Root Routes
===================== */
app.get("/", (req, res) => {
  res.send("Sheets → MySQL Sync API running 🚀");
});

app.get("/health", (req, res) => {
  res.status(200).json({ status: "ok" });
});

/* =====================
   Helper: Ensure Columns
===================== */
async function ensureColumnsExist(rowData) {
  const keys = Object.keys(rowData);
  if (keys.length === 0) return;

  const [columns] = await pool.query("SHOW COLUMNS FROM sheet_sync");
  const existingColumns = columns.map((col) => col.Field);

  const missingColumns = keys.filter(
    (key) => !existingColumns.includes(key) && key !== "_sheet_row_id",
  );

  for (const col of missingColumns) {
    const safeCol = col.replace(/[^a-zA-Z0-9_]/g, "_");
    console.log(`Creating new column: ${safeCol}`);
    try {
      await pool.query(`ALTER TABLE sheet_sync ADD COLUMN ${safeCol} TEXT`);
    } catch (err) {
      console.error(`Error adding column ${safeCol}:`, err.message);
    }
  }
}

/* =====================
   Sync Endpoint
===================== */
app.post("/sync-from-sheet", async (req, res) => {
  try {
    const { row_id, data } = req.body;

    if (!row_id || !data || typeof data !== "object") {
      return res.status(400).json({
        error: "Missing or invalid row_id or data",
      });
    }

    // Ensure table schema
    await ensureColumnsExist(data);

    const keys = Object.keys(data);
    const values = keys.map((k) => data[k]);

    const columnsSql = keys.join(", ");
    const placeholders = keys.map(() => "?").join(", ");
    const updateClause = keys.map((k) => `${k} = VALUES(${k})`).join(", ");

    const sql = `
      INSERT INTO sheet_sync (_sheet_row_id, ${columnsSql})
      VALUES (?, ${placeholders})
      ON DUPLICATE KEY UPDATE ${updateClause}
    `;

    await pool.query(sql, [row_id, ...values]);

    console.log(`✅ Synced row ${row_id}`);
    res.status(200).json({ status: "success" });
  } catch (err) {
    console.error("❌ Sync error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* =====================
   Start Server
===================== */
app.listen(PORT, () => {
  console.log(`✅ Server running on port ${PORT}`);
});
