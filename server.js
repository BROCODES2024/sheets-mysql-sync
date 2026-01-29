const express = require("express");
const mysql = require("mysql2/promise");
const bodyParser = require("body-parser");
const cors = require("cors");
const { google } = require("googleapis");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(bodyParser.json());

// --- CONFIGURATION ---
// REPLACE THIS with your actual Spreadsheet ID (from your URL)
const SPREADSHEET_ID = "1JQTJlDVkvYToA4qoIMVhncRsnvBURCjPC_x5mn44_0E";

// Database Pool
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Google Auth Setup
let auth;
try {
  let serviceAccount = process.env.GOOGLE_SERVICE_ACCOUNT;

  // SMART CHECK: If it doesn't look like JSON (doesn't start with "{"), assume it's Base64 and decode it
  if (serviceAccount && !serviceAccount.trim().startsWith("{")) {
    serviceAccount = Buffer.from(serviceAccount, "base64").toString("utf-8");
  }

  const credentials = JSON.parse(serviceAccount);

  auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  console.log("Google Auth Configured Successfully!");
} catch (error) {
  console.error("CRITICAL ERROR: Could not parse GOOGLE_SERVICE_ACCOUNT.");
  console.error(
    "Make sure the Railway variable matches your local method (Base64 vs Raw JSON).",
  );
  console.error("Error details:", error.message);
}

// Track the last sync time to avoid re-reading old data
let lastSyncTime = new Date();

// --- HELPER FUNCTIONS ---

async function ensureColumnsExist(rowData) {
  const keys = Object.keys(rowData);
  if (keys.length === 0) return;

  const [columns] = await pool.query(`SHOW COLUMNS FROM sheet_sync`);
  const existingColumns = columns.map((col) => col.Field);
  const missingColumns = keys.filter(
    (key) => !existingColumns.includes(key) && key !== "_sheet_row_id",
  );

  for (const col of missingColumns) {
    const safeCol = col.replace(/[^a-zA-Z0-9_]/g, "_");
    try {
      await pool.query(`ALTER TABLE sheet_sync ADD COLUMN ${safeCol} TEXT`);
      console.log(`Added column: ${safeCol}`);
    } catch (err) {
      console.error(`Error adding column ${safeCol}:`, err);
    }
  }
}

// --- ENDPOINTS ---

app.post("/sync-from-sheet", async (req, res) => {
  try {
    const { row_id, data } = req.body;

    if (!row_id || !data) return res.status(400).send("Missing row_id or data");

    await ensureColumnsExist(data);

    const keys = Object.keys(data).filter((k) => k !== "row_id");
    const values = keys.map((k) => data[k]);

    const setClause = keys.map((k) => `${k} = ?`).join(", ");
    const sql = `
            INSERT INTO sheet_sync (_sheet_row_id, ${keys.join(", ")}) 
            VALUES (?, ${keys.map(() => "?").join(", ")}) 
            ON DUPLICATE KEY UPDATE ${setClause}
        `;

    await pool.query(sql, [row_id, ...values, ...values]);

    // IMPORTANT: Update time so the poller doesn't send this back
    lastSyncTime = new Date();

    console.log(`Synced Row ${row_id} from Sheet`);
    res.status(200).send({ status: "success" });
  } catch (error) {
    console.error("Sync Error:", error);
    res.status(500).send(error.message);
  }
});

// --- POLLING WORKER (MySQL -> Sheets) ---
setInterval(async () => {
  if (!auth) return;

  try {
    // 1. Get rows modified AFTER the last sync
    const [rows] = await pool.query(
      `SELECT * FROM sheet_sync WHERE updated_at > ?`,
      [lastSyncTime],
    );

    if (rows.length === 0) return;

    console.log(`Found ${rows.length} DB updates. Pushing to Sheet...`);
    const sheets = google.sheets({ version: "v4", auth });

    // 2. Push updates
    for (const row of rows) {
      const sheetRowId = row._sheet_row_id;

      // Get Headers to map data correctly
      const headerRes = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: "Sheet1!1:1",
      });
      const headers = headerRes.data.values[0];

      // Map DB columns to Sheet Order
      const rowDataArray = headers.map((header) => {
        // Try exact match or match with underscores
        return row[header] || row[header.replace(/ /g, "_")] || "";
      });

      // Update Sheet
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `Sheet1!A${sheetRowId}`,
        valueInputOption: "RAW",
        requestBody: { values: [rowDataArray] },
      });
    }

    // Reset Clock
    lastSyncTime = new Date();
  } catch (err) {
    console.error("Polling Error:", err);
  }
}, 5000); // Runs every 5 seconds

app.listen(process.env.PORT, () => {
  console.log(`Server running on port ${process.env.PORT}`);
});
