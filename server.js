const express = require("express");
const mysql = require("mysql2/promise");
const bodyParser = require("body-parser");
const cors = require("cors");
const { google } = require("googleapis");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(bodyParser.json());

// 1. Database Connection
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

// 2. Google Sheets Authentication
const auth = new google.auth.GoogleAuth({
  keyFile: "service_account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
// REPLACE THIS WITH YOUR ACTUAL SPREADSHEET ID (from the URL)
const SPREADSHEET_ID = "12gqxqdM46ii9fLDmsPSjev4ZgwhWj2drcSWhYmuIY24";

// Memory variable to track the last time we checked the DB
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
      console.error(err);
    }
  }
}

// --- API ENDPOINTS ---

// RECEIVE data FROM Google Sheets
app.post("/sync-from-sheet", async (req, res) => {
  try {
    const { row_id, data } = req.body;
    if (!row_id || !data) return res.status(400).send("Missing data");

    await ensureColumnsExist(data);

    const keys = Object.keys(data).filter((k) => k !== "row_id");
    const values = keys.map((k) => data[k]);

    // Construct Query
    const setClause = keys.map((k) => `${k} = ?`).join(", ");
    const sql = `
            INSERT INTO sheet_sync (_sheet_row_id, ${keys.join(", ")}) 
            VALUES (?, ${keys.map(() => "?").join(", ")}) 
            ON DUPLICATE KEY UPDATE ${setClause}
        `;

    await pool.query(sql, [row_id, ...values, ...values]);

    // CRITICAL: Update lastSyncTime so we don't immediately echo this back
    lastSyncTime = new Date();

    console.log(`Synced Row ${row_id} from Sheet`);
    res.status(200).send({ status: "success" });
  } catch (error) {
    console.error(error);
    res.status(500).send(error.message);
  }
});

// --- POLLING JOB (DB -> SHEET) ---
// Runs every 5 seconds to check for changes in DB
setInterval(async () => {
  try {
    // 1. Find rows updated RECENTLY (after our last check)
    const [rows] = await pool.query(
      `SELECT * FROM sheet_sync WHERE updated_at > ?`,
      [lastSyncTime],
    );

    if (rows.length === 0) return; // No changes

    console.log(`Found ${rows.length} updates in DB. Pushing to Sheet...`);

    const sheets = google.sheets({ version: "v4", auth });

    // 2. Iterate and Push to Google Sheets
    for (const row of rows) {
      const sheetRowId = row._sheet_row_id;

      // Get current headers from Sheet to map values correctly
      // (In production, cache this!)
      const headerRes = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: "Sheet1!1:1",
      });
      const headers = headerRes.data.values[0];

      // Map DB columns to Sheet Order
      const rowDataArray = headers.map((header) => {
        // Handle case where DB column name might differ slightly or handle mapping
        return row[header] || row[header.replace(/ /g, "_")] || "";
      });

      // Update the specific row in Sheets
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `Sheet1!A${sheetRowId}`,
        valueInputOption: "RAW",
        requestBody: { values: [rowDataArray] },
      });
    }

    // Update timestamp
    lastSyncTime = new Date();
  } catch (error) {
    console.error("Polling Error:", error);
  }
}, 5000); // 5000ms = 5 seconds

app.listen(process.env.PORT || 3000, () => {
  console.log(`Server running on port ${process.env.PORT || 3000}`);
});
