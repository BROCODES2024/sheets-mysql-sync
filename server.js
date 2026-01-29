const express = require("express");
const mysql = require("mysql2/promise");
const bodyParser = require("body-parser");
const cors = require("cors");
const { google } = require("googleapis");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(bodyParser.json());

/* -------------------- DATABASE -------------------- */
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: Number(process.env.DB_PORT),
  waitForConnections: true,
  connectionLimit: 10,
});

/* -------------------- GOOGLE AUTH -------------------- */
let credentials;
try {
  credentials = JSON.parse(
    Buffer.from(process.env.GOOGLE_CREDENTIALS, "base64").toString("utf8"),
  );
} catch (err) {
  console.error("❌ Failed to load Google credentials");
  process.exit(1);
}

const auth = new google.auth.GoogleAuth({
  credentials,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

const SPREADSHEET_ID = "12gqxqdM46ii9fLDmsPSjev4ZgwhWj2drcSWhYmuIY24";

/* -------------------- SYNC STATE -------------------- */
let lastSyncTime = new Date();

/* -------------------- HELPERS -------------------- */
async function ensureColumnsExist(rowData) {
  const keys = Object.keys(rowData);
  if (!keys.length) return;

  const [columns] = await pool.query("SHOW COLUMNS FROM sheet_sync");
  const existing = columns.map((c) => c.Field);

  for (const key of keys) {
    if (key === "_sheet_row_id") continue;

    const safeKey = key.replace(/[^a-zA-Z0-9_]/g, "_");
    if (!existing.includes(safeKey)) {
      await pool.query(`ALTER TABLE sheet_sync ADD COLUMN ${safeKey} TEXT`);
      console.log(`➕ Added column: ${safeKey}`);
    }
  }
}

/* -------------------- API: SHEET → DB -------------------- */
app.post("/sync-from-sheet", async (req, res) => {
  try {
    const { row_id, data } = req.body;
    if (!row_id || !data) return res.status(400).send("Invalid payload");

    await ensureColumnsExist(data);

    const keys = Object.keys(data).map((k) => k.replace(/[^a-zA-Z0-9_]/g, "_"));
    const values = Object.values(data);

    const setClause = keys.map((k) => `${k} = ?`).join(", ");

    const sql = `
      INSERT INTO sheet_sync (_sheet_row_id, ${keys.join(", ")})
      VALUES (?, ${keys.map(() => "?").join(", ")})
      ON DUPLICATE KEY UPDATE ${setClause}
    `;

    await pool.query(sql, [row_id, ...values, ...values]);

    lastSyncTime = new Date();
    res.send({ status: "ok" });
  } catch (err) {
    console.error(err);
    res.status(500).send("Sync failed");
  }
});

/* -------------------- POLLING: DB → SHEET -------------------- */
setInterval(async () => {
  try {
    const [rows] = await pool.query(
      "SELECT * FROM sheet_sync WHERE updated_at > ?",
      [lastSyncTime],
    );

    if (!rows.length) return;

    const headerRes = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: "Sheet1!1:1",
    });

    const headers = headerRes.data.values[0];

    for (const row of rows) {
      const values = headers.map(
        (h) => row[h] || row[h.replace(/ /g, "_")] || "",
      );

      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `Sheet1!A${row._sheet_row_id}`,
        valueInputOption: "RAW",
        requestBody: { values: [values] },
      });
    }

    lastSyncTime = new Date();
  } catch (err) {
    console.error("Polling error:", err.message);
  }
}, 5000);

/* -------------------- START SERVER -------------------- */
app.listen(process.env.PORT || 3000, () => {
  console.log("🚀 Server running");
});
