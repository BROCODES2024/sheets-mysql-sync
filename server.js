const express = require("express");
const mysql = require("mysql2/promise");
const bodyParser = require("body-parser");
const cors = require("cors");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(bodyParser.json());

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

// Helper: Check and Add Columns Dynamically
async function ensureColumnsExist(rowData) {
  const keys = Object.keys(rowData);
  if (keys.length === 0) return;

  // Get current columns in the table
  const [columns] = await pool.query(`SHOW COLUMNS FROM sheet_sync`);
  const existingColumns = columns.map((col) => col.Field);

  // Find missing columns
  const missingColumns = keys.filter(
    (key) => !existingColumns.includes(key) && key !== "_sheet_row_id",
  );

  // Add missing columns dynamically
  for (const col of missingColumns) {
    // We use TEXT to be safe for all data types (numbers, strings, dates)
    // Sanitizing column name to prevent SQL Injection
    const safeCol = col.replace(/[^a-zA-Z0-9_]/g, "_");
    console.log(`Creating new column: ${safeCol}`);
    try {
      await pool.query(`ALTER TABLE sheet_sync ADD COLUMN ${safeCol} TEXT`);
    } catch (err) {
      console.error(`Error adding column ${safeCol}:`, err);
    }
  }
}

// Endpoint: Receive Data from Google Sheets
app.post("/sync-from-sheet", async (req, res) => {
  try {
    const { row_id, data } = req.body; // data is an object like { "Name": "Abhinav", "Role": "Dev" }

    if (!row_id || !data) {
      return res.status(400).send("Missing row_id or data");
    }

    // 1. Ensure table structure matches the incoming data
    await ensureColumnsExist(data);

    // 2. Prepare Query
    // We use INSERT ... ON DUPLICATE KEY UPDATE to handle both new rows and edits
    const keys = Object.keys(data).filter((k) => k !== "row_id");
    const values = keys.map((k) => data[k]);

    // Construct safe dynamic query
    const setClause = keys.map((k) => `${k} = ?`).join(", ");
    const sql = `
            INSERT INTO sheet_sync (_sheet_row_id, ${keys.join(", ")}) 
            VALUES (?, ${keys.map(() => "?").join(", ")}) 
            ON DUPLICATE KEY UPDATE ${setClause}
        `;

    // Execute
    await pool.query(sql, [row_id, ...values, ...values]);

    console.log(`Synced Row ${row_id}`);
    res.status(200).send({ status: "success" });
  } catch (error) {
    console.error("Sync Error:", error);
    res.status(500).send(error.message);
  }
});

app.listen(process.env.PORT, () => {
  console.log(`Server running on port ${process.env.PORT}`);
});
