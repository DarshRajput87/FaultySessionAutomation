  // ================= IMPORTS =================
  import axios from "axios";
  import fs from "fs";
  import path from "path";
  import XLSX from "xlsx";
  import nodemailer from "nodemailer";
  import { MongoClient, ObjectId } from "mongodb";
  import { PARTY_CONFIG } from "./config/partyConfig.js";

  // ================= CONFIG =================
  const TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2NDJlZTBkNmU1MmIzYjg1MWNmN2MxMjkiLCJhdXRoVG9rZW5WZXJzaW9uIjoidjEiLCJpYXQiOjE3NzEzMTc4MzgsImV4cCI6MTc3MjYxMzgzOCwidHlwZSI6ImFjY2VzcyJ9.qO5zt2MqTSSzuSLV8muFoO6ePafkr1sArArPhXISttQ";
  const API_URL = "https://appapi.chargecloud.net/v1//report/emspFaultyBookings";
  const MONGO_URI ="mongodb+srv://IT_INTERN:ITINTERN123@cluster1.0pycd.mongodb.net/chargezoneprod";

  const todayFolder = new Date().toISOString().split("T")[0];

  // ================= LOGGER =================
  function log(step, msg) {
    console.log(`[${new Date().toISOString()}] [${step}] ${msg}`);
  }

  // ================= MAIL =================
  const transporter = nodemailer.createTransport({
    service: "gmail",
    auth: {
      user: "darshraj3104@gmail.com",
      pass: "ddxg ddtb fiiz mygh"
    }
  });

  // ================= FOLDERS =================
  const reportDir = path.join("reports", todayFolder);
  const masterDir = "MasterData";
  const mailDir = path.join("PartyRecords", todayFolder);

  [reportDir, masterDir, mailDir].forEach(d => {
    if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
  });

  const excelPath = path.join(reportDir, "emspFaultyBookings.xlsx");

  // ================= DB =================
  const client = new MongoClient(MONGO_URI);
  let bookingCollection;

  async function connectDB() {
    await client.connect();
    bookingCollection = client.db("chargezoneprod").collection("chargerbookings");
    log("DB", "Connected");
  }

  // ================= DOWNLOAD EXCEL =================
  async function downloadExcel() {

    const now = new Date();
    const fromISO = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)
    ).toISOString();

    const toISO = now.toISOString();

    log("API", `Downloading ${fromISO} → ${toISO}`);

    const response = await axios.post(
      API_URL,
      {
        payment_status: "action_required",
        excel: true,
        from: fromISO,
        to: toISO
      },
      {
        responseType: "arraybuffer",
        headers: {
          authorization: `Bearer ${TOKEN}`,
          "content-type": "application/json"
        }
      }
    );

    fs.writeFileSync(excelPath, response.data);
    log("API", "Excel Downloaded");
  }

  // ================= FETCH BOOKINGS =================
  async function fetchBookingsBulk(ids) {

    const validIds = ids
      .filter(id => ObjectId.isValid(id))
      .map(id => new ObjectId(id));

    const docs = await bookingCollection.find({
      _id: { $in: validIds }
    }).toArray();

    const map = new Map();
    docs.forEach(d => map.set(String(d._id), d));
    return map;
  }

  // ================= FAULT CHECK =================
  function isFaulty(doc, partyId) {

    const party = PARTY_CONFIG[partyId];
    if (!party) return false;

    const credential = doc.ocpiCredential
      ? String(doc.ocpiCredential)
      : null;

    if (!party.ocpiCredentials.includes(credential))
      return false;

    const invoiceExists = !!doc.invoice;

    const faultyLength =
      Array.isArray(doc.faulty_booking_reason)
        ? doc.faulty_booking_reason.length
        : 0;

    return (
      doc.is_ocpi_based_booking &&
      doc.is_emsp_based_booking &&
      !invoiceExists &&
      faultyLength > 0 &&
      doc.payment_status === "action_required"
    );
  }

  // ================= MASTER =================
  function loadMaster(partyId) {

    const filePath = path.join(masterDir, `${partyId}_Master.xlsx`);

    if (!fs.existsSync(filePath))
      return { data: [], path: filePath };

    const wb = XLSX.readFile(filePath);
    const data =
      XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);

    return { data, path: filePath };
  }

  function saveMaster(data, filePath) {

    const reIndexed = data.map((row, i) => {
      const { "Sr No.": _, ...rest } = row;
      return { "Sr No.": i + 1, ...rest };
    });

    const ws = XLSX.utils.json_to_sheet(reIndexed);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Tracking");

    XLSX.writeFile(wb, filePath);
  }

  // ================= MAIL EXCEL =================
  function createMailExcel(rows, partyId, type) {

    const fileData = rows.map((r, i) => ({
      "Sr No.": i + 1,
      ...r
    }));

    const ws = XLSX.utils.json_to_sheet(fileData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Faulty");

    const filePath =
      path.join(mailDir,
        `${partyId}_${type}_${todayFolder}.xlsx`);

    XLSX.writeFile(wb, filePath);
    return filePath;
  }

  // ================= CORE =================
  async function reconcileAndProcess() {

    if (!fs.existsSync(excelPath)) {
      log("ERROR", "Excel missing");
      return;
    }

    const workbook = XLSX.readFile(excelPath);
    const jsonData =
      XLSX.utils.sheet_to_json(
        workbook.Sheets[workbook.SheetNames[0]],
        { range: 2 }
      );

    const partyMap = {};

    jsonData.forEach(r => {
      const partyId = String(r["Party ID"]).trim();
      if (!partyId) return;
      if (!partyMap[partyId]) partyMap[partyId] = [];
      partyMap[partyId].push(r);
    });

    for (const [partyId, rows] of Object.entries(partyMap)) {

      log("PROCESS", `Processing ${partyId}`);

      const bookingIds =
        rows.map(r => r["Authorization Reference"]);

      const bookingMap =
        await fetchBookingsBulk(bookingIds);

      const dbFaultyRows =
        rows.filter(r => {
          const doc =
            bookingMap.get(
              String(r["Authorization Reference"])
            );
          return doc &&
            isFaulty(doc, partyId);
        });

      const { data: masterData, path: masterPath } =
        loadMaster(partyId);

      const existingIds =
        masterData.map(r => r["Authorization Reference"]);

      const todayIds =
        dbFaultyRows.map(r => r["Authorization Reference"]);

      // ================= UPDATE STILL_EXIST =================
      masterData.forEach(row => {
        row["Still_Exist"] =
          todayIds.includes(row["Authorization Reference"])
            ? "YES" : "NO";

        row["Still_Exist_Timestamp"] =
          new Date().toISOString();
      });

      // ================= NEW IDS → NOTIFICATION =================
      const newRows =
        dbFaultyRows.filter(r =>
          !existingIds.includes(
            r["Authorization Reference"])
        );

      if (newRows.length) {

        const filePath =
          createMailExcel(newRows, partyId, "Notification");

        const subject =
          `Faulty Sessions - ${partyId} - ${todayFolder}`;

        const info =
          await transporter.sendMail({
            to: PARTY_CONFIG[partyId].emails.join(","),
            subject,
            text: "Please resolve attached faulty sessions.",
            attachments: [
              { filename: path.basename(filePath),
                path: filePath }
            ]
          });

        newRows.forEach(r => {
          masterData.push({
            ...r,
            "Batch_Date": todayFolder,
            "Thread_ID": info.messageId,
            "Notification_Timestamp":
              new Date().toISOString(),
            "Reminder1_Timestamp": "",
            "FinalReminder_Timestamp": "",
            "Still_Exist": "",
            "Still_Exist_Timestamp": ""
          });
        });

        log("MAIL", "Notification sent");
      }

      // ================= BATCH REMINDER =================
      const batches =
        [...new Set(masterData.map(r => r["Batch_Date"]))];

      for (const batch of batches) {

        const batchRows =
          masterData.filter(r =>
            r["Batch_Date"] === batch &&
            r["Still_Exist"] === "YES"
          );

        if (!batchRows.length) continue;

        const firstRow = batchRows[0];

        const now = Date.now();
        const notifTime =
          new Date(firstRow["Notification_Timestamp"]).getTime();

        const rem1Time =
          firstRow["Reminder1_Timestamp"]
            ? new Date(firstRow["Reminder1_Timestamp"]).getTime()
            : null;

        // REMINDER 1
        if (!firstRow["Reminder1_Timestamp"] &&
            now - notifTime >= 15 * 60 * 1000) {

          const filePath =
            createMailExcel(batchRows, partyId, "Reminder1");

          await transporter.sendMail({
            to: PARTY_CONFIG[partyId].emails.join(","),
            subject: firstRow["Subject"] || 
                    `Re: Faulty Sessions - ${partyId} - ${batch}`,
            text: "Reminder: Please resolve pending faulty sessions.",
            headers: {
              "In-Reply-To": firstRow["Thread_ID"],
              "References": firstRow["Thread_ID"]
            },
            attachments: [
              { filename: path.basename(filePath),
                path: filePath }
            ]
          });

          batchRows.forEach(r =>
            r["Reminder1_Timestamp"] =
              new Date().toISOString()
          );

          log("MAIL", `Reminder1 sent for batch ${batch}`);
        }

        // FINAL REMINDER
        if (rem1Time &&
            !firstRow["FinalReminder_Timestamp"] &&
            now - rem1Time >= 15 * 60 * 1000) {

          const filePath =
            createMailExcel(batchRows, partyId, "FinalReminder");

          await transporter.sendMail({
            to: PARTY_CONFIG[partyId].emails.join(","),
            subject: `Final Reminder - ${partyId} - ${batch}`,
            text: "FINAL REMINDER: Immediate action required.",
            headers: {
              "In-Reply-To": firstRow["Thread_ID"],
              "References": firstRow["Thread_ID"]
            },
            attachments: [
              { filename: path.basename(filePath),
                path: filePath }
            ]
          });

          batchRows.forEach(r =>
            r["FinalReminder_Timestamp"] =
              new Date().toISOString()
          );

          log("MAIL", `Final Reminder sent for batch ${batch}`);
        }
      }

      saveMaster(masterData, masterPath);
    }
  }

  // ================= RUN =================
  async function run() {

    log("SYSTEM", "Started");

    try {

      await connectDB();
      await downloadExcel();
      await reconcileAndProcess();

      log("SYSTEM", "Completed Successfully");

    } catch (error) {

      log("FATAL ERROR", error.message);
      console.error(error);

    } finally {

      try {
        await client.close();
        log("DB", "Connection Closed");
      } catch (e) {
        log("DB", "Close failed");
      }

      log("SYSTEM", "Auto Stopped");
      process.exit(0); // clean exit
    }
  }

  run();