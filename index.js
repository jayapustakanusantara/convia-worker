const express = require("express");
const { createClient } = require("redis");
const crypto = require("crypto");

const app = express();
app.use(express.json({ limit: "2mb" }));

// =====================================================
// ENV
// =====================================================
const PORT = process.env.PORT || 8080;
const REDIS_URL = process.env.REDIS_URL;
const RECAP_WEB_APP_URL = process.env.RECAP_WEB_APP_URL;
const RECAP_API_KEY = process.env.RECAP_API_KEY;
const CONVIA_API_KEY = process.env.CONVIA_API_KEY;

// =====================================================
// REDIS KEYS - KHUSUS CONVIA
// =====================================================
const QUEUE_NAME = "convia:queue";
const PROCESSING_QUEUE = "convia:processing";
const DEAD_QUEUE = "convia:dead";

const SEEN_TTL_SECONDS = 7 * 24 * 60 * 60;

// =====================================================
// REDIS CONNECTIONS
// =====================================================

// Redis #1:
// untuk webhook, status, dedup, enqueue, dll.
const redis = createClient({
  url: REDIS_URL,
});

// Redis #2:
// KHUSUS worker blocking queue.
// Ini penting supaya BRPOPLPUSH tidak memblokir /status.
const workerRedis = createClient({
  url: REDIS_URL,
});

redis.on("error", (err) => {
  console.error("[REDIS ERROR]", err);
});

workerRedis.on("error", (err) => {
  console.error("[WORKER REDIS ERROR]", err);
});

// =====================================================
// HELPERS
// =====================================================
function normalizePhone(value) {
  let phone = String(value || "").replace(/\D/g, "");

  if (!phone) return "";

  if (phone.startsWith("0")) {
    phone = "62" + phone.slice(1);
  }

  return phone;
}

function normalizeCommand(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function getEventId(body, phone, command) {
  const data = body?.data || {};
  const raw = data?.raw || {};

  const directId =
    body?.event_id ||
    data?.event_id ||
    data?.message_id ||
    data?.id ||
    raw?.id ||
    raw?.key?.id;

  if (directId) {
    return String(directId);
  }

  const timestamp =
    body?.timestamp ||
    data?.timestamp ||
    raw?.timestamp ||
    raw?.messageTimestamp ||
    "";

  if (!timestamp) {
    return crypto.randomUUID();
  }

  return crypto
    .createHash("sha256")
    .update(`${phone}|${command}|${timestamp}`)
    .digest("hex");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// =====================================================
// HEALTH
// =====================================================
app.get("/", (req, res) => {
  res.status(200).json({
    ok: true,
    service: "convia-worker",
  });
});

app.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    service: "convia-worker",
  });
});

app.get("/status", async (req, res) => {
  try {
    const pending = await redis.lLen(QUEUE_NAME);
    const processing = await redis.lLen(PROCESSING_QUEUE);
    const dead = await redis.lLen(DEAD_QUEUE);

    res.status(200).json({
      ok: true,
      service: "convia-worker",
      state: processing > 0 ? "BUSY" : "IDLE",
      queue: {
        pending,
        processing,
        dead,
      },
    });
  } catch (err) {
    console.error("[STATUS ERROR]", err);

    res.status(500).json({
      ok: false,
      service: "convia-worker",
    });
  }
});

// =====================================================
// CONVIA WEBHOOK
// =====================================================
app.post("/convia", async (req, res) => {
  try {
    const body = req.body || {};
    const data = body.data || {};

    const eventType = String(body.event_type || "").trim();

    if (eventType !== "message.received") {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "event_not_supported",
      });
    }

    const content = String(
      data.content ||
      data.raw?.text?.body ||
      ""
    ).trim();

    const command = normalizeCommand(content);

    if (command !== "REKAP LIVE" && command !== "REKAP BBW") {
      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "command_not_supported",
      });
    }

    const phone = normalizePhone(
      data.customer_phone ||
      data.raw?.from ||
      ""
    );

    if (!phone) {
      console.warn("[WEBHOOK] phone kosong");

      return res.status(200).json({
        ok: true,
        ignored: true,
        reason: "phone_missing",
      });
    }

    const recapType =
      command === "REKAP LIVE" ? "LIVE" : "BBW";

    const eventId = getEventId(body, phone, command);
    const seenKey = `convia:seen:${eventId}`;

    const claimed = await redis.set(
      seenKey,
      "1",
      {
        NX: true,
        EX: SEEN_TTL_SECONDS,
      }
    );

    if (!claimed) {
      return res.status(200).json({
        ok: true,
        duplicate: true,
      });
    }

    const job = {
      eventId,
      phone,
      recapType,
      command,
      receivedAt: new Date().toISOString(),
      attempts: 0,
    };

    await redis.lPush(
      QUEUE_NAME,
      JSON.stringify(job)
    );

    // Convia langsung dapat HTTP 200.
    // Apps Script diproses terpisah oleh worker.
    return res.status(200).json({
      ok: true,
      queued: true,
    });

  } catch (err) {
    console.error("[WEBHOOK ERROR]", err);

    return res.status(200).json({
      ok: false,
      queued: false,
    });
  }
});

// =====================================================
// CALL APPS SCRIPT
// =====================================================
async function sendToRecapApp(job) {
  const controller = new AbortController();

  const timeout = setTimeout(() => {
    controller.abort();
  }, 40000);

  try {
    const response = await fetch(RECAP_WEB_APP_URL, {
      method: "POST",

      headers: {
        "Content-Type": "application/json",
      },

      body: JSON.stringify({
        action: "create_link",
        phone: job.phone,
        recapType: job.recapType,
        apiKey: RECAP_API_KEY,
      }),

      redirect: "follow",
      signal: controller.signal,
    });

    const text = await response.text();

    if (!response.ok) {
      const err = new Error(
        `Apps Script HTTP ${response.status}`
      );

      err.retryable = true;
      throw err;
    }

    let result;

    try {
      result = JSON.parse(text);
    } catch {
      const err = new Error(
        "Apps Script response bukan JSON"
      );

      err.retryable = true;
      throw err;
    }

    if (result?.success !== true) {
      const err = new Error(
        result?.error ||
        result?.message ||
        "Apps Script success=false"
      );

      // Request sudah mencapai Apps Script.
      // Jangan retry otomatis untuk menghindari pesan dobel.
      err.retryable = false;

      throw err;
    }

    return result;

  } catch (err) {
    if (err.name === "AbortError") {
      const timeoutError = new Error(
        "Apps Script timeout 40 detik"
      );

      // Ambigu: mungkin Apps Script sudah mengirim.
      // Jadi jangan auto retry.
      timeoutError.retryable = false;
      timeoutError.ambiguous = true;

      throw timeoutError;
    }

    throw err;

  } finally {
    clearTimeout(timeout);
  }
}

// =====================================================
// PROCESS JOB
// =====================================================
async function processJob(job) {
  console.log(
    `[PROCESS] ${job.recapType} event=${job.eventId}`
  );

  const result = await sendToRecapApp(job);

  console.log(
    `[SUCCESS] ${job.recapType} event=${job.eventId}`
  );

  return result;
}

// =====================================================
// WORKER LOOP
// =====================================================
async function workerLoop() {
  console.log("[WORKER] started");

  while (true) {
    try {
      // PENTING:
      // blocking command memakai workerRedis,
      // BUKAN koneksi redis milik HTTP.
      const raw = await workerRedis.brPopLPush(
        QUEUE_NAME,
        PROCESSING_QUEUE,
        0
      );

      if (!raw) {
        continue;
      }

      let job;

      try {
        job = JSON.parse(raw);
      } catch (err) {
        console.error(
          "[WORKER] invalid JSON job",
          err
        );

        await workerRedis.lRem(
          PROCESSING_QUEUE,
          1,
          raw
        );

        await workerRedis.lPush(
          DEAD_QUEUE,
          raw
        );

        continue;
      }

      try {
        await processJob(job);

        await workerRedis.lRem(
          PROCESSING_QUEUE,
          1,
          raw
        );

      } catch (err) {
        console.error(
          `[JOB ERROR] event=${job.eventId}`,
          err.message
        );

        await workerRedis.lRem(
          PROCESSING_QUEUE,
          1,
          raw
        );

        if (err.retryable === true) {
          const attempts =
            Number(job.attempts || 0) + 1;

          if (attempts <= 3) {
            job.attempts = attempts;

            await sleep(attempts * 2000);

            await workerRedis.lPush(
              QUEUE_NAME,
              JSON.stringify(job)
            );

            console.log(
              `[RETRY] event=${job.eventId} attempt=${attempts}`
            );

            continue;
          }
        }

        const deadJob = {
          ...job,
          failedAt: new Date().toISOString(),
          error: err.message,
          ambiguous: err.ambiguous === true,
        };

        await workerRedis.lPush(
          DEAD_QUEUE,
          JSON.stringify(deadJob)
        );

        console.error(
          `[DEAD] event=${job.eventId}`
        );
      }

    } catch (err) {
      console.error(
        "[WORKER LOOP ERROR]",
        err
      );

      await sleep(2000);
    }
  }
}

// =====================================================
// START
// =====================================================
async function start() {
  if (!REDIS_URL) {
    throw new Error(
      "REDIS_URL belum di-set"
    );
  }

  if (!RECAP_WEB_APP_URL) {
    throw new Error(
      "RECAP_WEB_APP_URL belum di-set"
    );
  }

  if (!RECAP_API_KEY) {
    throw new Error(
      "RECAP_API_KEY belum di-set"
    );
  }

  // Connect Redis HTTP
  await redis.connect();
  console.log("[REDIS] connected");

  // Connect Redis worker secara TERPISAH
  await workerRedis.connect();
  console.log("[WORKER REDIS] connected");

  app.listen(PORT, "0.0.0.0", () => {
    console.log(
      `[HTTP] convia-worker listening on ${PORT}`
    );
  });

  workerLoop().catch((err) => {
    console.error("[WORKER FATAL]", err);
    process.exit(1);
  });
}

start().catch((err) => {
  console.error("[START ERROR]", err);
  process.exit(1);
});
