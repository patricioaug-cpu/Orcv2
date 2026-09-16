process.env.IS_SERVERLESS = "1";
process.env.VERCEL = process.env.VERCEL || "1";

import app from "../server";

process.on("unhandledRejection", (reason) => {
  console.error("[Vercel /api/index unhandledRejection]:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("[Vercel /api/index uncaughtException]:", err);
});

export const config = {
  maxDuration: 60,
  api: {
    bodyParser: false,
  },
};

/**
 * Safely consumes and parses the request body stream if not already parsed by Vercel.
 * Prevents stream deadlocks, hangs or body-parser unhandled errors.
 */
async function ensureBodyParsed(req: any): Promise<void> {
  if (req.body !== undefined && req.body !== null && typeof req.body === "object" && !Buffer.isBuffer(req.body)) {
    req._body = true;
    return;
  }

  if (typeof req.body === "string") {
    try {
      req.body = JSON.parse(req.body);
      req._body = true;
      return;
    } catch {
      return;
    }
  }

  if (Buffer.isBuffer(req.body)) {
    try {
      req.body = JSON.parse(req.body.toString("utf-8"));
      req._body = true;
      return;
    } catch {
      return;
    }
  }

  if (typeof req.on === "function") {
    try {
      const chunks: Buffer[] = [];
      for await (const chunk of req) {
        chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
      }
      if (chunks.length > 0) {
        const raw = Buffer.concat(chunks).toString("utf-8");
        req.rawBody = raw;
        try {
          req.body = JSON.parse(raw);
          req._body = true;
        } catch {
          req.body = raw;
        }
      }
    } catch (streamErr) {
      console.error("[Vercel /api/index body stream error]:", streamErr);
    }
  }
}

export default async function handler(req: any, res: any) {
  if (req.url && !req.url.startsWith("/api/")) {
    req.url = "/api" + (req.url.startsWith("/") ? req.url : "/" + req.url);
  }

  // Safely parse request body stream before handing to Express
  await ensureBodyParsed(req);

  return new Promise((resolve) => {
    let resolved = false;
    const safeResolve = () => {
      if (!resolved) {
        resolved = true;
        resolve(null);
      }
    };

    res.on("finish", safeResolve);

    try {
      app(req, res, (err: any) => {
        if (err) {
          console.error("[Vercel /api/index Express Error]:", err);
          if (!res.headersSent) {
            res.status(500).json({
              success: false,
              error: "Erro no processamento da API na Vercel: " + (err?.message || String(err)),
            });
          }
        }
        safeResolve();
      });
    } catch (err: any) {
      console.error("[Vercel /api/index Sync Error]:", err);
      if (!res.headersSent) {
        res.status(500).json({
          success: false,
          error: "Erro interno na API: " + (err?.message || String(err)),
        });
      }
      safeResolve();
    }
  });
}

