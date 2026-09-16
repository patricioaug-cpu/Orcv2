process.env.IS_SERVERLESS = "1";
process.env.VERCEL = process.env.VERCEL || "1";

import app from "../server";

process.on("unhandledRejection", (reason) => {
  console.error("[Vercel /api/analyze-project unhandledRejection]:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("[Vercel /api/analyze-project uncaughtException]:", err);
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
  // If already parsed into an object, mark as parsed
  if (req.body !== undefined && req.body !== null && typeof req.body === "object" && !Buffer.isBuffer(req.body)) {
    req._body = true;
    return;
  }

  // If already a parsed string, attempt JSON parse
  if (typeof req.body === "string") {
    try {
      req.body = JSON.parse(req.body);
      req._body = true;
      return;
    } catch {
      return;
    }
  }

  // If already a buffer, attempt JSON parse
  if (Buffer.isBuffer(req.body)) {
    try {
      req.body = JSON.parse(req.body.toString("utf-8"));
      req._body = true;
      return;
    } catch {
      return;
    }
  }

  // If readable stream is present, buffer it
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
      console.error("[Vercel /api/analyze-project body stream error]:", streamErr);
    }
  }
}

export default async function handler(req: any, res: any) {
  // Normalize incoming URL so Express router always matches /api/analyze-project or /analyze-project
  if (!req.url || req.url === "/" || req.url === "") {
    req.url = "/api/analyze-project";
  } else if (req.url.startsWith("/analyze-project")) {
    req.url = "/api" + req.url;
  } else if (!req.url.startsWith("/api/analyze-project")) {
    req.url = "/api/analyze-project" + (req.url.startsWith("/") ? req.url : "/" + req.url);
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
          console.error("[Vercel /api/analyze-project Express Error]:", err);
          if (!res.headersSent) {
            res.status(500).json({
              success: false,
              error: "Erro no servidor da Vercel ao processar projeto: " + (err?.message || String(err)),
            });
          }
        }
        safeResolve();
      });
    } catch (err: any) {
      console.error("[Vercel /api/analyze-project Sync Error]:", err);
      if (!res.headersSent) {
        res.status(500).json({
          success: false,
          error: "Erro no servidor da Vercel ao analisar projeto: " + (err?.message || String(err)),
        });
      }
      safeResolve();
    }
  });
}


