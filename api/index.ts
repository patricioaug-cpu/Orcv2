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
};

/**
 * Safely consumes and parses the request body stream if not already parsed by Vercel.
 * Prevents stream deadlocks, hangs or body-parser unhandled errors on Vercel.
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

  // If stream is already completed or ended, do not wait on it
  if (req.readableEnded || req.complete) {
    return;
  }

  // If readable stream is present, buffer it with a strict 2000ms timeout race to prevent serverless deadlocks
  if (typeof req.on === "function") {
    await new Promise<void>((resolve) => {
      let finished = false;
      const finish = () => {
        if (!finished) {
          finished = true;
          resolve();
        }
      };

      const safetyTimer = setTimeout(finish, 2000);
      const chunks: Buffer[] = [];

      req.on("data", (chunk: any) => {
        chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
      });

      req.on("end", () => {
        clearTimeout(safetyTimer);
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
        finish();
      });

      req.on("error", (err: any) => {
        clearTimeout(safetyTimer);
        console.error("[Vercel /api/index body stream error]:", err);
        finish();
      });
    });
  }
}

export default async function handler(req: any, res: any) {
  if (req.url && !req.url.startsWith("/api/")) {
    req.url = "/api" + (req.url.startsWith("/") ? req.url : "/" + req.url);
  }

  // Safely parse request body stream before handing to Express
  try {
    await ensureBodyParsed(req);
  } catch (parseErr) {
    console.warn("[Vercel /api/index ensureBodyParsed warning]:", parseErr);
  }

  return new Promise((resolve) => {
    let resolved = false;
    const safeResolve = () => {
      if (!resolved) {
        resolved = true;
        resolve(null);
      }
    };

    const safetyTimer = setTimeout(() => {
      if (!res.headersSent) {
        console.warn("[Vercel /api/index] Limite de 9.2s atingido. Enviando resposta HTTP 504 limpa.");
        try {
          res.status(504).json({
            success: false,
            error: "Tempo limite da função serverless na Vercel (10s) atingido.",
            details: "FUNCTION_TIMEOUT_GUARD_PREVENTED_INVOCATION_FAIL",
          });
        } catch {}
      }
      safeResolve();
    }, 9200);

    const onFinish = () => {
      clearTimeout(safetyTimer);
      safeResolve();
    };

    res.on("finish", onFinish);
    res.on("close", onFinish);

    try {
      app(req, res, (err: any) => {
        clearTimeout(safetyTimer);
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
      clearTimeout(safetyTimer);
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

