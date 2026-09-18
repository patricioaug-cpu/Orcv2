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
    bodyParser: {
      sizeLimit: "4.5mb",
    },
  },
};

/**
 * Safely consumes and parses the request body stream if not already parsed by Vercel.
 * Prevents stream deadlocks, hangs or body-parser unhandled errors on Vercel Serverless.
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

  // If stream is already completed or ended, do not wait on it
  if (req.readableEnded || req.complete) {
    return;
  }

  // If readable stream is present and body is still empty, buffer stream with safety window
  if (typeof req.on === "function") {
    await new Promise<void>((resolve) => {
      let finished = false;
      const finish = () => {
        if (!finished) {
          finished = true;
          resolve();
        }
      };

      // Generous 15s timeout for large uploads before failing safely
      const streamTimer = setTimeout(finish, 15000);
      const chunks: Buffer[] = [];

      req.on("data", (chunk: any) => {
        chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
      });

      req.on("end", () => {
        clearTimeout(streamTimer);
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
        clearTimeout(streamTimer);
        console.error("[Vercel /api/analyze-project body stream error]:", err);
        finish();
      });
    });
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
  try {
    await ensureBodyParsed(req);
  } catch (parseErr) {
    console.warn("[Vercel /api/analyze-project ensureBodyParsed warning]:", parseErr);
  }

  return new Promise((resolve) => {
    let resolved = false;
    const safeResolve = () => {
      if (!resolved) {
        resolved = true;
        resolve(null);
      }
    };

    // Vercel Hobby plan hard timeout is 10s. We trigger at 9.2s to guarantee a clean HTTP response
    // rather than letting the Vercel proxy forcibly terminate the process with FUNCTION_INVOCATION_FAILED.
    const safetyTimer = setTimeout(() => {
      if (!res.headersSent) {
        console.warn("[Vercel /api/analyze-project] Limite de 9.2s atingido. Enviando resposta HTTP 504 limpa.");
        try {
          res.status(504).json({
            success: false,
            error: "Tempo limite da função na Vercel (10s) atingido. Recomendação: Exporte a prancha como imagem JPEG ou PNG para leitura direta e instantânea pelo modelo.",
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
      clearTimeout(safetyTimer);
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


