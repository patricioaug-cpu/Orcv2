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

export default function handler(req: any, res: any) {
  if (req.url && !req.url.startsWith("/api/")) {
    req.url = "/api" + (req.url.startsWith("/") ? req.url : "/" + req.url);
  }

  return new Promise((resolve) => {
    let resolved = false;
    const finish = () => {
      if (!resolved) {
        resolved = true;
        resolve(null);
      }
    };

    res.on("finish", finish);
    res.on("close", finish);
    res.on("error", finish);

    try {
      app(req, res);
    } catch (err: any) {
      console.error("[Vercel /api/index Error]:", err);
      if (!res.headersSent) {
        res.status(500).json({
          success: false,
          error: "Erro interno na API: " + (err?.message || String(err)),
        });
      }
      finish();
    }
  });
}

