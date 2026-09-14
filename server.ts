import express from "express";
import path from "path";
import fs from "fs";
import { GoogleGenAI } from "@google/genai";
import dotenv from "dotenv";
import {
  processOfficialMnemonics,
  getAllOfficialMnemonics,
  getMnemonicCatalogStats,
} from "./server/mnemonicService";
import {
  consultarItemPorDescricao,
  getAllCatalogRecords,
  getCatalogStats,
  executarValidacaoCatalogo,
} from "./server/itemCatalogService";
import {
  getSimbologiaOficial,
  getSimbologiaCategorias,
  getSimbologiaPorCategoria,
  getSimbologiaStats,
} from "./server/simbologiaService";
import {
  getAllAssociacoes,
  getAssociacaoPorSimboloId,
  getAssociacoesPorMnemonico,
  getAssociacoesStats,
} from "./server/simbologiaMnemonicService";
import {
  executarMotorReconhecimento,
  ElementoDetectadoInput,
  RelatorioReconhecimentoProjeto,
} from "./server/recognitionEngineService";
import {
  reconhecerSimbologiaProjeto,
  getAllOfficialSymbols,
  getOfficialSymbolById,
  ProjectSymbolCandidateInput,
} from "./server/projectSymbolRecognitionService";
import {
  processarProjetoComSimbologiaOficial,
  ProjetoProcessadoOficial,
} from "./server/orchestrationService";
import {
  analysisCache,
  buildOptimizedSystemPrompt,
} from "./server/tokenOptimizationService";
import { projectProcessingPipeline } from "./server/projectProcessingPipeline";
import { jobStorage } from "./server/jobStorageService";
import { jobExecutionService } from "./server/jobExecutionService";
import { qstashVerifier } from "./server/qstashAuthService";
import { pageCache } from "./server/pageCacheService";
import {
  UserService,
  ADMIN_EMAIL,
  evaluateUserTrial,
} from "./server/userService";

dotenv.config();

const app = express();
const PORT = 3000;

// Add CORS headers middleware for iframe and development preview compatibility
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

// Robust body parsing compatible with both Vercel Serverless Functions and standalone Express
app.use((req, res, next) => {
  // If Vercel pre-parsed the body as string, parse it
  if (typeof req.body === "string") {
    try {
      req.body = JSON.parse(req.body);
      (req as any)._body = true;
    } catch {}
  }
  // If req.body is already present (e.g. from Vercel built-in parser),
  // flag it so body-parser does not attempt to re-read the consumed stream
  if (req.body !== undefined && req.body !== null) {
    (req as any)._body = true;
    return next();
  }
  next();
});

app.use((req, res, next) => {
  if ((req as any)._body) {
    return next();
  }
  express.json({
    limit: "50mb",
    verify: (req: any, _res, buf) => {
      req.rawBody = buf;
    },
  })(req, res, next);
});

app.use((req, res, next) => {
  if ((req as any)._body) {
    return next();
  }
  express.urlencoded({ extended: true, limit: "50mb" })(req, res, next);
});

// Express JSON parsing error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  if (err && (err.type === "entity.too.large" || err.status === 413)) {
    return res.status(413).json({
      success: false,
      error: "O arquivo enviado é muito grande para o processamento direto (HTTP 413). Tente reduzir a resolução ou compactar o arquivo.",
    });
  }
  if (err) {
    console.error("Express middleware error:", err);
    return res.status(400).json({ success: false, error: "Formato de requisição inválido." });
  }
  next();
});


// Health check endpoint
app.get(["/api/health", "/health"], (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// Splash Screen & Asset image route
app.get(["/splash_screen.png", "/file_000000000bf4820e964dd2c8ded3136c.png"], (req, res) => {
  const imgPath = path.join(process.cwd(), "public", "splash_screen.png");
  if (fs.existsSync(imgPath)) {
    res.setHeader("Content-Type", "image/png");
    res.setHeader("Cache-Control", "public, max-age=86400");
    return res.sendFile(imgPath);
  }
  res.status(404).send("Image not found");
});

// ==================== AUTH & TRIAL & ADMIN ROUTES ====================

// Register
app.post(["/api/auth/register", "/auth/register"], async (req, res) => {
  try {
    const { nome, email, password, deviceSerial } = req.body;
    const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress || "127.0.0.1";
    const result = await UserService.registerUser({
      nome,
      email,
      password,
      deviceSerial,
      ip: String(Array.isArray(ip) ? ip[0] : ip),
    });
    const { senha_hash, ...safeUser } = result.user;
    res.json({ success: true, user: safeUser, trialInfo: result.trialInfo });
  } catch (err: any) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// Login
app.post(["/api/auth/login", "/auth/login"], async (req, res) => {
  try {
    const { email, password, deviceSerial } = req.body;
    const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress || "127.0.0.1";
    const result = await UserService.loginUser({
      email,
      password,
      deviceSerial,
      ip: String(Array.isArray(ip) ? ip[0] : ip),
    });
    const { senha_hash, ...safeUser } = result.user;
    res.json({ success: true, user: safeUser, trialInfo: result.trialInfo });
  } catch (err: any) {
    res.status(401).json({ success: false, error: err.message });
  }
});

// Forgot password
app.post(["/api/auth/forgot-password", "/auth/forgot-password"], async (req, res) => {
  try {
    const { email } = req.body;
    const result = await UserService.generatePasswordResetCode(email);
    res.json({
      success: true,
      emailSent: result.emailSent,
      directReset: !result.emailSent,
      token: !result.emailSent ? result.token : undefined,
      message: result.emailSent
        ? "Código de verificação enviado para seu e-mail cadastrado. Verifique sua caixa de entrada e pasta de spam."
        : "Conta localizada com sucesso. Digite sua nova senha abaixo para redefinir o acesso.",
    });
  } catch (err: any) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// Reset password
app.post(["/api/auth/reset-password", "/auth/reset-password"], async (req, res) => {
  try {
    const { email, code, token, newPassword } = req.body;
    await UserService.resetPassword({ email, code, token, newPassword });
    res.json({ success: true, message: "Senha redefinida com sucesso. Faça login com sua nova senha." });
  } catch (err: any) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// Admin direct reset user password
app.post(["/api/admin/users/reset-password", "/admin/users/reset-password"], async (req, res) => {
  try {
    const { adminEmail, userId, newPassword } = req.body;
    if (String(adminEmail || "").trim().toLowerCase() !== ADMIN_EMAIL.toLowerCase()) {
      return res.status(403).json({ success: false, error: "Apenas o administrador mestre pode redefinir senhas." });
    }
    await UserService.adminResetUserPassword(userId, newPassword);
    res.json({ success: true, message: "Senha do usuário atualizada com sucesso pelo administrador." });
  } catch (err: any) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// Check Trial status
app.post(["/api/auth/check-trial", "/auth/check-trial"], (req, res) => {
  try {
    const { userId, userEmail, deviceSerial } = req.body;

    // Se for o administrador patricioaug@gmail.com, acesso é imediatamente liberado sem contabilizar trial
    if (String(userEmail || "").trim().toLowerCase() === ADMIN_EMAIL.toLowerCase()) {
      return res.json({
        success: true,
        trialInfo: {
          status: "liberado",
          isAdmin: true,
          isExpired: false,
          daysRemaining: -1,
          trialInicio: "",
          trialFim: "",
          deviceBound: false,
          message: "Acesso permanente de Administrador: sempre liberado, sem tempo de trial.",
        },
      });
    }

    const user = userId ? UserService.getUserById(userId) : UserService.getUserByEmail(userEmail);
    if (!user) {
      return res.status(404).json({ success: false, error: "Usuário não encontrado." });
    }
    const trialInfo = evaluateUserTrial(user, deviceSerial);
    res.json({ success: true, trialInfo });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Admin Middleware check: strictly patricioaug@gmail.com
function checkAdminAccess(req: express.Request, res: express.Response, next: express.NextFunction) {
  const adminEmailHeader =
    req.headers["x-user-email"] ||
    req.headers["x-admin-email"] ||
    req.query.adminEmail ||
    req.body.adminEmail;
  if (String(adminEmailHeader).trim().toLowerCase() !== ADMIN_EMAIL.toLowerCase()) {
    return res.status(403).json({ success: false, error: "Acesso restrito ao Administrador do sistema (patricioaug@gmail.com)." });
  }
  next();
}

// Admin: Get all users
app.get(["/api/admin/users", "/admin/users"], checkAdminAccess, (req, res) => {
  try {
    const users = UserService.getAllUsers();
    res.json({ success: true, users });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Admin: Update user status / manual release
app.post(["/api/admin/users/status", "/admin/users/status"], checkAdminAccess, (req, res) => {
  try {
    const { targetUserId, status, extendDays } = req.body;
    const user = UserService.updateUserStatus(targetUserId, status, extendDays);
    const { senha_hash, ...safeUser } = user;
    res.json({ success: true, user: safeUser });
  } catch (err: any) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// Admin: Get logins history
app.get(["/api/admin/logins", "/admin/logins"], checkAdminAccess, (req, res) => {
  try {
    const logins = UserService.getLoginHistory();
    res.json({ success: true, logins });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Admin: Get email notifications
app.get(["/api/admin/notifications", "/admin/notifications"], checkAdminAccess, (req, res) => {
  try {
    const notifications = UserService.getEmailNotifications();
    res.json({ success: true, notifications });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Save calculation
app.post(["/api/calculos/save", "/calculos/save"], async (req, res) => {
  try {
    const { userId, dadosJson, cargaTermica, metodo } = req.body;
    if (!userId) {
      return res.status(400).json({ success: false, error: "userId obrigatório." });
    }
    const record = await UserService.saveCalculo({
      userId,
      dadosJson,
      cargaTermica,
      metodo,
    });
    res.json({ success: true, record });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get calculations
app.get(["/api/calculos", "/calculos"], (req, res) => {
  try {
    const userId = String(req.query.userId || "");
    const calculos = UserService.getCalculos(userId || undefined);
    res.json({ success: true, calculos });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// API Route for Official Item Catalog Lookup
app.get("/api/catalog/items/all", (req, res) => {
  try {
    const records = getAllCatalogRecords();
    res.json({
      success: true,
      total: records.length,
      records,
    });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get(["/api/catalog/items", "/api/catalog/items/all"], (req, res) => {
  try {
    const records = getAllCatalogRecords();
    res.json({
      success: true,
      total: records.length,
      records,
    });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/catalog/items/lookup", (req, res) => {
  const descricao = String(req.query.descricao || req.query.desc || "");
  const result = consultarItemPorDescricao(descricao);
  res.json(result);
});

app.post("/api/catalog/items/lookup", (req, res) => {
  const { descricao, descricoes } = req.body;
  if (Array.isArray(descricoes)) {
    const results = descricoes.map((d) => ({
      descricao: d,
      ...consultarItemPorDescricao(String(d || "")),
    }));
    return res.json({ total: results.length, items: results });
  }
  const result = consultarItemPorDescricao(String(descricao || ""));
  res.json(result);
});

// API Route for Catalog Statistics
app.get("/api/catalog/items/stats", (req, res) => {
  try {
    const stats = getCatalogStats();
    res.json({ success: true, stats });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// API Route for Catalog Validation Suite
app.get("/api/catalog/items/validate", (req, res) => {
  try {
    const validacao = executarValidacaoCatalogo();
    res.json({ success: true, validacao });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// API Routes for Official Mnemonic Catalog
app.get("/api/catalog/mnemonics/all", (req, res) => {
  try {
    const mnemonicos = getAllOfficialMnemonics();
    res.json({
      success: true,
      total: mnemonicos.length,
      mnemonicos,
    });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/catalog/mnemonics/stats", (req, res) => {
  try {
    const stats = getMnemonicCatalogStats();
    res.json({ success: true, stats });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// API Routes for Exploding and Consolidating Mnemonics into Materials
app.post(["/api/explode-mnemonics", "/api/catalog/mnemonics/explode"], (req, res) => {
  try {
    const rawData = req.body;
    const result = processOfficialMnemonics(rawData);
    res.json({
      success: true,
      result,
      groupedMnemonics: result.groupedMnemonics,
      materials: result.materials,
      structureItemMap: result.structureItemMap,
      mnemonicosNaoEncontrados: result.mnemonicosNaoEncontrados,
      catalogStats: result.catalogStats,
    });
  } catch (err: any) {
    console.error("Erro na explosão de mnemônicos:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// API Routes for IT-EO-008 Official Symbology Base
app.get("/api/simbologia/all", (req, res) => {
  try {
    const base = getSimbologiaOficial();
    res.json({ success: true, base });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/simbologia/categories", (req, res) => {
  try {
    const categories = getSimbologiaCategorias();
    res.json({ success: true, total: categories.length, categories });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/simbologia/category/:name", (req, res) => {
  try {
    const category = getSimbologiaPorCategoria(req.params.name);
    if (!category) {
      return res.status(404).json({ success: false, error: `Categoria '${req.params.name}' não reconhecida na IT-EO-008.` });
    }
    res.json({ success: true, categoryName: req.params.name, data: category });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/simbologia/stats", (req, res) => {
  try {
    const stats = getSimbologiaStats();
    res.json({ success: true, stats });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// API Routes for Controlled Symbology -> Mnemonic Association (ETAPA 2)
app.get("/api/simbologia-mnemonicos", (req, res) => {
  try {
    const associacoes = getAllAssociacoes();
    res.json({ success: true, total: associacoes.length, associacoes });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/simbologia-mnemonicos/stats", (req, res) => {
  try {
    const stats = getAssociacoesStats();
    res.json({ success: true, stats });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/simbologia-mnemonicos/mnemonico/:codigo", (req, res) => {
  try {
    const associacoes = getAssociacoesPorMnemonico(req.params.codigo);
    res.json({ success: true, mnemonico: req.params.codigo, total: associacoes.length, associacoes });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/simbologia-mnemonicos/:simboloId", (req, res) => {
  try {
    const associacao = getAssociacaoPorSimboloId(req.params.simboloId);
    if (!associacao) {
      return res.status(404).json({ success: false, error: `Símbolo '${req.params.simboloId}' não encontrado na base de associações.` });
    }
    res.json({ success: true, associacao });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================================
// API Routes for Recognition & Mapping Engine (ETAPA 3)
// ============================================================
app.post("/api/recognition-engine/process", (req, res) => {
  try {
    const { elementos, arquivoOrigem } = req.body;
    if (!Array.isArray(elementos)) {
      return res.status(400).json({
        success: false,
        error: "O campo 'elementos' deve ser um array de elementos detectados.",
      });
    }

    const relatorio = executarMotorReconhecimento(elementos, arquivoOrigem);
    res.json({ success: true, relatorio });
  } catch (err: any) {
    console.error("Erro no motor de reconhecimento:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================================
// API Routes for Official Symbol Recognition in Projects (ETAPA 5 & 6)
// ============================================================
app.post("/api/project-symbols/recognize", (req, res) => {
  try {
    const { candidatos, arquivoOrigem } = req.body;
    if (!Array.isArray(candidatos)) {
      return res.status(400).json({
        success: false,
        error: "O campo 'candidatos' deve ser um array de candidatos detectados no projeto.",
      });
    }

    const relatorio = reconhecerSimbologiaProjeto(candidatos, arquivoOrigem);
    res.json({ success: true, relatorio });
  } catch (err: any) {
    console.error("Erro no reconhecimento de símbolos em projetos:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/project-symbols/official-list", (req, res) => {
  try {
    const symbols = getAllOfficialSymbols();
    res.json({ success: true, total: symbols.length, symbols });
  } catch (err: any) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================================
// API Routes for Integrated Orchestration Flow (ETAPA 4 & 6)
// Fluxo: Projeto -> Simbologia Oficial -> Mnemônico -> Materiais
// ============================================================
app.post("/api/orchestration/process-project", (req, res) => {
  try {
    const { elementos, arquivoOrigem } = req.body;
    if (!Array.isArray(elementos)) {
      return res.status(400).json({
        success: false,
        error: "O campo 'elementos' deve ser um array de elementos detectados do projeto.",
      });
    }

    const resultado = processarProjetoComSimbologiaOficial(elementos, arquivoOrigem);
    res.json({ success: true, resultado });
  } catch (err: any) {
    console.error("Erro na orquestração oficial da Etapa 4:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * Extrator e reparador tolerante a falhas de JSON retornado por modelos de visão.
 * Lida com markdown em blocos, vírgulas residuais, comentários, strings truncadas,
 * objetos aninhados e formatos fora do padrão estrito.
 */
function extractAndRepairProjectJson(rawText: string): any {
  if (!rawText || typeof rawText !== "string") {
    return {
      detectedStructures: [],
      detectedPoles: [],
      detectedTransformers: [],
      detectedGuys: [],
      detectedCables: [],
      unrecognizedItems: [],
      generalSummary: "Nenhum elemento identificado na resposta do modelo.",
    };
  }

  // 1. Remove cercas de markdown e espaços
  let cleaned = rawText.trim();
  cleaned = cleaned.replace(/```(?:json)?/gi, "").replace(/```/g, "").trim();

  // 2. Tentativa direta
  try {
    const direct = JSON.parse(cleaned);
    if (direct && typeof direct === "object") return direct;
  } catch {}

  // 3. Extrai o bloco mais amplo entre { e } ou [ e ]
  const firstBrace = cleaned.indexOf("{");
  const lastBrace = cleaned.lastIndexOf("}");
  let candidate = cleaned;
  if (firstBrace !== -1 && lastBrace > firstBrace) {
    candidate = cleaned.slice(firstBrace, lastBrace + 1);
  } else {
    const firstBracket = cleaned.indexOf("[");
    const lastBracket = cleaned.lastIndexOf("]");
    if (firstBracket !== -1 && lastBracket > firstBracket) {
      candidate = cleaned.slice(firstBracket, lastBracket + 1);
    }
  }

  // 4. Limpeza de comentários JS e vírgulas residuais antes de fechamentos
  let sanitized = candidate
    .replace(/\/\/[^\n\r]*/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/,\s*([\}\]])/g, "$1");

  try {
    const parsed = JSON.parse(sanitized);
    if (parsed && typeof parsed === "object") return parsed;
  } catch {}

  // 5. Reparo de JSON truncado (caso o modelo atinja o limite de tokens antes de fechar colchetes)
  let repaired = sanitized;
  let inString = false;
  let escape = false;
  const openStack: string[] = [];

  for (let i = 0; i < repaired.length; i++) {
    const char = repaired[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (char === "\\") {
      escape = true;
      continue;
    }
    if (char === '"') {
      inString = !inString;
      continue;
    }
    if (!inString) {
      if (char === "{" || char === "[") {
        openStack.push(char);
      } else if (char === "}" || char === "]") {
        const last = openStack[openStack.length - 1];
        if ((char === "}" && last === "{") || (char === "]" && last === "[")) {
          openStack.pop();
        }
      }
    }
  }

  if (inString) {
    repaired += '"';
  }

  // Remove vírgula final pendente
  repaired = repaired.replace(/,\s*$/, "");

  // Fecha pilhas abertas
  while (openStack.length > 0) {
    const open = openStack.pop();
    if (open === "{") repaired += "}";
    if (open === "[") repaired += "]";
  }

  repaired = repaired.replace(/,\s*([\}\]])/g, "$1");

  try {
    const parsed = JSON.parse(repaired);
    if (parsed && typeof parsed === "object") return parsed;
  } catch {}

  // 6. Recuperação heurística por Regex: extrai qualquer poste, estrutura, transformador etc.
  const recovered: any = {
    detectedStructures: [],
    detectedPoles: [],
    detectedTransformers: [],
    detectedGuys: [],
    detectedCables: [],
    unrecognizedItems: [],
    generalSummary: "Extração resiliente por reconhecimento estrutural.",
  };

  const structMatches = cleaned.matchAll(/\{[^{}]*"(?:code|id)"\s*:\s*"([^"]+)"[^{}]*\}/gi);
  for (const match of structMatches) {
    try {
      const item = JSON.parse(match[0].replace(/,\s*([\}\]])/g, "$1"));
      if (item.code) {
        recovered.detectedStructures.push(item);
      } else if (item.typeSpec || item.shape) {
        recovered.detectedPoles.push(item);
      } else if (item.powerKva) {
        recovered.detectedTransformers.push(item);
      } else if (item.cableType) {
        recovered.detectedCables.push(item);
      }
    } catch {}
  }

  return recovered;
}

// Initialize Gemini client server-side lazily or safely
function getGeminiClient() {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) {
    return null;
  }
  return new GoogleGenAI({
    apiKey,
    httpOptions: {
      headers: {
        "User-Agent": "aistudio-build",
      },
    },
  });
}

// Cancelamento de Job de análise
app.post(["/api/analyze-project/cancel", "/analyze-project/cancel"], async (req, res) => {
  const { jobId } = req.body || {};
  if (!jobId) {
    return res.status(400).json({ success: false, error: "Parâmetro jobId é obrigatório para cancelamento." });
  }
  const cancelled = await jobStorage.cancelJob(jobId);
  return res.json({ success: true, cancelled, jobId });
});

// Consulta de status do Job de análise
app.get(
  ["/api/analyze-project/status", "/analyze-project/status", "/api/jobs/:jobId", "/jobs/:jobId"],
  async (req, res) => {
    const jobId = (req.query.jobId || req.params.jobId) as string;
    if (!jobId) {
      return res.status(400).json({ success: false, error: "Parâmetro jobId é obrigatório." });
    }
    const job = await jobStorage.getJob(jobId);
    if (!job) {
      return res.status(404).json({ success: false, error: `Job '${jobId}' não encontrado.` });
    }
    return res.json({
      success: true,
      jobId: job.jobId,
      status: job.status,
      progress: job.progress,
      stageMessage: job.stageMessage,
      telemetry: job.telemetry,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt,
      result: job.status === "COMPLETED" ? job.result : undefined,
      error: job.status === "FAILED" ? job.error : undefined,
    });
  }
);

// Endpoint de Execução Distribuída de Jobs (chamado por QStash / Webhook / Fila externa)
app.post(["/api/jobs/execute", "/api/queue/process"], async (req, res) => {
  // ==============================================================================
  // VALIDAÇÃO OFICIAL DE AUTENTICIDADE E ASSINATURA QSTASH (ETAPA 4E)
  // ==============================================================================
  const signatureHeader = (req.headers["upstash-signature"] || req.headers["Upstash-Signature"]) as string | undefined;

  if (process.env.NODE_ENV === "production" || qstashVerifier.isConfigured()) {
    // 1. Em produção ou quando as chaves de assinatura estiverem configuradas,
    // se o secret não estiver configurado no servidor, falha de forma segura (HTTP 401).
    if (!qstashVerifier.isConfigured()) {
      return res.status(401).json({
        success: false,
        error: "QSTASH_KEYS_NOT_CONFIGURED",
        message: "Chaves de validação do QStash não configuradas no servidor.",
      });
    }

    // 2. Se a assinatura não for informada, rejeita imediatamente com HTTP 401
    if (!signatureHeader) {
      return res.status(401).json({
        success: false,
        error: "UNAUTHORIZED_MISSING_SIGNATURE",
        message: "Cabeçalho 'Upstash-Signature' ausente na requisição.",
      });
    }

    // 3. Obtenção do corpo bruto (raw body) para verificação do hash SHA-256
    const rawBodyBuffer = (req as any).rawBody || Buffer.from(typeof req.body === "string" ? req.body : JSON.stringify(req.body || {}));
    
    // 4. Validação criptográfica HMAC-SHA256, expiração (replay attack) e emissor
    const verification = qstashVerifier.verifySignature(signatureHeader, rawBodyBuffer);
    if (!verification.isValid) {
      return res.status(401).json({
        success: false,
        error: "UNAUTHORIZED_INVALID_SIGNATURE",
        code: verification.code,
        message: verification.error || "Assinatura QStash inválida.",
      });
    }
  }

  const { jobId, attempt, instanceId } = req.body || {};

  if (!jobId || typeof jobId !== "string") {
    return res.status(400).json({
      success: false,
      error: "Parâmetro 'jobId' é obrigatório no corpo da requisição.",
    });
  }

  try {
    const outcome = await jobExecutionService.executeJob(jobId, {
      attempt: typeof attempt === "number" ? attempt : 1,
      instanceId: typeof instanceId === "string" ? instanceId : undefined,
    });

    if (outcome.success) {
      return res.status(200).json({
        success: true,
        jobId: outcome.jobId,
        status: outcome.status,
        executionId: outcome.executionId,
        durationMs: outcome.durationMs,
        idempotent: outcome.idempotent || false,
      });
    }

    if (outcome.status === "CANCELLED") {
      return res.status(200).json({
        success: false,
        status: "CANCELLED",
        jobId: outcome.jobId,
        error: outcome.error,
      });
    }

    // Erro transitório (ex: concorrência em andamento, lock temporário): HTTP 503 para retry pela fila
    if (outcome.errorType === "TRANSITORIO") {
      return res.status(503).json({
        success: false,
        status: outcome.status,
        jobId: outcome.jobId,
        errorType: outcome.errorType,
        error: outcome.error,
        retryable: true,
      });
    }

    // Erro definitivo (ex: job não encontrado, formato inválido, max retries): HTTP 422 sem retry inútil
    return res.status(422).json({
      success: false,
      status: outcome.status,
      jobId: outcome.jobId,
      errorType: outcome.errorType,
      error: outcome.error,
      retryable: false,
    });
  } catch (err: any) {
    console.error(`[JobExecutionEndpoint] Erro inesperado ao executar Job ${jobId}:`, err);
    return res.status(500).json({
      success: false,
      jobId,
      error: err?.message || "INTERNAL_SERVER_ERROR",
    });
  }
});

// API Route for Analyzing CEMIG Project Drawings
app.all(["/api/analyze-project", "/analyze-project"], async (req, res) => {
  // Allow GET with jobId query for status polling compatibility
  if (req.method === "GET") {
    const jobId = req.query.jobId as string;
    if (jobId) {
      const job = await jobStorage.getJob(jobId);
      if (!job) {
        return res.status(404).json({ success: false, error: `Job '${jobId}' não encontrado.` });
      }
      return res.json({
        success: true,
        jobId: job.jobId,
        status: job.status,
        progress: job.progress,
        stageMessage: job.stageMessage,
        telemetry: job.telemetry,
        createdAt: job.createdAt,
        updatedAt: job.updatedAt,
        result: job.status === "COMPLETED" ? job.result : undefined,
        error: job.status === "FAILED" ? job.error : undefined,
      });
    }
    return res.status(400).json({ error: "Parâmetro jobId é obrigatório para consulta GET." });
  }

  if (req.method !== "POST") {
    return res.status(405).json({ error: "Método não permitido. Utilize POST para análise de projetos." });
  }

  try {
    const {
      imageBase64,
      mimeType,
      fileName,
      voltageLevel,
      userId,
      userEmail,
      deviceSerial,
      async: isAsyncRequested,
    } = req.body || {};

    if (!imageBase64) {
      return res.status(400).json({ error: "Nenhuma imagem ou arquivo PDF fornecido." });
    }

    // Trial / User Status verification
    if (userId || userEmail) {
      const isEmailAdmin = String(userEmail || "").trim().toLowerCase() === ADMIN_EMAIL.toLowerCase();
      const user = userId ? UserService.getUserById(userId) : UserService.getUserByEmail(userEmail);
      const isUserAdmin = isEmailAdmin || (user && user.email.toLowerCase() === ADMIN_EMAIL.toLowerCase());

      // O administrador patricioaug@gmail.com tem acesso sempre liberado e o tempo de trial não é contabilizado
      if (!isUserAdmin && user) {
        const trialInfo = evaluateUserTrial(user, deviceSerial);
        if (trialInfo.isExpired) {
          return res.status(403).json({
            success: false,
            trialExpired: true,
            error: "Seu período de avaliação terminou. Entre em contato pelo e mail patricioaug@gmail.com para continuar.",
          });
        }
      }
    }

    const voltageLabel =
      voltageLevel === "34.5kV"
        ? "34,5 kV (MT Rural / Subtransmissão - Classe 35 kV)"
        : voltageLevel === "7.97kV"
        ? "7,97 kV (Monofásico MT / MRT - Classe 15 kV)"
        : voltageLevel === "19.9kV"
        ? "19,9 kV (Monofásico MRT 34,5 kV - Classe 35 kV)"
        : voltageLevel === "BT"
        ? "Baixa Tensão (BT 380/220V / 220/127V)"
        : voltageLevel === "AUTO"
        ? "Automático / Misto (Detectar do Projeto)"
        : "13,8 kV (Padrão MT CEMIG Urbano/Rural - Classe 15 kV)";

    const fileHash = pageCache.computeProjectHash(imageBase64, voltageLevel || "AUTO");

    // Fast check for Level 1 Project Cache HIT (Instant 100% token savings)
    const cachedProject = pageCache.getProjectCache(fileHash, voltageLevel || "AUTO");
    if (cachedProject) {
      console.log(`[Cache Hit] Resultado recuperado do cache SHA-256 para o projeto (${fileHash.slice(0, 10)}...). Economia de 100% de tokens de IA.`);
      return res.json({
        success: true,
        ...cachedProject.result,
        source: "cache",
        cacheHit: true,
        hash: fileHash,
      });
    }

    // Default to async job mode when requested by client or headers for zero-timeout Vercel serverless safety
    const useAsyncJob = isAsyncRequested === true || req.headers["x-async-job"] === "true";

    if (useAsyncJob) {
      const job = await jobStorage.createJob(fileHash, voltageLevel || "13.8kV", voltageLabel);

      // 1. Persiste o payload de entrada completo no Redis antes do despacho
      await jobStorage.saveJobPayload(job.jobId, {
        jobId: job.jobId,
        base64Data: imageBase64,
        mimeType: mimeType || "image/jpeg",
        fileName: fileName || "projeto.pdf",
        voltageLevel: voltageLevel || "13.8kV",
        voltageLabel,
        userId,
        createdAt: Date.now(),
      });

      // 2. Despacha o Job através da camada distribuída desacoplada (ETAPA 4B)
      const dispatchResult = await jobExecutionService.dispatchJob(job.jobId);

      // 3. Se falhar no despacho em ambiente de produção, falha de forma segura sem fingir execução
      if (!dispatchResult.enqueued && process.env.NODE_ENV === "production") {
        await jobStorage.markFailed(job.jobId, `DISPATCH_FAILED: ${dispatchResult.error || "UNKNOWN"}`);
        return res.status(500).json({
          success: false,
          error: `Falha no despacho distribuído do Job: ${dispatchResult.error}`,
          jobId: job.jobId,
        });
      }

      // 4. Retorna HTTP 202 imediatamente com metadados do Job
      return res.status(202).json({
        success: true,
        async: true,
        jobId: job.jobId,
        status: job.status,
        progress: job.progress,
        stageMessage: job.stageMessage,
        dispatcher: dispatchResult.dispatcherType,
        dispatchedAt: dispatchResult.dispatchedAt,
      });
    }

    // Synchronous execution mode (for tests or sync clients)
    const finalResult = await projectProcessingPipeline.executePipeline({
      base64Data: imageBase64,
      mimeType: mimeType || "image/jpeg",
      fileName: fileName || "projeto.pdf",
      voltageLevel: voltageLevel || "13.8kV",
      voltageLabel,
    });

    if (userId && finalResult && finalResult.officialProcessing) {
      UserService.saveCalculo({
        userId,
        dadosJson: {
          fileName: fileName || "Projeto",
          voltageLevel: voltageLevel || "13.8kV",
          structures: finalResult?.data?.detectedStructures?.length || 0,
          materials: finalResult?.officialProcessing?.materials?.length || 0,
        },
        cargaTermica: 0,
        metodo: "Explosão de Mnemônicos CEMIG",
      }).catch(console.error);
    }

    return res.json({
      success: true,
      ...finalResult,
    });
  } catch (err: any) {
    console.error("Error in /api/analyze-project:", err);
    const errMsg = String(err?.message || "");
    let friendlyMessage = "Erro ao analisar o arquivo do projeto.";

    let statusCode = 500;
    if (
      errMsg.includes("503") ||
      errMsg.includes("high demand") ||
      errMsg.includes("UNAVAILABLE") ||
      errMsg.includes("temporarily unavailable") ||
      errMsg.includes("overloaded") ||
      errMsg.includes("spikes in demand")
    ) {
      statusCode = 503;
      friendlyMessage =
        "O serviço de IA está temporariamente com alta demanda (503). Por favor, aguarde alguns segundos e tente novamente.";
    } else if (errMsg.includes("429") || errMsg.includes("RESOURCE_EXHAUSTED") || errMsg.includes("quota")) {
      statusCode = 429;
      friendlyMessage =
        "Limite de requisições temporário atingido (429). Por favor, aguarde alguns instantes e tente novamente.";
    } else if (errMsg.includes("JOB_CANCELLED")) {
      return res.status(200).json({ success: true, cancelled: true, message: "Processamento cancelado com sucesso." });
    }

    return res.status(statusCode).json({
      success: false,
      error: friendlyMessage,
      details: errMsg.slice(0, 300),
    });
  }
});

// Universal Express error handling middleware to always return structured JSON
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error("[Universal Server Error Handler]:", err);
  if (res.headersSent) {
    return next(err);
  }
  const status = err.status || err.statusCode || 500;
  res.status(status).json({
    success: false,
    error: err.message || "Ocorreu um erro interno no servidor ao processar a requisição.",
    status,
  });
});

async function startServer() {
  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    // No ambiente local de desenvolvimento, habilita auto-processamento no despachante simulado
    jobExecutionService.getSimulatedDispatcher().autoProcessInDev = true;
    const { createServer: createViteServer } = await import("vite");
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`Servidor CEMIG rodando na porta ${PORT}`);
  });

  server.keepAliveTimeout = 120000;
  server.headersTimeout = 125000;
  server.timeout = 180000;
}

const isVercel = Boolean(process.env.VERCEL || process.env.NOW_REGION);
const isTest = process.env.NODE_ENV === "test" || Boolean(process.env.IS_TEST);
if (!isVercel && !isTest) {
  startServer();
}

export { app };
export default app;
