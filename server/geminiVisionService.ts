import { GoogleGenAI } from "@google/genai";
import { jobStorage } from "./jobStorageService";

export interface VisionExtractionResult {
  data: any;
  modelUsed: string;
  promptTokens?: number;
  candidateTokens?: number;
  retries: number;
}

/**
 * Optimized and resilient Gemini Vision service.
 * - Economical fast model (gemini-2.5-flash / gemini-3.1-flash-lite).
 * - Compact JSON schema output.
 * - maxOutputTokens reduced to 4096.
 * - Exponential backoff + jitter for 429/503/timeout (2s, 4s, 8s).
 * - Per-page resilience and cancellation checks.
 */
export class GeminiVisionService {
  private static instance: GeminiVisionService;

  public static getInstance(): GeminiVisionService {
    if (!GeminiVisionService.instance) {
      GeminiVisionService.instance = new GeminiVisionService();
    }
    return GeminiVisionService.instance;
  }

  private getClient(): GoogleGenAI | null {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) return null;
    return new GoogleGenAI({
      apiKey,
      httpOptions: {
        headers: {
          "User-Agent": "aistudio-build",
        },
      },
    });
  }

  /**
   * Interprets visual elements of an electrical sheet with token-efficient schema.
   */
  public async interpretSheet(
    imageBase64: string,
    mimeType: string = "image/jpeg",
    voltageLabel: string = "13,8 kV",
    jobId?: string
  ): Promise<VisionExtractionResult> {
    const ai = this.getClient();
    if (!ai) {
      throw new Error(
        "Chave de API GEMINI_API_KEY não configurada no ambiente. Configure a variável GEMINI_API_KEY no painel da Vercel (Project Settings > Environment Variables)."
      );
    }

    if (jobId && (await jobStorage.isJobCancelled(jobId))) {
      throw new Error("JOB_CANCELLED");
    }

    // Supported Gemini models: gemini-2.5-flash is ultra-fast with high quota limits, with gemini-flash-latest, gemini-3.8-flash, and flash-lite models as fallbacks
    const configuredPrimary = process.env.GEMINI_PRIMARY_MODEL?.trim();
    const defaultCascade = [
      "gemini-2.5-flash",
      "gemini-flash-latest",
      "gemini-3.8-flash",
      "gemini-2.5-flash-lite",
      "gemini-3.1-flash-lite",
    ];
    const models = configuredPrimary
      ? [configuredPrimary, ...defaultCascade.filter((m) => m !== configuredPrimary)]
      : defaultCascade;

    const compactPrompt = `Analise a prancha técnica do projeto elétrico CEMIG (Tensão: ${voltageLabel}).
EXTRAÇÃO DE REDE:
1. detectedPoles: [{id: "P1", typeSpec: "11-300", shape: "CIRCULAR", material: "CONCRETO", status: "INSTALAR", structures: ["N1", "CE1"]}]
2. detectedStructures: [{id: "P1", code: "N1", voltage: "MT", status: "INSTALAR", associatedPost: "11-300"}]
3. detectedEquipment: [{id: "EQ1", code: "CFS", type: "CHAVE_FUSIVEL", specification: "15kV 100A", associatedPole: "P1", status: "INSTALAR"}]
4. detectedTransformers: [{id: "TR1", associatedPole: "P2", powerKva: "45", voltage: "15kV", type: "TRIFASICO", status: "INSTALAR"}]
5. detectedGuys: [{id: "EST1", associatedPole: "P1", type: "ANCORA", quantity: 1, status: "INSTALAR"}]
6. detectedCables: [{cableType: "CAA 1/0 AWG", voltage: "MT", status: "INSTALAR", spansCount: 1, estimatedLengthMeters: 40}]

REGRAS:
- Retorne EXCLUSIVAMENTE JSON estruturado.
- Se houver demolição/retirada marcada, status="RETIRAR". Se novo, status="INSTALAR". Existente mantido, status="EXISTENTE".
- Não invente mnemônicos inexistentes.`;

    let totalRetries = 0;
    let lastError: any = null;
    const isVercelServerless = Boolean(process.env.VERCEL || process.env.IS_SERVERLESS);
    // On Vercel Serverless, limit timeout to 8.5s per attempt so function never crashes from platform timeout
    const MODEL_TIMEOUT_MS = isVercelServerless ? 8500 : 25000;

    for (let mIdx = 0; mIdx < models.length; mIdx++) {
      const modelName = models[mIdx];
      const MAX_RETRIES_PER_MODEL = isVercelServerless ? 1 : 2;
      const backoffDelays = [1000, 2500];

      for (let attempt = 0; attempt < MAX_RETRIES_PER_MODEL; attempt++) {
        if (jobId && (await jobStorage.isJobCancelled(jobId))) {
          throw new Error("JOB_CANCELLED");
        }

        try {
          console.log(`[GeminiVision] Chamando modelo ${modelName} (tentativa ${attempt + 1}/${MAX_RETRIES_PER_MODEL})...`);

          let timeoutTimer: NodeJS.Timeout | null = null;
          const timeoutPromise = new Promise<never>((_, reject) => {
            timeoutTimer = setTimeout(() => {
              reject(new Error(`Timeout de ${MODEL_TIMEOUT_MS / 1000}s atingido em ${modelName}`));
            }, MODEL_TIMEOUT_MS);
          });

          const rawData = (imageBase64.includes(";base64,") ? imageBase64.split(";base64,")[1] : imageBase64)
            .replace(/[\r\n\s]+/g, "")
            .trim();

          if (!rawData || rawData.length < 30) {
            throw new Error("Formato base64 de imagem ou prancha técnica vazio ou corrompido.");
          }

          let effectiveMime = mimeType;
          if (imageBase64.includes(";base64,")) {
            const extractedMime = imageBase64.split(";base64,")[0].replace("data:", "").trim();
            if (extractedMime) effectiveMime = extractedMime;
          }
          if (rawData.startsWith("JVBERi0") || rawData.startsWith("JVBERi")) {
            effectiveMime = "application/pdf";
          } else if (rawData.startsWith("iVBORw0KGgo")) {
            effectiveMime = "image/png";
          } else if (rawData.startsWith("/9j/")) {
            effectiveMime = "image/jpeg";
          } else if (!effectiveMime) {
            effectiveMime = "image/jpeg";
          }

          const modelCall = ai.models.generateContent({
            model: modelName,
            contents: {
              parts: [
                {
                  inlineData: {
                    mimeType: effectiveMime,
                    data: rawData,
                  },
                },
                {
                  text: compactPrompt,
                },
              ],
            },
            config: {
              responseMimeType: "application/json",
              maxOutputTokens: 4096, // Reduced from 16384 for substantial token savings and faster response
            },
          });

          const response = await Promise.race([modelCall, timeoutPromise]);
          if (timeoutTimer) clearTimeout(timeoutTimer);

          if (response && response.text) {
            const rawText = response.text;
            let parsedData: any = {};
            try {
              parsedData = JSON.parse(rawText.replace(/```json\s*|```/g, "").trim());
            } catch {
              // Extract first JSON object match
              const match = rawText.match(/\{[\s\S]*\}/);
              if (match) {
                parsedData = JSON.parse(match[0]);
              }
            }

            const usage = (response as any).usageMetadata || {};

            return {
              data: parsedData,
              modelUsed: modelName,
              promptTokens: usage.promptTokenCount,
              candidateTokens: usage.candidatesTokenCount,
              retries: totalRetries,
            };
          }
        } catch (err: any) {
          lastError = err;
          totalRetries++;
          const errMsg = String(err?.message || "").toLowerCase();

          // If quota / 429 / resource_exhausted, immediately skip to the next model without wasting retry attempts
          const isQuota =
            errMsg.includes("429") ||
            errMsg.includes("resource_exhausted") ||
            errMsg.includes("quota");

          if (isQuota) {
            console.warn(`[GeminiVision] Cota atingida no modelo ${modelName}. Alternando imediatamente para o próximo modelo...`);
            break;
          }

          const isTransient =
            errMsg.includes("503") ||
            errMsg.includes("overloaded") ||
            errMsg.includes("timeout") ||
            errMsg.includes("tempo limite");

          if (isTransient && attempt < MAX_RETRIES_PER_MODEL - 1 && !isVercelServerless) {
            const baseDelay = backoffDelays[attempt] || 1000;
            const jitter = Math.floor(Math.random() * 300);
            const waitTime = baseDelay + jitter;
            console.warn(`[GeminiVision] Erro transitório (${errMsg.slice(0, 70)}). Aguardando ${waitTime}ms para retry...`);
            await new Promise((res) => setTimeout(res, waitTime));
          } else {
            // Move to fallback model
            break;
          }
        }
      }
    }

    if (lastError) {
      const errStr = String(lastError?.message || "");
      if (errStr.includes("RESOURCE_EXHAUSTED") || errStr.includes("quota") || errStr.includes("429")) {
        throw new Error(
          "Cota de requisições da API Gemini temporariamente atingida (429/Quota Exceeded). Aguarde alguns instantes para nova tentativa ou atualize sua GEMINI_API_KEY no painel da Vercel."
        );
      }
    }

    throw lastError || new Error("Falha ao interpretar prancha após todas as tentativas.");
  }
}

export const geminiVision = GeminiVisionService.getInstance();
