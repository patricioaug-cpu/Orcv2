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
    return new GoogleGenAI({ apiKey });
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
      throw new Error("Chave de API GEMINI_API_KEY não configurada no ambiente.");
    }

    if (jobId && (await jobStorage.isJobCancelled(jobId))) {
      throw new Error("JOB_CANCELLED");
    }

    // Economical fast models first, reserving heavier models only as last resort
    const primaryModel = process.env.GEMINI_PRIMARY_MODEL || "gemini-2.5-flash";
    const fallbackModels = ["gemini-3.1-flash-lite", "gemini-flash-latest"];
    const models = [primaryModel, ...fallbackModels];

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

    for (let mIdx = 0; mIdx < models.length; mIdx++) {
      const modelName = models[mIdx];
      const MAX_RETRIES_PER_MODEL = 3;
      const backoffDelays = [2000, 4000, 8000];

      for (let attempt = 0; attempt < MAX_RETRIES_PER_MODEL; attempt++) {
        if (jobId && (await jobStorage.isJobCancelled(jobId))) {
          throw new Error("JOB_CANCELLED");
        }

        try {
          console.log(`[GeminiVision] Chamando modelo ${modelName} (tentativa ${attempt + 1}/${MAX_RETRIES_PER_MODEL})...`);

          const MODEL_TIMEOUT_MS = 28000;
          let timeoutTimer: NodeJS.Timeout | null = null;
          const timeoutPromise = new Promise<never>((_, reject) => {
            timeoutTimer = setTimeout(() => {
              reject(new Error(`Timeout de ${MODEL_TIMEOUT_MS / 1000}s atingido em ${modelName}`));
            }, MODEL_TIMEOUT_MS);
          });

          const modelCall = ai.models.generateContent({
            model: modelName,
            contents: {
              parts: [
                {
                  inlineData: {
                    mimeType: mimeType || "image/jpeg",
                    data: imageBase64.includes(";base64,") ? imageBase64.split(";base64,")[1] : imageBase64,
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
          const isTransient =
            errMsg.includes("429") ||
            errMsg.includes("503") ||
            errMsg.includes("resource_exhausted") ||
            errMsg.includes("overloaded") ||
            errMsg.includes("timeout") ||
            errMsg.includes("tempo limite");

          if (isTransient && attempt < MAX_RETRIES_PER_MODEL - 1) {
            // Exponential backoff + random jitter (200-600ms)
            const baseDelay = backoffDelays[attempt] || 4000;
            const jitter = Math.floor(Math.random() * 400);
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

    throw lastError || new Error("Falha ao interpretar prancha após todas as tentativas.");
  }
}

export const geminiVision = GeminiVisionService.getInstance();
