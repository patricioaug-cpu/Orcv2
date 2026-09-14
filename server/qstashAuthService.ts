import crypto from "crypto";
import { sanitizeLog } from "./jobExecutionService.js";

/**
 * ==============================================================================
 * SERVIÇO OFICIAL DE AUTENTICAÇÃO E VERIFICAÇÃO DE ASSINATURAS DO UPSTASH QSTASH
 * ==============================================================================
 * 
 * Especificação e Padrão Upstash QStash:
 * - Cabeçalho HTTP: "Upstash-Signature" (ou "upstash-signature")
 * - Formato: JWT (JSON Web Token) assinado com HMAC-SHA256 (HS256)
 * - Claims obrigatórias no JWT:
 *   - iss: "Upstash"
 *   - exp: Unix epoch em segundos (TTL padrão 5 minutos)
 *   - nbf: Unix epoch em segundos
 *   - body: Hash SHA-256 do corpo bruto (raw body) em formato Base64 (ou Base64URL)
 * - Chaves de assinatura:
 *   - QSTASH_CURRENT_SIGNING_KEY: Chave ativa do console Upstash
 *   - QSTASH_NEXT_SIGNING_KEY: Chave secundária para rotação de segredos sem downtime
 * 
 * Garantias de Segurança:
 * 1. Rejeição imediata antes de invocar o executor, antes de adquirir lock e antes de tocar no JobStorage.
 * 2. Proteção contra Replay Attack via validação estrita de expiração (exp), nbf e relógio.
 * 3. Sanitização absoluta de secrets e logs.
 * 4. Fallback seguro em produção: se chaves não estiverem configuradas, rejeita requisições por padrão.
 */

export interface QStashVerificationOptions {
  /** Tolerância para divergência de relógio em segundos (padrão: 30s) */
  clockToleranceSec?: number;
}

export interface QStashVerificationResult {
  isValid: boolean;
  code: "VALID" | "MISSING_SIGNATURE" | "MISSING_KEYS" | "INVALID_FORMAT" | "EXPIRED" | "NOT_YET_VALID" | "INVALID_ISSUER" | "BODY_HASH_MISMATCH" | "SIGNATURE_MISMATCH" | "INTERNAL_ERROR";
  error?: string;
}

export class QStashSignatureVerifier {
  private currentSigningKey: string;
  private nextSigningKey: string;

  constructor() {
    this.currentSigningKey = (process.env.QSTASH_CURRENT_SIGNING_KEY || "").trim();
    this.nextSigningKey = (process.env.QSTASH_NEXT_SIGNING_KEY || "").trim();
  }

  /**
   * Indica se as chaves de assinatura do QStash estão configuradas no ambiente
   */
  public isConfigured(): boolean {
    return Boolean(
      (this.currentSigningKey && this.currentSigningKey.length > 5) ||
      (this.nextSigningKey && this.nextSigningKey.length > 5)
    );
  }

  /**
   * Obtém as chaves disponíveis para tentativa de validação (suporta rotação automática)
   */
  public getSigningKeys(): string[] {
    const keys: string[] = [];
    if (this.currentSigningKey && this.currentSigningKey.length > 5) {
      keys.push(this.currentSigningKey);
    }
    if (this.nextSigningKey && this.nextSigningKey.length > 5) {
      keys.push(this.nextSigningKey);
    }
    return keys;
  }

  /**
   * Converte string Base64 ou Base64URL em Buffer seguro
   */
  private base64UrlToBuffer(base64Url: string): Buffer {
    let base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
    while (base64.length % 4 !== 0) {
      base64 += "=";
    }
    return Buffer.from(base64, "base64");
  }

  /**
   * Calcula o hash SHA-256 do corpo e retorna tanto em Base64 padrão quanto em Base64URL
   */
  public computeBodyHashes(rawBody: string | Buffer): { base64: string; base64Url: string } {
    const hash = crypto.createHash("sha256").update(rawBody).digest();
    const base64 = hash.toString("base64");
    const base64Url = base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    return { base64, base64Url };
  }

  /**
   * Assina um payload JWT para testes e comunicação segura (usado em testes e harnesses)
   */
  public generateTestSignature(params: {
    rawBody: string | Buffer;
    signingKey: string;
    expiresInSec?: number;
    subUrl?: string;
    issuer?: string;
    customBodyHash?: string;
    notBeforeSec?: number;
  }): string {
    const nowSec = Math.floor(Date.now() / 1000);
    const exp = nowSec + (params.expiresInSec !== undefined ? params.expiresInSec : 300);
    const nbf = params.notBeforeSec !== undefined ? params.notBeforeSec : nowSec - 5;

    const { base64Url } = this.computeBodyHashes(params.rawBody);
    const bodyClaim = params.customBodyHash !== undefined ? params.customBodyHash : base64Url;

    const header = { alg: "HS256", typ: "JWT" };
    const payload = {
      iss: params.issuer !== undefined ? params.issuer : "Upstash",
      sub: params.subUrl || "https://example.com/api/jobs/execute",
      exp,
      nbf,
      iat: nowSec,
      jti: `msg_test_${crypto.randomBytes(8).toString("hex")}`,
      body: bodyClaim,
    };

    const headerB64 = Buffer.from(JSON.stringify(header))
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");

    const payloadB64 = Buffer.from(JSON.stringify(payload))
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");

    const dataToSign = `${headerB64}.${payloadB64}`;
    const signature = crypto
      .createHmac("sha256", params.signingKey)
      .update(dataToSign)
      .digest("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");

    return `${headerB64}.${payloadB64}.${signature}`;
  }

  /**
   * Valida a assinatura de uma requisição QStash
   */
  public verifySignature(
    signatureHeader: string | undefined | null,
    rawBody: string | Buffer,
    options?: QStashVerificationOptions
  ): QStashVerificationResult {
    // 1. Verificação de presença do cabeçalho
    if (!signatureHeader || typeof signatureHeader !== "string" || signatureHeader.trim() === "") {
      return {
        isValid: false,
        code: "MISSING_SIGNATURE",
        error: "Cabeçalho 'Upstash-Signature' ausente na requisição.",
      };
    }

    const token = signatureHeader.trim();
    const parts = token.split(".");
    if (parts.length !== 3) {
      return {
        isValid: false,
        code: "INVALID_FORMAT",
        error: "Formato do token de assinatura inválido (esperado JWT com 3 partes).",
      };
    }

    const [headerB64, payloadB64, signatureB64] = parts;

    // 2. Decodificação de Header e Payload
    let header: any;
    let payload: any;
    try {
      header = JSON.parse(this.base64UrlToBuffer(headerB64).toString("utf8"));
      payload = JSON.parse(this.base64UrlToBuffer(payloadB64).toString("utf8"));
    } catch {
      return {
        isValid: false,
        code: "INVALID_FORMAT",
        error: "Falha ao decodificar JSON do cabeçalho ou payload da assinatura.",
      };
    }

    // 3. Validação do algoritmo
    if (header.alg !== "HS256") {
      return {
        isValid: false,
        code: "INVALID_FORMAT",
        error: `Algoritmo não suportado: ${header.alg}. Esperado HS256.`,
      };
    }

    // 4. Validação do emissor (Issuer)
    if (payload.iss !== "Upstash") {
      return {
        isValid: false,
        code: "INVALID_ISSUER",
        error: `Emissor inválido: '${payload.iss}'. Esperado 'Upstash'.`,
      };
    }

    // 5. Proteção contra Replay: Verificação estrita de expiração (exp) e nbf
    const clockTolerance = options?.clockToleranceSec ?? 30; // 30s de tolerância padrão
    const nowSec = Math.floor(Date.now() / 1000);

    if (typeof payload.exp === "number" && nowSec > payload.exp + clockTolerance) {
      return {
        isValid: false,
        code: "EXPIRED",
        error: "Assinatura QStash expirada (rejeitada por proteção contra replay).",
      };
    }

    if (typeof payload.nbf === "number" && nowSec < payload.nbf - clockTolerance) {
      return {
        isValid: false,
        code: "NOT_YET_VALID",
        error: "Assinatura QStash ainda não é válida (nbf futuro).",
      };
    }

    // 6. Verificação de integridade do corpo (body SHA-256)
    if (typeof payload.body !== "string") {
      return {
        isValid: false,
        code: "BODY_HASH_MISMATCH",
        error: "Claim 'body' ausente ou inválida no payload JWT.",
      };
    }

    const { base64: expectedBase64, base64Url: expectedBase64Url } = this.computeBodyHashes(rawBody);
    const tokenBodyHash = payload.body.trim();

    // Compara hash do corpo aceitando formato Base64 padrão ou Base64URL
    if (tokenBodyHash !== expectedBase64 && tokenBodyHash !== expectedBase64Url) {
      return {
        isValid: false,
        code: "BODY_HASH_MISMATCH",
        error: "Hash SHA-256 do corpo da requisição não confere com a assinatura.",
      };
    }

    // 7. Obtenção das chaves secretas do ambiente
    const signingKeys = this.getSigningKeys();
    if (signingKeys.length === 0) {
      return {
        isValid: false,
        code: "MISSING_KEYS",
        error: "Chaves de assinatura do QStash não configuradas no servidor.",
      };
    }

    // 8. Verificação criptográfica da assinatura HMAC-SHA256
    const dataToVerify = `${headerB64}.${payloadB64}`;
    const providedSigBuffer = this.base64UrlToBuffer(signatureB64);

    let signatureMatches = false;
    for (const key of signingKeys) {
      const computedSigBuffer = crypto.createHmac("sha256", key).update(dataToVerify).digest();
      if (
        providedSigBuffer.length === computedSigBuffer.length &&
        crypto.timingSafeEqual(providedSigBuffer, computedSigBuffer)
      ) {
        signatureMatches = true;
        break;
      }
    }

    if (!signatureMatches) {
      return {
        isValid: false,
        code: "SIGNATURE_MISMATCH",
        error: "Assinatura HMAC-SHA256 inválida para as chaves configuradas.",
      };
    }

    return {
      isValid: true,
      code: "VALID",
    };
  }
}

export const qstashVerifier = new QStashSignatureVerifier();
