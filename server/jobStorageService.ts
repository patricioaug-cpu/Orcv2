import fs from "fs";
import path from "path";
import crypto from "crypto";
import { redisService } from "./redisService.js";

export type JobStatus =
  | "QUEUED"
  | "PREPROCESSING"
  | "CLASSIFYING"
  | "PROCESSING"
  | "VALIDATING"
  | "FINALIZING"
  | "COMPLETED"
  | "FAILED"
  | "CANCELLED";

export interface JobTelemetry {
  fileHash: string;
  totalPages: number;
  relevantPages: number;
  discardedPages: number;
  cachedPages: number;
  geminiPages: number;
  geminiCalls: number;
  retriesCount: number;
  ambiguitiesCount: number;
  processingTimeMs: number;
  cacheHit?: boolean;
  promptTokens?: number;
  candidateTokens?: number;
  deterministicExtractionUsed?: boolean;
  modelUsed?: string;
}

export interface AnalysisJob {
  jobId: string;
  status: JobStatus;
  progress: number; // 0 to 100
  stageMessage: string;
  voltageLevel: string;
  voltageLabel: string;
  createdAt: number;
  updatedAt: number;
  lastHeartbeat?: number;
  attempts?: number;
  instanceId?: string;
  cancelled?: boolean;
  error?: string;
  telemetry: JobTelemetry;
  result?: any;
}

export interface JobInputPayload {
  jobId?: string;
  base64Data: string;
  mimeType: string;
  fileName?: string;
  voltageLevel?: string;
  voltageLabel?: string;
  userId?: string;
  createdAt?: number;
}

export class JobStorageService {
  private static instance: JobStorageService;
  // Memória volátil estritamente para testes locais em desenvolvimento quando variáveis Upstash não existem
  private localDevJobs = new Map<string, AnalysisJob>();
  private localDevCancelKeys = new Set<string>();
  private localDevPayloads = new Map<string, JobInputPayload>();

  public static getInstance(): JobStorageService {
    if (!JobStorageService.instance) {
      JobStorageService.instance = new JobStorageService();
    }
    return JobStorageService.instance;
  }

  private getJobKey(jobId: string): string {
    return `calcpro:job:${jobId}`;
  }

  private getPayloadKey(jobId: string): string {
    return `calcpro:job:${jobId}:payload`;
  }

  private getCancelKey(jobId: string): string {
    return `calcpro:job:${jobId}:cancelled`;
  }

  /**
   * Salva o payload de entrada do Job (dados de arquivo/imagem) no Redis com TTL seguro de 2h
   */
  public async saveJobPayload(jobId: string, payload: JobInputPayload): Promise<void> {
    if (!redisService.isConfigured()) {
      if (process.env.NODE_ENV === "production") {
        throw new Error(
          "[JobStorage] UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN não configurados. Armazenamento de payload requer Redis em produção."
        );
      }
      this.localDevPayloads.set(jobId, payload);
      return;
    }
    try {
      await redisService.set(this.getPayloadKey(jobId), JSON.stringify(payload), 7200);
    } catch (err: any) {
      console.error(`[JobStorage] Falha ao persistir payload do job ${jobId} no Redis:`, err?.message || err);
      throw err;
    }
  }

  /**
   * Recupera o payload de entrada do Job para execução pelo executor
   */
  public async getJobPayload(jobId: string): Promise<JobInputPayload | null> {
    if (!redisService.isConfigured()) {
      return this.localDevPayloads.get(jobId) || null;
    }
    try {
      const raw = await redisService.get<string | JobInputPayload>(this.getPayloadKey(jobId));
      if (!raw) return null;
      if (typeof raw === "string") {
        return JSON.parse(raw) as JobInputPayload;
      }
      return raw as JobInputPayload;
    } catch (err) {
      console.error(`[JobStorage] Erro ao buscar payload do job ${jobId}:`, err);
      return null;
    }
  }

  /**
   * Remove o payload de entrada do Job após processamento ou cancelamento
   */
  public async deleteJobPayload(jobId: string): Promise<void> {
    this.localDevPayloads.delete(jobId);
    if (!redisService.isConfigured()) {
      return;
    }
    try {
      await redisService.del(this.getPayloadKey(jobId));
    } catch {}
  }

  public getTtlForStatus(status: JobStatus): number {
    if (status === "COMPLETED") {
      return 86400; // 24 horas para Jobs concluídos com sucesso
    }
    // Estados em andamento (QUEUED, PREPROCESSING, CLASSIFYING, PROCESSING, VALIDATING, FINALIZING)
    // ou finalizados com falha/cancelamento: 2 horas
    return 7200;
  }

  /**
   * Obtém o TTL restante em segundos do Job no Redis
   */
  public async getJobTtl(jobId: string): Promise<number> {
    if (!redisService.isConfigured()) {
      return this.getTtlForStatus("QUEUED");
    }
    return await redisService.ttl(this.getJobKey(jobId));
  }

  public async createJob(
    fileHash: string,
    voltageLevel: string = "AUTO",
    voltageLabel: string = "Automático / Misto (Detectar do Projeto)"
  ): Promise<AnalysisJob> {
    const jobId = `job_${Date.now()}_${crypto.randomBytes(6).toString("hex")}`;
    const now = Date.now();
    const job: AnalysisJob = {
      jobId,
      status: "QUEUED",
      progress: 0,
      stageMessage: "Job enfileirado para processamento assíncrono...",
      voltageLevel,
      voltageLabel,
      createdAt: now,
      updatedAt: now,
      lastHeartbeat: now,
      attempts: 1,
      cancelled: false,
      telemetry: {
        fileHash,
        totalPages: 0,
        relevantPages: 0,
        discardedPages: 0,
        cachedPages: 0,
        geminiPages: 0,
        geminiCalls: 0,
        retriesCount: 0,
        ambiguitiesCount: 0,
        processingTimeMs: 0,
      },
    };

    if (!redisService.isConfigured()) {
      if (process.env.NODE_ENV === "production") {
        throw new Error(
          "[JobStorage] UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN não configurados. Armazenamento persistente de jobs requer Redis em produção."
        );
      }
      this.localDevJobs.set(job.jobId, job);
      return job;
    }

    await this.saveJobToRedis(job);
    return job;
  }

  public async getJob(jobId: string): Promise<AnalysisJob | null> {
    if (!redisService.isConfigured()) {
      if (process.env.NODE_ENV === "production") {
        throw new Error(
          "[JobStorage] UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN não configurados. Armazenamento persistente de jobs requer Redis em produção."
        );
      }
      return this.localDevJobs.get(jobId) || null;
    }

    try {
      const raw = await redisService.get<string | AnalysisJob>(this.getJobKey(jobId));
      if (!raw) return null;
      if (typeof raw === "string") {
        return JSON.parse(raw) as AnalysisJob;
      }
      return raw as AnalysisJob;
    } catch (err: any) {
      console.error(`[JobStorage] Erro ao recuperar job ${jobId} do Redis:`, err?.message || err);
      throw err;
    }
  }

  public async updateJob(job: AnalysisJob): Promise<void> {
    job.updatedAt = Date.now();
    job.lastHeartbeat = Date.now();
    if (!redisService.isConfigured()) {
      this.localDevJobs.set(job.jobId, job);
      return;
    }
    await this.saveJobToRedis(job);
  }

  public async updateStatus(
    jobId: string,
    status: JobStatus,
    progress: number,
    stageMessage: string,
    extra?: Partial<AnalysisJob>
  ): Promise<AnalysisJob | null> {
    const job = await this.getJob(jobId);
    if (!job) return null;

    job.status = status;
    job.progress = progress;
    job.stageMessage = stageMessage;
    job.updatedAt = Date.now();
    job.lastHeartbeat = Date.now();

    if (extra) {
      Object.assign(job, extra);
    }

    await this.updateJob(job);
    return job;
  }

  public async cancelJob(jobId: string): Promise<boolean> {
    if (!redisService.isConfigured()) {
      this.localDevCancelKeys.add(jobId);
      const job = this.localDevJobs.get(jobId);
      if (!job) return false;
      if (job.status === "COMPLETED" || job.status === "FAILED") return false;
      job.cancelled = true;
      job.status = "CANCELLED";
      job.stageMessage = "Processamento cancelado pelo usuário.";
      job.updatedAt = Date.now();
      this.localDevJobs.set(jobId, job);
      return true;
    }

    // Flag atômica de cancelamento no Redis para interrupção imediata entre instâncias
    try {
      await redisService.set(this.getCancelKey(jobId), "1", 7200);
    } catch (err) {
      console.warn(`[JobStorage] Erro ao registrar cancel key para ${jobId}:`, err);
    }

    const job = await this.getJob(jobId);
    if (!job) return false;

    if (job.status === "COMPLETED" || job.status === "FAILED") {
      return false;
    }

    job.cancelled = true;
    job.status = "CANCELLED";
    job.stageMessage = "Processamento cancelado pelo usuário.";
    job.updatedAt = Date.now();
    job.lastHeartbeat = Date.now();
    await this.saveJobToRedis(job);
    return true;
  }

  public async isJobCancelled(jobId: string): Promise<boolean> {
    if (!jobId) return false;

    if (!redisService.isConfigured()) {
      if (this.localDevCancelKeys.has(jobId)) return true;
      const job = this.localDevJobs.get(jobId);
      return job ? Boolean(job.cancelled || job.status === "CANCELLED") : false;
    }

    try {
      const hasCancelKey = await redisService.exists(this.getCancelKey(jobId));
      if (hasCancelKey) return true;
    } catch (err) {
      // Fallback para leitura do job
    }

    const job = await this.getJob(jobId);
    return job ? Boolean(job.cancelled || job.status === "CANCELLED") : false;
  }

  public async markCompleted(jobId: string, result: any): Promise<void> {
    const job = await this.getJob(jobId);
    if (!job) return;

    job.status = "COMPLETED";
    job.progress = 100;
    job.stageMessage = "Análise concluída com sucesso!";
    job.result = result;
    job.updatedAt = Date.now();
    job.lastHeartbeat = Date.now();

    await this.saveJobToRedis(job);

    // Limpa chave de cancelamento se existir
    if (redisService.isConfigured()) {
      try {
        await redisService.del(this.getCancelKey(jobId));
      } catch {}
    }
  }

  public async markFailed(jobId: string, error: string): Promise<void> {
    const job = await this.getJob(jobId);
    if (!job) return;

    job.status = "FAILED";
    job.error = error;
    job.stageMessage = `Falha no processamento: ${error}`;
    job.updatedAt = Date.now();
    job.lastHeartbeat = Date.now();

    await this.saveJobToRedis(job);
  }

  private async saveJobToRedis(job: AnalysisJob): Promise<void> {
    if (!redisService.isConfigured()) {
      if (process.env.NODE_ENV === "production") {
        throw new Error(
          "[JobStorage] UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN não configurados. Armazenamento persistente de jobs requer Redis em produção."
        );
      }
      console.warn(`[JobStorage] Redis não configurado no ambiente. Job ${job.jobId} não persistido em Redis.`);
      return;
    }

    const ttl = this.getTtlForStatus(job.status);
    const key = this.getJobKey(job.jobId);
    const payload = JSON.stringify(job);

    try {
      await redisService.set(key, payload, ttl);
    } catch (err: any) {
      console.error(`[JobStorage] Falha ao persistir job ${job.jobId} no Redis:`, err?.message || err);
      throw err;
    }
  }

  // ==========================================
  // DISTRIBUTED REDIS CONCURRENCY LOCK (Lease-based)
  // ==========================================
  // Allows configurable concurrent Gemini calls across all Vercel instances (default: 1)
  private readonly MAX_CONCURRENT = parseInt(process.env.GEMINI_MAX_CONCURRENCY || "1", 10);
  private readonly LOCK_LEASE_SECONDS = 45; // Auto-expire after 45s if worker crashes
  private activeLocksByJob = new Map<string, { slotKey: string; token: string }>();

  public async acquireGeminiSlot(jobId: string, timeoutMs: number = 30000): Promise<boolean> {
    const startTime = Date.now();

    // Se o job foi cancelado, aborta imediatamente
    if (await this.isJobCancelled(jobId)) {
      return false;
    }

    // Se já detém um slot nesta execução, reutiliza
    if (this.activeLocksByJob.has(jobId)) {
      return true;
    }

    // Verificação de ambiente Redis
    if (!redisService.isConfigured()) {
      if (process.env.NODE_ENV === "production") {
        throw new Error(
          "[DistributedLock] UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN não configurados. Lock distribuído não pode operar sem Redis na Vercel."
        );
      }
      console.warn(
        "[DistributedLock] Redis não configurado no ambiente local de desenvolvimento. Para concorrência distribuída em produção na Vercel, configure as variáveis Upstash."
      );
      this.activeLocksByJob.set(jobId, { slotKey: "calcpro:lock:gemini:slot:1", token: "local_dev" });
      return true;
    }

    const token = `${jobId}:${Date.now()}:${Math.random().toString(36).substring(2, 8)}`;

    while (Date.now() - startTime <= timeoutMs) {
      if (await this.isJobCancelled(jobId)) {
        return false;
      }

      // Tenta adquirir um dos slots globais (1 até MAX_CONCURRENT)
      for (let slotIndex = 1; slotIndex <= this.MAX_CONCURRENT; slotIndex++) {
        const slotKey = `calcpro:lock:gemini:slot:${slotIndex}`;
        try {
          const acquired = await redisService.setNx(slotKey, token, this.LOCK_LEASE_SECONDS);
          if (acquired) {
            this.activeLocksByJob.set(jobId, { slotKey, token });
            return true;
          }
        } catch (err: any) {
          // Erro de rede ou transitório, tenta o próximo slot ou backoff
        }
      }

      // Backoff com jitter para evitar concorrência simultânea agressiva
      const jitter = 200 + Math.floor(Math.random() * 250);
      await new Promise((res) => setTimeout(res, jitter));
    }

    // Timeout atingido sem conseguir slot livre
    return false;
  }

  public async releaseGeminiSlot(jobId: string): Promise<void> {
    const lockInfo = this.activeLocksByJob.get(jobId);
    if (!lockInfo) {
      return;
    }
    this.activeLocksByJob.delete(jobId);

    if (!redisService.isConfigured() || lockInfo.token === "local_dev") {
      return;
    }

    try {
      // Liberação atômica via Lua script: só remove a chave se o valor for estritamente igual ao token
      await redisService.releaseLockAtomic(lockInfo.slotKey, lockInfo.token);
    } catch (err: any) {
      console.warn(`[DistributedLock] Erro ao liberar slot ${lockInfo.slotKey} para job ${jobId}:`, err);
    }
  }
}

export const jobStorage = JobStorageService.getInstance();
