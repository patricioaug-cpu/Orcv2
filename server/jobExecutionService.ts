import crypto from "crypto";
import { redisService } from "./redisService.js";
import { jobStorage, AnalysisJob, JobStatus } from "./jobStorageService.js";
import { projectProcessingPipeline } from "./projectProcessingPipeline.js";
import { UserService } from "./userService.js";

/**
 * ==============================================================================
 * ETAPA 4A — INFRAESTRUTURA DE EXECUÇÃO DISTRIBUÍDA DOS JOBS
 * ==============================================================================
 * 
 * Responsabilidades:
 * 1. Interface abstrata JobDispatcher (desacoplada de Vercel Queues / QStash).
 * 2. Controle de ciclo de vida, idempotência e identificação de instâncias (instanceId, executionId).
 * 3. Heartbeat periódico compatível com serverless (não-bloqueante, cancelável em finally).
 * 4. Verificação rigorosa de cancelamento via Redis soberano.
 * 5. Utilização exclusiva do lock distribuído existente da ETAPA 2 (acquireGeminiSlot / releaseGeminiSlot).
 * 6. Classificação de erros transitórios vs definitivos.
 * 7. Executor dummy/de teste para isolamento estrito (SEM chamar pipeline real nesta etapa).
 * 8. Sanitização de credenciais e logs.
 */

// ==========================================
// CONSTANTES E TIMEOUTS DOCUMENTADOS
// ==========================================
/** Timeout para enfileirar/despachar o job para a fila ou webhook externo */
export const DISPATCH_TIMEOUT_MS = 10000; // 10 segundos

/** Timeout esperado para entrega da mensagem pelo despachante */
export const DELIVERY_TIMEOUT_MS = 30000; // 30 segundos

/** Timeout máximo aguardando slot do lock distribuído Gemini (da ETAPA 2) */
export const LOCK_ACQUISITION_TIMEOUT_MS = 35000; // 35 segundos

/** Tempo de expiração (TTL) do lease de execução para prevenir lock órfão em crash */
export const EXECUTION_LEASE_TTL_SEC = 300; // 5 minutos (300 segundos)

/** Intervalo entre batimentos de coração (heartbeats) da instância executora */
export const HEARTBEAT_INTERVAL_MS = 5000; // 5 segundos

/** Limiar de tempo sem heartbeat para considerar uma execução como abandonada */
export const ABANDONED_THRESHOLD_SEC = 120; // 2 minutos (120 segundos)

/** Número máximo de tentativas de re-execução para erros transitórios */
export const MAX_RETRY_ATTEMPTS = 3;

/** Expressão regular estrita para validação de formato seguro de jobId */
export const JOB_ID_REGEX = /^job_\d+_[a-f0-9]{6,}$/;

// ==========================================
// TIPOS E INTERFACES
// ==========================================
export type ExecutionErrorType = "TRANSITORIO" | "DEFINITIVO";

export interface ExecutionMetadata {
  executionId: string;
  jobId: string;
  instanceId: string;
  attempt: number;
  startedAt: number;
  lastHeartbeat: number;
  status: "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED" | "ABANDONED";
  leaseExpiresAt: number;
  errorType?: ExecutionErrorType;
  error?: string;
}

export interface DispatchOptions {
  attempt?: number;
  delaySec?: number;
  metadata?: Record<string, any>;
}

export interface DispatchResult {
  enqueued: boolean;
  jobId: string;
  dispatcherType: "SIMULATED" | "QSTASH" | "WEBHOOK";
  messageId?: string;
  dispatchedAt: number;
  attempt: number;
  error?: string;
}

export interface ExecutionOptions {
  instanceId?: string;
  attempt?: number;
  customExecutor?: JobTaskExecutor;
  lockTimeoutMs?: number;
  acquireGeminiSlotInExecutor?: boolean;
}

export interface ExecutionOutcome {
  success: boolean;
  jobId: string;
  status: JobStatus;
  executionId: string;
  instanceId: string;
  attempt: number;
  durationMs: number;
  errorType?: ExecutionErrorType;
  error?: string;
  idempotent?: boolean;
  result?: any;
}

export interface ExecutionContext {
  executionId: string;
  instanceId: string;
  attempt: number;
  signalHeartbeat: () => Promise<void>;
  isCancelled: () => Promise<boolean>;
}

/**
 * Assinatura do executor de tarefas plugável.
 * Na ETAPA 4A, utiliza estritamente executores de teste/dummy.
 */
export type JobTaskExecutor = (
  job: AnalysisJob,
  context: ExecutionContext
) => Promise<any>;

/**
 * Interface Abstrata do Despachante de Jobs
 */
export interface JobDispatcher {
  readonly type: "SIMULATED" | "QSTASH" | "WEBHOOK";
  enqueue(jobId: string, options?: DispatchOptions): Promise<DispatchResult>;
  execute(jobId: string, options?: ExecutionOptions): Promise<ExecutionOutcome>;
  getExecutionMetadata(jobId: string): Promise<ExecutionMetadata | null>;
}

// ==========================================
// UTILITÁRIO DE SEGURANÇA E LOGS
// ==========================================
export function sanitizeLog(message: string): string {
  if (!message) return "";
  return message
    .replace(/(Bearer\s+)[A-Za-z0-9_\-\.]{8,}/gi, "$1[REDACTED_TOKEN]")
    .replace(/(token=)[A-Za-z0-9_\-\.]{8,}/gi, "$1[REDACTED_TOKEN]")
    .replace(/(key=)[A-Za-z0-9_\-\.]{8,}/gi, "$1[REDACTED_KEY]")
    .replace(/(redis|rediss|https?):\/\/[^:]+:[^@]+@/gi, "$1://[REDACTED_USER_PASS]@")
    .replace(/AIzaSy[A-Za-z0-9_\-]{20,}/g, "[REDACTED_GEMINI_KEY]")
    .replace(/[a-zA-Z0-9_\-]{32,}/g, "[REDACTED_SECRET]");
}

/**
 * Classifica um erro ocorrido durante o ciclo de vida do job
 */
export function classifyExecutionError(err: any): ExecutionErrorType {
  const msg = String(err?.message || err || "").toLowerCase();
  
  // Erros definitivos: não adianta repetir
  if (
    msg.includes("job_not_found") ||
    msg.includes("job_already_cancelled") ||
    msg.includes("job_already_completed") ||
    msg.includes("invalid_payload") ||
    msg.includes("invalid_job_id") ||
    msg.includes("max_retries_exceeded") ||
    msg.includes("job não encontrado") ||
    msg.includes("cancelado pelo usuário")
  ) {
    return "DEFINITIVO";
  }

  // Erros transitórios: concorrência, timeouts, indisponibilidade temporária de rede ou lock
  return "TRANSITORIO";
}

// ==========================================
// SIMULADOR DETERMINÍSTICO DE DISPATCHER (Para testes e dev)
// ==========================================
export class SimulatedJobDispatcher implements JobDispatcher {
  public readonly type = "SIMULATED";
  private queuedJobs: Array<{ jobId: string; options?: DispatchOptions }> = [];
  private localLeases = new Map<string, { token: string; metadata: ExecutionMetadata; expiresAt: number }>();
  public autoProcessInDev: boolean = true;

  public getQueueLength(): number {
    return this.queuedJobs.length;
  }

  public clearQueue(): void {
    this.queuedJobs = [];
    this.localLeases.clear();
  }

  public async enqueue(jobId: string, options?: DispatchOptions): Promise<DispatchResult> {
    if (!JOB_ID_REGEX.test(jobId)) {
      return {
        enqueued: false,
        jobId,
        dispatcherType: this.type,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
        error: "INVALID_JOB_ID_FORMAT",
      };
    }

    this.queuedJobs.push({ jobId, options });

    if (this.autoProcessInDev && process.env.NODE_ENV !== "production" && !options?.metadata?.preventAutoProcess) {
      setImmediate(async () => {
        try {
          await jobExecutionService.executeJob(jobId, { attempt: options?.attempt || 1 });
        } catch (err) {
          console.error(`[SimulatedJobDispatcher] Erro ao auto-executar job ${jobId}:`, err);
        }
      });
    }

    return {
      enqueued: true,
      jobId,
      dispatcherType: this.type,
      messageId: `sim_msg_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
      dispatchedAt: Date.now(),
      attempt: options?.attempt || 1,
    };
  }

  public async execute(jobId: string, options?: ExecutionOptions): Promise<ExecutionOutcome> {
    return await jobExecutionService.executeJob(jobId, options);
  }

  public async getExecutionMetadata(jobId: string): Promise<ExecutionMetadata | null> {
    return await jobExecutionService.getExecutionMetadata(jobId);
  }

  // Métodos de lease em memória exclusivos para testes locais quando Redis não está conectado
  public acquireLocalLease(jobId: string, token: string, metadata: ExecutionMetadata, ttlSec: number): boolean {
    const existing = this.localLeases.get(jobId);
    const now = Date.now();
    if (existing && existing.expiresAt > now && existing.token !== token) {
      return false; // Bloqueado por outra instância ativa
    }
    this.localLeases.set(jobId, {
      token,
      metadata,
      expiresAt: now + ttlSec * 1000,
    });
    return true;
  }

  public renewLocalLease(jobId: string, token: string, ttlSec: number): boolean {
    const existing = this.localLeases.get(jobId);
    if (!existing || existing.token !== token) return false;
    existing.expiresAt = Date.now() + ttlSec * 1000;
    existing.metadata.lastHeartbeat = Date.now();
    return true;
  }

  public releaseLocalLease(jobId: string, token: string): boolean {
    const existing = this.localLeases.get(jobId);
    if (!existing || existing.token !== token) return false;
    this.localLeases.delete(jobId);
    return true;
  }

  public getLocalLease(jobId: string): ExecutionMetadata | null {
    const existing = this.localLeases.get(jobId);
    if (!existing) return null;
    if (existing.expiresAt < Date.now()) {
      existing.metadata.status = "ABANDONED";
    }
    return existing.metadata;
  }
}

// ==========================================
// DISPATCHER QSTASH / WEBHOOK (Para Vercel Serverless em Produção)
// ==========================================
export class QStashJobDispatcher implements JobDispatcher {
  public readonly type = "QSTASH";
  private qstashUrl: string;
  private qstashToken: string;
  private webhookBaseUrl: string;

  constructor() {
    this.qstashUrl = (process.env.QSTASH_URL || "https://qstash.upstash.io/v2/publish/").trim();
    this.qstashToken = (process.env.QSTASH_TOKEN || "").trim();
    this.webhookBaseUrl = (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : "http://localhost:3000").trim();
  }

  public isConfigured(): boolean {
    return Boolean(this.qstashToken && this.qstashToken.length > 5);
  }

  public async enqueue(jobId: string, options?: DispatchOptions): Promise<DispatchResult> {
    if (!JOB_ID_REGEX.test(jobId)) {
      return {
        enqueued: false,
        jobId,
        dispatcherType: this.type,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
        error: "INVALID_JOB_ID_FORMAT",
      };
    }

    if (!this.isConfigured()) {
      return {
        enqueued: false,
        jobId,
        dispatcherType: this.type,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
        error: "QSTASH_NOT_CONFIGURED",
      };
    }

    const destinationUrl = `${this.webhookBaseUrl}/api/jobs/execute`;
    const publishEndpoint = `${this.qstashUrl}${encodeURIComponent(destinationUrl)}`;

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), DISPATCH_TIMEOUT_MS);

      const response = await fetch(publishEndpoint, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.qstashToken}`,
          "Content-Type": "application/json",
          "Upstash-Deduplication-Id": `dedup_${jobId}_att${options?.attempt || 1}`,
          "Upstash-Retries": String(MAX_RETRY_ATTEMPTS),
          ...(options?.delaySec ? { "Upstash-Delay": `${options.delaySec}s` } : {}),
        },
        body: JSON.stringify({
          jobId,
          attempt: options?.attempt || 1,
          dispatchedAt: Date.now(),
          metadata: options?.metadata || {},
        }),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errText = await response.text();
        return {
          enqueued: false,
          jobId,
          dispatcherType: this.type,
          dispatchedAt: Date.now(),
          attempt: options?.attempt || 1,
          error: `HTTP_${response.status}: ${sanitizeLog(errText.slice(0, 150))}`,
        };
      }

      const resJson: any = await response.json().catch(() => ({}));
      return {
        enqueued: true,
        jobId,
        dispatcherType: this.type,
        messageId: resJson.messageId || `msg_${Date.now()}`,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
      };
    } catch (err: any) {
      return {
        enqueued: false,
        jobId,
        dispatcherType: this.type,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
        error: sanitizeLog(err?.message || String(err)),
      };
    }
  }

  public async execute(jobId: string, options?: ExecutionOptions): Promise<ExecutionOutcome> {
    return await jobExecutionService.executeJob(jobId, options);
  }

  public async getExecutionMetadata(jobId: string): Promise<ExecutionMetadata | null> {
    return await jobExecutionService.getExecutionMetadata(jobId);
  }
}

// ==========================================
// SERVIÇO PRINCIPAL DE EXECUÇÃO DE JOBS
// ==========================================
export class JobExecutionService {
  private static instance: JobExecutionService;
  private simulatedDispatcher = new SimulatedJobDispatcher();
  private qstashDispatcher = new QStashJobDispatcher();
  private activeHeartbeats = new Map<string, NodeJS.Timeout>();

  public static getInstance(): JobExecutionService {
    if (!JobExecutionService.instance) {
      JobExecutionService.instance = new JobExecutionService();
    }
    return JobExecutionService.instance;
  }

  /**
   * Retorna o dispatcher apropriado para o ambiente atual.
   * Se QStash estiver configurado em produção, utiliza QStash; caso contrário, utiliza o simulador seguro.
   */
  public getDispatcher(): JobDispatcher {
    if (this.qstashDispatcher.isConfigured() && process.env.NODE_ENV === "production") {
      return this.qstashDispatcher;
    }
    return this.simulatedDispatcher;
  }

  /**
   * Retorna a referência do simulador para fins de testes controlados
   */
  public getSimulatedDispatcher(): SimulatedJobDispatcher {
    return this.simulatedDispatcher;
  }

  /**
   * Chave Redis para o lease de execução da instância
   * 
   * DOCUMENTAÇÃO DA NOVA CHAVE DE EXECUÇÃO:
   * - Nome: calcpro:execution:{jobId}:lease
   * - Finalidade: Garantir exclusividade de execução entre múltiplas instâncias serverless (Vercel),
   *   evitando corrida ou execução duplicada simultânea do mesmo Job.
   * - TTL: 300 segundos (EXECUTION_LEASE_TTL_SEC), renovado monotonicamente pelo heartbeat.
   * - Proprietário: Instância que adquiriu o lease (instanceId + executionId).
   * - Criada: No início de executeJob().
   * - Removida/Finalizada: No bloco finally de executeJob().
   */
  public getExecutionLeaseKey(jobId: string): string {
    return `calcpro:execution:${jobId}:lease`;
  }

  /**
   * Despacha um Job previamente criado para a fila de execução
   */
  public async dispatchJob(jobId: string, options?: DispatchOptions): Promise<DispatchResult> {
    if (!JOB_ID_REGEX.test(jobId)) {
      return {
        enqueued: false,
        jobId,
        dispatcherType: this.getDispatcher().type,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
        error: "INVALID_JOB_ID_FORMAT",
      };
    }

    // Validação soberana: o Job DEVE existir no Job Storage Redis
    const job = await jobStorage.getJob(jobId);
    if (!job) {
      return {
        enqueued: false,
        jobId,
        dispatcherType: this.getDispatcher().type,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
        error: "JOB_NOT_FOUND",
      };
    }

    // Se já foi concluído ou cancelado, não despacha
    if (job.status === "COMPLETED") {
      return {
        enqueued: false,
        jobId,
        dispatcherType: this.getDispatcher().type,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
        error: "JOB_ALREADY_COMPLETED",
      };
    }

    if (job.status === "CANCELLED" || job.cancelled || (await jobStorage.isJobCancelled(jobId))) {
      return {
        enqueued: false,
        jobId,
        dispatcherType: this.getDispatcher().type,
        dispatchedAt: Date.now(),
        attempt: options?.attempt || 1,
        error: "JOB_ALREADY_CANCELLED",
      };
    }

    // Encaminha para o despachante ativo
    return await this.getDispatcher().enqueue(jobId, options);
  }

  /**
   * Executor padrão de Produção conectado ao pipeline real de engenharia.
   * Conecta a camada externa de execução distribuída ao projectProcessingPipeline
   * preservando integralmente todas as regras de engenharia, BOM, catálogos e cálculos.
   */
  public static defaultProductionExecutor: JobTaskExecutor = async (job, context) => {
    // 1. Recupera o payload de entrada do Job a partir do Redis
    const payload = await jobStorage.getJobPayload(job.jobId);
    if (!payload) {
      throw new Error(`PAYLOAD_NOT_FOUND: Dados de entrada do Job ${job.jobId} não encontrados no armazenamento.`);
    }

    // 2. Checkpoint de cancelamento
    if (await context.isCancelled()) {
      throw new Error("JOB_CANCELLED_BEFORE_PIPELINE");
    }

    await context.signalHeartbeat();

    // 3. Executa o pipeline real existente sem alterar a lógica de engenharia
    const result = await projectProcessingPipeline.executePipeline({
      base64Data: payload.base64Data,
      mimeType: payload.mimeType,
      fileName: payload.fileName || "projeto.pdf",
      voltageLevel: payload.voltageLevel || job.voltageLevel,
      voltageLabel: payload.voltageLabel || job.voltageLabel,
      jobId: job.jobId,
    });

    // 4. Salva cálculo no histórico do usuário se userId informado
    if (payload.userId && result && result.officialProcessing) {
      try {
        await UserService.saveCalculo({
          userId: payload.userId,
          dadosJson: {
            fileName: payload.fileName || "Projeto",
            voltageLevel: payload.voltageLevel || "13.8kV",
            structures: result?.data?.detectedStructures?.length || 0,
            materials: result?.officialProcessing?.materials?.length || 0,
          },
          cargaTermica: 0,
          metodo: "Explosão de Mnemônicos CEMIG",
        });
      } catch (err) {
        console.error("[JobExecutionService] Erro ao salvar cálculo do usuário:", err);
      }
    }

    // 5. Limpa os dados de payload do Redis após conclusão
    await jobStorage.deleteJobPayload(job.jobId);

    return result;
  };

  /**
   * Executor padrão Dummy para testes isolados da ETAPA 4A.
   * Não chama o pipeline real nem a API Gemini, conforme estrita determinação das regras.
   */
  public static defaultDummyExecutor: JobTaskExecutor = async (job, context) => {
    // Simula etapas graduais de processamento sem consumir IA real
    await jobStorage.updateStatus(
      job.jobId,
      "PROCESSING",
      25,
      "Executor isolado: iniciando processamento do Job..."
    );

    await context.signalHeartbeat();

    if (await context.isCancelled()) {
      throw new Error("JOB_CANCELLED_DURING_EXECUTION");
    }

    await jobStorage.updateStatus(
      job.jobId,
      "VALIDATING",
      75,
      "Executor isolado: validando integridade de dados e estruturas..."
    );

    await context.signalHeartbeat();

    if (await context.isCancelled()) {
      throw new Error("JOB_CANCELLED_DURING_EXECUTION");
    }

    return {
      success: true,
      dummyExecuted: true,
      jobId: job.jobId,
      executionId: context.executionId,
      instanceId: context.instanceId,
      attempt: context.attempt,
      timestamp: Date.now(),
      officialMnemonicsPreserved: 7203,
      materialsPreserved: 1558,
    };
  };

  /**
   * Executa um Job com controle estrito de concorrência, idempotência,
   * lease de execução, cancelamento e liberação garantida via try/finally.
   */
  public async executeJob(jobId: string, options?: ExecutionOptions): Promise<ExecutionOutcome> {
    const startTime = Date.now();
    const instanceId = options?.instanceId || `instance_${crypto.randomBytes(4).toString("hex")}`;
    const executionId = `exec_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
    const attempt = options?.attempt || 1;
    // 1. Validação estrutural do ID
    if (!JOB_ID_REGEX.test(jobId)) {
      return {
        success: false,
        jobId,
        status: "FAILED",
        executionId,
        instanceId,
        attempt,
        durationMs: Date.now() - startTime,
        errorType: "DEFINITIVO",
        error: "INVALID_JOB_ID_FORMAT",
      };
    }

    // 2. Recuperação soberana do Job no Redis
    const job = await jobStorage.getJob(jobId);
    if (!job) {
      return {
        success: false,
        jobId,
        status: "FAILED",
        executionId,
        instanceId,
        attempt,
        durationMs: Date.now() - startTime,
        errorType: "DEFINITIVO",
        error: "JOB_NOT_FOUND",
      };
    }

    // Seleção de executor: customExecutor se fornecido; senão defaultProductionExecutor se houver payload; senão defaultDummyExecutor para testes isolados
    const hasPayload = (await jobStorage.getJobPayload(jobId)) !== null;
    const taskExecutor =
      options?.customExecutor ||
      (hasPayload ? JobExecutionService.defaultProductionExecutor : JobExecutionService.defaultDummyExecutor);
    const lockTimeoutMs = options?.lockTimeoutMs || LOCK_ACQUISITION_TIMEOUT_MS;
    // Conforme ETAPA 4B: no pipeline real, Gemini Slot é adquirido/liberado estritamente em torno das chamadas de visão.
    // Em testes ou dummy, o lock Gemini é exercido pelo executor para validação de contrato de concorrência.
    const shouldAcquireGeminiSlot =
      options?.acquireGeminiSlotInExecutor ?? (taskExecutor !== JobExecutionService.defaultProductionExecutor);

    // 3. Verificação de Idempotência: Se o Job já está concluído, não reexecuta
    if (job.status === "COMPLETED") {
      return {
        success: true,
        jobId,
        status: "COMPLETED",
        executionId,
        instanceId,
        attempt,
        durationMs: Date.now() - startTime,
        idempotent: true,
        result: job.result,
      };
    }

    // 4. Verificação de Cancelamento Pré-Execução
    const isCancelledBefore = job.status === "CANCELLED" || job.cancelled || (await jobStorage.isJobCancelled(jobId));
    if (isCancelledBefore) {
      return {
        success: false,
        jobId,
        status: "CANCELLED",
        executionId,
        instanceId,
        attempt,
        durationMs: Date.now() - startTime,
        errorType: "DEFINITIVO",
        error: "JOB_ALREADY_CANCELLED",
      };
    }

    // 5. Verificação de Limite de Tentativas
    if (attempt > MAX_RETRY_ATTEMPTS) {
      const err = `Limite máximo de tentativas (${MAX_RETRY_ATTEMPTS}) excedido.`;
      await jobStorage.markFailed(jobId, err);
      return {
        success: false,
        jobId,
        status: "FAILED",
        executionId,
        instanceId,
        attempt,
        durationMs: Date.now() - startTime,
        errorType: "DEFINITIVO",
        error: "MAX_RETRIES_EXCEEDED",
      };
    }

    // 6. Aquisição do Lease de Execução (Evita duas instâncias processando o mesmo Job simultaneamente)
    const leaseKey = this.getExecutionLeaseKey(jobId);
    const leaseToken = `${instanceId}:${executionId}`;
    const leaseMetadata: ExecutionMetadata = {
      executionId,
      jobId,
      instanceId,
      attempt,
      startedAt: Date.now(),
      lastHeartbeat: Date.now(),
      status: "RUNNING",
      leaseExpiresAt: Date.now() + EXECUTION_LEASE_TTL_SEC * 1000,
    };

    let leaseAcquired = false;
    if (redisService.isConfigured()) {
      try {
        leaseAcquired = await redisService.setNx(leaseKey, JSON.stringify(leaseMetadata), EXECUTION_LEASE_TTL_SEC);
      } catch (err: any) {
        return {
          success: false,
          jobId,
          status: job.status,
          executionId,
          instanceId,
          attempt,
          durationMs: Date.now() - startTime,
          errorType: "TRANSITORIO",
          error: `REDIS_LEASE_ERROR: ${sanitizeLog(err?.message)}`,
        };
      }
    } else {
      leaseAcquired = this.simulatedDispatcher.acquireLocalLease(
        jobId,
        leaseToken,
        leaseMetadata,
        EXECUTION_LEASE_TTL_SEC
      );
    }

    if (!leaseAcquired) {
      // Outra instância está processando este Job neste exato momento
      return {
        success: false,
        jobId,
        status: job.status,
        executionId,
        instanceId,
        attempt,
        durationMs: Date.now() - startTime,
        errorType: "TRANSITORIO",
        error: "CONCURRENT_EXECUTION_IN_PROGRESS",
      };
    }

    // 7. Aquisição do Lock Distribuído Global da ETAPA 2 (acquireGeminiSlot) se solicitado
    let slotAcquired = false;
    if (shouldAcquireGeminiSlot) {
      try {
        slotAcquired = await jobStorage.acquireGeminiSlot(jobId, lockTimeoutMs);
      } catch (err: any) {
        await this.releaseExecutionLease(jobId, leaseToken);
        return {
          success: false,
          jobId,
          status: job.status,
          executionId,
          instanceId,
          attempt,
          durationMs: Date.now() - startTime,
          errorType: "TRANSITORIO",
          error: `LOCK_ACQUISITION_FAILED: ${sanitizeLog(err?.message)}`,
        };
      }

      if (!slotAcquired) {
        await this.releaseExecutionLease(jobId, leaseToken);
        const wasCancelled = await jobStorage.isJobCancelled(jobId);
        return {
          success: false,
          jobId,
          status: wasCancelled ? "CANCELLED" : job.status,
          executionId,
          instanceId,
          attempt,
          durationMs: Date.now() - startTime,
          errorType: wasCancelled ? "DEFINITIVO" : "TRANSITORIO",
          error: wasCancelled ? "JOB_CANCELLED_WHILE_WAITING_LOCK" : "LOCK_ACQUISITION_TIMEOUT",
        };
      }
    }

    // 8. Início do Heartbeat Não-Bloqueante
    this.startHeartbeat(jobId, leaseToken, leaseMetadata);

    try {
      // Atualiza metadados no Job no Redis
      job.instanceId = instanceId;
      job.attempts = attempt;
      job.lastHeartbeat = Date.now();
      job.status = "PROCESSING";
      job.stageMessage = `Processamento iniciado pela instância ${instanceId} (Tentativa ${attempt})...`;
      await jobStorage.updateJob(job);

      // Contexto da execução entregue ao executor
      const context: ExecutionContext = {
        executionId,
        instanceId,
        attempt,
        signalHeartbeat: async () => {
          await this.tickHeartbeat(jobId, leaseToken, leaseMetadata);
        },
        isCancelled: async () => {
          return await jobStorage.isJobCancelled(jobId);
        },
      };

      // Execução da tarefa
      const result = await taskExecutor(job, context);

      // Verificação final de cancelamento
      if (await jobStorage.isJobCancelled(jobId)) {
        await jobStorage.cancelJob(jobId);
        leaseMetadata.status = "CANCELLED";
        return {
          success: false,
          jobId,
          status: "CANCELLED",
          executionId,
          instanceId,
          attempt,
          durationMs: Date.now() - startTime,
          errorType: "DEFINITIVO",
          error: "JOB_CANCELLED_DURING_EXECUTION",
        };
      }

      // Marcação de conclusão no Job Storage oficial
      await jobStorage.markCompleted(jobId, result);
      leaseMetadata.status = "COMPLETED";

      return {
        success: true,
        jobId,
        status: "COMPLETED",
        executionId,
        instanceId,
        attempt,
        durationMs: Date.now() - startTime,
        result,
      };
    } catch (err: any) {
      const errMsg = sanitizeLog(err?.message || String(err));
      const errorType = classifyExecutionError(errMsg);

      if (errMsg.includes("JOB_CANCELLED")) {
        await jobStorage.cancelJob(jobId);
        leaseMetadata.status = "CANCELLED";
        return {
          success: false,
          jobId,
          status: "CANCELLED",
          executionId,
          instanceId,
          attempt,
          durationMs: Date.now() - startTime,
          errorType: "DEFINITIVO",
          error: "JOB_CANCELLED",
        };
      }

      await jobStorage.markFailed(jobId, errMsg);
      leaseMetadata.status = "FAILED";
      leaseMetadata.error = errMsg;
      leaseMetadata.errorType = errorType;

      return {
        success: false,
        jobId,
        status: "FAILED",
        executionId,
        instanceId,
        attempt,
        durationMs: Date.now() - startTime,
        errorType,
        error: errMsg,
      };
    } finally {
      // GARANTIA ABSOLUTA DE LIBERAÇÃO DE RECURSOS EM TRY/FINALLY:
      // 1. Interrompe heartbeat imediatamente
      this.stopHeartbeat(jobId);

      // 2. Libera o slot de concorrência Gemini se adquirido neste nível
      if (shouldAcquireGeminiSlot && slotAcquired) {
        try {
          await jobStorage.releaseGeminiSlot(jobId);
        } catch (releaseErr) {
          console.warn(`[JobExecutionService] Erro ao liberar slot Gemini para ${jobId}:`, releaseErr);
        }
      }

      // 3. Libera ou atualiza lease de execução
      try {
        await this.releaseExecutionLease(jobId, leaseToken);
      } catch (leaseErr) {
        console.warn(`[JobExecutionService] Erro ao liberar lease de execução para ${jobId}:`, leaseErr);
      }
    }
  }

  /**
   * Dispara um batimento cardíaco manual ou periódico
   */
  private async tickHeartbeat(jobId: string, leaseToken: string, metadata: ExecutionMetadata): Promise<void> {
    metadata.lastHeartbeat = Date.now();
    metadata.leaseExpiresAt = Date.now() + EXECUTION_LEASE_TTL_SEC * 1000;

    // Atualiza heartbeat no Job oficial
    const job = await jobStorage.getJob(jobId);
    if (job && job.status !== "COMPLETED" && job.status !== "FAILED" && job.status !== "CANCELLED") {
      job.lastHeartbeat = Date.now();
      await jobStorage.updateJob(job);
    }

    // Renova lease no Redis
    if (redisService.isConfigured()) {
      try {
        const leaseKey = this.getExecutionLeaseKey(jobId);
        await redisService.set(leaseKey, JSON.stringify(metadata), EXECUTION_LEASE_TTL_SEC);
      } catch {}
    } else {
      this.simulatedDispatcher.renewLocalLease(jobId, leaseToken, EXECUTION_LEASE_TTL_SEC);
    }
  }

  /**
   * Inicia o temporizador de heartbeat não-bloqueante
   */
  private startHeartbeat(jobId: string, leaseToken: string, metadata: ExecutionMetadata): void {
    this.stopHeartbeat(jobId);

    const timer = setInterval(() => {
      this.tickHeartbeat(jobId, leaseToken, metadata).catch(() => {});
    }, HEARTBEAT_INTERVAL_MS);

    // Garante que o timer não impeça o término do processo Node
    if (timer.unref) {
      timer.unref();
    }

    this.activeHeartbeats.set(jobId, timer);
  }

  /**
   * Interrompe o heartbeat
   */
  private stopHeartbeat(jobId: string): void {
    const timer = this.activeHeartbeats.get(jobId);
    if (timer) {
      clearInterval(timer);
      this.activeHeartbeats.delete(jobId);
    }
  }

  /**
   * Libera o lease exclusivo de execução
   */
  private async releaseExecutionLease(jobId: string, leaseToken: string): Promise<void> {
    if (redisService.isConfigured()) {
      const leaseKey = this.getExecutionLeaseKey(jobId);
      try {
        await redisService.del(leaseKey);
      } catch {}
    } else {
      this.simulatedDispatcher.releaseLocalLease(jobId, leaseToken);
    }
  }

  /**
   * Recupera metadados da execução ativa ou mais recente
   */
  public async getExecutionMetadata(jobId: string): Promise<ExecutionMetadata | null> {
    if (redisService.isConfigured()) {
      const leaseKey = this.getExecutionLeaseKey(jobId);
      try {
        const raw = await redisService.get<string | ExecutionMetadata>(leaseKey);
        if (!raw) return null;
        if (typeof raw === "string") {
          return JSON.parse(raw);
        }
        return raw as ExecutionMetadata;
      } catch {
        return null;
      }
    } else {
      return this.simulatedDispatcher.getLocalLease(jobId);
    }
  }

  /**
   * Diagnóstico de Execução Abandonada:
   * Verifica se o Job está em status de execução (PROCESSING, CLASSIFYING, etc.)
   * porém com último heartbeat anterior ao limiar de tolerância (ABANDONED_THRESHOLD_SEC).
   */
  public async checkExecutionAbandonment(jobId: string): Promise<{
    isAbandoned: boolean;
    lastHeartbeatAgeSec: number;
    status: JobStatus | "NOT_FOUND";
    instanceId?: string;
  }> {
    const job = await jobStorage.getJob(jobId);
    if (!job) {
      return { isAbandoned: false, lastHeartbeatAgeSec: 0, status: "NOT_FOUND" };
    }

    if (job.status === "COMPLETED" || job.status === "FAILED" || job.status === "CANCELLED" || job.status === "QUEUED") {
      return { isAbandoned: false, lastHeartbeatAgeSec: 0, status: job.status, instanceId: job.instanceId };
    }

    const execMeta = await this.getExecutionMetadata(jobId);
    const now = Date.now();
    const lastBeat = execMeta?.lastHeartbeat !== undefined
      ? execMeta.lastHeartbeat
      : (job.lastHeartbeat || job.updatedAt || job.createdAt);
    const ageSec = Math.floor((now - lastBeat) / 1000);

    const isAbandoned = ageSec >= ABANDONED_THRESHOLD_SEC;

    return {
      isAbandoned,
      lastHeartbeatAgeSec: ageSec,
      status: job.status,
      instanceId: execMeta?.instanceId || job.instanceId,
    };
  }
}

export const jobExecutionService = JobExecutionService.getInstance();
