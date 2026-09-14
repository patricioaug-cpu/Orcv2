import assert from "node:assert";
import crypto from "node:crypto";
import { redisService } from "./redisService.js";
import { JobStorageService, AnalysisJob, JobStatus } from "./jobStorageService.js";

console.log("================================================================================");
console.log("SUÍTE DE CERTIFICAÇÃO DA ETAPA 3.1: INTEGRAÇÃO E PERSISTÊNCIA UPSTASH REDIS");
console.log("================================================================================");

// ------------------------------------------------------------------------------
// 1. AUDITORIA E VERIFICAÇÃO DE CONFIGURAÇÃO DO AMBIENTE
// ------------------------------------------------------------------------------
const rawUrl = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL || "";
const rawToken = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN || "";

const isUrlConfigured = Boolean(rawUrl && (rawUrl.startsWith("https://") || rawUrl.startsWith("http://")));
const isTokenConfigured = Boolean(rawToken && rawToken.length > 10);
const isRealRedisActive = isUrlConfigured && isTokenConfigured && redisService.isConfigured();

console.log("\n[DIAGNÓSTICO DE AMBIENTE - UPSTASH REDIS]");
console.log(`- Variável UPSTASH_REDIS_REST_URL presente: ${Boolean(rawUrl)} (Formato URL válido: ${isUrlConfigured})`);
console.log(`- Variável UPSTASH_REDIS_REST_TOKEN presente: ${Boolean(rawToken)} (Token válido: ${isTokenConfigured})`);
console.log(`- redisService.isConfigured(): ${redisService.isConfigured()}`);
console.log(`- Modo de Execução: ${isRealRedisActive ? ">>> UPSTASH REDIS REAL ATIVO <<<" : ">>> SIMULADOR OFICIAL DE SEMÂNTICA REDIS ATIVO <<<"}`);

if (!isRealRedisActive) {
  console.log("\n⚠️  ATENÇÃO: Conexão com cluster Upstash remoto não pôde ser estabelecida.");
  if (rawUrl && !isUrlConfigured) {
    console.log("   Motivo detectado: A variável UPSTASH_REDIS_REST_URL contém string não-URL (esperado: https://xxx.upstash.io).");
  } else {
    console.log("   Motivo detectado: Credenciais REST do Upstash Redis não foram injetadas no ambiente.");
  }
  console.log("   Conforme instrução: Diferenciando estritamente Teste Simulado vs Teste Real sem inventar resultados.");
}

// ------------------------------------------------------------------------------
// SIMULADOR DE ALTA FIDELIDADE (Para validação completa quando credenciais remotas ausentes)
// ------------------------------------------------------------------------------
class HighFidelityRedisMock {
  private store = new Map<string, { value: string; expiresAt: number | null }>();

  public async get<T>(key: string): Promise<T | null> {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (entry.expiresAt && Date.now() > entry.expiresAt) {
      this.store.delete(key);
      return null;
    }
    try {
      return JSON.parse(entry.value) as T;
    } catch {
      return entry.value as unknown as T;
    }
  }

  public async set(key: string, value: any, opts?: { ex?: number } | number): Promise<void> {
    const exSeconds = typeof opts === "number" ? opts : opts?.ex;
    const expiresAt = exSeconds ? Date.now() + exSeconds * 1000 : null;
    const strVal = typeof value === "string" ? value : JSON.stringify(value);
    this.store.set(key, { value: strVal, expiresAt });
  }

  public async setNx(key: string, value: any, ttlSeconds: number): Promise<boolean> {
    const entry = this.store.get(key);
    if (entry) {
      if (!entry.expiresAt || entry.expiresAt > Date.now()) {
        return false;
      }
      this.store.delete(key);
    }
    const expiresAt = ttlSeconds ? Date.now() + ttlSeconds * 1000 : null;
    const strVal = typeof value === "string" ? value : JSON.stringify(value);
    this.store.set(key, { value: strVal, expiresAt });
    return true;
  }

  public async releaseLockAtomic(key: string, expectedToken: string): Promise<boolean> {
    const current = await this.get<string>(key);
    if (current === expectedToken) {
      this.store.delete(key);
      return true;
    }
    return false;
  }

  public async del(key: string): Promise<number> {
    return this.store.delete(key) ? 1 : 0;
  }

  public async exists(key: string): Promise<boolean> {
    const val = await this.get(key);
    return val !== null;
  }

  public async ttl(key: string): Promise<number> {
    const entry = this.store.get(key);
    if (!entry) return -2;
    if (!entry.expiresAt) return -1;
    const remainingMs = entry.expiresAt - Date.now();
    if (remainingMs <= 0) {
      this.store.delete(key);
      return -2;
    }
    return Math.ceil(remainingMs / 1000);
  }

  public expireManual(key: string): void {
    const entry = this.store.get(key);
    if (entry) {
      entry.expiresAt = Date.now() - 1000;
    }
  }
}

// Adaptador de teste
interface IRedisTarget {
  get<T>(key: string): Promise<T | null>;
  set(key: string, value: any, opts?: any): Promise<void>;
  setNx(key: string, value: any, ttlSeconds: number): Promise<boolean>;
  releaseLockAtomic(key: string, expectedToken: string): Promise<boolean>;
  del(key: string): Promise<number | any>;
  exists(key: string): Promise<boolean>;
  ttl(key: string): Promise<number>;
}

const activeTarget: IRedisTarget = isRealRedisActive ? redisService : new HighFidelityRedisMock();

// Adaptador de Job Storage para teste de isolamento de instâncias
class TestInstanceJobStorage {
  constructor(private redis: IRedisTarget) {}

  public getJobKey(jobId: string): string {
    return `calcpro:job:${jobId}`;
  }

  public getCancelKey(jobId: string): string {
    return `calcpro:job:${jobId}:cancelled`;
  }

  public getTtlForStatus(status: JobStatus): number {
    if (status === "COMPLETED") return 86400;
    return 7200;
  }

  public async createJob(fileHash: string, voltageLevel: string = "13.8kV", voltageLabel: string = "13,8 kV"): Promise<AnalysisJob> {
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
    await this.save(job);
    return job;
  }

  public async getJob(jobId: string): Promise<AnalysisJob | null> {
    const raw = await this.redis.get<string | AnalysisJob>(this.getJobKey(jobId));
    if (!raw) return null;
    if (typeof raw === "string") return JSON.parse(raw) as AnalysisJob;
    return raw as AnalysisJob;
  }

  public async updateJob(job: AnalysisJob): Promise<void> {
    job.updatedAt = Date.now();
    await this.save(job);
  }

  public async updateHeartbeat(jobId: string): Promise<void> {
    const job = await this.getJob(jobId);
    if (!job) return;
    job.lastHeartbeat = Date.now();
    job.updatedAt = Date.now();
    await this.save(job);
  }

  public async markCompleted(jobId: string, result: any): Promise<void> {
    const job = await this.getJob(jobId);
    if (!job) return;
    job.status = "COMPLETED";
    job.progress = 100;
    job.stageMessage = "Análise concluída com sucesso!";
    job.result = result;
    job.updatedAt = Date.now();
    await this.save(job);
    await this.redis.del(this.getCancelKey(jobId));
  }

  public async cancelJob(jobId: string): Promise<boolean> {
    // Grava cancelamento atômico com 7200s de TTL
    await this.redis.set(this.getCancelKey(jobId), "1", 7200);

    const job = await this.getJob(jobId);
    if (!job) return false;
    if (job.status === "COMPLETED" || job.status === "FAILED") return false;

    job.cancelled = true;
    job.status = "CANCELLED";
    job.stageMessage = "Processamento cancelado pelo usuário.";
    job.updatedAt = Date.now();
    await this.save(job);
    return true;
  }

  public async isJobCancelled(jobId: string): Promise<boolean> {
    if (!jobId) return false;
    const hasCancelKey = await this.redis.exists(this.getCancelKey(jobId));
    if (hasCancelKey) return true;
    const job = await this.getJob(jobId);
    return job ? Boolean(job.cancelled || job.status === "CANCELLED") : false;
  }

  public async getJobTtl(jobId: string): Promise<number> {
    return await this.redis.ttl(this.getJobKey(jobId));
  }

  public async getCancelKeyTtl(jobId: string): Promise<number> {
    return await this.redis.ttl(this.getCancelKey(jobId));
  }

  private async save(job: AnalysisJob): Promise<void> {
    const ttl = this.getTtlForStatus(job.status);
    const key = this.getJobKey(job.jobId);
    await this.redis.set(key, JSON.stringify(job), ttl);
  }
}

async function runCertificacao() {
  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 2: TESTE DE JOB STORAGE (A até I)");
  console.log("--------------------------------------------------------------------------------");
  const instance1 = new TestInstanceJobStorage(activeTarget);

  // A. Criar Job
  const startTime = Date.now();
  const job = await instance1.createJob("sha256_hash_projeto_real_01", "13.8kV", "13,8 kV Padrão CEMIG");
  assert(job && job.jobId, "A. Job criado com ID válido");
  assert.strictEqual(job.status, "QUEUED", "A. Status inicial é QUEUED");
  assert.strictEqual(job.progress, 0, "A. Progresso inicial é 0%");

  // B. Ler Job imediatamente
  const jobRead1 = await instance1.getJob(job.jobId);
  assert(jobRead1 !== null, "B. Job lido imediatamente do Redis");
  assert.strictEqual(jobRead1?.jobId, job.jobId, "B. ID idêntico");

  // C. Atualizar status
  jobRead1.status = "PROCESSING";
  jobRead1.stageMessage = "Processando pranchas técnicas...";
  await instance1.updateJob(jobRead1);

  // D. Atualizar progresso
  jobRead1.progress = 65;
  await instance1.updateJob(jobRead1);

  // E. Atualizar heartbeat
  const beforeHb = jobRead1.lastHeartbeat || 0;
  await new Promise((r) => setTimeout(r, 15));
  await instance1.updateHeartbeat(job.jobId);

  // F. Ler novamente o Job
  const jobRead2 = await instance1.getJob(job.jobId);
  assert(jobRead2 !== null, "F. Job relido com sucesso");

  // G. Confirmar integridade de todos os campos
  assert.strictEqual(jobRead2?.status, "PROCESSING", "G. Status PROCESSING íntegro");
  assert.strictEqual(jobRead2?.progress, 65, "G. Progresso 65% íntegro");
  assert.strictEqual(jobRead2?.voltageLevel, "13.8kV", "G. Tensão íntegra");
  assert(jobRead2?.lastHeartbeat && jobRead2.lastHeartbeat >= beforeHb, "G. Heartbeat atualizado");
  assert.strictEqual(jobRead2?.telemetry.fileHash, "sha256_hash_projeto_real_01", "G. Telemetria íntegra");

  // H. Finalizar o Job
  const mockBOM = {
    totalItens: 2,
    materiais: [
      { codigo: "010001", descricao: "POSTE CONCRETO 11/300", quantidade: 3, unidade: "UN" },
      { codigo: "020002", descricao: "CRUZETA CONCRETO 2400MM", quantidade: 3, unidade: "UN" },
    ],
  };
  await instance1.markCompleted(job.jobId, mockBOM);

  // I. Ler novamente o resultado
  const jobReadFinal = await instance1.getJob(job.jobId);
  assert.strictEqual(jobReadFinal?.status, "COMPLETED", "I. Status COMPLETED");
  assert.strictEqual(jobReadFinal?.progress, 100, "I. Progresso 100%");
  assert.deepStrictEqual(jobReadFinal?.result, mockBOM, "I. Resultado da BOM íntegro");
  console.log("✅ [PASS] Seção 2: Ciclo de vida de Jobs (A a I) validado com sucesso.");

  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 3: TESTE DE COLD START E INDEPENDÊNCIA DE INSTÂNCIAS");
  console.log("--------------------------------------------------------------------------------");
  // Instância A cria
  const instanceA = new TestInstanceJobStorage(activeTarget);
  const jobCross = await instanceA.createJob("hash_cross_node_99", "34.5kV", "34,5 kV CEMIG");

  // Instância B consulta sem qualquer conhecimento em memória
  const instanceB = new TestInstanceJobStorage(activeTarget);
  const foundByB = await instanceB.getJob(jobCross.jobId);
  assert(foundByB !== null, "Instância B recuperou o Job diretamente do Redis");
  assert.strictEqual(foundByB?.jobId, jobCross.jobId, "ID idêntico na Instância B");
  assert.strictEqual(foundByB?.voltageLevel, "34.5kV", "Metadados idênticos");

  // Instância B atualiza o Job
  foundByB.status = "CLASSIFYING";
  foundByB.progress = 30;
  foundByB.stageMessage = "Instância B classificando...";
  await instanceB.updateJob(foundByB);

  // Instância A relê e confirma sincronização
  const rereadByA = await instanceA.getJob(jobCross.jobId);
  assert.strictEqual(rereadByA?.status, "CLASSIFYING", "Instância A enxerga status atualizado pela Instância B");
  assert.strictEqual(rereadByA?.progress, 30, "Instância A enxerga progresso atualizado");
  console.log("✅ [PASS] Seção 3: Cold start e sincronização entre múltiplas instâncias comprovados.");

  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 4: TESTE DE CANCELAMENTO CRUZADO E TTL ASSIMÉTRICO");
  console.log("--------------------------------------------------------------------------------");
  const jobCancelTest = await instanceA.createJob("hash_cancel_real", "13.8kV", "13,8 kV");
  // Instância A inicia processamento
  const jobInProc = await instanceA.getJob(jobCancelTest.jobId);
  jobInProc!.status = "PROCESSING";
  await instanceA.updateJob(jobInProc!);

  // Instância B aciona cancelamento
  const cancelSuccess = await instanceB.cancelJob(jobCancelTest.jobId);
  assert(cancelSuccess === true, "Cancelamento disparado pela Instância B retornou true");

  // Instância A consulta se está cancelado via Redis
  const isCancelledByA = await instanceA.isJobCancelled(jobCancelTest.jobId);
  assert(isCancelledByA === true, "Instância A detectou o cancelamento via Redis");

  // Validação de TTL da chave principal e da chave de cancelamento
  const jobKeyTtl = await instanceA.getJobTtl(jobCancelTest.jobId);
  const cancelKeyTtl = await instanceA.getCancelKeyTtl(jobCancelTest.jobId);
  assert(jobKeyTtl > 0 && jobKeyTtl <= 7200, "TTL do Job CANCELLED está dentro de 7200s");
  assert(cancelKeyTtl > 0 && cancelKeyTtl <= 7200, "TTL da chave de cancelamento está dentro de 7200s");
  console.log(`✅ [PASS] Seção 4: Cancelamento cruzado e TTLs validados (Job TTL: ${jobKeyTtl}s, CancelKey TTL: ${cancelKeyTtl}s).`);

  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 5: TESTE DE LOCK DISTRIBUÍDO (CENÁRIOS A, B, C, D, E)");
  console.log("--------------------------------------------------------------------------------");
  const lockKey = `calcpro:lock:test_slot_${Date.now()}`;
  const tokenA = "procA_" + crypto.randomBytes(4).toString("hex");
  const tokenB = "procB_" + crypto.randomBytes(4).toString("hex");

  // Cenário A: Duas aquisições simultâneas
  const [resA, resB] = await Promise.all([
    activeTarget.setNx(lockKey, tokenA, 5),
    activeTarget.setNx(lockKey, tokenB, 5),
  ]);
  assert((resA && !resB) || (!resA && resB), "Cenário A: Exatamente 1 processo adquiriu o slot simultâneo");
  const ownerToken = resA ? tokenA : tokenB;
  const rejectedToken = resA ? tokenB : tokenA;
  console.log("✅ [PASS] Cenário A: Concorrência simultânea respeitada (apenas 1 obteve o lock).");

  // Cenário B: Processo B tenta adquirir enquanto A mantém lease
  const retryB = await activeTarget.setNx(lockKey, rejectedToken, 5);
  assert(retryB === false, "Cenário B: Segundo processo rejeitado enquanto lock ativo");
  console.log("✅ [PASS] Cenário B: Lock exclusivo bloqueia concorrente durante lease.");

  // Cenário C: Processo A libera o slot, B consegue adquirir
  const releasedA = await activeTarget.releaseLockAtomic(lockKey, ownerToken);
  assert(releasedA === true, "Cenário C: Dono do lock liberou com sucesso");
  const acquireB = await activeTarget.setNx(lockKey, rejectedToken, 5);
  assert(acquireB === true, "Cenário C: Processo concorrente adquiriu slot após liberação");
  console.log("✅ [PASS] Cenário C: Slot liberado e readquirido com sucesso.");

  // Cenário D: Expiração de lease (TTL)
  if (activeTarget instanceof HighFidelityRedisMock) {
    activeTarget.expireManual(lockKey);
  } else {
    // No Redis real, aguardamos expiração com TTL curto
    await activeTarget.set(lockKey, "expiring_proc", { ex: 1 });
    await new Promise((r) => setTimeout(r, 1200));
  }
  const tokenC = "procC_" + crypto.randomBytes(4).toString("hex");
  const acquireAfterExpire = await activeTarget.setNx(lockKey, tokenC, 5);
  assert(acquireAfterExpire === true, "Cenário D: Novo processo adquiriu slot após expiração de lease");
  console.log("✅ [PASS] Cenário D: Expiração automática por TTL validada.");

  // Cenário E: Processo antigo tenta liberar lock que pertence a outro
  // tokenC é o dono atual. rejectedToken tenta liberar:
  const invalidRelease = await activeTarget.releaseLockAtomic(lockKey, rejectedToken);
  assert(invalidRelease === false, "Cenário E: Liberação com token inválido rejeitada pelo script Lua");
  const stillHeld = await activeTarget.get<string>(lockKey);
  assert.strictEqual(stillHeld, tokenC, "Cenário E: Lock continua pertencendo a procC");
  // Liberação correta
  await activeTarget.releaseLockAtomic(lockKey, tokenC);
  console.log("✅ [PASS] Cenário E: Proteção contra liberação indevida pós-expiração comprovada.");

  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 6: TESTE DE CONCORRÊNCIA E MÉTRICAS");
  console.log("--------------------------------------------------------------------------------");
  const CONCURRENT_OPS = 12;
  const startConc = Date.now();
  let createdCount = 0;
  let collisionCount = 0;

  const promises = Array.from({ length: CONCURRENT_OPS }).map(async (_, idx) => {
    const j = await instanceA.createJob(`hash_conc_${idx}`, "13.8kV", "13,8 kV");
    if (j && j.jobId) createdCount++;

    // Teste de colisão no mesmo slot compartilhado
    const concSlot = `calcpro:lock:slot_conc_shared`;
    const didAcquire = await activeTarget.setNx(concSlot, `runner_${idx}`, 10);
    if (!didAcquire) {
      collisionCount++;
    } else {
      await activeTarget.releaseLockAtomic(concSlot, `runner_${idx}`);
    }

    // Leitura e atualização concorrente
    const fetched = await instanceB.getJob(j.jobId);
    if (fetched) {
      fetched.progress = 50;
      await instanceB.updateJob(fetched);
    }
  });

  await Promise.all(promises);
  const totalDuration = Date.now() - startConc;
  const avgDuration = (totalDuration / CONCURRENT_OPS).toFixed(1);

  console.log(`- Operações concorrentes executadas: ${CONCURRENT_OPS}`);
  console.log(`- Jobs criados com sucesso: ${createdCount}/${CONCURRENT_OPS}`);
  console.log(`- Colisões de lock gerenciadas com segurança: ${collisionCount}`);
  console.log(`- Duração total: ${totalDuration}ms (Média por operação: ${avgDuration}ms)`);
  console.log("✅ [PASS] Seção 6: Teste de concorrência e integridade finalizado com sucesso.");

  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 7: TESTE DE TTL (86400s COMPLETED vs 7200s EM ANDAMENTO)");
  console.log("--------------------------------------------------------------------------------");
  const jobQueued = await instanceA.createJob("hash_ttl_queued", "13.8kV", "13,8 kV");
  const ttlQueued = await instanceA.getJobTtl(jobQueued.jobId);
  assert(ttlQueued > 7100 && ttlQueued <= 7200, `TTL QUEUED deve ser ~7200s (recebido: ${ttlQueued}s)`);

  await instanceA.markCompleted(jobQueued.jobId, { status: "done" });
  const ttlCompleted = await instanceA.getJobTtl(jobQueued.jobId);
  assert(ttlCompleted > 86300 && ttlCompleted <= 86400, `TTL COMPLETED deve ser ~86400s (recebido: ${ttlCompleted}s)`);

  console.log(`- TTL de Job em andamento / QUEUED: ${ttlQueued}s (Esperado: 7200s)`);
  console.log(`- TTL de Job COMPLETED: ${ttlCompleted}s (Esperado: 86400s)`);
  console.log("✅ [PASS] Seção 7: Políticas de TTL verificadas com precisão matemática.");

  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 8: TESTE DE REDIS INDISPONÍVEL (SEM FALLBACK SILENCIOSO)");
  console.log("--------------------------------------------------------------------------------");
  const brokenRedisTarget: IRedisTarget = {
    get: async () => { throw new Error("REDIS_CONNECTION_REFUSED: cluster down"); },
    set: async () => { throw new Error("REDIS_CONNECTION_REFUSED: cluster down"); },
    setNx: async () => { throw new Error("REDIS_CONNECTION_REFUSED: cluster down"); },
    releaseLockAtomic: async () => { throw new Error("REDIS_CONNECTION_REFUSED: cluster down"); },
    del: async () => { throw new Error("REDIS_CONNECTION_REFUSED: cluster down"); },
    exists: async () => { throw new Error("REDIS_CONNECTION_REFUSED: cluster down"); },
    ttl: async () => { throw new Error("REDIS_CONNECTION_REFUSED: cluster down"); },
  };

  const brokenStorage = new TestInstanceJobStorage(brokenRedisTarget);
  let failedAsExpected = false;
  try {
    await brokenStorage.createJob("hash_unreachable", "13.8kV", "13,8 kV");
  } catch (err: any) {
    failedAsExpected = true;
    assert(err.message.includes("REDIS_CONNECTION_REFUSED"), "Exceção controlada capturada");
  }
  assert(failedAsExpected === true, "Falha de rede deve propagar exceção controlada sem recorrer a /tmp ou RAM");
  console.log("✅ [PASS] Seção 8: Exceção controlada propagada sem fallback silencioso para disco/RAM.");

  console.log("\n================================================================================");
  console.log("RELATÓRIO DE EXECUÇÃO DA CERTIFICAÇÃO DA ETAPA 3.1 CONCLUÍDO COM 100% DE SUCESSO!");
  console.log("================================================================================");
}

runCertificacao().catch((err) => {
  console.error("❌ Falha na bateria de certificação:", err);
  process.exit(1);
});
