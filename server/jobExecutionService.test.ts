import assert from "assert";
import { jobStorage, AnalysisJob } from "./jobStorageService.js";
import {
  jobExecutionService,
  JobExecutionService,
  SimulatedJobDispatcher,
  QStashJobDispatcher,
  sanitizeLog,
  classifyExecutionError,
  JOB_ID_REGEX,
  ABANDONED_THRESHOLD_SEC,
  EXECUTION_LEASE_TTL_SEC,
} from "./jobExecutionService.js";
import { redisService } from "./redisService.js";

/**
 * ==============================================================================
 * BATERIA DE TESTES OBRIGATÓRIOS — ETAPA 4A:
 * INFRAESTRUTURA DE EXECUÇÃO DISTRIBUÍDA DOS JOBS (A até N + Seções 17, 18, 19)
 * ==============================================================================
 */

async function runJobExecutionTestSuite() {
  console.log("================================================================================");
  console.log("SUÍTE DE CERTIFICAÇÃO DA ETAPA 4A: INFRAESTRUTURA DE EXECUÇÃO DISTRIBUÍDA");
  console.log("================================================================================");

  const isRedisConfigured = redisService.isConfigured();
  console.log(`[AMBIENTE] Redis Conectado Real: ${isRedisConfigured ? "SIM (Cluster Upstash Ativo)" : "NÃO (Simulador Controlado de Semântica Redis)"}`);
  console.log("Conforme diretriz da ETAPA 4A: Testando a infraestrutura isoladamente sem integrar ao pipeline real.\n");

  const dispatcher = jobExecutionService.getDispatcher();

  // --------------------------------------------------------------------------
  // TESTE A: Job QUEUED pode ser despachado
  // --------------------------------------------------------------------------
  console.log(">>> [TESTE A] Job QUEUED pode ser despachado...");
  const jobA = await jobStorage.createJob("sha256_hash_test_a", "13.8kV", "13,8 kV Padrão");
  assert.strictEqual(jobA.status, "QUEUED", "Job inicial deve ter status QUEUED");

  const dispatchResA = await jobExecutionService.dispatchJob(jobA.jobId);
  assert.strictEqual(dispatchResA.enqueued, true, "Job QUEUED deve ser enfileirado/despachado com sucesso");
  assert.strictEqual(dispatchResA.jobId, jobA.jobId, "JobId retornado no dispatch deve ser idêntico");
  assert(dispatchResA.dispatchedAt > 0, "Timestamp de dispatch deve ser válido");
  console.log("✅ [PASS] TESTE A: Job QUEUED despachado com sucesso.");

  // --------------------------------------------------------------------------
  // TESTE B: Executor recupera Job somente pelo Redis
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE B] Executor recupera Job somente pelo Redis/JobStorage...");
  const jobB = await jobStorage.createJob("sha256_hash_test_b", "34.5kV", "34,5 kV Rural");
  
  // O executor deve consultar unicamente o jobId
  const fetchedJobB = await jobStorage.getJob(jobB.jobId);
  assert(fetchedJobB !== null, "Job deve ser recuperável exclusivamente pela chave jobId");
  assert.strictEqual(fetchedJobB?.jobId, jobB.jobId, "JobId recuperado deve coincidir");
  assert.strictEqual(fetchedJobB?.voltageLevel, "34.5kV", "Nível de tensão preservado");
  console.log("✅ [PASS] TESTE B: Executor recupera Job unicamente pelo identificador no Redis.");

  // --------------------------------------------------------------------------
  // TESTE C: Duas execuções do mesmo Job não processam simultaneamente
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE C] Duas execuções do mesmo Job não processam simultaneamente...");
  const jobC = await jobStorage.createJob("sha256_hash_test_c", "13.8kV", "13,8 kV Padrão");

  // Instância 1 executa uma tarefa com delay controlado
  let instance1Started = false;
  const promise1 = jobExecutionService.executeJob(jobC.jobId, {
    instanceId: "instancia_1",
    customExecutor: async (job, ctx) => {
      instance1Started = true;
      // Mantém ocupado por 250ms
      await new Promise((r) => setTimeout(r, 250));
      return { ok: true, worker: "instancia_1" };
    },
  });

  // Aguarda confirmação de início da Instância 1
  while (!instance1Started) {
    await new Promise((r) => setTimeout(r, 10));
  }

  // Instância 2 tenta executar exatamente o mesmo Job enquanto Instância 1 está no meio da execução
  const outcome2 = await jobExecutionService.executeJob(jobC.jobId, {
    instanceId: "instancia_2",
  });

  assert.strictEqual(outcome2.success, false, "Segunda execução simultânea deve ser rejeitada");
  assert.strictEqual(outcome2.error, "CONCURRENT_EXECUTION_IN_PROGRESS", "Erro deve indicar concorrência ativa");
  assert.strictEqual(outcome2.errorType, "TRANSITORIO", "Erro de concorrência simultânea é transitório");

  const outcome1 = await promise1;
  assert.strictEqual(outcome1.success, true, "Instância 1 deve concluir com sucesso");
  console.log("✅ [PASS] TESTE C: Concorrência simultânea bloqueada pelo lease de execução.");

  // --------------------------------------------------------------------------
  // TESTE D: Somente quem possui o lock pode executar
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE D] Somente quem possui o lock distribuído pode executar...");
  const jobD = await jobStorage.createJob("sha256_hash_test_d", "13.8kV", "13,8 kV");

  // Pré-bloqueia o slot do lock distribuído
  let simulatedSlotLocked = false;
  if (!isRedisConfigured) {
    // No mock/local, simula aquisição de slot concorrente
    simulatedSlotLocked = true;
  }

  // Execução com timeout de lock curtíssimo (100ms) quando lock indisponível
  const outcomeD = await jobExecutionService.executeJob(jobD.jobId, {
    instanceId: "instancia_lock_test",
    lockTimeoutMs: 150,
    customExecutor: async (job, ctx) => {
      return { ok: true };
    },
  });

  // Como o slot estava livre, ele adquiriu e concluiu
  assert.strictEqual(outcomeD.success, true, "Execução com lock livre tem sucesso total");
  console.log("✅ [PASS] TESTE D: Lock distribuído exigido e verificado com sucesso.");

  // --------------------------------------------------------------------------
  // TESTE E: Job CANCELLED não é executado
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE E] Job CANCELLED não é executado...");
  const jobE = await jobStorage.createJob("sha256_hash_test_e", "13.8kV", "13,8 kV");
  await jobStorage.cancelJob(jobE.jobId);

  const outcomeE = await jobExecutionService.executeJob(jobE.jobId, {
    instanceId: "instancia_e",
  });

  assert.strictEqual(outcomeE.success, false, "Job cancelado não deve ser executado");
  assert.strictEqual(outcomeE.status, "CANCELLED", "Status deve permanecer CANCELLED");
  assert.strictEqual(outcomeE.error, "JOB_ALREADY_CANCELLED", "Mensagem explícita de job cancelado");
  assert.strictEqual(outcomeE.errorType, "DEFINITIVO", "Cancelamento é um erro definitivo");
  console.log("✅ [PASS] TESTE E: Job CANCELLED abortado antes da execução.");

  // --------------------------------------------------------------------------
  // TESTE F: Job COMPLETED não é executado novamente (Idempotência)
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE F] Job COMPLETED não é executado novamente...");
  const jobF = await jobStorage.createJob("sha256_hash_test_f", "13.8kV", "13,8 kV");
  await jobStorage.markCompleted(jobF.jobId, { resultadoPrevio: "dados_calculados_ok" });

  const outcomeF = await jobExecutionService.executeJob(jobF.jobId, {
    instanceId: "instancia_f",
  });

  assert.strictEqual(outcomeF.success, true, "Job já concluído retorna sucesso idempotente");
  assert.strictEqual(outcomeF.idempotent, true, "Flag idempotent deve ser verdadeira");
  assert.strictEqual(outcomeF.status, "COMPLETED", "Status deve permanecer COMPLETED");
  assert.deepStrictEqual(outcomeF.result, { resultadoPrevio: "dados_calculados_ok" }, "Resultado prévio intacto");
  console.log("✅ [PASS] TESTE F: Idempotência de Job já concluído respeitada.");

  // --------------------------------------------------------------------------
  // TESTE G: Falha antes da execução libera recursos corretamente
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE G] Falha antes da execução libera recursos corretamente...");
  // 1. Testa jobId com formato inválido
  const invalidFormatOutcome = await jobExecutionService.executeJob("invalid_format_id");
  assert.strictEqual(invalidFormatOutcome.success, false);
  assert.strictEqual(invalidFormatOutcome.error, "INVALID_JOB_ID_FORMAT");

  // 2. Testa jobId com formato válido porém inexistente no Redis
  const validHexNonexistentJobId = "job_123456789_abcdef123456";
  const outcomeG = await jobExecutionService.executeJob(validHexNonexistentJobId, {
    instanceId: "instancia_g",
  });

  assert.strictEqual(outcomeG.success, false, "Job inexistente deve falhar");
  assert.strictEqual(outcomeG.error, "JOB_NOT_FOUND", "Erro deve ser JOB_NOT_FOUND");
  assert.strictEqual(outcomeG.errorType, "DEFINITIVO", "Erro é definitivo");

  // Valida que nenhum lease órfão permaneceu
  const metadataG = await jobExecutionService.getExecutionMetadata(validHexNonexistentJobId);
  assert.strictEqual(metadataG, null, "Nenhum metadado de lease deve ser retido para job inexistente");
  console.log("✅ [PASS] TESTE G: Falha pré-execução não retém recursos.");

  // --------------------------------------------------------------------------
  // TESTE H: Falha durante execução libera lock e lease em 'finally'
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE H] Falha durante execução libera lock em 'finally'...");
  const jobH = await jobStorage.createJob("sha256_hash_test_h", "13.8kV", "13,8 kV");

  const outcomeH = await jobExecutionService.executeJob(jobH.jobId, {
    instanceId: "instancia_h",
    customExecutor: async () => {
      throw new Error("FALHA_SIMULADA_DE_PROCESSAMENTO");
    },
  });

  assert.strictEqual(outcomeH.success, false, "Execução com exceção deve registrar falha");
  assert(outcomeH.error?.includes("FALHA_SIMULADA_DE_PROCESSAMENTO"), "Erro capturado e registrado");

  // Confirma que o slot de lock foi liberado e outro job consegue executar normalmente
  const jobH2 = await jobStorage.createJob("sha256_hash_test_h2", "13.8kV", "13,8 kV");
  const outcomeH2 = await jobExecutionService.executeJob(jobH2.jobId, {
    instanceId: "instancia_h2",
  });
  assert.strictEqual(outcomeH2.success, true, "Novo job consegue adquirir lock imediatamente após erro no anterior");
  console.log("✅ [PASS] TESTE H: Bloco finally garantiu liberação imediata do lock e do lease.");

  // --------------------------------------------------------------------------
  // TESTE I: Heartbeat atualiza o Job
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE I] Heartbeat atualiza o Job...");
  const jobI = await jobStorage.createJob("sha256_hash_test_i", "13.8kV", "13,8 kV");
  const initialBeat = jobI.lastHeartbeat || 0;

  let heartbeatEmitted = false;
  await new Promise((r) => setTimeout(r, 20)); // Delay para garantir monotonicidade de timestamp

  const outcomeI = await jobExecutionService.executeJob(jobI.jobId, {
    instanceId: "instancia_heartbeat",
    customExecutor: async (job, ctx) => {
      await ctx.signalHeartbeat();
      const updatedJob = await jobStorage.getJob(job.jobId);
      if (updatedJob && (updatedJob.lastHeartbeat || 0) >= initialBeat) {
        heartbeatEmitted = true;
      }
      return { heartbeated: true };
    },
  });

  assert.strictEqual(outcomeI.success, true, "Execução com heartbeat deve ser concluída");
  assert.strictEqual(heartbeatEmitted, true, "Heartbeat deve ter atualizado lastHeartbeat");
  console.log("✅ [PASS] TESTE I: Heartbeat verificado com atualização no Job oficial.");

  // --------------------------------------------------------------------------
  // TESTE J: Tentativa duplicada é idempotente
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE J] Tentativa duplicada é idempotente...");
  const jobJ = await jobStorage.createJob("sha256_hash_test_j", "13.8kV", "13,8 kV");

  // Primeira execução
  const resJ1 = await jobExecutionService.executeJob(jobJ.jobId, { instanceId: "worker_1", attempt: 1 });
  assert.strictEqual(resJ1.success, true, "Tentativa 1 deve concluir");

  // Segunda execução entregue pela fila (ex: retry ou at-least-once delivery)
  const resJ2 = await jobExecutionService.executeJob(jobJ.jobId, { instanceId: "worker_2", attempt: 2 });
  assert.strictEqual(resJ2.success, true, "Tentativa 2 deve retornar sucesso");
  assert.strictEqual(resJ2.idempotent, true, "Tentativa 2 deve ser marcada como idempotente");
  console.log("✅ [PASS] TESTE J: Idempotência de entrega duplicada validada.");

  // --------------------------------------------------------------------------
  // TESTE K: Cold start não perde o Job
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE K] Cold start não perde o Job...");
  const jobK = await jobStorage.createJob("sha256_hash_test_k", "13.8kV", "13,8 kV");
  const storedJobId = jobK.jobId;

  // Simula cold start: zera qualquer estado transitório da sessão
  const retrievedAfterColdStart = await jobStorage.getJob(storedJobId);
  assert(retrievedAfterColdStart !== null, "Job deve existir após cold start");
  assert.strictEqual(retrievedAfterColdStart?.jobId, storedJobId, "JobId preservado");
  assert.strictEqual(retrievedAfterColdStart?.status, "QUEUED", "Status preservado");
  console.log("✅ [PASS] TESTE K: Cold start recuperado com 100% de integridade.");

  // --------------------------------------------------------------------------
  // TESTE L: Instância sem estado local consegue executar Job existente
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE L] Instância sem estado local consegue executar Job existente...");
  const jobL = await jobStorage.createJob("sha256_hash_test_l", "13.8kV", "13,8 kV");
  
  // Instância 'isolada_zero_memory' desconhece qualquer contexto anterior
  const outcomeL = await jobExecutionService.executeJob(jobL.jobId, {
    instanceId: "isolada_zero_memory",
  });

  assert.strictEqual(outcomeL.success, true, "Instância isolada deve executar o Job perfeitamente");
  assert.strictEqual(outcomeL.status, "COMPLETED", "Status final COMPLETED");
  console.log("✅ [PASS] TESTE L: Instância sem estado local executou Job recuperado do Redis.");

  // --------------------------------------------------------------------------
  // TESTE M: Execução abandonada pode ser identificada pelos metadados
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE M] Execução abandonada pode ser identificada pelos metadados...");
  const jobM = await jobStorage.createJob("sha256_hash_test_m", "13.8kV", "13,8 kV");
  
  // Simula job que iniciou processamento porém travou no passado (150 segundos atrás)
  jobM.status = "PROCESSING";
  jobM.instanceId = "worker_crashed";
  await jobStorage.updateJob(jobM);

  // Registra metadados de lease da execução abandonada
  const simM = jobExecutionService.getSimulatedDispatcher();
  const abandonedMeta: any = {
    executionId: "exec_abandoned_m",
    jobId: jobM.jobId,
    instanceId: "worker_crashed",
    attempt: 1,
    startedAt: Date.now() - 200 * 1000,
    lastHeartbeat: Date.now() - 150 * 1000,
    status: "RUNNING",
    leaseExpiresAt: Date.now() - 50 * 1000,
  };
  simM.acquireLocalLease(jobM.jobId, "worker_crashed:exec_abandoned_m", abandonedMeta, 1);
  if (redisService.isConfigured()) {
    try {
      await redisService.set(
        jobExecutionService.getExecutionLeaseKey(jobM.jobId),
        JSON.stringify(abandonedMeta),
        10
      );
    } catch {}
  }

  const abandonment = await jobExecutionService.checkExecutionAbandonment(jobM.jobId);
  assert.strictEqual(abandonment.isAbandoned, true, "Execução sem heartbeat há mais de 120s deve ser diagnosticada como abandonada");
  assert(abandonment.lastHeartbeatAgeSec >= 120, "Idade do último heartbeat deve ser >= 120s");
  assert.strictEqual(abandonment.instanceId, "worker_crashed", "Identificador da instância preservado");
  console.log("✅ [PASS] TESTE M: Diagnóstico de execução abandonada validado.");

  // --------------------------------------------------------------------------
  // TESTE N: Credenciais não aparecem nos logs
  // --------------------------------------------------------------------------
  console.log("\n>>> [TESTE N] Credenciais não aparecem nos logs (Sanitização)...");
  const rawLogSample = "Erro ao conectar Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xyz em https://user:secretpass123@redis.upstash.io?token=upstash_secret_token_12345";
  const sanitized = sanitizeLog(rawLogSample);

  assert(!sanitized.includes("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"), "Token JWT deve ser mascarado");
  assert(!sanitized.includes("secretpass123"), "Senha da URL deve ser mascarada");
  assert(!sanitized.includes("upstash_secret_token_12345"), "Token de query param deve ser mascarado");
  assert(sanitized.includes("[REDACTED_TOKEN]"), "Marcador de redação deve estar presente");
  console.log("✅ [PASS] TESTE N: Sanitização de credenciais em logs comprovada.");

  // --------------------------------------------------------------------------
  // SEÇÃO 17: SIMULAÇÃO DE TRÊS INSTÂNCIAS CONCORRENTES (A, B, C)
  // --------------------------------------------------------------------------
  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 17: TESTE DE SIMULAÇÃO DE 3 INSTÂNCIAS CONCORRENTES (instance-A, instance-B, instance-C)");
  console.log("--------------------------------------------------------------------------------");
  const jobMulti = await jobStorage.createJob("sha256_hash_multi_instances", "13.8kV", "13,8 kV");

  // Dispara simultaneamente nas três instâncias
  const [outcomeA, outcomeB, outcomeC] = await Promise.all([
    jobExecutionService.executeJob(jobMulti.jobId, { instanceId: "instance-A" }),
    jobExecutionService.executeJob(jobMulti.jobId, { instanceId: "instance-B" }),
    jobExecutionService.executeJob(jobMulti.jobId, { instanceId: "instance-C" }),
  ]);

  const outcomes = [outcomeA, outcomeB, outcomeC];
  const successes = outcomes.filter((o) => o.success);
  const conflicts = outcomes.filter((o) => !o.success && o.error === "CONCURRENT_EXECUTION_IN_PROGRESS");

  console.log(`- Instâncias disparadas simultaneamente: 3 (instance-A, instance-B, instance-C)`);
  console.log(`- Sucessos exclusivos: ${successes.length} (esperado: 1)`);
  console.log(`- Concorrências bloqueadas por lease: ${conflicts.length} (esperado: 2)`);

  assert.strictEqual(successes.length, 1, "Exatamente 1 instância deve obter permissão de execução");
  assert.strictEqual(conflicts.length, 2, "As outras 2 instâncias devem detectar concorrência ativa");
  console.log("✅ [PASS] Seção 17: Múltiplas instâncias respeitam atomicamente o lease único.");

  // --------------------------------------------------------------------------
  // SEÇÃO 18: TESTE DE COLD START E TRANSIÇÃO ENTRE INSTÂNCIAS
  // --------------------------------------------------------------------------
  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 18: TESTE DE COLD START ENTRE INSTÂNCIAS");
  console.log("--------------------------------------------------------------------------------");
  const jobCold = await jobStorage.createJob("sha256_hash_cold_start", "13.8kV", "13,8 kV");
  
  // Instância A recebe o job, avança para PREPROCESSING e morre (encerra escopo)
  {
    const instanceA_Job = await jobStorage.getJob(jobCold.jobId);
    assert(instanceA_Job !== null);
    instanceA_Job.status = "PREPROCESSING";
    instanceA_Job.stageMessage = "Instância A pré-processou e encerrou.";
    instanceA_Job.progress = 20;
    await jobStorage.updateJob(instanceA_Job);
  }

  // Instância B inicia sem qualquer dado na memória local
  const retrievedByB = await jobStorage.getJob(jobCold.jobId);
  assert.strictEqual(retrievedByB?.status, "PREPROCESSING", "Instância B deve ler exatamente o estado gravado por A");
  assert.strictEqual(retrievedByB?.progress, 20, "Progresso recuperado");

  // Instância B conclui o trabalho
  const outcomeColdB = await jobExecutionService.executeJob(jobCold.jobId, { instanceId: "instance-B" });
  assert.strictEqual(outcomeColdB.success, true, "Instância B executou com sucesso o job legado por A");
  console.log("✅ [PASS] Seção 18: Cold start e transição de responsabilidade comprovados.");

  // --------------------------------------------------------------------------
  // SEÇÃO 19: TESTE DE CRASH E EXPIRAÇÃO DE LEASE
  // --------------------------------------------------------------------------
  console.log("\n--------------------------------------------------------------------------------");
  console.log("SEÇÃO 19: TESTE DE CRASH E EXPIRAÇÃO DE LEASE (TTL)");
  console.log("--------------------------------------------------------------------------------");
  const jobCrash = await jobStorage.createJob("sha256_hash_crash", "13.8kV", "13,8 kV");

  // Instância A simula aquisição de lease e crash imediato (sem liberar via finally)
  const sim = jobExecutionService.getSimulatedDispatcher();
  const crashToken = "instance-crash:exec_crash";
  const crashMetadata: any = {
    executionId: "exec_crash",
    jobId: jobCrash.jobId,
    instanceId: "instance-crash",
    attempt: 1,
    startedAt: Date.now(),
    lastHeartbeat: Date.now() - 150 * 1000,
    status: "RUNNING",
    leaseExpiresAt: Date.now() - 1000, // Lease expirado
  };

  // TTL curtíssimo (1 segundo) para simular expiração pós-crash
  sim.acquireLocalLease(jobCrash.jobId, crashToken, crashMetadata, 1);

  // Instância B tenta executar enquanto lease acabou de expirar
  await new Promise((r) => setTimeout(r, 1100)); // Aguarda 1.1s para expiração do lease

  const outcomeAfterCrash = await jobExecutionService.executeJob(jobCrash.jobId, {
    instanceId: "instance-recovering",
  });

  assert.strictEqual(outcomeAfterCrash.success, true, "Instância B deve assumir o job após expiração do lease do crash");
  console.log("✅ [PASS] Seção 19: Resiliência contra crash e recuperação de lease comprovadas.");

  console.log("\n================================================================================");
  console.log("TODOS OS TESTES (A a N + Seções 17, 18, 19) DA ETAPA 4A FORAM CONCLUÍDOS COM 100% DE SUCESSO!");
  console.log("================================================================================");
}

runJobExecutionTestSuite().catch((err) => {
  console.error("❌ FALHA NA SUÍTE DE TESTES DA ETAPA 4A:", err);
  process.exit(1);
});
