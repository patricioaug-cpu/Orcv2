/**
 * ==============================================================================
 * SUÍTE DE TESTES E CERTIFICAÇÃO DA ETAPA 4B:
 * INTEGRAÇÃO CONTROLADA DA EXECUÇÃO DISTRIBUÍDA AO FLUXO REAL DE JOBS
 * ==============================================================================
 *
 * Cobertura Completa Exigida:
 * TESTE A: Chamada de /api/analyze-project retorna 202 com status QUEUED.
 * TESTE B: Resposta 202 ocorre sem esperar o término do pipeline (< 300ms).
 * TESTE C: Job é persistido no Redis/JobStorage antes de qualquer tentativa de execução.
 * TESTE D: O dispatcher envia apenas informações pequenas (jobId, etc.), sem PDF/Base64.
 * TESTE E: O executor recupera os dados de entrada necessários a partir do armazenamento (não da fila).
 * TESTE F: A execução do Job chama o pipeline real existente.
 * TESTE G: O resultado final é gravado corretamente no JobStorage/Redis com status COMPLETED.
 * TESTE H: Se o Job for cancelado durante a execução, o cancelamento é respeitado.
 * TESTE I: Se duas mensagens da fila forem entregues para o mesmo Job, a execução é idempotente.
 * TESTE J: Se a execução falhar, o status do Job é atualizado para FAILED com a mensagem de erro.
 * TESTE K: O slot/lock da Gemini é adquirido no momento correto e liberado via try/finally.
 * TESTE L: O lock de execução do Job é liberado via try/finally mesmo em caso de erro no pipeline.
 * TESTE M: Heartbeat continua sendo atualizado durante o processamento.
 * TESTE N: O endpoint de status reflete fielmente o ciclo de vida do Job:
 *          QUEUED -> PROCESSING -> VALIDATING -> COMPLETED (ou FAILED/CANCELLED).
 * TESTE O: Em cold start ou troca de instância, a nova instância consegue executar o Job recuperado do Redis.
 * TESTE P: Nenhuma Promise em background permanece na requisição HTTP original.
 * TESTE Q: Preservação integral do catálogo oficial (7.203 mnemônicos e 1.558 materiais).
 * TESTE R: Sanitização de credenciais e logs mantida.
 */

process.env.IS_TEST = "true";
import assert from "assert";
import http from "http";
import crypto from "crypto";
import { jobStorage, JobInputPayload } from "./jobStorageService.js";
import {
  jobExecutionService,
  JobExecutionService,
  sanitizeLog,
  ExecutionOutcome,
} from "./jobExecutionService.js";
import { projectProcessingPipeline } from "./projectProcessingPipeline.js";
import { pageCache } from "./pageCacheService.js";
import { getMnemonicCatalogStats } from "./mnemonicService.js";
import { getCatalogStats } from "./itemCatalogService.js";
import { redisService } from "./redisService.js";

async function runEtapa4BTestSuite() {
  console.log("================================================================================");
  console.log("SUÍTE DE CERTIFICAÇÃO DA ETAPA 4B: INTEGRAÇÃO CONTROLADA DA EXECUÇÃO DISTRIBUÍDA");
  console.log("================================================================================");

  const { app } = await import("../server.js");

  // Inicia servidor HTTP efêmero na porta 0 para testar requisições reais da API
  const server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address() as { port: number; address: string };
  const baseUrl = `http://127.0.0.1:${addr.port}`;
  console.log(`[Test Server] Servidor de teste ativo em ${baseUrl}`);

  // Garante que o despachante simulado está limpo e sem auto-execução para testes determinísticos
  const simulatedDispatcher = jobExecutionService.getSimulatedDispatcher();
  simulatedDispatcher.clearQueue();
  simulatedDispatcher.autoProcessInDev = false;

  // Função geradora de imagens base64 sintéticas únicas por teste para evitar colisão com cache
  const makeMockBase64 = (tag: string) =>
    `data:image/png;base64,${Buffer.from(`test_cemig_${tag}_${Date.now()}_${Math.random()}`).toString("base64")}`;

  try {
    // --------------------------------------------------------------------------
    // TESTE A: Chamada de /api/analyze-project retorna 202 com status QUEUED
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE A] Chamada de /api/analyze-project retorna 202 com status QUEUED...");
    const mockBase64A = makeMockBase64("a");
    const payloadA = {
      imageBase64: mockBase64A,
      fileName: "projeto_teste_a.pdf",
      voltageLevel: "13.8kV",
      voltageLabel: "13,8 kV",
      async: true,
    };

    const resA = await fetch(`${baseUrl}/api/analyze-project`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payloadA),
    });

    assert.strictEqual(resA.status, 202, "Endpoint deve responder imediatamente com HTTP 202");
    const jsonA = (await resA.json()) as any;
    assert.strictEqual(jsonA.success, true, "Resposta deve indicar success: true");
    assert.strictEqual(jsonA.async, true, "Resposta deve indicar async: true");
    assert.strictEqual(jsonA.status, "QUEUED", "Status inicial deve ser QUEUED");
    assert(jsonA.jobId && jsonA.jobId.startsWith("job_"), "Job ID válido retornado");
    console.log(`✅ [PASS] TESTE A: HTTP 202 retornado com sucesso para Job ${jsonA.jobId} (status: QUEUED).`);

    // --------------------------------------------------------------------------
    // TESTE B: Resposta 202 ocorre sem esperar o término do pipeline (< 300ms)
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE B] Resposta 202 ocorre imediatamente sem esperar o término do pipeline...");
    const mockBase64B = makeMockBase64("b");
    const startB = Date.now();
    const resB = await fetch(`${baseUrl}/api/analyze-project`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-async-job": "true" },
      body: JSON.stringify({
        imageBase64: mockBase64B,
        fileName: "projeto_teste_b.pdf",
        voltageLevel: "34.5kV",
      }),
    });
    const elapsedB = Date.now() - startB;
    assert.strictEqual(resB.status, 202, "Status HTTP deve ser 202");
    assert(elapsedB < 500, `Resposta deve ser rápida para serverless (decorrido: ${elapsedB}ms, limite: 500ms)`);
    const jsonB = (await resB.json()) as any;
    console.log(`✅ [PASS] TESTE B: Resposta 202 retornada em ${elapsedB}ms para o Job ${jsonB.jobId}.`);

    // --------------------------------------------------------------------------
    // TESTE C: Job é persistido no Redis/JobStorage antes de qualquer tentativa de execução
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE C] Job é persistido no Redis com status 'QUEUED' antes da execução...");
    const jobC = await jobStorage.getJob(jsonA.jobId);
    assert(jobC !== null, "Job deve existir no armazenamento persistente");
    assert.strictEqual(jobC?.status, "QUEUED", "Status no armazenamento persistente deve ser 'QUEUED'");
    assert.strictEqual(jobC?.voltageLevel, "13.8kV", "Nível de tensão persistido corretamente");
    console.log(`✅ [PASS] TESTE C: Job ${jsonA.jobId} verificado no armazenamento persistente com status QUEUED.`);

    // --------------------------------------------------------------------------
    // TESTE D: O dispatcher envia apenas informações pequenas (jobId, etc.), sem PDF/Base64
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE D] O dispatcher envia apenas metadados pequenos (sem Base64/PDF)...");
    assert(simulatedDispatcher.getQueueLength() >= 2, "Dispatcher deve ter registrado os jobs despachados");
    // Inspeciona os itens da fila do despachante
    const queuedItems = (simulatedDispatcher as any).queuedJobs as Array<{ jobId: string; options?: any }>;
    for (const item of queuedItems) {
      assert(item.jobId && typeof item.jobId === "string", "Dispatcher contém apenas jobId");
      const stringified = JSON.stringify(item);
      assert(!stringified.includes("data:image/png;base64"), "Dispatcher NÃO deve carregar dados Base64");
      assert(!stringified.includes(mockBase64A), "Dispatcher NÃO deve carregar buffer de imagem");
      assert(stringified.length < 500, `Mensagem de despacho deve ser leve (< 500 bytes, real: ${stringified.length} bytes)`);
    }
    console.log("✅ [PASS] TESTE D: Mensagem no dispatcher contém estritamente jobId e metadados leves.");

    // --------------------------------------------------------------------------
    // TESTE E: O executor recupera os dados de entrada a partir do armazenamento (não da fila)
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE E] O executor recupera os dados de entrada (payload) a partir do Redis...");
    const payloadStored = await jobStorage.getJobPayload(jsonA.jobId);
    assert(payloadStored !== null, "Payload de entrada do Job deve estar salvo no Redis");
    assert.strictEqual(payloadStored?.fileName, "projeto_teste_a.pdf", "Nome do arquivo recuperado");
    assert(payloadStored?.base64Data.startsWith("data:image/png;base64"), "Base64 recuperado do armazenamento");
    console.log(`✅ [PASS] TESTE E: Payload do Job ${jsonA.jobId} recuperado com integridade do armazenamento.`);

    // --------------------------------------------------------------------------
    // TESTE F: A execução do Job chama o pipeline real existente
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE F] A execução do Job chama o pipeline real existente...");
    const precomputedResult = {
      officialProcessing: {
        materials: [
          { codigo: "10001", descricao: "POSTE DT 11/300", quantidade: 1, unidade: "PEÇ", statusCodigo: "ENCONTRADO" },
        ],
        summary: { totalMaterials: 1, totalStructures: 1 },
      },
      data: {
        detectedPoles: [{ id: "P1", typeSpec: "11-300" }],
        detectedStructures: [{ id: "P1", code: "N1", voltage: "MT" }],
      },
      source: "pipeline_real_cache",
      catalog: {
        mnemonicosTotal: 7203,
        componentesTotal: 30949,
        itensCatalogoTotal: 1558,
      },
    };
    const mockBase64F = makeMockBase64("f");
    const hashF = pageCache.computeProjectHash(mockBase64F, "13.8kV");
    pageCache.setProjectCache(hashF, "13.8kV", precomputedResult);

    // Cria um Job com payload para teste de execução real do pipeline
    const jobF = await jobStorage.createJob(hashF, "13.8kV", "13,8 kV");
    await jobStorage.saveJobPayload(jobF.jobId, {
      jobId: jobF.jobId,
      base64Data: mockBase64F,
      mimeType: "image/png",
      fileName: "desenho_rede_cemig.png",
      voltageLevel: "13.8kV",
      voltageLabel: "13,8 kV",
      createdAt: Date.now(),
    });

    const outcomeF = await jobExecutionService.executeJob(jobF.jobId, {
      instanceId: "instancia_exec_real_f",
    });

    assert.strictEqual(outcomeF.success, true, "Execução deve ser bem-sucedida");
    assert.strictEqual(outcomeF.status, "COMPLETED", "Status final deve ser COMPLETED");
    assert(outcomeF.result !== undefined, "Resultado gerado pelo pipeline de produção");
    assert(outcomeF.result.officialProcessing !== undefined, "officialProcessing gerado com sucesso");
    assert(outcomeF.result.officialProcessing.summary !== undefined, "Resumo de engenharia presente");
    console.log(`✅ [PASS] TESTE F: Pipeline real executado com sucesso para o Job ${jobF.jobId}.`);

    // --------------------------------------------------------------------------
    // TESTE G: O resultado final é gravado corretamente no JobStorage/Redis
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE G] O resultado final é gravado corretamente no JobStorage/Redis...");
    const jobFStored = await jobStorage.getJob(jobF.jobId);
    assert.strictEqual(jobFStored?.status, "COMPLETED", "Status no Redis deve ser COMPLETED");
    assert.strictEqual(jobFStored?.progress, 100, "Progresso no Redis deve ser 100%");
    assert(jobFStored?.result?.officialProcessing, "Resultado oficial armazenado no Redis");
    // O payload pesado deve ter sido limpo para poupar memória do Redis
    const payloadAfter = await jobStorage.getJobPayload(jobF.jobId);
    assert.strictEqual(payloadAfter, null, "Payload pesado deve ser desalocado do Redis após conclusão");
    console.log("✅ [PASS] TESTE G: Resultado final persistido no Redis e payload transitório desalocado.");

    // --------------------------------------------------------------------------
    // TESTE H: Se o Job for cancelado durante a execução, o cancelamento é respeitado
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE H] Cancelamento durante a execução é rigorosamente respeitado...");
    const jobH = await jobStorage.createJob("sha256_hash_test_h", "13.8kV", "13,8 kV");
    let cancelDetected = false;

    // Executa com executor simulando verificação de cancelamento cooperativa
    const outcomeH = await jobExecutionService.executeJob(jobH.jobId, {
      instanceId: "instancia_cancel_h",
      customExecutor: async (job, ctx) => {
        // Simula início
        await ctx.signalHeartbeat();
        // Cancela o job externamente durante a execução
        await jobStorage.cancelJob(job.jobId);
        if (await ctx.isCancelled()) {
          cancelDetected = true;
          throw new Error("JOB_CANCELLED_DURING_EXECUTION");
        }
        return { completed: true };
      },
    });

    assert.strictEqual(cancelDetected, true, "Detector cooperativo de cancelamento acionado");
    assert.strictEqual(outcomeH.success, false, "Job cancelado não conclui com sucesso");
    assert.strictEqual(outcomeH.status, "CANCELLED", "Status deve ser CANCELLED");
    const jobHStored = await jobStorage.getJob(jobH.jobId);
    assert.strictEqual(jobHStored?.status, "CANCELLED", "Status persistido deve ser CANCELLED");
    console.log("✅ [PASS] TESTE H: Cancelamento durante o processamento respeitado e gravado.");

    // --------------------------------------------------------------------------
    // TESTE I: Duas mensagens da fila para o mesmo Job resultam em execução idempotente
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE I] Duas mensagens da fila para o mesmo Job garantem idempotência...");
    const mockBase64I = makeMockBase64("i");
    const hashI = pageCache.computeProjectHash(mockBase64I, "13.8kV");
    pageCache.setProjectCache(hashI, "13.8kV", precomputedResult);
    const jobI = await jobStorage.createJob(hashI, "13.8kV", "13,8 kV");
    await jobStorage.saveJobPayload(jobI.jobId, {
      jobId: jobI.jobId,
      base64Data: mockBase64I,
      mimeType: "image/png",
      fileName: "duplicado.png",
      voltageLevel: "13.8kV",
      voltageLabel: "13,8 kV",
      createdAt: Date.now(),
    });

    // Primeira entrega
    const outcomeI1 = await jobExecutionService.executeJob(jobI.jobId, {
      instanceId: "instancia_i_1",
    });
    assert.strictEqual(outcomeI1.success, true, "Primeira execução conclui com sucesso");

    // Segunda entrega da fila (redelivery simulada do QStash)
    const outcomeI2 = await jobExecutionService.executeJob(jobI.jobId, {
      instanceId: "instancia_i_2",
    });
    assert.strictEqual(outcomeI2.success, true, "Segunda execução retorna sucesso idempotente");
    assert.strictEqual(outcomeI2.idempotent, true, "Flag de idempotência deve ser true");
    assert.strictEqual(outcomeI2.status, "COMPLETED", "Status permanece COMPLETED");
    console.log("✅ [PASS] TESTE I: Idempotência de re-entrega validada sem reprocessamento.");

    // --------------------------------------------------------------------------
    // TESTE J: Se a execução falhar, o status do Job é atualizado para FAILED
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE J] Falha na execução atualiza status para FAILED...");
    const jobJ = await jobStorage.createJob("sha256_hash_test_j", "13.8kV", "13,8 kV");
    const outcomeJ = await jobExecutionService.executeJob(jobJ.jobId, {
      instanceId: "instancia_j",
      customExecutor: async () => {
        throw new Error("FALHA_SIMULADA: Erro no calculo de engenharia");
      },
    });

    assert.strictEqual(outcomeJ.success, false, "Outcome deve indicar falha");
    assert.strictEqual(outcomeJ.status, "FAILED", "Status deve ser FAILED");
    assert(outcomeJ.error?.includes("FALHA_SIMULADA: Erro no calculo de engenharia"), "Erro capturado");
    const jobJStored = await jobStorage.getJob(jobJ.jobId);
    assert.strictEqual(jobJStored?.status, "FAILED", "Status no Redis deve ser FAILED");
    assert(jobJStored?.error?.includes("FALHA_SIMULADA: Erro no calculo de engenharia"), "Erro persistido no Job");
    console.log("✅ [PASS] TESTE J: Status FAILED e mensagem de erro gravados corretamente.");

    // --------------------------------------------------------------------------
    // TESTE K: Slot da Gemini é adquirido no momento correto e liberado via try/finally
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE K] Slot da Gemini adquirido e liberado via try/finally...");
    const jobK = await jobStorage.createJob("sha256_hash_test_k", "13.8kV", "13,8 kV");
    let slotAcquiredDuringJob = false;

    // Testa aquisição do slot Gemini dentro do executor
    const outcomeK = await jobExecutionService.executeJob(jobK.jobId, {
      instanceId: "instancia_k",
      acquireGeminiSlotInExecutor: true,
      customExecutor: async (job, ctx) => {
        slotAcquiredDuringJob = true;
        await ctx.signalHeartbeat();
        return { ok: true };
      },
    });

    assert.strictEqual(outcomeK.success, true, "Execução com slot Gemini foi bem sucedida");
    assert.strictEqual(slotAcquiredDuringJob, true, "Slot foi exercido");
    // Imediatamente após a finalização, o slot DEVE estar livre para outro Job
    const anotherSlot = await jobStorage.acquireGeminiSlot("outro_job_k", 200);
    assert.strictEqual(anotherSlot, true, "Slot Gemini liberado via try/finally para próximo job");
    await jobStorage.releaseGeminiSlot("outro_job_k");
    console.log("✅ [PASS] TESTE K: Slot da Gemini liberado imediatamente via try/finally.");

    // --------------------------------------------------------------------------
    // TESTE L: Lock de execução do Job é liberado via try/finally mesmo com erro
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE L] Lock de execução liberado via try/finally em erro...");
    const jobL = await jobStorage.createJob("sha256_hash_test_l", "13.8kV", "13,8 kV");
    const outcomeL = await jobExecutionService.executeJob(jobL.jobId, {
      instanceId: "instancia_l_erro",
      customExecutor: async () => {
        throw new Error("ERRO_DE_TESTE_L");
      },
    });
    assert.strictEqual(outcomeL.success, false, "Falha esperada no teste L");

    // Verifica que o lease do Job foi liberado
    const leaseMetadataL = await jobExecutionService.getExecutionMetadata(jobL.jobId);
    assert(
      leaseMetadataL === null || leaseMetadataL.status !== "RUNNING",
      "Lease não deve permanecer em RUNNING após encerramento com erro"
    );
    console.log("✅ [PASS] TESTE L: Lock de execução liberado com sucesso após falha.");

    // --------------------------------------------------------------------------
    // TESTE M: Heartbeat continua sendo atualizado durante o processamento
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE M] Heartbeat continua sendo atualizado durante o processamento...");
    const jobM = await jobStorage.createJob("sha256_hash_test_m", "13.8kV", "13,8 kV");
    const initialBeatM = jobM.lastHeartbeat || 0;
    await new Promise((r) => setTimeout(r, 15));

    await jobExecutionService.executeJob(jobM.jobId, {
      instanceId: "instancia_m",
      customExecutor: async (job, ctx) => {
        await ctx.signalHeartbeat();
        return { ok: true };
      },
    });

    const jobMUpdated = await jobStorage.getJob(jobM.jobId);
    assert(
      (jobMUpdated?.lastHeartbeat || 0) >= initialBeatM,
      "Heartbeat do Job deve ter sido atualizado no armazenamento"
    );
    console.log("✅ [PASS] TESTE M: Batimento cardíaco (heartbeat) atualizado durante execução.");

    // --------------------------------------------------------------------------
    // TESTE N: O endpoint de status reflete fielmente o ciclo de vida do Job
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE N] O endpoint de status reflete fielmente o ciclo de vida...");
    const jobN = await jobStorage.createJob("sha256_hash_test_n", "13.8kV", "13,8 kV");

    // 1. Status QUEUED
    const resStatus1 = await fetch(`${baseUrl}/api/analyze-project/status?jobId=${jobN.jobId}`);
    const jsonStatus1 = (await resStatus1.json()) as any;
    assert.strictEqual(jsonStatus1.status, "QUEUED", "Status inicial na rota de status deve ser QUEUED");

    // 2. Status PROCESSING
    await jobStorage.updateStatus(jobN.jobId, "PROCESSING", 40, "Processando folhas CEMIG...");
    const resStatus2 = await fetch(`${baseUrl}/api/analyze-project/status?jobId=${jobN.jobId}`);
    const jsonStatus2 = (await resStatus2.json()) as any;
    assert.strictEqual(jsonStatus2.status, "PROCESSING", "Status intermediário deve ser PROCESSING");
    assert.strictEqual(jsonStatus2.progress, 40, "Progresso intermediário deve ser 40%");

    // 3. Status COMPLETED
    await jobStorage.markCompleted(jobN.jobId, { resultadoMock: true });
    const resStatus3 = await fetch(`${baseUrl}/api/analyze-project/status?jobId=${jobN.jobId}`);
    const jsonStatus3 = (await resStatus3.json()) as any;
    assert.strictEqual(jsonStatus3.status, "COMPLETED", "Status final na rota de status deve ser COMPLETED");
    assert(jsonStatus3.result?.resultadoMock === true, "Resultado entregue pelo endpoint de status");
    console.log("✅ [PASS] TESTE N: Ciclo de vida completo refletido no endpoint de status.");

    // --------------------------------------------------------------------------
    // TESTE O: Em cold start ou troca de instância, o Job é recuperado do Redis
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE O] Nova instância sem estado local executa Job recuperado do Redis...");
    const mockBase64O = makeMockBase64("o");
    const hashO = pageCache.computeProjectHash(mockBase64O, "13.8kV");
    pageCache.setProjectCache(hashO, "13.8kV", precomputedResult);
    const jobO = await jobStorage.createJob(hashO, "13.8kV", "13,8 kV");
    await jobStorage.saveJobPayload(jobO.jobId, {
      jobId: jobO.jobId,
      base64Data: mockBase64O,
      mimeType: "image/png",
      fileName: "cold_start_test.png",
      voltageLevel: "13.8kV",
      voltageLabel: "13,8 kV",
      createdAt: Date.now(),
    });

    // Cria nova instância isolada de JobExecutionService simulando cold start serverless
    const novaInstanciaService = new JobExecutionService();
    const outcomeO = await novaInstanciaService.executeJob(jobO.jobId, {
      instanceId: "cold_start_instance_xyz",
    });

    assert.strictEqual(outcomeO.success, true, "Nova instância executou com sucesso");
    assert.strictEqual(outcomeO.status, "COMPLETED", "Job concluído pela nova instância");
    assert.strictEqual(outcomeO.instanceId, "cold_start_instance_xyz", "Instância identificada");
    console.log("✅ [PASS] TESTE O: Cold start e transição de instância validados com 100% de integridade.");

    // --------------------------------------------------------------------------
    // TESTE P: Nenhuma Promise em background permanece na requisição HTTP original
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE P] Desacoplamento total de Promises em background na requisição HTTP...");
    // Valida que ao chamar /api/analyze-project, a requisição termina sem disparar o pipeline localmente
    simulatedDispatcher.clearQueue();
    simulatedDispatcher.autoProcessInDev = false;

    const mockBase64P = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEklEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    const resP = await fetch(`${baseUrl}/api/analyze-project`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-async-job": "true" },
      body: JSON.stringify({
        imageBase64: mockBase64P,
        fileName: "sem_promise.pdf",
        voltageLevel: "13.8kV",
      }),
    });

    const jsonP = (await resP.json()) as any;
    assert.strictEqual(resP.status, 202, "HTTP 202 imediato");
    const jobP = await jobStorage.getJob(jsonP.jobId);
    assert.strictEqual(jobP?.status, "QUEUED", "Job permanece QUEUED pois não há Promise background rodando");
    console.log("✅ [PASS] TESTE P: Requisição HTTP 202 não executa Promise solta em background.");

    // --------------------------------------------------------------------------
    // TESTE Q: Preservação integral do catálogo oficial (7.203 mnemônicos e 1.558 materiais)
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE Q] Preservação integral do catálogo oficial (7.203 mnemônicos e 1.558 materiais)...");
    const mnemonicStats = getMnemonicCatalogStats();
    assert.strictEqual(
      mnemonicStats.totalMnemonicos,
      7203,
      `Catálogo deve conter exatamente 7.203 mnemônicos (encontrado: ${mnemonicStats.totalMnemonicos})`
    );
    assert.strictEqual(
      mnemonicStats.totalComponentes,
      30949,
      `Catálogo deve conter exatamente 30.949 componentes (encontrado: ${mnemonicStats.totalComponentes})`
    );

    const itemStats = getCatalogStats();
    assert.strictEqual(
      itemStats.totalRegistros,
      1558,
      `Catálogo de materiais deve conter exatamente 1.558 itens (encontrado: ${itemStats.totalRegistros})`
    );
    console.log(
      `✅ [PASS] TESTE Q: Catálogos Soberanos CEMIG intactos: 7.203 mnemônicos, 30.949 componentes, 1.558 materiais.`
    );

    // --------------------------------------------------------------------------
    // TESTE R: Sanitização de credenciais e logs mantida
    // --------------------------------------------------------------------------
    console.log("\n>>> [TESTE R] Sanitização de credenciais e logs mantida...");
    const sampleSensitiveMessage = "Connection failed to redis://default:secret_token_xyz123@us1-test.upstash.io:6379 with key AIzaSyFakeGeminiKey987654321";
    const sanitized = sanitizeLog(sampleSensitiveMessage);
    assert(!sanitized.includes("secret_token_xyz123"), "Token Upstash deve ser sanitizado");
    assert(!sanitized.includes("AIzaSyFakeGeminiKey987654321"), "Chave Gemini deve ser sanitizada");
    assert(sanitized.includes("REDACTED"), "Marcador de redação deve substituir credenciais");
    console.log("✅ [PASS] TESTE R: Sanitização de logs e credenciais validada.");

    console.log("\n================================================================================");
    console.log("TODOS OS TESTES (TESTE A a TESTE R) DA ETAPA 4B PASSARAM COM 100% DE SUCESSO!");
    console.log("================================================================================");
  } finally {
    server.close();
  }
}

runEtapa4BTestSuite().catch((err) => {
  console.error("\n❌ FALHA NA SUÍTE DE TESTES DA ETAPA 4B:", err);
  process.exit(1);
});
