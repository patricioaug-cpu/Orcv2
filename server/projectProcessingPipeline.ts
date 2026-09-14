import { pageCache } from "./pageCacheService";
import { jobStorage, AnalysisJob } from "./jobStorageService";
import { pagePreprocessor, DocumentPreprocessResult } from "./pagePreprocessorService";
import { strictValidation } from "./strictValidationService";
import { geminiVision } from "./geminiVisionService";
import { processarProjetoComSimbologiaOficial } from "./orchestrationService";
import { ElementoDetectadoInput } from "./recognitionEngineService";

export interface PipelineOptions {
  base64Data: string;
  mimeType: string;
  fileName?: string;
  voltageLevel?: string;
  voltageLabel?: string;
  jobId?: string;
}

/**
 * Main Orchestrator for the Optimized Hybrid Processing Architecture.
 * Strictly respects all engineering rules, catalog integrity, zero-hallucination,
 * per-page cache, PDF classification, and serverless concurrency controls.
 */
export class ProjectProcessingPipeline {
  private static instance: ProjectProcessingPipeline;

  public static getInstance(): ProjectProcessingPipeline {
    if (!ProjectProcessingPipeline.instance) {
      ProjectProcessingPipeline.instance = new ProjectProcessingPipeline();
    }
    return ProjectProcessingPipeline.instance;
  }

  /**
   * Executes the full pipeline asynchronously or in a worker job.
   */
  public async executePipeline(options: PipelineOptions): Promise<any> {
    const { base64Data, mimeType, voltageLevel = "13.8kV", voltageLabel = "13,8 kV" } = options;
    const fileHash = pageCache.computeProjectHash(base64Data, voltageLevel);

    // 1. Check or initialize Job
    let job: AnalysisJob;
    if (options.jobId) {
      job = (await jobStorage.getJob(options.jobId)) || (await jobStorage.createJob(fileHash, voltageLevel, voltageLabel));
    } else {
      job = await jobStorage.createJob(fileHash, voltageLevel, voltageLabel);
    }

    const startTime = Date.now();

    try {
      // Check cancellation
      if (await jobStorage.isJobCancelled(job.jobId)) {
        job.status = "CANCELLED";
        job.stageMessage = "Job cancelado pelo usuário.";
        await jobStorage.updateJob(job);
        return { cancelled: true };
      }

      // ==========================================
      // STAGE 1: Check Level 1 Project Cache
      // ==========================================
      const cachedProject = pageCache.getProjectCache(fileHash, voltageLevel);
      if (cachedProject) {
        console.log(`[Pipeline] Cache HIT no projeto completo (${fileHash.slice(0, 10)}). 100% economia de tokens.`);
        job.status = "COMPLETED";
        job.progress = 100;
        job.stageMessage = "Projeto recuperado instantaneamente do cache seguro.";
        job.telemetry.cachedPages = 1;
        job.telemetry.cacheHit = true;
        job.telemetry.processingTimeMs = Date.now() - startTime;
        job.result = {
          ...cachedProject.result,
          source: "cache",
          cacheHit: true,
          hash: fileHash,
          telemetry: job.telemetry,
        };
        await jobStorage.updateJob(job);
        return job.result;
      }

      // ==========================================
      // STAGE 2: PREPROCESSING (pdfjs-dist & metadata)
      // ==========================================
      job.status = "PREPROCESSING";
      job.progress = 15;
      job.stageMessage = "Extraindo páginas, textos e metadados com pdfjs-dist...";
      await jobStorage.updateJob(job);

      const prepResult: DocumentPreprocessResult = await pagePreprocessor.analyzeDocument(base64Data, mimeType);
      job.telemetry.totalPages = prepResult.totalPages;
      job.telemetry.relevantPages = prepResult.relevantPagesCount;
      job.telemetry.discardedPages = prepResult.discardedPagesCount;

      console.log(
        `[Pipeline] Pré-processamento concluído: ${prepResult.totalPages} páginas (${prepResult.relevantPagesCount} relevantes, ${prepResult.discardedPagesCount} descartadas).`
      );

      // Check cancellation
      if (await jobStorage.isJobCancelled(job.jobId)) {
        job.status = "CANCELLED";
        await jobStorage.updateJob(job);
        return { cancelled: true };
      }

      // ==========================================
      // STAGE 3: CLASSIFYING
      // ==========================================
      job.status = "CLASSIFYING";
      job.progress = 30;
      job.stageMessage = `Classificação concluída: ${prepResult.relevantPagesCount} pranchas de rede, ${prepResult.discardedPagesCount} páginas descartadas sem custo.`;
      await jobStorage.updateJob(job);

      // ==========================================
      // STAGE 4: PROCESSING (Deterministic text or Gemini per page)
      // ==========================================
      job.status = "PROCESSING";
      job.progress = 45;
      job.stageMessage = "Interpretando pranchas técnicas relevantes...";
      await jobStorage.updateJob(job);

      const aggregatedExtractedData: any = {
        detectedPoles: [],
        detectedStructures: [],
        detectedEquipment: [],
        detectedTransformers: [],
        detectedGuys: [],
        detectedCables: [],
      };

      const relevantPages = prepResult.pages.filter((p) => p.relevance !== "IRRELEVANTE");

      for (let i = 0; i < relevantPages.length; i++) {
        if (await jobStorage.isJobCancelled(job.jobId)) {
          job.status = "CANCELLED";
          await jobStorage.updateJob(job);
          return { cancelled: true };
        }

        const page = relevantPages[i];
        const pageProgress = 45 + Math.floor(((i + 1) / relevantPages.length) * 30);
        job.progress = pageProgress;
        job.stageMessage = `Processando prancha ${page.pageNumber} de ${prepResult.totalPages}...`;
        await jobStorage.updateJob(job);

        // Path A: Deterministic complete extraction from selectable text (0 tokens)
        if (page.isDeterministicComplete && page.deterministicData) {
          console.log(`[Pipeline] Prancha ${page.pageNumber}: Extração 100% determinística via texto nativo (0 tokens).`);
          this.mergePageData(aggregatedExtractedData, page.deterministicData, page.pageNumber);
          job.telemetry.deterministicExtractionUsed = true;
          continue;
        }

        // Path B: Check per-page cache
        const cachedPageData = pageCache.getPageCache(page.pageHash);
        if (cachedPageData) {
          console.log(`[Pipeline] Prancha ${page.pageNumber}: Cache HIT granular por página (${page.pageHash.slice(0, 10)}).`);
          this.mergePageData(aggregatedExtractedData, cachedPageData.extractedData, page.pageNumber);
          job.telemetry.cachedPages++;
          continue;
        }

        // Path C: Call Gemini Vision with serverless slot concurrency lock
        console.log(`[Pipeline] Prancha ${page.pageNumber}: Solicitando IA com modelo econômico...`);
        job.telemetry.geminiPages++;

        const slotAcquired = await jobStorage.acquireGeminiSlot(job.jobId, 35000);
        if (!slotAcquired && (await jobStorage.isJobCancelled(job.jobId))) {
          job.status = "CANCELLED";
          await jobStorage.updateJob(job);
          return { cancelled: true };
        }

        let visionResult: any;
        try {
          visionResult = await geminiVision.interpretSheet(
            page.imageBufferOrBase64 || base64Data,
            mimeType,
            voltageLabel,
            job.jobId
          );
        } finally {
          jobStorage.releaseGeminiSlot(job.jobId);
        }

        job.telemetry.geminiCalls++;
        job.telemetry.retriesCount += visionResult.retries || 0;
        if (visionResult.promptTokens) {
          job.telemetry.promptTokens = (job.telemetry.promptTokens || 0) + visionResult.promptTokens;
        }
        if (visionResult.candidateTokens) {
          job.telemetry.candidateTokens = (job.telemetry.candidateTokens || 0) + visionResult.candidateTokens;
        }
        job.telemetry.modelUsed = visionResult.modelUsed;

        // Save into per-page cache
        pageCache.setPageCache(page.pageHash, visionResult.data);
        this.mergePageData(aggregatedExtractedData, visionResult.data, page.pageNumber);
      }

      // ==========================================
      // STAGE 5: VALIDATING (Strict Catalog Rules)
      // ==========================================
      job.status = "VALIDATING";
      job.progress = 80;
      job.stageMessage = "Validando estruturas contra o catálogo oficial de 7.203 mnemônicos...";
      await jobStorage.updateJob(job);

      const validationReport = strictValidation.validateExtractedElements(aggregatedExtractedData);
      job.telemetry.ambiguitiesCount = validationReport.ambiguousCount;

      // ==========================================
      // STAGE 6: FINALIZING (Deterministic Orchestration Engine)
      // ==========================================
      job.status = "FINALIZING";
      job.progress = 90;
      job.stageMessage = "Explodindo mnemônicos em componentes oficiais e calculando quantitativos...";
      await jobStorage.updateJob(job);

      // Convert aggregated elements to ElementoDetectadoInput for sovereign orchestration
      const elementosParaReconhecimento: ElementoDetectadoInput[] = [];

      aggregatedExtractedData.detectedPoles.forEach((p: any, idx: number) => {
        elementosParaReconhecimento.push({
          id: p.id || `P${idx + 1}`,
          tipo: "POSTE",
          especificacao: p.typeSpec || p.associatedPost,
          formato: p.shape,
          material: p.material,
          mnemonicCode: p.mnemonicCode,
          tensao: voltageLevel,
          status: p.status,
          descricao: `Poste ${p.id || idx + 1} (${p.typeSpec || ""})`,
          pagina: Number(p.pageNumber || 1),
        });
      });

      aggregatedExtractedData.detectedStructures.forEach((s: any, idx: number) => {
        elementosParaReconhecimento.push({
          id: s.id || `ESTR_${idx + 1}`,
          tipo: s.voltage === "BT" ? "ESTRUTURA BT" : "ESTRUTURA MT",
          codigo: s.code,
          mnemonicCode: s.mnemonicCode,
          tensao: s.voltage === "BT" ? "BT" : voltageLevel,
          status: s.status,
          descricao: s.description || `Estrutura ${s.code || ""}`,
          especificacao: s.associatedPost,
          localizacao: s.associatedPost ? `Poste ${s.associatedPost}` : s.id ? `Poste ${s.id}` : undefined,
          pagina: Number(s.pageNumber || 1),
        });
      });

      aggregatedExtractedData.detectedEquipment.forEach((eq: any, idx: number) => {
        elementosParaReconhecimento.push({
          id: eq.id || `EQ_${eq.associatedPole || idx + 1}`,
          tipo: eq.type || "EQUIPAMENTO",
          codigo: eq.code || eq.specification || eq.type,
          mnemonicCode: eq.mnemonicCode,
          tensao: eq.voltage || voltageLevel,
          status: eq.status,
          especificacao: eq.specification,
          descricao: eq.description || `${eq.type || "Equipamento"} ${eq.code || ""}`.trim(),
          localizacao: eq.associatedPole ? `Poste ${eq.associatedPole}` : undefined,
          quantidade: Number(eq.quantity) || 1,
          pagina: Number(eq.pageNumber || 1),
        });
      });

      aggregatedExtractedData.detectedTransformers.forEach((t: any, idx: number) => {
        elementosParaReconhecimento.push({
          id: t.associatedPole ? `TR_${t.associatedPole}` : `TR_${idx + 1}`,
          tipo: "TRANSFORMADOR",
          mnemonicCode: t.mnemonicCode,
          tensao: t.voltage || voltageLevel,
          especificacao: `${t.powerKva || ""}KVA ${t.voltage || ""}`,
          status: t.status,
          descricao: `Transformador ${t.powerKva || ""}kVA`,
          localizacao: t.associatedPole ? `Poste ${t.associatedPole}` : undefined,
          pagina: Number(t.pageNumber || 1),
        });
      });

      aggregatedExtractedData.detectedGuys.forEach((g: any, idx: number) => {
        elementosParaReconhecimento.push({
          id: `ESTAI_${idx + 1}`,
          tipo: "ESTAI",
          mnemonicCode: g.mnemonicCode,
          tensao: voltageLevel,
          status: g.status,
          descricao: `Estai de ${g.type || "Âncora"}`,
          localizacao: g.associatedPole ? `Poste ${g.associatedPole}` : undefined,
          quantidade: g.quantity || 1,
          pagina: Number(g.pageNumber || 1),
        });
      });

      // Execute sovereign orchestration engine
      const orchestrationResult = processarProjetoComSimbologiaOficial(
        elementosParaReconhecimento,
        options.fileName || "projeto_analisado.pdf"
      );

      job.telemetry.processingTimeMs = Date.now() - startTime;

      const finalResult = {
        data: aggregatedExtractedData,
        recognitionAudit: orchestrationResult.recognitionAudit,
        orchestration: orchestrationResult,
        officialProcessing: orchestrationResult.officialProcessing,
        relatorioOcorrencias: orchestrationResult.relatorio_ocorrencias,
        validationReport,
        telemetry: job.telemetry,
        source: "pipeline_optimized",
        catalog: {
          mnemonicosTotal: 7203,
          componentesTotal: 30949,
          itensCatalogoTotal: 1558,
          simbolosOficiaisTotal: 142,
        },
        cacheHit: false,
        hash: fileHash,
      };

      // Save into Level 1 Project Cache
      pageCache.setProjectCache(fileHash, voltageLevel, finalResult);

      job.status = "COMPLETED";
      job.progress = 100;
      job.stageMessage = "Análise concluída com sucesso!";
      job.result = finalResult;
      await jobStorage.updateJob(job);

      return finalResult;
    } catch (err: any) {
      console.error("[Pipeline] Erro durante o processamento do pipeline:", err);
      job.status = "FAILED";
      job.error = err?.message || String(err);
      job.stageMessage = `Falha no processamento: ${job.error}`;
      await jobStorage.updateJob(job);
      throw err;
    }
  }

  private mergePageData(target: any, source: any, pageNum: number): void {
    if (!source) return;

    if (Array.isArray(source.detectedPoles)) {
      source.detectedPoles.forEach((p: any) => {
        target.detectedPoles.push({ ...p, pageNumber: pageNum });
      });
    }

    if (Array.isArray(source.detectedStructures)) {
      source.detectedStructures.forEach((s: any) => {
        target.detectedStructures.push({ ...s, pageNumber: pageNum });
      });
    }

    if (Array.isArray(source.detectedEquipment)) {
      source.detectedEquipment.forEach((eq: any) => {
        target.detectedEquipment.push({ ...eq, pageNumber: pageNum });
      });
    }

    if (Array.isArray(source.detectedTransformers)) {
      source.detectedTransformers.forEach((t: any) => {
        target.detectedTransformers.push({ ...t, pageNumber: pageNum });
      });
    }

    if (Array.isArray(source.detectedGuys)) {
      source.detectedGuys.forEach((g: any) => {
        target.detectedGuys.push({ ...g, pageNumber: pageNum });
      });
    }

    if (Array.isArray(source.detectedCables)) {
      source.detectedCables.forEach((c: any) => {
        target.detectedCables.push({ ...c, pageNumber: pageNum });
      });
    }
  }
}

export const projectProcessingPipeline = ProjectProcessingPipeline.getInstance();
