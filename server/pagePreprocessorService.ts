import crypto from "crypto";

export type PageRelevance = "RELEVANTE" | "POSSIVELMENTE_RELEVANTE" | "IRRELEVANTE";

export interface AnalyzedPage {
  pageNumber: number;
  pageHash: string;
  relevance: PageRelevance;
  relevanceReason: string;
  hasNativeText: boolean;
  textSnippet: string;
  deterministicData?: {
    poles: any[];
    structures: any[];
    equipment: any[];
    transformers: any[];
    cables: any[];
    guys: any[];
  };
  isDeterministicComplete: boolean;
  imageBufferOrBase64?: string;
}

export interface DocumentPreprocessResult {
  isPdf: boolean;
  totalPages: number;
  pages: AnalyzedPage[];
  relevantPagesCount: number;
  discardedPagesCount: number;
  overallDeterministicComplete: boolean;
}

// Regex patterns for electrical network detection and technical project sheets
const KEYWORD_NETWORK_RELEVANTE =
  /\b(POSTE|P[0-9]{1,4}\b|ESTR|ESTRUTURA|REDE|DISTRIBUI[ÇC]|CEMIG|CHAVE|FUS[IÍ]VEL|SECCIONADOR|DISJUNTOR|TRANSFORMADOR|TRAFO|KVA|CONDUTOR|CABO|CAA|CAL|COP|MULTIPLEX|ESTAI|[AÁ]NCORA|13\.?8\s*K?V|34\.?5\s*K?V|380\s*V|220\s*V|CRUZETA|ISOLADOR|DAN|CIRCULAR|DUPLO\s*T|DT\b|N[1-4]\b|M[1-4]\b|CE[1-4]\b|2CE[1-4]\b|B[1-4]\b|U[1-4]\b|SI[1-4]\b|S[1-2][1-4]N\b|PLANTA|PRANCHA|CROQUI|UNIFILAR|TRIFILAR|DIAGRAMA|ESQUEMA|ALIMENTADOR|CIRCUITO|SUBESTA[ÇC][ÃA]O|DERIVA[ÇC][ÃA]O|RAMAL|BARRAMENTO|PARA[\-\s]*RAIO|CHAVE[\-\s]*FACA|SECCIONADORA)\b/i;

const KEYWORD_DOCUMENT_IRRELEVANTE =
  /\b(ANOTA[ÇC][ÃA]O DE RESPONSABILIDADE T[EÉ]CNICA|A\.?R\.?T\.?|TERMO DE COMPROMISSO|CONTRATO DE PRESTA[ÇC][ÃA]O|CERTID[ÃA]O|PROCURA[ÇC][ÃA]O)\b/i;

/**
 * Pre-processes PDF document or image to classify pages and extract deterministic text.
 */
export class PagePreprocessorService {
  private static instance: PagePreprocessorService;

  public static getInstance(): PagePreprocessorService {
    if (!PagePreprocessorService.instance) {
      PagePreprocessorService.instance = new PagePreprocessorService();
    }
    return PagePreprocessorService.instance;
  }

  /**
   * Analyzes an input buffer or base64 (PDF or image).
   */
  public async analyzeDocument(
    base64Data: string,
    mimeType: string = "image/jpeg"
  ): Promise<DocumentPreprocessResult> {
    const isPdf = mimeType.toLowerCase().includes("pdf") || base64Data.startsWith("JVBERi0");

    if (!isPdf) {
      // Single Image document (JPEG/PNG)
      const pageHash = crypto.createHash("sha256").update(base64Data).digest("hex");
      const page: AnalyzedPage = {
        pageNumber: 1,
        pageHash,
        relevance: "RELEVANTE",
        relevanceReason: "Arquivo de imagem direta (planta/croqui do projeto).",
        hasNativeText: false,
        textSnippet: "",
        isDeterministicComplete: false,
        imageBufferOrBase64: base64Data,
      };

      return {
        isPdf: false,
        totalPages: 1,
        pages: [page],
        relevantPagesCount: 1,
        discardedPagesCount: 0,
        overallDeterministicComplete: false,
      };
    }

    // PDF processing using pdfjs-dist legacy build
    return await this.analyzePdfDocument(base64Data);
  }

  private async analyzePdfDocument(base64Data: string): Promise<DocumentPreprocessResult> {
    const buffer = Buffer.from(base64Data, "base64");
    const pages: AnalyzedPage[] = [];

    try {
      // Dynamic import of legacy pdfjs build for Node.js compatibility
      // @ts-ignore
      const pdfjsLib = await import("pdfjs-dist/legacy/build/pdf.mjs");
      const loadingTask = pdfjsLib.getDocument({
        data: new Uint8Array(buffer),
        disableFontFace: true,
        useSystemFonts: true,
      });

      const pdfDoc = await loadingTask.promise;
      const totalPages = pdfDoc.numPages;

      for (let pageNum = 1; pageNum <= totalPages; pageNum++) {
        const page = await pdfDoc.getPage(pageNum);
        const textContent = await page.getTextContent();
        const pageText = textContent.items
          // @ts-ignore
          .map((item: any) => item.str || "")
          .join(" ")
          .trim();

        // Stable page hash based on page text + page index + file buffer segment
        const pageContentSample = `${pageNum}::${pageText}::${buffer.subarray(0, 1024).toString("hex")}`;
        const pageHash = crypto.createHash("sha256").update(pageContentSample).digest("hex");

        const hasText = pageText.length > 20;
        let relevance: PageRelevance = "RELEVANTE";
        let relevanceReason = "";

        if (hasText) {
          const hasNetworkKeywords = KEYWORD_NETWORK_RELEVANTE.test(pageText);
          const hasIrrelevantKeywords = KEYWORD_DOCUMENT_IRRELEVANTE.test(pageText);

          if (hasIrrelevantKeywords && !hasNetworkKeywords) {
            relevance = "IRRELEVANTE";
            relevanceReason = "Página de documentação administrativa (ART/Termo/Certidão sem diagrama).";
          } else if (hasNetworkKeywords) {
            relevance = "RELEVANTE";
            relevanceReason = "Página técnica contendo palavras-chave e marcações da rede elétrica.";
          } else {
            // Default to POSSIVELMENTE_RELEVANTE so diagrams or CAD sheets are not dropped
            relevance = "POSSIVELMENTE_RELEVANTE";
            relevanceReason = "Prancha gráfica ou diagrama técnico do projeto.";
          }
        } else {
          // Scanned page or pure vector diagram with minimal selectable text
          relevance = "RELEVANTE";
          relevanceReason = "Prancha gráfica com desenho técnico ou escaneada.";
        }

        // Attempt deterministic extraction if native text contains structured schedules
        let deterministicData = undefined;
        let isDeterministicComplete = false;

        if (hasText && relevance !== "IRRELEVANTE") {
          const extracted = this.extractDeterministicFromText(pageText, pageNum);
          if (
            extracted.poles.length > 0 ||
            extracted.structures.length > 0 ||
            extracted.transformers.length > 0 ||
            extracted.cables.length > 0 ||
            extracted.equipment.length > 0
          ) {
            deterministicData = extracted;
            // If table has comprehensive poles and structures
            if (extracted.poles.length >= 2 && extracted.structures.length >= 2) {
              isDeterministicComplete = true;
            }
          }
        }

        pages.push({
          pageNumber: pageNum,
          pageHash,
          relevance,
          relevanceReason,
          hasNativeText: hasText,
          textSnippet: pageText.slice(0, 200),
          deterministicData,
          isDeterministicComplete,
        });
      }

      const relevantCount = pages.filter((p) => p.relevance !== "IRRELEVANTE").length;
      const discardedCount = totalPages - relevantCount;
      const allRelevantDeterministic =
        relevantCount > 0 &&
        pages
          .filter((p) => p.relevance !== "IRRELEVANTE")
          .every((p) => p.isDeterministicComplete);

      return {
        isPdf: true,
        totalPages,
        pages,
        relevantPagesCount: relevantCount,
        discardedPagesCount: discardedCount,
        overallDeterministicComplete: allRelevantDeterministic,
      };
    } catch (err: any) {
      console.warn("[PagePreprocessor] Fallback na leitura PDF (tratando como página única):", err?.message);
      // Fallback safe: treat as 1 page relevant
      const pageHash = crypto.createHash("sha256").update(base64Data.slice(0, 2048)).digest("hex");
      return {
        isPdf: true,
        totalPages: 1,
        pages: [
          {
            pageNumber: 1,
            pageHash,
            relevance: "RELEVANTE",
            relevanceReason: "Fallback de leitura direta do PDF.",
            hasNativeText: false,
            textSnippet: "",
            isDeterministicComplete: false,
          },
        ],
        relevantPagesCount: 1,
        discardedPagesCount: 0,
        overallDeterministicComplete: false,
      };
    }
  }

  /**
   * Deterministic regex parser for structured schedule tables or notes on CEMIG sheets.
   */
  public extractDeterministicFromText(
    text: string,
    pageNum: number
  ): {
    poles: any[];
    structures: any[];
    equipment: any[];
    transformers: any[];
    cables: any[];
    guys: any[];
  } {
    const poles: any[] = [];
    const structures: any[] = [];
    const equipment: any[] = [];
    const transformers: any[] = [];
    const cables: any[] = [];
    const guys: any[] = [];

    // Pattern: P1, P2 ... P99 followed by specification (e.g., "P1 11-300 N1 CE1", "POSTE 1: 10/150 - N2")
    const poleRegex =
      /\b(?:POSTE\s*|P)([0-9]{1,3})\s*[:\-\/]?\s*([0-9]{1,2}[\-\/][0-9]{2,4}[A-Z]*|[A-Z0-9\-\/]+)?(?:\s+(CIRCULAR|DUPLO\s*T|DT|CIRC))?/gi;

    let match;
    const seenPoles = new Set<string>();

    while ((match = poleRegex.exec(text)) !== null) {
      const pNum = match[1];
      const pId = `P${pNum}`;
      if (!seenPoles.has(pId)) {
        seenPoles.add(pId);
        const spec = match[2] || "11-300";
        const shape = match[3]
          ? match[3].toUpperCase().includes("DUPLO") || match[3].toUpperCase() === "DT"
            ? "DUPLO T"
            : "CIRCULAR"
          : "CIRCULAR";

        poles.push({
          id: pId,
          typeSpec: spec,
          shape,
          material: "CONCRETO",
          status: "INSTALAR",
          pageNumber: pageNum,
        });
      }
    }

    // Pattern: Structures like N1, N2, N3, CE1, CE2, M1, B1, U1
    const structRegex =
      /\b(N[1-4]|M[1-4]|CE[1-4]|2CE[1-4]|B[1-4]|U[1-4]|SI[1-4]|S[1-2][1-4]N)\b/gi;
    let sMatch;
    let sIdx = 1;
    while ((sMatch = structRegex.exec(text)) !== null) {
      const code = sMatch[1].toUpperCase();
      structures.push({
        id: `ESTR_${pageNum}_${sIdx++}`,
        code,
        voltage: code.startsWith("CE") ? "BT" : "MT",
        status: "INSTALAR",
        associatedPost: "11-300",
        description: `Estrutura ${code}`,
        pageNumber: pageNum,
      });
    }

    // Transformers (e.g. "TRAFO 45KVA", "15 KVA", "30 KVA")
    const trafoRegex = /\b(?:TRAFO|TRANSFORMADOR)?\s*([0-9]+(?:\.[0-9]+)?)\s*KVA\b/gi;
    let tMatch;
    let tIdx = 1;
    while ((tMatch = trafoRegex.exec(text)) !== null) {
      const power = tMatch[1];
      transformers.push({
        id: `TR_${pageNum}_${tIdx++}`,
        powerKva: power,
        voltage: "15kV",
        type: "TRIFASICO",
        status: "INSTALAR",
        description: `Transformador ${power}kVA`,
        pageNumber: pageNum,
      });
    }

    // Cables (e.g. "CAA 1/0", "CAA 4 AWG", "MULTIPLEXADO 3X70")
    const cableRegex =
      /\b(CAA\s*[0-9\/]+\s*(?:AWG|MCM)?|CAL\s*[0-9\/]+|MULTIPLEXADO\s*[0-9x\+]+)\b/gi;
    let cMatch;
    let cIdx = 1;
    while ((cMatch = cableRegex.exec(text)) !== null) {
      const cType = cMatch[1].trim();
      cables.push({
        id: `CAB_${pageNum}_${cIdx++}`,
        cableType: cType,
        voltage: cType.includes("MULTIPLEXADO") ? "BT" : "MT",
        status: "INSTALAR",
        spansCount: 1,
        estimatedLengthMeters: 40,
        pageNumber: pageNum,
      });
    }

    // Equipment: switches, cutouts, surge arresters, etc.
    const equipRegex = /\b(CFS|CHAVE\s*FUS[IÍ]VEL|CHAVE\s*FACA|SECCIONADORA|DISJUNTOR|PARA[\-\s]*RAIO|MUFLA)\b/gi;
    let eqMatch;
    let eqIdx = 1;
    while ((eqMatch = equipRegex.exec(text)) !== null) {
      const eqRaw = eqMatch[1].toUpperCase();
      const code = eqRaw.includes("FUS") || eqRaw === "CFS" ? "CFS" : eqRaw;
      equipment.push({
        id: `EQ_${pageNum}_${eqIdx++}`,
        code,
        type: code === "CFS" ? "CHAVE_FUSIVEL" : "EQUIPAMENTO",
        status: "INSTALAR",
        description: `Equipamento ${code}`,
        pageNumber: pageNum,
      });
    }

    // Guys / Stay wires
    const guyRegex = /\b(ESTAI|CONTRA[\-\s]*POSTE|[AÁ]NCORA|ESTAIAMENTO)\b/gi;
    let gMatch;
    let gIdx = 1;
    while ((gMatch = guyRegex.exec(text)) !== null) {
      guys.push({
        id: `EST_${pageNum}_${gIdx++}`,
        type: "ANCORA",
        quantity: 1,
        status: "INSTALAR",
        pageNumber: pageNum,
      });
    }

    return {
      poles,
      structures,
      equipment,
      transformers,
      cables,
      guys,
    };
  }
}

export const pagePreprocessor = PagePreprocessorService.getInstance();
