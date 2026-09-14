import fs from "fs";
import path from "path";
import crypto from "crypto";

export const PROMPT_VERSION = "2.1";
export const MODEL_VERSION = "flash-v3";
export const RECOGNITION_VERSION = "1.0";

export interface PageCacheEntry {
  pageHash: string;
  versionKey: string;
  timestamp: number;
  extractedData: any;
  confidence: number;
}

export interface ProjectCacheEntry {
  hash: string;
  voltageLevel: string;
  timestamp: number;
  result: any;
}

function getCacheDir(): string {
  const tmpDir = path.join("/tmp", "calcpro_cache");
  try {
    if (!fs.existsSync(tmpDir)) {
      fs.mkdirSync(tmpDir, { recursive: true });
    }
    return tmpDir;
  } catch {
    const localDir = path.join(process.cwd(), "data", "cache");
    if (!fs.existsSync(localDir)) {
      try {
        fs.mkdirSync(localDir, { recursive: true });
      } catch {}
    }
    return localDir;
  }
}

export class PageCacheService {
  private static instance: PageCacheService;
  private memoryPageCache = new Map<string, PageCacheEntry>();
  private memoryProjectCache = new Map<string, ProjectCacheEntry>();
  private readonly maxMemoryEntries = 100;

  public static getInstance(): PageCacheService {
    if (!PageCacheService.instance) {
      PageCacheService.instance = new PageCacheService();
    }
    return PageCacheService.instance;
  }

  public computePageHash(contentOrBuffer: string | Buffer): string {
    return crypto.createHash("sha256").update(contentOrBuffer).digest("hex");
  }

  public getVersionKey(pageHash: string): string {
    return `${pageHash}_p${PROMPT_VERSION}_m${MODEL_VERSION}_r${RECOGNITION_VERSION}`;
  }

  public getPageCache(pageHash: string): PageCacheEntry | null {
    const versionKey = this.getVersionKey(pageHash);

    if (this.memoryPageCache.has(versionKey)) {
      return this.memoryPageCache.get(versionKey)!;
    }

    try {
      const filePath = path.join(getCacheDir(), `page_${versionKey}.json`);
      if (fs.existsSync(filePath)) {
        const raw = fs.readFileSync(filePath, "utf-8");
        const entry: PageCacheEntry = JSON.parse(raw);
        this.memoryPageCache.set(versionKey, entry);
        return entry;
      }
    } catch {}

    return null;
  }

  public setPageCache(pageHash: string, extractedData: any, confidence: number = 0.95): void {
    const versionKey = this.getVersionKey(pageHash);
    const entry: PageCacheEntry = {
      pageHash,
      versionKey,
      timestamp: Date.now(),
      extractedData,
      confidence,
    };

    if (this.memoryPageCache.size >= this.maxMemoryEntries) {
      const firstKey = this.memoryPageCache.keys().next().value;
      if (firstKey) this.memoryPageCache.delete(firstKey);
    }
    this.memoryPageCache.set(versionKey, entry);

    try {
      const filePath = path.join(getCacheDir(), `page_${versionKey}.json`);
      fs.writeFileSync(filePath, JSON.stringify(entry), "utf-8");
    } catch {}
  }

  public computeProjectHash(content: string, voltageLevel: string = "AUTO"): string {
    return crypto
      .createHash("sha256")
      .update(`${voltageLevel}::${content}`)
      .digest("hex");
  }

  public getProjectCache(fileHash: string, voltageLevel: string = "AUTO"): ProjectCacheEntry | null {
    const key = `${fileHash}_${voltageLevel}`;

    if (this.memoryProjectCache.has(key)) {
      return this.memoryProjectCache.get(key)!;
    }

    try {
      const filePath = path.join(getCacheDir(), `proj_${key}.json`);
      if (fs.existsSync(filePath)) {
        const raw = fs.readFileSync(filePath, "utf-8");
        const entry: ProjectCacheEntry = JSON.parse(raw);
        this.memoryProjectCache.set(key, entry);
        return entry;
      }
    } catch {}

    return null;
  }

  public setProjectCache(fileHash: string, voltageLevel: string = "AUTO", result: any): void {
    const key = `${fileHash}_${voltageLevel}`;
    const entry: ProjectCacheEntry = {
      hash: fileHash,
      voltageLevel,
      timestamp: Date.now(),
      result,
    };

    if (this.memoryProjectCache.size >= this.maxMemoryEntries) {
      const firstKey = this.memoryProjectCache.keys().next().value;
      if (firstKey) this.memoryProjectCache.delete(firstKey);
    }
    this.memoryProjectCache.set(key, entry);

    try {
      const filePath = path.join(getCacheDir(), `proj_${key}.json`);
      fs.writeFileSync(filePath, JSON.stringify(entry), "utf-8");
    } catch {}
  }
}

export const pageCache = PageCacheService.getInstance();
