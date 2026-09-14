import { Redis } from "@upstash/redis";

/**
 * Interface de Diagnóstico de Conexão com Redis
 */
export interface RedisConnectionTestResult {
  status: "OK" | "NOT_CONFIGURED" | "UNREACHABLE" | "AUTH_ERROR";
  message: string;
}

/**
 * Opções de gravação no Redis
 */
export interface RedisSetOptions {
  /** Tempo de expiração em segundos */
  ex?: number;
}

/**
 * Serviço de Acesso Isolado ao Upstash Redis / Vercel KV
 * 
 * Executa exclusivamente no lado servidor (Node.js / Vercel Serverless).
 * Utiliza o protocolo REST HTTP nativo do Upstash, sem conexões TCP persistentes.
 * 
 * REGRA ABSOLUTA DE SEGURANÇA E CONCORRÊNCIA:
 * Não possui fallback para Map em RAM nem para /tmp. Se as variáveis não estiverem
 * configuradas em ambiente que exija persistência distribuída, lança erro controlado
 * e explícito, impedindo ilusão de concorrência segura em instâncias isoladas.
 */
export class RedisService {
  private static instance: RedisService | null = null;
  private client: Redis | null = null;
  private readonly configured: boolean = false;
  private readonly hasAttemptedInit: boolean = false;

  private constructor() {
    const rawUrl = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
    const rawToken = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

    const url = rawUrl ? rawUrl.trim() : "";
    const token = rawToken ? rawToken.trim() : "";

    if (url && token && (url.startsWith("https://") || url.startsWith("http://"))) {
      try {
        this.client = new Redis({
          url,
          token,
        });
        this.configured = true;
      } catch (err: any) {
        this.client = null;
        this.configured = false;
        console.warn(`[RedisService] Falha ao instanciar Upstash Redis: ${err?.message || err}`);
      }
    } else {
      this.client = null;
      this.configured = false;
    }
    this.hasAttemptedInit = true;
  }

  /**
   * Obtém a instância singleton do serviço
   */
  public static getInstance(): RedisService {
    if (!RedisService.instance) {
      RedisService.instance = new RedisService();
    }
    return RedisService.instance;
  }

  /**
   * Retorna se o Redis possui credenciais configuradas no ambiente
   */
  public isConfigured(): boolean {
    return this.configured && this.client !== null;
  }

  /**
   * Obtém o cliente nativo do Upstash Redis.
   * Lança erro explícito se não estiver configurado.
   */
  public getClient(): Redis {
    if (!this.client || !this.configured) {
      throw new Error(
        "[RedisService] Upstash Redis não está configurado. As variáveis de ambiente UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN (ou KV_REST_API_*) são obrigatórias para operações com persistência distribuída na Vercel."
      );
    }
    return this.client;
  }

  /**
   * Busca um valor por chave
   */
  public async get<T>(key: string): Promise<T | null> {
    const client = this.getClient();
    try {
      const result = await client.get<T>(key);
      return result;
    } catch (err: any) {
      this.handleRedisError("get", key, err);
      throw err;
    }
  }

  /**
   * Grava um valor com expiração opcional (em segundos ou objeto RedisSetOptions)
   */
  public async set(key: string, value: any, opts?: RedisSetOptions | number): Promise<void> {
    const client = this.getClient();
    const exSeconds = typeof opts === "number" ? opts : opts?.ex;
    try {
      if (exSeconds && exSeconds > 0) {
        await client.set(key, value, { ex: exSeconds });
      } else {
        await client.set(key, value);
      }
    } catch (err: any) {
      this.handleRedisError("set", key, err);
      throw err;
    }
  }

  /**
   * Operação Atômica SET NX EX (Adquire lock ou chave de idempotência exclusiva)
   * Retorna true se a chave foi adquirida (não existia antes), false se já existia.
   */
  public async setNx(key: string, value: any, ttlSeconds: number): Promise<boolean> {
    const client = this.getClient();
    try {
      const result = await client.set(key, value, {
        nx: true,
        ex: ttlSeconds,
      });
      return result === "OK";
    } catch (err: any) {
      this.handleRedisError("setNx", key, err);
      throw err;
    }
  }

  /**
   * Liberação atômica de lock via script Lua
   * Deleta a chave SOMENTE se o valor atual for estritamente igual ao token esperado.
   * Retorna true se a chave pertencia ao token e foi deletada, ou false caso contrário.
   */
  public async releaseLockAtomic(key: string, expectedToken: string): Promise<boolean> {
    const client = this.getClient();
    const luaScript = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
      else
        return 0
      end
    `;
    try {
      const result = await client.eval(luaScript, [key], [expectedToken]);
      return result === 1;
    } catch (err: any) {
      this.handleRedisError("releaseLockAtomic", key, err);
      throw err;
    }
  }

  /**
   * Remove uma chave
   */
  public async del(key: string): Promise<number> {
    const client = this.getClient();
    try {
      return await client.del(key);
    } catch (err: any) {
      this.handleRedisError("del", key, err);
      throw err;
    }
  }

  /**
   * Verifica se uma chave existe
   */
  public async exists(key: string): Promise<boolean> {
    const client = this.getClient();
    try {
      const result = await client.exists(key);
      return result === 1;
    } catch (err: any) {
      this.handleRedisError("exists", key, err);
      throw err;
    }
  }

  /**
   * Grava múltiplos campos em um Hash Redis
   */
  public async hset(key: string, fieldValues: Record<string, any>): Promise<number> {
    const client = this.getClient();
    try {
      return await client.hset(key, fieldValues);
    } catch (err: any) {
      this.handleRedisError("hset", key, err);
      throw err;
    }
  }

  /**
   * Obtém todos os campos e valores de um Hash Redis
   */
  public async hgetall<T extends Record<string, any>>(key: string): Promise<T | null> {
    const client = this.getClient();
    try {
      const result = await client.hgetall<T>(key);
      if (!result || Object.keys(result).length === 0) {
        return null;
      }
      return result;
    } catch (err: any) {
      this.handleRedisError("hgetall", key, err);
      throw err;
    }
  }

  /**
   * Define TTL (expiração) para uma chave em segundos
   */
  public async expire(key: string, ttlSeconds: number): Promise<boolean> {
    const client = this.getClient();
    try {
      const result = await client.expire(key, ttlSeconds);
      return result === 1;
    } catch (err: any) {
      this.handleRedisError("expire", key, err);
      throw err;
    }
  }

  /**
   * Consulta o TTL (tempo de vida restante em segundos) de uma chave no Redis
   * Retorna:
   *   - Segundos restantes (> 0)
   *   - -1 se a chave existe mas não tem expiração definida
   *   - -2 se a chave não existe
   */
  public async ttl(key: string): Promise<number> {
    const client = this.getClient();
    try {
      const result = await client.ttl(key);
      return result;
    } catch (err: any) {
      this.handleRedisError("ttl", key, err);
      throw err;
    }
  }

  /**
   * Teste seguro de conectividade que categoriza o status sem expor credenciais
   */
  public async testConnection(): Promise<RedisConnectionTestResult> {
    if (!this.isConfigured()) {
      return {
        status: "NOT_CONFIGURED",
        message: "Variáveis UPSTASH_REDIS_REST_URL ou UPSTASH_REDIS_REST_TOKEN não encontradas no ambiente.",
      };
    }

    try {
      const client = this.client!;
      const testKey = `calcpro:healthcheck:${Date.now()}`;
      
      // Realiza gravação de teste com TTL de 5 segundos e leitura
      await client.set(testKey, "health_ok", { ex: 5 });
      const readBack = await client.get<string>(testKey);
      await client.del(testKey);

      if (readBack === "health_ok") {
        return {
          status: "OK",
          message: "Conexão com Upstash Redis validada com sucesso.",
        };
      } else {
        return {
          status: "UNREACHABLE",
          message: "Operação de leitura/escrita retornou valor inconsistente.",
        };
      }
    } catch (err: any) {
      const errorMsg = String(err?.message || err);
      const isAuthError = 
        errorMsg.toLowerCase().includes("unauthorized") || 
        errorMsg.toLowerCase().includes("forbidden") ||
        errorMsg.includes("401") || 
        errorMsg.includes("403");

      if (isAuthError) {
        return {
          status: "AUTH_ERROR",
          message: "Falha de autenticação: Credenciais do Upstash Redis inválidas ou expiradas.",
        };
      }

      return {
        status: "UNREACHABLE",
        message: `Serviço Upstash Redis inacessível ou erro de rede: ${this.sanitizeErrorMessage(errorMsg)}`,
      };
    }
  }

  /**
   * Higienização estrita de mensagens de erro para impedir vazamento de URLs ou tokens
   */
  private sanitizeErrorMessage(msg: string): string {
    return msg
      .replace(/https?:\/\/[^\s"'`]+/gi, "[REDACTED_URL]")
      .replace(/Bearer\s+[A-Za-z0-9_\-\.]+/gi, "[REDACTED_TOKEN]")
      .replace(/[a-zA-Z0-9_\-]{32,}/g, "[REDACTED_SECRET]");
  }

  /**
   * Tratamento de erro seguro
   */
  private handleRedisError(operation: string, key: string, err: any): void {
    const safeKey = key.split(":").slice(0, 3).join(":");
    const safeMsg = this.sanitizeErrorMessage(String(err?.message || err));
    console.error(`[RedisService] Erro na operação '${operation}' para chave '${safeKey}': ${safeMsg}`);
  }
}

export const redisService = RedisService.getInstance();
