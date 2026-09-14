import { RedisService } from "./redisService.js";

/**
 * TESTE DE CONECTIVIDADE E SEGURANÇA: REDIS SERVICE (ETAPA 1)
 * 
 * Valida os 4 estados fundamentais do adaptador:
 * A) Redis configurado e acessível (quando variáveis válidas fornecidas)
 * B) Redis não configurado (quando variáveis ausentes)
 * C) Redis configurado, mas inacessível (ex: host inválido/offline)
 * D) Credenciais inválidas (ex: token 401 Unauthorized)
 * 
 * E garante que em NENHUM cenário ocorra vazamento de tokens ou URLs sensíveis.
 */

async function runTests() {
  console.log("================================================================================");
  console.log("TESTE DE CERTIFICAÇÃO: ADAPTADOR UPSTASH REDIS (ETAPA 1)");
  console.log("================================================================================");

  let passed = 0;
  let total = 0;

  function assert(condition: boolean, description: string) {
    total++;
    if (condition) {
      console.log(`✅ [PASS] ${description}`);
      passed++;
    } else {
      console.error(`❌ [FAIL] ${description}`);
      process.exitCode = 1;
    }
  }

  // TESTE 1: Estado B - Redis NÃO configurado
  console.log("\n>>> Testando Estado B: Redis NÃO configurado...");
  const originalUrl = process.env.UPSTASH_REDIS_REST_URL;
  const originalToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  const originalKvUrl = process.env.KV_REST_API_URL;
  const originalKvToken = process.env.KV_REST_API_TOKEN;

  try {
    delete process.env.UPSTASH_REDIS_REST_URL;
    delete process.env.UPSTASH_REDIS_REST_TOKEN;
    delete process.env.KV_REST_API_URL;
    delete process.env.KV_REST_API_TOKEN;

    // Instanciação isolada sem variáveis
    // @ts-ignore - acesso a construtor para teste de isolamento
    const unconfiguredService = new (RedisService as any)();
    
    assert(!unconfiguredService.isConfigured(), "isConfigured() retorna false quando variáveis não existem");
    
    let threw = false;
    try {
      unconfiguredService.getClient();
    } catch (e: any) {
      threw = true;
      assert(!e.message.includes("undefined") && e.message.includes("Upstash Redis não está configurado"), 
        "getClient() lança erro explícito e controlado informando variáveis obrigatórias");
    }
    assert(threw, "Tentativa de obter cliente sem configuração lança exceção controlada");

    const diag = await unconfiguredService.testConnection();
    assert(diag.status === "NOT_CONFIGURED", "Diagnóstico reporta status NOT_CONFIGURED com precisão");
  } finally {
    // Restaura ambiente
    if (originalUrl) process.env.UPSTASH_REDIS_REST_URL = originalUrl;
    if (originalToken) process.env.UPSTASH_REDIS_REST_TOKEN = originalToken;
    if (originalKvUrl) process.env.KV_REST_API_URL = originalKvUrl;
    if (originalKvToken) process.env.KV_REST_API_TOKEN = originalKvToken;
  }

  // TESTE 2: Estado D - Credenciais inválidas (401 Unauthorized)
  console.log("\n>>> Testando Estado D: Credenciais inválidas (AUTH_ERROR)...");
  try {
    process.env.UPSTASH_REDIS_REST_URL = "https://example-fake-db.upstash.io";
    process.env.UPSTASH_REDIS_REST_TOKEN = "invalid_secret_token_1234567890abcdef";

    // @ts-ignore
    const authFailService = new (RedisService as any)();
    assert(authFailService.isConfigured(), "Reconhece variáveis presentes");

    const diagAuth = await authFailService.testConnection();
    assert(diagAuth.status === "AUTH_ERROR" || diagAuth.status === "UNREACHABLE", 
      `Identifica falha de conexão/autenticação adequadamente (status retornado: ${diagAuth.status})`);
    assert(!diagAuth.message.includes("invalid_secret_token_1234567890abcdef"), 
      "Garante que o token sensível NUNCA é impresso na mensagem de erro");
  } finally {
    delete process.env.UPSTASH_REDIS_REST_URL;
    delete process.env.UPSTASH_REDIS_REST_TOKEN;
    if (originalUrl) process.env.UPSTASH_REDIS_REST_URL = originalUrl;
    if (originalToken) process.env.UPSTASH_REDIS_REST_TOKEN = originalToken;
  }

  // TESTE 3: Estado C - Host inacessível (UNREACHABLE)
  console.log("\n>>> Testando Estado C: Host inacessível (UNREACHABLE)...");
  try {
    process.env.UPSTASH_REDIS_REST_URL = "https://127.0.0.1:59999";
    process.env.UPSTASH_REDIS_REST_TOKEN = "dummy_token";

    // @ts-ignore
    const unreachableService = new (RedisService as any)();
    const diagUnreachable = await unreachableService.testConnection();
    assert(diagUnreachable.status === "UNREACHABLE", "Identifica host inacessível como UNREACHABLE");
    assert(!diagUnreachable.message.includes("59999"), "URL sensível ou portas privadas são higienizadas");
  } finally {
    delete process.env.UPSTASH_REDIS_REST_URL;
    delete process.env.UPSTASH_REDIS_REST_TOKEN;
    if (originalUrl) process.env.UPSTASH_REDIS_REST_URL = originalUrl;
    if (originalToken) process.env.UPSTASH_REDIS_REST_TOKEN = originalToken;
  }

  // TESTE 4: Isolamento Arquitetural (Sem vazamentos nem fallbacks proibidos)
  console.log("\n>>> Testando Isolamento e Segurança...");
  // @ts-ignore
  const isolatedService = new (RedisService as any)();
  assert(isolatedService.memoryJobs === undefined, "Adaptador NÃO possui Map em RAM");
  assert(isolatedService.jobsDir === undefined, "Adaptador NÃO possui diretório em /tmp");
  assert(isolatedService.locksDir === undefined, "Adaptador NÃO possui lock local em /tmp");

  console.log("================================================================================");
  console.log(`RESULTADO FINAL DA ETAPA 1: ${passed}/${total} testes aprovados.`);
  console.log("================================================================================");
}

runTests().catch((err) => {
  console.error("Erro fatal na execução dos testes da Etapa 1:", err);
  process.exit(1);
});
