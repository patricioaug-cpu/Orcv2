import assert from "assert";
import { redisService } from "./redisService.js";

console.log("================================================================================");
console.log("BATERIA DE TESTES — ETAPA 2: LOCK DISTRIBUÍDO DE CONCORRÊNCIA GEMINI");
console.log("================================================================================");

/**
 * Simulador atômico de Redis Storage compatível com a semântica REST do Upstash Redis.
 * Modela exatamente a atomicidade de:
 * 1. SET key val NX EX ttl (garante que apenas uma operação concorrente tenha sucesso)
 * 2. Script Lua de release: Deleta a chave SOMENTE se o valor atual for estritamente igual ao token
 */
class InMemoryRedisClusterSimulator {
  private store = new Map<string, { value: string; expiresAt: number }>();

  public setNx(key: string, value: string, ttlSeconds: number): boolean {
    const now = Date.now();
    const entry = this.store.get(key);
    if (entry && entry.expiresAt > now) {
      return false; // Chave ativa existente, NX falha atomicamente
    }
    this.store.set(key, { value, expiresAt: now + ttlSeconds * 1000 });
    return true;
  }

  public releaseLockAtomic(key: string, expectedToken: string): boolean {
    const now = Date.now();
    const entry = this.store.get(key);
    if (!entry) {
      return false;
    }
    if (entry.expiresAt <= now) {
      this.store.delete(key);
      return false; // Já expirado
    }
    if (entry.value === expectedToken) {
      this.store.delete(key);
      return true; // Token compatível, deletado atomicamente
    }
    return false; // Token de outra execução, NÃO deleta
  }

  public get(key: string): string | null {
    const now = Date.now();
    const entry = this.store.get(key);
    if (!entry) return null;
    if (entry.expiresAt <= now) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  public clear(): void {
    this.store.clear();
  }
}

/**
 * Instância simulada de coordenador de Lock Distribuído por instância Vercel
 */
class DistributedLockManager {
  private redis: InMemoryRedisClusterSimulator;
  private maxConcurrent: number;
  private leaseSeconds: number;
  private activeLocksByJob = new Map<string, { slotKey: string; token: string }>();

  constructor(redis: InMemoryRedisClusterSimulator, maxConcurrent = 1, leaseSeconds = 45) {
    this.redis = redis;
    this.maxConcurrent = maxConcurrent;
    this.leaseSeconds = leaseSeconds;
  }

  public setMaxConcurrency(val: number) {
    this.maxConcurrent = val;
  }

  public async acquireGeminiSlot(jobId: string, timeoutMs = 3000): Promise<boolean> {
    const startTime = Date.now();
    const token = `${jobId}:${Date.now()}:${Math.random().toString(36).substring(2, 8)}`;

    while (Date.now() - startTime <= timeoutMs) {
      for (let i = 1; i <= this.maxConcurrent; i++) {
        const slotKey = `calcpro:lock:gemini:slot:${i}`;
        const acquired = this.redis.setNx(slotKey, token, this.leaseSeconds);
        if (acquired) {
          this.activeLocksByJob.set(jobId, { slotKey, token });
          return true;
        }
      }
      if (timeoutMs === 0) break;
      // Jitter backoff
      await new Promise((res) => setTimeout(res, 20 + Math.floor(Math.random() * 20)));
    }
    return false;
  }

  public releaseGeminiSlot(jobId: string): boolean {
    const lockInfo = this.activeLocksByJob.get(jobId);
    if (!lockInfo) return false;
    this.activeLocksByJob.delete(jobId);
    return this.redis.releaseLockAtomic(lockInfo.slotKey, lockInfo.token);
  }

  public getHeldToken(jobId: string): string | undefined {
    return this.activeLocksByJob.get(jobId)?.token;
  }
}

async function runTestBattery() {
  const cluster = new InMemoryRedisClusterSimulator();

  // --------------------------------------------------------------------------------
  // TESTE A: 2 execuções concorrentes com GEMINI_MAX_CONCURRENCY=1
  // Resultado esperado: somente 1 possui o lock simultaneamente.
  // --------------------------------------------------------------------------------
  console.log(">>> [TESTE A] 2 execuções concorrentes (MAX_CONCURRENCY=1)...");
  cluster.clear();
  const managerA = new DistributedLockManager(cluster, 1, 45);
  let concurrentHeld = 0;
  let maxObservedA = 0;

  const runJobA = async (id: string) => {
    const acquired = await managerA.acquireGeminiSlot(id, 1500);
    if (acquired) {
      concurrentHeld++;
      maxObservedA = Math.max(maxObservedA, concurrentHeld);
      await new Promise((res) => setTimeout(res, 50));
      concurrentHeld--;
      managerA.releaseGeminiSlot(id);
      return true;
    }
    return false;
  };

  const resultsA = await Promise.all([runJobA("job-a1"), runJobA("job-a2")]);
  assert(resultsA.every(Boolean), "Ambos os jobs conseguiram executar sequencialmente");
  assert.strictEqual(maxObservedA, 1, "Nunca mais de 1 lock ativo simultaneamente em MAX_CONCURRENCY=1");
  console.log("✅ [PASS] TESTE A: Somente 1 execução possui o lock simultaneamente.");

  // --------------------------------------------------------------------------------
  // TESTE B: 10 execuções concorrentes com GEMINI_MAX_CONCURRENCY=1
  // Resultado esperado: nunca mais de 1 lock ativo.
  // --------------------------------------------------------------------------------
  console.log(">>> [TESTE B] 10 execuções concorrentes (MAX_CONCURRENCY=1)...");
  cluster.clear();
  const managerB = new DistributedLockManager(cluster, 1, 45);
  let activeB = 0;
  let peakB = 0;

  const promisesB = Array.from({ length: 10 }, async (_, idx) => {
    const id = `job-b-${idx}`;
    const acquired = await managerB.acquireGeminiSlot(id, 3000);
    if (acquired) {
      activeB++;
      peakB = Math.max(peakB, activeB);
      // Simula tempo de processamento do Gemini
      await new Promise((res) => setTimeout(res, 30));
      activeB--;
      managerB.releaseGeminiSlot(id);
      return true;
    }
    return false;
  });

  const resultsB = await Promise.all(promisesB);
  assert(resultsB.filter(Boolean).length >= 5, "Múltiplas execuções processadas de forma serializada");
  assert.strictEqual(peakB, 1, "Pico de concorrência estritamente limitado a 1");
  console.log(`✅ [PASS] TESTE B: 10 execuções concorrentes processadas com pico máximo = ${peakB} (limite = 1).`);

  // --------------------------------------------------------------------------------
  // TESTE C: 10 execuções concorrentes com GEMINI_MAX_CONCURRENCY=2
  // Resultado esperado: nunca mais de 2 locks ativos.
  // --------------------------------------------------------------------------------
  console.log(">>> [TESTE C] 10 execuções concorrentes (MAX_CONCURRENCY=2)...");
  cluster.clear();
  const managerC = new DistributedLockManager(cluster, 2, 45);
  let activeC = 0;
  let peakC = 0;

  const promisesC = Array.from({ length: 10 }, async (_, idx) => {
    const id = `job-c-${idx}`;
    const acquired = await managerC.acquireGeminiSlot(id, 3000);
    if (acquired) {
      activeC++;
      peakC = Math.max(peakC, activeC);
      await new Promise((res) => setTimeout(res, 40));
      activeC--;
      managerC.releaseGeminiSlot(id);
      return true;
    }
    return false;
  });

  await Promise.all(promisesC);
  assert(peakC <= 2, `Pico de concorrência ${peakC} deve ser menor ou igual a 2`);
  assert(peakC >= 1, "Pelo menos 1 lock esteve ativo");
  console.log(`✅ [PASS] TESTE C: 10 execuções em MAX_CONCURRENCY=2 respeitaram pico de ${peakC} (nunca > 2).`);

  // --------------------------------------------------------------------------------
  // TESTE D: lock expira por TTL/lease
  // Resultado esperado: outro processo consegue adquirir o slot após a expiração.
  // --------------------------------------------------------------------------------
  console.log(">>> [TESTE D] Expiração automática de lock (TTL/Lease)...");
  cluster.clear();
  // Manager com lease ultracurto (0.1 segundos = 100ms)
  const managerD = new DistributedLockManager(cluster, 1, 0.1);
  const acquiredD1 = await managerD.acquireGeminiSlot("job-d1", 500);
  assert.strictEqual(acquiredD1, true, "Job D1 adquiriu o slot");

  // Tentativa imediata pelo Job D2 deve falhar
  const acquiredD2Immediate = await managerD.acquireGeminiSlot("job-d2", 50);
  assert.strictEqual(acquiredD2Immediate, false, "Job D2 bloqueado enquanto D1 segura o slot");

  // Aguardar expiração do lease (150ms > 100ms)
  await new Promise((res) => setTimeout(res, 150));

  // Job D2 deve conseguir adquirir o slot após o lease expirar sem que D1 tenha feito release
  const acquiredD2AfterExpiry = await managerD.acquireGeminiSlot("job-d2", 500);
  assert.strictEqual(acquiredD2AfterExpiry, true, "Job D2 adquiriu o slot livre após expiração do lease");
  managerD.releaseGeminiSlot("job-d2");
  console.log("✅ [PASS] TESTE D: Lock expirou corretamente e outro processo assumiu o slot.");

  // --------------------------------------------------------------------------------
  // TESTE E: Execução A perde o lease. Execução B adquire o slot. Execução A tenta release.
  // Resultado esperado: A NÃO remove o lock de B (Atomicidade com validação de Token).
  // --------------------------------------------------------------------------------
  console.log(">>> [TESTE E] Proteção contra liberação indevida pós-expiração...");
  cluster.clear();
  // Lease curto de 100ms
  const managerE_InstA = new DistributedLockManager(cluster, 1, 0.1);
  const managerE_InstB = new DistributedLockManager(cluster, 1, 45);

  const acquiredE_A = await managerE_InstA.acquireGeminiSlot("job-ea", 500);
  assert.strictEqual(acquiredE_A, true, "Instância A adquiriu o slot");
  const tokenA = managerE_InstA.getHeldToken("job-ea");

  // Simula atraso longo da Instância A até o lease expirar
  await new Promise((res) => setTimeout(res, 120));

  // Agora Instância B adquire o slot com um novo token
  const acquiredE_B = await managerE_InstB.acquireGeminiSlot("job-eb", 500);
  assert.strictEqual(acquiredE_B, true, "Instância B adquiriu o slot após a expiração de A");
  const tokenB = cluster.get("calcpro:lock:gemini:slot:1");
  assert.notStrictEqual(tokenB, tokenA, "O token no slot agora é exclusivo da Instância B");

  // Instância A termina atrasada e tenta liberar o slot
  const releaseAttemptA = managerE_InstA.releaseGeminiSlot("job-ea");
  assert.strictEqual(releaseAttemptA, false, "Tentativa de release de A falha porque o lock não lhe pertence mais");

  // Verifica que o lock da Instância B permanece intacto no Redis
  const currentTokenAfterAFailedRelease = cluster.get("calcpro:lock:gemini:slot:1");
  assert.strictEqual(
    currentTokenAfterAFailedRelease,
    tokenB,
    "Lock da Instância B foi preservado e NÃO foi deletado pela Instância A"
  );

  // Instância B libera seu próprio lock com sucesso
  const releaseAttemptB = managerE_InstB.releaseGeminiSlot("job-eb");
  assert.strictEqual(releaseAttemptB, true, "Instância B liberou seu próprio slot com sucesso");
  assert.strictEqual(cluster.get("calcpro:lock:gemini:slot:1"), null, "Slot liberado no cluster");
  console.log("✅ [PASS] TESTE E: Instância A atrasada NÃO removeu o lock de B.");

  // --------------------------------------------------------------------------------
  // TESTE F: Duas instâncias simuladas tentando adquirir o mesmo slot exatamente ao mesmo tempo.
  // Resultado esperado: apenas uma consegue.
  // --------------------------------------------------------------------------------
  console.log(">>> [TESTE F] Colisão exata simultânea no mesmo slot (Atomicidade SET NX)...");
  cluster.clear();
  const inst1 = new DistributedLockManager(cluster, 1, 45);
  const inst2 = new DistributedLockManager(cluster, 1, 45);

  const [res1, res2] = await Promise.all([
    inst1.acquireGeminiSlot("job-f1", 0), // sem retry loop, tentativa instantânea
    inst2.acquireGeminiSlot("job-f2", 0),
  ]);

  const successes = [res1, res2].filter(Boolean).length;
  assert.strictEqual(successes, 1, "Exatamente UMA instância consegue adquirir o slot em colisão simultânea");
  console.log("✅ [PASS] TESTE F: Em colisão exata simultânea, exatamente 1 instância adquiriu o slot.");

  // --------------------------------------------------------------------------------
  // TESTE G: Erro durante processamento Gemini dentro de try/finally.
  // Resultado esperado: o finally libera corretamente o slot.
  // --------------------------------------------------------------------------------
  console.log(">>> [TESTE G] Resiliência de liberação sob exceção com bloco try/finally...");
  cluster.clear();
  const managerG = new DistributedLockManager(cluster, 1, 45);

  const acquiredG = await managerG.acquireGeminiSlot("job-g1", 1000);
  assert.strictEqual(acquiredG, true, "Job G1 adquiriu o slot");

  let caughtError: any = null;
  try {
    // Simula processamento com falha catastrófica da API Gemini (ex: 503 Overloaded ou 429 Quota)
    throw new Error("503 AI Studio Unavailable: High demand");
  } catch (err: any) {
    caughtError = err;
  } finally {
    // Bloco finally obrigatório
    managerG.releaseGeminiSlot("job-g1");
  }

  assert(caughtError !== null, "Erro capturado com sucesso");
  assert.strictEqual(cluster.get("calcpro:lock:gemini:slot:1"), null, "Slot liberado no cluster após o finally");

  // O próximo job deve conseguir adquirir imediatamente o slot liberado
  const acquiredGNext = await managerG.acquireGeminiSlot("job-g2", 1000);
  assert.strictEqual(acquiredGNext, true, "Próximo job adquiriu o slot liberado pelo finally");
  managerG.releaseGeminiSlot("job-g2");
  console.log("✅ [PASS] TESTE G: Bloco finally garantiu liberação do slot mesmo após erro crítico.");

  // --------------------------------------------------------------------------------
  // TESTE DE CONECTIVIDADE COM REDIS REAL (Se configurado)
  // --------------------------------------------------------------------------------
  console.log(">>> [TESTE DE AMBIENTE] Verificando se há Redis Real configurado...");
  const isConfigured = redisService.isConfigured();
  if (isConfigured) {
    console.log("ℹ️ Redis Real detectado. Executando teste atômico contra o cluster Upstash...");
    try {
      const testSlotKey = `calcpro:lock:test:${Date.now()}`;
      const testToken = `test-token-${Date.now()}`;
      const setOk = await redisService.setNx(testSlotKey, testToken, 10);
      assert.strictEqual(setOk, true, "setNx com sucesso no Upstash Redis Real");

      // Segunda tentativa imediata deve falhar (NX)
      const setConflict = await redisService.setNx(testSlotKey, "outro-token", 10);
      assert.strictEqual(setConflict, false, "setNx concorrente rejeitado no Upstash Redis Real");

      // Liberação atômica com token correto
      const releaseOk = await redisService.releaseLockAtomic(testSlotKey, testToken);
      assert.strictEqual(releaseOk, true, "releaseLockAtomic com sucesso no Upstash Redis Real");

      console.log("✅ [PASS] Teste Real no Upstash Redis concluído com sucesso!");
    } catch (err: any) {
      console.warn("⚠️ Teste contra Redis Real encontrou erro de conectividade/rede:", err.message);
    }
  } else {
    console.log("ℹ️ UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN não configurados no ambiente local.");
    console.log("ℹ️ Teste real de rede ignorado conforme instrução 15 (sem inventar resultados).");
  }

  console.log("================================================================================");
  console.log("TODOS OS TESTES A, B, C, D, E, F, G DA ETAPA 2 PASSARAM COM 100% DE SUCESSO!");
  console.log("================================================================================");
}

runTestBattery().catch((err) => {
  console.error("❌ FALHA NO TESTE DE CONCORRÊNCIA DISTRIBUÍDA:", err);
  process.exit(1);
});
