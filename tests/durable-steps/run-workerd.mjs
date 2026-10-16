import { Miniflare, Log, LogLevel } from "miniflare";
import { fileURLToPath } from "node:url";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID, randomBytes } from "node:crypto";
import assert from "node:assert/strict";

const stateRoot = await mkdtemp(join(tmpdir(), "2026-10-durable-steps-workerd-"));
const entropy = JSON.stringify([...randomBytes(10)]);
function options(name) {
  return {
    log: new Log(LogLevel.WARN), modules: true,
    modulesRules: [{ type: "ESModule", include: ["**/*.js", "**/*.mjs"] }, { type: "CompiledWasm", include: ["**/*.wasm"] }],
    scriptPath: fileURLToPath(new URL("./build/shim.mjs", import.meta.url)),
    compatibilityDate: "2026-07-29",
    durableObjects: { STEPS: { className: "StepsObject", useSQLite: true }, JOURNEY: { className: "JourneyObject", useSQLite: true }, RECEIVER: { className: "ReceiverObject", useSQLite: true } },
    durableObjectsPersist: stateRoot, bindings: { STEPS_OBJECT_NAME: name, JOURNEY_OBJECT_NAME: name, JOURNEY_ENTROPY: entropy }, cf: false,
  };
}
try {
  for (const path of ["/suite", "/alarm-atomic"]) {
    const runtime = new Miniflare(options(randomUUID()));
    try {
      await runtime.ready;
      const response = await runtime.dispatchFetch(`http://steps.local${path}`);
      const body = await response.text();
      console.log(JSON.stringify({ path, status: response.status, body }));
      assert.equal(response.status, 200);
      if (path === "/suite") assert.equal(JSON.parse(body).length, 14);
      else assert.ok(Number(JSON.parse(body)) > Date.now());
    } finally { await runtime.dispose(); }
  }
  const name = randomUUID();
  let runtime = new Miniflare(options(name));
  try {
    await runtime.ready;
    const response = await runtime.dispatchFetch("http://steps.local/journey/start");
    assert.equal(response.status, 200, await response.clone().text());
    const admitted = await response.json();
    assert.equal(admitted.state.state, "running");
    const before = await runtime.dispatchFetch("http://steps.local/journey/inspect");
    assert.equal(before.status, 200, await before.clone().text());
    const [local, receiver] = await before.json();
    assert.equal(local.completed, false);
    assert.equal(local.first, 1);
    assert.equal(local.last, 0);
    assert.equal(local.steps, 2);
    assert.deepEqual(receiver, { remote: 1, deliveries: 1 });
    console.log(JSON.stringify({ journey: "before_restart", operation: admitted.id, state: admitted.state.state }));
  } finally { await runtime.dispose(); }
  const restartedAt = Date.now();
  runtime = new Miniflare(options(name));
  try {
    await runtime.ready;
    const limit = Date.now() + 10_000;
    let observed;
    do {
      // This endpoint only reads; the platform alarm is the sole drive entry.
      const response = await runtime.dispatchFetch("http://steps.local/journey/inspect");
      assert.equal(response.status, 200, await response.clone().text());
      observed = await response.json();
      if (observed[0].completed) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    } while (Date.now() < limit);
    const [local, receiver] = observed;
    assert.equal(local.completed, true, "automatic alarm did not finish after restart");
    assert.equal(local.first, 1);
    assert.equal(local.last, 1);
    assert.equal(local.steps, 0);
    assert.ok(local.alarm_runs >= 1);
    assert.ok(local.last_alarm_at >= restartedAt, "completion needs a platform alarm after runtime restart");
    assert.deepEqual(receiver, { remote: 1, deliveries: 1 });
    console.log(JSON.stringify({ journey: "restarted_platform_alarm_completed", local, receiver }));
  } finally { await runtime.dispose(); }
} finally { await rm(stateRoot, { recursive: true, force: true }); }
