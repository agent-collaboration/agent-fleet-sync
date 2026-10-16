import { Miniflare, Log, LogLevel } from "miniflare";
import { fileURLToPath } from "node:url";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";

const stateRoot = await mkdtemp(join(tmpdir(), "2026-10-durable-steps-workerd-"));
function options(name) {
  return {
    log: new Log(LogLevel.INFO), modules: true,
    modulesRules: [{ type: "ESModule", include: ["**/*.js", "**/*.mjs"] }, { type: "CompiledWasm", include: ["**/*.wasm"] }],
    scriptPath: fileURLToPath(new URL("./build/shim.mjs", import.meta.url)),
    compatibilityDate: "2026-07-29",
    durableObjects: { STEPS: { className: "StepsObject", useSQLite: true } },
    durableObjectsPersist: stateRoot, bindings: { STEPS_OBJECT_NAME: name }, cf: false,
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
      if (path === "/suite") assert.equal(JSON.parse(body).length, 11);
      else assert.ok(Number(JSON.parse(body)) > Date.now());
    } finally { await runtime.dispose(); }
  }
} finally { await rm(stateRoot, { recursive: true, force: true }); }
