import { Miniflare, Log, LogLevel } from "miniflare";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
const fixtureRoot = fileURLToPath(new URL(".", import.meta.url));
const stateRoot = await mkdtemp(join(tmpdir(), "2026-10-fleet-sync-workerd-"));
const objectName = randomUUID();
const options = {
  log: new Log(LogLevel.INFO),
  modules: true,
  modulesRules: [{ type: "ESModule", include: ["**/*.js", "**/*.mjs"] }, { type: "CompiledWasm", include: ["**/*.wasm"] }],
  scriptPath: resolve(fixtureRoot, "build/shim.mjs"),
  compatibilityDate: "2026-07-29",
  durableObjects: { PROBE: { className: "ProbeObject", useSQLite: true } },
  durableObjectsPersist: stateRoot,
  bindings: { PROBE_OBJECT_NAME: objectName },
  cf: false,
};
async function request(runtime,path,expectedStatus=200) {
  const response = await runtime.dispatchFetch("http://probe.local"+path);
  const body = await response.text();
  console.log(JSON.stringify({path,status: response.status,body}));
  if (response.status !== expectedStatus) throw new Error("request failed");
  return body;
}
let runtime = new Miniflare(options);
let before;
try {
  await runtime.ready;
  const suite = await request(runtime,"/suite");
  if (!suite.includes("11 exact values/classes passed")) throw new Error("scenario failed");
  const effect = await request(runtime,"/effect");
  if (effect !== "effect observed after durable commit: http-committed") throw new Error("effect order failed");
  const rejected = await request(runtime,"/effect",503);
  if (rejected !== "storage failed") throw new Error("shared error policy failed");
  before = await request(runtime,"/inspect");
} finally { await runtime.dispose(); }
runtime = new Miniflare(options);
try {
  await runtime.ready;
  const after = await request(runtime,"/inspect");
  if (before !== after) throw new Error("state changed across workerd restart");
  console.log("workerd process restart retained exact committed rows; object="+objectName);
} finally { await runtime.dispose(); }

await rm(stateRoot, { recursive: true, force: true });
