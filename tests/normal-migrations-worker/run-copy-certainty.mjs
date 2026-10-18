import { Miniflare, Log, LogLevel } from "miniflare";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";

const fixture = JSON.parse(await readFile(resolve(process.argv[2]), "utf8"));
const evidence = resolve(process.argv[3]);
await mkdir(evidence, { recursive: true });
const runtime = new Miniflare({
  log: new Log(LogLevel.WARN), modules: true,
  modulesRules: [{ type: "ESModule", include: ["**/*.js", "**/*.mjs"] }, { type: "CompiledWasm", include: ["**/*.wasm"] }],
  scriptPath: resolve(import.meta.dirname, "build/shim.mjs"), compatibilityDate: "2026-07-29",
  durableObjects: { PROBE: { className: "ProbeObject", useSQLite: true } },
  durableObjectsPersist: resolve(evidence, "state"), cf: false,
});
const observations = [];
async function call(name, body) {
  const response = await runtime.dispatchFetch("http://copy.local/", {
    method: "POST", headers: { "X-Probe-Object": name }, body,
    signal: AbortSignal.timeout(15000),
  });
  const text = await response.text();
  assert.equal(response.status, 200, text);
  return JSON.parse(text);
}
async function apply(name, piece, fault) {
  // Piece JSON remains raw text: a JS parse would round its wide integers.
  return call(name, `{"action":"certaintyApply","sender":${JSON.stringify(fixture.sender)},"fault":${JSON.stringify(fault)},"piece":${piece}}`);
}
async function counts(name) {
  const result = await call(name, JSON.stringify({ action: "inspect", queries: [
    "SELECT CAST(count(*) AS TEXT) AS result FROM samples",
    "SELECT CAST(count(*) AS TEXT) AS result FROM _fleet_sync_receipts",
  ] }));
  return result.results.map(rows => Number(rows[0]));
}
try {
  await runtime.ready;
  for (const fault of ["rollbackAfterApply", "lostRollbackReply", "lostCommitReply", "unknownRollbackCode"]) {
    const name = randomUUID();
    await call(name, JSON.stringify({ action: "initialize", create: fixture.create, journal: true }));
    assert.equal((await apply(name, fixture.pieces[0], "none")).kind, "applied");
    assert.deepEqual(await counts(name), [0, 1]);
    const refused = await apply(name, fixture.pieces[1], fault);
    const committed = fault === "lostCommitReply";
    assert.equal(refused.kind, fault === "rollbackAfterApply" ? "copyUnavailable" : "outcomeUnknown");
    assert.equal(refused.value.reason.kind, fault === "rollbackAfterApply" ? "damaged" : "storeBoundary");
    assert.deepEqual(await counts(name), committed ? [1, 2] : [0, 1]);
    const repaired = await apply(name, fixture.pieces[1], "none");
    assert.equal(repaired.kind, committed ? "alreadyApplied" : "applied");
    assert.deepEqual(await counts(name), [1, 2]);
    assert.equal((await apply(name, fixture.pieces[1], "none")).kind, "alreadyApplied");
    assert.deepEqual(await counts(name), [1, 2]);
    observations.push({ fault, refused, repaired, rows: 1, receipts: 2 });
  }
  await writeFile(resolve(evidence, "results.json"), JSON.stringify(observations, null, 2));
  console.log(JSON.stringify({ passed: observations.length, failed: 0, observations }));
} finally { await runtime.dispose(); }
