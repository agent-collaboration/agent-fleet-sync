import { Miniflare, Log, LogLevel } from "miniflare";
import { fileURLToPath } from "node:url";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import assert from "node:assert/strict";

// /suite returns each shared conformance case's name after it passes. The
// expected list comes from the module itself, so a case that is added, renamed
// or skipped on workerd fails this run instead of drifting from a fixed count.
const conformanceSource = await readFile(
  new URL("../../crates/durable-steps/tests/conformance/mod.rs", import.meta.url), "utf8",
);
// Decode the escapes a Rust string literal allows, so a label matches what the
// compiled worker returns.
const rustEscapes = { n: "\n", r: "\r", t: "\t", "\\": "\\", "0": "\0", "'": "'", '"': '"' };
function decodeRustString(literal) {
  // Rust: \x takes two hex digits up to 7F; \u{} takes one to six hex digits,
  // each optionally followed by underscores; a line continuation skips only
  // tab, LF, CR and space.
  return literal.replace(/\\(x([0-7][0-9a-fA-F])|u\{((?:[0-9a-fA-F]_*){1,6})\}|\n[\t\n\r ]*|[\s\S])/g, (escape, body, ascii, unicode) => {
    if (unicode !== undefined) return String.fromCodePoint(parseInt(unicode.replaceAll("_", ""), 16));
    if (ascii !== undefined) return String.fromCharCode(parseInt(ascii, 16));
    if (body.startsWith("\n")) return "";
    if (Object.hasOwn(rustEscapes, body)) return rustEscapes[body];
    throw new Error(`unsupported Rust escape ${escape} in a conformance label`);
  });
}
const conformanceCases = [...conformanceSource.matchAll(/passed\.push\(\s*"((?:[^"\\]|\\[\s\S])*)"\s*,?\s*\)/g)]
  .map(([, name]) => decodeRustString(name));
assert.ok(conformanceCases.length > 0, "no conformance cases found");
// /scope-migrations returns each scope-migration case after it passes; the
// expected names are the CASES list beside the shared cases, so a skipped or
// renamed case fails here rather than shrinking the run.
const scopeSource = await readFile(
  new URL("../../crates/durable-steps/tests/integration/scope_migration_cases.rs", import.meta.url), "utf8",
);
const scopeBlock = scopeSource.match(/pub\(crate\) const CASES: &\[&str\] = &\[([\s\S]*?)\];/);
assert.ok(scopeBlock, "no CASES list in scope_migration_cases.rs");
const scopeCases = [...scopeBlock[1].matchAll(/"((?:[^"\\]|\\[\s\S])*)"/g)].map(([, name]) => decodeRustString(name));
assert.ok(scopeCases.length > 0, "no scope-migration cases found");

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
  for (const path of ["/suite", "/scope-migrations", "/metadata-upgrade", "/reborrow", "/alarm-atomic", "/alarm-concurrent"]) {
    const runtime = new Miniflare(options(randomBytes(16).toString("hex")));
    try {
      await runtime.ready;
      const response = await runtime.dispatchFetch(`http://steps.local${path}`, { signal: AbortSignal.timeout(15_000) });
      const body = await response.text();
      console.log(JSON.stringify({ path, status: response.status, body }));
      assert.equal(response.status, 200);
      if (path === "/suite") assert.deepEqual(JSON.parse(body), conformanceCases);
      else if (path === "/scope-migrations") assert.deepEqual(JSON.parse(body), scopeCases);
      else if (path === "/metadata-upgrade") assert.deepEqual(JSON.parse(body), [
        "v1 history and metadata survive upgrade",
        "repeat bootstrap preserves row ids",
        "saved local effect replays once",
        "all natural keys and DO primary keys enforced",
        "DO malformed metadata ids refused",
        "DO writes use the injected clock",
      ]);
      else if (path === "/reborrow") assert.deepEqual(JSON.parse(body), [
        "SQL and journal share parent commit and rollback",
        "parent and view share the observed arbitration owner",
        "callback rollback restores absent alarm", "SQL rollback restores absent alarm",
        "callback rollback restores existing alarm", "SQL rollback restores existing alarm",
        "view then external overlapping wake", "external then view overlapping wake",
      ]);
      else if (path === "/alarm-concurrent") {
        const observations = JSON.parse(body);
        assert.deepEqual(observations.map(({ order }) => order), [
          "late_then_early", "transaction_late_then_early",
          "transactions_late_then_early",
          "early_then_late", "transaction_early_then_late",
          "transactions_early_then_late",
          "transaction_then_external", "external_then_transaction",
        ]);
        for (const { earlier, later, actual } of observations) {
          assert.ok(earlier < later);
          assert.equal(actual, earlier);
        }
      }
      else assert.ok(Number(JSON.parse(body)) > Date.now());
    } finally { await runtime.dispose(); }
  }
  const retryName = randomBytes(16).toString("hex");
  let retryRuntime = new Miniflare(options(retryName));
  let retryOperation;
  try {
    await retryRuntime.ready;
    const staged = await retryRuntime.dispatchFetch("http://steps.local/retry-stage");
    assert.equal(staged.status, 200, await staged.clone().text());
    retryOperation = await staged.json();
  } finally { await retryRuntime.dispose(); }
  retryRuntime = new Miniflare(options(retryName));
  try {
    await retryRuntime.ready;
    const finished = await retryRuntime.dispatchFetch(`http://steps.local/retry-finish/${retryOperation}`);
    const body = await finished.text();
    assert.equal(finished.status, 200, body);
    console.log(JSON.stringify({ journey: "NoEffect_actual_runtime_reopen", body }));
  } finally { await retryRuntime.dispose(); }
  const name = randomBytes(16).toString("hex");
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
