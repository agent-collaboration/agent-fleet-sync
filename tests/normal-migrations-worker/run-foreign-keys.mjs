import { Miniflare, Log, LogLevel } from "miniflare";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";

const fixtures = resolve(process.argv[2]);
const evidence = resolve(process.argv[3]);
await mkdir(evidence, { recursive: true });
const runtime = new Miniflare({
  log: new Log(LogLevel.WARN), modules: true,
  modulesRules: [{ type: "ESModule", include: ["**/*.js", "**/*.mjs"] }, { type: "CompiledWasm", include: ["**/*.wasm"] }],
  scriptPath: resolve(import.meta.dirname, "build/shim.mjs"), compatibilityDate: "2026-07-29",
  durableObjects: { PROBE: { className: "ProbeObject", useSQLite: true } },
  durableObjectsPersist: resolve(evidence, "state"), cf: false,
});
const results = [];
async function call(name, body) {
  const response = await runtime.dispatchFetch("http://copy.local/", {
    method: "POST", headers: { "X-Probe-Object": name }, body: JSON.stringify(body),
    signal: AbortSignal.timeout(30000),
  });
  const text = await response.text();
  assert.equal(response.status, 200, text);
  return JSON.parse(text);
}
async function count(name, tables) {
  const reply = await call(name, { action: "inspect", queries: tables.map(table => `SELECT CAST(COUNT(*) AS TEXT) AS result FROM "${table}"`) });
  return reply.results.map(rows => Number(rows[0]));
}
async function run(name, observe) {
  try { results.push({ name, kind: "passed", observation: await observe(randomUUID()) }); }
  catch (error) { results.push({ name, kind: "failed", error: String(error) }); }
}
const violation = { kind: "integrityViolation", value: { table: "a_child", references: "z_parent" } };
try {
  await runtime.ready;
  for (const valid of [true, false]) {
    const input = JSON.parse(await readFile(resolve(fixtures, valid ? "seed-valid.json" : "seed-orphan.json"), "utf8"));
    await run(valid ? "child-page-before-parent" : "orphan-seed-rollback", async name => {
      await call(name, { action: "initialize", create: input.create, journal: true });
      const began = await call(name, { action: "seed", input: { kind: "begin", database: input.create.database, attempt: input.attempt } });
      assert.equal(began.kind, "seedStarted");
      assert.deepEqual(began.value.seed, input.done.seed);
      for (const page of input.pages) {
        assert.deepEqual(await call(name, { action: "seed", input: { kind: "page", page } }), { kind: "seedPage", value: { kind: "staged" } });
      }
      const completed = await call(name, { action: "seed", input: { kind: "complete", done: input.done } });
      if (valid) {
        assert.deepEqual(completed, { kind: "seedComplete", value: { kind: "published", value: { position: input.done.position } } });
        assert.deepEqual(await count(name, ["a_child", "z_parent"]), [1, 1]);
      } else {
        assert.deepEqual(completed, { kind: "seedComplete", value: violation });
        const inspect = await call(name, { action: "inspect", queries: ["SELECT CAST(COUNT(*) AS TEXT) AS result FROM sqlite_schema WHERE name IN ('a_child','z_parent')", "SELECT json_extract(record,'$.state') AS result FROM _fleet_sync_copy"] });
        assert.deepEqual(inspect.results, [["0"], ["seeding"]]);
      }
      return { completed };
    });
  }
  for (const action of ["NO-ACTION", "RESTRICT"]) {
    for (const split of [false, true]) {
      const input = JSON.parse(await readFile(resolve(fixtures, `apply-${action}-${split}.json`), "utf8"));
      await run(`apply-${action}-${split}`, async name => {
        await call(name, { action: "initialize", create: input.create, journal: true });
        const apply = piece => call(name, { action: "certaintyApply", sender: input.sender, fault: "none", piece });
        assert.equal((await apply(input.schema)).kind, "applied");
        for (const [index, piece] of input.insert.entries()) {
          assert.equal((await apply(piece)).kind, index + 1 === input.insert.length ? "applied" : "partStaged");
        }
        const before = await count(name, ["a_child", "z_parent", "_fleet_sync_receipts"]);
        assert.deepEqual(before, [1, 1, 1 + input.insert.length]);
        const refused = await apply(input.orphan);
        assert.deepEqual(refused, { kind: "refused", value: violation });
        assert.deepEqual(await count(name, ["a_child", "z_parent", "_fleet_sync_receipts"]), before);
        assert.equal((await apply(input.delete)).kind, "applied");
        assert.deepEqual(await count(name, ["a_child", "z_parent"]), [0, 0]);
        return { refused, afterRefusal: before };
      });
    }
  }
  const composite = JSON.parse(await readFile(resolve(fixtures, "composite-self.json"), "utf8"));
  await run("nullable-composite-and-self-reference", async name => {
    await call(name, { action: "initialize", create: composite.create, journal: true });
    const apply = piece => call(name, { action: "certaintyApply", sender: composite.sender, fault: "none", piece });
    assert.equal((await apply(composite.schema)).kind, "applied");
    assert.equal((await apply(composite.insert)).kind, "applied");
    const before = await count(name, ["a_composite", "z_composite", "self_nodes", "_fleet_sync_receipts"]);
    assert.deepEqual(before, [2, 1, 2, 2]);
    assert.deepEqual(await apply(composite.orphan), { kind: "refused", value: { kind: "integrityViolation", value: { table: "a_composite", references: "z_composite" } } });
    assert.deepEqual(await count(name, ["a_composite", "z_composite", "self_nodes", "_fleet_sync_receipts"]), before);
    return { afterRefusal: before };
  });
  const captured = JSON.parse(await readFile(resolve(fixtures, "captured.json"), "utf8"));
  await run("real-writer-captured-order", async name => {
    await call(name, { action: "initialize", create: captured.create, journal: true });
    let inserts = 0, deletes = 0;
    for (const piece of captured.pieces) {
      assert.equal((await call(name, { action: "certaintyApply", sender: captured.sender, fault: "none", piece })).kind, "applied");
      const rows = piece.transactions.filter(transaction => transaction.kind === "rows").flatMap(transaction => transaction.rows);
      if (rows.some(row => row.kind === "upsert")) {
        assert.deepEqual(rows.map(row => row.table), ["a_child", "z_parent"]);
        assert.deepEqual(await count(name, ["a_child", "z_parent"]), [1, 1]);
        inserts++;
      }
      if (rows.some(row => row.kind === "delete")) {
        assert.deepEqual(rows.map(row => row.table), ["z_parent", "a_child"]);
        assert.deepEqual(await count(name, ["a_child", "z_parent"]), [0, 0]);
        deletes++;
      }
    }
    assert.deepEqual([inserts, deletes], [1, 1]);
    return { inserts, deletes };
  });
} finally {
  await runtime.dispose();
  await writeFile(resolve(evidence, "results.json"), JSON.stringify(results, null, 2));
  console.log(JSON.stringify(results));
  if (results.some(row => row.kind !== "passed")) process.exitCode = 1;
}
