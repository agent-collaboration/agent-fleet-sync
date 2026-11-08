// Exercise the exact production SQL on workerd SQLite; the native companion
// invokes the public Rust migrators and enforces their caller-stack budget.
import { Miniflare, Log, LogLevel } from "miniflare";
import { readFile, readdir } from "node:fs/promises";
import assert from "node:assert/strict";

const repository = new URL("../../", import.meta.url);
const specifications = {
  journal: ["durable-steps", 1],
};
const corpora = {};
for (const [engine, [crate, released]] of Object.entries(specifications)) {
  const directory = new URL(`crates/${crate}/migrations/`, repository);
  const files = (await readdir(directory)).filter(name => name.endsWith(".sql")).sort();
  assert.ok(files.length > 0, engine);
  corpora[engine] = { released, migrations: await Promise.all(files.map(async name => ({
    version: Number(name.split("_")[0]),
    sql: await readFile(new URL(name, directory), "utf8"),
  }))) };
}
const script = `
const corpora = ${JSON.stringify(corpora)};
export class MigrationObject {
  constructor(context) { this.context = context; }
  fetch(request) {
    const [engine, layout] = new URL(request.url).pathname.slice(1).split('/');
    const corpus = corpora[engine];
    const result = this.context.storage.transactionSync(() => {
      const sql = this.context.storage.sql;
      const upgrading = layout === 'v0.4.0';
      if (upgrading) {
        for (const migration of corpus.migrations.filter(row => row.version <= corpus.released)) sql.exec(migration.sql);
      }
      for (const migration of corpus.migrations.filter(row => !upgrading || row.version > corpus.released)) sql.exec(migration.sql);
      const tables = sql.exec("SELECT name FROM sqlite_master WHERE type='table'").toArray();
      const scratch = tables.flatMap(({name}) => sql.exec('PRAGMA table_info('+name+')').toArray().filter(row => row.name.startsWith('_uuid_')));
      return {engine,layout,migrations:corpus.migrations.length,tables:tables.length,scratch};
    });
    return Response.json(result);
  }
}
export default { fetch(request, env) {
  return env.MIGRATIONS.get(env.MIGRATIONS.idFromName(new URL(request.url).pathname)).fetch(request);
}};
`;
const runtime = new Miniflare({
  log: new Log(LogLevel.WARN), modules: true, script,
  compatibilityDate: "2026-07-29",
  durableObjects: { MIGRATIONS: { className: "MigrationObject", useSQLite: true } },
  cf: false,
});
try {
  await runtime.ready;
  for (const engine of Object.keys(specifications)) {
    for (const layout of ["fresh", "v0.4.0"]) {
      const response = await runtime.dispatchFetch(`http://migrations.local/${engine}/${layout}`, {
        signal: AbortSignal.timeout(15_000),
      });
      const body = await response.text();
      assert.equal(response.status, 200, body);
      const result = JSON.parse(body);
      assert.equal(result.engine, engine);
      assert.equal(result.layout, layout);
      assert.equal(result.migrations, corpora[engine].migrations.length);
      assert.ok(result.tables > 0);
      assert.deepEqual(result.scratch, []);
      console.log(JSON.stringify({ engine, layout, migrations: result.migrations,
        scratchColumns: result.scratch.length }));
    }
  }
} finally {
  await runtime.dispose();
}
