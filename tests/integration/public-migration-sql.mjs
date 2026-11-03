// Exercise the exact production SQL on workerd SQLite; the native companion
// invokes the public Rust migrators and enforces their caller-stack budget.
import { Miniflare, Log, LogLevel } from "miniflare";
import { readFile, readdir } from "node:fs/promises";
import assert from "node:assert/strict";

const repository = new URL("../../", import.meta.url);
const specifications = {
  delivery: ["fleet-sync-delivery", 3],
  lease: ["fleet-sync-lease", 4],
  session: ["fleet-sync-session", 202610180001],
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
const legacyDelivery = await readFile(new URL(
  "crates/fleet-sync-service/tests/integration/fixtures/v0_4_delivery.sql", repository,
), "utf8");
const deliveryQueries = Object.fromEntries(await Promise.all([
  "write_status_state", "write_status_event", "write_publication",
  "read_status_state", "read_status_events", "drop_status_events",
].map(async name => [name, await readFile(new URL(
  `crates/fleet-sync-delivery/sql/${name}.sql`, repository,
), "utf8")])));
const script = `
const corpora = ${JSON.stringify(corpora)};
const legacyDelivery = ${JSON.stringify(legacyDelivery)};
const queries = ${JSON.stringify(deliveryQueries)};
const owner = '019eef00-0000-7000-8000-000000000001';
const item = '01900000-0000-7000-8000-000000000091';
function seedTyped(sql) {
  for (const kind of ['machine','project']) {
    const identity = kind === 'machine' ? item : item.slice(0,-1)+'2';
    sql.exec("INSERT INTO _fleet_sync_outbox_items(item_id,live,payload_bytes,settlement_charge_bytes,capacity_slot,record_kind,consumer_sender,submitter_kind,submitter_id,last_report_seq,created_at) VALUES(?1,0,0,0,0,'recovered',?2,?3,?4,0,1717986918400)",identity,JSON.stringify({owner:kind,session:'retained/exact'}),kind,owner);
    sql.exec("INSERT INTO _fleet_sync_status_state(submitter_kind,submitter_id,record) VALUES(?1,?2,?3)",kind,owner,JSON.stringify({ownerRoute:'typed route',head:7,floor:5}));
    const event = {cursor:6,item:identity,version:2,status:{kind:'held'},cancel:null};
    sql.exec("INSERT INTO _fleet_sync_status_events(submitter_kind,submitter_id,cursor,item_id,version,record) VALUES(?1,?2,6,?3,2,?4)",kind,owner,identity,JSON.stringify(event));
    const target = {kind:'statusStream',value:{sender:{kind,value:owner},route:'typed route'}};
    sql.exec("INSERT INTO _fleet_sync_status_owed(item_id,target,version,record,submitter_kind,submitter_id) VALUES(?1,?2,2,?3,?4,?5)",identity,JSON.stringify(target),JSON.stringify(event),kind,owner);
  }
}
function statusIsolation(sql) {
  const identity = '019eef00-0000-7000-8000-000000000099';
  const event = {cursor:1,item,version:1,status:{kind:'sent'},cancel:null};
  for (const kind of ['machine','project']) {
    sql.exec(queries.write_status_state,JSON.stringify({ownerRoute:'same route',head:1,floor:0}),kind,identity,'77','77');
    sql.exec(queries.write_status_event,'1',item,'1',JSON.stringify(event),kind,identity,'77','77');
    for (const target of [{kind:'statusStream',value:{sender:{kind,value:identity},route:'same route'}},
      kind === 'machine' ? {kind:'peer',value:{peer:identity,route:'same route'}} : {kind:'project',value:{project:identity,route:'same route'}}]) {
      sql.exec(queries.write_publication,item,JSON.stringify(target),'1',JSON.stringify(event),'77','77');
    }
  }
  sql.exec(queries.drop_status_events,'1','machine',identity);
  return {
    machine:sql.exec(queries.read_status_events,'0','10','machine',identity).toArray(),
    project:sql.exec(queries.read_status_events,'0','10','project',identity).toArray(),
    state:sql.exec("SELECT submitter_kind,head,floor FROM _fleet_sync_status_state WHERE submitter_id=?1 ORDER BY submitter_kind",identity).toArray(),
    owed:sql.exec("SELECT submitter_kind,target_kind,submitter_id FROM _fleet_sync_status_owed WHERE submitter_id=?1 ORDER BY submitter_kind,target_kind",identity).toArray(),
  };
}
export class MigrationObject {
  constructor(context) { this.context = context; }
  fetch(request) {
    const [engine, layout] = new URL(request.url).pathname.slice(1).split('/');
    const corpus = corpora[engine];
    const result = this.context.storage.transactionSync(() => {
      const sql = this.context.storage.sql;
      const upgrading = layout === 'v0.4.0';
      const typed = layout === 'typed-intermediate';
      if (typed) {
        for (const migration of corpus.migrations.filter(row => row.version <= 6)) sql.exec(migration.sql);
        seedTyped(sql);
      }
      if (upgrading) {
        for (const migration of corpus.migrations.filter(row => row.version <= corpus.released)) sql.exec(migration.sql);
        if (engine === 'delivery') sql.exec(legacyDelivery);
      }
      for (const migration of corpus.migrations.filter(row => typed ? row.version > 6 : !upgrading || row.version > corpus.released)) sql.exec(migration.sql);
      const tables = sql.exec("SELECT name FROM sqlite_master WHERE type='table'").toArray();
      const scratch = tables.flatMap(({name}) => sql.exec('PRAGMA table_info('+name+')').toArray().filter(row => row.name.startsWith('_uuid_')));
      const preserved = [];
      if (engine === 'delivery' && upgrading) {
        for (const [table,key] of [['_fleet_sync_outbox_items','item_id'],['_fleet_sync_inbox_items','item_id'],['_fleet_sync_reports','item_id'],['_fleet_sync_outbox_cancel_links','cancel_item'],['_fleet_sync_inbox_cancel_links','cancel_item']]) {
          preserved.push(...sql.exec('SELECT '+key+' AS item,created_at,updated_at FROM '+table+' ORDER BY '+key).toArray().map(row => ({table,...row})));
        }
      }
      const typedRows = typed ? {
        custody:sql.exec('SELECT submitter_kind,submitter_id,consumer_sender,created_at FROM _fleet_sync_outbox_items ORDER BY submitter_kind').toArray(),
        state:sql.exec('SELECT submitter_kind,submitter_id,head,floor FROM _fleet_sync_status_state ORDER BY submitter_kind').toArray(),
        events:sql.exec('SELECT submitter_kind,cursor,version,status_kind FROM _fleet_sync_status_events ORDER BY submitter_kind').toArray(),
        owed:sql.exec('SELECT submitter_kind,target_kind,status_kind FROM _fleet_sync_status_owed ORDER BY submitter_kind').toArray(),
      } : null;
      const isolation = engine === 'delivery' ? statusIsolation(sql) : null;
      return {engine,layout,migrations:corpus.migrations.length,tables:tables.length,scratch,preserved,typedRows,isolation};
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
    for (const layout of engine === "delivery" ? ["fresh", "v0.4.0", "typed-intermediate"] : ["fresh", "v0.4.0"]) {
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
      if (engine === "delivery") {
        assert.equal(result.isolation.machine.length, 0);
        assert.equal(result.isolation.project.length, 1);
        assert.equal(result.isolation.project[0].submitter_kind, "project");
        assert.deepEqual(result.isolation.state, [
          {submitter_kind:"machine",head:1,floor:0},
          {submitter_kind:"project",head:1,floor:0},
        ]);
        assert.deepEqual(result.isolation.owed.map(row => [row.submitter_kind,row.target_kind]), [
          ["machine","peer"], ["machine","statusStream"],
          ["project","project"], ["project","statusStream"],
        ]);
      }
      if (layout === "typed-intermediate") {
        for (const [index,kind] of ["machine","project"].entries()) {
          const row = result.typedRows.custody[index];
          assert.equal(row.submitter_kind,kind);
          assert.equal(row.submitter_id,"019eef00-0000-7000-8000-000000000001");
          assert.equal(row.consumer_sender,JSON.stringify({owner:kind,session:"retained/exact"}));
          assert.equal(row.created_at,1_717_986_918_400);
          assert.deepEqual(result.typedRows.state[index],{submitter_kind:kind,submitter_id:row.submitter_id,head:7,floor:5});
          assert.deepEqual(result.typedRows.events[index],{submitter_kind:kind,cursor:6,version:2,status_kind:"held"});
          assert.deepEqual(result.typedRows.owed[index],{submitter_kind:kind,target_kind:"statusStream",status_kind:"held"});
        }
      }
      if (engine === "delivery" && layout === "v0.4.0") {
        assert.equal(result.preserved.length, 10);
        for (const row of result.preserved) {
          const expected = Number.parseInt(row.item.replaceAll("-", "").slice(0, 12), 16);
          assert.ok(Number.isSafeInteger(expected));
          assert.equal(row.created_at, expected, row.table);
          assert.equal(row.updated_at, expected, row.table);
        }
      }
      console.log(JSON.stringify({ engine, layout, migrations: result.migrations,
        preservedRows: result.preserved.length, scratchColumns: result.scratch.length }));
    }
  }
} finally {
  await runtime.dispose();
}
