import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import assert from "node:assert/strict";

const binary = resolve("target/debug/examples/native_journey");
const cuts = ["admission", "claim", "first", "intent", "receiver", "answer", "last", "completed"];
const roots = [];
const children = new Map();
function launch(mode, directory, cut = "none") {
  const child = spawn(binary, [mode, directory, cut], { stdio: ["ignore", "pipe", "pipe"] });
  const records = [];
  let stderr = "";
  let checkpointResolve;
  const checkpoint = new Promise(resolve => { checkpointResolve = resolve; });
  const lines = createInterface({ input: child.stdout });
  lines.on("line", line => {
    const record = JSON.parse(line);
    records.push(record);
    if (record.checkpoint) checkpointResolve(record);
  });
  child.stderr.on("data", data => { stderr += data; });
  const done = new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      children.delete(child);
      resolve({ code, signal, records, stderr });
    });
  });
  children.set(child, done);
  const timeout = setTimeout(() => { child.kill("SIGKILL"); }, 60_000);
  done.then(() => clearTimeout(timeout), () => clearTimeout(timeout));
  return { child, waitForCheckpoint: () => Promise.race([checkpoint, done.then(result => { throw new Error(`exited before checkpoint: ${JSON.stringify(result)}`); })]), done };
}
try {
  await Promise.all(cuts.map(async cut => {
    const directory = await mkdtemp(join(tmpdir(), `2026-10-durable-native-${cut}-`));
    roots.push(directory);
    const first = launch("start", directory, cut);
    const stopped = await first.waitForCheckpoint();
    assert.equal(stopped.checkpoint, cut);
    first.child.kill("SIGKILL");
    const killed = await first.done;
    assert.equal(killed.signal, "SIGKILL");
    if (cut === "receiver") {
      assert.deepEqual(killed.records.find(record => record.competing_drive), { competing_drive: true, completed: 0, failed: 0, retried: 0 });
      console.log(JSON.stringify({ cut, competing_drive_skipped_leased_operation: true }));
    }
    const restarted = await launch("resume", directory).done;
    assert.equal(restarted.code, 0, restarted.stderr);
    assert.deepEqual(restarted.records, [{ completed: true, first: 1, last: 1, remote: 1, deliveries: cut === "receiver" ? 2 : 1, remaining_steps: 0 }]);
    console.log(JSON.stringify({ cut, signal: killed.signal, startup_scan_and_timer: "completed", ...restarted.records[0] }));
  }));
  console.log(JSON.stringify({ native_sigkill_cuts_passed: cuts.length }));
} finally {
  const active = [...children];
  for (const [child] of active) child.kill("SIGKILL");
  await Promise.all(active.map(([, done]) => done));
  await Promise.all(roots.map(path => rm(path, { recursive: true, force: true })));
}
