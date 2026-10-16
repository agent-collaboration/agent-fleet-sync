# Durable steps product proof

Run these commands from the workspace root. Native builds on macOS need a compiler with the SDK headers (Xcode clang), rather than an LLVM installation without a configured SDK.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
node tests/durable-steps/check-query-metadata.mjs
cargo check -p durable-steps --no-default-features --target wasm32-unknown-unknown --locked
cargo tree -p durable-steps --no-default-features --target wasm32-unknown-unknown --edges normal
```

The last tree must contain no Tokio. The `turso` adapter deliberately enables Tokio; the default execution core does not.

## Native process recovery

```sh
cargo build -p durable-steps --features turso --example native_journey --locked
node tests/durable-steps/run-native.mjs
```

The controller creates eight isolated control/receiver database pairs. It waits for a named committed checkpoint, sends the child SIGKILL, and restarts it. Cuts are admission, claim, first local output, call intent, receiver receipt, call answer, last local output and terminal compaction. The restarted process uses its start-up scan and earliest-deadline timer, with no repeated `start` call. A competing drive in the same owning process skips the leased operation while the receiver call is held open. Turso owns a file in one process; another process cannot open the live file to act as a competing drive.

Every cut must finish with `first=1`, `last=1`, `remote=1` and no remaining steps. The answer-before-save cut makes two deliveries under the same CallId and one receiver effect. All other cuts make one delivery. In particular, a saved call answer must never be sent again. A failure of local replay, call replay, receiver deduplication, token ownership, start-up scanning or timer re-arming can fail this journey. The expected counts come from the task's two local effects and one receipted remote effect, not from engine output.

## Durable Object recovery

Install the pinned Node dependencies and **wasm-bindgen CLI 0.2.129**, then:

```sh
npx pnpm@12.11.2 install --frozen-lockfile
cargo build -p durable-steps-worker --target wasm32-unknown-unknown --locked
wasm-bindgen --target bundler --no-typescript --out-dir tests/durable-steps/build --out-name index target/wasm32-unknown-unknown/debug/durable_steps_worker.wasm
cp tests/durable-steps/shim.mjs tests/durable-steps/build/shim.mjs
node tests/durable-steps/run-workerd.mjs
```

The first endpoint runs the same 11-case conformance module on real DO SQLite transactions. The second tests alarm rollback with a business write and absolute, earlier-only scheduling. The journey then starts the **same three-step task as native**, with its last local step due three seconds later. Before shutdown it checks that the first local effect and remote answer are saved, with two step records. It disposes the runtime and opens the same persisted object in a new runtime. Only `alarm()` drives the operation after restart; inspection only reads. A persisted alarm count and timestamp prove a platform alarm ran after the restart. The final observations require one local effect each, one remote delivery/effect and zero step records. The bounded inspection loop observes that result; it does not drive the engine or run an alarm by hand.

## Conformance oracles and limits

`crates/durable-steps/tests/conformance` is shared source compiled by the volatile and Turso test runners and the DO worker. Its controlled clock, receiver receipts and fault controls are test stand-ins, so these cases prove journal and replay rules, not receiver durability. The two process/runtime journeys supply the durable interaction proof.

| Case | Wrong behavior it rejects | Expected observation |
|---|---|---|
| Admission and mixed rollback | business or journal writes survive an error; integer digest rounds; key conflict is ignored | no rolled-back writes/deadlines; exact adjacent large inputs conflict; committed read visible |
| Stale drive | replaced token completes another drive's operation | typed StaleDriveToken; replacement can complete |
| Generic parent and retention | business and child split across commits; synchronous callback invoked on replay; history/input retained forever; compact digest ignored | rollback together, one committed business effect, callback count two including rollback, no terminal steps/input, stable repeat ID and changed-input conflict |
| Failure hook | old input decoded on version mismatch; domain, owed child and Failed row commit separately | exact version failure reaches hook by request key; failed hook rolls everything back; successful hook commits all three |
| Delete at end | KEEP_RESULT=false leaves journal state behind | no operation or steps; owner may admit a new ID under that key |
| Bounded NotSent | never-sent decision is recomputed on replay | saved NeverSent, no replay dispatch |
| Lost answer then NotSent | an earlier possible effect is forgotten after the bound | still Running, later Answered, one receiver effect |
| Answer after NotSent | prior send failure suppresses an answer | Answered with the receiver's doubled value |
| Non-deduplicating lost answer | a possible effect is repeated blindly | named Uncertain, one dispatch |
| In-flight recovery | dropped dispatch is treated as proven unsent | no NeverSent, stable CallId, one receiver effect |
| Non-deduplicating in-flight | restart blindly repeats an unresolved dispatch | Uncertain, no second dispatch |

The new contract cases first passed when introduced; no pre-fix product red was observed for them. The real Turso runner did fail when the terminal-row update had an incomplete bind list, and passed after the binds were corrected. Earlier replay/clock sensitivity evidence is tracked in the implementation trace. A target-specific independent Verifier observation is separate from these author-run receipts. Hosted Cloudflare deployment is not exercised by local workerd.

## Checked SQL

The product migration and fixture migrations seed a fresh native Turso database for query descriptions. The DO driver cannot describe queries or run `sqlx::migrate!`: it executes the same SQL files, maps named private rows, validates their closed domain states, and bootstraps inside its real storage transaction with an explicit schema version. The call row's `may_have_reached` column is true for an unresolved dispatch; the private decoder checks that it agrees with the core's richer attempt state.

```sh
cargo run --locked -p sqlx-turso --example seed_native_schema -- tmp/durable-steps-schema.db crates/durable-steps/migrations crates/durable-steps/tests/conformance/migrations
SQLX_OFFLINE=false SQLX_OFFLINE_DIR="$PWD/crates/durable-steps/.sqlx" DATABASE_URL="turso://$PWD/tmp/durable-steps-schema.db" cargo check -p durable-steps --all-targets --features turso
node tests/durable-steps/check-query-metadata.mjs
```

The seed tool refuses an existing file: choose a fresh path for another generation. Remove only obsolete generated query JSON files after a query changes. The metadata check verifies source freshness; regeneration supplies database descriptions. Turso's high-level driver has no parameter metadata, so checked macros do **not** validate bind arity. Real-store execution remains necessary. Inputs and results stay typed; terminal records retain only the digest and compact result/failure, with a finish time.
