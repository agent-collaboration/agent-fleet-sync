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

`engine_send` asserts that `start`, `drive_due` and `status` return Send futures from a store-generic task with higher-ranked scope bounds, then actually spawns it on Tokio and reads its result from Turso. It failed with a higher-ranked Send error before the engine declared explicit future bounds. The targeted `manual_async_fn` expectations preserve those bounds rather than accepting Clippy's suggestion to restore the failing form.

`TursoStore::open_and_migrate(path, host)` enforces foreign keys by default. Copy owners can use `TursoStore::open_with(path, TursoStoreOptions { foreign_keys: false }, host)`; the policy is applied during connection setup before the migration transaction. The native options test checks actual orphan business-row rejection by default and committed readback when enforcement is disabled. It failed with the old hardcoded policy and passed after the option was wired through. The journal migration-history table remains `_fleet_sync_migrations`.

`TursoScope::new(&mut transaction, &host)` borrows the caller's connection without owning its transaction. `migrate_journal(&mut connection)` installs the journal on that same connection; the caller supplies the surrounding migration transaction. The borrowed-scope test commits and rolls back an admitted operation with a business row through one caller-owned connection.

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

The first endpoint runs the same 15-case conformance module on real DO SQLite transactions. The second tests alarm rollback with a business write and absolute, earlier-only scheduling. The third concurrently polls actual `AlarmOwner` clones in the same object event: both deadline orders outside and inside a caller transaction, then both polling orders of an external wake and a deadline-writing transaction. All six must finish and retain the earlier stored alarm. The compiled adapter failed the early-then-late case before shared async exclusion was added, and passed afterward. No cached alarm value or nested transaction substitutes for the real storage operations.

The journey then starts the **same three-step task as native**, with its last local step due three seconds later. Before shutdown it checks that the first local effect and remote answer are saved, with two step records. It disposes the runtime and opens the same persisted object in a new runtime. Only `alarm()` drives the operation after restart; inspection only reads. A persisted alarm count and timestamp prove a platform alarm ran after the restart. The final observations require one local effect each, one remote delivery/effect and zero step records. The bounded inspection loop observes that result; it does not drive the engine or run an alarm by hand.

## Conformance oracles and limits

`crates/durable-steps/tests/conformance` is shared source compiled by the volatile and Turso test runners and the DO worker. Its controlled clock, receiver receipts and fault controls are test stand-ins, so these cases prove journal and replay rules, not receiver durability. The two process/runtime journeys supply the durable interaction proof.

| Case | Wrong behavior it rejects | Expected observation |
|---|---|---|
| Admission and mixed rollback | business or journal writes survive an error; integer digest rounds; key conflict is ignored | no rolled-back writes/deadlines; exact adjacent large inputs conflict; committed read visible |
| Stale drive | replaced token completes another drive's operation | typed StaleDriveToken; replacement can complete |
| Generic parent and retention | business and child split across commits; synchronous callback invoked on replay; history/input retained forever; compact digest ignored | rollback together, one committed business effect, callback count two including rollback, no terminal steps/input, stable repeat ID and changed-input conflict |
| Failure hook | old input decoded on version mismatch; domain, owed child and Failed row commit separately | resume hook invocation count stays zero on version mismatch; exact version failure reaches failure hook by request key; failed hook rolls everything back; successful hook commits all three |
| Delete at end | KEEP_RESULT=false leaves journal state behind | no operation or steps; owner may admit a new ID under that key |
| Bounded NotSent | never-sent decision is recomputed on replay | saved NeverSent, no replay dispatch |
| Lost answer then NotSent | an earlier possible effect is forgotten after the bound | still Running, later Answered, one receiver effect |
| Answer after NotSent | prior send failure suppresses an answer | Answered with the receiver's doubled value |
| Non-deduplicating lost answer | a possible effect is repeated blindly | named Uncertain, one dispatch |
| In-flight recovery | dropped dispatch is treated as proven unsent | no NeverSent, stable CallId, one receiver effect |
| Non-deduplicating in-flight | restart blindly repeats an unresolved dispatch | Uncertain, no second dispatch |
| Resume Finish | a superseded operation resends an unanswered call or runs later steps | supplied output, no further dispatch, hook writes and normal compact/delete retention commit together |
| Resume Continue | the hook bypasses saved history or changes normal output | saved first effect remains once, unfinished effect runs once, normal task output |
| Resume Finish rollback | hook domain writes commit separately from a failed Finish | failed commit leaves business write absent, operation Running and both steps intact; retry commits supplied output and hook write without dispatch |
| Stale resume hook | a replaced drive invokes the hook before checking its token | yielded drive, zero synchronous hook/task invocations, no step writes |

The resume Finish case failed on the unwired engine and passed after wiring. The stale-hook case forces a replacement claim after the original claim commits and before its hook transaction, using a forwarding store wrapper and controlled clock; both claims and hook transactions use the real adapter. The Finish rollback case injects an error after the callback has produced Completed but before the real transaction commits. Its readback must show both hook and journal rollback, followed by a successful retry. The earlier contract cases first passed when introduced; no pre-fix product red was observed for them. The real Turso runner did fail when the terminal-row update had an incomplete bind list, and passed after the binds were corrected. Earlier replay/clock sensitivity evidence is tracked in the implementation trace. A target-specific independent Verifier observation is separate from these author-run receipts. Hosted Cloudflare deployment is not exercised by local workerd.

## Checked SQL

The product migration and fixture migrations seed a fresh native Turso database for query descriptions. The DO driver cannot describe queries or run `sqlx::migrate!`: it executes the same SQL files, maps named private rows, validates their closed domain states, and bootstraps inside its real storage transaction with an explicit schema version. The call row's `may_have_reached` column is true for an unresolved dispatch; the private decoder checks that it agrees with the core's richer attempt state.

```sh
cargo run --locked -p sqlx-turso --example seed_native_schema -- tmp/durable-steps-schema.db crates/durable-steps/migrations crates/durable-steps/tests/conformance/migrations
SQLX_OFFLINE=false SQLX_OFFLINE_DIR="$PWD/crates/durable-steps/.sqlx" DATABASE_URL="turso://$PWD/tmp/durable-steps-schema.db" cargo check -p durable-steps --all-targets --features turso
node tests/durable-steps/check-query-metadata.mjs
```

The seed tool refuses an existing file: choose a fresh path for another generation. Remove only obsolete generated query JSON files after a query changes. The metadata check verifies source freshness; regeneration supplies database descriptions. Turso's high-level driver has no parameter metadata, so checked macros do **not** validate bind arity. Real-store execution remains necessary. Inputs and results stay typed; terminal records retain only the digest and compact result/failure, with a finish time.
