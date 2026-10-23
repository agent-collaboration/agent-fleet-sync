# W-C43: independent durable calls

A held external call no longer serializes other due operations. Engine drives poll independent operation futures, sharing eight execution-capacity slots across overlapping drives of the same Engine. Eight leaves seven peers able to progress beside one held call while bounding task contexts and dispatches. Due-order report folding, journal claims, tokens, call identities, retries, before-resume hooks and committed outcomes retain their existing owners.

TokioRunner owns up to eight pending drive passes, continues polling its host's single timer while earlier passes await calls, and drains every owned pass on shutdown or a scheduling error. Its constructor is now `TokioRunner::new(engine, host)`; there is no ignored compatibility lock argument. All repository call sites are updated. Downstream board runtime assembly and runner probes using the former third argument must update at the new pin.

Native store connections and writer control/maintenance transactions still serialize storage. Publication and file-drain owners retain their connection/control ownership. The runner and registry entry point no longer hold an exclusive engine lock across an external call.

The mailbox timer owner likewise tracks up to eight pass tasks in the service's existing JoinSet. It arms custody deadlines after storage and before awaiting publication. The same timer owner polls subsequent wakes while publication calls remain pending. Operational backoff blocks external kicks through both its deadline and its retry in flight; a successful sibling cannot cancel a pending retry. Corruption marks the owner unavailable and stops scheduling. Shutdown/health/error stop new passes and drain owned passes.

The existing hold-copy-drive and hold-registry-drive controls now acquire all eight existing capacity slots before claims. Their DriveHold type and method exist only behind durable-steps' existing test-support feature, forwarded by the service. Domain transactions remain usable while this test-only hold blocks claims. No production pause mechanism was added.

## Product proof

- Before any engine/runner production edit, the decisive production files matched local C42 byte-for-byte. Real native tests observed two stalls (initially due readers and a later committed/woken reader) and one successful duplicate-drive check: 1 passed, 2 failed, exit 101, with the intended independent-reader-stalled assertion.
- A direct one-drive/two-due-operation native regression separately checks the shared engine. The runner's later-wake regression checks wake ownership. Call two's carrier waits for call one's entry signal, so scheduling order cannot bypass the held-first-call scenario.
- The duplicate-drive case holds a real native dispatch and overlaps a second drive within the existing lease. Dispatch count stays one and the operation commits one completed outcome after release. Existing recovery/conformance suites cover expired tokens, replay and durable journals; this case does not claim that leases never expire.
- A shutdown regression polls the actual runner after its shutdown input is ready while the native call is still held. It must stay pending, then complete the durable operation after release. No sleep establishes this order.
- The mailbox regression uses real Mailbox/PublishStatus tasks, native storage, the actual mailbox owner loop and native routed-call carrier. It independently commits and wakes the second publication while the first answer is held, requires the second carrier entry first, then replies Applied to settle the two actual tasks and checks zero owed publications. It proves carrier dispatch, not downstream peer receipt. Before the mailbox loop repair: 0 passed, 1 failed, exit 101, with the intended later-wake-stalled assertion; repaired: 1 passed, 0 failed, exit 0.
- The capacity conformance case uses the existing volatile journal host. One synchronous poll fills eight held dispatches; an overlapping drive cannot acquire another independent pool. It proves the shared execution bound, not native durability.

The first serial-engine control unexpectedly passed through overlapping runner passes, exposing a proof gap. The added direct single-drive case fails that wrong build. The original receipt is retained; it is not counted as successful sensitivity evidence.

## Sensitivity and validation

The corrected serial-engine control, serial-runner control, serial-mailbox control and nine-slot control each failed for their intended assertion, exit 101; each source file was restored byte-for-byte. W-C41's independent per-row controls likewise failed at 256 row queries for a 256-row page. Final strengthened native fixtures again rejected serial engine/runner builds, and an early-return-on-shutdown build failed the shutdown assertion (each 0 passed / 1 failed, exit 101). All sources were restored exactly. The final restored runner group passed 5 tests / 0 failures, exit 0, and its all-target/all-feature Clippy check passed with warnings denied. These targeted runs are not added to the full-suite totals.

## Crash recovery under overlap

Full consumer validation exposed a stale fixture ordering assumption at the Received-acknowledgment kill. ReceiveItems admits DeliverLocally in its committed store step before awaiting the remote acknowledgment. Independent driving therefore permits the local external effect before the Received call finishes. A killed unanswered NeverRepeat local call must recover as Uncertain; the received barrier alone does not establish that its answer was checkpointed.

The existing success-recovery case keeps its strict Delivered/one-effect assertion and kills with Received still held, after observing committed Delivered. A separate permanent native case uses existing Held/Release/Arm controls to force both Received and the unanswered Local call to remain held. After the real process kill, direct journal readback verifies exactly one unanswered local call, NeverRepeat policy and Dispatching reach. Real restart returns Uncertain at both custodians and preserves one effect after receive replay (1 passed / 0 failed, exit 0, 30.62 seconds). This is an existing replay contract; no production ordering or protocol outcome changed. The original failed run and a six-hop diagnostic replay are retained separately; the failed run's exact recovered state was not retained, so it is not claimed as directly observed Uncertain.

## Durable Object scope

The Durable Object journey alarm directly calls the same Engine::drive_due, so it previously inherited its sequential dispatch and now uses the same concurrent engine implementation. Its transaction callbacks still contain storage work rather than external dispatch. The wasm build checks this binding. No Worker endpoint or alarm protocol was added. A live Durable Object held-call concurrency observation is not claimed by this native packet.

## Remaining delivery boundary

These are implementer observations. Independent Verifier observation, hosted CI for the eventual pushed batch, downstream board pin updates and acceptance remain with the Lead's delivery flow.

## Final validation

The final workspace suite passed **763 parent tests / 0 failures / 2 existing ignores**, across 111 groups. Its raw summaries contain 764 passes because the diagnostics child reports its own one-test summary; that child is counted once in the parent total. The external consumer passed **96 tests / 0 failures / 0 ignores**, across 48 groups. The first consumer run failed its Received-kill fixture; its logs were retained, the fixture boundary was made explicit as described above, and the complete consumer suite then passed. The initial consumer-only formatting mismatch was also corrected before its final full gates.

| Gate | Exact command | Exit |
|---|---|---:|
| fmt | `cargo fmt --all -- --check` | 0 |
| clippy | `cargo clippy --workspace --all-targets --all-features --locked -j4 -- -D warnings` | 0 |
| test | `cargo test --workspace --all-features --locked -j4` | 0 |
| query-metadata | `node tests/durable-steps/check-query-metadata.mjs` | 0 |
| wasm-workspace | `cargo check --target wasm32-unknown-unknown -p sqlx-durable-object -p sqlx-durable-object-core -p probe-core -p probe-worker -p runtime-primitives -p durable-steps -p fleet-sync-protocol -p fleet-sync-session -p fleet-sync-receiver -p fleet-sync-lease -p fleet-sync-delivery --locked -j4` | 0 |
| wasm-worker | `cargo check --target wasm32-unknown-unknown -p durable-steps-worker --locked -j4` | 0 |
| wasm-core | `cargo check --target wasm32-unknown-unknown -p durable-steps --no-default-features --locked -j4` | 0 |
| tree-core | `cargo tree --target wasm32-unknown-unknown -p durable-steps --no-default-features --edges normal --locked` | 0 |
| tree-delivery | `cargo tree --target wasm32-unknown-unknown -p fleet-sync-delivery --no-default-features --edges normal --locked` | 0 |
| consumer-fmt | `cargo fmt --manifest-path tests/consumer/Cargo.toml --all -- --check` | 0 |
| consumer-clippy | `cargo clippy --manifest-path tests/consumer/Cargo.toml --all-targets --all-features --locked -j4 -- -D warnings` | 0 |
| consumer-test | `cargo test --manifest-path tests/consumer/Cargo.toml --all-features --locked -j4` | 0 |
| consumer-queries | `cargo run -p fleet-sync-query-gen --features native --locked -j4 -- --manifest tests/consumer/checked-queries.json --check` | 0 |

Top-level Cargo build/test commands used the normal worktree target, locked dependencies and four build jobs, without alternate profiles or RUSTFLAGS. The absolute normal target path was passed to nested harnesses, so trybuild output stayed under that same target owner. The 55 GiB start floor and 45 GiB stop floor were checked.

Additional final checks:

- `cargo test -p fleet-sync-tokio-runner --all-features --lib object_runner_tests --locked -j4`: 5 passed / 0 failed, exit 0.
- `cargo clippy -p fleet-sync-tokio-runner --all-targets --all-features --locked -j4 -- -D warnings`: exit 0.
- `cargo check -p fleet-sync-service --no-default-features --locked -j4`: exit 0.
- `cargo build -p durable-steps --no-default-features --locked -j4 --message-format=json`: exit 0; the library artifact reported an empty feature list.
- An actual rustc metadata compilation against that exact feature-off artifact, with stdin `use durable_steps::DriveHold;`, returned the expected exit 1 / E0432. The compiler identified the missing export as gated behind test-support. The default artifact cannot name DriveHold; this is compiler evidence, not a source-string check. No temporary Rust test file or executable was created outside the normal target.
- Root and external-consumer format checks and `git diff --check`: exit 0.

Native measurement fixtures were removed after their jobs settled: 319.789 MiB logical, with a contemporaneous df available-space increase of 319.766 MiB. Needed logs/reports/receipts remain. The normal build target is retained for the paired verification run.
