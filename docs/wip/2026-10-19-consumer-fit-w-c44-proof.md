# W-C44 operation-scoped registry settlement

Based on `30ee6b53`. Decision 555 and its amendments keep service requests synchronous while allowing unrelated durable operations to progress. These are local implementation observations; independent verification and hosted CI on this revision remain separate.

## Behavior

`Engine::start_and_settle` shares admission with `start`, then drives or joins only its own operation. Existing `start` still drives all due work. A runtime-independent futures mutex belongs to each active operation; its guard releases on completion or cancellation. The shared weak-entry map is locked only for lookup and pruning, without storage or external calls under that map lock. Durable journal claims and tokens remain authoritative.

Completed results, scheduled backoff and unexpired predecessor leases need no execution capacity. The latter two remain Running/Pending when no drive on this Engine is progressing them. There is no core timer or Tokio dependency. Tasks must retain their terminal journal result to use this API.

`SettleError` distinguishes `Admission(StartRefusal)` from `Execution(StoreError)`. The service maps those to Rule and Storage respectively, while terminal Failed maps to Operation and Running maps to Pending. The shared registry helper covers registration, import, retirement and takeover preview. No other service request start/drive/status site remained.

The native scheduling probe adds Create to its existing test-fault-only copy-call kinds. It holds actual CopyCreate before SQL; storage and its result are real. Import helper request errors identify the initial import, fresh matching/conflicting import, original-attempt replay, and before/after restart phase.

## Proof

| Case | Boundary and oracle | Wrong behavior rejected / observed red |
| --- | --- | --- |
| Runner-owned import | Real FleetService, native committed admission, actual held CopyCreate and durable completion; decision 555 requires Imported after release | Before settlement: exact Pending, 0 passed / 1 failed, exit 101. Final wait-removal control repeats that error, exit 101. |
| Execution error mapping | Corrupt the actual committed native input digest while registry capacity is held; adopted amendment requires Storage(Corrupt input_digest) | Before typed-error correction: Rule, 0/1, exit 101. Corrected native suite: 2/0, exit 0. |
| Admission refusal identity | Core conformance store, conflicting inputs under one request key, original operation id | Admission(RequestConflict) retains original id. No pre-fix red observed for this additional case. |
| Join and cancelled waiter | Core conformance store and gated host; two settlers join one call, one waiter is cancelled | Exactly one dispatch and effect. Removing settlement waits fails the pending assertion, exit 101. |
| Unrelated due operation | Core conformance store; independent decision input 23 must complete while unrelated call remains due | Request returns 23 without dispatching that call. No pre-fix red observed for this additional case. |
| Saturated capacity | Core conformance store; eight held calls coexist with an already-completed result and scheduled backoff | Initial implementation incorrectly waited for capacity; 1 failed, exit 101. Corrected result and backoff both return without capacity. |
| Backoff and terminal failure | Controlled core task returns Retry or Refused; decision 555 preserves these states | Running with scheduled backoff and terminal Failed remain distinct. No pre-fix red observed for this additional case. |
| Cancelled drive and stale lease | Core conformance store with controlled clock and held dispatch | Cancelling the drive releases its waiter; unexpired lease returns Running without another dispatch; after expiry it can recover. No pre-fix red observed for this additional case. |
| Generic Send future | Existing native generic Turso engine test now also spawns start_and_settle | Compiler checks the declared Send contract and real task returns 43. No pre-fix red observed for this extension. |

The existing native engine/runner/mailbox independence and duplicate-dispatch cases pass in the full workspace suite. The lifecycle suite passes through real process restart. Core conformance observations above do not establish native durability by themselves.

Final results: workspace **769 parent passed / 0 failed / 2 existing ignored**. The raw log contains 770 passes because it separately reports the nested checked-query diagnostics child. Consumer **98 passed / 0 failed / 0 ignored**. Standalone default-core settlement **6/0**, already included in workspace coverage. All 14 gates below exit 0. Wrong-build source was restored byte-exact before final validation.

## Commands

Each Cargo command used the worktree target and four build jobs. The two dependency trees were additionally checked for absence of Tokio.

| Gate | Command |
| --- | --- |
| default-core-settlement | `cargo test -p durable-steps --no-default-features --locked --test operation_settlement` |
| workspace-fmt | `cargo fmt --all -- --check` |
| workspace-clippy | `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` |
| workspace-tests | `cargo test --workspace --all-features --locked` |
| query-metadata | `node tests/durable-steps/check-query-metadata.mjs` |
| wasm-packages | `cargo check --target wasm32-unknown-unknown -p sqlx-durable-object -p sqlx-durable-object-core -p probe-core -p probe-worker -p runtime-primitives -p durable-steps -p fleet-sync-protocol -p fleet-sync-session -p fleet-sync-receiver -p fleet-sync-lease -p fleet-sync-delivery --locked` |
| wasm-worker | `cargo check --target wasm32-unknown-unknown -p durable-steps-worker --locked` |
| wasm-core | `cargo check --target wasm32-unknown-unknown -p durable-steps --no-default-features --locked` |
| no-tokio-core | `cargo tree --target wasm32-unknown-unknown -p durable-steps --no-default-features --edges normal` |
| no-tokio-delivery | `cargo tree --target wasm32-unknown-unknown -p fleet-sync-delivery --no-default-features --edges normal` |
| consumer-fmt | `cargo fmt --manifest-path tests/consumer/Cargo.toml --all -- --check` |
| consumer-clippy | `cargo clippy --manifest-path tests/consumer/Cargo.toml --all-targets --all-features --locked -- -D warnings` |
| consumer-tests | `cargo test --manifest-path tests/consumer/Cargo.toml --all-features --locked` |
| consumer-query-freshness | `cargo run -p fleet-sync-query-gen --features native --locked -- --manifest tests/consumer/checked-queries.json --check` |

## Limits

This packet proves local native behavior and wasm32 compilation. It does not prove live Durable Object settlement, Linux hosted CI on this revision, or delivery acceptance. The old hosted failure did not retain per-import context, so its exact import invocation remains unknown; the deterministic reproduction establishes the matching Pending mechanism. No wire format, migration, dependency, lockfile, verification driver or governing design was changed.
