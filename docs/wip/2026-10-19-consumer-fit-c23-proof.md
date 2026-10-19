# C23: status-event acknowledgement

C23 implements program design section 13a W-C25 and r4 plan row C23, from accepted C26/C26b base `9331f5ef`. A sender retires only its applied prefix, the stream persists a monotone acknowledged floor, and an above-head acknowledgement reports its head without changing state. The remote watermark does not advance a consumer's locally applied cursor. A publication retried after acknowledgement may reappear above the floor; consumers deduplicate by item and StatusVersion.

The existing status owner handles the new shared StatusAck request/answer outside mailbox-failure disposition. Cursor idempotency needs no receipt journal. The private stream JSON gains a required floor field, checked against head; the standing hard-cutover rule adds no missing-field fallback for pre-feature development records. There is no new table, cursor clamp or publication tombstone. The library README documents the consumer contract.

## Behavior and sensitivity

The [seven permanent core scenarios](../../crates/fleet-sync-delivery/tests/status_acknowledgement.rs) ran first against a deliberately non-retiring scaffold for the new API: 0 passed, 7 failed, exit 101, each for its intended behavior difference. No pre-existing runtime failure of the formerly absent API is claimed. After implementation, all seven passed, exit 0.

The required wrong build changed only the checked delete predicate to delete events above through, then regenerated the corpus. It produced 5 passes and 2 failures, exit 101: prefix retirement lost the unacknowledged third event, and the actual lost-first-hop retry encountered the wrongly retained old event below the floor. This is sensitivity after implementation. The correct predicate/corpus were restored before wider checks.

The [native WebSocket case](../../tests/consumer/tests/status_acknowledgement.rs) passed 1/0/0, exit 0: real service processes, authenticated socket, typed empty-stream answers for zero/future/maximum cursors, same-call replay after service restart, and a real StatusFetch response. It establishes carrier/status-owner routing and restart handling. Nonzero floor/event-retirement persistence is covered by core Turso tests; it is not inferred from this empty-stream case. The test was written before implementation but first run afterward, so no pre-fix red is claimed for it. Both service processes were killed and joined.

| Case | Wrong behavior caught | Oracle and real layer |
| --- | --- | --- |
| Prefix and later append | Losing the suffix or hiding new events | W-C25; literal cursors/items 1 through 4 in StatusStream/Turso transactions |
| Equal/lower replay and reopen | Lowering the floor or repeating a retirement effect | W-C25; literal floor 2 and exact suffix after reopening the store |
| Above-head acknowledgement | Clamping or deleting unread events | W-C25; diagnostic head 3 and unread events 2/3, including maximum input cursor |
| Empty stream | Accepting a future cursor or creating a head | W-C25 at floor/head zero; real initialized store |
| Lost first-hop retry after ack | Keeping the old event or regressing the sender | W-C25; real durable publication call loses its committed answer, retries, and real Outbox keeps Held at version 2 |
| Empty fetch with higher floor | Advancing through without an applied event | W-C25 remote/local contract; real fetch preserves the requested through cursor |
| Corrupt floor | Trusting floor above head | W-C25 floor invariant and strict stored-state decode; corrupt a real row, public acknowledgement returns DeliveryError::Corrupt |
| Native status owner | Mailbox routing, call-receipt replay, wrong typed answer | W-C25; authenticated WebSocket, service restart and same call with different through values |

## Final local gates

All commands ran from the repository root in the normal target, with four Cargo jobs and at least 45 GiB free before builds. The project verification skill has no status-ack journey; these are explicitly commissioned product checks, with no driver writes.

- `cargo fmt --all -- --check` and the consumer manifest's formatter check: exit 0.
- `cargo clippy -p fleet-sync-delivery -p fleet-sync-protocol -p fleet-sync-service --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0, zero Clippy warnings.
- External consumer all-target/all-feature Clippy with its manifest, normal target and four jobs: exit 0, zero warnings.
- `cargo test -p fleet-sync-delivery -p fleet-sync-protocol -p fleet-sync-service --all-features --locked -j 4`: 126 tests passed, zero failed or ignored, exit 0.
- `CARGO_TARGET_DIR=target cargo test --manifest-path tests/consumer/Cargo.toml --all-features --locked -j 4 -- --test-threads=4`: 95 tests passed, zero failed or ignored, exit 0. This includes the new socket case and the existing status-owner corruption/service-infrastructure journey.
- `cargo check --target wasm32-unknown-unknown -p fleet-sync-delivery -p fleet-sync-protocol --all-features --locked -j 4`: exit 0; compiler proof, not a wasm runtime claim.
- The granted query-generator command regenerated the delivery corpus; its same `--check` form passed, exit 0. Generated calls and metadata are committed with the query.
- After helper-name clarification, the same seven core scenario bodies passed again, exit 0, and delivery all-target/all-feature Clippy passed again, exit 0. These reruns are not additional distinct tests.
- `git diff --check`: exit 0. The production diff adds no unchecked unwrap, cast, unsafe code or SQL constraint. Existing stored-event and route decoding remains at the owned boundary.

Raw commands/output remain in ignored checkout scratch with the `c23-` prefix. CI and independent observation are declared separately by immutable commit to the Lead and paired Verifier. These local gates do not establish board/central integration, hosted or Durable Object runtime behavior, whole-PR readiness or acceptance. No stand-ins remain. C18a/C18b follow; C24 waits for the Lead's later neutral brief.
