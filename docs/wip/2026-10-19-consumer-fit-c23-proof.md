# C23: status-event acknowledgement

C23 implements program design section 13a W-C25 and r4 plan row C23, from accepted C26/C26b base `9331f5ef`. A sender retires only its applied prefix, the stream persists a monotone acknowledged floor, and an above-head acknowledgement reports its head without changing state. The remote watermark does not advance a consumer's locally applied cursor. A publication retried after acknowledgement may reappear above the floor; consumers deduplicate by item and StatusVersion.

The existing status owner handles the new shared StatusAck request/answer outside mailbox-failure disposition. Cursor idempotency needs no receipt journal. The private stream JSON gains a required floor field, checked against head. There is no new table, cursor clamp or publication tombstone. The library README documents the consumer contract.

The stored-state cutover has an explicit basis: governing program design §13a SO51, the engine-key NOT NULL decision, records that neither consumer has adopted the library and that no deployed database exists; initial migrations are amended in place, with rebuild migrations required if any deployed store exists. On 2026-10-19 the Lead rechecked consumer adoption and confirmed that basis still holds: AgentStudio has no library dependency and the consuming Router board stack remains unmerged. Applying that recorded decision to C23, the Lead explicitly selected refusal of pre-feature development stream records, with no forward migration or inspection-corpus regeneration. The required floor therefore has no missing-field default: an old record is corrupt. The adoption recheck is Lead-reported evidence, not an independent deployment inventory by this implementer. Discovery of any deployed store reopens the engine migration decision, including this field.

## Behavior and sensitivity

The [seven permanent core scenarios](../../crates/fleet-sync-delivery/tests/status_acknowledgement.rs) ran first against a deliberately non-retiring scaffold for the new API: 0 passed, 7 failed, exit 101, each for its intended behavior difference. No pre-existing runtime failure of the formerly absent API is claimed. After implementation, all seven passed, exit 0.

The required wrong build changed only the checked delete predicate to delete events above through, then regenerated the corpus. The initial local run, before the final literal unread-suffix assertion, produced 5 passes and 2 failures, exit 101: prefix retirement lost the unacknowledged third event, and the actual lost-first-hop retry encountered the wrongly retained old event below the floor. At immutable `e1146f9632211841f742512a9306fdf5a6b7a4e1`, the independent Verifier repeated this control and observed 4 passes and 3 failures, exit 101: the final above-head scenario also catches the missing literal suffix. Both raw logs were inspected. These are separate sensitivity runs, not matching-count requirements. The correct predicate/corpus were restored before wider checks.

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

Raw commands/output remain in ignored checkout scratch with the `c23-` prefix. CI and independent observation are declared separately by immutable commit to the Lead and paired Verifier. These local gates do not establish a StatusAck-specific injected service-storage failure, board/central integration, hosted or Durable Object runtime behavior, whole-PR readiness or acceptance. No stand-ins remain. C27 precedes C18a/C18b; C24 waits for the Lead's later neutral brief.

CI run [39206310953](https://github.com/agent-collaboration/agent-fleet-sync/actions/runs/39206310953) completed successfully for `e1146f9632211841f742512a9306fdf5a6b7a4e1`: fmt, Clippy, workspace tests, wasm and the external consumer job all passed. The blocking watch exited 0, and a fresh PR read confirmed that exact head while the PR remained draft.

Independent local observation at that same immutable target completed with seven core cases, 126 touched native tests and 95 external consumer tests passing, zero failures or ignored tests, and exit 0 for each command. The query-generator check passed after exact restoration of all three control files. The source check compared 1056 tracked blobs with zero mismatches. The independent CI watch also exited 0. Raw test counts match the parent counts; the focused seven and carrier case are included in the wider totals, not additional distinct tests. All observation jobs and children settled before the Verifier returned the build/runtime slot. The Lead accepted C23 at that target on 2026-10-19, including the recorded absence of a StatusAck-specific service storage/corruption injection case. This does not establish whole-PR readiness.
