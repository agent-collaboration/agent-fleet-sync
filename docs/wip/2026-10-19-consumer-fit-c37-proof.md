# C37: delivery projection hooks

The delivery inbox now calls consumer projection hooks through the same borrowed transaction scope as its custody writes. This is local native Turso evidence; independent Verifier observation and external application integration remain unverified.

## Target and adopted contract

- Branch: `feat/consumer-fit-c37`, based on `fc46750cdb85999d4a44f562cb5a3f64bb66b54c`.
- Governing design: W-C38, decisions 447 and 448, design revision `4dbfd2a3`. Decision 447 uses the existing `RecipientOutcome` and `ItemId`; decision 448 adds a bounded local observation.
- Build: assigned worktree's normal `target/debug`, repository Rust toolchain and lockfile, real Turso 0.8.1, `SQLX_OFFLINE=true` from repository configuration. Entry disk availability: 109 GiB; subsequent check: 112 GiB, above the commissioned 45 GiB stop threshold.
- No peer wire format, custody/report storage format, or migration changes. The local deliver-call journal answer changes; DeliverLocally advances to task version 2. In-tree consumers adopt unit observations while preserving their existing outcomes.

`DeliveryConsumer::on_received(scope, item)` runs only for a newly stored incoming `DeliveryItem`, after its custody row is written. Controls use their existing cancellation path. Local enqueue and local-to-relayed promotion do not call `on_received` again.

`DeliveryConsumer::on_outcome(scope, item_id, outcome, observation)` runs once in `record_header_outcome`, before its local-route return. Its borrowed `RecipientOutcome` retains every existing variant and the consumer's exact acceptance value. The existing consumer is threaded through receive-time recording, attempt completion, terminal failure, and cancellation recording for both the original and its control. A malformed stored digest adds no header validation to terminal failure.

`DeliveryTypes::LocalObservation` belongs to the consumer. `MAX_LOCAL_OBSERVATION_BYTES` bounds its canonical JSON; the default four bytes admit unit observations. A consumer returns `LocalDelivery { outcome, observation }`. The local adapter validates both acceptance and observation bounds, then journals `LocalDeliveryAnswer::Observed { outcome, observation }`; an invalid receipt journals `Uncertain` with no observation. This enum prevents arbitrary non-uncertain outcomes without their observation. No competing answer byte bound was found in durable-steps' CallDone serializer.

Only an outcome recorded from a validated local answer receives `Some(&observation)`. Receive-time recording, check-time expiry, terminal failure and cancellation receive `None`. The observation is present in the local call checkpoint and application projection only; recipient reports, inbox custody records and peer publications retain their existing shapes.

Both hooks default to successful no-ops. Errors propagate to the existing transaction owner, which rolls back custody and application writes together. Consumers must keep projection writes on the supplied scope and propagate library errors through that transaction, as the tests do.

## Permanent behavioral proof

The calling programs are [projection_hooks.rs](../../crates/fleet-sync-delivery/tests/projection_hooks.rs) and [projection_recovery.rs](../../crates/fleet-sync-delivery/tests/projection_recovery.rs). Their [fixture](../../crates/fleet-sync-delivery/tests/support/projection.rs) writes an application-owned projection table through the received scope. Independent SQL reads compare exact committed custody, payload, report, and projection contents. Public Inbox operations and the real DeliverLocally task/Engine execute the library paths; the existing test network supplies external delivery answers. It does not replace custody, the journal, transactions, hooks, or projection persistence.

All expectations below come from corrected W-C38 and literal fixture values. Each row is supported on this source target.

| Permanent test | Wrong behavior rejected and observed result |
|---|---|
| `received_projection_commits_once_and_replay_writes_nothing` | A new item commits one custody row and one exact body projection. Replaying the batch and receiving the same ID at a later sequence leave custody/reports/projections unchanged. Rejects a hook called on replay or duplicates. |
| `received_hook_error_and_later_abort_roll_back_custody_and_projection` | A hook writes and then refuses; both sides remain absent. A separate callback abort after successful library work also leaves both absent. A subsequent successful retry creates exactly one of each. Rejects partial commits and ignored hook errors. |
| `local_attempt_projects_every_outcome_and_exact_acceptance` | Real local-delivery tasks preserve delivered acceptance, uncertain, dropped, and refused outcomes. Exact projection JSON equals each literal oracle and stored custody outcome; local body compaction commits with the projection. Rejects omitted local outcomes or changed acceptance. |
| `held_then_released_projects_both_attempts_and_stale_failure_is_inert` | Held records one projection; release and the next attempt add the delivered projection. Old-attempt and repeated terminal failure calls change neither side. Rejects skipped Held and duplicate terminal recording. |
| `expired_attempt_projects_without_dispatching` | Expiry commits one expired projection and dispatches no local call. Rejects omission of the pre-dispatch expiry path. |
| `malformed_digest_failure_projects_and_hook_error_rolls_back_both` | Both a refusing outcome hook and an abort after successful recording preserve the exact malformed-digest custody and empty projection. Successful retry commits the refused/custodyCorrupt outcome and projection while preserving that digest. Rejects added header validation, ignored hook errors, and non-atomic writes. |
| `receive_time_refusal_projects_once_when_local_custody_becomes_relayed` | An already failed local item gains its relayed report and one identical refused projection when received. Receive replay and repeated failure do not add another. Rejects omission of receive-time outcome recording. |
| `cancellation_projects_original_and_control_in_one_transaction` | A refusing hook rolls back the incoming cancellation, original custody change, reports and projection. Retry records two prevented outcomes, one for the original and one for the control; replay changes nothing. Rejects either missing cancellation recording path. |
| `control_outcomes_preserve_too_late_refusal_and_expiry` | Cancellation controls record TooLate, sender-mismatch refusal, and expiry once with exact values, preserving the original delivered outcome. The control report matches its projection. Rejects flattened or omitted control variants. |
| `abort_after_saved_answer_recovers_custody_and_projection_together` | A bounded Notify barrier holds the real task after durable CallDone and before outcome. The task is aborted and joined. Readback shows saved exact acceptance, Attempting custody with its body, and no outcome projection. A new Engine recovers the saved answer: final custody, body compaction and one delivered projection commit together; total external calls remain one. Rejects premature projection, missing recovery projection, or a repeated external effect. |
| `task_failure_projects_malformed_digest_without_header_validation` | A real task fails its delivery check on a malformed header digest. Its on_failed transaction commits the matching custodyCorrupt failure and projection. Rejects omission of the task-failure consumer or new header validation. |

The first eight tests were run with default hook methods present and no production calls wired: 0 passed, 8 failed, exit 101, because projections were missing or hook errors had no effect. Initial test import/error-conversion setup failures were corrected before counting that behavioral red. The later control and recovery cases were added after the first green; no pre-wiring red is claimed for those three tests.

The test fixture's generic SQL writer explicitly returns a `Send` future with one scope lifetime. Its earlier `async fn` form caused Rust's “implementation is not general enough” error under the transaction callback. The correction stays in fixture code; production hooks retain the repository's ordinary future signatures.

Decision 448 has six additional permanent tests in [local_observations.rs](../../crates/fleet-sync-delivery/tests/local_observations.rs), with a [real host/consumer fixture](../../crates/fleet-sync-delivery/tests/support/local_observation.rs). The host resolves real inbox custody, invokes the consumer and calls the production validator. Expected values come from decision 448 and literal receipt contents. No pre-change behavioral red is claimed for these tests; fresh wrong builds below establish sensitivity.

| Test | Wrong behavior rejected and observed result |
|---|---|
| `held_and_refused_attempts_project_distinct_local_observations` | Two relayed attempts retain their distinct consumer receipts in the projection. Both real reports and custody rows exclude the observation and its contents. Rejects missing observations and observation leakage. |
| `saved_answer_replays_the_same_observation_after_task_abort` | A bounded barrier holds the real task before its outcome step after CallDone. Aborting/joining leaves no projection. Typed checkpoint readback contains the exact held receipt; a new Engine projects it once without a second consumer call. Rejects lost or rebuilt crash receipts. |
| `oversized_observation_is_uncertain_without_projection_data_or_resend` | An oversize held receipt becomes Uncertain with no observation, and the consumer is called once. Rejects unchecked local receipt size. |
| `expiry_terminal_failure_and_cancellation_record_no_local_observation` | Check-time expiry, failure, receive-time promotion, and both cancellation records produce five projections with absent observations and no consumer call. Rejects observations attached to unrelated recording paths. |
| `observation_hook_error_rolls_back_projection_and_outcome_together` | A hook writes the observation and refuses. The Engine reports a retry; custody remains Attempting and projection is absent. Retrying from the saved call commits the exact refusal receipt with one total consumer call. Rejects partial projection/custody commits or lost retry observations. |
| `observation_bound_counts_canonical_json_bytes_including_quotes` | Direct validator calls admit a 96-byte JSON string and refuse a 97-byte string. Rejects off-by-one or string-length bounds. This boundary-only case needs no database; the oversize task test above keeps the journal and custody real. |

One added test initially expected the Engine to return an error for a hook storage failure. Source at `operation_engine.rs` shows that storage failures roll back and become Running/retried operations; the test was corrected to that existing behavior. No product behavior or proof gate was changed to satisfy that incorrect expectation.

## Deliberate wrong builds

All eight mutations were rerun on the decision-448 code. Each ran alone against a permanent exact-selected test, failed at runtime with 0 passed / 1 failed, and exited 101. No compiler failure counts as a caught mutation. All five mutated production files were restored byte for byte before the final crate run.

| Mutation | Deciding observation |
|---|---|
| Move `on_received` out of the new-item branch | Replay/duplicate test reads three received projections instead of one. |
| Skip `RecipientOutcome::Held` | Held/release test reads no held projection. |
| Replace the projected outcome with Uncertain | Delivered test reads Uncertain instead of exact acceptance. |
| Ignore `on_outcome` errors | Malformed-digest test gets success for a refusing projection hook. |
| Return for local route before calling `on_outcome` | Delivered test reads no projection. |
| Remove terminal stale/repeated-outcome guard | Held/release test reads two extra refused projections and changed final custody. |
| Drop the observation before attempt completion | Held/refused observation test reads absent receipts. |
| Ignore the local observation byte bound | Oversize test reads a Held outcome and oversized receipt instead of Uncertain without observation. |

Selectors are the corresponding test names above. The first six use `projection_hooks`; the two observation mutations use `local_observations`. Each command has this form:

```sh
cargo test -p fleet-sync-delivery --test TEST_SUITE TEST_NAME -- --exact --nocapture
```

Restored production SHA-256 values:

```text
inbox_receive.rs   6d77119a6af008967ed77379ccffa139a3455d14b9a4769598e49f75aad6667f
inbox_storage.rs   2d4205c519dc913c2c70478e56585c96cac151d9f488dde9c5ffd9eb99b1a975
inbox_delivery.rs  0cd309878709ed6307c9846e88f3408d39711012d52d3f31adf60cbaa53930d3
deliver_task.rs    7dd0b3d2f5d80043b3fc96458f423f3de7512d7aeb2af8680c6dec26cceab169
delivery_calls.rs  c742a272544dfa6ef56a5807d35c71087c8bb836e766536bd704d85c3e194d7c
```

## Commands and results

| Command | Result | Exit |
|---|---|---|
| `cargo test -p fleet-sync-delivery` | 93 passed, 0 failed, 0 ignored across 27 harnesses, including empty unit/doc harnesses; 76 pre-existing tests and 17 new tests | 0 |
| `cargo test -p fleet-sync-delivery -p fleet-sync-protocol` | 93 delivery and 60 protocol tests passed, 0 failed or ignored. Reused for unchanged protocol sources; final delivery run covers the later local type-parameter simplification. | 0 |
| `cargo clippy -p fleet-sync-delivery -p fleet-sync-protocol --all-targets -- -D warnings` | Passed | 0 |
| `cargo fmt -p fleet-sync-delivery -p fleet-sync-protocol -- --check` | Passed | 0 |
| `cargo fmt --manifest-path tests/consumer/Cargo.toml -- --check` | Passed | 0 |
| `CARGO_TARGET_DIR=<assigned-worktree>/target cargo check --manifest-path tests/consumer/Cargo.toml --features test-support --all-targets` | Native test-consumer library, binaries and tests compile | 0 |
| `CARGO_TARGET_DIR=<assigned-worktree>/target cargo clippy --manifest-path tests/consumer/Cargo.toml --features test-support --all-targets -- -D warnings` | Passed | 0 |
| `git diff --check` | Passed | 0 |

Local raw evidence remains under the worktree's ignored `tmp/`: `c37-pre-wiring.log`, `c37-projection-green.log`, `c37-v448-crate-tests.log`, `c37-v448-final-delivery-tests.log`, `c37-v448-clippy.log`, `c37-v448-consumer-check.log`, `c37-v448-consumer-clippy.log`, `c37-v448-wrong-*.log`, and `c37-v448-wrong-build-results.json`. These local logs do not travel with the commit; permanent tests and this report do. The earlier decision-447 logs remain separately retained.

Some build invocations emit diagnostic text while enumerating invalid template manifests in the pinned workers-rs dependency checkout. Cargo's final build/test result succeeds; no dependency checkout or build gate was modified to suppress those diagnostics.

Clippy initially rejected the fixture's explicit future as `manual_async_fn`. A function-local `#[expect]` records why this spelling is required for the observed higher-ranked `TransactionScope` compiler error. The corrected clippy run succeeds with `-D warnings`. The later public `LocalDelivery<D>` shape follows `DeliveryItem<D>` and derives its payloads from DeliveryTypes, avoiding repeated associated-type expressions rather than suppressing the type-complexity lint.

## Limits and handback

- This evidence covers native Turso and a controlled task abort/recovery. It does not establish physical process termination, power-loss recovery, workerd/Cloudflare transactions, release packaging, or an external application's integration.
- All 76 pre-existing delivery tests preserve their behavioral assertions; their local consumers/adapters were mechanically cut over to unit observations. AgentStudio and the full workspace were not built; workspace-wide commands were outside the commission.
- The existing project verification skill has no delivery-projection journey. Its files and drivers were left untouched; the commissioned permanent library tests supply the bounded evidence.
- No deliberate split-transaction implementation was constructed. Hook-error rollback, a later transaction abort, and task interruption establish the observed atomic paths; eight separate mutations establish the sensitivities listed above.
- No independent Verifier observation has arrived. The Lead owns commissioning it on the declared head and accepting this result. This work does not claim delivery acceptance.
- No production casts, duplicate recipient-outcome types, non-exhaustive outcome conversions, or peer formats were introduced. Test-only `serde_json::Value` reads inspect independently stored records; shared fixtures retain the repository's test-only unwrap/dead-code allowances. The observation fixture's host is a controlled local recipient; real custody resolution, consumer invocation, validation, journal and Turso transactions remain on the exercised path.
- The local answer hard cutover advances DeliverLocally to version 2. Existing version-1 operations are subject to the engine's existing version-mismatch handling; there is no old-answer decoder or migration. External consumers must add the associated type and adapt the local delivery result/validator call before compiling against this API.
- The first hook slice was committed as `4d352a4` before decision 448 arrived. The addition is a second scoped commit because the commission permits only new commits and forbids history rewriting. Both commits belong to the final handback; neither was pushed.

If merged with a misbehaving consumer, a projection hook that writes outside its supplied scope could still create an application-side crash gap. This change supplies the atomic seam; the consuming application's use of it remains its own integration obligation.
