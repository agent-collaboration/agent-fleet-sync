# C37: delivery projection hooks

The delivery inbox now calls consumer projection hooks through the same borrowed transaction scope as its custody writes. This is local native Turso evidence; independent Verifier observation and external application integration remain unverified.

## Target and adopted contract

- Branch: `feat/consumer-fit-c37`, based on `fc46750cdb85999d4a44f562cb5a3f64bb66b54c`.
- Governing design: W-C38, corrected by decision 447 at design revision `aa21b954`. The correction replaces the proposed four-case settlement enum and validated header with the existing `RecipientOutcome` and `ItemId`.
- Build: assigned worktree's normal `target/debug`, repository Rust toolchain and lockfile, real Turso 0.8.1, `SQLX_OFFLINE=true` from repository configuration. Entry disk availability: 109 GiB; subsequent check: 112 GiB, above the commissioned 45 GiB stop threshold.
- No wire format, storage format, migration, or existing consumer implementation changes.

`DeliveryConsumer::on_received(scope, item)` runs only for a newly stored incoming `DeliveryItem`, after its custody row is written. Controls use their existing cancellation path. Local enqueue and local-to-relayed promotion do not call `on_received` again.

`DeliveryConsumer::on_outcome(scope, item_id, outcome)` runs once in `record_header_outcome`, before its local-route return. Its borrowed `RecipientOutcome` retains every existing variant and the consumer's exact acceptance value. The existing consumer is threaded through receive-time recording, attempt completion, terminal failure, and cancellation recording for both the original and its control. A malformed stored digest adds no header validation to terminal failure.

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

## Deliberate wrong builds

Each mutation ran alone against a permanent exact-selected test, failed at runtime with 0 passed / 1 failed / 8 filtered out, and exited 101. No compiler failure counts as a caught mutation. All mutated production files were restored byte for byte before the full crate run.

| Mutation | Deciding observation |
|---|---|
| Move `on_received` out of the new-item branch | Replay/duplicate test reads three received projections instead of one. |
| Skip `RecipientOutcome::Held` | Held/release test reads no held projection. |
| Replace the projected outcome with Uncertain | Delivered test reads Uncertain instead of exact acceptance. |
| Ignore `on_outcome` errors | Malformed-digest test gets success for a refusing projection hook. |
| Return for local route before calling `on_outcome` | Delivered test reads no projection. |
| Remove terminal stale/repeated-outcome guard | Held/release test reads two extra refused projections and changed final custody. |

Selectors are the corresponding test names above. Each command has this form:

```sh
cargo test -p fleet-sync-delivery --test projection_hooks TEST_NAME -- --exact --nocapture
```

Restored production SHA-256 values:

```text
inbox_receive.rs   817bd139963c551726b8984ed04603f73437c4546bc91c72093bbaa58807710b
inbox_storage.rs   2e7db13105fa26bc82d66fa17c5ac71fa8bb097bc941316da575d7ee9b56cb37
inbox_delivery.rs  104237720353f99a00a983e345030b2a421c1aed70834c7f60b364a40575e93e
```

## Commands and results

| Command | Result | Exit |
|---|---|---|
| `cargo test -p fleet-sync-delivery --test projection_hooks --test projection_recovery -- --nocapture` | 11 passed, 0 failed, 0 ignored | 0 |
| `cargo test -p fleet-sync-delivery` | 87 passed, 0 failed, 0 ignored across 26 harnesses, including empty unit/doc harnesses; 76 pre-existing tests and 11 new tests | 0 |
| `cargo clippy -p fleet-sync-delivery --all-targets -- -D warnings` | Passed | 0 |
| `cargo fmt -p fleet-sync-delivery -- --check` | Passed | 0 |
| `git diff --check` | Passed | 0 |

Local raw evidence remains under the worktree's ignored `tmp/`: `c37-pre-wiring.log`, `c37-projection-green.log`, `c37-crate-tests.log`, `c37-clippy.log`, `c37-wrong-*.log`, and `c37-wrong-build-results.json`. These local logs do not travel with the commit; permanent tests and this report do.

Some build invocations emit diagnostic text while enumerating invalid template manifests in the pinned workers-rs dependency checkout. Cargo's final build/test result succeeds; no dependency checkout or build gate was modified to suppress those diagnostics.

Clippy initially rejected the fixture's explicit future as `manual_async_fn`. A function-local `#[expect]` records why this spelling is required for the observed higher-ranked `TransactionScope` compiler error. The corrected clippy run succeeds with `-D warnings`. This annotation is the only post-suite Rust change; it does not change runtime behavior or invalidate the suite's observations.

## Limits and handback

- This evidence covers native Turso and a controlled task abort/recovery. It does not establish physical process termination, power-loss recovery, workerd/Cloudflare transactions, release packaging, or an external application's integration.
- Existing delivery test consumers compile and all 76 pre-existing delivery tests pass unchanged. AgentStudio and the full workspace were not built; workspace-wide commands were outside the commission.
- The existing project verification skill has no delivery-projection journey. Its files and drivers were left untouched; the commissioned permanent library tests supply the bounded evidence.
- No deliberate split-transaction implementation was constructed. Hook-error rollback, a later transaction abort, and task interruption establish the observed atomic paths; six separate mutations establish the sensitivities listed above.
- No independent Verifier observation has arrived. The Lead owns commissioning it on the declared head and accepting this result. This work does not claim delivery acceptance.
- No new stand-ins, production casts, duplicate outcome types, non-exhaustive outcome conversions, or format changes were introduced. Test-only `serde_json::Value` reads inspect independently stored records; shared fixtures retain the repository's test-only unwrap/dead-code allowances.

If merged with a misbehaving consumer, a projection hook that writes outside its supplied scope could still create an application-side crash gap. This change supplies the atomic seam; the consuming application's use of it remains its own integration obligation.
