# Public engine migrators — W-C17

Each engine owns and exports its migration corpus; the host applies that
corpus alongside its application migrations in the same host-owned migration
transaction before calling engine rules. W-C17/C14 adopts this public boundary.
Verify the actual exported corpus and its real Turso rule path, then inspect
the service's use of the same export. Common journal setup and consumer hook
migrations have different owners and cannot stand in for an engine corpus.

Inputs are the caller's declared immutable head, selected engine/host changes
and observation/fault grant. Use [build setup](build-and-evidence.md) for the
pinned archive, exclusive slot, normal worktree target and evidence. Run the
commands below from that archive root with its pinned configuration. These
commands reuse permanent tests; no duplicate SQL harness or new initializer
is needed.

## Inspect the corpus and its callers

Read each selected crate's `src/lib.rs`, `migrations/` and README:

| Engine | Public export | Real rule/fixture source |
| --- | --- | --- |
| Registry | `fleet_sync_lease::LEASE_MIGRATOR` | `crates/fleet-sync-lease/src/turso_lifecycle_tests.rs` and `turso_registry_fixture.rs` |
| Replication copy | `fleet_sync_receiver::RECEIVER_MIGRATOR` | `crates/fleet-sync-receiver/src/turso_rule_tests.rs` and `turso_rule_fixture.rs` |
| Delivery | `fleet_sync_delivery::DELIVERY_MIGRATOR` | `crates/fleet-sync-delivery/tests/inbox_integrity.rs` and `tests/support/mod.rs` |

Each public `sqlx::migrate::Migrator` must embed its own crate's corpus. Follow
the fixture to the owning transaction before trusting a passing rule test:

- Registry opens its separate durable-steps store/journal, applies only the
  lease corpus for registry storage and reports accepted schema support before
  registration. Those report calls can expose a missing corpus before register.
- Receiver applies the receiver corpus on its transaction, then initializes
  the consumer's hook-local migrations and creates/applies the copy. Hook-local
  tables stay outside replicated schema membership. The copy connection uses
  foreign-key enforcement disabled, as its README requires; the writer owns
  integrity of the whole transactions the copy replays.
- Delivery opens its separate durable-steps store/journal, applies the delivery
  corpus in the host transaction, then uses public inbox receive rules.

Do not count durable-steps journal tables or consumer hook tables as the
lease/receiver/delivery corpus. Match the migration history-table choice to
its host; do not impose one global remapping on all callers.

In `crates/fleet-sync-service/src/`, inspect `registry_host.rs`, `copy_owner.rs`
and `delivery_owner.rs`. They must use the corresponding public export through
their existing host transaction and history-table policy. There must be no
second private `migrate!` embedding for these three corpora in those owners.
The registry/copy/delivery history tables remain
`_fleet_sync_registry_migrations`, `_fleet_sync_copy_migrations` and
`_fleet_sync_delivery_migrations`. Source inspection establishes routing;
the commands below establish the exercised behavior.

## Drive the matching real rules

For a change spanning all three exports, run each selector once, retaining
literal commands, selected Rust counts and exits:

```bash
cargo test --offline --locked -p fleet-sync-lease --all-features --lib turso_lifecycle_tests::register_creates_copy_before_grant_and_replay_never_issues_another_epoch -- --exact --nocapture
cargo test --offline --locked -p fleet-sync-receiver --all-features --lib turso_rule_tests::repeated_piece_returns_original_position_without_repeating_hook_writes -- --exact --nocapture
cargo test --offline --locked -p fleet-sync-delivery --all-features --test inbox_integrity canonical_pending_cancel_applies_when_its_original_arrives -- --exact --nocapture
```

Require the selected test to execute; a filtered zero is not a pass.

- Registration must create the real copy before granting epoch one. Replaying
  the same registration returns its original answer without another copy call
  or epoch; inspect the rule's direct copy-status assertions.
- Apply must store the two real Pieces' rows and position. Repeating the same
  Piece returns its original position/AlreadyApplied without repeating the
  hook writes; inspect rows, receipt/position and hook counters in the test.
- Receive must retain the pending cancellation, apply it when the original
  arrives and report the exact Prevented identity. Inspect the inbox reports
  and pending-state readbacks, not just an `Ok` return.

For native service regression coverage, use the unchanged standalone consumer
suite at that same pinned head:

```bash
cargo test --offline --locked --manifest-path tests/consumer/Cargo.toml --all-features -- --nocapture
```

Reuse a completed observation only when its head, configuration, commands and
provenance still match. If this suite is already required for frame changes,
one invocation covers both selections. Keep its target summaries and counts
separate from the three library rule tests, sensitivity controls and restores.
Do not add an equivalent green rerun or unrelated workspace gate.

## Authorized missing-corpus sensitivity

Only an explicit caller fault grant permits this step. Keep the mutation in
the isolated pinned archive and retain original bytes/hashes. After the correct
rule test passes, change just that engine's public migrator to embed the
existing session corpus instead of its own. The session corpus is a deliberately
wrong, incomplete setup for this engine; no SQL is copied into the fixture.
Run the matching selector unchanged. Repeat independently for each commissioned
engine, restoring its exact original bytes and hash before the next mutation.

The expected failure is an actual missing-engine-table SQL error, with its
real boundary recorded:

| Wrong export | Missing table in the existing fixture | Boundary reached |
| --- | --- | --- |
| Lease | `_fleet_sync_schema_support` | `report_schema_support` during initialization, before registration |
| Receiver | `_fleet_sync_hook_migrations` | `initialize_hook` during initialization, before copy apply |
| Delivery | `_fleet_sync_inbox_state` | Public inbox receive |

Inspect the actual failure if fixture order changes; do not force it to match
this table. An earlier initialization failure proves missing-corpus sensitivity
there, not that the later register/apply rule ran negatively. If the commission
requires a later negative boundary, return the gap to Lead. A compile error,
unavailable tool or unrelated fixture failure is not the intended control.
These are post-fix sensitivity checks, not pre-change baseline reds.

Retain each mutation diff, command, exit, selected pass/fail counts and exact
SQL boundary. Verify all mutated files against the pinned source after exact
restoration, rerun the unchanged matching selectors successfully, and preserve
the restored executable hashes. Without a fault grant, report these controls
as unavailable rather than silently adding mutation authority.

## Completion and limits

Return the selected public exports/corpora, real host transaction and rule
assertions, service routing check, raw library and consumer counts, authorized
wrong-corpus/restored results, exact failure boundaries and remaining gaps.
Confirm every job and fixture child settled, keep evidence outside build output
and follow the main skill's full normal-target cleanup before slot return.

This recipe does not migrate central's Durable Objects, change central
bootstrap mapping, introduce a public initializer, or establish Worker/hosted
runtime behavior. C6's producer/adapter, lease-view and B2b/G3 observations keep
their own proof layers. Native library/service evidence and inspected public
API wiring do not establish PR readiness or delivery acceptance. A successful
recipe run is not a fresh-agent retrieval/application evaluation.
