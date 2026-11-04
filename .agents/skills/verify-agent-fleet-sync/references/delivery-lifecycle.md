# Delivery lifecycle by host role

Run `bash scripts/verification/delivery-lifecycle.sh --help`, then run that
script with a new `--evidence-dir`. It executes the real delivery package suite
and requires the named L5 cases to pass; an empty or missing selector cannot
count as coverage. Use the declared worktree's normal target. Configure the
host's Cargo signing runner before launching executables where required.
The script leaves target cleanup until all observations sharing it finish.

The adopted outcomes are D1 plan L5's removal of per-machine relay custody and
the declared L5 contracts: explicit `DeliveryHostRole::{Machine, Relay}`, ten
retired relay tables, unchanged Machine custody, preserved Local-to-Push report,
and a typed unadmitted-cancel Report refusal before mutation. These contracts
are also documented in `crates/fleet-sync-delivery/README.md`. A caller's adopted
reference decides disputed expectations; current output is not an oracle.

## Relay retirement and lifecycle

`retire_relay_host_tables` runs inside the host's startup transaction after the
unchanged delivery migrations. It drops these relay-only retired tables:

- `_fleet_sync_mail_cancel_links`, `_fleet_sync_mail_controls`,
  `_fleet_sync_mail_items`, `_fleet_sync_mail_state`;
- `_fleet_sync_delivery_receipts`, `_fleet_sync_inbox_state`,
  `_fleet_sync_outbox_status`;
- `_fleet_sync_reports`, `_fleet_sync_inbox_cancel_links`,
  `_fleet_sync_inbox_items`.

The `relay_schema::tests` cases start a real migrated Turso store containing all
ten tables, retire them, run public submit/Push/report/settle APIs, drop the
store and reopen it. Table absence is checked **before another retirement**;
startup migration replay, Relay backfill and removal scan must keep them absent.
Typed query retains Delivered, receipt `real report` and version 3; status and
publication remain usable. A Relay `RemoveDeliveryRecord` removes the retained
item at its injected 60-day deadline without accessing Inbox tables.

The same group checks caller rollback, a second retirement, an empty store,
and the Machine role. Machine startup must preserve exact custody/cursor row
values and all ten tables; Machine removal clears both custody homes while
retaining the applied status cursor. Inspect role selection in
`record_retention.rs`, `submitter_backfill.rs`, `test_support.rs` and their host
callers: role is supplied by the host, with no default, stored role or schema
probe. Original Machine SQL remains the Machine branch.

## Local then Push promotion

`local_cancel_routes::push_promotes_a_prevented_local_original_and_reports_once_across_reopen`
uses real Local admission and Local cancellation, then engine-driven Push of
the same original. Resolve the report through the public Inbox API: item 1,
report sequence 2, `Prevented` with cancel item 2. Exactly one report is owed;
reopen and duplicate Push retain the full snapshot and do not renew debt.
Driving reports produces exactly one terminal ExecutionFailed refusal, no
retry after reopen/clock advancement, and zero Local delivery calls. The
projection-refusal companion checks atomic rollback of promotion and report
debt through a rejecting consumer callback.

The in-process network fixture serializes actual delivery calls and executes
real recipient/relay APIs and transactions. It does not establish WebSocket,
hosted or Cloudflare behavior.

## Typed cancel-report refusal

`relay_outbox::unadmitted_cancel_reports_return_typed_refusal_without_any_transaction_write`
calls `Outbox::recipient_report` with Prevented and TooLate reports naming an
unadmitted cancel. It must return `ReportResult::Refused` with
`ProcessingFailed { kind: ExecutionFailed }` and leave the connection's
`total_changes()` unchanged. Corrupt stored custody still returns corruption;
a missing validation table stays a storage error. Test-network or reference
host translation must not manufacture the refusal.

## Native caller and evidence boundaries

When the assignment requires the consumer CI gate, run once from the root:

```sh
cargo test --manifest-path tests/consumer/Cargo.toml --all-features --locked
```

Use the same normal target and configure signing for its `delivery_node` and
`fleet-sync-test-consumer` child apps as well as test harnesses. The native
fixtures require child PID-correlated READY followed by a matching authenticated
Welcome; they own socket/process shutdown. The status-corruption case seeds a
real Sent@1 item, corrupts its status cursor, requires a transport failure,
then queries the nonempty healthy item with exact id, digest, status and version.
An empty query does not prove healthy custody reads.

Keep raw counts/exits, source state, commands, disk checks and executable hashes
and signatures. Compare migration paths and exact bytes with the adopted
baseline when required. A passing current test is distinct from an observed
seeded failure: report sensitivity as unobserved unless authorized fault evidence
was actually inspected. Do not mutate product code to obtain it without a grant.
Retain evidence and fixture state needed by its consumer; check no process uses
the normal target before the build setup's dry-run and cleanup. These journeys
prove native library paths; integrated Router, workerd and Cloudflare acceptance
remain separate.
