# Replication copy rules

The host owns the copy's SQL transaction. Apply `RECEIVER_MIGRATOR` alongside
application migrations in the same host-owned migration transaction before
calling these rules. Configure the copy connection with foreign-key
enforcement disabled: the writer owns integrity, and the copy applies whole writer
transactions. The receiver does not use `PRAGMA foreign_key_check`, which Turso
0.8.1 silently ignores.

Run `initialize_hook(scope, hook)` in the host's initialization transaction to
apply the consumer's `local_migrations`. Their tables stay outside replicated
schema membership, snapshot pages and schema identity.

`apply_piece` calls the consumer's `ReceiverHook::on_applied` inside that transaction.
For `Applied`, pass its returned items to `admit_follow_ups::<Store, Item>` on the
same scope, then commit before answering. Register `ApplyFollowUp<Store, Item>` on
the engine. Its one `FollowUpCall<Item>` carries the database and item; the host's
`dispatch` routes it to the configured hook's `follow_up(item, call_id)`. Deliveries
can repeat under the same call ID, so the consumer owns their deduplication.
Dispatch maps `Ok(FollowUpDone)` to `FollowUpAnswer::Done` and a permanent
`FollowUpFailure::Failed { reason }` to `FollowUpAnswer::Failed { reason }`.
That answer is saved, and the operation ends with the same refusal code.
`FollowUpFailure::Retry` maps to `CallFailure::AnswerLost`: the hook may already
have had effects, so the engine retries the same CallId with its bounded backoff.

An apply error requires rollback. `RefusedDiscardingParts` additionally requires
`discard_staged_parts` in a separate committed transaction before answering the
refusal. Only a successful complete piece records receipts and advances `acked`.

Seeding requires an existing copy created with `CopyOrigin::AwaitingSeed`.
Every seed call takes a `SeedAuthority` supplied by the authenticated host, never
from a frame. `Writer(peer)` must match the immutable `CreateCopy` holder; `Admin`
is accepted only for an imported copy created without a holder and awaiting a seed.
Authorization precedes generation, page and publication replay. Dispatch maps
`ReceiverError::NotWriter { current }` to the existing
`ReplicationAnswer::Refused(PieceRefusal::NotWriter { current })` for seeds as well
as other replication requests.
`seed_begin` issues a monotonically increasing generation; repeat attempts return
their original generation. Pages are staged without exposing rows. A successful
`seed_complete(scope, done, hook, authority)` publishes schema, rows and position in one
transaction, then calls `ReceiverHook::on_snapshot_installed` while those rows are
visible. For `SeedDecision::Published`, admit the returned items with
`admit_snapshot_follow_ups` in that same transaction before committing. A replay
returns `SeedDecision::Answer` and must not admit more work. Snapshot work uses
distinct request keys from piece work retained at the same position. Snapshot
installation uses `run_snapshot_hook` in its ready transaction through the same
callback and admission path. A
`ReceiverError::SeedRefused { answer }` requires rollback before sending that
answer. Other errors require rollback too. Retirement leaves a permanent marker
that takes precedence over old receipts.

Static bookkeeping SQL is described by the shared native generator and checked
for freshness by the build script. Regenerate from the workspace root:

```sh
cargo run -p fleet-sync-query-gen --features native -- \
  --manifest crates/fleet-sync-receiver/checked-queries.json
```

Append `--check` to verify freshness without writing. Dynamic replicated-table
statements quote schema-derived identifiers and bind values; integer values cross
the driver as decimal text with explicit SQL casts.
