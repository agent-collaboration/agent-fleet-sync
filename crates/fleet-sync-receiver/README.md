# Replication copy rules

The host owns the copy's SQL transaction. Run the migrations in `migrations/`
before calling these rules. Configure the copy connection with foreign-key
enforcement disabled: the writer owns integrity, and the copy applies whole writer
transactions. The receiver does not use `PRAGMA foreign_key_check`, which Turso
0.8.1 silently ignores.

`apply_piece` calls the consumer's `ReceiverHook::on_applied` inside that transaction.
For `Applied`, pass its returned items to `admit_follow_ups::<Store, Item>` on the
same scope, then commit before answering. Register `ApplyFollowUp<Store, Item>` on
the engine. Its one `FollowUpCall<Item>` carries the database and item; the host's
`dispatch` routes it to the configured hook's `follow_up(item, call_id)`. Deliveries
can repeat under the same call ID, so the consumer owns their deduplication.

An apply error requires rollback. `RefusedDiscardingParts` additionally requires
`discard_staged_parts` in a separate committed transaction before answering the
refusal. Only a successful complete piece records receipts and advances `acked`.

Seeding requires an existing copy created with `CopyOrigin::AwaitingSeed`.
`seed_begin` issues a monotonically increasing generation; repeat attempts return
their original generation. Pages are staged without exposing rows. A successful
`seed_complete` publishes schema, rows and position in one transaction. A
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
