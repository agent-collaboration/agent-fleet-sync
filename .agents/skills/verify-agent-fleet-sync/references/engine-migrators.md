# Journal migration and native recovery

Use [build setup](build-and-evidence.md) for the declared source, exclusive slot,
normal target and retained evidence. Configure the host's Cargo signing runner
before every native `cargo test` or `cargo run` where required.

## Native journal

Run the permanent general-engine integration suite:

```bash
cargo test --offline --locked -p durable-steps --all-features --test integration -- --nocapture
```

Inspect the selected cases and their exact assertions:

- `journal_keys::journal_keys_declare_not_null_and_reject_null_with_valid_insert_controls`
  applies the production corpus on native Turso and audits the three journal
  primary keys. Each must declare NOT NULL, refuse a NULL insert at its own
  column and accept the corresponding valid insert.
- `journal_metadata::native_upgrade_preserves_saved_history_and_replays_it_once`
  checks exact UUIDv7 primary-key metadata, saved history, migration receipts,
  injected timestamps and one-time replay after upgrading a frozen v1 fixture.
- `journal_constraints::every_natural_unique_refuses_duplicates_without_changing_the_upgraded_rows`
  requires each natural UNIQUE refusal and unchanged journal rows.

For the native process-restart journey, build the existing example, sign its
executable under the host's policy, then run its controller:

```bash
cargo build --offline --locked -p durable-steps --features turso --example native_journey
node tests/durable-steps/run-native.mjs
```

Require all eight SIGKILL cuts, exact effect/delivery counts after restart and
joined child processes. Building alone does not prove recovery.

## Workerd journal and SQL

Use the durable-steps worker build and controller in
[scope proof](consumer-fit.md#shortened-transaction-scope-views--w-c14).
The controller includes metadata upgrade and exact journal constraints through
the real Rust Durable Object adapter.

The public SQL controller separately executes the exact production SQL corpus
through raw workerd SQLite transactions:

```bash
node tests/integration/public-migration-sql.mjs
node tests/durable-steps/check-query-metadata.mjs
```

Require both `journal/fresh` and `journal/v0.4.0`, a nonempty corpus, successful
migration and no `_uuid_` scratch columns. The metadata command checks all
committed SQLx receipt files against their current SQL sources. These are
separate observations from native Turso and Rust adapter execution.

Return commands, selected counts, exits, runtime observations and evidence at
the declared head. Missing cases remain unknown; no hosted or release
acceptance follows from these checks.
