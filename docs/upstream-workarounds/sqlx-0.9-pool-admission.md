# SQLx 0.9 prepared metadata and native file admission

The driver must detach SDK handles from prepared results before returning them
through the public SQLx executor. Turso 0.8.1 prepared statements own a connection
and keep its database alive. Returning a raw SDK statement allowed prepared
metadata to retain a writable database after the owning pool had drained.
A subsequent read-only opener could reuse that writable cached database.

## Retained driver workaround

`crates/sqlx-turso-core/src/executor/mod.rs::prepare_with` returns
`TursoStatement::into_metadata`, which clears the native handle. SQL, parameters
and column metadata remain owned and cloneable; native statements stay in the
connection-local cache. Query execution prepares on the connection it actually
pins. Describe results, materialized rows, values and query results own no
native connection. Active streams and snapshots retain their existing
connection or pooled transaction until completion or drop.

Run the driver regression from this repository's root:

```sh
cargo test --locked -p sqlx-turso-core --lib statement::tests::prepared_metadata_survives_close_and_uses_the_pinned_target -- --exact
```

It closes the original connection while retaining prepared metadata and its
clone, then executes the clone against a different connection and checks that
connection's row. It also checks the retained SQL and column metadata.

Reassess detachment on an SQLx or Turso upgrade. Remove it only when prepared
results no longer retain native SDK connections and both the driver regression
and the fleet retained-file lifetime controls below still pass. Successful
execution on another connection alone does not establish physical-file drain.

## Fleet admission ownership moved

The separate pool-metadata lifetime workaround now belongs to codex-router's
[fleet-project-writer](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-project-writer).
Its [NativeFileOwner::close](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-project-writer/src/native_file_owner.rs)
keeps the physical claim until writer, reader and maintenance pools finish
closing, then releases it terminally. A later Arc drop cannot release another
writer's claim. Cancelled close retains the claim until a retry finishes draining.
SQLx pool metadata can outlive drained connections; its Arc lifetime alone
cannot decide file admission.

The moved [retained-file lifetime tests](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-project-writer/src/retained_file_lifetime_tests.rs)
cover held connections, closed pool clones, a later writer claim, prepared
transaction/journal results and a live public row stream. Their names include
`closed_pool_metadata_does_not_keep_a_drained_writer_claim`,
`dropping_old_closed_pool_metadata_cannot_release_a_new_writer_claim`,
`prepared_transaction_results_do_not_outlive_native_admission` and
`prepared_journal_results_do_not_outlive_native_admission`.
Keep those controls when reassessing the fleet admission workaround; pool size
zero alone is insufficient. No upstream issue has been filed.
