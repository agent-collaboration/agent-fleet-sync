# sqlx-turso

A SQLx driver for the [Turso](https://github.com/tursodatabase/turso) database engine: one owned
connection per store, compile-time checked queries and SQLx migrations.

## Origin and license

This driver is derived from [`avencera/sqlx-turso`](https://github.com/avencera/sqlx-turso) by
Praveen Perera, licensed **MIT OR Apache-2.0** as declared in its manifests (upstream ships no
separate license file). [`agent-collaboration/sqlx-turso`](https://github.com/agent-collaboration/sqlx-turso)
mirrors upstream. The crates were imported from that mirror at commit `1833f652` (upstream
`b7e5fab9` plus the Turso 0.8.1 pin and its batch-error mapping) and then trimmed and
restructured in this repository. They are distributed under the same terms as the rest of the
repository: MIT OR Apache-2.0.

## Crates

| Crate | Job |
|---|---|
| `sqlx-turso` | What applications import: the driver types and the checked query macros |
| `sqlx-turso-core` | The SQLx `Database` implementation: options, engine connection, executor, values, transactions, migrations |
| `sqlx-turso-macros` | `query!`, `query_as!`, `query_scalar!`, `query_file!`, `query_file_as!`, `query_file_scalar!` |

## Features

| Feature | Default | Enables |
|---|---|---|
| `runtime-tokio` | yes | The Tokio runtime integration |
| `macros` | yes | The checked query macros |
| `migrate` | yes | SQLx's `Migrate` for `TursoConnection` |
| `chrono` | yes | Chrono date and time codecs (UTC datetimes as RFC 3339 text) |

All features are on by default.

## Use

```rust,ignore
use sqlx_turso::{TursoConnectOptions, sqlx::{ConnectOptions, Connection}};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

let mut store = TursoConnectOptions::new()
    .filename("project.db")
    .create_if_missing(true)
    .connect()
    .await?;

// Migrations own the schema and run in one owned write transaction.
let mut transaction = store.begin_with("BEGIN IMMEDIATE").await?;
MIGRATOR.run_direct(None, &mut *transaction, false).await?;
transaction.commit().await?;

let task = sqlx_turso::query!(r#"SELECT status AS "status!: String" FROM tasks WHERE id = ?"#, id)
    .fetch_one(&mut store)
    .await?;
```

- Applications depend on `sqlx` (for `migrate!` and the SQLx traits) without its `sqlite`
  feature, and use `sqlx_turso::query!`, not `sqlx::query!`.
- Foreign keys are on by default. A migration that rebuilds a table other tables reference
  turns them off before the owned transaction (`PRAGMA foreign_keys = OFF`), reinserts rows,
  checks `PRAGMA foreign_key_check` before commit, then turns them back on and reads the flag
  back.
- `read_only(true)` or `?mode=ro` requests the engine's read-only file mode and refuses
  missing files. The caller must exclude live writable SDK instances by physical file
  identity before opening: Turso's process cache can otherwise reuse a writable instance.
  `fleet-sync-writer::RetainedFile` owns this admission for retained library files.

## Offline metadata

Checked queries read `query-*.json` files when `SQLX_OFFLINE=true`, which `.cargo/config.toml`
sets for every build. Turso metadata lives beside the crate that invokes the macros
(`crates/<crate>/.sqlx/`).

The committed metadata was imported with the driver. The diagnostic suite also describes
queries online against a fresh in-memory Turso database, checking both accepted queries and
compile-time rejection of invalid SQL. A standalone metadata regeneration command is not yet
packaged in this repository.

## Tests

Run the driver tests against the local Turso engine:

```sh
cargo test -p sqlx-turso -p sqlx-turso-core -p sqlx-turso-macros --all-features
```

## Known limitations

| Limitation | Evidence |
|---|---|
| Bind arity is not checked: an extra bind fails only at execution; a missing bind reads NULL | Tested |
| `PRAGMA defer_foreign_keys = ON` does not defer; write parents first | Tested |
| Rebuilding a referenced table with foreign keys on fails; use the policy above | Tested |
| Read-only mode alone does not exclude an in-process writable SDK instance | Source |
| No `VACUUM`: the engine keeps it behind an experimental flag this driver does not set | Source |
| Each statement step does its file IO (`pread`, `pwrite`, `fsync`) synchronously inside `poll`, on the runtime worker that polls it; opening a store does too | Source |

Lock waits use a Tokio timer with a five-second default budget. The driver disables the native
binding's busy spin; batches resume only the busy statement. The permanent contention and
pool-return tests exercise the lock-wait behavior.
