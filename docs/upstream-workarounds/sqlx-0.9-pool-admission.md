# SQLx 0.9 pool metadata and native file admission

A closed SQLx pool can outlive all its database connections. Native file admission must distinguish that metadata lifetime from the writer's terminal close.

In pinned `sqlx-core` 0.9.0, `pool/connection.rs::PoolConnection::return_to_pool` keeps an extra `PoolInner` Arc around its asynchronous return and minimum-connection maintenance. `pool/inner.rs::PoolInner::close` waits for connection permits, closes idle connections and sets the size to zero. It does not require every Arc or the pool options to disappear. Our `open_pool` callback captures the `FileAdmission` token in those options, so relying only on its final Arc drop can keep a Writer claim after every SDK connection has drained. An immediate `RetainedFile` open can then return `FileInUse`.

This is a mismatch in our admission lifetime, rather than evidence that SQLx failed to close a connection. The permanent lifecycle regression reproduces the post-drain refusal on both `7bf44196` and `3283e53f`, before the repair. It holds real closed pool clones with size zero; no timing, retry or fabricated connection is involved. The original CI failure that prompted investigation did not identify which fallible open failed, so this reproduction does not retroactively localize that CI call site.

`NativeFileOwner::close` keeps `OwnerState::Closing` and its claim while draining the writer, reader and maintenance pools. Only after all three close operations finish does it release the claim terminally. A once flag in `FileAdmission` makes later Arc drops harmless, including after another writer has acquired the same file. A cancelled close keeps the claim until a retry finishes draining; ordinary Arc drop remains the fallback for abandoned owners. Pool metadata cannot reopen a closed pool.

Run the permanent checks from the repository root:

```sh
cargo test --locked --all-features -j4 -p fleet-sync-writer --lib closed_pool_metadata
```

`closed_pool_metadata_does_not_keep_a_drained_writer_claim` first holds a real checked-out SDK connection while public close is pending. Retained access must refuse while that connection can still query the file. It then closes that connection, awaits public close, checks all three pools are closed and empty, and opens the retained file while the closed pool clones remain alive. `dropping_old_closed_pool_metadata_cannot_release_a_new_writer_claim` opens a new writer before dropping old closed metadata and verifies that the new claim still excludes a retained reader.

Reassess this binding on an SQLx upgrade. Remove the explicit terminal release only when the first regression passes with that release removed while closed pool clones remain held, and the held-connection exclusion, cancelled-close and later-Arc-drop checks also pass. An awaited pool close or size-zero observation alone is insufficient evidence. Do not substitute sleeps, retries, skipped tests or a weaker admission barrier. This change files no upstream issue.

## Prepared results and the native drain

Pool size zero establishes a real native drain only when returned metadata no longer owns an SDK connection. Turso 0.8.1 prepared statements own a connection and keep its database alive. Returning a raw SDK statement from SQLx preparation allowed the public guarded reader's prepared result to outlive writer close, after the physical claim was released. A retained opener could then reuse the still-writable cached database despite requesting read-only mode.

The driver now detaches SDK handles at its public `Executor::prepare_with` return. SQL, parameters and column metadata remain owned and cloneable; native statements stay in the connection-local cache. Query execution prepares on the connection it actually pins. This covers preparation from `ReadPool`, `ReadTransaction`, `WriteTransaction` and `MaintenanceTransaction`, including prepared results returned by a `WriterJournalScope` callback, without delaying admission for metadata-only values. Describe results, materialized rows, values and query results own no native connection. Active streams and snapshots retain their existing pooled transaction/pin until completion or drop.

The C36 regression now holds a prepared result and the database itself across close. It observes a committed WAL, challenges native read-only behavior below the private reader guard, and compares the exact post-close file set through retained open/read/close. A second case holds read/write transaction statements and clones, rows and descriptions. A journal case returns a prepared statement from the real maintenance transaction. A public row-stream case keeps close pending until the stream drops, then holds its materialized rows across retained admission. A driver case executes surviving prepared metadata on another pinned connection. No public raw-pool capability or additional lock is introduced.
