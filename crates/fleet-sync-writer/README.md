# fleet-sync-writer

`ReplicatedDatabase` owns a native writer and its guarded readers. The consumer
must keep at most one live `ReplicatedDatabase` owner per physical database file,
including inside one process. Independent owners racing an open or publication
are outside this contract. The consumer also prevents writers in other processes
and writable handles outside the library. File admission does not prove or
enforce that single-owner precondition.

Use `RetainedFile::open_read_only(path, schema)` for a moved-aside, rejected or
retained file after its writer has stopped and drained. It returns a named
`RetainedFile`: `reader()` provides the existing guarded `ReadPool`, and
`registration_outcome()` returns the persisted `WriterRegistrationOutcome` read
once at open. A rejected registration retains its exact reason after restart.
Unknown or invalid stored authority is refused.

The opener uses the driver's actual read-only mode. It never creates a missing
file, runs migrations, starts capture or invokes writer recovery. Supply the
migration bundle matching the retained file: supported bookkeeping and
application histories are checked by version and checksum without changing
them. Unsupported histories are refused. `begin_read()` keeps related SELECTs
on one snapshot; the same reader guard refuses writes, DDL and transaction
control.

```rust
let retained = RetainedFile::open_read_only(path, schema).await?;
let outcome = retained.registration_outcome();
let mut read = retained.reader().begin_read().await?;
// Run related SELECT queries through &mut read.
read.close().await?;
```

Turso caches open databases by physical file identity. On Unix, library admission
refuses retained access while a writable instance is live, including through a
hard-link alias, and refuses writable opens while a retained reader is live.
The claim lasts through the pool and every outliving `ReadTransaction`. Other
platforms refuse retained access with `OpenError::FileIdentityUnsupported`.
Native library paths must be lossless UTF-8. This is same-process protection;
external quiescence remains the consumer's precondition.

During publication, the writer keeps the old physical claim and acquires the
replacement identity before recovery opens it. A retained reader admitted while
that replacement is fully closed makes recovery return the typed `FileInUse`
refusal. Retry recovery after the reader and its transactions have closed.
