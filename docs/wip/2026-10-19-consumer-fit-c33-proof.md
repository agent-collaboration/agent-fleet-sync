# C33: own a guarded read-copy generation

Authority: program design §13a W-C35, canonical r4 row C33 and the adopted concurrent-generation clarification. Base: accepted C35 commit 7bf44196d43366a67779b4cb8eea0ce2610585e6.

The native `ReadCopy` capability owns one disposable, manifest-verified follower file. It exposes the existing guarded `ReadPool` and an `InstalledSnapshot` containing database, installed replication position and schema identity. `ReadCopy::open_verified` validates the stored install identity and verified marker, follower identity/position/schema and actual catalog fingerprint. It uses an exclusive physical-file admission and read-only noncreating connections, with no writer record, registration, grant, renewal, capture or migration.

`InstallCoordinator::mark_read_copy_ready` reuses the receiver's ready transition without writer authority or consumer hooks. `InstallCoordinator::read_copy` requires the matching Ready target, drains and checkpoints that target, closes it permanently, checks for an empty WAL, and opens its guarded read owner. Existing `InstallSnapshot`, `StagingStore` and publication own fetching, receipts and installation. The consumer supplies a fresh target file per generation and owns cache keys, cache root and the swap between handles.

An internal read pin retains the disposable file for owned read transactions and guarded streams. Existing writer and retained readers have no disposable file owner. The final owner/pin drop schedules pool closure before exact-file/sidecar cleanup on the creating Tokio runtime. Consuming `ReadCopy::close` waits for that result; callers keep the runtime alive through cleanup. The public opener requires a closed, checkpointed file with no other writable handles or path replacement. Validation failure leaves the caller's file intact. No raw pool constructor is exported.

## Real-Turso proof

The first executable positive run passed three new native cases:

- Verified pages and manifest are published and marked ready, then read through the guarded pool with literal body and position 20. Writes, transaction control and PRAGMA mutation refuse; a duplicate file owner refuses; the old writable target stays closed. Actual catalog/CDC checks and a zero outbound-call counter show no writer/capture state or authority calls. Consuming close removes the file and sidecars.
- An owned read transaction pins the old generation. Its `ReadCopy` handle is dropped, a different file at position 30 is installed and exposed, and the old transaction still reads its literal old value while the new handle reads its new value. The old file remains until its final pin closes, then disappears; the newer handle closes independently.
- A partial install survives store close/reopen but cannot become a read capability. The coordinator refuses before closing an incomplete target. A wrong declared schema and a real unexpected catalog index both refuse, preserving the file; restoring the catalog permits guarded exposure and cleanup.

The existing durable `InstallSnapshot` restart case now exercises three source answers: explicit Moved between pages, a page-position mismatch and a moved manifest. Each abandons its original install ID, creates generation 2 at position 30, preserves only the old page-zero receipt, and reads one correct row through the new capability. Abandoned rows carry a different literal body so stale receipts or mixed contents cannot pass unnoticed. This is one parent test with three subcases, initially passing with exit 0.

The first two compile attempts caught an internal snapshot-holder guard type and a fixture error-variant name; no tests ran in those attempts. Tests were authored before implementation. No pre-feature behavioral red is claimed for the previously absent capability.

## Validation status

Three compiled wrong builds failed unchanged permanent tests, each 0 passed / 1 failed, exit 101:

- Bypass manifest verification: a bounded diagnostic rerun recorded verified=false followed by a successful ReadCopy return, then the partial-install refusal assertion failed.
- Omit the file pin and unlink before pool closure: the old file was actually absent while its read transaction and the newer generation were live.
- Refuse instead of restarting a moved snapshot: the durable task completed no operation, violating the expected completion.

All four source files touched by controls were restored byte-exact; both permanent test hashes stayed unchanged. These are post-implementation sensitivity controls. Restored full writer/runner tests pass 149/0/0, exit 0: 30 runner, 83 writer unit and 36 writer integration cases. The focused cases are included; child stdout is captured on success and not added as distinct cases. Quality checks pass:

- `cargo clippy --locked --all-features --all-targets -j4 -p fleet-sync-writer -p fleet-sync-tokio-runner -- -D warnings`: exit 0.
- Scoped `cargo fmt` check and `git diff --check`: exit 0.
- Fourteen-file publication scan: no private paths or identities.

No query, corpus, migration, schema or dependency input changed; existing strict checked-query verification remains enabled. No workspace-wide, generator, redundant local consumer or Wasm command was needed. Exact-head CI supplies the separate workspace/consumer/portable integration layer. CI, paired observation and acceptance are pending at declaration.

This is native library/file proof, with source pages/manifests and movement replies supplied by existing fixture transports. It observes store close/reopen, not a process restart or crash. Remote central sessions, sockets, board cache/root/swap integration, hostile external writers, runtime teardown, deployed DO and whole-PR readiness are unobserved. Physical-file admission follows the existing Unix support boundary; no new Windows capability is claimed. The existing read guard admits SELECT, including reserved tables; the capability preserves that contract and consumers use typed metadata.
