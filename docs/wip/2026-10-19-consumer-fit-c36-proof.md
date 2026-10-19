# C36: release native admission after terminal close

Authority: canonical plan r4 row C36, admission released at close. Base: C33 commit 3283e53fa8709d4185cfe5f58e1f11c3e8b6a781.

`NativeFileOwner::close` retains its claim while the writer, reader and maintenance pools drain. It now releases that claim terminally after all three awaits, before returning Closed. `FileAdmission` releases once: later Arc drops from closed SQLx pool metadata cannot decrement another owner's claim for the same physical file. Cancellation during draining still keeps the claim in Closing.

The change is local to process admission and its existing close owner. No public contract, storage, wire, schema, query, dependency, driver, profile, stack setting or test gate changes. Existing test bodies remain byte-identical; two permanent lifecycle cases are appended. The durable SQLx workaround note records the source, reproduction and removal check.

## Pre-fix behavior on both versions

The same new permanent test was run before any production repair on C33 and parent 7bf44196d43366a67779b4cb8eea0ce2610585e6. Each failed 0 passed / 1 failed, exit 101, at the phase-specific assertion: post-drain retained open with closed pool metadata returned Some(FileInUse).

The test first holds a real checked-out writer connection while public close is pending. A retained open must refuse, and the held connection can still query committed local data. It then closes that connection, awaits public close and verifies that all three held SQLx pools are closed with size zero. Retained access must succeed while those real closed pool clones remain alive. This forces metadata retention without sleeps, retries or simulated SDK connections.

This proves the close/admission defect predates C33. The original CI failure reported FileInUse without identifying its fallible call site; the new reproduction does not retroactively localize that original CI error. An initial current run reported FileInUse; the new test then gained its phase-specific assertion before the identical parent/current runs. Later rustfmt changes only line wrapping and indentation, preserving all assertions and literals.

## Repair and sensitivity

Both new cases passed 2/0/0, exit 0, after the repair. The second opens a new writer before dropping old closed metadata, then verifies the new writer still excludes retained access and can commit.

Two compiled wrong builds each failed unchanged permanent tests 0/1, exit 101:

- Release before draining: the held-connection FileInUse exclusion assertion fails while a real checked-out SDK connection remains live.
- Release again on later Arc drop: the new-writer FileInUse exclusion assertion fails after old closed metadata is dropped.

Both production files were restored byte-exact and the permanent test hash stayed unchanged across controls. The original retained-grant test was neither modified nor skipped.

## Validation status

Restored complete writer/runner tests pass **151/0/0, exit 0**: 30 runner, 85 writer unit and 36 writer integration cases. The new two and the original retained-grant case are included once. Successful child stdout is captured and not added to the count. Quality checks pass: all-target/all-feature Clippy for writer and runner, package formatting, and git diff check exit 0. The five-file publication scan has no private paths or identities. No generator input changed; strict checked-query verification stays enabled. No workspace-wide or redundant consumer/Wasm command was needed. Exact-head CI, C33 closure, paired C36 observation and acceptance remain pending.

This is native SDK/SQLx lifecycle proof with real pools and files. It does not establish cross-process locking, hostile path replacement, crash/power-loss behavior or deployed/runtime integration. The parent baseline was an immutable source archive with only the two permanent test additions overlaid, sharing the existing normal target; after current-tree reanchoring, a zero-reference artifact scan and exact no-reader checks, its disposable source was removed while provenance and logs were retained. No workspace-wide command or verification-driver change was made.
