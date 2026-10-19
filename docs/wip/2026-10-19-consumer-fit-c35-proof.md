# C35: inspect native file metadata before opening a writer

Authority: program design §13a W-C37 and canonical r4 row C35. Base: C34 commit 4dfe76198f309e3ac2d4e0bc02d7366b9e55070b.

`fleet_sync_writer::inspect_native_file(&path)` returns `NativeFileInspection`: the stored origin `{database, epoch, start, kind}`, optional bootstrap state, and optional persisted authority-loss observation. It accepts a published pending bootstrap without local migration history. The absence of authority loss does not establish a current lease.

The function uses the existing physical-file retained admission, a read-only, noncreating Turso connection and the existing validated writer-record decoder. It awaits connection close before releasing admission, including when decoding fails. It exposes metadata only. `RetainedFile::open_read_only` and its complete format checks remain unchanged. No schema argument, SQL reader, writable fallback, stored field, migration, query, corpus, dependency or verification-driver input is added.

## Real-Turso proof

Five permanent cases passed through the public API:

- Actual source snapshot pages are staged, manifest-verified and published into a seeding writer. The published file has no local-history table. After all writer owners close, inspection returns the original database, epoch 2, snapshot start, Bootstrap origin and the exact verified/not-ready marker.
- A separate published bootstrap completes `mark_bootstrap_ready` and migrations. Inspection preserves its exact ready marker, original snapshot start and Bootstrap identity.
- A normal ready Created file retains its database, epoch 1 and sequence-zero start, with no bootstrap or loss observation.
- An actual rejected registration is committed and moved aside through the library. Inspection preserves its origin and literal persisted rejection reason.
- A missing path is never created. An invalid stored origin is refused as corrupt without repair.

Every valid file is inspected twice under read-only file and directory permissions after writable owners close. Directory entry sets, file contents, file modes, file modification times and directory modification time remain unchanged. The writer cases compare SHA-256 and sizes; the bootstrap cases compare full bytes. The corrupt file is also inspected twice with unchanged filesystem evidence.

## Sensitivity and restoration

| Compiled wrong build | Passed / failed | Exit |
| --- | --- | --- |
| Require local migration history during inspection | 0 / 1 | 101 |
| Drop the persisted authority-loss projection | 0 / 1 | 101 |

The first control fails on the missing local-history table in the actual published pending seed. The second returns None instead of the stored RegistrationRejected value. Both run unchanged permanent tests. Production source restoration is byte-exact and both test hashes remain unchanged. These are post-implementation sensitivity controls; no pre-feature behavioral red is claimed for the previously absent API. Tests were authored before the implementation.

## Validation status

- `cargo test --locked --all-features -j4 -p fleet-sync-writer -p fleet-sync-tokio-runner -- --test-threads=4`: restored **146 passed, 0 failed, 0 ignored**, exit 0. This is 27 runner cases, 83 writer unit cases and 36 writer integration cases. The focused five are included. One summary per executable; child stdout is captured on success and is not added to the count.
- `cargo clippy --locked --all-features --all-targets -j4 -p fleet-sync-writer -p fleet-sync-tokio-runner -- -D warnings`: exit 0.
- Package-scoped formatting and `git diff --check`: exit 0. Six-file publication scan: no private paths or identities.

CI, paired observation and acceptance are pending at declaration. No workspace-wide command or generator run was needed. These native crates are outside the portable Wasm package set; no new local Wasm claim is made.

This is native SDK/file proof. Bootstrap grants are fixture inputs; a real remote registry grant, board restart classification, cross-process inspection, hostile external writers, crash recovery, deployed DO and whole-PR readiness are unobserved. The public entry point documents external-writer exclusion. It reuses the existing authority projector; the new retained scenario directly observes RegistrationRejected, while existing authority tests cover other projections.
