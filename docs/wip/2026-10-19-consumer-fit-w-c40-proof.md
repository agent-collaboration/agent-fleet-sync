# Linear snapshot and transaction-fragment sizing

W-C40 preserves the release's page and part identities while removing repeated encoding and hashing of growing prefixes. Base: `77da7d4a279df254ebee24adae48c5d02e4a46f8`.

The writer and receiver measure each empty snapshot page once. Each candidate then counts only its row JSON, an array comma when needed, and replacement of the `next` cursor JSON. All current sizing carriers contain the page once and a fixed-width digest, so this changes every carrier by the same exact number of bytes. Before returning any page, the producer measures the complete page and checks equality with the tracked size. A mismatch returns typed corruption, without emitting a fallback page.

Transaction fragments count each row or schema statement once into byte prefix sums. Up to two empty-fragment carriers establish the unannotated and annotated bases without cloning the annotation. The existing provisional maximum-width descriptor still determines all split decisions and refusal sizes. Final construction adjusts only the descriptor's JSON width, measures each actual piece once, and refuses a size mismatch through the existing `part.size_changed` error.

No wire shape, digest definition, unit limit, cursor rule, stored representation, production SQL query, or dependency changes.

| Site | Base implementation | Replacement |
| --- | --- | --- |
| Writer snapshot page | `writer_snapshot.rs:159` measured the complete growing page per row | `snapshot_size_after_row` at `writer_snapshot.rs:155`; final equality at `:258` |
| Receiver snapshot page | `snapshot_reads.rs:179` measured the complete growing page per row | Row/cursor delta at `snapshot_reads.rs:177`; final equality at `:222` |
| Captured transaction cutter | `piece_builder.rs:91` built, hashed and measured each growing candidate | Item prefix sums at `piece_builder.rs:308`; selection at `:91`; final descriptor adjustment/equality at `:273` |

Paths in that table are relative to the corresponding crate's `src/`. Base anchors refer to the base commit, replacement anchors to this change.

The remainder of the original byte-bound change was audited. Receiver assembly hashes the complete assembled transaction once after the count guard (`part_assembly.rs:9,53`). Seed validation and storage hash individual complete pages (`seed_rules.rs:114`, `seed_storage.rs:107`); snapshot installation also hashes one complete page (`snapshot_install.rs:127`). Catalog sizing counts its statement vector once (`fleet-sync-protocol/src/schema_catalog.rs:195`). These are not growing-unit measurement loops.

## Differential and work-bound proof

The pre-change algorithms remain under `cfg(test)` in `snapshot_sizing_oracle.rs` in each pager crate and `piece_sizing_oracle.rs` in the writer. Their algorithm bodies match the base modulo function names, visibility and formatting. The piece oracle retains its own pre-change finishing algorithm.

The permanent tests exercise real native writer snapshots and receiver SQLx scopes, plus the real cutter, typed carriers, serialization and digests. They compare encoded outputs byte for byte and compare typed oversize errors including `encoded_size`.

Coverage includes generated rows with JSON escapes, Unicode, blobs, integers and nulls; exact `TARGET_UNIT_BYTES` and `MAX_UNIT_BYTES` minus one, equal, and plus one; oversized rows and snapshot schema; row-count and byte-count splits; multiple tables, empty tables, table transitions and mid-table cursors; invalid receiver cursors; row and schema fragments; non-Clone annotations, oversized annotations and an empty final annotation fragment. Separate tests require typed corruption when a final page size differs.

The test-only counters require at most two full measurements per returned page and at most the number of emitted fragments plus three full piece constructions/measurements. No timing assertion is used.

Focused baseline: `cargo test -p fleet-sync-writer -p fleet-sync-receiver --locked -j4 --lib sizing -- --nocapture`: writer 5/0, receiver 3/0, exit 0.

Each site was deliberately changed back to full measurement inside its candidate loop. For the pagers, the control also removed the final measurement, reproducing the old strategy. Each strengthened control selected one test and failed specifically at its work-count assertion:

| Wrong build | Observed work | Result |
| --- | --- | --- |
| Writer page | 257 full measurements for a 256-row page | 0 passed, 1 failed, exit 101 |
| Receiver page | 257 full measurements for a 256-row page | 0 passed, 1 failed, exit 101 |
| Transaction cutter | 38 full constructions/measurements for two emitted fragments | 0 passed, 1 failed, exit 101 |

Every mutated production file was restored byte for byte, checked by SHA-256, before the final checks. Runs occurred as baseline, controlled wrong builds, restoration, final checks; no pre-fix tests-first run is claimed.

## Final local gates

All thirteen commands below completed with exit 0, sequentially, using the normal shared target and `-j4` for compilation. Free disk was checked before every command and throughout compilation; neither resource floor was crossed.

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | passed, exit 0 |
| `cargo clippy --workspace --all-targets --all-features --locked -j4 -- -D warnings` | passed, exit 0 |
| `cargo test --workspace --all-features --locked -j4` | 755 parent-level passes, 0 failures, 2 ignored |
| `node tests/durable-steps/check-query-metadata.mjs` | all 18 files match |
| `cargo check --target wasm32-unknown-unknown -p sqlx-durable-object -p sqlx-durable-object-core -p probe-core -p probe-worker -p runtime-primitives -p durable-steps -p fleet-sync-protocol -p fleet-sync-session -p fleet-sync-receiver -p fleet-sync-lease -p fleet-sync-delivery --locked -j4` | passed, exit 0 |
| `cargo check --target wasm32-unknown-unknown -p durable-steps-worker --locked -j4` | passed, exit 0 |
| `cargo check --target wasm32-unknown-unknown -p durable-steps --no-default-features --locked -j4` | passed, exit 0 |
| `cargo tree --target wasm32-unknown-unknown -p durable-steps --no-default-features --edges normal --locked` | no Tokio |
| `cargo tree --target wasm32-unknown-unknown -p fleet-sync-delivery --no-default-features --edges normal --locked` | no Tokio |
| `cargo fmt --manifest-path tests/consumer/Cargo.toml --all -- --check` | passed, exit 0 |
| `cargo clippy --manifest-path tests/consumer/Cargo.toml --all-targets --all-features --locked -j4 -- -D warnings` | passed, exit 0 |
| `cargo test --manifest-path tests/consumer/Cargo.toml --all-features --locked -j4` | 95 passed, 0 failed, 0 ignored |
| `cargo run -p fleet-sync-query-gen --features native --locked -j4 -- --manifest tests/consumer/checked-queries.json --check` | verified checked SQL |

The writer unit suite reports 102/0 and the receiver 110/0, including all eight new cases. Workspace raw summaries contain 756 passes because the checked-query diagnostics harness also reports one nested test; the parent-level total above avoids counting that nested result twice. The two ignored entries are the spawned session-host helper and the existing local Durable Object scenario requiring a separately running Worker. The consumer run includes real websocket delivery, pinned snapshots, capture replay across process kill, lost answers, updates/deletes, seed retries, migrations and lifecycle transitions.

The full workspace and consumer gates use the commands in `.github/workflows/ci.yml`; compilation parallelism is bounded locally. Per-command logs, argv, exit codes, disk readings and mutation/restoration receipts were retained in the implementation handoff. The generated `trybuild` output was removed after its test completed and no open users remained; the shared target is retained for the paired verifier.

## Limits

This is implementation-owned native and compile/check evidence. Independent observation, hosted CI on the new commit, and the board's runtime after its pin update remain separate. The board's end-to-end timing is not established by the deterministic work counters.
