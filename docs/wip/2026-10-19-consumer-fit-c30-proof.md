# C30: a versioned registry snapshot

Governing authority is current program design §13a W-C32 and canonical r4 plan row C30. The implementation is based on C28 commit 129ad5bc1ee2b278bd137aa12a429c81c4ac2fc2. Source-only test preparation preceded the paired verifier's explicit slot return; all C30 builds and runtime below follow that return.

## Model and source boundary

The registry owns lifecycle and the stored current grant. The snapshot versions those facts and captures their committed mutation time in the same transaction. It contains no read-clock-derived Held/Expired state or remaining time. A positive state change counter is scoped to an actual stored epoch and resets to one at a new grant. It never reads the reservation floor for identity or order. A fresh renewal with identical holder, epoch and expiry keeps the same revision and mutation time while writing its own attempt receipt. Replays, reservations, Released operation phases and cancellations do not increment. A renewal that changes expiry, including a shorter expiry or revived expired lease, increments. An open transfer may still renew the old epoch; its directory display has no proven wall-clock lag bound. A completed Move is one new-epoch revision at the actual successor grant.

The in-place required-field amendment to DatabaseRecord is explicitly authorized by W-C32 on the recorded no-deployed-store basis; staging stores reset under the existing G4 boundary. No old-record default, forward migration, new end storage, publication state or incarnation is introduced. Missing or corrupt revisions fail closed; checked counter exhaustion is a typed refusal. Publication/acknowledgement and qualified restore safety through Fleet incarnation remain consumer-owned.

The public projection reuses DatabaseLifecycle and derives one optional current grant from the private RegistryLease, omitting its private renewal bookkeeping. This partial read shape has a different purpose from the stored lease and is never a second persisted authority. RegistryRevision uses a positive NonZeroU64 counter rather than a defaultable integer. The existing database_ids remains non-retired for Welcome. A separate registry_snapshot_ids includes Retired for publication repair, sharing the same validated stored-record reader and no second inventory. The closest type principle case is one authoritative schema with a checked boundary; the storage principle case is a required field amendment, whose in-place cutover is an explicit governing exception here.

## Public reads

`registry_snapshot(scope, database) -> Result<Option<RegistrySnapshot>, LeaseError>` returns `database`, `revision { epoch, change: NonZeroU64 }`, `changed_at`, the existing `DatabaseLifecycle`, and an optional `RegistryGrant { epoch, holder, expires_at }`. Unknown identities return None. `registry_snapshot_ids(scope)` returns every validated stored identity, including Retired; `database_ids(scope)` retains its non-retired Welcome contract. Both inventories share one reader, which checks record identity, typed revision and agreement with any stored grant. No second inventory is persisted.

## Proof map

| Obligation | Permanent real-path scenario | Result |
| --- | --- | --- |
| Registration and replay | Real task grant, cached replay and distinct same-writer attempt keep one revision | Supported by permanent real-Turso cases |
| State versus call order | Changed-expiry renewal increments; two equal fresh same-time renewals get receipts with stable revision/bytes; replay is unchanged | Supported by permanent real-Turso cases |
| Expiry and read clock | Expired/shorter-expiry renewals increment; later read clocks and reopening preserve versioned bytes | Supported by permanent real-Turso cases |
| Open transfer | Actual Released phase and reserved floor leave state unchanged; old epoch renewal increments; cancellation/withdrawal preserve renewed snapshot | Supported by permanent real-Turso cases |
| Successful Move | Real successor grant resets once to new epoch/change1, frozen on replay | Supported by permanent real-Turso cases |
| Import and claim | Actual Unheld import/replay, then seeded-copy Claim resets epoch/change1 | Supported by permanent real-Turso cases |
| Retirement and repair | Retiring/Retired commits and reopen; repair includes retired while database_ids excludes it | Supported by permanent real-Turso cases |
| Corruption and exhaustion | Actual damaged rows refuse; no partial lease, revision, time or receipt changes | Supported by permanent real-Turso cases |
| Atomic mutators | Real Turso transaction aborted after each listed mutation; revision and facts remain unchanged, retry commits once | Supported by permanent real-Turso cases |

The rollback wrapper runs the actual mutator and journal over the real Turso transaction, observes the target revision before commit, then returns an injected Aborted error. It does not stub a store result or model a successful commit; an observed rollback count is required to prevent a vacuous proof. Its temporary failure flag is test-owned, with no product seam. Real remote holder drain and deployed DO behavior remain outside this evidence layer. The original empty read scaffolds compiled and failed all nine cases, exit 101, on missing snapshots or the absent registration commit-boundary observation. The first implementation passed all nine. After the final Takeover assertion and fixture alignment, the restored lease/service/runner suite passes 121/0/0, exit 0: 92 lease unit cases, 4 holder-authority cases, and 25 runner cases; the service crate compiles and has no tests in that invocation. The Takeover case advances the real copy beyond the old registry floor, then proves an actual grant at epoch 8 has revision 8/change1 rather than using a reserved count.

## Sensitivity and restoration

Skipping the record's actual revision update yields 1 passed/8 failed, exit 101. It exposes missing renewal increments, refused epoch/grant consistency, missing rollback-target observation and an accepted exhausted counter. Always incrementing the fresh-renewal branch yields 0/1, exit 101, on the second equal-facts renewal changing revision 2 to 3 and altering the serialized snapshot. Both controls compiled and ran the unchanged permanent tests. The two product files were restored byte-for-byte, with saved test hashes proving no test changed during either control. The final 121/0 suite is after both restorations.

The detailed new assertions have no separate pre-implementation red beyond the empty-API baseline and the two named wrong builds; those controls are not claimed to cover every assertion. Real-path corruption, cancellation, repair enumeration and atomicity observations remain their own green evidence.

## Quality and integration gates

- `cargo test -p fleet-sync-lease -p fleet-sync-service -p fleet-sync-tokio-runner --all-features --locked -j 4 -- --test-threads=4`: exit 0, 121 passed, 0 failed, 0 ignored, after both control restorations and final fixture/assertion updates.
- `CARGO_TARGET_DIR=target cargo test --manifest-path tests/consumer/Cargo.toml --all-features --locked -j 4 -- --test-threads=4`: exit 0, 95 passed, 0 failed, 0 ignored. The real native service, holder changes, lifecycle, session restart, rollback, socket and custody paths ran. These integration scenarios do not expose the new snapshot API through an RPC; the nine native core cases exercise that API directly.
- `cargo fmt -p fleet-sync-lease -- --check` and `git diff --check`: exit 0.
- Public content scan across all seventeen changed/new files: zero private-path, identity, host or session matches.
- Scoped library and native consumer Clippy, all targets/features with `-D warnings`: exit 0. The first library Clippy run found an assertion helper returning Result. It now returns unit and compares the same literal epoch/counter/time expectations, with no lint suppression. The final 121/0 run above follows that mechanical test-only cleanup. Both controls were refreshed after this helper cleanup against the final unchanged tests: 1/8 and 0/1, exit 101. Both product files were restored byte-for-byte, and the final positive rerun is 121/0/0, exit 0.
- `cargo check --target wasm32-unknown-unknown -p fleet-sync-lease --all-features --locked -j 4`: exit 0, after an explicit low-disk hold was lifted. All earlier local jobs had settled when the hold was read; no partial run was counted and no build started during the hold. This is compilation, not deployed DO proof. No SQL, migration, checked-query manifest, generator or driver input changed; existing strict build-time corpus verification remains active, and no C30 generation/write command has been run. No workspace-wide command has been run. Independent observation and exact-head CI remain pending at declaration; this packet does not claim acceptance or whole-PR readiness. All local jobs are settled. The normal target is retained for the queued C29/C32/C31/C18 work; no broad cleanup was run.

C28's predecessor evidence is preserved separately: independent original history6/0, three independent controls0/1 each, restored112/0, lease corpus byte-identical, and all five exact-head CI jobs successful. Current-tree helpers were rebuilt for the six packages whose binaries referenced the verifier archive; 193 executable artifacts then had zero exact archive-prefix references, and the archive dependency was explicitly released. The normal build target is retained for queued work.
