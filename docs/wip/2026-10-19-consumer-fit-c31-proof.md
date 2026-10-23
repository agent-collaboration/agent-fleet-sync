# C31: discover a mailbox store's recipient

Authority: program design §13a W-C33 and canonical r4 plan row C31. Base: C32 commit 563b15e941e69006c482cdce99776fcb884d9d5a.

`Mailbox::stored_recipient(scope) -> Result<Option<PeerId>, DeliveryError>` reads the existing singleton mail state. Empty state returns None without inventing a peer or writing. A private stored-state reader shares the existing decoder and sequence invariants with owner-bound mailbox operations. Those operations still refuse a different owner. The public method exposes only the recipient; no table, query, stored field, format, peer index or cache is added.

The caller reads the recipient, constructs the mailbox and expires due custody within one existing transaction. It reads again on each alarm. Earliest-wake scheduling stays at its current owner. Central's empty-lookup-to-first-admission wake is a separate consumer obligation, outside this library slice's proof.

## Permanent real-Turso scenarios

Three new integration cases passed on the first implementation:

- Empty stores return None with unchanged empty custody rows. Two independent stores retain their own recipients after initialization. A second peer's owner-bound open is refused inside a transaction that commits the refusal, leaving the first owner's rows unchanged.
- After dropping and reopening the complete store, no request supplies the peer. The transaction discovers it, constructs the mailbox and expires a never-fetched item at its deadline. It observes Expired/version 2 and the matching owed publication/version 2 before an injected abort, proves all selected custody/state/publication rows roll back, then commits the same path. The next deadline remains 75 ms, and repeating expiry changes no rows.
- Missing fields, malformed peer identity, canonical unsigned sequence heads above the signed storage bound, and receipt heads above stream heads are refused by both discovery and owner-bound opening. The committed check does not repair or replace the damaged state.

These are actual SQL transactions and store reopening. The alarm test invokes the caller's transaction pattern directly; it does not claim to observe central's scheduler, a deployed alarm, socket publication delivery or a new wake after an empty read.

## Sensitivity

| Compiled wrong build | Passed / failed | Exit |
| --- | --- | --- |
| Always return None | 0 / 3 | 101 |
| Omit shared sequence validation | 0 / 1 | 101 |
| Omit the existing different-owner refusal | 0 / 1 | 101 |

The unchanged permanent tests reject each wrong build. Both production files were restored byte-for-byte, and the test file's hash stayed unchanged. These are post-fix controls for a new accessor; no pre-feature behavioral red is claimed.

## Validation status

- `cargo test -p fleet-sync-delivery --all-features --locked -j 4 -- --test-threads=4`: restored 76 passed, 0 failed, 0 ignored, exit 0. The three focused cases are included, with one summary per test binary.
- `cargo clippy -p fleet-sync-delivery --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0.
- `cargo check -p fleet-sync-delivery --target wasm32-unknown-unknown --locked -j 4`: exit 0; portable compile proof only.
- `cargo fmt -p fleet-sync-delivery -- --check` and `git diff --check`: exit 0.

CI, paired observation and Lead acceptance are pending at this declaration. The next board pin waits for C31 and the following peer-notice slice.

No generator, SQL, manifest, migration, corpus or verification-driver input changed. Existing strict checked-query build verification remains active. Native Turso proof does not establish deployed DO behavior, power-loss behavior or board integration. The normal build target is retained for the authorized queue.
