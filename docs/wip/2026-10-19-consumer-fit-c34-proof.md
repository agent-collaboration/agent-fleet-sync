# C34: publish a notice to a peer

Authority: program design §13a W-C36 and canonical r4 row C34. Base: C31 commit bab0fb82eceb8add4a13bd467e1be0aafcdd03b4.

`publish_notice_to_peer(scope, &peer, body: Notice<P>)` uses the existing peer index and delegates each append to the existing per-session notice publisher. Every non-ended logical session keeps its own notice position and durable backlog. Only an attached connection receives a send effect. An absent or ended-only peer returns an empty effect list without writes. The caller commits before applying effects and discards them on rollback.

Consumer notice payloads retain the existing Serialize/DeserializeOwned contract. They need no Clone implementation: the function encodes the body once and obtains owned payloads through that wire contract. It adds no session index, stored field, query, schema, migration, corpus, payload trait bound or public error variant. No-open-session detection precedes body encoding.

## Real-Turso proof

Five permanent rule cases passed on the first implementation:

- A notice with a non-Clone consumer payload commits, survives a new SQL transaction and yields the expected attached-connection frame. Its encoded frame decodes with the literal body and position 1. Detaching the session retains a second notice at position 2 with no send effect.
- A peer's ended session remains unchanged while its live session advances. Another peer's record and notice history remain unchanged.
- Absent and ended-only peers return no effects. Complete selected session/notice rows and registrations remain unchanged, including after commit.
- A notice and head update observed inside a real transaction disappear on rollback. The previously committed attached session remains at position zero with no notice history; the caller discards the pending effect.
- A session at u64::MAX minus one advances to u64::MAX. The next publication returns NoticePositionExhausted without changing rows.

The existing SqlScope fixture uses actual Turso transactions. Its attachment helper acknowledges external schema publication as a fixture, as in the existing session-rule suite. This proof inspects typed/encoded send effects after SQL commit; it does not claim native SessionHost dispatch, socket arrival or registry-publication integration. Central's durable TailReports read remains the authoritative path; the notice is a hint.

## Sensitivity and restoration

| Compiled wrong build | Passed / failed | Exit |
| --- | --- | --- |
| Skip all session appends | 1 / 4 | 101 |
| Report an absent peer as SessionEnded | 0 / 1 | 101 |
| Append to the ended session | 0 / 1 | 101 |

The ended-session control removes both the new fan-out filter and the old publisher's ended guard, so it exercises an actual persisted wrong notice: the ended head becomes 2 instead of 1. All controls run unchanged permanent tests. The production file was restored byte-for-byte and the test hash stayed unchanged. These are post-fix controls for a new API, with no claimed pre-feature behavioral red.

The restored complete session suite passes 20/0/0, exit 0: 14 rule cases and 6 client lifecycle cases. The focused five are included, with one summary per test binary.

## Validation status

- `cargo test -p fleet-sync-session --all-features --locked -j 4 -- --test-threads=4`: restored 20/0/0, exit 0.
- `cargo clippy -p fleet-sync-session --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0.
- `cargo check -p fleet-sync-session --target wasm32-unknown-unknown --locked -j 4`: exit 0; portable compilation only.
- Package-scoped formatting and `git diff --check`: exit 0. The four-file publication scan has no private paths or identities.

CI, paired observation and acceptance are pending at declaration. The next board pin waits for this slice alongside accepted C31.

No generator or verification-driver input changed; existing strict checked-query verification stays active. No workspace-wide command was needed. Deployed DO/runtime, board composition, power loss and whole-PR readiness remain outside this slice's proof.
