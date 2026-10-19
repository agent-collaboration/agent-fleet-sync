# C28: bounded granted epoch history

This slice follows C27 and implements governing program design §13a W-C30 and canonical r4 plan row C28. It adds a read-only, ordered page of actual grants in the open interval (after, before), where before is the caller's frozen current epoch. Each result combines EpochGrant with Released, Fenced or Open end information from existing registry history and transfer records. Reservations, floors and imported unheld epochs never become grants. No table or persisted format changes are authorized.

## Source preflight

At base eb85e0fb2d9b3173bc94429cc98df8a41b35e452 the exact-grant API validates existing EpochHistory records through RegistryStorage::history. Actual history rows are written by registration and change_completion::grant. A reservation and a floor repair write separate state; floor_repair can skip epochs and replace a transfer reservation without creating history. Numeric iteration therefore cannot enumerate actual grants.

The successful grant transaction closes the previous actual RegistryLease history and writes its successor history together with the transfer's Granted outcome. A Move retains Released with its exact final position in phase_history. A cancelled Move can withdraw that release; a release phase alone does not establish a permanent end. End derivation must identify the successful transfer for the next actual grant, validate its target/epoch/start against the two history records, and then distinguish a successful Move release from a fence-based takeover. The transfer table's intent cell stores the encoded optional reserved epoch, so a bounded lookup can reuse that existing key value without JSON string inspection or new storage. The permanent real-Turso scenarios exercise this route.

Epoch keys are decimal text. Numeric ordering must preserve all u64 values, including values above SQLite's signed integer range. A range query can order canonical decimal strings by length and binary lexical value, while the Rust decoder validates canonical spelling and agreement with each stored record. No lossy signed cast or normalization is allowed.

## Proof map

| Obligation | Real path and independent expected outcome | Current result |
| --- | --- | --- |
| Enumerate actual grants | Actual registration/move/takeover history; include a zero-write grant immediately fenced afterward | Supported by the six permanent real-Turso cases |
| Exclude reservations and imports | Actual floor repair or cancelled reservation; no invented intermediate epoch or unheld import | Supported by the six permanent real-Turso cases |
| Preserve end kinds | Successful clean Move is Released at the literal final position; takeover is Fenced at its recorded cutoff | Supported by the six permanent real-Turso cases |
| Stable bounded paging | Frozen upper bound, literal ordered grants and continuation; no integer stepping; preserve unsigned boundaries | Supported by the six permanent real-Turso cases |
| Refuse corrupt state | Actual damaged history/transfer rows fail without repair, default or skip | Supported by the six permanent real-Turso cases |
| Read-only behavior | Durable records and consumer-call count unchanged before/after reads and reopen | Supported by the six permanent real-Turso cases |
| Sensitivity | Wrong integer-iteration build is caught by permanent cases | Supported by the six permanent real-Turso cases |

## Red, green and sensitivity

The initial empty projection reached all six real scenarios and failed 0/6 with exit 101 on the intended missing grants, missing end kinds, accepted corruption and invalid zero limit. The setup completed actual registration, Move, Takeover, floor repair, import/claim and release/cancellation before those assertions. The first implementation passed 6/0. The final six cases also assert unchanged durable records across successful reads and damaged-state refusals, reopen behavior, frozen paging after later grants, numeric order for actual grants 1, 2 and u64::MAX-1, and no invented import or cancelled reservation.

The integer-walk wrong build replaces the bounded page query with exact epoch lookups over successive integers, skipping absent epochs. The final control cooperatively yields between lookups so the permanent five-second deadline can observe the unbounded sparse scan. It fails the unsigned-order case 0/1, exit 101, with Elapsed; the unchanged correct build returns the literal three granted epochs in order. Two preliminary control attempts are excluded: a non-yielding loop was interrupted with exit 130 without an assertion result, and a yield using a dev-only runtime dependency failed compilation. The final control uses the standard Future poll API. Product source was restored byte-for-byte and the permanent tests were unchanged during the control.

The projection fetches at most limit+1 history rows, one actual successor per returned grant, and at most two candidate transfer identities per successor. It never materializes all database history in memory or iterates reserved epoch numbers. Decimal key validation and ordering may scan database rows; no indexed or constant-time execution claim is made. The existing exact-grant read and the page read share the same history-record decoder. A successful Move must match its one Released position; a Takeover must match the prior holder and acknowledged cutoff. Missing, conflicting or corrupt successor evidence is refused. Existing transfer/history formats are unchanged.

## Validation

- `cargo test -p fleet-sync-lease -p fleet-sync-tokio-runner --all-features --locked -j 4 -- --test-threads=4`: exit 0, 112 passed, 0 failed, 0 ignored (83 lease unit cases, 4 holder-authority integration cases, 25 runner cases). This is after control restoration and the final permanent assertions.
- Lease corpus generation: `cargo run -p fleet-sync-query-gen --features native --locked -j 4 -- --manifest crates/fleet-sync-lease/checked-queries.json`, exit 0. Four checked read queries and their generated descriptions were added; no migration changed.
- `cargo clippy -p fleet-sync-lease -p fleet-sync-tokio-runner --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0.
- `cargo fmt -p fleet-sync-lease -- --check` and `git diff --check`: exit 0.
- `cargo check --target wasm32-unknown-unknown -p fleet-sync-lease --all-features --locked -j 4`: exit 0. Compilation is not deployed DO runtime proof.
- The same granted generator command with `--check`: exit 0. Generated metadata and Rust were produced by the checked-query tool, never edited by hand.
- Public content scan across the thirteen changed/new files: zero private-path, identity, host or session matches. Existing workers-rs template discovery diagnostics appear in Cargo logs; the scoped commands above completed successfully.

## Limits and remaining work

The registry and copy paths are real file-backed Turso stores. Holder release uses the repository's existing receipted host fixture; this does not establish a real remote writer-drain journey. No deployed Durable Object observation or outer-storage precedence claim is added. No workspace-wide command was run for C28. Independent observation and exact-head CI remain separate gates; this packet makes no acceptance or whole-PR readiness claim.

C27's predecessor was accepted within its named limits after its five CI jobs passed. Its archive-dependent helpers were rebuilt from the current tree, then the exact archive dependency was explicitly released to the verifier. The normal target is retained for queued C30/C29/C18 work; its measured size was 26 GiB and free disk about 84 GiB, so cleaning it would impose unnecessary rebuild cost. Small source backups and raw logs stay in the ignored trace. C30 follows C28, then C29, C31, C18a/C18b, and C24 only under its future neutral brief.
