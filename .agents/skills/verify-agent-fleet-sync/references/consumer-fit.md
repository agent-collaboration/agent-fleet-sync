# Consumer-fit journeys

The oracle is the caller's adopted consumer-fit Verification Design, rows SO52,
SO53, SO56 and SO57, including native-session scope and staged C3/C6 controls.
Query freshness, guarded reader, upgrade refusal and scope-view proof additionally
use the adopted Program Design W-C11, W-C12, W-C13, W-C14 and W-C15.
Take expected behavior from that source, never from today's product answer.

Build and cleanup use [the normal worktree target recipe](build-and-evidence.md).

## Stored cancel identities — SO51, focused receive proof

Use the adopted canonical stored-identity rule and the real inbox boundary:

```bash
cargo test --offline --locked -p fleet-sync-delivery --all-features --test inbox_integrity -- --nocapture
```

The pending-cancel test receives a cancellation before its target, corrupts
only the stored cancel-item spelling, then receives the target through public
Inbox::store_batch in a real Turso transaction. Uppercase, compact, braced, URN
and canonical non-v7 text must return Corrupt with the owning identity field.
Require exact unchanged inbox headers, reports and receive-state records.
The separate canonical control must prevent the target and produce both reports.
These are subcases within the selected Rust tests, not constructor-only proof
or extra pass counts. Source-check the strict raw ItemId conversion before
normalization; no schema, public identity format or consumer effects change.

## Holder refusals — SO52

Run `bash scripts/verification/holder-refusals.sh` after changes to holder-change
rules, checks, completion or refusal messages.

The script publishes a schema-5 unheld import through the existing consumer's
Import/SeedBegin/SeedPage/SeedComplete boundary and verifies copy status and
snapshot. P claims it. A different peer's claim must receive
`alreadyHeld{holder:P}`, with that same refusal in its stored view. A real
takeover gives it to Q; repeating the original claim must still name P and
return its identical recorded outcome.

The journey also creates a real ReplicatedDatabase holder. After two applied
migrations, it offers migration 7, whose duplicate-key insert fails. Readbacks
must show the first two migrations, the original row and the unchanged capture
position. A real move then delivers a Release request on the holder's session;
the driver passes that correlated call to the writer's public
`request_release` and returns its actual answer on that connection. Both the
writer answer and the registry refusal must name migration 7 and `pending`.
Removing the pending offer and repeating the same move must replay the first
refusal. This observes the real writer producer and registry/session path;
the driver dispatches the call rather than running NativeWriterRuntime.
The writer's entry is `ReplicatedDatabase::request_release` in
`crates/fleet-sync-writer/src/writer_release.rs`; the registry consumes it in
`crates/fleet-sync-lease/src/holder_change_rules.rs`.

**Named limit: no real writer produces Failed.** A failed migration rolls back
without recording a failed state and fails writer open. A later failing
`migrate` call on an already-open writer leaves the migration unapplied, so its
release check answers Pending. The Failed protocol variant remains unchanged;
creating a durable failed state or changing writer-open behavior is outside
this journey. Do not build a failed-holder fixture for delivered observation.

The registry's product test owns Failed pass-through at the HolderRelease seam
(`crates/fleet-sync-lease/src/turso_move_refusal_tests.rs`). It must check exact
migration/state detail in both the response and canonical durable outcome,
replay the first result after the scripted answer changes and across registry
reopen, and show no extra release, fence, lease or epoch effect. That is the
Implementer's product proof. The historical Failed fixture remains baseline
registry evidence only; the delivered journey contains no synthetic Failed
holder. It is no evidence of a real Failed producer.

Wrong builds: unit AlreadyHeld or MigrationPending, or a replay naming Q.

## Target capability — SO53

Run `bash scripts/verification/target-capability.sh` after publication, registry
schema projection, holder admission or fence changes.

The script verifies a ready, unheld schema-5 copy and attaches B with support 4.
B must receive `schemaTooNew{store:5,supported:4}`. B reattaches with support 5;
the same attempt must replay its first refusal. A fresh B attempt on an
independent ready, unheld copy must grant. The independent copy avoids holder
contention deciding this positive control. Fresh moves additionally test targets
declaring support 4 and 5, and a target without an accepted report. The holder
fixture answers the real Release at the copy's independently verified final
position. This observes registry fencing, not native writer draining.

The timing cases use the existing consumer's SchemaControlRequest and the real
FleetService/SessionHost/registry owners:

- Admit an admin move with accepted support 5 while registry drive is held
  before check, publish support 4, then resume. The admission row keeps 5 and
  the move grants. Repeat with a process kill/restart before check; a fresh
  admission on an independent unheld copy freezes 4 and is refused.
- Hold an old report while a newer attach declares 4. After releasing the old
  report, read back the newer attach sequence and support. Restart with the
  same receiver and files, read the persisted row, and observe a schema-5
  claim refused with support 4.
- Hold a new report of 4 after accepted support 5. No Welcome arrives while
  the report is held; an admin admission in that window freezes 5 and grants.
  Releasing the report yields matching Welcome and an Attached registration.
  Admin and own-session claims after publication freeze 4 and are refused.
- On another held report, a raw pre-Welcome request closes without serving
  it. Another connection's credential revocation and the target's close
  progress. Releasing the report does not resurrect the closed registration.
- Fail real finalization after staging. Read back Attaching and no committed
  attach step or Welcome; release the retry and observe Attached plus one
  Welcome. Delayed finalization after replacement or process restart never
  resurrects the old attach or adds a Welcome to the newer connection.
- Drive the production SessionPump with a real client store: a readiness
  refusal before Welcome is NotSent and the consumer count stays zero; after
  matching Welcome an Echo is served once. Separately leave a production pump
  unstarted with its command receiver alive: the actual local reply deadline
  returns AnswerLost. That control proves local timeout classification, with
  no network dispatch. Other encoding or uncertain-send failures are not
  injected by this journey and must not be called no-effect.

Wire absence is sampled within a bounded window after an operation-owned hold
or completion, never used to signal readiness. Restart checks retain the real
store; marker restoration is not part of this journey. The scripted holder
release remains registry/fence evidence, not writer-draining evidence.
Wrong builds include a global maximum, latest-arrival-wins, reading the
projection at check, replaying a new decision and Welcome before publication.

## Frame shapes — SO56

The frame-specific teaching now lives in [frame shapes](frame-shapes.md).
It owns the exact decoder-reported fields, socket controls, authorized
wrong-build/restoration procedure and native-versus-G3 limits.

## Copy containment — SO57

Run `bash scripts/verification/copy-containment.sh` after replication host,
socket loop, writer session calls, sender or copy codec changes.

The route promotes the adopted X7 native probe. Two real writers share a peer
and one SessionPump/WriterSessionCalls connection to FleetService. Both copies
first acknowledge their schema and a row. After service and pump stop and join,
only A's saved copy marker is damaged. B remains healthy. The service restarts
with the same receiver and storage. The observing transport holds reconnect,
so another socket cannot masquerade as containment.

A's next Piece must receive correlated replication CopyUnavailable with a
typed damaged reason, retain its identity and acknowledgement, and add no
effect. B must acknowledge on connection 1 and add one durable row and receipt.
After stopping service, restoring A's exact saved marker and reading it back,
a new session resends A's identical Piece. It applies once; an explicit
duplicate answers AlreadyApplied without adding another row or receipt.

The separate answer-loss control drops a real committed Applied response at
the observing transport. The sender must see AnswerLost and keep its durable
acknowledgement. A readback must show the committed row and receipt. Resending
the identical Piece after restart must receive AlreadyApplied, advance the
sender acknowledgement and leave the entire copy snapshot unchanged.

The registry control publishes an unheld import, stops the service, damages
only its copy marker, and submits a real claim. It waits on the operation's
committed copy-wait state through CopyFaultRequest::WaitHolderChange. The
claim must retain its reservation and chosen epoch, record a typed Damaged
reason with NoEffect reach, and keep the copy call pending. Independent file
readbacks inspect the pending step's reason and may_have_reached projection.
The driver holds registry drive, then kills and awaits its owned child before
reading the latest receiver reason. A graceful shutdown may legitimately make
a newer NotSent attempt and clear that latest-attempt diagnostic without
changing NoEffect history. Another restart before repair must preserve the
frozen receiver state. After exact marker
restoration and restart, the same attempt must grant the reserved epoch and
settle the original gate reservation. An independent fresh import is the
readiness control; an already-held final refusal replays across another
restart. Marker restoration stands in for repaired storage, not a reseed API.

The native C6 controls run at the declared immutable head:

- The actual copy object's test-fault feature loses a transaction reply after
  a real commit or rollback. A must receive correlated OutcomeUnknown with
  complete StoreBoundary detail; the sender retains its acknowledgement and
  gets AnswerLost. B must keep progressing on that same connection. File
  readbacks distinguish the real commit from rollback. A resends its original
  Piece after restart, receiving AlreadyApplied or one apply respectively;
  the explicit duplicate adds no effect.
- A separate byte-boundary control changes the real OutcomeUnknown reason
  kind to an unknown one. Decoding follows AnswerLost and the original Piece
  reconciles after restart. That deliberate wire fault does not prove the
  normal typed answer kept the socket open; the unmodified cases do.
- A stopped-owner readback first verifies a quiescent initial copy journal.
  The copy engine's drive is held while the actual host's post-apply-specific
  wake wrapper fails the committed apply's hint. The correlated Applied
  still returns, its follow-up remains owed, and startup completes it once.
  The driver kills and awaits the child with drive still held, then reads the
  owed operation ID and matches it to the recovered external effect. A second
  wake and durable copy readback must add neither effect nor apply.
- The closed reason codec round-trips complete payloads, discriminates only
  by code, and refuses unknown or unrelated codes and malformed unit payloads.
  Damaged details stay raw diagnostics even when they resemble JSON or another
  code. These pure codec cases do not independently establish a producer's
  rollback certainty.
- SeedFile's real begin, page and complete calls each wait on copy damage,
  then repeat the identical operation after repair. Writer commits after the
  held begin/page stay outside the pinned snapshot. The same generation,
  seed, page, position and content resume on one session. This drives the
  owning WriterSessionCalls boundary, not a complete InstallCoordinator
  recovery journey.

The move controls hold the actual acknowledgement or fence call before SQL,
damage its marker through the owning transaction, release the call and observe
its pending NoEffect journal. Freeze/kill/reopen retains the call, epoch and
reason; exact repair grants and replays the same attempt with one fence receipt.
The holder's Release is supplied at its socket boundary; it does not prove
real writer draining.

The script deliberately emits two separate-proof gaps: real-engine NoEffect
joins/reopen and closed-view controls, and the real workerd adapter/classifier
receipts. It does not run those suites or turn their receipts into native
journey passes. Check the focused durable-steps shared_conformance and lease
turso_copy_retry_tests cases, plus the existing workerd engine/copy drivers,
at the same immutable revision and report their counts separately. Passing the
native journey alone does not close those entries or central's G3 session gate.
Retain its raw gap counts alongside the separate observations; do not rewrite
the journey's result after running another suite.

For the separate SO57 receipts, use the declared PR head rather than inherited
producer logs. Run `durable-steps`' real `shared_conformance` test with `turso`,
and the native `fleet-sync-lease` lib test
`turso_copy_retry_tests::claim_ack_wait_and_fence_keep_pending_history_then_resume_the_same_attempt`.
The latter checks pending NoEffect history, reopen, closed-code refusal and the
same repaired attempt. Its workerd lease-view runtime is unobserved here and
owed at B2a/G3; do not claim that native execution is a Worker observation.

Use the owning Worker build/controller commands under W-C14 below for the
fresh engine/reopen receipts. For the production copy adapter/classifier,
generate raw Piece strings through the existing native fixture test, then use
the existing normal-migrations Worker and controller:

```bash
COPY_CERTAINTY_FIXTURE="$copy_fixture" cargo test --offline --locked --manifest-path tests/consumer/Cargo.toml --all-features --test native_copy_containment an_unknown_commit_reconciles_the_identical_piece_without_a_second_effect -- --exact --nocapture
cargo build --offline --locked --manifest-path tests/normal-migrations-worker/Cargo.toml --target wasm32-unknown-unknown --target-dir "$verification_worker_target"
"$verification_bindgen" --target bundler --no-typescript --out-dir tests/normal-migrations-worker/build --out-name index "$verification_worker_target/wasm32-unknown-unknown/debug/normal_migrations_worker.wasm"
cp tests/normal-migrations-worker/shim.mjs tests/normal-migrations-worker/build/shim.mjs
node tests/normal-migrations-worker/run-copy-certainty.mjs "$copy_fixture" "$copy_evidence"
```

Use the normal worktree build target, owned evidence paths and matching pinned
tools. Keep Piece JSON raw;
a JS parse can round its wide integers. All four controls must distinguish
real rollback from commit using SQLite row/receipt counts, call production
DurableObjectStore and classify_copy_store_error, then apply or reconcile the
same piece once. Keep controller results and native test counts separate.

A registry call must not
cache transient no-effect results as CallDone or end as Refused(Unavailable).
Run the real-receiver Turso claim controls:

```bash
cargo test --offline --locked -p fleet-sync-lease --all-features --lib turso_claim -- --nocapture
```

An unseeded status or a copy missing after status stays pending with
NoEffect/Uninitialized.
Registry reopen preserves the view; real seed repair then grants the same
operation, attempt, reservation, epoch floor and copy CallId, once. Retired and
moved copies stay terminal. Keep the separate settlement-debt control using a
seeded then retired copy and the absent-copy preview's Running/no-preview,
no-transfer, no-floor assertions. The test host must return shared retryable_copy
only after the actual receiver transaction commits. Administrative replacement
and seeding of a lost fixture is not production automatic reseeding.
The separate exact selectors are
`turso_workflow_failure_tests::refused_claim_with_unanswered_settle_reports_owed_from_the_wrapped_reserve_answer`
and `turso_epoch_proof_tests::absent_copy_and_floor_cannot_create_a_preview_or_manufacture_epoch_evidence`.
Keep final-refusal replay separate. Authoritative NotInitialized reseed remains
the design's explicit limit. Store-boundary injection does not prove real disk
failure. Workerd receiver/driver atomicity and the codec use the real apply
fixture. Producer classification counts there only when it calls production
DurableObjectStore and the real host's classifier, with both commit and
no-commit controls; a copied mapping is fixture policy, not proof. Otherwise
producer classification, like session containment, stays at central's G3
against the exact library revision. Implemented C slices do not establish PR C
readiness before those G3 receipts exist.

## Guarded read transactions — W-C12, focused API proof

The reference is Program Design W-C12, including its fail-fast maintenance
correction. This is a public Rust API contract on a real managed writer, not
a session journey. Do not launch the consumer or add a second SQL test harness.
At the caller's declared immutable C10 head, check and run the existing tests:

`cargo test -p fleet-sync-writer --locked --lib read_transaction -- --nocapture`

Run from the archived repository root with its pinned toolchain and retained
logs; inspect actual selection/counts. This command is a selector, not a claim
that an earlier run applies to the current head.

- `read_transaction_tests` pins related reads through
  `ReplicatedDatabase::reader().begin_read()`. A real writer commits between
  them: the transaction keeps its earlier view, while an independent reader
  sees the new row. Admission occurs while a write guard already owns the
  writer control lock, so a re-entrant lock would fail the bounded call.
- `read_transaction_guard_tests` sends write, transaction-control, schema,
  pragma, attach and stacked SQL through all four Executor entry points:
  fetch_many, fetch_optional, prepare_with and describe. Each must return the
  typed WriteOnReader refusal. Successful read controls exercise all four, so
  blanket rejection cannot pass.
- With a read transaction open, with_drained_file and move_aside must answer
  ActiveReaders promptly; they do not queue. Closing the read permits a fresh
  retry, and move-aside checks both source/destination state and the closed
  old pool.
- Explicit close, ordinary drop and cancellation of an unpolled close future
  each permit later real reads, publication and a new read transaction. The
  offered cancellation case is before its first poll; do not report a tested
  cancellation window inside an already-running rollback without additional
  evidence. Drop only schedules the existing pooled rollback cleanup; it is
  not proof of synchronous cleanup by itself.

The separate `a_once_polled_close_releases_the_snapshot_for_reads_and_publication`
control polls the actual driver close future once, then drops it and checks
fresh reads and closed-file publication. Run that existing test at its declared
supplemental head; it is absent from the original C10 head:

`cargo test -p fleet-sync-writer --locked --lib read_transaction_guard_tests::a_once_polled_close_releases_the_snapshot_for_reads_and_publication -- --exact --nocapture`

Report its literal Ready or Pending observation. Ready establishes synchronous first-poll rollback
completion; only a Pending observation exercises cancellation with rollback
still in progress. Neither the original unpolled case nor a Ready result can
claim that latter window.

Check that ReadTransaction exposes no raw connection, writable pool, Deref or
commit escape. Private representation/source checks complement the runtime
tests; they do not establish later consumer high-water/feed composition.
No hosted or board/central acceptance follows from this focused API proof.

## Typed upgrade refusals — W-C13, focused transport proof

The reference is the adopted Program Design W-C13 header amendment. At the
caller's declared immutable C11 head, check and run the existing real-path
tests from the archived repository root:

```bash
cargo test --manifest-path tests/consumer/Cargo.toml --locked --test native_upgrade_refusal -- --nocapture
cargo test -p fleet-sync-transport-websocket --locked --test upgrade_refusal -- --nocapture
```

Retain the target, commands, raw selection/counts and exits separately. These
selectors are not passing evidence before they run. Keep the producer's
tests-first baseline separate from the delivered-target observation.

The carrier is one `fleet-sync-auth-refusal` response header containing only
the existing AuthRefusal JSON Serde unit value. The header name and encoding
are implementation choices under the adopted typed-header contract.

- The real FleetService tests supply each typed authenticator refusal:
  Unknown, Expired, Revoked and WrongKind. The native transport must return
  that exact reason. A separate wire read checks the actual HTTP status and
  exactly one correctly encoded header. The controlled authenticator is an
  input; it does not establish a credential verifier's expiry or revocation
  policy.
- The real TCP transport tests write the refusal headers first and withhold
  a competing JSON body until `connect` returns. All four typed reasons must
  survive this split response without waiting for or decoding that body.
  Operation-owned release and joined server tasks replace timing assumptions.
- Missing, malformed, unrecognized, structurally wrong (including the alternate
  enum-object form `{"expired":null}`), trailing, combined or
  repeated header values must return Unknown. A bare 403 also returns Unknown;
  status alone cannot infer WrongKind. A non-authentication HTTP failure keeps
  its existing Unavailable classification even if it carries a valid header.

Check the server writes from its authenticator's typed answer and the client
accepts exactly one header on 401/403, with no body fallback. Mid-session
Closed/CredentialRejected frames keep their existing typed path. These source
checks complement the wire tests and do not establish board reqwest, central
ConnectionObject, hosted or release acceptance.

If moving an existing dependency from dev to normal dependencies changes a
standalone local-package lock edge, refresh only that edge offline, inspect
the exact diff, then rerun locked. Do not discard a lock refusal or silently
omit the native verification driver's Cargo.lock from scoped upkeep.

## Portable query freshness — W-C11, focused CLI/build proof

The reference is adopted Program Design W-C11: generator-owned inputs have
portable logical metadata keys, while Cargo watches use their actual physical
paths. Both generation and public verification use the same classification.
This is native generation and build-time consumer proof, without a session
consumer. Reuse the permanent real-path tests at the declared immutable head:

```bash
cargo test -p fleet-sync-query-gen --offline --locked --features native --lib --tests --target-dir "$verification_target" -- --nocapture
```

Run from the archived repository root, with `verification_target` naming the
normal worktree target configured above. The native feature is required; an empty
feature-gated target is not proof. Inspect each target's actual counts and the
external test's observations, without counting its subcases as extra tests.

- `external_freshness` must invoke the actual CLI to generate an outside-workspace
  Cargo consumer, then compile its generated module through a real build.rs
  calling public verify_manifest. Distinct query names share one SQL file as a
  positive coalescing control.
- First move only the library, then move only the consumer to a different
  depth. Both real Cargo builds must pass without regeneration; generated
  source and metadata bytes stay unchanged. Moving both together cannot
  substitute for these independent controls. The fixture copies source homes
  directly and works from an archive without .git.
- Actual emitted Cargo watches must exist and include the relocated generator,
  driver, lock/toolchain, consumer SQL and generated output. Absolute physical
  watch paths are expected; they do not belong in metadata keys.
- Seven fixture-only mutations cover SQL, migration, generated output,
  generator source, owned driver source, workspace lock and toolchain. Each
  real build must fail on its exact intended Stale key, rather than a compiler
  or fixture error. Restore the exact bytes and require a successful build.
  Converting every generator entry to a complete, valid old relative-path map
  must still be refused, without a legacy fallback.
- Native generation must refuse a consumer input or generated output colliding
  with `generator:workspace/Cargo.lock`, before overwriting artifacts. Keep the
  generated-output marker unchanged on refusal. Catalog controls additionally
  require same-canonical-input/same-origin coalescing, and refusal for identical
  bytes under distinct paths or origins; digest equality alone cannot pass.
- `generated_calls` checks committed freshness and executes generated calls
  against real Turso transactions, with rollback and committed-value controls.

Enumerate every library corpus at the declared head (nine at C9), including
both delivery corpora and the standalone consumer. Using the test command's
built CLI, run exactly one generation and one check per corpus in the ignored
archive, with `corpus_manifest` set to that corpus's manifest:

```bash
"$verification_target/debug/fleet-sync-query-gen" --manifest "$corpus_manifest"
"$verification_target/debug/fleet-sync-query-gen" --manifest "$corpus_manifest" --check
```

Compare with the pinned artifacts before generation. Regeneration must leave
their bytes unchanged. For this freshness-key cutover, producer text, query
descriptions, generated Rust and output digests must also match the parent
revision. Inspect all source-map keys
for absolute paths and require portable normalized generator keys. Retain
per-corpus commands, exits and hashes separately from Rust test counts. These
artifact-only writes do not authorize regeneration in a mutable live tree.

The external test owns a separate nested Cargo target and waits for its child
commands; confirm the parent and nested processes settled before slot return
or cleanup. This proof does not establish consumer service runtime, wasm,
workerd, board/central integration or hosted acceptance. Keep those receipts
and C6's separate-proof gaps distinct.

## Query-description receipts — W-C15

Use the adopted W-C15 contract and the permanent public build-gate matrix:

```bash
cargo test --offline --locked -p fleet-sync-query-gen --features native --test external_freshness metadata_integrity_cases::public_build_gate_binds_complete_ordered_descriptions_on_native_and_wasm -- --exact --nocapture
```

The selected test actually generates a consumer through the CLI and compiles
its emitted module on native and wasm32. Its build.rs calls public
verify_manifest on the host for each target; this does not execute queries in
wasm. Require the selected Rust test to run. Its Cargo subcases are not extra
Rust test passes.

- Metadata-only controls include Rust type (the acceptance control), SQL type,
  names, nullable, type source, and bind_slots, which is absent from the Rust
  output. SQL and generated Rust must remain unchanged during these edits.
- Query/column deletion and reordering, an empty query vector, and a missing
  or wrong digest must be refused. Check the required-field diagnostic for a
  missing digest and Stale on the metadata filename for the other mutants;
  an unrelated compiler failure cannot pass.
- Whitespace-only and object-key-order changes must pass independently. The
  key-order control must preserve parsed values while changing bytes.
- Existing SQL/output changes still fail on their owning Stale keys, and
  exact restored builds pass on both targets.

At C9b these are fourteen metadata mutants per target, four formatting positives
overall, source/output controls and recovery, within one Rust test. Inspect
actual observations rather than adding subcases to the suite count.

For all nine corpora, run the W-C11 artifact-only generation/check procedure
after the generator source is frozen. Independently recompute SHA-256 from the
typed serialization of the complete ordered query vector, preserving field
values and both query/column orders; do not hash original JSON formatting or
import the producer helper for this comparison. Each queries_digest must
match. Query vectors, producer text, source keys and generated Rust stay
unchanged for this cutover; only the changed generator source hashes and the
new receipt field differ. Committed artifact bytes must survive regeneration
unchanged.

Source-check one shared helper, a required field without default/legacy
fallback, and the same vector used by native rendering and metadata emission.
The digest is a generation receipt. It does not re-describe SQL at consumer
build time or independently prove the producer correct. Keep native/wasm
build-gate evidence separate from runtime, G3 and hosted observations.

## Shortened transaction scope views — W-C14

The reference is adopted Program Design W-C14. ReborrowScope is an optional
capability with three explicit implementations: TursoStore, TokioObject and
DurableObjectStore. AtomicStore's scope GAT and callback contract stay unchanged.
The view keeps the parent's transaction, journal, host and wake owner, while
exclusively borrowing the parent. Match every check to its owning runtime and
the caller's declared observation target.

At the immutable archive root, run the existing native controls:

```bash
cargo test --offline --locked -p durable-steps -p fleet-sync-tokio-runner --all-features reborrow -- --nocapture
```

Inspect the actual selected tests; zero-test filtered targets add no passes.
The real Turso helper requires the view to see the parent's uncommitted SQL
and existing admission identity, then the parent to see the view's SQL and new
admission before commit. Only committed business rows and journal admissions
survive the later rollback case, with the same host deadline. TokioObject's
real-file case separately checks its delegating view and parent commit/rollback.

For the declared workerd target, use the archive's pinned Node dependencies,
Worker build and existing controller. At C12 the wasm-bindgen CLI must be
0.2.129, matching the crate; verify its version rather than using another
executable on PATH. With owned `verification_worker_target` and the matching
`verification_bindgen` executable:

```bash
pnpm install --offline --frozen-lockfile
cargo build --offline --locked -p durable-steps-worker --target wasm32-unknown-unknown --target-dir "$verification_worker_target"
"$verification_bindgen" --version
"$verification_bindgen" --target bundler --no-typescript --out-dir tests/durable-steps/build --out-name index "$verification_worker_target/wasm32-unknown-unknown/debug/durable_steps_worker.wasm"
cp tests/durable-steps/shim.mjs tests/durable-steps/build/shim.mjs
node tests/durable-steps/run-workerd.mjs
```

The `/reborrow` response must contain all eight asserted observations:

- Real DO SQL and journal writes share parent commit and rollback.
- The test-support observer identifies the shared Arc/Mutex arbitration state,
  not a wrapper's address. A legitimate clone matches; a new AlarmOwner over
  the same storage differs. Capture the parent's identity before lending, then
  compare the view directly without using the parent during its loan.
- Four rollback controls cross callback/SQL error with absent/existing alarm.
  Each first checks its exact requested pre-state, then reads the actual changed
  durable alarm inside the transaction after the view wake. After the deliberate
  error, SQL is absent and the durable alarm equals its exact previous value.
  A SQL-only rollback or a wake that did nothing cannot satisfy these controls.
- Both legal view/external wake polling orders use that same observed owner
  and commit the earlier deadline. Do not alias the exclusively borrowed parent.

The controller also checks the existing 20 shared scenarios, eight alarm race
orders, alarm atomicity, NoEffect actual-runtime reopen and platform-alarm
restart. Report these as endpoint/runtime observations, separately from native
Rust test counts. The controller awaits readiness and disposes its runtimes;
retain raw JSON and artifact hashes before cleaning owned output.

Run the adopted one-off borrow probe outside the repository, with the actual
public trait as a dependency. Keep a view live after a second attempted mutable
parent borrow: rustc must reject that use with E0499 (or the applicable borrow
diagnostic), at that use. Remove only the illegal function and require a passing
control using the parent after the view ends, alongside an unchanged external
AtomicStore forwarding wrapper. Preserve both sources, commands, lock and raw
compiler results. This is type evidence; do not add a permanent compile-fail
test or treat it as runtime proof.

Source-check the exact optional signature, three faithful implementations and
test-support-only identity observers. No new connection, transaction, lock,
AlarmOwner, copied transaction state, unsafe or Clone escape belongs in reborrow.
Consumer-composed frame/Engine publication, registration/Welcome rollback,
recovered journal identity and native post-commit timer activation belong to
B2b. Hosted Cloudflare and C6's adapter/classifier and lease-view controls need
their own observations; the shared engine controller does not run those paths.
