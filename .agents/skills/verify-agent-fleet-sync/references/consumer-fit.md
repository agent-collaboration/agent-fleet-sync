# General consumer and scope proof

Use [build setup](build-and-evidence.md) for the declared source and normal
worktree target. Configure the native signing runner where the host requires it.

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

Enumerate the remaining generator corpus at the declared head:
`crates/fleet-sync-query-gen/checked-test-queries.json`. Using the test command's
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
workerd, board/central integration or hosted acceptance. Keep those receipts distinct.

## Query-description receipts — W-C15

Use the adopted W-C15 contract and the permanent public build-gate matrix:

```bash
cargo test --offline --locked -p fleet-sync-query-gen --features native --test integration external_freshness::metadata_integrity_cases::public_build_gate_binds_complete_ordered_descriptions_on_native_and_wasm -- --exact --nocapture
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

For the remaining corpus, run the W-C11 artifact-only generation/check procedure
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
capability with retained TursoStore and DurableObjectStore implementations.
AtomicStore's scope GAT and callback contract stay unchanged.
The view keeps the parent's transaction, journal, host and wake owner, while
exclusively borrowing the parent. Match every check to its owning runtime and
the caller's declared observation target.

At the immutable archive root, run the existing native controls:

```bash
cargo test --offline --locked -p durable-steps --all-features reborrow -- --nocapture
```

Inspect the actual selected tests; zero-test filtered targets add no passes.
The real Turso helper requires the view to see the parent's uncommitted SQL
and existing admission identity, then the parent to see the view's SQL and new
admission before commit. Only committed business rows and journal admissions
survive the later rollback case, with the same host deadline.

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

The controller also checks the shared conformance names read from
`crates/durable-steps/tests/conformance/mod.rs`, eight alarm race
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

Source-check the exact optional signature, both retained implementations and
test-support-only identity observers. No new connection, transaction, lock,
AlarmOwner, copied transaction state, unsafe or Clone escape belongs in reborrow.
Hosted Cloudflare, fleet receiver/classifier and session behavior require
separate observations in codex-router; this general controller does not run
those paths.
