# C26: retained supersession inspection

This records the original C26 target. The amended semantic checks and their fresh proof are tracked in [C26b](2026-10-19-consumer-fit-c26b-proof.md); the original observations do not establish those added cases.

Scope: expose rejected tails and persisted authority through the native retained-file reader, preserving registration-outcome meaning. C24 remains parked and excluded. The implementation begins at `d439dffa`.

The commission names program design section 13a W-C28 and the r4 plan C26 row as the expected-outcome sources. The retained reader must identify supersession after restart, including an empty tail, preserve bytes and modification times, and reject corrupt stored values. Its registration result remains independent of current authority.

## Preflight and proof design

The branch was clean after the explicitly requested local parking commit. Cargo is 1.98.1; disk availability was 116 GiB; the existing PR head matches the local base. Builds use the worktree's normal target directory, four jobs, and the shared slot granted by the Lead. The writer is native-only, so its slice has no wasm build gate. Native writer tests, formatter and touched-package Clippy are required; remote CI follows a scoped push.

The project verification skill has no retained-authority journey. Product proof will use its existing real-Turso test and process-restart fixtures through the public retained-file API; no verification driver changes are planned. Cases: real fence and sender observation with a rejected tail, empty-tail supersession, a live grant without supersession, retired and rejected registration observations, corrupt tails/authority, unchanged file bytes and times, and reader write refusal. The negative control must distinguish original Ready registration from current supersession. Expected tails and epochs come from the fence fixture and committed write positions.

## Source gap

The real sender fence path persists a StaleEpoch renewal refusal, and a replayed successful renewal can also expose a higher current epoch. W-C28 lists only NotWriter, Retired and RegistrationRejected projections. The mapping of those two supersession observations into NotWriter was sent to the Lead before implementation. No product edits or build have occurred.

## Checkpoint

C26 is in source inspection. Its observations are unknown until the planned tests run. C23 and both C18 halves remain unstarted. No stand-ins are in use.

## Settled model and test sensitivity

The Lead amended W-C28: `Superseded { current, ended_at }` preserves the fence cutoff; NotWriter and a higher-epoch current renewal are also supersession. A wrong-database renewal keeps the existing LeaseMismatch refusal. The implementation factors the existing newer-epoch recognition into one helper used by restoration and passive inspection. No stored format or decoder changes.

Test runs, in order:

1. New API scaffolding deliberately derived authority from registration, the first required wrong-build control. The initial run had two passing tests, one valid projection failure and one fixture setup failure. The sender fixture had granted only its in-memory holder; the test now restores that fixture's provisional holder before driving the public persisted registration path. The setup failure is not counted as behavioral sensitivity.
2. With that fixture corrected, the registration-derived control produced two passes and two failures, exit 101. The NotWriter projection and fenced process restart both rejected missing supersession behind a Ready registration. This is a deliberate wrong-build control, not a claim that the previously absent API failed at runtime.
3. The corrected implementation passed all four focused tests, exit 0. The restart test also ran two child-process cases, each 1/1 passing, for a tail and an empty cutoff. Child cases are reported separately rather than added to the four parent tests.
4. The restart assertions were strengthened to compare the exact cutoff and complete tails passed from the parent fence/sender fixture. Deliberately dropping StaleEpoch then failed the restart case, exit 101: expected Superseded with the exact fence cutoff, observed None. This second control is sensitivity after implementation. The deliberate break was removed before quality checks.

`cargo fmt --all -- --check` passed, exit 0. Full touched-package checks are running. Raw local logs are retained under the ignored `tmp/` directory with the `c26-` prefix; they are not public artifacts.

## Final local evidence

- `cargo fmt --all -- --check`: exit 0.
- `cargo clippy -p fleet-sync-writer --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0; zero Clippy warnings.
- `cargo test -p fleet-sync-writer --all-features --locked -j 4`: exit 0; 113 parent tests passed across 9 binaries, zero failed, zero ignored. The 77 library tests include the four new authority tests and the existing corrupt-authority and wrong-database controls. Restart children are not double-counted.
- `git diff --check`: exit 0.
- No wasm gate applies to the native writer crate. No SQL or migrations changed, so existing checked-query metadata is reused unchanged.

Every W-C28 native obligation is supported through real Turso and the public retained opener: fenced tail and exact cutoff after restart, empty tail, NotWriter with known or unknown epoch, higher-current renewed observation, live grant, retired and rejected observations, read-only guard, unchanged directory/file bytes and times, corrupt tail and authority refusals. The original registration result remains Ready in the supersession cases. The tests use the established real receiver fence/sender fixture; the empty-tail case records the registry's StaleEpoch observation after the actual fence.

Rust standards pass: the public authority is an enum with its approved payloads; existing decoders validate persisted input; production code adds no casts, unwraps or panic paths. The existing in-memory fixture grant is reset only in new test setup before persisting registration through the real public writer API. Supersession restoration retains its existing newer-epoch predicate, shared with inspection. No compatibility lane, new table, writable retained handle or stand-in was added.

Delivery is limited to the existing draft PR. The commit and its CI receipt are declared directly to the Lead and paired Verifier. Independent observation and board recovery integration remain separate from these product-test results; this document does not claim them or whole-PR readiness. C23, C18a and C18b remain subsequent assigned slices.
