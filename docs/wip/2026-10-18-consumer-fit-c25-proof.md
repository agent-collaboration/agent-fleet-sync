# C25: typed delivery rollback

Scope: W-C27, preserving delivery refusals and custody corruption through the
shared `AtomicStore` boundary. The native mailbox answer table moves into the
delivery library; health remains a host responsibility. Status-owner errors,
the pre-transaction health gate and post-commit wake failures remain separate.

Base: `f9674f6`. Plan r4 remains ready with terminal `pr-ready-unmerged` and
separate PRs. The successor commission explicitly orders C25 before C24,
superseding the older sequence in W-C27. The existing untracked writer migration
test is outside C25 and remains untouched. No repository AGENTS.md exists;
the supplied repository instructions and explicit commission govern.

Admission: ready. The build/runtime slot was explicitly returned by the paired
Verifier. No separate C execution thread exists; the Lead explicitly selected
this public proof document as the trace and direct declarations as handback.

## Proof map before changes

| Obligation | Observation | Wrong behavior caught |
| --- | --- | --- |
| Refused/Corrupt survive rollback | Real Turso through `AtomicStore::transaction`, after a custody mutation | Backend erases the typed failure; partial custody, sequence or publication commits |
| Strict marker validation | Exported encoder/classifier with malformed, foreign and lookalike controls | An uncertain boundary becomes a refusal or quarantine |
| Operation-aware mailbox disposition | Submit, Report and other mailbox operations | A refusal uses the wrong answer; unsupported failures quarantine |
| Native parity and containment | Unchanged native delivery suites through FleetService | Answer mapping or mailbox isolation differs from the library path |
| Quality and portability | fmt, touched-package clippy/native tests, delivery wasm check | Invalid Rust or runtime dependency reaches the portable library |

The project verification skill has no C25 journey. Bounded product tests and
the existing native consumer suites are the available real paths; its skill
and drivers are outside this implementation grant. Durable Object failure
precedence and composed central behavior remain unverified here.

## Checkpoint

Sources inspected: W-C27 and W-C18/C15b, delivery contracts and mailbox tests,
the native delivery request/owner paths, and the receiver codec. No stand-ins.
Tests will first observe the existing Backend bridge losing typed outcomes,
then use the delivery-owned codec. Disk check: 134 GiB available.

## Observations

The corrected pre-change regression ran two tests: 0 passed, 2 failed, exit 101.
Both failures were the intended erased type: `TooLarge(Sender)` and
`Corrupt(mail custody state)` became Backend after the transaction rolled back.
The initial corruption fixture instead hit malformed JSON in SQL; that initial
failure is a fixture failure, not evidence of a typed corruption. After the
encoder change, both regressions passed. The final crate run includes all five
rollback cases and totals 66 passed, 0 failed, 0 ignored, exit 0. Service and
documentation targets with no tests contribute no behavior evidence.

| New case | Oracle and real boundary | Observed wrong behavior or proof limit |
| --- | --- | --- |
| Refusal after custody change | W-C27; actual Mailbox writes and publication admission inside Turso AtomicStore | Pre-change Backend red; fixed refusal, empty custody/publications/journal and sequence restarts at 1 |
| Corrupt row after custody change | W-C27; actual missing-payload custody validation and rollback | Pre-change Backend red; fixed owned field and health signal identifies only the affected mailbox; sequence restarts at 1 |
| Failed rollback | W-C27; real transaction ended before the store's required rollback | Final Backend stays unhandled/unknown; no refusal or health signal; no pre-change red for this new control |
| Malformed/lookalike markers | W-C27; real Serde and exported classifier | Reserved payload corruption, unknown fields/enums, empty/control-character fields, foreign marker and Backend/Corrupt lookalikes stay unknown; no pre-change red for this new API |
| Operation disposition | W-C27's native table; exported pure disposition | Submit and Report retain their refusal; other operations and infrastructure failures stay unhandled; all mailbox operations carry only their mailbox's corruption signal; no pre-change red for this new API |
| Native parity | W-C27; actual FleetService/WebSocket responses compared with final real-Turso AtomicStore failures | Submit, Report and corrupt-custody answers match exactly; no fixture copies the mapping; no pre-change red for this new comparison |
| Status owner | W-C27 exclusion; real stopped-file mutation, service reopen and native socket | Typed transport closure on status corruption; mailbox Query stays healthy. Initial live-file lock and subsequent wrong error-text assertion were fixture/oracle failures, corrected using the typed transport error |

The selected native delivery suites total 20 passed, 0 failed, 0 ignored across
the completed suite run and its remaining selectors. The first aggregate stopped
at the status fixture failure after 15 passes; remaining selectors ran after
that fixture correction. Existing native delivery tests were preserved.

Local commands (logs under `tmp/`):

- `cargo test -j 4 -p fleet-sync-delivery --test delivery_rollback`
- `cargo test -j 4 -p fleet-sync-delivery -p fleet-sync-service` (66/0/0, exit 0)
- `cargo clippy -j 4 -p fleet-sync-delivery -p fleet-sync-service --all-targets -- -D warnings` (exit 0)
- `cargo fmt --all --check` (exit 0)
- Consumer manifest formatting check (exit 0).
- Consumer native delivery selectors: containment, expiry, handshake,
  initialization, journey, relay restart, service, status owner, storage retry
  and unavailable (20/0/0, final selectors exit 0).

The native service retains its existing rule-transaction wrapper and storage
fault injection. Only its final error classification and mailbox disposition
move to the shared library. Its health gate, status-owner early return and
post-commit wake path retain their owners. This corrected reversible drift from
an initial switch to the native AtomicStore wrapper, which would have bypassed
the existing storage-fault probes.

Rust inventory: no production casts, panic paths, dependency additions or
handwritten refusal shapes. The private corruption payload owns its serialized
field. Catch-all error arms deliberately keep all other DeliveryError variants
on the infrastructure path. Test SQL uses existing named-column inspection
conventions; no product SQL, migration or checked-query corpus changed.

Setup recovery: a cached generator artifact referenced a released source
archive. Its build-script IO failure is retained separately; rebuilding only
the normal-target query-generator package restored the unchanged freshness gate.
Pinned upstream template diagnostics also appear in Cargo output; command exit
codes and the actual discovered test counts above are the receipts.

Merge authority: readiness-only, from the commission's explicit no-ready/no-merge
boundary. No merge question is needed. PR stays draft. Risk: a wrongly decoded
marker could quarantine custody or manufacture refusal; strict negative controls
and real rollback/native parity tests cover that risk at the native boundary.
Durable Object failure precedence and central composition remain unverified.
Independent Verifier observation and CI for the new commit are still pending.

Final local gates: consumer all-targets Clippy with test-support exits 0;
`cargo check -j 4 --target wasm32-unknown-unknown -p fleet-sync-delivery` exits 0.
The Lead inspected the exact ten staged paths and passed the public-safety gate.
The C24 migration test remains untracked and unchanged. Next: commit these ten
paths, push, watch the exact-head CI, then declare the target to the Lead and
paired Verifier. No acceptance or deployed-runtime claim is made.
