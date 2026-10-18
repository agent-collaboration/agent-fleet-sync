---
name: verify-agent-fleet-sync
description: "Use when verifying agent-fleet-sync stored identity integrity, native holder refusals, target schema support, malformed session frames, replication copy containment, guarded read transactions, transaction scope views, typed upgrade refusals, checked-query freshness, or public engine migrators and their migration corpora after protocol, delivery, registry, session, receiver, sender, writer, store, transport, or query-generator changes. Not for product implementation or delivery acceptance."
---

# Verify Agent Fleet Sync

Drive the real native consumer and inspect its protocol responses and durable
state. Each journey preserves evidence and distinguishes contract failures from
unavailable controls. These scripts cover native sessions; workerd apply and
Cloudflare sessions have separate real hosts.
Guarded read transactions use focused tests through the real writer API,
upgrade refusals use real service and TCP transport tests, and query freshness
uses the native generator and real external Cargo consumers.
Transaction scope views use their real Turso, TokioObject and Durable Object
owners on the caller's declared targets.
Public engine migrators use their exported corpora and real Turso storage rules
in the host-owned transaction; their service callers remain a separate check.

1. Establish the declared source revision, configuration and client. Work from
   the pinned source archive's root and coordinate an exclusive build window.
   Use [the build setup](references/build-and-evidence.md): the assigned worktree's
   normal target only, with native binaries under `target/debug`. Never make
   per-run or scratch target copies. Builds remain ordered by the exclusive slot.
   Source pinning and recorded binary hashes establish each run's provenance;
   a fresh target directory is not an independence requirement. The launcher
   builds the existing `tests/consumer` executable and the native journey driver
   using the repository's pinned Rust toolchain and Cargo configuration. It runs
   `df -h .` before each build and retains the output.
   For focused API, transport or generator proof, use the reference's selection
   on the archived target. Do not build an unrelated session journey.
2. Select the teaching for every surface in the requested observation:
   - IF frame decoding, envelope handling or malformed-body socket recovery is
     in scope, load `references/frame-shapes.md` and return the adopted outcome,
     selected socket cases, unavailable controls and target.
   - IF public engine migrators, their migration corpora or their host migration
     routing is in scope, load `references/engine-migrators.md` and return the
     selected corpus/rule cases, negative boundaries, unavailable controls and
     target.
   - IF stored identities, holder refusals, schema support, copy containment,
     guarded reads, transaction scope views, upgrade refusals or query freshness
     is in scope, load `references/consumer-fit.md` and return the selected
     journey's adopted outcome, required cases, unavailable controls and target.
3. For a session journey, run the selected script with Bash. `--help` describes
   its inputs. The default evidence location is a new directory under
   `scripts/verification/.runs/`;
   `--evidence-dir DIR` selects a nonexistent directory. The service owns its
   ephemeral loopback port and isolated Turso files. Readiness is its `READY`
   message with the matching child PID, then an authenticated matching Welcome.
4. Read the JSON/controller observations or focused test's raw counts, build
   logs, source state and executable hashes where applicable.
   A session response must carry its request's frame identity. Replay and
   durability claims additionally require stored-state readbacks. Return each
   case as supported, unsupported or unknown at the build that ran it.
5. Confirm that owned consumer, pump, test server and workerd runtime tasks
   stopped and joined or disposed. Retain the emitted evidence and the session
   journeys' saved fixture databases until their consumer no longer needs them.
   Focused tests and controllers own their fixture cleanup. Clean only this
   run's residue under the caller's cleanup authority. Run `cargo clean` on the
   normal worktree target after observation, before returning the build slot.
   A completed run keeps its evidence and no per-run Cargo target directory.

| Journey | Command | When to run |
|---|---|---|
| stored cancel identities (focused receive) | See the real inbox tests in the reference. | Stored cancellation identity decoding changes (SO51). |
| holder-refusals | `bash scripts/verification/holder-refusals.sh` | Holder refusal payloads or recorded outcome replay change (SO52). |
| target-capability | `bash scripts/verification/target-capability.sh` | Schema declarations, attach ordering or holder admission change (SO53). |
| frame-shapes | See the focused socket tests and basic script in `references/frame-shapes.md`. | Frame decoding or native socket failure handling change (SO56). |
| copy-containment | `bash scripts/verification/copy-containment.sh` | Copy classification, sender retries or session containment change (SO57). |
| guarded reads (focused API) | See the selected writer tests in the reference. | Reader snapshot, guard or publication exclusion changes (W-C12). |
| upgrade refusals (focused transport) | See the selected service and transport tests in the reference. | HTTP authentication refusal encoding or decoding changes (W-C13). |
| query freshness (focused CLI/build) | See the generator tests, description-integrity matrix and corpus commands in the reference. | Freshness keys, query-description receipts, source identity, Cargo watches or generation change (W-C11, W-C15). |
| scope views (focused runtimes) | See the native, workerd and external type controls in the reference. | ReborrowScope or an owning store's shortened view changes (W-C14). |
| engine migrators (focused library/service) | See the three storage-rule selectors and native consumer suite in `references/engine-migrators.md`. | Public engine migration corpora or host migration routing change (W-C17). |

For a session journey, the read-only health check is authenticated Hello/Welcome
on the owned service.
`--no-build` reuses binaries only when their provenance has already been
established for the declared target.

Session scripts use these exits: 0 means every implemented obligation ran with
no missing case; 1 means a contract mismatch; 2 means setup or harness failure;
3 means required coverage is missing. An exit 1 may also contain unknown cases;
read the summary. Missing coverage is never a pass.

SO52 combines the native AlreadyHeld/Pending journey with the product test's
Failed pass-through proof; no real writer produces Failed. SO53 uses the real
native publication and admission controls described in the reference. SO57
keeps engine/closed-view and workerd receipts separate from the native journey's
counts. Close those coverage entries only with their named fresh observations.
Native proof
does not establish hosted, Cloudflare edge or release
acceptance. Workerd apply certainty belongs to `tests/normal-migrations-worker`;
Cloudflare session behavior belongs to central's real runtime.
