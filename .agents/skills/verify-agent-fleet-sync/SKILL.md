---
name: verify-agent-fleet-sync
description: Use when verifying agent-fleet-sync native holder refusals, target schema support, malformed session frames, replication copy containment, guarded read transactions, typed upgrade refusals, or checked-query freshness after protocol, registry, session, receiver, sender, writer, transport, or query-generator changes.
---

# Verify Agent Fleet Sync

Drive the real native consumer and inspect its protocol responses and durable
state. Each journey preserves evidence and distinguishes contract failures from
unavailable controls. These scripts cover native sessions; workerd apply and
Cloudflare sessions have separate real hosts.
Guarded read transactions use focused tests through the real writer API,
upgrade refusals use real service and TCP transport tests, and query freshness
uses the native generator and real external Cargo consumers.

1. Establish the declared source revision, configuration and client. Work from
   the repository root and coordinate an exclusive build window. The launcher
   builds the existing `tests/consumer` executable and the native journey driver
   using the repository's pinned Rust toolchain and Cargo configuration. It runs
   `df -h .` before each build and retains the output.
   For focused API, transport or generator proof, use the reference's selection
   on the archived target. Do not build an unrelated session journey.
2. MUST load `references/consumer-fit.md` and return the selected journey's
   adopted outcome, required cases, unavailable controls and target.
3. For a session journey, run the selected script with Bash. `--help` describes
   its inputs. The default evidence location is a new directory under
   `scripts/verification/.runs/`;
   `--evidence-dir DIR` selects a nonexistent directory. The service owns its
   ephemeral loopback port and isolated Turso files. Readiness is its `READY`
   message with the matching child PID, then an authenticated matching Welcome.
4. Read the JSON observations or focused test's raw counts, build logs, source
   state and executable hashes where applicable.
   A session response must carry its request's frame identity. Replay and
   durability claims additionally require stored-state readbacks. Return each
   case as supported, unsupported or unknown at the build that ran it.
5. Confirm that owned consumer, pump and test server tasks stopped and joined.
   Retain the evidence, including fixture databases, until its consumer no
   longer needs it. Clean only this run's residue under the caller's cleanup
   authority.

| Journey | Command | When to run |
|---|---|---|
| holder-refusals | `bash scripts/verification/holder-refusals.sh` | Holder refusal payloads or recorded outcome replay change (SO52). |
| target-capability | `bash scripts/verification/target-capability.sh` | Schema declarations, attach ordering or holder admission change (SO53). |
| frame-shapes | `bash scripts/verification/frame-shapes.sh` | Frame decoding or native socket failure handling change (SO56). |
| copy-containment | `bash scripts/verification/copy-containment.sh` | Copy classification, sender retries or session containment change (SO57). |
| guarded reads (focused API) | See the selected writer tests in the reference. | Reader snapshot, guard or publication exclusion changes (W-C12). |
| upgrade refusals (focused transport) | See the selected service and transport tests in the reference. | HTTP authentication refusal encoding or decoding changes (W-C13). |
| query freshness (focused CLI/build) | See the generator tests and corpus commands in the reference. | Freshness keys, source identity, Cargo watches or generation change (W-C11). |

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
keeps separate-proof gaps for engine/closed-view and workerd receipts; native
C6 controls do not close those entries. Native proof
does not establish hosted, Cloudflare edge or release
acceptance. Workerd apply certainty belongs to `tests/normal-migrations-worker`;
Cloudflare session behavior belongs to central's real runtime.
