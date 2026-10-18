---
name: verify-agent-fleet-sync
description: Use when verifying agent-fleet-sync native holder refusals, target schema support, malformed session frames, or replication copy containment after changes to its protocol, registry, session, receiver, or sender.
---

# Verify Agent Fleet Sync

Drive the real native consumer and inspect its protocol responses and durable
state. Each journey preserves evidence and distinguishes contract failures from
unavailable controls. These scripts cover native sessions; workerd apply and
Cloudflare sessions have separate real hosts.

1. Establish the declared source revision, configuration and client. Work from
   the repository root and coordinate an exclusive build window. The launcher
   builds the existing `tests/consumer` executable and the native journey driver
   using the repository's pinned Rust toolchain and Cargo configuration. It runs
   `df -h .` before each build and retains the output.
2. MUST load `references/consumer-fit.md` and return the selected journey's
   adopted outcome, required cases, unavailable controls and target.
3. Run the selected script with Bash. `--help` describes its inputs. The default
   evidence location is a new directory under `scripts/verification/.runs/`;
   `--evidence-dir DIR` selects a nonexistent directory. The service owns its
   ephemeral loopback port and isolated Turso files. Readiness is its `READY`
   message with the matching child PID, then an authenticated matching Welcome.
4. Read the JSON observations, build logs, source state and executable hashes.
   A response must carry its request's frame identity. Replay and durability
   claims additionally require stored-state readbacks. Return each case as
   supported, unsupported or unknown at the build that ran it.
5. Confirm that owned consumer and pump tasks stopped and joined. Retain the
   evidence, including fixture databases, until its consumer no longer needs
   it. Clean only this run's residue under the caller's cleanup authority.

| Journey | Command | When to run |
|---|---|---|
| holder-refusals | `bash scripts/verification/holder-refusals.sh` | Holder refusal payloads or recorded outcome replay change (SO52). |
| target-capability | `bash scripts/verification/target-capability.sh` | Schema declarations, attach ordering or holder admission change (SO53). |
| frame-shapes | `bash scripts/verification/frame-shapes.sh` | Frame decoding or native socket failure handling change (SO56). |
| copy-containment | `bash scripts/verification/copy-containment.sh` | Copy classification, sender retries or session containment change (SO57). |

The read-only health check is authenticated Hello/Welcome on the owned service.
`--no-build` reuses binaries only when their provenance has already been
established for the declared target.

Exit 0 means every implemented obligation ran with no missing case. Exit 1
means a contract mismatch; exit 2 means setup or harness failure; exit 3 means
required coverage is missing. An exit 1 may also contain unknown cases; read
the summary. Missing coverage is never a pass.

SO52 combines the native AlreadyHeld/Pending journey with the product test's
Failed pass-through proof; no real writer produces Failed. SO53 uses the real
native publication and admission controls described in the reference. SO57
remains incomplete while its missing cases are reported; connect C6 controls
at its declared head before closing those cases. Native proof
does not establish hosted, Cloudflare edge or release
acceptance. Workerd apply certainty belongs to `tests/normal-migrations-worker`;
Cloudflare session behavior belongs to central's real runtime.
