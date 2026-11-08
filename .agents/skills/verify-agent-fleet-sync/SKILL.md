---
name: verify-agent-fleet-sync
description: "Use when verifying durable-steps native or workerd behavior, journal migration SQL, or fleet-sync-query-gen freshness in this repository. Not for delivery acceptance."
---

# Verify Agent Fleet Sync

Keep proof at the runtime that owns the behavior. Native journal tests use
Turso; Durable Object store tests use real workerd through Miniflare; query
freshness tests generate and compile real external Cargo consumers.

1. Establish the declared source revision, configuration and client. Use
   [build setup](references/build-and-evidence.md) for the exclusive build slot,
   normal worktree target and retained evidence. Run Cargo builds sequentially.
2. Select the remaining general-engine surface:
   - For journal migration, key constraints, native recovery or public SQL
     corpora, load [journal proof](references/engine-migrators.md) and return
     the selected commands, runtime and required assertions.
   - For query freshness, query-description receipts or shortened transaction
     scope views, load [consumer fit](references/consumer-fit.md) and return
     the selected tests and runtime observations.
3. Run the selected permanent tests or controllers against the declared target.
   Inspect actual selection, raw pass/fail counts and exits; a zero-test
   selection is missing coverage. Keep controller observations separate from
   Rust test counts, and nested Cargo controls separate from their parent test.
4. Return each claim as supported, unsupported or unknown, with its source,
   command, evidence and unverified interactions. Confirm fixture children and
   Miniflare runtimes settled before cleanup under the caller's authority.

Native and workerd evidence remain separate. Neither establishes hosted
Cloudflare, integrated Router or release acceptance. Fleet session, delivery,
registry and replication recipes belong in codex-router.
