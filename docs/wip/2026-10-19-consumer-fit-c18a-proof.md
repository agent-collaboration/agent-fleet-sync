# C18a: current replicated catalogs and schema admission

Authority: canonical plan r4 C18a/C18b split and program design §13a W-C21. Base: integrated C24 commit 13816305c3af7a071e1bd0002ab6b21a0460493a. This slice covers current catalogs and the 256 KiB schema bound. Complete page/part byte budgets remain C18b.

## Behavior and ownership

The pure migration AST inspection and ownership classifier now live in `fleet-sync-protocol`, behind `schema_catalog`. Native writer SQL access, transactions, read/write/maintenance guards and error adaptation stay in the writer. The four existing writer ownership test bodies are unchanged; the two AST inspection cases moved with their byte-identical source module.

Writer and copy snapshots use one current-catalog selector: tables and indexes by physical owning table, views through migration ownership checks, reserved objects and triggers excluded, then deterministic table/index/view order. Copy membership includes views, but only physical member tables become row sources. Retained schema history remains stored and has no aggregate-history quota; it is no longer replayed into copy page headers.

The shared bound measures the complete JSON array of current DDL, including escaping and delimiters, without allocating another encoded catalog. It admits exactly 256 KiB and refuses a larger array with its encoded size. Writer migration admission checks inside its owned transaction before schema-step history and capture can commit. Bootstrap conversion uses the same bound inside its publication transaction. Page producers also return a local typed `SnapshotSchema` refusal for an imported oversized catalog. No origin identity is invented for catalog entries.

Only the receiver checked-query corpus changed: its catalog read now includes the owning table and sees the catalog needed by the shared selector; its unused historical-header read was removed. Generation and the subsequent strict `--check` both pass. Stored migrations, dependency manifests, lockfiles, wire shapes and digest algorithms are unchanged.

## Real behavior and sensitivity

The two primary regressions were authored before product changes at the base:

- A real copy with more than 256 KiB of retired schema history emitted 27 historical statements instead of three current definitions: 0 passed / 1 failed, exit 101.
- Public writer migration admitted a change leaving a catalog above 256 KiB: 0/1, exit 101. Its final assertion also requires the typed encoded size.

Seven new permanent cases now pass. Actual Turso fixtures cover small current headers after large histories; a table, explicit index and view alongside real writer-local and hook-local tables/indexes and bookkeeping; and staging, verifying and publishing separate stores from writer and copy pages. The seeded stores have identical headers, rows, manifests and canonical fingerprints, checked against independent literal DDL and row values. Pure selection coverage also rejects private/transitively private views, cycles, reserved objects and triggers, and distinguishes index names from their physical owners.

The writer refusal compares durable state images of the catalog, SQLx history, schema-step history, capture and writer record before and after public migration. The bootstrap case exercises the actual conversion transaction directly, so prefix preflight cannot mask its gate. It starts with empty consumer SQLx history: conversion must attempt to record the prefix and then roll back on the oversized imported catalog. The imported-copy case uses escaped SQL whose raw text is below the cap but whose JSON array exceeds it; both page and manifest producers return the exact typed size.

Four compiled wrong builds fail unchanged behavioral assertions, each 0/1, exit 101:

- Permissive shared owner selection exposes the real writer-local table and index.
- A copy-only loose selector, with the writer correct, selects the real hook-local table and index; public copy reading fails its row-source membership check.
- Omitting migration admission admits the oversized catalog.
- Disabling the aggregate bound lets bootstrap conversion return `Ok(())`.

The last control was repeated against the stronger empty-history bootstrap fixture. Its fixed case passes 1/0 and its wrong build fails 0/1 with explicit `Ok(())`. Product homes were restored byte-exact; test hashes remained unchanged across each control. The other five new cases have no pre-feature runtime red; their positive runs and the relevant wrong-build failures are the evidence.

## Validation and limits

The restored complete suites pass **320 / 0 / 0, exit 0**: protocol 4 unit and 56 integration, receiver 102, runner 31, writer 88 unit and 39 integration cases. Raw output has 325 passes because five recursive child summaries are already represented in their parent cases. The new cases repeat inside this total and are counted once.

Commands run from the repository root, using the normal target, a fresh disk-floor check and four build jobs:

```sh
cargo test -p fleet-sync-protocol -p fleet-sync-receiver -p fleet-sync-writer -p fleet-sync-tokio-runner --all-features --locked -j4 -- --nocapture --test-threads=4
cargo clippy -p fleet-sync-protocol -p fleet-sync-receiver -p fleet-sync-writer -p fleet-sync-tokio-runner --all-targets --all-features --locked -j4 -- -D warnings
cargo check --target wasm32-unknown-unknown -p fleet-sync-protocol -p fleet-sync-receiver --locked -j4
cargo fmt -p fleet-sync-protocol -p fleet-sync-receiver -p fleet-sync-writer -p fleet-sync-tokio-runner -- --check
cargo run -p fleet-sync-query-gen --features native --locked -j4 -- --manifest crates/fleet-sync-receiver/checked-queries.json --check
```

All exit 0. The full native suite was refreshed after the final fixture strengthening and a Clippy correction removing an unnecessary error clone. Formatting, source comparison and diff checks pass. Exact-head CI, paired observation and acceptance are pending at declaration.

This is native SDK/SQLx behavior and Wasm compilation evidence. It does not establish Durable Object staging, session/HTTP delivery, hosted behavior, crash recovery or the complete-unit bounds still assigned to C18b and G3/G4. The bootstrap rollback case covers the conversion transaction directly; existing public bootstrap journeys also pass in the full suite. A selector defect could omit replicated DDL or expose private DDL; the shared existing ownership policy, literal-catalog equivalence and wrong builds constrain that risk. Consumers exceeding the current-schema cap are deliberately refused.
