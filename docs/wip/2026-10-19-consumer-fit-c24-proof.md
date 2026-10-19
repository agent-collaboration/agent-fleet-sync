# C24: preserve orphan detection with a shallower statement

Authority: canonical plan r4 row C24, program design §13a W-C26, and the settled keyed-join commission. Base: `7bf44196d43366a67779b4cb8eea0ce2610585e6`.

The writer's production orphan statement now uses a keyed `LEFT JOIN`. Every comparison remains parent-key = child-key, using the metadata-mapped and quoted identifiers. Only those comparisons enter `ON`; the child non-null guards and first mapped parent key's null anti-match remain in `WHERE`. A matched parent key cannot be null when the checked child key is non-null. Statement construction is a private helper used directly by `reject_orphans`; its missing-first-column case returns the existing metadata refusal.

No public contract, schema, storage, width cap, dependency, Cargo profile, stack setting or verification driver changes. The upstream-workarounds note records the historical measured basis, the rejected cross-collation alternatives and the engine-upgrade removal check. Those measurements were read, not rerun.

## Meaning through the production generator

Three permanent real-Turso tests cover widths 1, 2 and 98. Each checks explicit and implicit parent references, different parent/child key-column names with embedded quotes, a partial child index, and both initial and reopened files. The seven literal cases are valid, orphan, null matching, null nonmatching, cross-row composite, parent NOCASE/child BINARY, and parent BINARY/child NOCASE.

That is 84 case/reference/reopen observations. Each runs the independently constructed historical query, the actual production generator's query, and `reject_orphans` with typed refusal assertions. Both query shapes agree with each literal oracle. Width one's null cases are entirely null and its cross-row case is an absent key among two parents; the partial-null and mixed-parent composite claims come from widths 2 and 98.

These tests catch missing key comparisons, rejecting null-bearing children, reversed collation ownership, invalid identifier quoting and incorrect implicit key mapping. The historical comparator accepts the same cases using the materially different correlated shape. It is test-only SQL, not a second production path.

## Public open and migrate

Three integration tests exercise widths 1, 2 and 98 on ordinary default test threads through the real public writer API:

- Create the schema, close the writer, then open with a new valid parent/child migration; the child is readable.
- Offer a migration inserting an orphan whose final key component is absent; `migrate()` returns the typed `ForeignKeyOrphan` naming children and parents.
- Read back the original child and both migration histories; the orphan is absent and both histories end at version 2.
- Close and reopen with the accepted bundle; the exact original child identity remains.

Keys are canonical UUIDv7 values. Enforcement is disabled by the production migration path, so the explicit orphan check must cause refusal. No stand-in replaces the generator, metadata reader, driver, file, migration or rollback. This automated real-path proof needs no equivalent manual replay.

## Baseline and sensitivity

Runs occurred in this order:

1. The first matrix run failed 0 passed / 3 failed, exit 101, during metadata lookup for embedded quotes in fixture table names. It never reached the orphan assertions. Ordinary table names repaired this fixture; key-column quote coverage stayed intact. This is not a behavioral red.
2. With the extracted, unchanged historical production builder, the matrix passed 3 / 0, exit 0.
3. With the keyed join, the same matrix passed 3 / 0, exit 0.
4. A compiled wrong build dropped the final join comparison. The unchanged width-two test failed 0 / 1, exit 101: explicit-reference Orphan before reopen returned false instead of the literal true. The historical comparator still passed that case.
5. Production was restored byte-exact to SHA-256 `5324dcc1e61d000339a772a0e06cd910b7d4a1c50d627dddd1032a2d9414da72`; both permanent test files retained their pre-control hashes. The restored writer suite passed.

The deliberate break establishes sensitivity after implementation. No new pre-fix parser-abort run or small-stack test was made.

## Validation

- `cargo test --locked --all-features -j4 -p fleet-sync-writer`: 125 passed, 0 failed, 0 ignored, exit 0 (86 unit and 39 integration cases; zero doctests). The six new tests are included once.
- `cargo clippy --locked --all-features --all-targets -j4 -p fleet-sync-writer -- -D warnings`: exit 0, no warnings.
- `cargo fmt -p fleet-sync-writer -- --check`: exit 0.
- `git diff --check`: exit 0.

The existing dependency-template diagnostics appear in Cargo output; test completion and exit status above are the observed results. Builds use only this worktree's normal `target/debug`; disk was at least 99 GiB before builds. No workspace-wide Cargo command was used. Fixed checked-query inputs are unchanged; the normal writer build retains their manifest verification. The schema-dependent existence query continues using the existing quoted-identifier `AssertSqlSafe` boundary.

Rust standards inventory: no new production panic/unwrap, cast, non-exhaustive switch or duplicate boundary shape. The test-only panic-in-result allowance permits assertions while propagating fixture failures, matching existing writer tests. Dynamic test DDL follows existing fixture conventions. No language-standard departure was introduced.

## Remaining proof

The scoped meaning and native public-path claims are supported on pinned Turso 0.8.1. The board's formerly aborting test at its actual call depth, its later official integrated pin, paired independent observation, CI, hosted/DO behavior and whole-PR acceptance remain unobserved here. The Lead owns the board-depth and integration observations. No push is authorized for this assignment. The declared commit stays frozen for the paired verifier.
