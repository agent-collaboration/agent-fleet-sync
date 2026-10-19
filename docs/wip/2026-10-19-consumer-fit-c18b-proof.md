# C18b: bounded snapshot pages and transaction parts

Authority: canonical plan r4 C18a/C18b split, program design §13a W-C21 and the adopted library-only sizing and annotation policy. Base: accepted C18a fc46750cdb85999d4a44f562cb5a3f64bb66b54c.

## Behavior and ownership

Library producers target **1 MiB** and refuse an indivisible item above a **1.5 MiB hard cap**, retaining the existing count limits. A first item may exceed the target while fitting the hard cap. Both limits are below the Durable Object 2 MB row limit. They are constants for this build: changing them requires draining or restarting unfinished seeds and part assemblies; this slice adds no release-transition mechanism.

The protocol sizing owner counts actual typed carriers. A page moves through SeedPage, the seed request frame and snapshot answer frame; a piece moves through its actual consumer-typed request frame. The original owned value is extracted afterward. Consumer annotations need no Clone implementation, and there is no parallel wire representation.

Staged seed sizing includes the complete seed JSON, maximum-width generation TEXT, page INTEGER and digest TEXT. Staged part sizing includes complete piece JSON, its actual serialized transaction key, index INTEGER and a maximum-width decimal i64 timestamp TEXT. Each reserves another 128 bytes for SQLite record/cell framing. This conservatively bounds logical record fields and framing, not database-file or overflow-page allocation. The reserve follows SQLite's [record format](https://www.sqlite.org/fileformat2.html#record_format): at most five header varints for four columns, plus table-cell varints and the initial overflow pointer. Future seed counters and provisional part descriptors use maximum legal widths; the final actual part descriptors are measured again before any piece is returned.

Writer and copy pages fetch one row per keyset query, with one-row lookahead, inside the existing owned transaction. They close at the byte or row limit and continue after the last emitted key. The writer manifest follows the returned continuation, including byte-short pages in the same table. The copy SQLx backend deliberately drains a query synchronously, so LIMIT 1 bounds that buffer without retaining a JavaScript cursor across an await or changing the driver.

Transaction parts retain the whole transaction digest and ordered row/statement content. Each final descriptor and piece digest covers its actual boundary. The annotation is computed once from the complete captured transaction and normally stays with the final row or statement. Only when the final item and whole annotation fit individually but exceed the hard cap together does the producer use the existing empty final fragment. Annotation fragmentation and receiver assembly are unchanged. All parts are prepared before transport sends begin.

Oversize refusals are local typed errors: rows retain their actual table and ordered TEXT key; schema statements retain the actual commit, migration version and statement index; annotations retain their originating commit and encoded size. Defensive snapshot-schema refusal carries only encoded size. There is no invented row or statement identity and no new wire refusal. C18a's separate 256 KiB current-catalog admission bound remains intact.

Consumer import-page types belong to their consumer. This slice measures only library pages, seed carriers, session frames and staged parts. It changes no migration, persisted format, dependency, digest algorithm or checked-query input.

## Real behavior and sensitivity

Thirteen new permanent native cases cover writer and copy paging, actual staged seed fields, staged parts and atomic apply, non-Clone annotations, first-item progress, exact typed refusals and lost-answer replay. Expected byte limits are independent numeric literals in the tests.

Before the page implementation, both primary paging cases failed: writer page **3,152,617 bytes**, copy page **3,150,781 bytes**, each 0/1, exit 101. The two real 2 MiB row cases also failed because the old producers emitted them.

All five initial part regressions were authored and failed before changing the count-only splitter: a **3,190,816-byte** piece; a **1,843,916-byte** row-plus-annotation request; and missing typed refusals for a 2 MiB row, 2 MiB annotation and a JSON-escaped historical schema statement. The historical statement is admitted because its transient table is dropped and the current catalog stays small. It is the individual encoded statement that cannot be emitted.

The positive paths write through the real native SDK and actual capture reader. The public sender commits bookkeeping before transport, stages real parts, keeps rows and annotation effects invisible until final atomic apply, then reproduces identical piece bytes after losing the committed final answer. The 900 KiB row plus 900 KiB annotation uses two existing fragments; 1,100 KiB rows with a fitting final annotation stay packed. Writer and copy pages reconstruct the independent expected rows and canonical table digests, preserve replay bytes and continue at the actual last emitted key.

Four compiled wrong builds fail unchanged permanent assertions:

- Loose target and hard limits: 0 passed / 3 failed, exit 101, reproducing the oversized writer page, copy page and piece.
- Continuing after the rejected candidate rather than the last emitted row: 0/2, exit 101, with the exact skipped-key mismatch on both page producers.
- Omitting the empty final annotation fragment: 0/1, exit 101; the fitting row and annotation are wrongly refused together as an oversized row.
- A loose hard cap: 0/5, exit 101; writer/copy page rows, a part row, the historical schema statement and annotation are emitted instead of returning their contextual refusals.

All four mutated product homes and five permanent test homes were restored byte-exact. Source/test hashes and immediate wrong-build executable hashes were retained for each control; the final restored native run records 25 current executable hashes.

## Validation and limits

The final restored complete suites pass **333 distinct / 0 failed / 0 ignored, exit 0**: protocol 4 unit and 56 integration, receiver 106, runner 31, writer 97 unit and 39 integration. Raw output has 338 passes because five recursive child summaries are already counted by their parents. Clippy for all four packages, all targets and all features passes with warnings denied.

Commands run from the repository root, using the normal target, a fresh disk-floor check before each Cargo invocation and four build jobs:

~~~sh
cargo test -p fleet-sync-protocol -p fleet-sync-receiver -p fleet-sync-writer -p fleet-sync-tokio-runner --all-features --locked -j4 --no-fail-fast -- --nocapture --test-threads=4
cargo clippy -p fleet-sync-protocol -p fleet-sync-receiver -p fleet-sync-writer -p fleet-sync-tokio-runner --all-targets --all-features --locked -j4 -- -D warnings
cargo check --target wasm32-unknown-unknown -p fleet-sync-protocol -p fleet-sync-receiver --locked -j4
cargo fmt -p fleet-sync-protocol -p fleet-sync-receiver -p fleet-sync-writer -p fleet-sync-tokio-runner -- --check
cargo run -p fleet-sync-query-gen --features native --locked -j4 -- --manifest crates/fleet-sync-receiver/checked-queries.json --check
~~~

All exit 0. The native suite was refreshed after restoring all controls; Clippy ran on the identical correct source before the controls. Final Wasm compilation, formatting, receiver checked-query freshness and diff checks pass. No query generation was needed. Exact-head CI, paired observation and acceptance are pending at declaration.

One-row queries and repeated complete-unit serialization cost more local work. In the final restored run, the copy fixture took 6.67 seconds for four pages and 48 rows, and the writer fixture 7.24 seconds for six pages and 49 rows, including replay and manifest reconstruction under concurrent tests. These are native fixture timings, not a latency guarantee.

This evidence does not establish the Durable Object storage cap or real session/HTTP delivery. Real seed/part staging at the chosen limit and large delivery over those paths remain G3/G4. A consumer with an indivisible item over the hard cap is deliberately refused; supporting larger items would require an unapproved fragmentation contract. Boundary changes must not take over unfinished assemblies from another budgeting build.
