# Foundation local proof — 2026-10-16

This receipt covers the repository foundation and its owned SQLx drivers. Hosted CI is recorded on the PR. It establishes no deployment or replication-engine behavior.

## Commands and results

All commands ran from the repository root with Rust/Cargo 1.98.1. On macOS, native C dependencies used Apple clang (`CC=/usr/bin/clang`, `CXX=/usr/bin/clang++`); inherited Homebrew LLVM lacked `fenv.h`. No host configuration changed.

| Command | Exit | Observation |
| --- | --- | --- |
| `cargo fmt --all -- --check` | 0 | Formatting matches the copied rustfmt configuration |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 0 | All workspace targets inherit the canonical lint table; no Clippy diagnostics |
| `cargo test --workspace --all-features --locked` | 0 | 126 tests and 5 doctests passed; zero failed; one optional inherited DO HTTP test explicitly ignored |
| `cargo check --target wasm32-unknown-unknown -p sqlx-durable-object -p sqlx-durable-object-core -p probe-core -p probe-worker --locked` | 0 | DO libraries and the journey compile for wasm |
| `cargo build --target wasm32-unknown-unknown -p probe-worker --locked` | 0 | Built the Worker exercised below |
| `wasm-bindgen --target bundler --no-typescript --out-dir tests/workerd/build --out-name index target/wasm32-unknown-unknown/debug/probe_worker.wasm` | 0 | Bundled with wasm-bindgen CLI 0.2.129 |
| `node tests/workerd/run-workerd.mjs` | 0 | Real local workerd journey, Miniflare 4.20260730.0 / workerd 1.20260730.1 |

The Turso portion includes 98 ordinary tests and 5 doctests. Its diagnostic test runs two valid and two compile-fail cases against the engine, including missing-column and multi-column scalar rejection. The diagnostic child process repeats its parent test name in the output; it is counted once. All nine Turso engine packages resolve to 0.8.1, and the driver graph rejects SQLite/libSQL engines. The DO portion adds 27 unit tests and one facade test.

## Workerd observations

The caller is the imported Rust Worker through the owned DO driver, using a fresh SQLite-backed Durable Object. The journey sends HTTP requests through Miniflare's real workerd runtime and disposes it before starting another runtime on the same storage.

- `/suite` returned 200: commit, callback rollback, SQL rollback, read-own-writes and forced overlapping transactions passed.
- All nine values and storage classes matched: i64 MIN/MAX, 2^53−1, 2^53, 2^53+1, REAL 1.0, NULL, text and blob. Integers use decimal casts and tagged readback.
- `/effect` returned 200 with `effect observed after durable commit: http-committed`; one `EFFECT http-committed` trace was emitted. Repeating the request returned 503 `storage failed`.
- `/inspect` returned exactly `["cancel-prefix", "committed", "http-committed", "second-callback"]` before and after workerd restart.
- The temporary journey state was removed after both runtimes were disposed.

## Limits and test sensitivity

The tests are imported behavior checks, with their existing driver expectations as the oracle; no behavior pre-fix red is claimed. Format and lint failures were observed during integration. The isolated wasm check also failed before the DO driver explicitly enabled SQLx's `offline` feature, which its `Executor::describe` implementation requires; the corrected check passed. No required gate or lint was relaxed. The inherited DO HTTP suite used to return success without a URL; it now reports ignored in the host suite and fails if explicitly invoked without `DO_WORKER_URL`. It is not counted as runtime proof.

The wasm dependency graph has featureless Tokio through workers-rs, with no Tokio runtime feature. Cargo emits nonfatal diagnostics while scanning template manifests in the pinned workers-rs Git checkout; successful command exits above are checked separately.

Raw DO i64 binds beyond JavaScript's safe range remain refused. Dropping a waiting transaction future on Worker can still commit: this run observed `callback_still_alive=true` and `cancel_prefix_committed=true`. Portable rollback-on-drop, hosted eviction/hibernation, deployment and later milestones remain unverified. Standalone checked-query metadata regeneration tooling is not packaged in this foundation.
