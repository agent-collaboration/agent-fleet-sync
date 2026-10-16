one-way, lease-based replication, durable steps and delivery for fleets of agent machines

# agent-fleet-sync

The repository contains owned SQLx drivers and durable operations for Turso and Cloudflare Durable Object SQL. Replication, sessions and delivery build on these foundations.

- `sqlx-turso`, `sqlx-turso-core`, `sqlx-turso-macros`: native Turso 0.8.1, checked queries and migrations.
- `sqlx-durable-object`, `sqlx-durable-object-core`: SQLx over Durable Object SQLite, without owned unsafe code.
- `runtime-primitives`: shared time, call identity, backoff and earlier-only wake scheduling; Tokio timer and Durable Object alarm adapters behind features.
- `durable-steps`: a runtime-free execution core, atomic local steps and receipted calls, with volatile, Turso and Durable Object stores. See [its conformance and recovery journeys](tests/durable-steps/README.md).

## Checks

Rust 1.98.1, rustfmt and Clippy are selected by `rust-toolchain.toml`. All workspace crates inherit the single lint table in `Cargo.toml`.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check --target wasm32-unknown-unknown -p sqlx-durable-object -p sqlx-durable-object-core -p probe-core -p probe-worker
```

The checked-query fixtures and offline metadata are imported with the Turso driver. `SQLX_OFFLINE=true` is configured in `.cargo/config.toml`. Tests cover checked-query diagnostics, migrations, constraints, engine pinning, contention and pool return.

## Local workerd integration

Install the wasm target, wasm-bindgen CLI **0.2.129**, Node.js, and the pinned package dependencies, then run:

```sh
npx --yes pnpm@12.11.2 install --frozen-lockfile
cargo build --target wasm32-unknown-unknown -p probe-worker
wasm-bindgen --target bundler --no-typescript --out-dir tests/workerd/build --out-name index target/wasm32-unknown-unknown/debug/probe_worker.wasm
node -e "require('node:fs').copyFileSync('tests/workerd/shim.mjs', 'tests/workerd/build/shim.mjs')"
node tests/workerd/run-workerd.mjs
```

The imported journey uses real SQLite-backed Durable Objects under Miniflare/workerd. It checks commit, rollback, read-own-writes, overlapping transactions, eleven exact values and storage classes, including positive and negative infinity, effect ordering and state across a runtime restart. Integer fidelity uses decimal casts and tagged reads; arbitrary raw i64 binds outside JavaScript's safe range remain refused. Dropping the transaction future can still commit on the Worker; portable rollback-on-drop is not claimed.

## Provenance and license

Turso drivers and tests were imported from codex-router commit `9cc19dd2`; see [their origin](crates/sqlx-turso/README.md). The Durable Object driver comes from the one-core probe fork of sqlx-cloudflare commit `35e5219f4fb772827f393488fb255893e09d20fb`, retaining its [MIT attribution](licenses/LICENSE.sqlx-cloudflare). The workers-rs dependency is pinned to `b171cb4bdd6fa943bb81f429df0a6b0df37e14b3`, the SDK revision exercised by that probe.

Original code is licensed MIT OR Apache-2.0. The imported Durable Object driver remains MIT.
