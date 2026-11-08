Durable workflow execution and owned SQLx drivers for Turso and Cloudflare Durable Objects

# agent-fleet-sync

The repository contains the general durable-steps workflow engine, runtime primitives, owned SQLx drivers and a typed query generator for Turso and Cloudflare Durable Object SQL. The fleet sync layer moved to [codex-router](https://github.com/agent-collaboration/agent-collaboration).

- `sqlx-turso`, `sqlx-turso-core`, `sqlx-turso-macros`: native Turso 0.8.1, checked queries and migrations.
- `sqlx-durable-object`, `sqlx-durable-object-core`: SQLx over Durable Object SQLite, without owned unsafe code.
- `runtime-primitives`: shared time, call identity, backoff and earlier-only wake scheduling; Tokio timer and Durable Object alarm adapters behind features.
- `durable-steps`: a runtime-free execution core, atomic local steps and receipted calls, with volatile, Turso and Durable Object stores. See [its conformance and recovery journeys](tests/durable-steps/README.md).
- `fleet-sync-query-gen`: describes SQL with the owned Turso driver and generates typed calls over borrowed workflow transactions. See [the generator guide](crates/fleet-sync-query-gen/README.md).

## Checks

Rust 1.98.1, rustfmt and Clippy are selected by `rust-toolchain.toml`. All workspace crates inherit the single lint table in `Cargo.toml`.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check --target wasm32-unknown-unknown -p sqlx-durable-object -p sqlx-durable-object-core -p probe-core -p probe-worker
```

The checked-query fixtures and offline metadata are imported with the Turso driver. `SQLX_OFFLINE=true` is configured in `.cargo/config.toml`. Tests cover checked-query diagnostics, migrations, constraints, engine pinning, contention and pool return.

## Non-repeating call reconciliation

A `CallTarget` with `DEDUPLICATES = false` can opt into
`RECONCILES = true`. On recovery of a possibly reached call, or after a live
dispatch returns `AnswerLost`, the engine asks
`Host::reconcile(target, request, CallId)` for an already established answer.
The lookup runs outside the journal transaction and must only read: it must
never dispatch the call or perform its effect. `Some(answer)` is saved as the
normal `CallDone` answer before the task advances; saved answers replay without
another lookup. Return `Some` once the answer is recorded, `Retryable` while the
effect may still be in flight, and `None` only when no answer will ever be
established. `None` and non-retryable errors are a final `Uncertain` outcome.
A `Retryable` lookup error retains `MayHaveReached` and its retry reason,
then retries only the read through the engine's normal backoff. It never clears
earlier possible reach or falls back to dispatching the effect.
Reserve `Retryable` for conditions that will clear; the engine retries without
an attempt limit until the owning task ends.

Fresh calls and `NeverSent`/`NoEffect` retries dispatch first; only a lost
answer can then trigger a read-only lookup. Deduplicating calls never reconcile,
even if they opt in. The target flag defaults to false;
the Host method defaults to `Ok(None)`.
Malformed reconciled answers become `AnswerLost`, as malformed dispatch answers do.

A reconciled answer must pass the same target-specific validator as a dispatched
answer before journaling; decoding the answer's shape alone is insufficient.

## Fleet sync moved to codex-router

Fleet-specific APIs and integration guidance now live with their owners:

- [fleet-mail-delivery](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-mail-delivery): custody, status recovery and delivery reconciliation.
- [fleet-project-lease](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-project-lease): holder-change admission, claims, moves and takeovers.
- [fleet-project-writer](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-project-writer): guarded read transactions, retained files and native file admission.
- [fleet-session](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-session) and [fleet-native-session](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-native-session): session protocol, schema publication and native session ownership.
- [fleet-session-client](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-session-client) and [fleet-websocket-transport](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-websocket-transport): attachment, recovery, link state and typed upgrade refusals.
- [fleet-project-copy](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-project-copy): replication, seed/snapshot application and foreign-key integrity.
- [fleet-native-services](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-native-services) and [fleet-tokio-objects](https://github.com/agent-collaboration/agent-collaboration/tree/6862368070f3a1ad274b6eeb97a240bf0781c89a/crates/fleet-tokio-objects): native service composition and object publication.

These links pin the codex-router revision that introduced the new crate homes.

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
