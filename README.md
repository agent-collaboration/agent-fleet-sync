one-way, lease-based replication, durable steps and delivery for fleets of agent machines

# agent-fleet-sync

The repository contains owned SQLx drivers and durable operations for Turso and Cloudflare Durable Object SQL. Replication, sessions and delivery build on these foundations.

- `sqlx-turso`, `sqlx-turso-core`, `sqlx-turso-macros`: native Turso 0.8.1, checked queries and migrations.
- `sqlx-durable-object`, `sqlx-durable-object-core`: SQLx over Durable Object SQLite, without owned unsafe code.
- `runtime-primitives`: shared time, call identity, backoff and earlier-only wake scheduling; Tokio timer and Durable Object alarm adapters behind features.
- `durable-steps`: a runtime-free execution core, atomic local steps and receipted calls, with volatile, Turso and Durable Object stores. See [its conformance and recovery journeys](tests/durable-steps/README.md).
- `fleet-sync-delivery`: durable custody and status recovery. See the [status-event acknowledgement contract](crates/fleet-sync-delivery/README.md#status-event-acknowledgement).

## Checks

Rust 1.98.1, rustfmt and Clippy are selected by `rust-toolchain.toml`. All workspace crates inherit the single lint table in `Cargo.toml`.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check --target wasm32-unknown-unknown -p sqlx-durable-object -p sqlx-durable-object-core -p probe-core -p probe-worker
```

The checked-query fixtures and offline metadata are imported with the Turso driver. `SQLX_OFFLINE=true` is configured in `.cargo/config.toml`. Tests cover checked-query diagnostics, migrations, constraints, engine pinning, contention and pool return.

For related queries that must share a snapshot, use `database.reader().begin_read().await?` and pass `&mut read` to SQLx. `ReadTransaction` guards every statement as read-only and exposes no raw connection. `read.close().await?` rolls back; drop and cancelled close use the existing pooled rollback cleanup. An open read transaction keeps maintenance's `ActiveReaders` refusal until released; retry maintenance after closing it.

## Holder-change workflows

The registry's authenticated host starts moves, takeovers and claims with `admit_holder_change::<Store, Database>(scope, request, requested_by, now)` inside its transaction, then drives the engine after commit. Admission binds the attempt to the engine's operation ID and keeps the first requester on repeats. Register `HolderChangeTask`, `HolderChangeCancelTask` and `SettleReservationTask`; the first two take the registry configuration and the same shared `Clock` as the engine host. Apply every registry migration before admitting work.

`holder_change_view` reads the current phase, retained phase history, pending dependency and scheduled retry time through the public journal interface. A claim's outcome is final when decided; `settlement_owed` separately shows unfinished settlement. A failed follow-up keeps that debt and exposes `follow_up_failure`; only an acknowledged completion clears it. Released means the old holder stopped at its final captured position; the copy can still be behind, and granting waits for its acknowledgement. Read repeated answers through `holder_change_answer`: a cancelled move's compact engine result can remain Pending while its withdrawal task later records Cancelled.

The admin surface can call `cancel_holder_change` before Fencing commits. A move stays Cancelling until its release is withdrawn, or a takeover supersedes it. A permanently failed withdrawal records its first cleanup failure and leaves the move active for takeover. A claim records Cancelled and owes its gate a settlement; the gate must refuse a late reservation after settlement. Once Fencing commits, cancellation is TooLate, including during floor repair. Retry times are schedules, and an already-dispatched call can remain unanswered. `move_needs_renewal_within` independently bounds the holder's renewal age when a move starts; consumers choose the bound.

These registry tests use actual Turso journals and receipted holder-call fixtures. The real holder's withdrawal, writer resumption and session integration belong to the writer/runner/service layer. Historical development transfer rows without admission identity or phase history fail closed; this foundation has no compatibility path that invents an authenticated requester.

## Session schema support

An unsuccessful peer WebSocket upgrade carries one `fleet-sync-auth-refusal` response header with the existing `AuthRefusal` JSON value, such as `"expired"`. The service uses 401, or 403 for `WrongKind`. Native clients decode that single header strictly; missing, malformed, unknown or repeated values mean `Unknown`. A bare 403 also means `Unknown`. There is no body fallback or session frame on rejection.

Before welcoming an attachment, the session owner durably reports its schema support to the registry. A per-peer `AttachSequence` orders reports across connection replacement and restart; an older report cannot overwrite a newer declaration. This projection is capability data, persists after disconnect, and has no expiry. `admit_holder_change` freezes the target's `Option<SchemaSupport>` in its admission transaction. Repeats, restart and both fence calls use that value. A target without an accepted report is refused with `SchemaTooNew { supported: None, .. }`; there is no global schema maximum in `RegistryConfig`.

Every host implements `SessionScope::publish_schema_support(input, now)` by admitting `PublishSchemaSupport` and ensuring its durable deadline through the Hello transaction's borrowed journal. `welcome_after_commit(connection, peer, session_id)` buffers a host effect, while the finalizing local step records only `()`. A successful commit publishes staged registration edits and exposes effects; rollback discards both. External effects run after transaction exclusion is released, and the runtime then supplies activation. Route `SchemaSupportRegistry` to the registry's `report_schema_support` rule inside its own transaction.

`FleetService` owns and joins the native session publication runner. Consumers embedding `SessionHost` directly also join its `run` future alongside their connection tasks and shutdown signal. Its adapter stages under the input path's runtime-to-connection lock order, publishes registrations on commit, drops both guards, then signals its native timer and dispatches Welcome. The connection, session and attach sequence are checked in the finalizing transaction; the later send by connection ID is not another identity check.

`SessionClient::connect` activates the transport and sends Hello. Keep pumping receive and server answers during attachment; only the matching Welcome makes operation requests ready. `send_request` refuses before Welcome without sending a frame. A readiness or connection refusal returned before dispatch is `CallFailure::NotSent`; a timeout or uncertain dispatch stays `AnswerLost`, including other pre-send failures and the pump's local reply timeout. Only a durably settled NotSent with no earlier uncertain reach preserves NeverSent.

Request and answer envelopes are identified before their body is decoded. A readable kind and frame ID with an invalid body gets a correlated `OperationAnswer::InvalidShape { field }` and leaves the session open. The field names a missing, unknown or duplicate field when reported by the decoder, or `body` for errors without a field location, and is capped at 64 Unicode characters. An unreadable kind or frame ID closes as `MalformedFrame`.

A replication callback that refuses before applying or successfully rolls back can answer `CopyUnavailable { reason }`. The sender retains its piece, and seed and snapshot callers retain their generation or pinned install and retry. Store-boundary uncertainty answers `OutcomeUnknown { reason: StoreBoundary { detail } }` on the same socket; its diagnostic is validated and capped at 1024 characters. Callers reconcile the original identity through `AnswerLost`, including when a reason cannot decode. A failed timer hint after commit preserves the committed answer; durable follow-ups recover on the next wake or startup.

The unreleased seed protocol includes `SeedPageAnswer::UnitTooLarge`, encoded as `{"kind":"unitTooLarge"}` with no payload. The receiver checks the serialized page against the encoded unit limit before staging it. This reply ends the durable seed operation with `SeedCopyRefusal`; it schedules no retry. Sender and receiver must use the same protocol revision; there is no compatibility fallback.

Registry copy calls use `CallFailure::Retryable` only for known no-new-effect refusals. The call remains pending with its generic code/detail reason, while `holder_change_view.waiting.copy_unavailable` decodes the closed copy reason. `CallReach::NoEffect` records a receiver response with no earlier unresolved possible effect. It survives restart, cannot satisfy an unsent timeout, and records `Dispatching` before retry; an earlier `MayHaveReached` is never cleared by a later refusal. Unknown retry codes fail closed. Final holder-change refusals remain terminal. The library's `test-fault` feature supplies controls on the actual native copy transaction replies and host timer; normal builds omit them.

## Receiver foreign-key integrity

Seed replacement and each assembled replicated transaction validate the final
relationships of the replicated tables before publication, receipts or the
acknowledged position advance. Durable Object replay defers immediate foreign
keys within that transaction; the native receiver uses the same metadata-based
orphan check. A matching snapshot manifest does not replace this check during
native staging verification. FK declarations and schema fingerprints stay intact.

`SeedAnswer::IntegrityViolation { table, references }` and
`PieceRefusal::IntegrityViolation { table, references }` name only the child and
parent tables. They carry no row keys or content. A violating transaction rolls
back, and consumers treat the refusal as terminal for that seed attempt or piece,
not as retryable storage unavailability. These additions to exhaustive enums
require the coordinated v0.2.0 update on both sides of the protocol.

Turso 0.8.1 does not implement `PRAGMA defer_foreign_keys`; successful execution
of an unsupported pragma is not an integrity check. Receiver tests explicitly
disable native engine FKs to prove the metadata gate rejects orphans itself.
The regular writer pool enables native engine FKs; captured child-first writes
in the regression use a declared deferred constraint, preserving source validity.

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
