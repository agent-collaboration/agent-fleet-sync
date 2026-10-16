# sqlx-durable-object

The owned SQLx 0.9 driver for a Cloudflare Durable Object's SQLite storage.

Use `DoConnection::new(state.storage())` inside an object. Both shared and mutable connection references implement SQLx's executor. Transactions use `DoConnection::transaction`, backed by `Storage::transaction`; SQLx's ordinary `begin` is refused because the platform owns the transaction lifetime.

This crate is a fork of sqlx-cloudflare-do, imported through the one-core probe. It uses the SDK's safe `SendWrapper`, keeps SQL storage on the connection, and inherits the workspace's `unsafe_code = "forbid"` lint. See the [MIT attribution](../../licenses/LICENSE.sqlx-cloudflare). It is not published to crates.io.

The driver refuses raw i64 arguments outside JavaScript's safe range. The [workerd integration journey](../../README.md#local-workerd-integration) checks exact i64 transport through decimal casts and storage-class-tagged reads, plus transactions and restart readback. Dropping an awaiting transaction future does not guarantee rollback on Worker.

The inherited HTTP driver suite requires its own local test Worker. It is explicitly ignored in the host suite and fails if invoked without `DO_WORKER_URL`; the mandatory foundation journey is `node tests/workerd/run-workerd.mjs`.
