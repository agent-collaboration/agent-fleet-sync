# Local delivery configuration without a relay

W-C42 removes relay enrollment from construction of local delivery. `DeliverLocally::new` now accepts `LocalDeliveryConfig { owner, limits }`. The relay-facing `DeliveryTaskConfig` contains `local: LocalDeliveryConfig`, `relay` and the fetch bounds. There is no old-signature overload, implicit conversion or compatibility alias.

`Inbox::new` already accepted only owner and limits, so its API stays intact. `SubmitItem` already takes limits and obtains its relay from each `SubmitInput`; that routing contract also stays intact. Receive/report tasks read owner and limits through `config.local`. The reference consumer builds local settings for local inbox operations and local call dispatch, and obtains remote settings only for relay-facing work. Task inputs, KIND/VERSION, receipts, wire encodings, database records, migrations and checked SQL are unchanged.

The existing suite assertions are retained; fixture changes only adopt the new configuration shape. The added `local_delivery_constructs_and_replays_without_relay_configuration` test registers only the local delivery task over real native custody and the durable engine. Its established local-only Host accepts only `DeliverCall`, resolves real custody, calls the consumer and projects its observation in SQL. No relay identity, relay service, receive task or report task is configured. It verifies the committed Delivered receipt/observation, then re-enqueues the same item and verifies no repeated task or effect.

A deliberate wrong build disabled the local-route guard that suppresses remote reports. The selected case failed **0 passed / 1 failed, exit 101** because the next drive attempted the uninstalled `fleet-sync.delivery.report-outcome` kind. The source file was restored byte for byte by SHA-256, and the restored case passed. This is runtime sensitivity for preserving local independence, not a pre-fix runtime red for the old Rust constructor signature.

Scoped validation, each command exit 0:

- `cargo test -p fleet-sync-delivery --all-features --locked -j4`: **96 passed / 0 failed / 0 ignored**.
- `CARGO_TARGET_DIR=target cargo test --manifest-path tests/consumer/Cargo.toml --all-features --locked -j4 --test 'delivery_*'`: **20 passed / 0 failed / 0 ignored** across ten delivery targets.
- `cargo clippy -p fleet-sync-delivery --all-targets --all-features --locked -j4 -- -D warnings`.
- `CARGO_TARGET_DIR=target cargo clippy --manifest-path tests/consumer/Cargo.toml --all-targets --all-features --locked -j4 -- -D warnings`.
- `cargo check --target wasm32-unknown-unknown -p fleet-sync-delivery --locked -j4`.
- Root and external-consumer `cargo fmt ... -- --check`.
- Restored single relay-free case: **1 passed / 0 failed**, exit 0.

This is the local W-C42 slice. Full combined gates, independent observation, batch acceptance and the single batch push follow W-C41 and any subsequently commissioned runner slice. No hosted CI or Durable Object runtime claim is made for this unpushed slice.
