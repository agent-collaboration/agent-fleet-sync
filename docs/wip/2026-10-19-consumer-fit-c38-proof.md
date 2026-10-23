# C38: consumer-coded recipient refusals

Authority: plan r4 C38, program design §13a W-C39, decision 487 and the adopted bounded-code and reserve-proof amendments. Base: accepted C37 integration 68394e062e143a133d46943ef23d01095ab5fe7b.

## Contract

DeliveryRefusal adds RecipientRefused with a RecipientRefusalCode. The code is consumer-owned and carried without interpretation. Its constructor and String decoding enforce 1–64 bytes of ASCII letters and digits. MAX_RECIPIENT_REFUSAL_CODE_BYTES is a library constant. There is no detail or structured payload, and existing refusal meanings are unchanged.

The new validated type is deliberately separate from validated_text!, whose existing types reject only empty strings. Unbounded refusal text would violate delivery's finite settlement reserve. The explicit maximum coded refusal is included in the existing reserve enumeration, so all existing header, report and historical-publication calculations cover it.

A 64-byte coded refusal and the existing ItemConflict with a 64-hex digest both encode to **111 canonical JSON bytes**. This tie is documented beside the reserve entry; the minimum allowance does not grow. Removing only the new tied entry would not change behavior, so no single-entry sensitivity is claimed.

The new refusal travels through the existing Refused outcome/status paths and is terminal. No new custody rule, migration, dependency, checked query or consumer adapter is needed.

## Permanent cases and real path

Five permanent cases cover:

- Literal code round trips and terminal status, including a 64-byte maximum.
- Rejection of empty, missing, non-string, over-bound and non-alphanumeric codes at decode, including unknown detail fields.
- Matching construction/decode boundaries: one and 64 bytes pass; 65 bytes, punctuation, whitespace and non-ASCII fail. Independent encoded-size assertions establish the 111-byte tie.
- A native four-store delivery round trip for seven literal codes. Real Outbox, Mailbox, Inbox, Engine, journal, owed report, status stream and remote sender status preserve the exact refusal. The application projection hook sees the same RecipientOutcome. A repeated receive and fresh drive do not resend the terminal item.
- A real item/cancel settlement with four maximum-length historical status routes each, ending in maximum coded refusals. Independent SQL readbacks count retained headers, eight owed publications and the cancellation. It fits a configuration using exactly the computed minimum; minimum minus one is rejected.

The existing network fixture controls the external local-recipient answer and routes typed calls through real native owners. It does not replace custody, reports, projection writes, status publication or persistence. This is native library proof through a controlled transport, not sockets or a deployed remote application.

Before product edits, the valid-code protocol case failed on the missing recipientRefused variant (one pass/one failure, exit 101). Both native cases also failed on that decode before reaching their new paths (0/2, exit 101); those are missing-contract reds, not prior native-path behavior proof. The constructor case was added after its API existed. The initial native fixture assumed incidental Engine batch counts and that rerouting alone created status history; source inspection corrected those assumptions without changing product behavior.

Four compiled wrong builds each fail an unchanged exact-selected permanent case, 0 passed / 1 failed, exit 101:

- Drop the reason at mailbox report handling by replacing it with DroppedByPolicy.
- Rewrite the code at that boundary to a different valid code.
- Bypass code validation, allowing empty-code decode.
- Divide the computed reserve by 16: actual retained metadata is 4,157 bytes against the faulty 377-byte minimum.

Three product homes were restored byte-exact, and both permanent test-file hashes remained unchanged across controls. Immediate wrong-build executable hashes and raw logs are retained separately from final restored evidence.

## Validation and limits

The restored delivery/protocol/writer/receiver suites pass **400 distinct / 0 failed / 0 ignored, exit 0** (delivery 95, protocol 63, writer 136, receiver 106). Raw output has 405 passes, including five recursive writer child summaries already counted by their parents. All five new permanent cases pass inside this total. Fifty-two current executed binaries were hashed after the run. Four-package all-target/all-feature Clippy with warnings denied, formatting and portable delivery/protocol/receiver Wasm checks pass. Standalone consumer all-target/all-feature compilation and Clippy with warnings denied also pass. Exact-head CI, paired observation and Lead acceptance are pending at declaration.

No external consumer runtime, actual peer socket/session/HTTP delivery, workerd/DO, process termination or power-loss claim is made. Existing native task/journal tests remain the regression boundary. Consumers assign meaning to codes; the library does not infer retry, permission or capability semantics from them. A malformed external code is rejected rather than normalized. Protocol peers must understand the new tagged refusal variant.

Scoped commands, using the normal target and a fresh disk-floor check before every Cargo invocation:

~~~sh
cargo test -p fleet-sync-delivery -p fleet-sync-protocol -p fleet-sync-writer -p fleet-sync-receiver --all-features --locked -j4 --no-fail-fast -- --nocapture --test-threads=4
cargo clippy -p fleet-sync-delivery -p fleet-sync-protocol -p fleet-sync-writer -p fleet-sync-receiver --all-targets --all-features --locked -j4 -- -D warnings
cargo check --target wasm32-unknown-unknown -p fleet-sync-delivery -p fleet-sync-protocol -p fleet-sync-receiver --locked -j4
cargo fmt -p fleet-sync-delivery -p fleet-sync-protocol -p fleet-sync-writer -p fleet-sync-receiver -- --check
CARGO_TARGET_DIR=target cargo check --manifest-path tests/consumer/Cargo.toml --all-targets --all-features --locked -j4
CARGO_TARGET_DIR=target cargo clippy --manifest-path tests/consumer/Cargo.toml --all-targets --all-features --locked -j4 -- -D warnings
~~~

All exit 0. No checked-query input changed and no generation was needed. Pinned dependency template-manifest enumeration diagnostics remain unchanged; the requested Cargo gates complete successfully.
