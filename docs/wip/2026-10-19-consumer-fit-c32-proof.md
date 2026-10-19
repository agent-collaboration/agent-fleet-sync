# C32: retained holder-change reasons

The governing reference is program design §13a W-C34 and canonical r4 plan row C32. The base is C29 commit 8461aafda51b7bed47356ab3a97a45e8b78f33bf.

## Contract and ownership

`HolderChangeRequest::Move` and `Takeover` require `reason: HolderChangeReason`. The checked String holds 1 through 16,384 UTF-8 bytes, preserving whitespace and content verbatim. Construction and serde decoding use the same validation. Empty, oversize, missing, null and wrongly typed values are refused. Claim remains a machine request without a reason and refuses an extra reason field.

The full request already owns canonical journal input, the stored admission, transfer intent and the public holder-change view. Its existing content digest and equality checks now include the reason. There is no second reason record, schema, SQL, migration, generated corpus or default. A changed reason under the same attempt is a request conflict before mutation. Admission and reason recording preserve the registry snapshot revision, which versions lifecycle and current grant facts rather than operation metadata.

This required wire and stored-request field is an explicitly authorized in-place amendment on W-C34's no-deployed-store basis. Existing Move or Takeover records without it do not receive a fabricated reason or compatibility path. Fifty-two existing concrete constructors across 26 fixture files receive an explicit test reason; product match arms already preserve the complete request.

## Permanent proof

Three protocol cases establish exact UTF-8 and whitespace preservation, accepted maximum-length ASCII and multibyte strings, byte-bound refusals, required Move/Takeover wire fields, and Claim's strict reason-free shape.

Three actual-Turso cases establish:

- Move and Takeover admission each expose the literal caller reason, preserve it after a full registry-store reopen before execution, grant through the real task, return the recorded outcome on replay without new peer calls, and preserve reason and outcome after another full reopen.
- Changed reasons are refused both before execution and after completion. The test commits the transaction containing the refusal, then compares admission and journal rows, registry authority records, the public registry snapshot and peer-call counts. Transaction rollback cannot mask an unintended mutation.
- Invalid wire reasons fail decoding before typed admission and leave the real registry and journal unchanged. A real import, seeded-copy Claim and grant survive reopen with no reason field.

The invalid-value boundary is the checked protocol type: safe callers cannot construct an invalid reason and independently pass it to typed admission. The test does not claim a second raw-string admission validator.

## Sensitivity and restoration

Three deliberate wrong builds compiled and ran unchanged selected tests:

| Wrong behavior | Passed / failed | Exit |
| --- | --- | --- |
| Ignore changed reason during admission | 0 / 1 | 101 |
| Replace the caller reason in the public view | 0 / 1 | 101 |
| Truncate oversize reasons into accepted values | 1 / 2 | 101 |

The first fails the expected conflict, the second compares a substituted reason with the literal caller text, and the third fails constructor and wire refusals. All three production files were restored byte-for-byte. These are post-fix sensitivity controls; no pre-feature behavioral red is claimed for the previously absent required field.

Clippy initially rejected two unchecked JSON assignments in protocol test setup. They were replaced by object-map insertion with the same assertions. The affected truncation control was refreshed against the final test, again 1/2 and exit 101, followed by exact restoration. The lease test stayed unchanged throughout its controls. The restored complete protocol/lease suite is 155 passed, 0 failed, 0 ignored, exit 0: 95 lease unit cases, 4 holder-authority cases and 56 protocol cases. Focused cases are included in that count.

## Validation

- `cargo test -p fleet-sync-protocol -p fleet-sync-lease --all-features --locked -j 4 -- --test-threads=4`: 155/0/0, exit 0 after restoration.
- `cargo clippy -p fleet-sync-protocol -p fleet-sync-lease --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0.
- `cargo check -p fleet-sync-protocol -p fleet-sync-lease --target wasm32-unknown-unknown --locked -j 4`: exit 0; portable compile proof only.
- `CARGO_TARGET_DIR=target cargo test --manifest-path tests/consumer/Cargo.toml --all-features --locked -j 4 -- --test-threads=4`: 95/0/0, exit 0 through actual native services, sockets, stores and restarts.
- `CARGO_TARGET_DIR=target cargo clippy --manifest-path tests/consumer/Cargo.toml --all-targets --all-features --locked -j 4 -- -D warnings`: exit 0.
- Scoped library and consumer formatting checks and `git diff --check`: exit 0. The 31-file publication scan has no private path or identity matches.

CI and paired independent observation are pending at this declaration. Acceptance remains with the Lead.

Existing strict checked-query build verification remained active. No workspace-wide command or generator invocation was required for the unchanged query inputs. All runtime tests use real native Turso; holder and gate fixtures retain their existing scope. Deployed DO behavior, remote holder drain, board composition, power-loss behavior, whole-PR readiness and delivery acceptance remain outside this slice's evidence.
