# Frame shapes — SO56

An identified request or answer with an invalid body receives correlated
`InvalidShape { field }`; an unreadable envelope closes as `MalformedFrame`.
The body refusal leaves the same authenticated connection usable. This is
SO56's adopted outcome, with decisions 199–201 requiring the decoder's actual
unknown, missing or duplicate field rather than a guess from diagnostic text.

Inputs are the caller's declared immutable head, requested frame cases and
observation grant. Use the pin, exclusive slot, normal target, provenance and
cleanup procedure in [build setup](build-and-evidence.md). A native socket
observation does not establish Cloudflare sessions or central's G3 gate.

## Select the real socket route

For decoder-field changes, run the existing complete socket selection from the
pinned archive root:

```bash
cargo test --offline --locked --manifest-path tests/consumer/Cargo.toml --all-features --test native_frame_shapes -- --nocapture
```

Its source is `tests/consumer/tests/native_frame_shapes.rs`. The tests start
the actual consumer service, authenticate WebSocket connections and read real
answers. If the whole standalone consumer suite is already required at that
same head, it includes this target: retain its selection and raw observations
instead of running an equivalent extra green selection. Individual frames
inside a test are subcases, not extra Rust test passes.

For the basic malformed-body/envelope journey, the existing self-describing
script remains available:

```bash
bash scripts/verification/frame-shapes.sh
```

It owns readiness through its child PID/READY message and matching authenticated
Welcome. It sends missing Echo text, missing body, an unexpected field, an
invalid client answer and an oversized mixed-width Unicode field. Require a
distinct non-nil UUIDv4 FrameId for each, exact correlated `invalidShape`, and a
valid Echo on that same connection. FrameId is a correlation ID, so entity-key
UUIDv7 rules do not apply. Only the five valid follow-ups may reach the consumer.
Separate connections send malformed JSON, an invalid or missing ID and an
unknown kind; each must close as malformedFrame without another consumer call.
The script records later cases even after a wrong close. It does not replace
the fuller decoder-field controls below. Select it when its narrower coverage
fits the requested journey; do not run both routes merely to increase counts.

## Required decoder-field controls

Inspect the actual selected tests and their assertions at the pinned head.
Retain all seven existing controls when verifying the structured-field change:

- `identified_bad_bodies_are_correlated_and_the_same_session_stays_usable`:
  missing Echo text names `text`, missing body names `body`, the outer unknown
  names `unexpected`, and the unnamed answer-variant failure names `body`.
  The oversized name returns its first 64 Unicode characters, not 64 bytes.
  Each body failure is followed by a valid request on the same socket; only
  those five valid follow-ups route. The 64-character cap is the current bounded
  diagnostic policy, not a new spec constant.
- `unreadable_kind_or_frame_id_closes_as_malformed_without_routing`:
  malformed JSON, invalid ID, missing ID and unknown kind close their separate
  connections, with routed count zero.
- `unknown_fields_with_backticks_keep_their_complete_correlated_names`:
  embedded, leading and diagnostic-like backtick names remain whole. Each
  answer has the exact field and FrameId, then a valid same-socket follow-up;
  routed count is three.
- `unknown_fields_choose_the_actual_empty_or_nonempty_diagnostic_suffix`:
  the legal deny_unknown_fields empty request and the nonempty outer envelope
  carry names containing opposite diagnostic markers or line/column-like text.
  All six exact names and frame identities survive, with six valid same-socket
  empty-request follow-ups and routed count six. This historical test name
  does not authorize parsing a diagnostic suffix.
- `unknown_fields_are_not_confused_with_declared_names_or_nested_input_keys`:
  a legal declared name contains expected-list-like text. An unknown
  `unexpected` field must still name exactly `unexpected`, both for a scalar
  value and a nested key that imitates the ambiguous diagnostic. Repeat each
  with adjacent-tag content before its kind. All four answers retain their
  exact FrameId and valid same-socket follow-up; routed count is four. A parser
  cursor after the unknown key cannot identify that key reliably.
- `discarded_backtracking_errors_do_not_replace_the_final_reported_field`:
  a discarded untagged candidate must not replace the final `outerAfter`
  field; when the final error has no structured field, require `body`.
  Both valid same-socket follow-ups route, and only those two.
- `missing_and_duplicate_declared_fields_keep_their_exact_names`:
  missing and duplicate fields both retain the complete declared name,
  including its embedded backtick/expected-list text. Require matching frame
  identities, valid same-socket follow-ups and routed count two.

When entity-key decoding is also in scope, keep the separate existing
`native_engine_identities` seed-attempt control: invalid UUIDv4 entity key,
correlated InvalidShape, same-session Echo and zero seed receipts after the
owning process stops. Frame correlation and entity-key admission are different
boundaries; this control is not an eighth frame test.

Source-check `crates/fleet-sync-session/src/frame_shape.rs` and its session
caller. Structured Serde unknown/missing/duplicate errors carry the actual
field; unnamed errors fall back to `body`, then the Unicode cap applies.
Per-delegation capture must restore only the returned error so a caught,
discarded candidate cannot leak into the final answer. Parsing Display text,
recursively matching input keys and guessing from parser position each lose
information covered by the controls above. A source check complements the
socket assertions; it is not their runtime proof.

## Authorized wrong-build control and restoration

Fault injection requires the caller's explicit grant. Without it, return that
control as unavailable; reading this recipe grants no product or fault writes.
With the grant, use only the isolated pinned source archive, keep the original
bytes and source hashes, and run the correct selection first. A useful own
control makes field selection use the former Display-based parser while the
ordinary decode path remains intact. The declared-name/content-first socket
assertions must fail on the wrong returned field; a compiler or fixture failure
does not demonstrate sensitivity.

Keep the exact mutation diff, command, exit, raw selected counts and mismatched
field/frame. Label it a post-fix wrong-build sensitivity control, not a
pre-change baseline RED. Restore every changed archive file byte-for-byte,
verify its hash against the pinned source, then require the unchanged full
frame selection to pass. Retain restored executable hashes before cleanup.
An envelope-only mutant may show the body/envelope distinction but cannot
substitute for the ambiguous-field control when that is the commissioned risk.

## Completion and limits

Return the declared head, actual cases/fields/frame identities, raw Rust counts
and script case counts separately, valid follow-up/routed-count observations,
any authorized mutant and exact-restoration results, and unavailable controls.
A reconnect cannot satisfy same-session recovery. The test helpers stop their
owned service; confirm all jobs and children settled, preserve evidence and
perform the main skill's normal-target cleanup before returning the slot.
Missing cases stay unknown. Native socket success is not a hosted, G3 or
delivery-acceptance claim, and executable recipe evidence is not a fresh-agent
skill-evaluation pass.
