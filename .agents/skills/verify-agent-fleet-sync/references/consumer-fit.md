# Consumer-fit journeys

The oracle is the caller's adopted consumer-fit Verification Design, rows SO52,
SO53, SO56 and SO57, including native-session scope and staged C3/C6 controls.
Take expected behavior from that source, never from today's product answer.

## Holder refusals — SO52

Run `bash scripts/verification/holder-refusals.sh` after changes to holder-change
rules, checks, completion or refusal messages.

The script publishes a schema-5 unheld import through the existing consumer's
Import/SeedBegin/SeedPage/SeedComplete boundary and verifies copy status and
snapshot. P claims it. A different peer's claim must receive
`alreadyHeld{holder:P}`, with that same refusal in its stored view. A real
takeover gives it to Q; repeating the original claim must still name P and
return its identical recorded outcome.

The journey also creates a real ReplicatedDatabase holder. After two applied
migrations, it offers migration 7, whose duplicate-key insert fails. Readbacks
must show the first two migrations, the original row and the unchanged capture
position. A real move then delivers a Release request on the holder's session;
the driver passes that correlated call to the writer's public
`request_release` and returns its actual answer on that connection. Both the
writer answer and the registry refusal must name migration 7 and `pending`.
Removing the pending offer and repeating the same move must replay the first
refusal. This observes the real writer producer and registry/session path;
the driver dispatches the call rather than running NativeWriterRuntime.
The writer's entry is `ReplicatedDatabase::request_release` in
`crates/fleet-sync-writer/src/writer_release.rs`; the registry consumes it in
`crates/fleet-sync-lease/src/holder_change_rules.rs`.

**Named limit: no real writer produces Failed.** A failed migration rolls back
without recording a failed state and fails writer open. A later failing
`migrate` call on an already-open writer leaves the migration unapplied, so its
release check answers Pending. The Failed protocol variant remains unchanged;
creating a durable failed state or changing writer-open behavior is outside
this journey. Do not build a failed-holder fixture for delivered observation.

The registry's product test owns Failed pass-through at the HolderRelease seam
(`crates/fleet-sync-lease/src/turso_move_refusal_tests.rs`). It must check exact
migration/state detail in both the response and canonical durable outcome,
replay the first result after the scripted answer changes and across registry
reopen, and show no extra release, fence, lease or epoch effect. That is the
Implementer's product proof. The historical Failed fixture remains baseline
registry evidence only; the delivered journey contains no synthetic Failed
holder. It is no evidence of a real Failed producer.

Wrong builds: unit AlreadyHeld or MigrationPending, or a replay naming Q.

## Target capability — SO53

Run `bash scripts/verification/target-capability.sh` after publication, registry
schema projection, holder admission or fence changes.

The script verifies a ready, unheld schema-5 copy and attaches B with support 4.
B must receive `schemaTooNew{store:5,supported:4}`. B reattaches with support 5;
the same attempt must replay its first refusal. A fresh B attempt on an
independent ready, unheld copy must grant. The independent copy avoids holder
contention deciding this positive control. Fresh moves additionally test targets
declaring support 4 and 5, and a target without an accepted report. The holder
fixture answers the real Release at the copy's independently verified final
position. This observes registry fencing, not native writer draining.

The timing cases use the existing consumer's SchemaControlRequest and the real
FleetService/SessionHost/registry owners:

- Admit an admin move with accepted support 5 while registry drive is held
  before check, publish support 4, then resume. The admission row keeps 5 and
  the move grants. Repeat with a process kill/restart before check; a fresh
  admission on an independent unheld copy freezes 4 and is refused.
- Hold an old report while a newer attach declares 4. After releasing the old
  report, read back the newer attach sequence and support. Restart with the
  same receiver and files, read the persisted row, and observe a schema-5
  claim refused with support 4.
- Hold a new report of 4 after accepted support 5. No Welcome arrives while
  the report is held; an admin admission in that window freezes 5 and grants.
  Releasing the report yields matching Welcome and an Attached registration.
  Admin and own-session claims after publication freeze 4 and are refused.
- On another held report, a raw pre-Welcome request closes without serving
  it. Another connection's credential revocation and the target's close
  progress. Releasing the report does not resurrect the closed registration.
- Fail real finalization after staging. Read back Attaching and no committed
  attach step or Welcome; release the retry and observe Attached plus one
  Welcome. Delayed finalization after replacement or process restart never
  resurrects the old attach or adds a Welcome to the newer connection.
- Drive the production SessionPump with a real client store: a readiness
  refusal before Welcome is NotSent and the consumer count stays zero; after
  matching Welcome an Echo is served once. Separately leave a production pump
  unstarted with its command receiver alive: the actual local reply deadline
  returns AnswerLost. That control proves local timeout classification, with
  no network dispatch. Other encoding or uncertain-send failures are not
  injected by this journey and must not be called no-effect.

Wire absence is sampled within a bounded window after an operation-owned hold
or completion, never used to signal readiness. Restart checks retain the real
store; marker restoration is not part of this journey. The scripted holder
release remains registry/fence evidence, not writer-draining evidence.
Wrong builds include a global maximum, latest-arrival-wins, reading the
projection at check, replaying a new decision and Welcome before publication.

## Frame shapes — SO56

Run `bash scripts/verification/frame-shapes.sh` after session rules, frame types
or native socket routing change.

The script authenticates a real WebSocket session and sends identified frames
with missing Echo text, missing body, an unexpected field and a malformed
client answer. Each uses a distinct non-nil UUIDv4 correlation ID; entity-key
UUIDv7 rules do not apply to FrameId. Each answer must be correlated
`invalidShape{field}` (the named field, or `body` for the unnamed variant
failure), followed by a valid Echo on that same connection. Only the four
valid follow-ups may reach the consumer.

Separate authenticated connections receive malformed JSON, an invalid ID, a
missing ID and an unknown kind. Each must close as malformedFrame, with no
additional consumer request. The script continues after a wrong close so it
records body failures and envelope controls. Reconnecting cannot satisfy a
same-session follow-up. The strengthened native_engine_identities product test
separately checks the invalid UUIDv4 seed attempt: correlated InvalidShape,
same-session Echo, and zero seed receipts after the owning process stops.

Wrong build: treating every decode failure as an unreadable envelope.
Cloudflare sessions belong to central's real runtime gate, outside this journey.

## Copy containment — SO57

Run `bash scripts/verification/copy-containment.sh` after replication host,
socket loop, writer session calls, sender or copy codec changes.

The route promotes the adopted X7 native probe. Two real writers share a peer
and one SessionPump/WriterSessionCalls connection to FleetService. Both copies
first acknowledge their schema and a row. After service and pump stop and join,
only A's saved copy marker is damaged. B remains healthy. The service restarts
with the same receiver and storage. The observing transport holds reconnect,
so another socket cannot masquerade as containment.

A's next Piece must receive correlated replication CopyUnavailable with a
typed damaged reason, retain its identity and acknowledgement, and add no
effect. B must acknowledge on connection 1 and add one durable row and receipt.
After stopping service, restoring A's exact saved marker and reading it back,
a new session resends A's identical Piece. It applies once; an explicit
duplicate answers AlreadyApplied without adding another row or receipt.

The separate answer-loss control drops a real committed Applied response at
the observing transport. The sender must see AnswerLost and keep its durable
acknowledgement. A readback must show the committed row and receipt. Resending
the identical Piece after restart must receive AlreadyApplied, advance the
sender acknowledgement and leave the entire copy snapshot unchanged.

The registry control publishes an unheld import, stops the service, removes
only its copy marker, and submits a real claim. The claim must keep its
reservation and chosen epoch while waiting for the copy. After service stop,
exact marker restoration and restart, repeating that same attempt must grant
the reserved epoch with the same gate reservation identity. An independent
fresh import is the readiness control, and an already-held final refusal is
replayed across another restart. Marker restoration stands in for repaired
storage, not for a reseed API. The view observation reads the operation's own
pending-copy or final-outcome state with a bound; it does not use a fixed sleep.

Required cases still reported as gaps:

- The claim's typed availability reason and journal CallPending/CallDone
  readback, plus the move acknowledgement-wait and fence repair cases.
- C6's store wrapper hides a commit or rollback: A receives typed OutcomeUnknown,
  never no-effect; B continues; A reconciles its original Piece. An unknown
  reason kind takes the answer-loss reconciliation path.
- C6's scheduler fails the real post-commit wake: committed Applied returns;
  admitted follow-ups run once later or after restart.
- Closed rollback-reason codec rejects unknown, malformed or unrelated codes
  and preserves every typed reason's complete payload.
- Seed and snapshot paths retain and resume identities after copy repair.

Connect seam-dependent cases at the declared C6 head. A registry call must not
cache transient no-effect results as CallDone or end as Refused(Unavailable).
Keep final-refusal replay separate. Authoritative NotInitialized reseed remains
the design's explicit limit. Store-boundary injection does not prove real disk
failure. Workerd receiver/driver atomicity and the codec use the real apply
fixture. Producer classification counts there only when it calls production
DurableObjectStore and the real host's classifier, with both commit and
no-commit controls; a copied mapping is fixture policy, not proof. Otherwise
producer classification, like session containment, stays at central's G3
against the exact library revision. Implemented C slices do not establish PR C
readiness before those G3 receipts exist.
