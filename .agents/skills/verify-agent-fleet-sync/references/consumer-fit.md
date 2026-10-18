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

The baseline also drives real moves whose owned fixture holder returns pending
and failed migration 7 through the real Release call. This observes registry
propagation only; the holder's migration state is a stand-in. A delivered C2
observation must use a real ReplicatedDatabase writer with a genuinely pending
migration and a genuinely failed migration recorded in its own history. If
either cannot be produced, return the exact gap; the fixture cannot close it.
The writer's entry is `ReplicatedDatabase::request_release` in
`crates/fleet-sync-writer/src/writer_release.rs`; the registry consumes it in
`crates/fleet-sync-lease/src/holder_change_rules.rs`.

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

Required cases still reported as gaps:

- Admit with accepted support 5, hold drive before check, publish support 4,
  then resume: the attempt freezes 5. Repeat across restart; fresh admissions
  freeze 4.
- Deliver an old report after a newer attach: persisted attach sequence and
  support retain the newer declaration.
- Hold the new report: no Welcome or served requests before publication;
  admissions before and after publication freeze 5 and 4 respectively.

Connect the timing controls to C3's real seams and run at its declared head.
Wrong builds include a global maximum, latest-arrival-wins, reading the
projection at check, replaying a new decision and Welcome before publication.

## Frame shapes — SO56

Run `bash scripts/verification/frame-shapes.sh` after session rules, frame types
or native socket routing change.

The script authenticates a real WebSocket session and sends a valid frame ID
with an Echo body missing text. It must receive `invalidShape{field}` for that
exact ID, then a valid Echo must answer on that same connection. An unreadable
JSON envelope on another authenticated connection must close as malformedFrame.
The script continues after a wrong close so it records both the body failure
and the envelope control. Reconnecting cannot satisfy the body case.

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

Required cases still reported as gaps:

- Committed apply with lost answer, followed by same-identity reconciliation.
- Claim, move acknowledgement wait and fence blocked by a copy; repair and
  restart preserve attempt, chosen epoch, reservation and pending call.
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
