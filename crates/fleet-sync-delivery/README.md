# fleet-sync-delivery

Apply `DELIVERY_MIGRATOR` alongside application migrations in the same host-owned migration transaction before calling delivery rules.

A machine Outbox submits to the relay's Outbox. The relay owns custody until a
real Push receipt or recipient Report establishes the next holder's commit.
Recipients accept Push in their Inbox and journal local delivery and Reports in
the same transaction. The Mailbox role, Fetch/Received cycle and Retire operation
have been removed.

The host supplies the authenticated Submitter and status route to
`Outbox::submit_relay`. SenderBinding validates the consumer's frozen sender in
the supplied custody transaction. A Machine route stays pinned; a Project stream
is owned by the Project identity independently of its current holder's route.
`DeliveryTypes::StatusRoute` is a serializable host address whose canonical
encoded bytes fit `MAX_STATUS_ROUTE_BYTES`.

Hosts choose `DeliveryHostRole::Machine` or `Relay` explicitly for the removal
task, startup scan, submitter backfill and inspection. The Fleet uses Relay;
Copy stores install no delivery tables. Relay paths read only Outbox and status
tables. Machine paths retain their existing Inbox, reports and applied cursor.

Relay hosts call `retire_relay_host_tables` explicitly after applying the delivery
migrator. It drops all ten unused tables transactionally and idempotently. Machine
hosts do not call it. Migration files remain unchanged; machine cleanup belongs
to the machine cutover. Records retain their UUIDv7 creation deadline and are
removed after 60 days by `RemoveDeliveryRecord`.

## Status-event acknowledgement

After durably applying fetched status events, call
`StatusStream::acknowledge(through)` or send
`DeliveryRequest::StatusAck { call, through }`. Only events at or below `through`
are deleted. The acknowledged floor survives reopening, and equal or lower
replays return the current floor without changing the stream. Fetch returns
only events above that floor. Acknowledgement uses its cursor for idempotency
and needs no receipt journal.

`StatusAckResult::Acknowledged { floor }` reports the remote retirement
watermark; it does not advance the sender's locally applied cursor.
`Refused { head }` means the requested cursor is above the stream head and
nothing changed. Treat `head` as diagnostic information, never as an instruction
to clamp the acknowledgement and discard an unread prefix.

A first-hop publication retried after acknowledgement can append the same
`(item, StatusVersion)` again at a new cursor above the floor. Consumers must
deduplicate by that pair and apply statuses forward-only by `StatusVersion`.
`Outbox::publish_status` and `apply_status_batch` preserve that ordering.
Retiring a `PublicationRef` does not retire status events; acknowledgement owns
that deletion.

## Local cancellation

Pass the delivery consumer to `Inbox::enqueue_local` or
`enqueue_local_and_admit`. A pending local cancel is resolved in the same custody
transaction before a new original can be offered for delivery. Relayed controls
are left to their existing Push and completion paths.

Both methods return `LocalItemAdmission`: `Attempt(DeliveryAttempt)` is a fresh
pending item, `Replayed` is existing same-digest custody with no new hooks or rows,
and `SettledOnAdmission` is a fresh item made terminal by a pending local cancel.
Its outcome hook has already run in the same transaction. Consumers can build
fresh-admission projections for both fresh variants without repeating them on
replay. The wrapper has already admitted an `Attempt` to the journal before
returning its delivery payload.

`DeliveryConsumer::on_cancel_outcome(scope, cancel, target, outcome)` projects a
control's outcome with its target, immediately after `on_outcome` in the same
custody transaction. It runs once when the control settles on Local and remote
routes, including `TooLate`, `Prevented`, `Refused` and `Expired`; replays do not
repeat it. The default does nothing. An error aborts custody and both hooks'
projections. Local controls still create no remote report.

`Inbox::enqueue_local_cancel(cancel, consumer, now)` returns
`LocalCancelAdmission { reference, effects }`. Use the returned reference: a
second cancel ID for an already linked target replays the first canonical cancel.
Read its recorded result through `local_outcome(reference.item)`; pending controls
return `None`. Authorization refusal is a recorded outcome. Changed digests return
`Refused(ItemConflict)` with the stored digest; admission capacity returns
`Refused(MailboxFull)`.

Fully local custody needs no receive sequence or acknowledgement and produces no
remote report. Local control rows and links remain stored for replay and count
against the inbox byte cap, including terminal controls. A compact original has
no charged settlement reserve. The shared 60-day record-removal task owns their retention.

A matching Push upgrades Local custody to Relayed with its relay route. A preserved
final original outcome owes one Report; later Push replays do not renew it. If the
recipient reports a local-only cancel that the relay never admitted,
`recipient_report` returns a typed terminal processing refusal without writes.
Storage and corruption errors remain errors. The Report task records the failure and
does not retry or deliver the original again.
