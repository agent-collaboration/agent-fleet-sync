# fleet-sync-delivery

Apply `DELIVERY_MIGRATOR` alongside application migrations in the same host-owned migration transaction before calling delivery rules.

Enrollment calls `Mailbox::open()` for both a new recipient and a readmitted
installation. Repeated opens preserve the mailbox's sequences, recorded answers
and owed publications. Items refused by `close()` remain terminal; new IDs can be
submitted after `open()`.

`DeliveryTypes::StatusRoute` is an owned, serializable host address. Its canonical
encoded bytes must fit `MAX_STATUS_ROUTE_BYTES`; the settlement allowance includes
the current route and all four possible historical publications per item and cancel.
The host supplies the submitting peer's authenticated route to `Mailbox::submit`,
with an optional rank. A same-digest replay moves only the current status target
when its rank is present and strictly higher (`None` sorts below `Some(0)`).
The host leaves session and human mail unranked. Immutable origin and payload stay
unchanged; a different digest always conflicts.

Each sender's `StatusStream` keeps its own verified owner route. Committed
`PublicationRef` and `PublishCall` values retain the full target through both hops,
including after a target move. Retirement follows the live current target and
returns per-item `retired`, `unknown`, and `notCurrentTarget` lists. Saved call
receipts replay first; compacted items remain retired and never regain a target.

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
are left to their existing receive and completion paths.

Both methods return `LocalItemAdmission`: `Attempt(DeliveryAttempt)` is a fresh
pending item, `Replayed` is existing same-digest custody with no new hooks or rows,
and `SettledOnAdmission` is a fresh item made terminal by a pending local cancel.
Its outcome hook has already run in the same transaction. Consumers can build
fresh-admission projections for both fresh variants without repeating them on
replay. The wrapper has already admitted an `Attempt` to the journal before
returning its delivery payload.

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
no charged settlement reserve. Control retirement and compaction need a separate
retention policy.
