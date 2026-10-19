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
