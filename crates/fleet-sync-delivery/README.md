# fleet-sync-delivery

Apply `DELIVERY_MIGRATOR` alongside application migrations in the same host-owned migration transaction before calling delivery rules.

Enrollment calls `Mailbox::open()` for both a new recipient and a readmitted
installation. Repeated opens preserve the mailbox's sequences, recorded answers
and owed publications. Items refused by `close()` remain terminal; new IDs can be
submitted after `open()`.
