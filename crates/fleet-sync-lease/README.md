# fleet-sync-lease

Apply `LEASE_MIGRATOR` alongside application migrations in the same host-owned migration transaction before calling registry rules.

`epoch_holder(scope, database, epoch)` reads the validated historical `EpochGrant`,
or `None` when that epoch was never granted. It preserves the granted holder and
time after a successor takes over; `ended_at` is the recorded final replication
position. It neither reads the current lease to guess a holder nor writes state.
