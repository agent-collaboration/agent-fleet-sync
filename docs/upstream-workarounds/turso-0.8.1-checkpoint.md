# Turso 0.8.1 native publication checkpoint

The Tokio publication owner runs exactly `PRAGMA wal_checkpoint(TRUNCATE)` before closing its connection and replacing a live file. The shared query generator refuses this statement because the owned driver describes `busy`, `log` and `checkpointed` as `NULL` types. There is no result-type override in the generator.

This one host maintenance query uses a runtime-checked adapter, as authorized in design decision 40. It requires exactly one row and exactly those three named columns. `busy` must be a nonnegative integer. On success, both counters must be nonnegative integers and equal. A busy or incomplete checkpoint refuses publication retryably; no rename callback runs.

The pinned engine's failure path writes only `busy = 1`, leaving both counters NULL (`turso_core` 0.8.1, `vdbe/execute.rs:799-806`). NULL counters are accepted only when busy is nonzero. Other type or shape changes produce `InvalidCheckpoint`. Re-check this behavior at the next native driver upgrade. Every other fixed runner query remains on its owner's checked SQL path.

The permanent real-file tests in `fleet-sync-tokio-runner/src/native_publish_tests.rs` cover a successful checkpoint, close, rename and reopen of the replacement file, and a held reader that prevents checkpoint completion. The refusal test observes the engine's actual NULL counters, proves the rename callback was not entered, ends the reader, and then successfully retries publication. No checkpoint rows are fabricated.

This adapter does not by itself prove snapshot verification, local-state carry, install-ID replay, or process-kill recovery. Those belong to the complete snapshot-install journey. No upstream issue has been filed.
