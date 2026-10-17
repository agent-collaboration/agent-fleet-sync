SELECT attempt,intent,outcome,phase,phase_history,cleanup_failed FROM _fleet_sync_transfers WHERE database=?1 AND outcome IS NULL
