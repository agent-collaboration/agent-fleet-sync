SELECT CAST(position AS TEXT) AS position,phase_kind,final_epoch,final_sequence,CAST(entered_at AS TEXT) AS entered_at FROM _fleet_sync_transfer_phase_history WHERE attempt=?1 ORDER BY position
