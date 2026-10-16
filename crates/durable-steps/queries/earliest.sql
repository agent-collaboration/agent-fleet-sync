SELECT MIN(CASE WHEN drive_until > next_attempt_at THEN drive_until ELSE next_attempt_at END) AS "at?: i64"
FROM _fleet_sync_operations WHERE state = 'running'
