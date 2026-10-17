SELECT operation_id AS "operation_id!" FROM _fleet_sync_operations
WHERE state = 'running' AND next_attempt_at <= ?
  AND (drive_until IS NULL OR drive_until <= ?)
ORDER BY next_attempt_at, operation_id
