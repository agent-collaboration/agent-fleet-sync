UPDATE _fleet_sync_operations
SET state = ?, result = ?, failure = ?, finished_at = ?, input = ?, next_attempt_at = ?, attempt = ?, drive_token = ?, drive_until = ?
WHERE operation_id = ? AND drive_token IS ?
