INSERT INTO _fleet_sync_operations (
    operation_id, kind, kind_version, request_key, input, input_digest, state,
    result, failure, finished_at, next_attempt_at, attempt, drive_token, drive_until
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
