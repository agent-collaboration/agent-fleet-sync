INSERT INTO _fleet_sync_steps(id, created_at, updated_at, operation_id, position, step_name, kind, intent, output, first_attempt_at, may_have_reached)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(operation_id, position) DO UPDATE SET
    updated_at = excluded.updated_at, step_name = excluded.step_name, kind = excluded.kind, intent = excluded.intent, output = excluded.output, first_attempt_at = excluded.first_attempt_at, may_have_reached = excluded.may_have_reached
