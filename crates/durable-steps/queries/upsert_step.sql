INSERT INTO _fleet_sync_steps(operation_id, position, step_name, kind, intent, output, first_attempt_at, may_have_reached)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(operation_id, position) DO UPDATE SET
    step_name = excluded.step_name, kind = excluded.kind, intent = excluded.intent, output = excluded.output, first_attempt_at = excluded.first_attempt_at, may_have_reached = excluded.may_have_reached
