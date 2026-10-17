SELECT operation_id AS "operation_id!", position AS "position!: i64", step_name AS "step_name!", kind AS "kind!", intent, output, first_attempt_at AS "first_attempt_at?: i64", may_have_reached AS "may_have_reached?: i64"
FROM _fleet_sync_steps WHERE operation_id = ? AND position = ?
