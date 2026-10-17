SELECT operation_id AS "operation_id!", kind AS "kind!", kind_version AS "kind_version!: i64",
       request_key AS "request_key!", input, input_digest AS "input_digest!", state AS "state!",
       result, failure, finished_at AS "finished_at?: i64", next_attempt_at AS "next_attempt_at?: i64", attempt AS "attempt!: i64",
       drive_token, drive_until AS "drive_until?: i64"
FROM _fleet_sync_operations WHERE operation_id = ?
