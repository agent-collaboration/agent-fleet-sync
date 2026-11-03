SELECT CAST(category AS TEXT) AS category, CAST(identity AS TEXT) AS identity, CAST(metadata AS TEXT) AS metadata, CAST(content AS TEXT) AS content, CAST(payload_bytes AS TEXT) AS payload_bytes FROM (
SELECT CAST('mailbox' AS TEXT) AS category, item_id AS identity, CAST(live AS TEXT) AS metadata, record AS content, CAST(payload_bytes AS TEXT) AS payload_bytes FROM _fleet_sync_mail_items
UNION ALL SELECT CAST('outbox' AS TEXT), item_id, CAST(live AS TEXT), payload, CAST(payload_bytes AS TEXT) FROM _fleet_sync_outbox_items
UNION ALL SELECT CAST('inbox' AS TEXT), item_id, CAST(live AS TEXT), payload, CAST(payload_bytes AS TEXT) FROM _fleet_sync_inbox_items
UNION ALL SELECT CAST('report' AS TEXT), item_id, CAST(report_seq AS TEXT), outcome_kind, CAST(NULL AS TEXT) FROM _fleet_sync_reports
UNION ALL SELECT CAST('publication' AS TEXT), item_id, CAST(json_object('kind',target_kind,'value',json(CASE target_kind WHEN 'peer' THEN json_object('peer',machine_id,'route',json(status_route)) WHEN 'statusStream' THEN json_object('route',json(status_route),'sender',machine_id) END)) AS TEXT), status_kind, CAST(NULL AS TEXT) FROM _fleet_sync_status_owed
UNION ALL SELECT CAST('statusEvent' AS TEXT), item_id, CAST(json_object('machine',machine_id,'version',version) AS TEXT), status_kind, CAST(NULL AS TEXT) FROM _fleet_sync_status_events
UNION ALL SELECT CAST('mailState' AS TEXT), CAST(singleton AS TEXT), CAST(NULL AS TEXT), record, CAST(NULL AS TEXT) FROM _fleet_sync_mail_state
UNION ALL SELECT CAST('inboxState' AS TEXT), CAST(singleton AS TEXT), CAST(NULL AS TEXT), record, CAST(NULL AS TEXT) FROM _fleet_sync_inbox_state
) AS observations;
