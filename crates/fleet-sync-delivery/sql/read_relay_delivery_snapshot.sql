SELECT CAST(category AS TEXT) AS category, CAST(identity AS TEXT) AS identity, CAST(metadata AS TEXT) AS metadata, CAST(content AS TEXT) AS content, CAST(payload_bytes AS TEXT) AS payload_bytes FROM (
SELECT CAST('outbox' AS TEXT) AS category, item_id AS identity, CAST(live AS TEXT) AS metadata, payload AS content, CAST(payload_bytes AS TEXT) AS payload_bytes FROM _fleet_sync_outbox_items
UNION ALL SELECT CAST('publication' AS TEXT), item_id, CAST(json_object('targetKind',target_kind,'submitterKind',submitter_kind,'submitterId',submitter_id,'route',status_route) AS TEXT), status_kind, CAST(NULL AS TEXT) FROM _fleet_sync_status_owed
UNION ALL SELECT CAST('statusEvent' AS TEXT), item_id, CAST(json_object('submitter',json_object('kind',submitter_kind,'value',submitter_id),'version',version) AS TEXT), status_kind, CAST(NULL AS TEXT) FROM _fleet_sync_status_events
) AS observations;
