SELECT CAST(source AS TEXT) AS source,CAST(row_key AS TEXT) AS row_key,
       CAST(item_id AS TEXT) AS item_id
FROM (
    SELECT 'outbox' AS source,rowid AS row_key,
           CASE WHEN typeof(item_id)='text' THEN item_id END AS item_id
    FROM _fleet_sync_outbox_items
);
