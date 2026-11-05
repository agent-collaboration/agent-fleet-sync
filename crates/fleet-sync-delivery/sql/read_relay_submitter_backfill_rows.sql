SELECT CAST(source AS TEXT) AS source,CAST(row_key AS TEXT) AS row_key,
       CAST(item_id AS TEXT) AS item_id,CAST(item_class AS TEXT) AS item_class,
       CAST(submitter_kind AS TEXT) AS submitter_kind,CAST(submitter_kind_class AS TEXT) AS submitter_kind_class,
       CAST(submitter_id AS TEXT) AS submitter_id,CAST(submitter_id_class AS TEXT) AS submitter_id_class,
       CAST(consumer_sender AS TEXT) AS consumer_sender,CAST(consumer_sender_class AS TEXT) AS consumer_sender_class,
       CAST(payload_present AS TEXT) AS payload_present,CAST(record_kind AS TEXT) AS record_kind,
       CAST(status_kind AS TEXT) AS status_kind,CAST(progress AS TEXT) AS progress,
       CAST(submit_attempted AS TEXT) AS submit_attempted,CAST(live AS TEXT) AS live,CAST(legacy_read_only AS TEXT) AS legacy_read_only
FROM (
  SELECT 'outbox' AS source,rowid AS row_key,
         CASE WHEN typeof(item_id)='text' THEN item_id END AS item_id,typeof(item_id) AS item_class,
         CASE WHEN typeof(submitter_kind)='text' THEN submitter_kind END AS submitter_kind,typeof(submitter_kind) AS submitter_kind_class,
         CASE WHEN typeof(submitter_id)='text' THEN submitter_id END AS submitter_id,typeof(submitter_id) AS submitter_id_class,
         CASE WHEN typeof(consumer_sender)='text' THEN consumer_sender END AS consumer_sender,typeof(consumer_sender) AS consumer_sender_class,
         CASE WHEN payload IS NULL THEN '0' ELSE '1' END AS payload_present,
         record_kind,status_kind,NULL AS progress,submit_attempted,live,legacy_read_only
  FROM _fleet_sync_outbox_items
) AS custody ORDER BY source,row_key;
