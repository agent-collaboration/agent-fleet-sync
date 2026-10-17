CREATE INDEX _fleet_sync_mail_deadlines
ON _fleet_sync_mail_items (
    json_extract(record, '$.value.payload.value.validUntil'), item_id
)
WHERE live = 1 AND json_extract(record, '$.value.fetched') = 0;
