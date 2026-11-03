UPDATE _fleet_sync_outbox_items SET submitter_kind=?2,submitter_id=?3,updated_at=CAST(?4 AS INTEGER) WHERE item_id=?1 AND submitter_kind IS NULL AND submitter_id IS NULL AND legacy_read_only=0;
