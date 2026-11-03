-- The typed-table replacement in 0007 dropped the index created in 0005.
CREATE INDEX _fleet_sync_status_owed_submitter ON _fleet_sync_status_owed(submitter_kind,submitter_id, item_id);
