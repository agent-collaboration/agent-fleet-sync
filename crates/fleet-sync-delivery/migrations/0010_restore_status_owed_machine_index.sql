-- The typed-table replacement in 0007 dropped the index created in 0005.
CREATE INDEX _fleet_sync_status_owed_machine ON _fleet_sync_status_owed(machine_id, item_id);
