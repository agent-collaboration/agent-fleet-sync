-- Views are members of the replicated schema but never have snapshot row pages.
CREATE TABLE _fleet_sync_copy_tables (name TEXT NOT NULL PRIMARY KEY);
CREATE TABLE _fleet_sync_hook_migrations (version TEXT NOT NULL PRIMARY KEY, checksum TEXT NOT NULL);
