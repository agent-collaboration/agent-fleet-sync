SELECT CAST(version AS TEXT) AS version, typeof(version) AS version_class, CAST(checksum AS TEXT) AS checksum, typeof(checksum) AS checksum_class FROM _fleet_sync_hook_migrations
