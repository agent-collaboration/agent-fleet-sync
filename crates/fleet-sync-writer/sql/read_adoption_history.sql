SELECT CAST(version AS TEXT) AS version, typeof(version) AS version_class,
       CAST(success AS TEXT) AS success, typeof(success) AS success_class,
       checksum, typeof(checksum) AS checksum_class
FROM _sqlx_migrations ORDER BY version
