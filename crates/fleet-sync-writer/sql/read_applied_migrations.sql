SELECT CAST(version AS TEXT) AS version, typeof(version) AS version_class
FROM _sqlx_migrations ORDER BY _sqlx_migrations.version
