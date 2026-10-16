SELECT CAST(version AS TEXT) AS version, typeof(version) AS version_class,
       CAST(statements AS TEXT) AS statements, typeof(statements) AS statements_class,
       CAST(schema_layout AS TEXT) AS schema_layout, typeof(schema_layout) AS layout_class
FROM _fleet_sync_schema_steps ORDER BY _fleet_sync_schema_steps.version
