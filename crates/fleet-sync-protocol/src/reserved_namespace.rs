/// Engine, platform, replication, and migration bookkeeping never belongs to
/// a consumer's replicated schema or rows. SQLite resolves these names without
/// ASCII case distinctions, so every boundary applies the same comparison.
#[must_use]
pub fn reserved_namespace(name: &str) -> bool {
    let lower_name = name.to_ascii_lowercase();
    lower_name == "_sqlx_migrations"
        || [
            "sqlite_",
            "turso_",
            "__turso_internal",
            "_cf_",
            "__miniflare_",
            "_fleet_sync_",
        ]
        .iter()
        .any(|prefix| lower_name.starts_with(prefix))
}
