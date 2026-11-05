fn main() -> Result<(), fleet_sync_query_gen::GenerateError> {
    for manifest in [
        "checked-queries.json",
        "checked-history-queries.json",
        "checked-renewal-queries.json",
        "checked-transfer-queries.json",
        "checked-lifecycle-queries.json",
        "checked-admission-queries.json",
        "checked-support-queries.json",
        "checked-legacy-queries.json",
    ] {
        fleet_sync_query_gen::verify_manifest(manifest)?;
    }
    Ok(())
}
