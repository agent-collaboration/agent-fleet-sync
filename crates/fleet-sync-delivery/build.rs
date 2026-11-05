#![forbid(unsafe_code)]
fn main() -> Result<(), fleet_sync_query_gen::GenerateError> {
    fleet_sync_query_gen::verify_manifest("custody-records-queries.json")?;
    fleet_sync_query_gen::verify_manifest("outbox-items-queries.json")?;
    fleet_sync_query_gen::verify_manifest("status-stream-queries.json")?;
    fleet_sync_query_gen::verify_manifest("delivery-reports-queries.json")?;
    fleet_sync_query_gen::verify_manifest("retention-removal-queries.json")?;
    fleet_sync_query_gen::verify_manifest("relay-submitter-queries.json")?;
    fleet_sync_query_gen::verify_manifest("custody-backfill-queries.json")?;
    fleet_sync_query_gen::verify_manifest("delivery-snapshots-queries.json")
}
