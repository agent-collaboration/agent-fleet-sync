#![forbid(unsafe_code)]
fn main() -> Result<(), fleet_sync_query_gen::GenerateError> {
    fleet_sync_query_gen::verify_manifest("checked-custody-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-outbox-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-status-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-report-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-retention-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-relay-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-backfill-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-inspection-queries.json")
}
