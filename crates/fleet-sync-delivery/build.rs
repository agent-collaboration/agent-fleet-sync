#![forbid(unsafe_code)]
fn main() -> Result<(), fleet_sync_query_gen::GenerateError> {
    fleet_sync_query_gen::verify_manifest("checked-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-custody-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-status-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-report-queries.json")?;
    fleet_sync_query_gen::verify_manifest("checked-inspection-queries.json")
}
