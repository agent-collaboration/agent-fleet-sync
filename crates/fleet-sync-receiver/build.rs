#![forbid(unsafe_code)]

fn main() -> Result<(), fleet_sync_query_gen::GenerateError> {
    fleet_sync_query_gen::verify_manifest("checked-queries.json")?;
    // Queries only the `inspect` feature compiles, so default builds carry none of them.
    fleet_sync_query_gen::verify_manifest("checked-inspect-queries.json")
}
