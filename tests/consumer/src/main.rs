#[tokio::main]
async fn main() -> fleet_sync_test_consumer::TestResult {
    let directory = std::env::args_os()
        .nth(1)
        .ok_or("usage: fleet-sync-test-consumer <state-directory> [port]")?;
    let port = std::env::args()
        .nth(2)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(0);
    fleet_sync_test_consumer::run_server(std::path::Path::new(&directory), port).await
}
