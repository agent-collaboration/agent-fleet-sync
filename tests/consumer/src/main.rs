#[tokio::main]
async fn main() -> fleet_sync_test_consumer::TestResult {
    let directory = std::env::args_os()
        .nth(1)
        .ok_or("usage: fleet-sync-test-consumer <state-directory> [port]")?;
    if std::env::args().nth(2).as_deref() == Some("writer") {
        let endpoint = std::env::args().nth(3).ok_or("writer needs endpoint")?;
        let mode = std::env::args().nth(4).ok_or("writer needs mode")?;
        let database = std::env::args()
            .nth(5)
            .map(|value| value.parse::<durable_steps::Uuid>())
            .transpose()?
            .map(fleet_sync_protocol::DatabaseId::new)
            .transpose()?;
        return fleet_sync_test_consumer::run_writer(
            std::path::Path::new(&directory),
            &endpoint,
            &mode,
            database,
        )
        .await;
    }
    let port = std::env::args()
        .nth(2)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(0);
    let receiver = std::env::args()
        .nth(3)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(3);
    fleet_sync_test_consumer::run_server_with_receiver(
        std::path::Path::new(&directory),
        port,
        receiver,
    )
    .await
}
