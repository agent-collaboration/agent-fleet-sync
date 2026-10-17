#[tokio::main]
async fn main() -> fleet_sync_test_consumer::TestResult {
    let mut arguments = std::env::args().skip(1);
    let directory = arguments.next().ok_or("missing node directory")?;
    let peer = arguments.next().ok_or("missing node peer")?.parse()?;
    fleet_sync_test_consumer::delivery_node::run(std::path::Path::new(&directory), peer).await
}
