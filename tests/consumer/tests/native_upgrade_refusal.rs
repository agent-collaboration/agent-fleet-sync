use fleet_sync_lease::{LeasePolicy, RegistryConfig};
use fleet_sync_protocol::{AuthRefusal, CloseReason, ReceiverId};
use fleet_sync_service::{FleetService, ServiceOptions};
use fleet_sync_session::{
    ConnectFailure, Endpoint, PeerAuthenticator, PeerCredential, SessionTransport,
    TransportConnection, VerifiedPeer,
};
use fleet_sync_test_consumer::{Consumer, ConsumerStore, SampleProtocol, TestResult};
use fleet_sync_transport_websocket::WebSocketTransport;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::timeout,
};

struct RefusingAuthenticator(AuthRefusal);
impl PeerAuthenticator for RefusingAuthenticator {
    type Material = String;
    type Route = String;
    async fn verify(&self, _: String) -> Result<(VerifiedPeer, String), AuthRefusal> {
        Err(self.0)
    }
}

async fn wire_headers(port: u16) -> TestResult<Vec<String>> {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).await?;
    socket.write_all(b"GET /session HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer refusal-control\r\n\r\n").await?;
    let mut reader = BufReader::new(socket);
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Err("refusal ended before its headers".into());
        }
        if line == "\r\n" {
            return Ok(headers);
        }
        headers.push(line.trim().to_owned());
        if headers.len() > 32 {
            return Err("too many response headers".into());
        }
    }
}

async fn service_refusal(reason: AuthRefusal, status: &str, value: &str) -> TestResult {
    // The authenticator's decision is the controlled input. Its real host,
    // FleetService upgrade and native transport carry that decision unchanged.
    let directory = tempfile::tempdir()?;
    let store = ConsumerStore::open(&directory.path().join("consumer.db")).await?;
    let receiver = ReceiverId::new(durable_steps::Uuid::now_v7())?;
    let service = FleetService::<SampleProtocol, _, _>::open(
        ServiceOptions {
            directory: directory.path().join("service"),
            delivery: None,
            receiver,
            registry: RegistryConfig {
                lease: LeasePolicy {
                    lease_duration: Duration::from_secs(600),
                    renew_interval: Duration::from_secs(30),
                },
                authoritative_receiver: receiver,
                release_unsent_for: Duration::from_secs(5),
                move_needs_renewal_within: None,
            },
            call_timeout: Duration::from_secs(5),
            socket_timeout: Duration::from_secs(5),
        },
        Arc::new(Consumer::new(store.clone())?),
        Arc::new(RefusingAuthenticator(reason)),
    )
    .await?;
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let (shutdown, stopped) = watch::channel(false);
    let mut serving = tokio::spawn(service.serve(listener, axum::Router::new(), stopped));
    let observed = async {
        let endpoint = Endpoint::new(&format!("ws://127.0.0.1:{port}/session"))?;
        let credential = PeerCredential::new("Bearer refusal-control")?;
        let refusal = match WebSocketTransport::default()
            .connect(&endpoint, &credential)
            .await
        {
            Err(error) => Some(error),
            Ok(socket) => {
                socket.close(CloseReason::ClientClosed).await;
                None
            }
        };
        let headers = wire_headers(port).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((refusal, headers))
    };
    let observed = timeout(Duration::from_secs(5), observed).await;
    shutdown.send_replace(true);
    match timeout(Duration::from_secs(5), &mut serving).await {
        Ok(result) => result??,
        Err(error) => {
            serving.abort();
            let _ = serving.await;
            return Err(error.into());
        }
    }
    store.close().await;
    let (refusal, headers) = observed??;
    assert_eq!(refusal, Some(ConnectFailure::CredentialRejected(reason)));
    assert!(
        headers.first().is_some_and(|line| line.starts_with(status)),
        "{headers:?}"
    );
    let reasons = headers
        .iter()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("fleet-sync-auth-refusal"))
        .map(|(_, value)| value.trim())
        .collect::<Vec<_>>();
    assert_eq!(reasons, [value]);
    Ok(())
}

#[tokio::test]
async fn real_service_keeps_unknown_upgrade_refusal() -> TestResult {
    service_refusal(AuthRefusal::Unknown, "HTTP/1.1 401", "\"unknown\"").await
}
#[tokio::test]
async fn real_service_keeps_expired_upgrade_refusal() -> TestResult {
    service_refusal(AuthRefusal::Expired, "HTTP/1.1 401", "\"expired\"").await
}
#[tokio::test]
async fn real_service_keeps_revoked_upgrade_refusal() -> TestResult {
    service_refusal(AuthRefusal::Revoked, "HTTP/1.1 401", "\"revoked\"").await
}
#[tokio::test]
async fn real_service_keeps_wrong_kind_upgrade_refusal() -> TestResult {
    service_refusal(AuthRefusal::WrongKind, "HTTP/1.1 403", "\"wrongKind\"").await
}
