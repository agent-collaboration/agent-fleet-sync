#![allow(clippy::panic_in_result_fn)]
use fleet_sync_protocol::{AuthRefusal, CloseReason};
use fleet_sync_session::{
    ConnectFailure, Endpoint, PeerCredential, SessionTransport, TransportConnection,
};
use fleet_sync_transport_websocket::WebSocketTransport;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn split_refusal(status: &str, headers: &str) -> TestResult<Option<ConnectFailure>> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let endpoint = Endpoint::new(&format!("ws://{}/session", listener.local_addr()?))?;
    let header_block =
        format!("HTTP/1.1 {status}\r\n{headers}Content-Length: 11\r\nConnection: close\r\n\r\n");
    let (release_body, body_released) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await?);
            if request.len() > 8192 {
                return Err("upgrade request too large".into());
            }
        }
        socket.write_all(header_block.as_bytes()).await?;
        // The body is a valid competing reason, deliberately withheld until
        // the client has returned. Reading or guessing from it is forbidden.
        let _ = body_released.await;
        let _ = socket.write_all(b"\"wrongKind\"").await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    let observed = timeout(Duration::from_secs(3), async {
        match WebSocketTransport::default()
            .connect(&endpoint, &PeerCredential::new("Bearer refusal-control")?)
            .await
        {
            Err(error) => Ok(Some(error)),
            Ok(socket) => {
                socket.close(CloseReason::ClientClosed).await;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(None)
            }
        }
    })
    .await;
    let _ = release_body.send(());
    match timeout(Duration::from_secs(3), &mut server).await {
        Ok(result) => result??,
        Err(error) => {
            server.abort();
            let _ = server.await;
            return Err(error.into());
        }
    }
    observed?
}

#[tokio::test]
async fn every_typed_reason_survives_a_refusal_written_in_parts() -> TestResult {
    for (status, header, expected) in [
        ("401 Unauthorized", "\"unknown\"", AuthRefusal::Unknown),
        ("401 Unauthorized", "\"expired\"", AuthRefusal::Expired),
        ("401 Unauthorized", "\"revoked\"", AuthRefusal::Revoked),
        ("403 Forbidden", "\"wrongKind\"", AuthRefusal::WrongKind),
    ] {
        let actual =
            split_refusal(status, &format!("fleet-sync-auth-refusal: {header}\r\n")).await?;
        assert_eq!(
            actual,
            Some(ConnectFailure::CredentialRejected(expected)),
            "{header}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn missing_malformed_unknown_or_ambiguous_headers_never_guess_a_reason() -> TestResult {
    for (status, headers) in [
        ("401 Unauthorized", ""),
        ("403 Forbidden", ""),
        ("401 Unauthorized", "fleet-sync-auth-refusal: expired\r\n"),
        (
            "401 Unauthorized",
            "fleet-sync-auth-refusal: \"future\"\r\n",
        ),
        (
            "401 Unauthorized",
            "fleet-sync-auth-refusal: {\"kind\":\"expired\"}\r\n",
        ),
        (
            "401 Unauthorized",
            "fleet-sync-auth-refusal: {\"expired\":null}\r\n",
        ),
        (
            "401 Unauthorized",
            "fleet-sync-auth-refusal: \"expired\" trailing\r\n",
        ),
        (
            "401 Unauthorized",
            "fleet-sync-auth-refusal: \"expired\", \"revoked\"\r\n",
        ),
        (
            "401 Unauthorized",
            "fleet-sync-auth-refusal: \"expired\"\r\nFleet-Sync-Auth-Refusal: \"expired\"\r\n",
        ),
        (
            "403 Forbidden",
            "fleet-sync-auth-refusal: \"wrongKind\"\r\nfleet-sync-auth-refusal: \"expired\"\r\n",
        ),
    ] {
        assert_eq!(
            split_refusal(status, headers).await?,
            Some(ConnectFailure::CredentialRejected(AuthRefusal::Unknown)),
            "{status} / {headers:?}"
        );
    }
    assert_eq!(
        split_refusal(
            "500 Internal Server Error",
            "fleet-sync-auth-refusal: \"expired\"\r\n"
        )
        .await?,
        Some(ConnectFailure::Unavailable)
    );
    Ok(())
}
