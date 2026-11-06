#![allow(clippy::unwrap_used)]
use super::*;
use tokio_tungstenite::tungstenite::protocol::Role;

#[tokio::test(start_paused = true)]
async fn stalled_socket_send_reports_send_timeout_then_stays_closed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    let (_unread_server, _address) = accepted.unwrap();
    let socket = WebSocketStream::from_raw_socket(
        MaybeTlsStream::Plain(client.unwrap()),
        Role::Client,
        None,
    )
    .await;
    let mut connection = WebSocketConnection {
        socket,
        send_timeout: Duration::from_secs(1),
        closed: false,
    };
    // Larger than the socket buffers; the peer deliberately never reads.
    let frame = Bytes::from(vec![0; 16 * 1024 * 1024]);
    assert_eq!(
        connection.send(frame).await,
        Err(ConnectionClosed::SendTimeout)
    );
    assert!(connection.closed);
    assert_eq!(
        connection.send(Bytes::new()).await,
        Err(ConnectionClosed::Transport)
    );
}
