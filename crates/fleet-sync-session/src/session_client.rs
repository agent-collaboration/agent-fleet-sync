use crate::*;
use fleet_sync_protocol::*;
use runtime_primitives::{Backoff, Clock, IdSource, WakeFailure, WakeScheduler};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, VecDeque},
    marker::PhantomData,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientSessionState {
    pub session_id: SessionId,
    pub notice_cursor: NoticePosition,
}

pub trait SessionClientStore: Send + Sync + 'static {
    fn load(&self) -> impl Future<Output = Result<Option<ClientSessionState>, StoreError>> + Send;
    fn save(
        &self,
        state: &ClientSessionState,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Connect(#[from] ConnectFailure),
    #[error(transparent)]
    Connection(#[from] ConnectionClosed),
    #[error(transparent)]
    Wake(#[from] WakeFailure),
    #[error("cannot encode session frame")]
    Encoding,
    #[error("invalid server session frame")]
    Protocol,
    #[error("session has no current connection")]
    NotConnected,
    #[error("frame is already in flight")]
    FrameInFlight,
    #[error("reconnect is due at {at:?}")]
    RetryAt { at: SyncTime },
    #[error("the session ended; start a new session after authentication")]
    SessionEnded,
}

pub enum ClientEvent<P: ConsumerProtocol = NoConsumer> {
    Welcome {
        databases: Vec<DatabaseStatus>,
    },
    Answer {
        frame: FrameId,
        body: OperationAnswer<P>,
    },
    Request {
        frame: FrameId,
        body: ServerRequest<P>,
    },
    Notice {
        position: NoticePosition,
        body: Notice<P>,
    },
    Unknown {
        frames: Vec<FrameId>,
    },
    Disconnected {
        reason: Option<CloseReason>,
        retry_at: Option<SyncTime>,
    },
}

/// Runtime-free owner of one current connection. Call `connect` on the host's
/// wake, then pump `receive`; the client never starts a background task. A lost
/// connection emits Unknown for every pending frame. Its operation owners, not
/// this correlation layer, choose which operation identities to resend.
pub struct SessionClient<
    T: SessionTransport,
    S: SessionClientStore,
    P: ConsumerProtocol = NoConsumer,
> {
    transport: T,
    store: S,
    state: ClientSessionState,
    schema_support: SchemaSupport,
    backoff: Backoff,
    attempt: u32,
    retry_at: Option<SyncTime>,
    ended: bool,
    connection: Option<T::Connection>,
    pending: BTreeSet<FrameId>,
    events: VecDeque<ClientEvent<P>>,
    notice_seen: NoticePosition,
    protocol: PhantomData<P>,
}

impl<T: SessionTransport, S: SessionClientStore, P: ConsumerProtocol> SessionClient<T, S, P> {
    pub async fn load(
        transport: T,
        store: S,
        ids: &impl IdSource,
        schema_support: SchemaSupport,
        backoff: Backoff,
    ) -> Result<Self, ClientError> {
        let state = match store.load().await? {
            Some(state) => state,
            None => {
                let session_id = SessionId::new(ids.new_id()).map_err(|_| ClientError::Protocol)?;
                let state = ClientSessionState {
                    session_id,
                    notice_cursor: NoticePosition::default(),
                };
                store.save(&state).await?;
                state
            }
        };
        let notice_seen = state.notice_cursor;
        Ok(Self {
            transport,
            store,
            state,
            schema_support,
            backoff,
            attempt: 0,
            retry_at: None,
            ended: false,
            connection: None,
            pending: BTreeSet::new(),
            events: VecDeque::new(),
            notice_seen,
            protocol: PhantomData,
        })
    }

    pub fn state(&self) -> &ClientSessionState {
        &self.state
    }
    pub const fn reconnect_at(&self) -> Option<SyncTime> {
        self.retry_at
    }

    pub async fn connect<H: Clock + WakeScheduler>(
        &mut self,
        endpoint: &Endpoint,
        credential: &PeerCredential,
        host: &H,
    ) -> Result<(), ClientError> {
        if self.ended {
            return Err(ClientError::SessionEnded);
        }
        if self.connection.is_some() {
            return Ok(());
        }
        if let Some(at) = self.retry_at
            && host.now() < at
        {
            return Err(ClientError::RetryAt { at });
        }
        let mut connection = match self.transport.connect(endpoint, credential).await {
            Ok(connection) => connection,
            Err(error) => {
                if !matches!(error, ConnectFailure::CredentialRejected(_)) {
                    self.schedule_retry(host).await?;
                }
                return Err(error.into());
            }
        };
        let hello: ClientFrame<P> = ClientFrame::Hello {
            session_id: self.state.session_id,
            schema_support: self.schema_support,
        };
        let encoded = serde_json::to_vec(&hello).map_err(|_| ClientError::Encoding)?;
        if let Err(error) = connection.send(encoded.into()).await {
            connection.close(CloseReason::ClientClosed).await;
            self.schedule_retry(host).await?;
            return Err(error.into());
        }
        self.connection = Some(connection);
        // Welcome is received through the same pump as every other server frame.
        Ok(())
    }

    pub async fn send_request<H: Clock + WakeScheduler>(
        &mut self,
        frame: FrameId,
        body: OperationRequest<P>,
        host: &H,
    ) -> Result<(), ClientError> {
        if self.pending.contains(&frame) {
            return Err(ClientError::FrameInFlight);
        }
        let encoded = serde_json::to_vec(&ClientFrame::<P>::Request { frame, body })
            .map_err(|_| ClientError::Encoding)?;
        let connection = self.connection.as_mut().ok_or(ClientError::NotConnected)?;
        self.pending.insert(frame);
        if let Err(error) = connection.send(encoded.into()).await {
            self.disconnected(None, host).await?;
            return Err(error.into());
        }
        Ok(())
    }

    pub async fn send_answer<H: Clock + WakeScheduler>(
        &mut self,
        frame: FrameId,
        body: ClientAnswer<P>,
        host: &H,
    ) -> Result<(), ClientError> {
        let encoded = serde_json::to_vec(&ClientFrame::<P>::Answer { frame, body })
            .map_err(|_| ClientError::Encoding)?;
        let connection = self.connection.as_mut().ok_or(ClientError::NotConnected)?;
        if let Err(error) = connection.send(encoded.into()).await {
            self.disconnected(None, host).await?;
            return Err(error.into());
        }
        Ok(())
    }

    pub async fn receive<H: Clock + WakeScheduler>(
        &mut self,
        host: &H,
    ) -> Result<ClientEvent<P>, ClientError> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Ok(event);
            }
            let connection = self.connection.as_mut().ok_or(ClientError::NotConnected)?;
            let encoded = match connection.receive().await {
                Some(Ok(encoded)) => encoded,
                Some(Err(_)) | None => {
                    self.disconnected(None, host).await?;
                    continue;
                }
            };
            let frame = match serde_json::from_slice::<ServerFrame<P>>(&encoded) {
                Ok(frame) => frame,
                Err(_) => {
                    self.disconnected(Some(CloseReason::MalformedFrame), host)
                        .await?;
                    return Err(ClientError::Protocol);
                }
            };
            match frame {
                ServerFrame::Welcome {
                    session_id,
                    databases,
                } => {
                    if session_id != self.state.session_id {
                        self.disconnected(Some(CloseReason::MalformedFrame), host)
                            .await?;
                        return Err(ClientError::Protocol);
                    }
                    self.attempt = 0;
                    self.retry_at = None;
                    return Ok(ClientEvent::Welcome { databases });
                }
                ServerFrame::Answer { frame, body } => {
                    if self.pending.remove(&frame) {
                        return Ok(ClientEvent::Answer { frame, body });
                    }
                }
                ServerFrame::Request { frame, body } => {
                    return Ok(ClientEvent::Request { frame, body });
                }
                ServerFrame::Notice { position, body } => {
                    if position > self.state.notice_cursor {
                        self.notice_seen = self.notice_seen.max(position);
                        return Ok(ClientEvent::Notice { position, body });
                    }
                }
                ServerFrame::Closed { reason } => {
                    self.disconnected(Some(reason), host).await?;
                }
            }
        }
    }

    /// Persist only after the consumer has handled a notice, never on receipt alone.
    pub async fn acknowledge_notice(
        &mut self,
        position: NoticePosition,
    ) -> Result<(), ClientError> {
        if position > self.notice_seen {
            return Err(ClientError::Protocol);
        }
        if position <= self.state.notice_cursor {
            return Ok(());
        }
        let updated = ClientSessionState {
            session_id: self.state.session_id,
            notice_cursor: position,
        };
        self.store.save(&updated).await?;
        self.state = updated;
        Ok(())
    }

    pub async fn start_new_session(&mut self, ids: &impl IdSource) -> Result<(), ClientError> {
        let session_id = SessionId::new(ids.new_id()).map_err(|_| ClientError::Protocol)?;
        if session_id == self.state.session_id {
            return Err(ClientError::Protocol);
        }
        let updated = ClientSessionState {
            session_id,
            notice_cursor: NoticePosition::default(),
        };
        self.store.save(&updated).await?;
        if let Some(connection) = self.connection.take() {
            connection.close(CloseReason::ClientClosed).await;
        }
        self.report_unknown();
        self.state = updated;
        self.notice_seen = NoticePosition::default();
        self.ended = false;
        self.attempt = 0;
        self.retry_at = None;
        Ok(())
    }

    pub async fn close(mut self) {
        if let Some(connection) = self.connection.take() {
            connection.close(CloseReason::ClientClosed).await;
        }
    }

    fn report_unknown(&mut self) {
        if !self.pending.is_empty() {
            self.events.push_back(ClientEvent::Unknown {
                frames: std::mem::take(&mut self.pending).into_iter().collect(),
            });
        }
    }
    async fn schedule_retry<H: Clock + WakeScheduler>(
        &mut self,
        host: &H,
    ) -> Result<(), ClientError> {
        let at = host.now().after(self.backoff.delay(self.attempt));
        self.attempt = self.attempt.saturating_add(1);
        self.retry_at = Some(at);
        host.wake_at(at).await?;
        Ok(())
    }
    async fn disconnected<H: Clock + WakeScheduler>(
        &mut self,
        reason: Option<CloseReason>,
        host: &H,
    ) -> Result<(), ClientError> {
        if let Some(connection) = self.connection.take() {
            connection.close(CloseReason::ClientClosed).await;
        }
        self.report_unknown();
        self.ended = matches!(
            reason,
            Some(CloseReason::SessionEnded | CloseReason::CredentialRejected(AuthRefusal::Revoked))
        );
        if self.ended {
            self.retry_at = None;
        } else {
            self.schedule_retry(host).await?;
        }
        self.events.push_back(ClientEvent::Disconnected {
            reason,
            retry_at: self.retry_at,
        });
        Ok(())
    }
}
