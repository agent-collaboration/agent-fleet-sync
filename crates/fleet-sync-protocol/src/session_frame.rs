use crate::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// The consumer owns these payloads; synchronization never interprets them.
pub trait ConsumerProtocol: Send + Sync + 'static {
    type Delivery: DeliveryTypes;
    type Request: Serialize + DeserializeOwned + Send + Sync;
    type Answer: Serialize + DeserializeOwned + Send + Sync;
    type ServerRequest: Serialize + DeserializeOwned + Send + Sync;
    type ClientAnswer: Serialize + DeserializeOwned + Send + Sync;
    type Notice: Serialize + DeserializeOwned + Send + Sync;
    type Annotation: Serialize + DeserializeOwned + Send + Sync;
}

/// A protocol with no application payload, useful to replication-only consumers.
pub struct NoConsumer;
impl ConsumerProtocol for NoConsumer {
    type Delivery = NoDelivery;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = ();
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum CopyUnavailableReason {
    Uninitialized,
    StorageFull,
    Damaged { detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum DatabaseStatus {
    Ready {
        database: DatabaseId,
        acked: ReplicationPosition,
        lease: LeaseStatus,
    },
    Unavailable {
        database: DatabaseId,
        lease: Option<LeaseStatus>,
        reason: CopyUnavailableReason,
    },
    NeedsSnapshot {
        database: DatabaseId,
        acked: ReplicationPosition,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound(
        serialize = "A: Serialize",
        deserialize = "A: Serialize + Deserialize<'de>"
    )
)]
pub enum ReplicationRequest<A = ()> {
    Piece(Piece<A>),
    SeedBegin {
        database: DatabaseId,
        attempt: SeedAttemptId,
    },
    SeedPage(Box<SeedPage>),
    SeedComplete(SeedComplete),
    SnapshotManifest {
        database: DatabaseId,
        position: ReplicationPosition,
    },
    SnapshotPage {
        database: DatabaseId,
        cursor: Option<SnapshotCursor>,
    },
    CopyStatus {
        database: DatabaseId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HookRefusal {
    pub code: HookRefusalCode,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum PieceRefusal {
    StaleEpoch {
        newer: Epoch,
        ended_at: ReplicationPosition,
    },
    NotWriter {
        current: Option<Epoch>,
    },
    Gap {
        acked: ReplicationPosition,
    },
    PieceConflict,
    SchemaUnsupported {
        form: SchemaStatementForm,
    },
    NeedsSnapshot {
        acked: ReplicationPosition,
    },
    NotInitialized,
    CopyFull,
    Retired,
    Hook(HookRefusal),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum ReplicationAnswer {
    CopyUnavailable {
        reason: CopyUnavailableReason,
    },
    OutcomeUnknown {
        reason: OutcomeUnknownReason,
    },
    Applied {
        through: ReplicationPosition,
    },
    AlreadyApplied {
        through: ReplicationPosition,
    },
    PartStaged {
        transaction: TransactionKey,
        index: u32,
        acked: ReplicationPosition,
    },
    Refused(PieceRefusal),
    SeedStarted {
        seed: SeedId,
    },
    SeedPage(SeedPageAnswer),
    SeedComplete(SeedAnswer),
    SnapshotPage(SnapshotRead),
    SnapshotManifest(SnapshotManifestAnswer),
    CopyStatus(CopyStatusAnswer),
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum OperationRequest<P: ConsumerProtocol = NoConsumer> {
    Replication(ReplicationRequest<P::Annotation>),
    Lease(LeaseRequest),
    Delivery(DeliveryRequest<P::Delivery>),
    Consumer(P::Request),
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum OperationAnswer<P: ConsumerProtocol = NoConsumer> {
    InvalidShape { field: String },
    Replication(ReplicationAnswer),
    Lease(LeaseAnswer),
    Delivery(DeliveryAnswer<P::Delivery>),
    Consumer(P::Answer),
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum Notice<P: ConsumerProtocol = NoConsumer> {
    LeaseChanged { database: DatabaseId },
    Consumer(P::Notice),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthRefusal {
    Unknown,
    Expired,
    Revoked,
    WrongKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum CloseReason {
    ConnectionReplaced,
    SessionEnded,
    CredentialRejected(AuthRefusal),
    MalformedFrame,
    ClientClosed,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum ClientFrame<P: ConsumerProtocol = NoConsumer> {
    Hello {
        session_id: SessionId,
        schema_support: SchemaSupport,
    },
    Request {
        frame: FrameId,
        body: OperationRequest<P>,
    },
    Answer {
        frame: FrameId,
        body: ClientAnswer<P>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum ServerFrame<P: ConsumerProtocol = NoConsumer> {
    Welcome {
        session_id: SessionId,
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
    Closed {
        reason: CloseReason,
    },
}
