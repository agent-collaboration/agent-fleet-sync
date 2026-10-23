use crate::{sender_test_fixture::*, *};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{AppliedPiece, ApplyDecision, ApplyFailure, HookError, ReceiverRules};
use sqlx_core::{connection::Connection, query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Mutex;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct Annotation {
    pub text: String,
    pub origin: u64,
}
pub(super) struct Protocol;
impl ConsumerProtocol for Protocol {
    type Delivery = NoDelivery;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = Annotation;
}
pub(super) struct Annotator {
    pub bytes: usize,
}
impl PieceAnnotation for Annotator {
    type Protocol = Protocol;
    fn annotate(&self, transactions: &[CapturedTransaction<'_>]) -> Option<Annotation> {
        transactions.first().map(|transaction| Annotation {
            text: "a".repeat(self.bytes),
            origin: transaction.position.sequence.value(),
        })
    }
}
struct Rules;
impl ReceiverRules<crate::writer_scope::WriterScope<'_>> for Rules {
    type Protocol = Protocol;
    type FollowUp = ();
    async fn on_applied(
        &self,
        scope: &mut crate::writer_scope::WriterScope<'_>,
        applied: &AppliedPiece<'_, Annotation>,
    ) -> Result<Vec<()>, HookError> {
        let annotation = applied.annotation.ok_or_else(|| {
            HookError::Storage(fleet_sync_receiver::StoreError::Corrupt {
                field: "missing whole annotation".into(),
            })
        })?;
        query::<Turso>("INSERT INTO hook_calls(position) VALUES(?)")
            .bind(serde_json::to_string(annotation).map_err(|error| {
                HookError::Storage(fleet_sync_receiver::StoreError::Corrupt {
                    field: error.to_string(),
                })
            })?)
            .execute(&mut *scope.0)
            .await
            .map_err(fleet_sync_receiver::classify_sqlx_error)?;
        Ok(Vec::new())
    }
}

#[derive(Clone)]
pub(super) struct ObservedUnit {
    pub encoded: Vec<u8>,
    pub part: Option<TransactionPart>,
    pub rows: usize,
    pub annotated: bool,
    pub framed_bytes: usize,
}
pub(super) struct MeasuredTransport {
    pub inner: Arc<NativeTransport>,
    pub observed: Mutex<Vec<ObservedUnit>>,
    pub lose_final: AtomicBool,
    pub largest_staged_fields: AtomicUsize,
}
impl MeasuredTransport {
    pub fn new(inner: Arc<NativeTransport>, lose_final: bool) -> Self {
        Self {
            inner,
            observed: Mutex::new(Vec::new()),
            lose_final: AtomicBool::new(lose_final),
            largest_staged_fields: AtomicUsize::new(0),
        }
    }
}
fn unavailable(error: impl std::fmt::Display) -> SendFailure {
    SendFailure::Unavailable {
        message: error.to_string(),
    }
}
impl ReplicationTransport<Protocol> for MeasuredTransport {
    async fn send_piece(
        &self,
        copy: ReceiverId,
        piece: Piece<Annotation>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        let encoded = serde_json::to_vec(&piece).map_err(unavailable)?;
        let part = piece.part.clone();
        let rows = match piece.transactions.first() {
            Some(ReplicatedTransaction::Rows { rows, .. }) => rows.len(),
            _ => 0,
        };
        let annotated = piece.annotation.is_some();
        let request = ClientFrame::<Protocol>::Request {
            frame: FrameId::new(durable_steps::Uuid::from_u128(1)).map_err(unavailable)?,
            body: OperationRequest::Replication(ReplicationRequest::Piece(piece)),
        };
        let framed = serde_json::to_vec(&request).map_err(unavailable)?;
        assert!(
            framed.len() <= 1536 * 1024,
            "piece request frame encoded to {} bytes",
            framed.len()
        );
        let ClientFrame::Request {
            body: OperationRequest::Replication(ReplicationRequest::Piece(piece)),
            ..
        } = request
        else {
            return Err(unavailable("unexpected measured request"));
        };
        self.observed.lock().await.push(ObservedUnit {
            encoded,
            part,
            rows,
            annotated,
            framed_bytes: framed.len(),
        });
        let key = format!("{}:{}", piece.epoch.value(), piece.through.sequence.value());
        for sql in [
            "SELECT record FROM _fleet_sync_capture_bookkeeping WHERE commit_id=?",
            "SELECT id FROM local_marks WHERE id=?",
        ] {
            assert!(
                query::<Turso>(sql)
                    .bind(&key)
                    .fetch_optional(self.inner.writer.reader())
                    .await
                    .map_err(unavailable)?
                    .is_some(),
                "send preceded committed capture bookkeeping"
            );
        }
        let file = self
            .inner
            .files
            .get(&copy)
            .ok_or_else(|| unavailable("missing real copy"))?;
        let mut owner = file.connection.lock().await;
        let connection = owner
            .as_mut()
            .ok_or_else(|| unavailable("closed real copy"))?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        let peer = PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))
        .map_err(unavailable)?;
        let decision = fleet_sync_receiver::apply_piece(
            &mut crate::writer_scope::WriterScope(&mut transaction),
            &piece,
            &peer,
            &Rules,
            runtime_primitives::Clock::now(&runtime_primitives::SystemClock),
        )
        .await;
        let answer = match decision {
            Ok(decision) => {
                transaction.commit().await.map_err(unavailable)?;
                match decision {
                    ApplyDecision::Applied { through, .. } => {
                        ReplicationAnswer::Applied { through }
                    }
                    ApplyDecision::AlreadyApplied { through } => {
                        ReplicationAnswer::AlreadyApplied { through }
                    }
                    ApplyDecision::PartStaged {
                        transaction,
                        index,
                        acked,
                    } => ReplicationAnswer::PartStaged {
                        transaction,
                        index,
                        acked,
                    },
                }
            }
            Err(error) => {
                transaction.rollback().await.map_err(unavailable)?;
                return Err(unavailable(match error {
                    ApplyFailure::Refused(reason) => format!("real apply refused: {reason:?}"),
                    error => error.to_string(),
                }));
            }
        };
        if matches!(answer, ReplicationAnswer::PartStaged { .. }) {
            let exposed: i64 = query::<Turso>(
                "SELECT count(*) AS rows FROM text_records WHERE length(label)>1024",
            )
            .fetch_one(&mut *connection)
            .await
            .map_err(unavailable)?
            .try_get("rows")
            .map_err(unavailable)?;
            assert_eq!(
                exposed, 0,
                "staging exposed a row before whole-transaction apply"
            );
            let annotations: i64 =
                query::<Turso>("SELECT count(*) AS rows FROM hook_calls WHERE position LIKE '{%'")
                    .fetch_one(&mut *connection)
                    .await
                    .map_err(unavailable)?
                    .try_get("rows")
                    .map_err(unavailable)?;
            assert_eq!(
                annotations, 0,
                "staging applied an annotation before the final part"
            );
        }
        // Read the actual persisted payload and metadata after each real stage.
        for row in query::<Turso>("SELECT length(CAST(payload AS BLOB))+length(CAST(transaction_key AS BLOB))+length(CAST(staged_at AS BLOB))+8 AS bytes FROM _fleet_sync_staged_parts")
            .fetch_all(&mut *connection).await.map_err(unavailable)? {
            let bytes = usize::try_from(row.try_get::<i64, _>("bytes").map_err(unavailable)?).map_err(unavailable)?;
            assert!(bytes + 128 <= 1536 * 1024, "actual staged part fields exceed the complete-unit hard cap");
            self.largest_staged_fields.fetch_max(bytes, Ordering::SeqCst);
        }
        if matches!(
            answer,
            ReplicationAnswer::Applied { .. } | ReplicationAnswer::AlreadyApplied { .. }
        ) && self.lose_final.swap(false, Ordering::SeqCst)
        {
            return Err(SendFailure::AnswerLost);
        }
        Ok(answer)
    }
}

pub(super) async fn last_capture<S: ReplicatedSchema>(
    database: &ReplicatedDatabase<S>,
) -> TestResult<(ReplicationPosition, crate::capture_stream::CapturedCommit)> {
    let _control = database.control.lock().await;
    let mut connection = database.maintenance.acquire().await?;
    let mut transaction = connection.begin_with("BEGIN").await?;
    let record = crate::writer_file_record::read_file_record(&mut transaction)
        .await?
        .ok_or("missing source record")?;
    let mut commits =
        crate::capture_stream::read_window(&mut transaction, &record, record.retained_after, 64)
            .await?;
    let last = commits.pop().ok_or("no committed capture")?;
    let after = commits
        .last()
        .map_or(record.retained_after, |commit| commit.position);
    transaction.rollback().await?;
    Ok((after, last))
}
