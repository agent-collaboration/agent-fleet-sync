use crate::turso_registry_fixture::*;
use durable_steps::CallId;
use fleet_sync_protocol::*;
use fleet_sync_receiver::{AppliedPiece, FollowUpDone, FollowUpFailure, ReceiverRules};
use sqlx_core::connection::Connection;
use std::sync::Arc;

struct EmptyHook;
impl ReceiverRules<CopyScope<'_>> for EmptyHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut CopyScope<'_>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, fleet_sync_receiver::HookError> {
        Ok(Vec::new())
    }
}
impl fleet_sync_receiver::FollowUpHandler for EmptyHook {
    type Item = ();
    async fn follow_up(&self, _: &(), _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

pub(super) async fn push_rows(
    host: &Arc<TestHost>,
    commit: u64,
) -> TestResult<ReplicationPosition> {
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    let mut transaction = copy.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = CopyScope(&mut transaction);
    let CopyStatusAnswer::Status(status) = fleet_sync_receiver::copy_status(&mut scope).await?
    else {
        return Err("copy is not following".into());
    };
    let through = ReplicationPosition::new(status.fence, CommitSequence::new(commit));
    if !host.with_schema {
        return Err("fixture schema is disabled".into());
    }
    let mut transactions = Vec::new();
    if status.schema.version == 0 {
        transactions.push(ReplicatedTransaction::SchemaStep {
            commit_id: CommitSequence::new(
                commit
                    .checked_sub(1)
                    .ok_or("schema needs preceding commit")?,
            ),
            migration_version: 1,
            statements: vec![
                "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL)".into(),
            ],
        });
    }
    transactions.push(ReplicatedTransaction::Rows {
        commit_id: CommitSequence::new(commit),
        rows: vec![RowChange::upsert(
            "records",
            vec![SqlValue::Text(
                "019eef00-0000-7000-8000-000000000001".into(),
            )],
            vec![
                SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
                SqlValue::Text("kept row".into()),
            ],
        )?],
    });
    let piece = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: status.fence,
        continues_from: status.acked,
        through,
        transactions,
        part: None,
        annotation: None,
    })?;
    fleet_sync_receiver::apply_piece(
        &mut scope,
        &piece,
        &status.holder.ok_or("holder missing")?,
        &EmptyHook,
        SyncTime::from_unix_millis(1000),
    )
    .await?;
    transaction.commit().await?;
    Ok(through)
}
pub(super) async fn seed_import(host: &Arc<TestHost>) -> TestResult<ReplicationPosition> {
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    let mut transaction = copy.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = CopyScope(&mut transaction);
    let seed = fleet_sync_receiver::seed_begin(
        &mut scope,
        database()?,
        SeedAttemptId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000384,
        ))?,
        &fleet_sync_receiver::SeedAuthority::Admin,
    )
    .await?;
    let position = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(7));
    let sql = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT NOT NULL)";
    let schema = SchemaIdentity::new(
        3,
        schema_fingerprint(&[SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "records".into(),
            sql: sql.into(),
        }])?,
    )?;
    let row = vec![
        SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
        SqlValue::Text("imported row".into()),
    ];
    let contents = SnapshotPage {
        database: database()?,
        position,
        schema: schema.clone(),
        schema_steps: vec![sql.into()],
        table: "records".into(),
        rows: vec![row.clone()],
        next: None,
    };
    let page = SeedPage {
        seed,
        page: 0,
        digest: PageDigest::of(&contents)?,
        contents,
    };
    if fleet_sync_receiver::seed_page(
        &mut scope,
        &page,
        &fleet_sync_receiver::SeedAuthority::Admin,
    )
    .await?
        != SeedPageAnswer::Staged
    {
        return Err("seed page not staged".into());
    }
    let mut digest = TableDigestBuilder::new();
    digest.push_row(&row)?;
    let done = SeedComplete {
        database: database()?,
        seed,
        position,
        pages: 1,
        schema,
        table_digests: vec![(TableName::new("records")?, digest.finish())],
    };
    if !matches!(fleet_sync_receiver::seed_complete(&mut scope, &done, &EmptyHook, &fleet_sync_receiver::SeedAuthority::Admin).await?, fleet_sync_receiver::SeedDecision::Published { position: actual, follow_ups } if actual == position && follow_ups.is_empty())
    {
        return Err("seed not published".into());
    }
    transaction.commit().await?;
    Ok(position)
}
