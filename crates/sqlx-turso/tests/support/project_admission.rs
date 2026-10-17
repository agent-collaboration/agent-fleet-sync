//! A Router-shaped admission path over the project-store slice, written with checked queries
//!
//! It proves the storage path, not Router's rules: a task's state, its event and the ledger
//! decision commit together in one `BEGIN IMMEDIATE` transaction, and a snapshot read in one
//! transaction sees them together. [`ProjectSnapshot::read`] checks that agreement exactly.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx_turso::{TursoConnection, sqlx::Connection};

use super::{TestResult, scratch_store::foreign_keys_enabled};

pub const PROJECT_ID: &str = "019a0000-0000-7000-8000-000000000001";
pub const DEFAULT_MILESTONE_ID: &str = "019a0000-0000-7000-8000-000000000002";
pub const WRITER_EPOCH: i64 = 1;

/// Task states this fixture moves through
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Open,
    Done,
}

impl TaskStatus {
    fn as_stored(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Done => "done",
        }
    }
}

/// The typed payload stored as JSON TEXT in `events.payload`
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum TaskEventPayload {
    TaskCreated {
        task_id: String,
        status: TaskStatus,
        revision: i64,
    },
    TaskMoved {
        task_id: String,
        status: TaskStatus,
        revision: i64,
    },
}

impl TaskEventPayload {
    fn kind(&self) -> &'static str {
        match self {
            Self::TaskCreated { .. } => "TaskCreated",
            Self::TaskMoved { .. } => "TaskMoved",
        }
    }

    fn task_state(&self) -> (&str, TaskStatus, i64) {
        match self {
            Self::TaskCreated {
                task_id,
                status,
                revision,
            }
            | Self::TaskMoved {
                task_id,
                status,
                revision,
            } => (task_id, *status, *revision),
        }
    }
}

/// The ledger decision stored as JSON TEXT in `request_ledger.decision`
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerDecision {
    pub position: i64,
    pub epoch: i64,
}

pub fn task_id(sequence: i64) -> String {
    format!("019a0001-0000-7000-8000-{sequence:012}")
}

fn create_request_id(sequence: i64) -> String {
    format!("019a0002-0000-7000-8000-{sequence:012}")
}

fn move_request_id(sequence: i64) -> String {
    format!("019a0003-0000-7000-8000-{sequence:012}")
}

/// A deterministic router time per feed position, so tests never read the wall clock
fn router_time(position: i64) -> TestResult<DateTime<Utc>> {
    Utc.timestamp_opt(1_790_000_000 + position, 0)
        .single()
        .ok_or_else(|| format!("no timestamp for position {position}").into())
}

/// Creates the project, its default milestone and the first writer epoch
pub async fn initialize_project(connection: &mut TursoConnection) -> TestResult {
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_turso::query!(
        "INSERT INTO project (id, status, revision) VALUES (?, ?, ?)",
        PROJECT_ID,
        "active",
        1_i64
    )
    .execute(&mut *transaction)
    .await?;
    sqlx_turso::query!(
        "INSERT INTO milestones (id, project_id, is_default, status, revision) VALUES (?, ?, ?, ?, ?)",
        DEFAULT_MILESTONE_ID,
        PROJECT_ID,
        true,
        "open",
        1_i64
    )
    .execute(&mut *transaction)
    .await?;
    sqlx_turso::query!(
        "INSERT INTO writer_epochs (epoch, project_id, holder, start_position) VALUES (?, ?, ?, ?)",
        WRITER_EPOCH,
        PROJECT_ID,
        "writer-a",
        1_i64
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn next_feed_position(connection: &mut TursoConnection) -> sqlx::Result<i64> {
    sqlx_turso::query_scalar!(
        r#"SELECT COALESCE(MAX(position), 0) + 1 AS "next!: i64" FROM events"#
    )
    .fetch_one(connection)
    .await
}

async fn record_event(
    connection: &mut TursoConnection,
    request_id: &str,
    payload: &TaskEventPayload,
) -> TestResult {
    let position = next_feed_position(connection).await?;
    let decision = serde_json::to_string(&LedgerDecision {
        position,
        epoch: WRITER_EPOCH,
    })?;
    sqlx_turso::query!(
        "INSERT INTO request_ledger (request_id, canonical_hash, decision) VALUES (?, ?, ?)",
        request_id,
        format!("hash-{request_id}"),
        decision
    )
    .execute(&mut *connection)
    .await?;
    let (task_id, _, _) = payload.task_state();
    sqlx_turso::query!(
        "INSERT INTO events (position, epoch, kind, subject_id, request_id, router_time, payload)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
        position,
        WRITER_EPOCH,
        payload.kind(),
        task_id,
        request_id,
        router_time(position)?,
        serde_json::to_string(payload)?
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

/// Admits task `sequence`: the task depends on the previous one; state, event and decision
/// commit together
pub async fn admit_task(connection: &mut TursoConnection, sequence: i64) -> TestResult {
    let task = task_id(sequence);
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_turso::query!(
        "INSERT INTO tasks (id, milestone_id, status, revision) VALUES (?, ?, ?, ?)",
        task,
        DEFAULT_MILESTONE_ID,
        TaskStatus::Open.as_stored(),
        1_i64
    )
    .execute(&mut *transaction)
    .await?;
    if sequence > 1 {
        sqlx_turso::query!(
            "INSERT INTO task_dependencies (task_id, dependency_id) VALUES (?, ?)",
            task,
            task_id(sequence - 1)
        )
        .execute(&mut *transaction)
        .await?;
    }
    let payload = TaskEventPayload::TaskCreated {
        task_id: task,
        status: TaskStatus::Open,
        revision: 1,
    };
    record_event(&mut transaction, &create_request_id(sequence), &payload).await?;
    transaction.commit().await?;
    Ok(())
}

/// Moves task `sequence` to done; state, event and decision commit together
pub async fn complete_task(connection: &mut TursoConnection, sequence: i64) -> TestResult {
    let task = task_id(sequence);
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_turso::query!(
        "UPDATE tasks SET status = ?, revision = revision + 1 WHERE id = ?",
        TaskStatus::Done.as_stored(),
        task
    )
    .execute(&mut *transaction)
    .await?;
    let revision = sqlx_turso::query_scalar!(
        r#"SELECT revision AS "revision!: i64" FROM tasks WHERE id = ?"#,
        task
    )
    .fetch_one(&mut *transaction)
    .await?;
    let payload = TaskEventPayload::TaskMoved {
        task_id: task,
        status: TaskStatus::Done,
        revision,
    };
    record_event(&mut transaction, &move_request_id(sequence), &payload).await?;
    transaction.commit().await?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskRow {
    pub id: String,
    pub milestone_id: String,
    pub status: String,
    pub revision: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRow {
    pub task_id: String,
    pub dependency_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventRow {
    pub position: i64,
    pub epoch: i64,
    pub kind: String,
    pub subject_id: String,
    pub request_id: String,
    pub router_time: DateTime<Utc>,
    pub payload: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerRow {
    pub request_id: String,
    pub decision: String,
}

/// Every project row, read in one transaction and checked for exact agreement
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectSnapshot {
    pub tasks: Vec<TaskRow>,
    pub dependencies: Vec<DependencyRow>,
    pub events: Vec<EventRow>,
    pub ledger: Vec<LedgerRow>,
}

impl ProjectSnapshot {
    /// Reads the project in one transaction, then fails unless the rows agree exactly: dense
    /// feed positions, one decision per event naming its position and epoch, every task's
    /// latest event matching its stored state, foreign keys enforced and none dangling
    pub async fn read(connection: &mut TursoConnection) -> TestResult<Self> {
        let mut transaction = connection.begin().await?;
        let tasks = sqlx_turso::query_as!(
            TaskRow,
            r#"SELECT id AS "id!: String", milestone_id AS "milestone_id!: String",
                      status AS "status!: String", revision AS "revision!: i64"
               FROM tasks ORDER BY id"#
        )
        .fetch_all(&mut *transaction)
        .await?;
        let dependencies = sqlx_turso::query_as!(
            DependencyRow,
            r#"SELECT task_id AS "task_id!: String", dependency_id AS "dependency_id!: String"
               FROM task_dependencies ORDER BY task_id, dependency_id"#
        )
        .fetch_all(&mut *transaction)
        .await?;
        let events = sqlx_turso::query_as!(
            EventRow,
            r#"SELECT position AS "position!: i64", epoch AS "epoch!: i64", kind AS "kind!: String",
                      subject_id AS "subject_id!: String", request_id AS "request_id!: String",
                      router_time AS "router_time!: chrono::DateTime<chrono::Utc>",
                      payload AS "payload!: String"
               FROM events ORDER BY position"#
        )
        .fetch_all(&mut *transaction)
        .await?;
        let ledger = sqlx_turso::query_as!(
            LedgerRow,
            r#"SELECT request_id AS "request_id!: String", decision AS "decision!: String"
               FROM request_ledger ORDER BY request_id"#
        )
        .fetch_all(&mut *transaction)
        .await?;
        let foreign_keys_on = foreign_keys_enabled(&mut transaction).await?;
        let dangling = sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut *transaction)
            .await?;
        transaction.commit().await?;

        let snapshot = Self {
            tasks,
            dependencies,
            events,
            ledger,
        };
        if !foreign_keys_on {
            return Err("foreign keys are not enforced".into());
        }
        if !dangling.is_empty() {
            return Err(format!("{} dangling foreign-key references", dangling.len()).into());
        }
        snapshot.check_agreement()?;
        Ok(snapshot)
    }

    fn check_agreement(&self) -> TestResult {
        let decisions: BTreeMap<&str, LedgerDecision> = self
            .ledger
            .iter()
            .map(|row| {
                Ok((
                    row.request_id.as_str(),
                    serde_json::from_str(&row.decision)?,
                ))
            })
            .collect::<TestResult<_>>()?;
        if decisions.len() != self.events.len() {
            return Err(format!(
                "{} ledger decisions for {} events",
                decisions.len(),
                self.events.len()
            )
            .into());
        }

        let mut latest_state: BTreeMap<String, (TaskStatus, i64)> = BTreeMap::new();
        for (expected_position, event) in (1_i64..).zip(&self.events) {
            if event.position != expected_position {
                return Err(format!("feed position {} is not dense", event.position).into());
            }
            let decision = decisions
                .get(event.request_id.as_str())
                .ok_or_else(|| format!("event {} has no ledger decision", event.position))?;
            if decision.position != event.position || decision.epoch != event.epoch {
                return Err(format!(
                    "decision {decision:?} disagrees with event {}",
                    event.position
                )
                .into());
            }
            let payload: TaskEventPayload = serde_json::from_str(&event.payload)?;
            let (task_id, status, revision) = payload.task_state();
            if payload.kind() != event.kind || task_id != event.subject_id {
                return Err(format!(
                    "event {} payload disagrees with its columns",
                    event.position
                )
                .into());
            }
            latest_state.insert(task_id.to_owned(), (status, revision));
        }

        if latest_state.len() != self.tasks.len() {
            return Err(format!(
                "{} tasks but events for {}",
                self.tasks.len(),
                latest_state.len()
            )
            .into());
        }
        for task in &self.tasks {
            let (status, revision) = latest_state
                .get(&task.id)
                .ok_or_else(|| format!("task {} has no event", task.id))?;
            if status.as_stored() != task.status || *revision != task.revision {
                return Err(
                    format!("task {} state disagrees with its latest event", task.id).into(),
                );
            }
        }
        Ok(())
    }
}
