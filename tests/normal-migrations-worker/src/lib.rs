#![forbid(unsafe_code)]
mod copy_certainty;
use durable_steps::{CallId, storage::TransactionScope};
use fleet_sync_protocol::{
    CopyStatus, CreateCopy, HookRefusal, HookRefusalCode, NoConsumer, PeerId, Piece,
    SnapshotCursor, SnapshotManifest, SnapshotPage, SyncTime,
};
use fleet_sync_receiver::{
    AppliedPiece, ApplyDecision, FollowUpDone, FollowUpFailure, ReceiverRules, SnapshotInstall,
    SnapshotInstallRules,
};
use serde::Deserialize;
use sqlx::{Executor, Row};
use sqlx_durable_object::{Do, DoConnection, DoTransaction};
use worker::{Context, DurableObject, Env, Request, Response, State, durable_object, event};

struct WorkerScope<'scope>(&'scope DoTransaction);
impl TransactionScope for WorkerScope<'_> {
    type Database = Do;
    type Executor<'query>
        = &'query DoTransaction
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
struct ProbeHook {
    refusal: Option<HookRefusalCode>,
}
impl<S: TransactionScope> ReceiverRules<S> for ProbeHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut S,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, fleet_sync_receiver::HookError> {
        if let Some(code) = &self.refusal {
            return Err(HookRefusal {
                code: code.clone(),
                detail: "injected after schema/rows before receipt".into(),
            }
            .into());
        }
        Ok(Vec::new())
    }
}
impl fleet_sync_receiver::FollowUpHandler for ProbeHook {
    type Item = ();
    async fn follow_up(&self, _: &(), _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "camelCase")]
enum ProbeRequest {
    CertaintyApply {
        piece: Piece,
        sender: PeerId,
        fault: copy_certainty::CopyProbeFault,
    },
    Initialize {
        create: CreateCopy,
        #[serde(default)]
        journal: bool,
    },
    Apply {
        piece: Piece,
        sender: PeerId,
        #[serde(default)]
        refuse: bool,
    },
    Inspect {
        queries: Vec<String>,
    },
    Audit,
    SnapshotPrepare {
        install: SnapshotInstall,
        source: CopyStatus,
    },
    SnapshotPage {
        install: SnapshotInstall,
        index: u32,
        cursor: Option<SnapshotCursor>,
        page: SnapshotPage,
    },
    SnapshotFinish {
        install: SnapshotInstall,
        manifest: SnapshotManifest,
    },
}
fn error(e: impl std::fmt::Display) -> worker::Error {
    worker::Error::RustError(e.to_string())
}
fn storage_error(e: impl std::fmt::Display) -> sqlx::Error {
    sqlx::Error::Protocol(e.to_string())
}

#[durable_object]
pub struct ProbeObject {
    state: State,
}
impl DurableObject for ProbeObject {
    fn new(state: State, _: Env) -> Self {
        Self { state }
    }
    async fn fetch(&self, mut request: Request) -> worker::Result<Response> {
        // A JS JSON parse rounds i64. Rust owns deserialization from raw text.
        let input: ProbeRequest = serde_json::from_str(&request.text().await?).map_err(error)?;
        let conn = DoConnection::new(self.state.storage());
        let reply: serde_json::Value = match input {
            ProbeRequest::CertaintyApply { piece, sender, fault } => serde_json::to_value(
                copy_certainty::apply(&self.state, piece, sender, fault).await?
            ).map_err(error)?,
            ProbeRequest::Initialize { create, journal } => {
                if journal { copy_certainty::migrate_journal(&self.state).await?; }
                conn.transaction(move |tx| async move {
                for migration in [
                    include_str!("../../../crates/fleet-sync-receiver/migrations/0001_copy_rules.sql"),
                    include_str!("../../../crates/fleet-sync-receiver/migrations/0002_replicated_schema.sql"),
                    include_str!("../../../crates/fleet-sync-receiver/migrations/0003_snapshot_install.sql"),
                ] { (&tx).execute(migration).await?; }
                let result = fleet_sync_receiver::create_copy(&mut WorkerScope(&tx), &create).await.map_err(storage_error)?;
                Ok::<_, sqlx::Error>(serde_json::json!({"kind":"initialized", "result":format!("{result:?}")}))
            }).await.map_err(error)? },
            ProbeRequest::Apply { piece, sender, refuse } => {
                let result = conn.transaction(move |tx| async move {
                    let refusal = if refuse { Some(HookRefusalCode::new("probe-refusal").map_err(storage_error)?) } else { None };
                    let decision = fleet_sync_receiver::apply_piece(&mut WorkerScope(&tx), &piece, &sender, &ProbeHook { refusal }, SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
                    Ok::<_, sqlx::Error>(match decision {
                        ApplyDecision::Applied { through, .. } => serde_json::json!({"kind":"applied", "through":through}),
                        ApplyDecision::AlreadyApplied { through } => serde_json::json!({"kind":"duplicate", "through":through}),
                        ApplyDecision::PartStaged { transaction, index, acked } => serde_json::json!({"kind":"staged", "transaction":transaction, "index":index, "acked":acked}),
                    })
                }).await;
                match result { Ok(value) => value, Err(e) => serde_json::json!({"kind":"refused", "error":e.to_string()}) }
            }
            ProbeRequest::Inspect { queries } => {
                let mut results = Vec::new();
                for sql in queries {
                    let rows = sqlx::query::<Do>(sqlx::AssertSqlSafe(sql)).fetch_all(&conn).await.map_err(error)?;
                    results.push(rows.iter().map(|row| row.try_get::<String,_>("result")).collect::<Result<Vec<_>,_>>().map_err(error)?);
                }
                serde_json::json!({"kind":"inspection", "results":results})
            }
            ProbeRequest::SnapshotPrepare { install, source } => conn.transaction(move |tx| async move {
                for migration in [
                    include_str!("../../../crates/fleet-sync-receiver/migrations/0001_copy_rules.sql"),
                    include_str!("../../../crates/fleet-sync-receiver/migrations/0002_replicated_schema.sql"),
                    include_str!("../../../crates/fleet-sync-receiver/migrations/0003_snapshot_install.sql"),
                ] { (&tx).execute(migration).await?; }
                SnapshotInstallRules(&mut WorkerScope(&tx)).prepare(&install,&source).await.map_err(storage_error)?;
                Ok::<_,sqlx::Error>(serde_json::json!({"kind":"prepared"}))
            }).await.map_err(error)?,
            ProbeRequest::SnapshotPage { install,index,cursor,page } => conn.transaction(move |tx| async move {
                let receipt=SnapshotInstallRules(&mut WorkerScope(&tx)).stage_page(&install,index,cursor.as_ref(),&page).await.map_err(storage_error)?;
                Ok::<_,sqlx::Error>(serde_json::json!({"kind":"pageStaged","rows":receipt.rows}))
            }).await.map_err(error)?,
            ProbeRequest::SnapshotFinish { install,manifest } => conn.transaction(move |tx| async move {
                let mut scope=WorkerScope(&tx);
                SnapshotInstallRules(&mut scope).verify(&install,&manifest).await.map_err(storage_error)?;
                let ready=SnapshotInstallRules(&mut scope).mark_ready(&install,&ProbeHook {refusal:None}).await.map_err(storage_error)?;
                let installed=match ready {fleet_sync_receiver::SnapshotReady::Ready{installed,..}|fleet_sync_receiver::SnapshotReady::AlreadyReady{installed}=>installed};
                Ok::<_,sqlx::Error>(serde_json::json!({"kind":"published","position":installed.position,"schema":installed.schema}))
            }).await.map_err(error)?,
            ProbeRequest::Audit => {
                let schema = sqlx::query::<Do>("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT GLOB '_cf_*' ORDER BY name").fetch_all(&conn).await.map_err(error)?;
                let mut tables = Vec::new();
                for row in schema {
                    let name: String = row.try_get("name").map_err(error)?;
                    let sql: String = row.try_get("sql").map_err(error)?;
                    let quote = |value: &str| format!("\"{}\"", value.replace('"', "\"\""));
                    let metadata = sqlx::query::<Do>(sqlx::AssertSqlSafe(format!("PRAGMA table_info({})",quote(&name)))).fetch_all(&conn).await.map_err(error)?;
                    let columns = metadata.iter().map(|row|row.try_get::<String,_>("name")).collect::<Result<Vec<_>,_>>().map_err(error)?;
                    let projection = columns.iter().flat_map(|name| {
                        let column=quote(name);
                        [format!("typeof({column})"),format!("CASE WHEN typeof({column})='integer' THEN CAST({column} AS TEXT) WHEN typeof({column})='blob' THEN hex({column}) ELSE {column} END")]
                    }).collect::<Vec<_>>().join(",");
                    let rows = sqlx::query::<Do>(sqlx::AssertSqlSafe(format!("SELECT json_array({projection}) AS result FROM {} ORDER BY 1",quote(&name)))).fetch_all(&conn).await.map_err(error)?;
                    let values = rows.iter().map(|row|row.try_get::<String,_>("result")).collect::<Result<Vec<_>,_>>().map_err(error)?;
                    tables.push(serde_json::json!({"table":name,"sql":sql,"columns":columns,"rows":values}));
                }
                serde_json::json!({"kind":"audit","tables":tables})
            }
        };
        conn.storage().sync().await?;
        Response::from_json(&reply)
    }
}
#[event(fetch)]
async fn fetch(request: Request, env: Env, _: Context) -> worker::Result<Response> {
    let name = request
        .headers()
        .get("X-Probe-Object")?
        .ok_or_else(|| error("missing object name"))?;
    env.durable_object("PROBE")?
        .id_from_name(&name)?
        .get_stub()?
        .fetch_with_request(request)
        .await
}
