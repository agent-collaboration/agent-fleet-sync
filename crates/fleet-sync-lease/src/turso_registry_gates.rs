use crate::turso_registry_fixture::TursoGate;
use crate::{
    GateAnswer, GateFailure, HolderChangeGate, LifecycleGate, SettleOutcome, change_checks,
};
use durable_steps::CallId;
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, row::Row};
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

fn failure(error: impl std::fmt::Display) -> GateFailure {
    GateFailure {
        detail: error.to_string(),
    }
}
fn reason(code: &str) -> Result<GateReason, GateFailure> {
    Ok(GateReason {
        code: GateReasonCode::new(code).map_err(failure)?,
        detail: "fixture policy refused".into(),
    })
}
impl TursoGate {
    async fn admission(
        &self,
        call: CallId,
        request: String,
        answer: GateAnswer,
    ) -> Result<GateAnswer, GateFailure> {
        let call = serde_json::to_string(&call).map_err(failure)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(failure)?;
        let existing =
            sqlx_core::query::query::<Turso>("SELECT request,answer FROM calls WHERE call=?1")
                .bind(call.clone())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(failure)?;
        let answer = if let Some(existing) = existing {
            if existing.try_get::<String, _>("request").map_err(failure)? != request {
                return Err(failure("gate call identity conflict"));
            }
            serde_json::from_str(&existing.try_get::<String, _>("answer").map_err(failure)?)
                .map_err(failure)?
        } else {
            sqlx_core::query::query::<Turso>(
                "INSERT INTO calls(call,request,answer) VALUES(?1,?2,?3)",
            )
            .bind(call)
            .bind(request)
            .bind(serde_json::to_string(&answer).map_err(failure)?)
            .execute(&mut *transaction)
            .await
            .map_err(failure)?;
            answer
        };
        transaction.commit().await.map_err(failure)?;
        Ok(answer)
    }
    pub async fn settlement_count(&self) -> Result<String, GateFailure> {
        let mut connection = self.connection.lock().await;
        let row = sqlx_core::query::query::<Turso>(
            "SELECT CAST(count(*) AS TEXT) AS value FROM settlements",
        )
        .fetch_one(&mut *connection)
        .await
        .map_err(failure)?;
        row.try_get("value").map_err(failure)
    }
    pub async fn settlement_outcome(
        &self,
        id: TransferId,
    ) -> Result<Option<SettleOutcome>, GateFailure> {
        let mut connection = self.connection.lock().await;
        sqlx_core::query::query::<Turso>("SELECT outcome FROM settlements WHERE attempt=?1")
            .bind(id.as_uuid().to_string())
            .fetch_optional(&mut *connection)
            .await
            .map_err(failure)?
            .map(|row| {
                serde_json::from_str(&row.try_get::<String, _>("outcome").map_err(failure)?)
                    .map_err(failure)
            })
            .transpose()
    }
}
impl LifecycleGate for TursoGate {
    async fn admit_registration(
        &self,
        database: &DatabaseId,
        writer: &PeerId,
        call: CallId,
    ) -> Result<GateAnswer, GateFailure> {
        let answer = if self.reject_registration.load(Ordering::SeqCst) {
            GateAnswer::Refused {
                reason: reason("registration")?,
            }
        } else {
            GateAnswer::Admitted
        };
        self.admission(
            call,
            serde_json::to_string(&("register", database, writer)).map_err(failure)?,
            answer,
        )
        .await
    }
    async fn admit_retirement(
        &self,
        database: &DatabaseId,
        call: CallId,
    ) -> Result<GateAnswer, GateFailure> {
        let answer = if self.reject_retirement.load(Ordering::SeqCst) {
            GateAnswer::Refused {
                reason: reason("retirement")?,
            }
        } else {
            GateAnswer::Admitted
        };
        self.admission(
            call,
            serde_json::to_string(&("retire", database)).map_err(failure)?,
            answer,
        )
        .await
    }
}
impl HolderChangeGate for TursoGate {
    async fn reserve(
        &self,
        change: &HolderChangeRequest,
        call: CallId,
    ) -> Result<GateAnswer, GateFailure> {
        let call = serde_json::to_string(&call).map_err(failure)?;
        let request = serde_json::to_string(change).map_err(failure)?;
        let attempt = change_checks::attempt(change).as_uuid().to_string();
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(failure)?;
        if let Some(row) =
            sqlx_core::query::query::<Turso>("SELECT request,answer FROM calls WHERE call=?1")
                .bind(call.clone())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(failure)?
        {
            if row.try_get::<String, _>("request").map_err(failure)? != request {
                return Err(failure("reservation call identity conflict"));
            }
            let answer =
                serde_json::from_str(&row.try_get::<String, _>("answer").map_err(failure)?)
                    .map_err(failure)?;
            transaction.commit().await.map_err(failure)?;
            return Ok(answer);
        }
        let already_settled =
            sqlx_core::query::query::<Turso>("SELECT attempt FROM settlements WHERE attempt=?1")
                .bind(attempt.clone())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(failure)?
                .is_some();
        let answer = if self.reject_claim.load(Ordering::SeqCst) {
            GateAnswer::Refused {
                reason: reason("claim")?,
            }
        } else if already_settled {
            GateAnswer::Refused {
                reason: reason("settled")?,
            }
        } else {
            sqlx_core::query::query::<Turso>("INSERT INTO reservations(attempt,change) VALUES(?1,?2) ON CONFLICT(attempt) DO NOTHING").bind(attempt).bind(request.clone()).execute(&mut *transaction).await.map_err(failure)?;
            GateAnswer::Reserved
        };
        sqlx_core::query::query::<Turso>("INSERT INTO calls(call,request,answer) VALUES(?1,?2,?3)")
            .bind(call)
            .bind(request)
            .bind(serde_json::to_string(&answer).map_err(failure)?)
            .execute(&mut *transaction)
            .await
            .map_err(failure)?;
        transaction.commit().await.map_err(failure)?;
        Ok(answer)
    }
    async fn settle(
        &self,
        change: &HolderChangeRequest,
        outcome: SettleOutcome,
        call: CallId,
    ) -> Result<(), GateFailure> {
        let call = serde_json::to_string(&call).map_err(failure)?;
        let request = serde_json::to_string(&(change, &outcome)).map_err(failure)?;
        let outcome = serde_json::to_string(&outcome).map_err(failure)?;
        let attempt = change_checks::attempt(change).as_uuid().to_string();
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(failure)?;
        if let Some(row) =
            sqlx_core::query::query::<Turso>("SELECT request FROM calls WHERE call=?1")
                .bind(call.clone())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(failure)?
        {
            if row.try_get::<String, _>("request").map_err(failure)? != request {
                return Err(failure("settlement call identity conflict"));
            }
        } else {
            if let Some(row) =
                sqlx_core::query::query::<Turso>("SELECT outcome FROM settlements WHERE attempt=?1")
                    .bind(attempt.clone())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(failure)?
            {
                if row.try_get::<String, _>("outcome").map_err(failure)? != outcome {
                    return Err(failure("settlement outcome conflict"));
                }
            } else {
                sqlx_core::query::query::<Turso>(
                    "INSERT INTO settlements(attempt,outcome) VALUES(?1,?2)",
                )
                .bind(attempt)
                .bind(outcome)
                .execute(&mut *transaction)
                .await
                .map_err(failure)?;
            }
            sqlx_core::query::query::<Turso>(
                "INSERT INTO calls(call,request,answer) VALUES(?1,?2,'null')",
            )
            .bind(call)
            .bind(request)
            .execute(&mut *transaction)
            .await
            .map_err(failure)?;
        }
        transaction.commit().await.map_err(failure)?;
        Ok(())
    }
}
