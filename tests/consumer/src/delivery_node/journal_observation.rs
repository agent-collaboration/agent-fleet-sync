use super::*;
use sqlx_core::row::Row;

impl NodeState {
    pub(super) async fn wait_mailbox_unavailable(&self, hop: String) -> TestResult<Value> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some((_, call)) = self
                .unavailable
                .lock()
                .await
                .iter()
                .find(|(kind, _)| kind == &hop)
            {
                return Ok(serde_json::to_value(call)?);
            }
            notified.await;
        }
    }

    pub(super) async fn wait_report_retired(&self, item: ItemId) -> TestResult<Value> {
        let mut committed = self.object.subscribe_commits();
        loop {
            let pending = self
                .object
                .rule_transaction::<_, StoreError, _>(move |scope| {
                    Box::pin(async move {
                        Ok(sqlx_core::query::query::<sqlx_turso::Turso>(
                            "SELECT item_id FROM _fleet_sync_reports WHERE item_id=?1",
                        )
                        .bind(item.as_uuid().to_string())
                        .fetch_optional(scope.executor())
                        .await
                        .map_err(StoreError::backend)?
                        .is_some())
                    })
                })
                .await?;
            if !pending {
                return Ok(json!({"retired":true,"item":item}));
            }
            committed.changed().await?;
        }
    }
    pub(super) async fn wait_pending_call(&self, hop: String, attempts: u32) -> TestResult<Value> {
        let mut committed = self.object.subscribe_commits();
        loop {
            let hop = hop.clone();
            let pending = self.object.rule_transaction::<_, StoreError, _>(move |scope| Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT o.operation_id,o.request_key,o.attempt,s.intent FROM _fleet_sync_operations o JOIN _fleet_sync_steps s ON s.operation_id=o.operation_id WHERE s.step_name=?1 AND s.output IS NULL AND o.attempt>=?2 AND o.drive_token IS NULL ORDER BY o.operation_id LIMIT 1",
                ).bind(hop).bind(i64::from(attempts)).fetch_optional(scope.executor()).await.map_err(StoreError::backend)?;
                row.map(|row| Ok(json!({
                    "operation": row.try_get::<String,_>("operation_id").map_err(StoreError::backend)?,
                    "key": row.try_get::<String,_>("request_key").map_err(StoreError::backend)?,
                    "attempt": row.try_get::<i64,_>("attempt").map_err(StoreError::backend)?,
                    "intent": row.try_get::<Option<String>,_>("intent").map_err(StoreError::backend)?,
                }))).transpose()
            })).await?;
            if let Some(pending) = pending {
                return Ok(pending);
            }
            committed.changed().await?;
        }
    }
}
