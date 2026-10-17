use crate::{
    ClaimGateReceipt, ClaimSettlementReceipt, ConsumerStore, TestResult, checked_queries,
    consumer_store::ConsumerScope,
};
use durable_steps::CallId;
use fleet_sync_protocol::*;
use sqlx_core::connection::Connection;

fn attempt(request: &HolderChangeRequest) -> TransferId {
    match request {
        HolderChangeRequest::Claim { attempt, .. }
        | HolderChangeRequest::Move { attempt, .. }
        | HolderChangeRequest::Takeover { attempt, .. } => *attempt,
    }
}
fn decode(row: checked_queries::ClaimGateRow) -> TestResult<ClaimGateReceipt> {
    if row.attempt_class.as_deref() != Some("text") || row.record_class.as_deref() != Some("text") {
        return Err("claim receipt has invalid storage classes".into());
    }
    let receipt: ClaimGateReceipt =
        serde_json::from_str(&row.record.ok_or("claim receipt missing")?)?;
    if row.attempt.as_deref() != Some(attempt(&receipt.request).as_uuid().to_string().as_str()) {
        return Err("claim receipt identity mismatch".into());
    }
    Ok(receipt)
}
async fn read(
    scope: &mut ConsumerScope<'_>,
    request: &HolderChangeRequest,
) -> TestResult<ClaimGateReceipt> {
    let receipt = checked_queries::read_claim_gate(scope, attempt(request).as_uuid().to_string())
        .await?
        .map(decode)
        .transpose()?
        .unwrap_or(ClaimGateReceipt {
            request: request.clone(),
            reserved_by: None,
            settlement: None,
            reserve_attempts: 0,
            settle_attempts: 0,
        });
    if receipt.request != *request {
        return Err("claim attempt has another request".into());
    }
    Ok(receipt)
}
async fn save(scope: &mut ConsumerScope<'_>, receipt: &ClaimGateReceipt) -> TestResult {
    checked_queries::write_claim_gate(
        scope,
        attempt(&receipt.request).as_uuid().to_string(),
        serde_json::to_string(receipt)?,
    )
    .await?;
    Ok(())
}
impl ConsumerStore {
    pub async fn reserve_claim(
        &self,
        request: &HolderChangeRequest,
        call: CallId,
    ) -> TestResult<GateAnswer> {
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut scope = ConsumerScope(&mut transaction);
        let mut receipt = read(&mut scope, request).await?;
        let answer = if receipt.settlement.is_some() {
            GateAnswer::Refused {
                reason: GateReason {
                    code: GateReasonCode::new("claim_closed")?,
                    detail: "claim was already settled".into(),
                },
            }
        } else {
            receipt.reserved_by.get_or_insert(call);
            GateAnswer::Reserved
        };
        receipt.reserve_attempts = receipt
            .reserve_attempts
            .checked_add(1)
            .ok_or("reserve attempts overflow")?;
        save(&mut scope, &receipt).await?;
        transaction.commit().await?;
        self.changes.send_replace(());
        Ok(answer)
    }
    pub async fn settle_claim(
        &self,
        request: &HolderChangeRequest,
        outcome: SettleOutcome,
        call: CallId,
    ) -> TestResult {
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut scope = ConsumerScope(&mut transaction);
        let mut receipt = read(&mut scope, request).await?;
        if let Some(existing) = &receipt.settlement {
            if existing.outcome != outcome {
                return Err("claim already settled with another outcome".into());
            }
        } else {
            receipt.settlement = Some(ClaimSettlementReceipt { call, outcome });
        }
        receipt.settle_attempts = receipt
            .settle_attempts
            .checked_add(1)
            .ok_or("settle attempts overflow")?;
        save(&mut scope, &receipt).await?;
        transaction.commit().await?;
        self.changes.send_replace(());
        Ok(())
    }
    pub async fn claim_receipts(&self) -> TestResult<Vec<ClaimGateReceipt>> {
        let mut connection = self.pool.acquire().await?;
        checked_queries::read_claim_gates(&mut ConsumerScope(&mut connection))
            .await?
            .into_iter()
            .map(decode)
            .collect()
    }
    pub async fn wait_claim_settlement(
        &self,
        id: TransferId,
        attempts: u64,
    ) -> TestResult<ClaimGateReceipt> {
        let mut changed = self.changes.subscribe();
        loop {
            if let Some(receipt) = self.claim_receipts().await?.into_iter().find(|receipt| {
                attempt(&receipt.request) == id
                    && receipt.settle_attempts >= attempts
                    && receipt.settlement.is_some()
            }) {
                return Ok(receipt);
            }
            changed.changed().await?;
        }
    }
}
