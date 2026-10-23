use super::*;
use std::sync::atomic::Ordering;

#[derive(Serialize, Deserialize)]
pub struct DeliveryEffect {
    pub effect: durable_steps::Uuid,
    pub item: ItemId,
    pub call: CallId,
    pub body: String,
}
pub(super) async fn read_effects(path: &Path) -> TestResult<Vec<DeliveryEffect>> {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) => contents
            .lines()
            .map(|line| serde_json::from_str(line).map_err(Into::into))
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}
pub(super) struct NodeConsumer(pub Weak<NodeState>);
impl<S: TransactionScope> DeliveryConsumer<S> for NodeConsumer {
    type Protocol = SampleProtocol;
    async fn release_gate(
        &self,
        _scope: &mut S,
        _item: &DeliveryItem<SampleDelivery>,
    ) -> Result<GateDecision, DeliveryError> {
        Ok(GateDecision::Release)
    }
    async fn deliver_locally(
        &self,
        item: &DeliveryItem<SampleDelivery>,
        call: CallId,
    ) -> LocalDelivery<SampleDelivery> {
        let Some(state) = self.0.upgrade() else {
            return LocalDelivery {
                outcome: LocalOutcome::Uncertain,
                observation: (),
            };
        };
        if state.held.load(Ordering::SeqCst) {
            return LocalDelivery {
                outcome: LocalOutcome::Held { until: None },
                observation: (),
            };
        }
        let effect = DeliveryEffect {
            effect: durable_steps::Uuid::now_v7(),
            item: item.id(),
            call,
            body: item.body().clone(),
        };
        let result: TestResult = async {
            let _guard = state.effect_lock.lock().await;
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&state.effect_file)
                .await?;
            file.write_all(format!("{}\n", serde_json::to_string(&effect)?).as_bytes())
                .await?;
            file.sync_data().await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            return LocalDelivery {
                outcome: LocalOutcome::Uncertain,
                observation: (),
            };
        }
        state.changed.notify_waiters();
        LocalDelivery {
            outcome: LocalOutcome::Delivered {
                acceptance: if state.oversized.load(Ordering::SeqCst) {
                    "invalid receipt".repeat(1024)
                } else {
                    effect.effect.to_string()
                },
            },
            observation: (),
        }
    }
    fn may_cancel(&self, requester: &String, item: &DeliveryHeader<SampleDelivery>) -> bool {
        requester == item.sender()
    }
}
