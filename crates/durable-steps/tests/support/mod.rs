use durable_steps::{CallFailure, CallId, CallTarget, Host, SyncTime, Uuid, WakeFailure};
use runtime_primitives::{Clock, IdSource, WakeScheduler};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
};

struct ReceiverReceipt {
    request: Vec<u8>,
    answer: Vec<u8>,
}
pub type ReceiverDispatch = dyn Fn(CallId, Vec<u8>) -> durable_steps::BoxFuture<'static, Result<(Vec<u8>, bool), CallFailure>>
    + Send
    + Sync;

pub struct TestHost {
    pub receiver_dispatch: Mutex<Option<Arc<ReceiverDispatch>>>,
    pub clock: AtomicI64,
    pub invalid_ids: AtomicBool,
    next_id: AtomicU64,
    pub deliveries: AtomicU64,
    pub effects: AtomicU64,
    pub lose_answers: AtomicU64,
    pub retry_calls: AtomicBool,
    pub hang_after_effect: AtomicBool,
    pub failures: Mutex<VecDeque<CallFailure>>,
    pub wakes: Mutex<Vec<SyncTime>>,
    receiver: Mutex<BTreeMap<String, ReceiverReceipt>>,
}

impl TestHost {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            receiver_dispatch: Mutex::new(None),
            clock: AtomicI64::new(1_000),
            invalid_ids: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
            deliveries: AtomicU64::new(0),
            effects: AtomicU64::new(0),
            lose_answers: AtomicU64::new(0),
            retry_calls: AtomicBool::new(false),
            hang_after_effect: AtomicBool::new(false),
            failures: Mutex::new(VecDeque::new()),
            wakes: Mutex::new(Vec::new()),
            receiver: Mutex::new(BTreeMap::new()),
        })
    }
    #[allow(dead_code)] // The storage-only suite shares this host without advancing it.
    pub fn advance(&self, milliseconds: i64) {
        self.clock.fetch_add(milliseconds, Ordering::SeqCst);
    }
}

impl Clock for TestHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.clock.load(Ordering::SeqCst))
    }
}
impl IdSource for TestHost {
    fn new_id(&self) -> Uuid {
        if self.invalid_ids.load(Ordering::SeqCst) {
            return Uuid::from_u128(0x019eef00000040008000000000000001);
        }
        Uuid::from_u128(
            (u128::from(self.next_id.fetch_add(1, Ordering::SeqCst)))
                | 0x019eef00000070008000000000000000,
        )
    }
}
impl Host for TestHost {
    async fn dispatch<C: CallTarget>(
        &self,
        _target: C,
        request: C::Request,
        call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        let scripted = self.failures.lock().map_err(refused)?.pop_front();
        if let Some(failure) = scripted {
            return Err(failure);
        }
        if self.retry_calls.load(Ordering::SeqCst) {
            return Err(CallFailure::NotSent {
                reason: durable_steps::NotSentReason::Unreachable { last_seen: None },
            });
        }
        let key = serde_json::to_string(&call).map_err(refused)?;
        let input = serde_json::to_vec(&request).map_err(refused)?;
        let persistent = self.receiver_dispatch.lock().map_err(refused)?.clone();
        let output = if let Some(receiver) = persistent {
            let (output, new_effect) = receiver(call, input).await?;
            if new_effect {
                self.effects.fetch_add(1, Ordering::SeqCst);
            }
            output
        } else {
            let mut receiver = self.receiver.lock().map_err(refused)?;
            if let Some(recorded) = receiver.get(&key) {
                if recorded.request != input {
                    return Err(refused("call content conflict"));
                }
                recorded.answer.clone()
            } else {
                let value: u64 = serde_json::from_slice(&input).map_err(refused)?;
                let output = serde_json::to_vec(&(value * 2)).map_err(refused)?;
                receiver.insert(
                    key,
                    ReceiverReceipt {
                        request: input,
                        answer: output.clone(),
                    },
                );
                self.effects.fetch_add(1, Ordering::SeqCst);
                output
            }
        };
        if self.hang_after_effect.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self
            .lose_answers
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(CallFailure::AnswerLost);
        }
        serde_json::from_slice(&output).map_err(refused)
    }
}

fn refused(_error: impl std::fmt::Display) -> CallFailure {
    CallFailure::AnswerLost
}
impl WakeScheduler for TestHost {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        self.wakes
            .lock()
            .map_err(|error| WakeFailure::Unavailable {
                message: error.to_string(),
            })?
            .push(at);
        Ok(())
    }
}
