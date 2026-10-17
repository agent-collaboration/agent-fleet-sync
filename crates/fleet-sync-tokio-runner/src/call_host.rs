use durable_steps::{CallFailure, CallId, CallTarget, Host, NotSentReason, Uuid};
use runtime_primitives::{
    Clock, IdSource, SyncTime, SystemClock, TokioWakeTimer, UuidV7Ids, WakeFailure, WakeScheduler,
};
use serde_json::Value;
use std::{future::Future, sync::Arc};

pub struct CallEnvelope {
    pub kind: &'static str,
    pub target: Value,
    pub request: Value,
    pub call: CallId,
}

/// The service routes known object targets and authenticated peer calls. The
/// runner preserves the task's CallId across both routes.
pub trait CallRouter: Send + Sync + 'static {
    fn dispatch(
        &self,
        call: CallEnvelope,
    ) -> impl Future<Output = Result<Value, CallFailure>> + Send;
}

struct SharedClock<C>(Arc<C>);
impl<C: Clock> Clock for SharedClock<C> {
    fn now(&self) -> SyncTime {
        self.0.now()
    }
}

pub struct TokioHost<R: CallRouter, C: Clock + 'static = SystemClock> {
    router: Arc<R>,
    clock: Arc<C>,
    timer: TokioWakeTimer<SharedClock<C>>,
}
impl<R: CallRouter, C: Clock + 'static> TokioHost<R, C> {
    pub fn new(router: Arc<R>, clock: C) -> Self {
        let clock = Arc::new(clock);
        Self {
            router,
            clock: clock.clone(),
            timer: TokioWakeTimer::new(SharedClock(clock)),
        }
    }
    pub async fn wait(&self) -> SyncTime {
        self.timer.wait().await
    }
}
impl<R: CallRouter, C: Clock + 'static> Clock for TokioHost<R, C> {
    fn now(&self) -> SyncTime {
        self.clock.now()
    }
}
impl<R: CallRouter, C: Clock + 'static> IdSource for TokioHost<R, C> {
    fn new_id(&self) -> Uuid {
        UuidV7Ids.new_id()
    }
}
impl<R: CallRouter, C: Clock + 'static> WakeScheduler for TokioHost<R, C> {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        self.timer.wake_at(at).await
    }
}
impl<R: CallRouter, C: Clock + 'static> Host for TokioHost<R, C> {
    async fn dispatch<T: CallTarget>(
        &self,
        target: T,
        request: T::Request,
        call: CallId,
    ) -> Result<T::Answer, CallFailure> {
        let target = serde_json::to_value(target).map_err(encode_failure)?;
        let request = serde_json::to_value(request).map_err(encode_failure)?;
        let answer = self
            .router
            .dispatch(CallEnvelope {
                kind: T::KIND,
                target,
                request,
                call,
            })
            .await?;
        serde_json::from_value(answer).map_err(|error| {
            tracing::error!(kind=T::KIND,%error,"call answer did not match its registered target");
            CallFailure::AnswerLost
        })
    }
}
fn encode_failure(error: serde_json::Error) -> CallFailure {
    tracing::error!(%error,"call could not be encoded before dispatch");
    CallFailure::NotSent {
        reason: NotSentReason::Overloaded,
    }
}
