use super::*;

impl<S: AtomicStore, H: Host> Engine<S, H> {
    /// Admit and settle only this request, joining any drive on this Engine
    /// that already owns it. Unrelated due operations are not driven or awaited.
    ///
    /// Returns Running when no drive on this Engine is progressing the operation,
    /// including scheduled backoff and an unexpired predecessor's lease. There
    /// is no timer or retry of a leased call. Terminal outcomes confirmed by
    /// this Engine are returned even when the task prunes its journal row.
    /// Receipts live only with an active drive or settler; they do not survive
    /// another Engine or restart. Without a receipt, a missing row is an error.
    /// Consumers read any separate projection through their own API.
    /// Admission failures retain StartRefusal; later storage failures are Execution.
    /// The driving caller still receives a post-commit scheduling storage error,
    /// even after its receipt becomes available to other settlers.
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send makes higher-ranked store tasks spawnable; engine_send pins this contract"
    )]
    pub fn start_and_settle<T: Task<Store = S>>(
        &self,
        request_key: &RequestKey,
        input: T::Input,
    ) -> impl Future<Output = Result<OperationStatus, SettleError>> + Send {
        async move {
            // Admission attaches before its transaction commits or rearms.
            let (operation, activity) = self.admit::<T>(request_key, input).await?;
            loop {
                {
                    // Join only this operation before asking for execution
                    // capacity. Finished/backoff/predecessor states need none.
                    let released = activity.lock().await;
                    if let Some(receipt) = released.as_ref() {
                        return Ok(receipt.status(operation));
                    }
                    let status = self.status(&operation).await?;
                    let now = self.host.now();
                    if !matches!(&status.state, OperationState::Running {
                        next_attempt_at, drive, ..
                    } if *next_attempt_at <= now
                        && drive.as_ref().is_none_or(|lease| lease.until <= now))
                    {
                        return Ok(status);
                    }
                }
                self.drive_operation(operation).await?;
                if let Some(released) = activity.try_lock() {
                    if let Some(receipt) = released.as_ref() {
                        return Ok(receipt.status(operation));
                    }
                    // Exclude a new claim of this operation during its read.
                    // Other operations have different guards; this read makes
                    // no external call.
                    return self.status(&operation).await.map_err(Into::into);
                }
                // Dropping a drive, including cancellation, releases this
                // operation's guard. Recheck durable state after every release.
                drop(activity.lock().await);
            }
        }
    }

    pub(super) async fn admit<T: Task<Store = S>>(
        &self,
        request_key: &RequestKey,
        input: T::Input,
    ) -> Result<(OperationId, Arc<OperationActivity>), StartRefusal> {
        if !self.tasks.contains_key(T::KIND) {
            return Err(StartRefusal::UnknownKind);
        }
        let key = request_key.clone();
        let activities = Arc::clone(&self.operation_drives);
        let admitted = self
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    match scope.admit::<T>(&key, &input).await {
                        Err(StartRefusal::Storage(error)) => Err(error),
                        Err(refusal) => Ok(Err(refusal)),
                        Ok(admitted) => {
                            let operation = admitted.id();
                            // The transaction excludes a finish until its real
                            // operation identity has an attached strong owner.
                            let activity = activities.attach(operation).await;
                            Ok(Ok((operation, activity)))
                        }
                    }
                })
            })
            .await??;
        self.rearm().await?;
        Ok(admitted)
    }
}
