use super::*;

impl<S: AtomicStore, H: Host> Engine<S, H> {
    /// Admit and settle only this request, joining any drive on this Engine
    /// that already owns it. Unrelated due operations are not driven or awaited.
    ///
    /// Returns Running when no drive on this Engine is progressing the operation,
    /// including scheduled backoff and an unexpired predecessor's lease. There
    /// is no timer or retry of a leased call. As with status(), the task must
    /// retain its result to read a terminal journal row.
    /// Admission failures retain StartRefusal; later storage failures are Execution.
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
            let operation = self.admit::<T>(request_key, input).await?;
            // Retain the same per-operation owner across status reads and
            // release waits, so a completion cannot be lost between them.
            let activity = self.operation_drive_lock(operation).await;
            loop {
                {
                    // Join only this operation before asking for execution
                    // capacity. Finished/backoff/predecessor states need none.
                    let _released = activity.lock().await;
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
                if let Some(_released) = activity.try_lock() {
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
    ) -> Result<OperationId, StartRefusal> {
        if !self.tasks.contains_key(T::KIND) {
            return Err(StartRefusal::UnknownKind);
        }
        let key = request_key.clone();
        let admitted = self
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    match scope.admit::<T>(&key, &input).await {
                        Err(StartRefusal::Storage(error)) => Err(error),
                        result => Ok(result),
                    }
                })
            })
            .await??;
        self.rearm().await?;
        Ok(admitted.id())
    }

    pub(super) async fn operation_drive_lock(&self, operation: OperationId) -> Arc<Mutex<()>> {
        let mut drives = self.operation_drives.lock().await;
        // Only live drives and settlers keep a strong reference. Completed
        // operations do not accumulate an in-memory completion registry.
        drives.retain(|_, drive| drive.strong_count() > 0);
        if let Some(drive) = drives.get(&operation).and_then(Weak::upgrade) {
            return drive;
        }
        let drive = Arc::new(Mutex::new(()));
        drives.insert(operation, Arc::downgrade(&drive));
        drive
    }
}
