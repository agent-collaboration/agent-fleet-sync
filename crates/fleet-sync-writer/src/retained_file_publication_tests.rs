#![allow(clippy::panic_in_result_fn)]
use crate::{
    read_transaction_tests::{ReadSchema, writer},
    retained_file_fixture::*,
    *,
};
use sqlx_core::{executor::Executor, row::Row};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn retained_reader_in_a_closed_replacement_interval_refuses_recovery_until_closed()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let replacement = directory.path().join("replacement.db");
    let database = Arc::new(writer(&path).await?);
    add_mark(&database, "retained replacement").await?;
    database.begin_seeding().await?;
    let (send_reader, receive_reader) = tokio::sync::oneshot::channel();
    let publishing = {
        let database = database.clone();
        tokio::spawn(async move {
            database
                .with_drained_file(move |live| {
                    Box::pin(async move {
                        // Publication already drained/checkpointed this owned fixture.
                        // Copying then renaming creates a different physical generation
                        // with the same valid durable writer receipt.
                        tokio::fs::copy(live, &replacement)
                            .await
                            .map_err(native_file::PublishError::io)?;
                        tokio::fs::rename(&replacement, live)
                            .await
                            .map_err(native_file::PublishError::io)?;
                        let retained = RetainedFile::open_read_only(live, ReadSchema)
                            .await
                            .map_err(native_file::PublishError::io)?;
                        send_reader.send(retained).map_err(|_| {
                            native_file::PublishError::io("reader receiver dropped")
                        })?;
                        Ok(())
                    })
                })
                .await
        })
    };
    let retained = tokio::time::timeout(Duration::from_secs(5), receive_reader).await??;
    assert_eq!(mark_count(retained.reader()).await?, 1);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), publishing).await??,
        Err(native_file::PublishError::FileInUse)
    ));
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::FileInUse)
    ));
    assert!(matches!(
        database.begin_seeding().await,
        Err(OpenError::FileInUse)
    ));

    // Drain the actual retained pool, then retry the existing public reader
    // recovery route. The receipt was never repaired, and no path reservation
    // or automatic migration was used to get past the refusal.
    let pool = retained.reader().pool.current()?;
    drop(retained);
    pool.close().await;
    drop(pool);
    let mut recovered = database.reader().begin_read().await?;
    assert_eq!(
        (&mut recovered)
            .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
            .await?
            .try_get::<i64, _>("count")?,
        1
    );
    recovered.close().await?;
    database.close().await;
    Ok(())
}
