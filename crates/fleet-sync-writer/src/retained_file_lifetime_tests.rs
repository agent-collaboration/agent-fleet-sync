#![allow(clippy::panic_in_result_fn)]
use crate::{
    read_transaction_tests::{ReadSchema, writer},
    retained_file_fixture::*,
    *,
};
use sqlx_core::{executor::Executor, row::Row};
use std::{path::Path, time::Duration};

async fn eventually_reopen(
    identity: WriterIdentity,
    path: &Path,
) -> TestResult<ReplicatedDatabase<ReadSchema>> {
    // SQLx may still be returning the last connection after transaction drop.
    // Retry the authoritative admission result, with a deadline, never a sleep
    // interpreted as cleanup completion.
    Ok(tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match identity.open(path).await {
                Err(OpenError::FileInUse) => tokio::task::yield_now().await,
                result => break result,
            }
        }
    })
    .await??)
}

#[tokio::test]
async fn retained_live_writer_is_refused_at_the_path_and_its_hard_link() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let alias = directory.path().join("alias.db");
    let database = writer(&path).await?;
    add_mark(&database, "live writer").await?;
    std::fs::hard_link(&path, &alias)?;
    let before = snapshot(directory.path())?;
    for candidate in [&path, &alias] {
        assert!(
            matches!(
                RetainedFile::open_read_only(candidate, ReadSchema).await,
                Err(OpenError::FileInUse)
            ),
            "{candidate:?}"
        );
    }
    assert_eq!(snapshot(directory.path())?, before);
    // A refused retained opener cannot close or damage the real writer.
    add_mark(&database, "writer still works").await?;
    assert_eq!(mark_count(database.reader()).await?, 2);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn retained_pool_and_outliving_transaction_block_writer_opens_by_identity() -> TestResult {
    for through_alias in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let alias = directory.path().join("alias.db");
        let database = writer(&path).await?;
        add_mark(&database, "retained").await?;
        let identity = WriterIdentity::observe(&database).await;
        database.close().await;
        drop(database);
        std::fs::hard_link(&path, &alias)?;
        let candidate = if through_alias { &alias } else { &path };
        let before = snapshot(directory.path())?;
        let retained = RetainedFile::open_read_only(&path, ReadSchema).await?;
        assert!(
            matches!(identity.open(candidate).await, Err(OpenError::FileInUse)),
            "pool, alias={through_alias}"
        );
        let mut transaction = retained.reader().begin_read().await?;
        assert_eq!(
            (&mut transaction)
                .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
                .await?
                .try_get::<i64, _>("count")?,
            1
        );

        // No extra SQLx handle or top-level reader keeps the claim for this check.
        drop(retained);
        assert!(
            matches!(identity.open(candidate).await, Err(OpenError::FileInUse)),
            "outliving transaction, alias={through_alias}"
        );
        assert_eq!(
            (&mut transaction)
                .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
                .await?
                .try_get::<i64, _>("count")?,
            1
        );
        transaction.close().await?;
        assert_eq!(snapshot(directory.path())?, before);

        // The supported writer route can write again after actual reader cleanup.
        let reopened = eventually_reopen(identity, &path).await?;
        add_mark(&reopened, "after readers").await?;
        assert_eq!(mark_count(reopened.reader()).await?, 2);
        reopened.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn retained_simultaneous_opposed_opens_never_both_succeed() -> TestResult {
    for _ in 0..8 {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let database = writer(&path).await?;
        let identity = WriterIdentity::observe(&database).await;
        database.close().await;
        drop(database);
        let (reader, writer) = tokio::join!(
            RetainedFile::open_read_only(&path, ReadSchema),
            identity.open(&path)
        );
        assert_ne!(
            reader.is_ok(),
            writer.is_ok(),
            "exactly one opposed mode must be admitted"
        );
        match (reader, writer) {
            (Ok(readers), Err(OpenError::FileInUse)) => {
                drop(readers);
            }
            (Err(OpenError::FileInUse), Ok(database)) => {
                database.close().await;
            }
            _ => return Err("opposed opener failed for another reason".into()),
        }
    }
    Ok(())
}

#[tokio::test]
async fn retained_non_utf8_path_cannot_claim_one_file_and_open_another() -> TestResult {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};
    let directory = tempfile::tempdir()?;
    let lossy_target = directory.path().join("\u{fffd}.db");
    let invalid_path = directory.path().join(OsStr::from_bytes(b"\xff.db"));
    let database = writer(&lossy_target).await?;
    add_mark(&database, "must not read the lossy target").await?;
    // On Linux the second physical file is mandatory: admission on that inode
    // alone would miss the writer at the SDK's lossy-converted target.
    #[cfg(not(target_os = "macos"))]
    {
        std::fs::write(&invalid_path, b"another physical file")?;
        println!("non-UTF-8 second-file alias control: exercised (creation required)");
    }
    #[cfg(target_os = "macos")]
    println!(
        "non-UTF-8 second-file alias control: not applicable on macOS/APFS; exact pre-admission refusal remains mandatory"
    );
    let before = snapshot(directory.path())?;
    assert!(matches!(
        RetainedFile::open_read_only(&invalid_path, ReadSchema).await,
        Err(OpenError::NativePathNotUtf8)
    ));
    assert_eq!(snapshot(directory.path())?, before);
    database.close().await;
    Ok(())
}
