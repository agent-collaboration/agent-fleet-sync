use super::*;

// ---- Interactive transactions: `DoConnection::transaction`.

pub(super) async fn transaction_commits_and_returns_its_value(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;

    let id = conn
        .transaction(|tx| async move {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO users (email) VALUES ('tx@example.com') RETURNING id",
            )
            .fetch_one(&tx)
            .await?;
            sqlx::query("INSERT INTO posts (user_id, title) VALUES (?, 'first')")
                .bind(id)
                .execute(&tx)
                .await?;
            Ok::<_, sqlx::Error>(id)
        })
        .await
        .map_err(fail)?;

    let owner: i64 = sqlx::query_scalar("SELECT user_id FROM posts WHERE title = 'first'")
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(owner == id, format!("post owned by {owner}, user {id}"))
}

#[derive(Debug)]
#[allow(
    dead_code,
    reason = "the payload is read through `Debug`, when a scenario reports it"
)]
enum AppError {
    Refused,
    Sqlx(sqlx::Error),
}

impl From<sqlx::Error> for AppError {
    fn from(error: sqlx::Error) -> Self {
        AppError::Sqlx(error)
    }
}

// The caller's own error type comes back as the callback returned it, and
// what the callback wrote is gone.
pub(super) async fn transaction_rolls_back_when_the_callback_fails(
    conn: &mut DoConnection,
) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;

    let result = conn
        .transaction(|tx| async move {
            sqlx::query("INSERT INTO users (email) VALUES ('undone@example.com')")
                .execute(&tx)
                .await?;
            Err::<(), _>(AppError::Refused)
        })
        .await;
    ensure(
        matches!(result, Err(AppError::Refused)),
        format!("{result:?}"),
    )?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(count == 0, format!("{count} users: the insert survived"))
}

// A statement failing inside is the caller's error, `ErrorKind` and all,
// and the statement before it is rolled back.
pub(super) async fn transaction_keeps_the_statement_error(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    insert_user(conn, "taken@example.com").await?;

    let result = conn
        .transaction(|tx| async move {
            sqlx::query("INSERT INTO users (email) VALUES ('fresh@example.com')")
                .execute(&tx)
                .await?;
            sqlx::query("INSERT INTO users (email) VALUES ('taken@example.com')")
                .execute(&tx)
                .await?;
            Ok::<_, sqlx::Error>(())
        })
        .await;
    match &result {
        Err(sqlx::Error::Database(error)) if matches!(error.kind(), ErrorKind::UniqueViolation) => {
        }
        other => return Err(format!("{other:?}")),
    }

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(
        count == 1,
        format!("{count} users: the first insert survived"),
    )
}

// What a batch cannot do: read a write back, and decide on it, before the
// transaction ends.
pub(super) async fn transaction_reads_its_own_writes(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;

    let (inside, total) = conn
        .transaction(|tx| async move {
            for email in ["r1@example.com", "r2@example.com"] {
                sqlx::query("INSERT INTO users (email) VALUES (?)")
                    .bind(email)
                    .execute(&tx)
                    .await?;
            }
            let inside: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
                .fetch_one(&tx)
                .await?;
            let updated = sqlx::query("UPDATE users SET age = ?")
                .bind(inside)
                .execute(&tx)
                .await?;
            Ok::<_, sqlx::Error>((inside, updated.rows_affected()))
        })
        .await
        .map_err(fail)?;
    ensure(
        inside == 2 && total == 2,
        format!("saw {inside}, updated {total}"),
    )?;

    let ages: Vec<i64> = sqlx::query_scalar("SELECT age FROM users")
        .fetch_all(conn)
        .await
        .map_err(fail)?;
    ensure(ages == [2, 2], format!("ages {ages:?}"))
}
