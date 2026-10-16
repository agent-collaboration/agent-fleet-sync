use super::*;

// One statement per kind of result: rows from `RETURNING`, rows from a
// `SELECT`, and a write that returns none -- each item carrying both.
pub(super) async fn fetch_batch_returns_rows_and_results(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;

    let results: Vec<_> = conn
        .fetch_batch([
            sqlx::query("INSERT INTO users (email) VALUES ('bf1@example.com') RETURNING id"),
            sqlx::query("INSERT INTO users (email) VALUES ('bf2@example.com') RETURNING id"),
            sqlx::query("SELECT id, email FROM users ORDER BY id"),
            sqlx::query("UPDATE users SET name = 'x'"),
        ])
        .try_collect()
        .await
        .map_err(fail)?;

    let counts: Vec<usize> = results.iter().map(|r| r.rows().len()).collect();
    ensure(
        counts == [1, 1, 2, 0],
        format!("rows per statement {counts:?}"),
    )?;
    let changes: Vec<u64> = results.iter().map(|r| r.result().rows_affected()).collect();
    ensure(
        changes == [1, 1, 0, 2],
        format!("rows_affected {changes:?}"),
    )?;

    let first: i64 = results
        .first()
        .and_then(|result| result.rows().first())
        .ok_or("missing first row")?
        .get("id");
    let second: i64 = results
        .get(1)
        .and_then(|result| result.rows().first())
        .ok_or("missing second row")?
        .get("id");
    let selected: Vec<(i64, String)> = results
        .get(2)
        .ok_or("missing third result")?
        .rows()
        .iter()
        .map(|row| (row.get("id"), row.get("email")))
        .collect();
    ensure(
        selected
            == [
                (first, "bf1@example.com".into()),
                (second, "bf2@example.com".into()),
            ],
        format!("selected {selected:?}"),
    )
}

// What sqlc-gen-sqlx's `:batchexec`, `:batchone` and `:batchmany` would make
// of the stream: one item per input, mapped through `FromRow`.
pub(super) async fn fetch_batch_serves_sqlc_batch_kinds(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    let alice = insert_user(conn, "alice@example.com").await?;
    let bob = insert_user(conn, "bob@example.com").await?;
    for (user, title) in [(alice, "a1"), (alice, "a2"), (bob, "b1")] {
        sqlx::query("INSERT INTO posts (user_id, title) VALUES (?, ?)")
            .bind(user)
            .bind(title)
            .execute(conn)
            .await
            .map_err(fail)?;
    }

    // :batchone
    let users: Vec<User> = conn
        .fetch_batch(
            [alice, bob].map(|id| sqlx::query("SELECT * FROM users WHERE id = ?").bind(id)),
        )
        .and_then(|result| async move {
            let row = result.rows().first().ok_or(sqlx::Error::RowNotFound)?;
            User::from_row(row)
        })
        .try_collect()
        .await
        .map_err(fail)?;
    let emails: Vec<&str> = users.iter().map(|user| user.email.as_str()).collect();
    ensure(
        emails == ["alice@example.com", "bob@example.com"],
        format!("{emails:?}"),
    )?;

    // :batchmany
    let titles: Vec<Vec<String>> = conn
        .fetch_batch([alice, bob].map(|id| {
            sqlx::query("SELECT title FROM posts WHERE user_id = ? ORDER BY title").bind(id)
        }))
        .map_ok(|result| result.rows().iter().map(|row| row.get("title")).collect())
        .try_collect()
        .await
        .map_err(fail)?;
    ensure(
        titles == [vec!["a1", "a2"], vec!["b1"]],
        format!("{titles:?}"),
    )?;

    // :batchexec
    let done: Vec<()> = conn
        .fetch_batch(
            [alice, bob].map(|id| sqlx::query("DELETE FROM posts WHERE user_id = ?").bind(id)),
        )
        .map_ok(|_| ())
        .try_collect()
        .await
        .map_err(fail)?;
    ensure(done.len() == 2, format!("{} items", done.len()))?;

    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM posts")
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(left == 0, format!("{left} posts left"))
}

// A failure is the stream's one item, and nothing before it took effect.
pub(super) async fn fetch_batch_is_atomic(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    insert_user(conn, "taken@example.com").await?;

    let items: Vec<Result<_, sqlx::Error>> = conn
        .fetch_batch([
            sqlx::query("INSERT INTO users (email) VALUES ('fresh@example.com') RETURNING id"),
            sqlx::query("INSERT INTO users (email) VALUES ('taken@example.com')"),
        ])
        .collect()
        .await;
    ensure(items.len() == 1, format!("{} items", items.len()))?;
    match items.first().ok_or("missing batch error")? {
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

pub(super) async fn fetch_batch_of_nothing_is_empty(conn: &mut DoConnection) -> Outcome {
    let items: Vec<_> = conn
        .fetch_batch(std::iter::empty::<sqlx::query::Query<'_, Do, _>>())
        .try_collect()
        .await
        .map_err(fail)?;
    ensure(items.is_empty(), format!("{} items", items.len()))
}

// Durable Object storage reads every statement's cursor as arrays, batch or
// not, so -- unlike D1's batch -- both columns survive.
pub(super) async fn fetch_batch_keeps_duplicate_column_names(conn: &mut DoConnection) -> Outcome {
    let results: Vec<_> = conn
        .fetch_batch([sqlx::query("SELECT 1 AS a, 2 AS a")])
        .try_collect()
        .await
        .map_err(fail)?;

    let row = results
        .first()
        .and_then(|result| result.rows().first())
        .ok_or("missing first row")?;
    ensure(row.len() == 2, format!("{} columns", row.len()))?;
    ensure(
        row.get::<i64, _>(0) == 1 && row.get::<i64, _>(1) == 2,
        "values",
    )
}
