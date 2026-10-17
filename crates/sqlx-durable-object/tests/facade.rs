//! A consumer's view: code written against the `sqlx` facade, not
//! `sqlx-core`, with `Do` as its database. It only has to compile -- there is
//! no Durable Object on the host to run it against -- and it proves the two
//! crates agree on one `sqlx::Database` trait.
//!
//! The dev-dependency enables sqlx's `macros` feature, sqlx's default, which
//! is the one that turns on `sqlx-core/offline` and with it the required
//! `Executor::describe`. So this also proves a consumer can keep sqlx's
//! defaults.

#![allow(dead_code)]

use sqlx_durable_object::{Do, DoConnection, DoQueryResult};

#[derive(Debug, sqlx::FromRow)]
struct User {
    id: i64,
    name: String,
    email: Option<String>,
    active: bool,
    score: f64,
    avatar: Vec<u8>,
}

// The shape `sqlc-gen-sqlx` generates: generic over the executor, naming the
// database only through the associated type.
async fn get_user<'e, E>(executor: E, id: i64) -> Result<User, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Do>,
{
    sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = ?")
        .bind(id)
        .fetch_one(executor)
        .await
}

async fn rename(conn: &mut DoConnection, id: i64, name: &str) -> Result<u64, sqlx::Error> {
    let result: DoQueryResult = sqlx::query("UPDATE users SET name = ? WHERE id = ?")
        .bind(name)
        .bind(id)
        .execute(&mut *conn)
        .await?;

    Ok(result.rows_affected())
}

// A shared reference is an executor too.
async fn count(conn: &DoConnection) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(conn)
        .await
}

// A consumer's own types, through sqlx's derives. These two expand to impls
// generic over the database, so they reach `Do` with no help from this
// crate. (Enums stored as TEXT do not: that derive only targets sqlx's
// built-in drivers.)
#[derive(Debug, Clone, Copy, sqlx::Type)]
#[sqlx(transparent)]
struct UserId(i64);

#[derive(Debug, Clone, Copy, sqlx::Type)]
#[repr(i32)]
enum Role {
    Reader = 1,
    Writer = 2,
}

#[derive(Debug, sqlx::FromRow)]
struct Member {
    id: UserId,
    role: Role,
}

async fn members(conn: &DoConnection, role: Role) -> Result<Vec<Member>, sqlx::Error> {
    sqlx::query_as::<_, Member>("SELECT id, role FROM members WHERE role = ?")
        .bind(role)
        .fetch_all(conn)
        .await
}

// Bulk inserts go through `QueryBuilder`, with `?` placeholders.
async fn insert_members(conn: &DoConnection, ids: &[UserId]) -> Result<u64, sqlx::Error> {
    let mut builder = sqlx::QueryBuilder::<Do>::new("INSERT INTO members (id, role) ");
    builder.push_values(ids, |mut row, id| {
        row.push_bind(*id).push_bind(Role::Reader);
    });

    Ok(builder.build().execute(conn).await?.rows_affected())
}

// Handlers need `Send` futures (axum, for one), even inside a Worker.
fn assert_send<T: Send>(_: T) {}

fn futures_are_send(conn: &mut DoConnection) {
    assert_send(get_user(&mut *conn, 1));
    assert_send(rename(conn, 1, "x"));
    assert_send(count(conn));
    assert_send(members(conn, Role::Writer));
    assert_send(insert_members(conn, &[UserId(1)]));
    assert_send(conn.execute_batch([sqlx::query("DELETE FROM users")]));
    assert_send(conn.fetch_batch([sqlx::query("SELECT * FROM users")]));
    assert_send(conn.transaction(|tx| async move {
        sqlx::query("DELETE FROM users").execute(&tx).await?;
        Ok::<_, sqlx::Error>(())
    }));
}

#[test]
fn do_is_an_sqlx_database() {
    fn assert_database<DB: sqlx::Database>() {}
    assert_database::<Do>();
    assert_eq!(<Do as sqlx::Database>::NAME, "Do");
}
