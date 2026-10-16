use super::*;

pub(super) async fn select_literals(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    let row = sqlx::query("SELECT 1 AS n, 'x' AS s")
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure(row.get::<i64, _>("n") == 1, "n")?;
    ensure(row.get::<String, _>("s") == "x", "s")
}

pub(super) async fn bind_round_trip(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    let row = sqlx::query("SELECT ?1 AS i, ?2 AS r, ?3 AS t, ?4 AS b, ?5 AS small")
        .bind(42_i64)
        .bind(1.5_f64)
        .bind("héllo, wörld")
        .bind(true)
        .bind(-7_i16)
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure(row.get::<i64, _>("i") == 42, "i64")?;
    ensure((row.get::<f64, _>("r") - 1.5).abs() < f64::EPSILON, "f64")?;
    ensure(row.get::<&str, _>("t") == "héllo, wörld", "text")?;
    ensure(row.get::<bool, _>("b"), "bool")?;
    ensure(row.get::<i16, _>("small") == -7, "i16")
}

pub(super) async fn null_values(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    let row = sqlx::query("SELECT NULL AS a, ?1 AS b")
        .bind(Option::<String>::None)
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure(row.get::<Option<String>, _>("a").is_none(), "literal NULL")?;
    ensure(row.get::<Option<i64>, _>("b").is_none(), "bound NULL")?;
    ensure(row.try_get::<i64, _>("a").is_err(), "NULL into i64")
}

pub(super) async fn whole_real_decodes_as_f64(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    let value: f64 = sqlx::query_scalar("SELECT 3.0")
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure((value - 3.0).abs() < f64::EPSILON, format!("got {value}"))
}

// Open question 1: which JavaScript representation Do stores as a BLOB, and
// what it hands back.
pub(super) async fn blob_round_trip(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    let bytes = vec![0_u8, 1, 127, 128, 255];

    sqlx::query("INSERT INTO users (email, avatar) VALUES ('blob@example.com', ?)")
        .bind(&bytes)
        .execute(conn)
        .await
        .map_err(fail)?;

    let row = sqlx::query("SELECT avatar, typeof(avatar) AS kind FROM users")
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    let kind: String = row.get("kind");
    ensure(kind == "blob", format!("stored as {kind}"))?;
    let back: Vec<u8> = row.try_get("avatar").map_err(fail)?;
    ensure(back == bytes, format!("read back {back:?}"))
}

pub(super) async fn integer_bounds(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    const MAX: i64 = (1 << 53) - 1;

    for value in [MAX, -MAX] {
        sqlx::query("INSERT INTO numbers (value) VALUES (?)")
            .bind(value)
            .execute(conn)
            .await
            .map_err(fail)?;
    }

    let values: Vec<i64> = sqlx::query_scalar("SELECT value FROM numbers ORDER BY value")
        .fetch_all(conn)
        .await
        .map_err(fail)?;
    ensure(values == [-MAX, MAX], format!("read back {values:?}"))?;

    let refused = sqlx::query("SELECT ?").bind(MAX + 1).execute(conn).await;
    ensure(
        matches!(refused, Err(sqlx::Error::Encode(_))),
        format!("binding 2^53: {refused:?}"),
    )?;

    // Beyond 2^53 on the database side: comes back as a REAL, which must not
    // decode as i64.
    let row = sqlx::query("SELECT 9007199254740993 AS big")
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(
        row.try_get::<i64, _>("big").is_err(),
        "2^53 + 1 decoded as i64",
    )
}

pub(super) async fn duplicate_column_names(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    let user = insert_user(conn, "dup@example.com").await?;
    let post: i64 =
        sqlx::query_scalar("INSERT INTO posts (user_id, title) VALUES (?, 't') RETURNING id")
            .bind(user)
            .fetch_one(conn)
            .await
            .map_err(fail)?;

    let row = sqlx::query("SELECT u.id, p.id FROM users u JOIN posts p ON p.user_id = u.id")
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure(row.len() == 2, format!("{} columns", row.len()))?;
    ensure(row.get::<i64, _>(0) == user, "first id")?;
    ensure(row.get::<i64, _>(1) == post, "second id")?;
    // As sqlx-sqlite: the name finds the last column that has it.
    ensure(row.get::<i64, _>("id") == post, "id by name")
}

pub(super) async fn empty_result(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;

    let rows = sqlx::query("SELECT id FROM users")
        .fetch_all(conn)
        .await
        .map_err(fail)?;
    ensure(rows.is_empty(), "fetch_all")?;

    let row = sqlx::query("SELECT id FROM users")
        .fetch_optional(conn)
        .await
        .map_err(fail)?;
    ensure(row.is_none(), "fetch_optional")?;

    let missing = sqlx::query("SELECT id FROM users").fetch_one(conn).await;
    ensure(
        matches!(missing, Err(sqlx::Error::RowNotFound)),
        "fetch_one",
    )
}

pub(super) async fn fetch_optional_takes_the_first_row(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    let n: Option<i64> = sqlx::query_scalar("SELECT 1 UNION ALL SELECT 2 ORDER BY 1")
        .fetch_optional(conn)
        .await
        .map_err(fail)?;

    ensure(n == Some(1), format!("got {n:?}"))
}

pub(super) async fn execute_reports_changes(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;

    let inserted = sqlx::query("INSERT INTO users (email) VALUES ('meta@example.com')")
        .execute(conn)
        .await
        .map_err(fail)?;
    let id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE email = 'meta@example.com'")
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure(inserted.rows_affected() == 1, "insert rows_affected")?;
    ensure(
        inserted.last_insert_rowid() == id,
        format!(
            "last_insert_rowid {:?}, id {id}",
            inserted.last_insert_rowid()
        ),
    )?;

    insert_user(conn, "meta2@example.com").await?;
    let updated = sqlx::query("UPDATE users SET name = 'n'")
        .execute(conn)
        .await
        .map_err(fail)?;
    ensure(
        updated.rows_affected() == 2,
        format!("update rows_affected {}", updated.rows_affected()),
    )
}

pub(super) async fn constraint_errors(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    insert_user(conn, "taken@example.com").await?;

    type ConstraintCase = (&'static str, fn(&ErrorKind) -> bool);
    let cases: [ConstraintCase; 4] = [
        (
            "INSERT INTO users (email) VALUES ('taken@example.com')",
            |k| matches!(k, ErrorKind::UniqueViolation),
        ),
        ("INSERT INTO users (email) VALUES (NULL)", |k| {
            matches!(k, ErrorKind::NotNullViolation)
        }),
        (
            "INSERT INTO users (email, age) VALUES ('neg@example.com', -1)",
            |k| matches!(k, ErrorKind::CheckViolation),
        ),
        (
            "INSERT INTO posts (user_id, title) VALUES (999999, 't')",
            |k| matches!(k, ErrorKind::ForeignKeyViolation),
        ),
    ];

    for (sql, expected) in cases {
        match sqlx::query(sql).execute(conn).await {
            Err(sqlx::Error::Database(error)) if expected(&error.kind()) => {}
            Err(sqlx::Error::Database(error)) => {
                return Err(format!(
                    "{sql}: kind {:?} for message {:?}",
                    error.kind(),
                    error.message()
                ))
            }
            other => return Err(format!("{sql}: {other:?}")),
        }
    }

    Ok(())
}

pub(super) async fn batch_reports_each_statement(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;

    let results = conn
        .execute_batch([
            sqlx::query("INSERT INTO users (email) VALUES (?)").bind("b1@example.com"),
            sqlx::query("INSERT INTO users (email) VALUES (?)").bind("b2@example.com"),
            sqlx::query("UPDATE users SET name = 'x'"),
        ])
        .await
        .map_err(fail)?;

    let changes: Vec<u64> = results.iter().map(|r| r.rows_affected()).collect();
    ensure(changes == [1, 1, 2], format!("rows_affected {changes:?}"))
}

pub(super) async fn batch_is_atomic(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    insert_user(conn, "taken@example.com").await?;

    let result = conn
        .execute_batch([
            sqlx::query("INSERT INTO users (email) VALUES ('fresh@example.com')"),
            sqlx::query("INSERT INTO users (email) VALUES ('taken@example.com')"),
        ])
        .await;
    ensure(result.is_err(), "the batch succeeded")?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(
        count == 1,
        format!("{count} users: the first insert survived"),
    )
}

pub(super) async fn begin_fails(conn: &mut DoConnection) -> Outcome {
    ensure(conn.begin().await.is_err(), "begin succeeded")
}

pub(super) async fn from_row(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    sqlx::query(
        "INSERT INTO users (email, name, age, score) VALUES ('row@example.com', 'Row', 30, 2.5)",
    )
    .execute(conn)
    .await
    .map_err(fail)?;

    let user = sqlx::query_as::<Do, User>("SELECT * FROM users")
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure(user.email == "row@example.com", "email")?;
    ensure(user.name.as_deref() == Some("Row"), "name")?;
    ensure(user.age == Some(30), "age")?;
    ensure(user.score == Some(2.5), "score")?;
    ensure(user.active, "active")?;
    ensure(user.avatar.is_none(), "avatar")?;
    ensure(user.id > 0, "id")
}

// The reason `js` goes to `worker-sys` rather than `worker`: a statement Do
// rejects must come back as an error, not abort the Worker. A panic here
// would answer with an error page, which `tests/driver.rs` reports as such.
pub(super) async fn invalid_sql_is_an_error(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;

    for (sql, expected) in [
        ("SELEC 1", "syntax error"),
        ("SELECT * FROM missing", "no such table"),
    ] {
        match sqlx::query(sql).fetch_all(conn).await {
            Err(sqlx::Error::Database(error)) if error.message().contains(expected) => {}
            other => return Err(format!("{sql}: {other:?}")),
        }
    }

    Ok(())
}

pub(super) async fn wrong_argument_count_is_an_error(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;

    let too_few = sqlx::query("SELECT ?1, ?2")
        .bind(1_i64)
        .fetch_all(conn)
        .await;
    ensure(
        matches!(too_few, Err(sqlx::Error::Database(_))),
        format!("too few: {too_few:?}"),
    )?;

    let too_many = sqlx::query("SELECT ?")
        .bind(1_i64)
        .bind(2_i64)
        .fetch_all(conn)
        .await;
    ensure(
        matches!(too_many, Err(sqlx::Error::Database(_))),
        format!("too many: {too_many:?}"),
    )
}

pub(super) async fn empty_batch(conn: &mut DoConnection) -> Outcome {
    let results = conn
        .execute_batch(std::iter::empty::<sqlx::query::Query<'_, Do, _>>())
        .await
        .map_err(fail)?;

    ensure(results.is_empty(), format!("{} results", results.len()))
}

// The encode error is caught before anything reaches Do, so not even the
// statement ahead of the bad one runs.
pub(super) async fn batch_with_unencodable_argument_runs_nothing(
    conn: &mut DoConnection,
) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;

    let result = conn
        .execute_batch([
            sqlx::query("INSERT INTO users (email) VALUES ('first@example.com')"),
            sqlx::query("INSERT INTO numbers (value) VALUES (?)").bind(1_i64 << 53),
        ])
        .await;
    ensure(
        matches!(result, Err(sqlx::Error::Encode(_))),
        format!("{result:?}"),
    )?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(count == 0, format!("{count} users: the first insert ran"))
}

pub(super) async fn text_edge_cases(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    let astral = "🦀 𝄞 \u{0} null byte";

    let row = sqlx::query("SELECT ?1 AS empty, ?2 AS missing, ?3 AS astral")
        .bind("")
        .bind(Option::<&str>::None)
        .bind(astral)
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure(
        row.get::<Option<String>, _>("empty").as_deref() == Some(""),
        "the empty string came back as NULL",
    )?;
    ensure(row.get::<Option<String>, _>("missing").is_none(), "NULL")?;
    let back: String = row.get("astral");
    ensure(back == astral, format!("read back {back:?}"))
}

pub(super) async fn blob_edge_cases(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    let large: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();

    for (email, bytes) in [
        ("empty@example.com", Vec::new()),
        ("large@example.com", large),
    ] {
        sqlx::query("INSERT INTO users (email, avatar) VALUES (?, ?)")
            .bind(email)
            .bind(&bytes)
            .execute(conn)
            .await
            .map_err(fail)?;

        let row = sqlx::query("SELECT avatar, typeof(avatar) AS kind FROM users WHERE email = ?")
            .bind(email)
            .fetch_one(conn)
            .await
            .map_err(fail)?;

        let kind: String = row.get("kind");
        ensure(kind == "blob", format!("{email}: stored as {kind}"))?;
        let back: Option<Vec<u8>> = row.try_get("avatar").map_err(fail)?;
        ensure(
            back.as_ref() == Some(&bytes),
            format!("{email}: read back {:?} bytes", back.map(|b| b.len())),
        )?;
    }

    Ok(())
}

// `&DoConnection` is an executor, so one connection can run queries side by
// side within a request.
pub(super) async fn concurrent_queries_share_a_connection(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;

    let (a, b): (i64, String) = try_join(
        sqlx::query_scalar("SELECT 1").fetch_one(conn),
        sqlx::query_scalar("SELECT 'two'").fetch_one(conn),
    )
    .await
    .map_err(fail)?;

    ensure(a == 1 && b == "two", format!("got {a}, {b:?}"))
}

pub(super) async fn query_builder_bulk_insert(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    let users = [
        ("qb1@example.com", 10),
        ("qb2@example.com", 20),
        ("qb3@example.com", 30),
    ];

    let mut builder = sqlx::QueryBuilder::<Do>::new("INSERT INTO users (email, age) ");
    builder.push_values(users, |mut row, (email, age)| {
        row.push_bind(email).push_bind(age);
    });
    let inserted = builder.build().execute(conn).await.map_err(fail)?;
    ensure(
        inserted.rows_affected() == 3,
        format!("rows_affected {}", inserted.rows_affected()),
    )?;

    let mut builder = sqlx::QueryBuilder::<Do>::new("SELECT SUM(age) FROM users WHERE email IN (");
    let mut emails = builder.separated(", ");
    for (email, _) in &users[..2] {
        emails.push_bind(*email);
    }
    emails.push_unseparated(")");
    let sum: i64 = builder
        .build_query_scalar()
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(sum == 30, format!("sum {sum}"))
}

#[derive(Debug, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
struct UserId(i64);

#[derive(Debug, Clone, Copy, PartialEq, sqlx::Type)]
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

pub(super) async fn derived_types(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    let member = sqlx::query_as::<Do, Member>("SELECT ?1 AS id, ?2 AS role")
        .bind(UserId(5))
        .bind(Role::Writer)
        .fetch_one(conn)
        .await
        .map_err(fail)?;

    ensure(member.id == UserId(5), format!("id {:?}", member.id))?;
    ensure(
        member.role == Role::Writer,
        format!("role {:?}", member.role),
    )?;

    let reader: Role = sqlx::query_scalar("SELECT 1")
        .fetch_one(conn)
        .await
        .map_err(fail)?;
    ensure(reader == Role::Reader, format!("1 decoded as {reader:?}"))?;

    let unknown = sqlx::query_scalar::<Do, Role>("SELECT 3")
        .fetch_one(conn)
        .await;
    ensure(unknown.is_err(), format!("3 decoded: {unknown:?}"))
}

// `changes()` keeps the count of the last write, so a statement that wrote
// nothing must not report the one before it.
pub(super) async fn select_reports_no_changes(conn: &mut DoConnection) -> Outcome {
    let conn = &*conn;
    reset(conn).await?;
    insert_user(conn, "stale@example.com").await?;

    let selected = sqlx::query("SELECT * FROM users")
        .execute(conn)
        .await
        .map_err(fail)?;
    ensure(
        selected.rows_affected() == 0,
        format!("SELECT rows_affected {}", selected.rows_affected()),
    )?;
    ensure(
        selected.last_insert_rowid() == 0,
        format!(
            "SELECT last_insert_rowid {:?}",
            selected.last_insert_rowid()
        ),
    )?;

    let ignored = sqlx::query(
        "INSERT INTO users (email) VALUES ('stale@example.com') ON CONFLICT DO NOTHING",
    )
    .execute(conn)
    .await
    .map_err(fail)?;
    ensure(
        ignored.rows_affected() == 0,
        format!("ignored INSERT rows_affected {}", ignored.rows_affected()),
    )
}

// What `DoTransactionManager` refuses on the driver's side, `sql.exec` refuses
// on its own: there is no way round it by sending `BEGIN` as a query.
pub(super) async fn begin_statement_is_refused(conn: &mut DoConnection) -> Outcome {
    let result = sqlx::query("BEGIN TRANSACTION").execute(&*conn).await;
    ensure(result.is_err(), format!("BEGIN ran: {result:?}"))
}
