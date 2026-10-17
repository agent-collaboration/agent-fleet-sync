use super::*;

// ---- Worker-level: a second request while a transaction is open.

// The object delivers no other request while a transaction runs, even while
// its callback awaits a timer -- which is what keeps a concurrent request's
// writes out of it, and out of its rollback. Measured in #7; pinned here.
pub(super) async fn transaction_holds_off_other_requests(env: &Env) -> Outcome {
    let step = |op: &'static str| async move {
        step_on(env, op)
            .await
            .map_err(|error| format!("step `{op}`: {error}"))
    };

    step("reset").await?;
    let other = async {
        worker::Delay::from(std::time::Duration::from_millis(50)).await;
        step("insert").await
    };
    let (held, other) = futures_util::future::try_join(step("hold"), other).await?;
    let after = step("emails").await?;

    ensure(
        held.get("rolled_back") == Some(&serde_json::json!(true)),
        format!("hold: {held}"),
    )?;
    ensure(
        held.get("seen_inside") == Some(&serde_json::json!(["held"])),
        format!("the transaction saw {:?}", held.get("seen_inside")),
    )?;
    let (ended, at) = (
        held.get("ended").and_then(serde_json::Value::as_f64),
        other.get("at").and_then(serde_json::Value::as_f64),
    );
    ensure(
        matches!((ended, at), (Some(ended), Some(at)) if at >= ended),
        format!("the insert ran at {at:?}, before the transaction ended at {ended:?}"),
    )?;
    ensure(
        after == serde_json::json!(["other"]),
        format!("left {after}: the rollback took the other request's write, or kept its own"),
    )
}

// One step of a Worker-level scenario, on its own object so it cannot
// disturb the object the other scenarios share.
pub(super) async fn step_on(env: &Env, op: &str) -> worker::Result<serde_json::Value> {
    let namespace = env.durable_object("TEST")?;
    let stub = namespace.id_from_name("concurrency")?.get_stub()?;
    let mut response = stub.fetch_with_str(&format!("https://do/op/{op}")).await?;
    response.json().await
}

// How the `hold` step's callback ends: with what it saw, which rolls the
// transaction back -- or with a query's error, which is a failed step.
#[derive(Debug)]
#[allow(
    dead_code,
    reason = "the payload is read through `Debug`, when a scenario reports it"
)]
enum HoldError {
    Held(Vec<String>),
    Sqlx(sqlx::Error),
}

impl From<sqlx::Error> for HoldError {
    fn from(error: sqlx::Error) -> Self {
        HoldError::Sqlx(error)
    }
}

pub(super) async fn op_step(conn: &DoConnection, op: &str) -> worker::Result<Response> {
    let now = || worker::Date::now().as_millis();

    let body = match op {
        "reset" => {
            reset(conn).await.map_err(worker::Error::RustError)?;
            serde_json::json!({})
        }
        "emails" => {
            let emails: Vec<String> = sqlx::query_scalar("SELECT email FROM users ORDER BY id")
                .fetch_all(conn)
                .await
                .map_err(|error| worker::Error::RustError(error.to_string()))?;
            serde_json::json!(emails)
        }
        "insert" => {
            let at = now();
            sqlx::query("INSERT INTO users (email) VALUES ('other')")
                .execute(conn)
                .await
                .map_err(|error| worker::Error::RustError(error.to_string()))?;
            serde_json::json!({ "at": at })
        }
        "hold" => {
            let result = conn
                .transaction(|tx| async move {
                    sqlx::query("INSERT INTO users (email) VALUES ('held')")
                        .execute(&tx)
                        .await?;
                    worker::Delay::from(std::time::Duration::from_millis(300)).await;
                    let seen: Vec<String> =
                        sqlx::query_scalar("SELECT email FROM users ORDER BY id")
                            .fetch_all(&tx)
                            .await?;
                    Err::<(), _>(HoldError::Held(seen))
                })
                .await;
            let seen = match result {
                Err(HoldError::Held(seen)) => seen,
                other => return Response::error(format!("{other:?}"), 500),
            };
            serde_json::json!({ "rolled_back": true, "seen_inside": seen, "ended": now() })
        }
        _ => return Response::error(format!("no step `{op}`"), 404),
    };

    Response::from_json(&body)
}
