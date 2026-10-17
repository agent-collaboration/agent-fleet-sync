//! UW-4: direct engine reproduction, independent of SQLx and project storage.
use std::error::Error;

type ReproductionResult<TResult> = Result<TResult, Box<dyn Error + Send + Sync>>;
const ORIGINAL: &str = "SELECT r.kind, r.id, r.payload, t.redacted FROM json_each(?) k CROSS JOIN record_identities r ON r.kind = json_extract(k.value, '$[0]') AND r.id = json_extract(k.value, '$[1]') LEFT JOIN work_trail_rows t ON r.kind = 'trail' AND t.row_id = r.id ORDER BY r.kind, r.id";
const CORRECTED: &str = "SELECT r.kind, r.id, r.payload, CASE WHEN r.kind = 'trail' THEN t.redacted END FROM json_each(?) k CROSS JOIN record_identities r ON r.kind = json_extract(k.value, '$[0]') AND r.id = json_extract(k.value, '$[1]') LEFT JOIN work_trail_rows t ON t.row_id = r.id ORDER BY r.kind, r.id";
const SCHEMA: &str = "
CREATE TABLE record_identities(kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(kind,id));
CREATE TABLE work_trail_rows(row_id TEXT PRIMARY KEY, redacted BOOLEAN NOT NULL CHECK(redacted IN(0,1)), kind TEXT NOT NULL DEFAULT 'trail', FOREIGN KEY(kind,row_id) REFERENCES record_identities(kind,id));
CREATE INDEX only_trail ON record_identities(id) WHERE kind = 'trail';
INSERT INTO record_identities VALUES('member','one','{}');";

async fn selected_ids(
    connection: &turso::Connection,
    sql: &str,
) -> ReproductionResult<Vec<String>> {
    let mut statement = connection.prepare(sql).await?;
    let _columns = statement.columns();
    let mut rows = statement.query([r#"[["member","one"]]"#]).await?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await? {
        ids.push(row.get::<String>(1)?);
    }
    Ok(ids)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ReproductionResult<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("left-join.sqlite");
    let database_path = path.to_str().ok_or("temporary path is not UTF-8")?;
    {
        let database = turso::Builder::new_local(database_path).build().await?;
        let connection = database.connect()?;
        connection.execute_batch(SCHEMA).await?;
        println!(
            "before reopen: original={:?}, corrected={:?}",
            selected_ids(&connection, ORIGINAL).await?,
            selected_ids(&connection, CORRECTED).await?
        );
    }
    let database = turso::Builder::new_local(database_path).build().await?;
    let connection = database.connect()?;
    connection.execute("BEGIN", ()).await?;
    let original = selected_ids(&connection, ORIGINAL).await?;
    let corrected = selected_ids(&connection, CORRECTED).await?;
    for (label, sql) in [("original", ORIGINAL), ("corrected", CORRECTED)] {
        let mut plan = connection
            .query(
                format!("EXPLAIN QUERY PLAN {sql}"),
                [r#"[["member","one"]]"#],
            )
            .await?;
        while let Some(row) = plan.next().await? {
            println!("{label} plan: {}", row.get::<String>(3)?);
        }
    }
    println!(
        "after reopen: original={original:?}, corrected={corrected:?}; both should be [\"one\"]"
    );
    connection.execute("COMMIT", ()).await?;
    if corrected != ["one"] {
        return Err("the product query workaround lost its outer row".into());
    }
    if original != ["one"] {
        return Err("engine defect reproduced: original LEFT JOIN lost its outer row".into());
    }
    Ok(())
}
