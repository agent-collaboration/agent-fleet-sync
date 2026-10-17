fn main() {
    let _query = sqlx_turso::query!(r#"SELECT 1 AS "id!: i64""#);
}
