fn main() {
    let _query = sqlx_turso::query!("SELECT missing_column");
}
