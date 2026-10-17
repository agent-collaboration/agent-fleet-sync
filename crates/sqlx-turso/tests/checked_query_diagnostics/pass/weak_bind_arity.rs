// Known limitation: Turso reports no parameter count, so an extra bind still compiles.
fn main() {
    let _query = sqlx_turso::query!(r#"SELECT ? AS "value!: i64""#, 1_i64, 2_i64);
}
