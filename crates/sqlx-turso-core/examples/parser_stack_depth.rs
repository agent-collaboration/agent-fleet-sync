//! UW-3 recreation: run explicitly; the pinned upstream parser can abort this process.
use std::{error::Error, thread};
use turso_parser::parser::Parser;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let depth = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "16".to_owned())
        .parse::<usize>()?;
    if depth > 100 {
        return Err("depth must be at most 100".into());
    }
    let query = format!("SELECT {}1{};", "(".repeat(depth), ")".repeat(depth));
    println!("default native thread stack, depth={depth}, SQL={query}");
    thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut parser = Parser::new(query.as_bytes());
        let _ = parser.next().transpose()?;
        println!("parse completed without an engine abort");
        Ok(())
    })
    .join()
    .map_err(|_| "parser thread panicked")??;
    Ok(())
}
