//! Optional inherited HTTP scenarios for the local test Worker in `tests/worker`.
//! Run explicitly with `DO_WORKER_URL` and `cargo test -p sqlx-durable-object --test driver -- --ignored`.
//! The foundation's mandatory integration journey lives in `tests/workerd` at the workspace root.

use std::io::Read;

fn get(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut body = String::new();
    ureq::get(url)
        .call()?
        .into_body()
        .into_reader()
        .read_to_string(&mut body)?;
    Ok(body)
}

#[test]
#[ignore = "requires the local test Worker; run with DO_WORKER_URL and --ignored"]
fn every_scenario_passes_against_a_local_durable_object() -> Result<(), Box<dyn std::error::Error>>
{
    let base = std::env::var("DO_WORKER_URL")?;
    let base = base.trim_end_matches('/');

    let scenarios: Vec<String> = serde_json::from_str(&get(base)?)?;
    if scenarios.is_empty() {
        return Err("the Worker lists no scenarios".into());
    }

    // All of them, then one failure listing every scenario that failed:
    // stopping at the first would hide how much else is broken.
    let failures: Vec<String> = scenarios
        .iter()
        .filter_map(|name| {
            let body = match get(&format!("{base}/{name}")) {
                Ok(body) => body,
                Err(error) => return Some(format!("{name}: {error}")),
            };
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(outcome) if outcome["ok"] == true => None,
                Ok(outcome) => Some(format!("{name}: {}", outcome["error"])),
                // A panic in the Worker answers with an error page, not JSON.
                Err(_) => Some(format!("{name}: {body}")),
            }
        })
        .collect();

    if !failures.is_empty() {
        return Err(format!("failed:\n{}", failures.join("\n")).into());
    }
    eprintln!("{} scenarios passed", scenarios.len());
    Ok(())
}
