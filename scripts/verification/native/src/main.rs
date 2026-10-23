//! Consumer-fit journeys. Run through scripts/verification/<journey>.sh --help.
//! The adopted oracle is consumer-fit VD SO52, SO53, SO56 and SO57.
mod copy_containment;
mod fixture_service;
mod frame_shapes;
mod holder_refusals;
mod import_fixture;
mod session_observation;
#[allow(
    dead_code,
    unused_imports,
    unexpected_cfgs,
    reason = "reuse the consumer harness without its optional delivery-only tests"
)]
#[path = "../../../../tests/consumer/tests/support/mod.rs"]
mod support;
mod target_capability;

use fleet_sync_test_consumer::TestResult;
use serde_json::{Value, json};
use std::path::Path;

#[derive(Default)]
struct Observations {
    passed: usize,
    failed: usize,
    gaps: usize,
}
impl Observations {
    fn check(&mut self, case: &str, passed: bool, expected: Value, observed: Value) {
        if passed {
            self.passed += 1;
        } else {
            self.failed += 1;
        }
        println!(
            "{}",
            json!({"case":case,"status":if passed {"pass"} else {"fail"},"expected":expected,"observed":observed})
        );
    }
    fn gap(&mut self, case: &str, detail: &str) {
        self.gaps += 1;
        println!("{}", json!({"case":case,"status":"gap","detail":detail}));
    }
}

#[tokio::main]
async fn main() {
    let arguments = std::env::args().collect::<Vec<_>>();
    let mut observations = Observations::default();
    let result: TestResult = async {
        let journey = arguments.get(1).ok_or("missing journey")?;
        let evidence = Path::new(arguments.get(2).ok_or("missing evidence directory")?);
        let state = evidence.join("state");
        std::fs::create_dir(&state)?;
        match journey.as_str() {
            "frame-shapes" => frame_shapes::run(&state, &mut observations).await,
            "holder-refusals" => holder_refusals::run(&state, &mut observations).await,
            "target-capability" => target_capability::run(&state, &mut observations).await,
            "copy-containment" => copy_containment::run(&state, &mut observations).await,
            _ => Err("unknown journey".into()),
        }
    }
    .await;
    let exit = if let Err(error) = result {
        println!(
            "{}",
            json!({"status":"harness-error","detail":error.to_string()})
        );
        2
    } else if observations.failed > 0 {
        1
    } else if observations.gaps > 0 {
        3
    } else {
        0
    };
    println!(
        "{}",
        json!({"status":"summary","passed":observations.passed,"failed":observations.failed,"gaps":observations.gaps,"exit":exit})
    );
    std::process::exit(exit);
}
