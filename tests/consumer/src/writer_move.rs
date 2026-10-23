use crate::{SampleSchema, TestResult};
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{DatabaseId, PeerId};
use fleet_sync_writer::{MoveAsidePhase, ReplicatedDatabase, WriterOptions};
use runtime_primitives::SystemClock;
use std::{path::Path, sync::Arc, time::Duration};

pub async fn run_move(
    directory: &Path,
    destination: &Path,
    database: DatabaseId,
    phase: Option<MoveAsidePhase>,
    retry: bool,
) -> TestResult {
    let source = directory.join("writer.db");
    if retry {
        ReplicatedDatabase::<SampleSchema>::retry_move_aside(database, &source, destination)
            .await?;
    } else {
        tokio::fs::metadata(&source).await?;
        let peer = PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?;
        let owner = Arc::new(
            ReplicatedDatabase::open_with(
                &source,
                database,
                SampleSchema::new()?,
                LeaseHolder::provisional(
                    database,
                    peer,
                    LeasePolicy {
                        lease_duration: Duration::from_secs(600),
                        renew_interval: Duration::from_secs(1),
                    },
                ),
                Arc::new(SystemClock),
                WriterOptions::default(),
            )
            .await?,
        );
        if let Some(phase) = phase {
            owner.hold_move_aside(phase);
            let moving = owner.clone();
            let destination = destination.to_path_buf();
            let task = tokio::spawn(async move { moving.move_aside(&destination).await });
            let observed = owner.wait_move_aside().await?;
            println!("MOVE_PHASE {}", serde_json::to_string(&observed)?);
            task.await??;
        } else {
            owner.move_aside(destination).await?;
        }
    }
    println!("MOVE_DONE");
    Ok(())
}
