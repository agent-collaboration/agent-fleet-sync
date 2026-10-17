use crate::support::ServiceProcess;
use fleet_sync_protocol::{ItemId, PeerId};
use fleet_sync_test_consumer::TestResult;
use fleet_sync_test_consumer::delivery_node::{CustodySide, NodeCommand};
use serde_json::Value;
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

pub struct DeliveryNode {
    pub child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
}
impl DeliveryNode {
    pub async fn start(path: &Path, peer: u128) -> TestResult<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_delivery_node"))
            .arg(path)
            .arg(peer.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or("no node input")?;
        let mut output = BufReader::new(child.stdout.take().ok_or("no node output")?).lines();
        let ready = timeout(Duration::from_secs(20), output.next_line())
            .await??
            .ok_or("no node READY")?;
        let pid = ready
            .strip_prefix("READY ")
            .ok_or("invalid node READY")?
            .parse::<u32>()?;
        assert_eq!(child.id(), Some(pid));
        assert_ne!(pid, std::process::id());
        Ok(Self {
            child,
            input,
            output,
        })
    }
    pub async fn command(&mut self, command: NodeCommand) -> TestResult<Value> {
        let bytes = serde_json::to_vec(&command)?;
        self.input.write_all(&bytes).await?;
        self.input.write_all(b"\n").await?;
        self.input.flush().await?;
        // Engine ownership expires after its real 30-second lease on process recovery.
        let line = timeout(Duration::from_secs(50), self.output.next_line())
            .await??
            .ok_or("node closed")?;
        let answer: Value = serde_json::from_str(&line)?;
        if let Some(error) = answer.get("error") {
            return Err(
                format!("node: {error}; command={}", String::from_utf8_lossy(&bytes)).into(),
            );
        }
        Ok(answer["result"].clone())
    }
    pub async fn connect(&mut self, service: &ServiceProcess) -> TestResult {
        self.command(NodeCommand::Connect { port: service.port })
            .await?;
        Ok(())
    }
    pub async fn wait(&mut self, item: ItemId, side: CustodySide, kind: &str) -> TestResult<Value> {
        self.command(NodeCommand::WaitStatus {
            item,
            side,
            kind: kind.into(),
        })
        .await
    }
    pub async fn kill(&mut self) -> TestResult {
        self.child.kill().await?;
        self.child.wait().await?;
        Ok(())
    }
    pub async fn stop(mut self) -> TestResult {
        self.command(NodeCommand::Shutdown).await?;
        assert!(
            timeout(Duration::from_secs(20), self.child.wait())
                .await??
                .success()
        );
        Ok(())
    }
}
pub fn item_id(number: u64) -> TestResult<ItemId> {
    Ok(ItemId::new(durable_steps::Uuid::from_u128(
        0x01900000000070008000000000000000 + u128::from(number),
    ))?)
}
pub async fn enqueue(
    sender: &mut DeliveryNode,
    number: u64,
    recipient: PeerId,
) -> TestResult<ItemId> {
    let item = item_id(number)?;
    sender
        .command(NodeCommand::Enqueue {
            item,
            recipient,
            body: format!("delivery body {number}"),
            valid_until: None,
        })
        .await?;
    Ok(item)
}
pub fn effect_count(snapshot: &Value, item: ItemId) -> usize {
    snapshot["effects"].as_array().map_or(0, |effects| {
        effects
            .iter()
            .filter(|effect| effect["item"] == item.as_uuid().to_string())
            .count()
    })
}
