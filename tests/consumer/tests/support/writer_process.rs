use fleet_sync_protocol::{DatabaseId, SessionId};
use fleet_sync_test_consumer::{TestResult, WriterCommand, WriterReply};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

pub struct WriterProcess {
    pub child: Child,
    pub database: DatabaseId,
    pub session: SessionId,
    input: Option<ChildStdin>,
    lines: Lines<BufReader<ChildStdout>>,
}
impl WriterProcess {
    pub async fn start(
        path: &Path,
        endpoint: &str,
        mode: &str,
        database: Option<DatabaseId>,
    ) -> TestResult<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fleet-sync-test-consumer"));
        command.arg(path).arg("writer").arg(endpoint).arg(mode);
        if let Some(database) = database {
            command.arg(database.as_uuid().to_string());
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or("writer stdin missing")?;
        let mut lines = BufReader::new(child.stdout.take().ok_or("writer stdout missing")?).lines();
        let (pid, database, session) = timeout(Duration::from_secs(30), async {
            while let Some(line) = lines.next_line().await? {
                if let Some(ready) = line.strip_prefix("WRITER_READY ") {
                    let mut fields = ready.split_whitespace();
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                        fields.next().ok_or("writer PID missing")?.parse::<u32>()?,
                        DatabaseId::new(fields.next().ok_or("writer database missing")?.parse()?)?,
                        SessionId::new(fields.next().ok_or("writer session missing")?.parse()?)?,
                    ));
                }
            }
            Err("writer exited before ready".into())
        })
        .await??;
        assert_eq!(child.id(), Some(pid));
        assert_ne!(pid, std::process::id());
        Ok(Self {
            child,
            database,
            session,
            input: Some(input),
            lines,
        })
    }
    pub async fn command(&mut self, command: WriterCommand) -> TestResult<WriterReply> {
        let wait = if matches!(command, WriterCommand::WaitReady) {
            Duration::from_secs(70)
        } else if matches!(command, WriterCommand::WaitSeedPage) {
            // The engine retains a killed drive's ownership for 30 seconds.
            Duration::from_secs(45)
        } else {
            Duration::from_secs(20)
        };
        let input = self.input.as_mut().ok_or("writer input closed")?;
        input
            .write_all(serde_json::to_string(&command)?.as_bytes())
            .await?;
        input.write_all(b"\n").await?;
        input.flush().await?;
        timeout(wait, async {
            while let Some(line) = self.lines.next_line().await? {
                if let Some(reply) = line.strip_prefix("WRITER_REPLY ") {
                    let reply: WriterReply = serde_json::from_str(reply)?;
                    if let WriterReply::Error { message } = reply {
                        return Err(message.into());
                    }
                    return Ok(reply);
                }
            }
            Err("writer exited before reply".into())
        })
        .await?
    }
    pub async fn kill(&mut self) -> TestResult {
        self.child.kill().await?;
        self.child.wait().await?;
        Ok(())
    }
    pub async fn stop(mut self) -> TestResult {
        self.command(WriterCommand::Shutdown).await?;
        self.input.take();
        if !timeout(Duration::from_secs(15), self.child.wait())
            .await??
            .success()
        {
            return Err("writer shutdown failed".into());
        }
        Ok(())
    }
}
