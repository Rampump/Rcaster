use std::{fmt, path::PathBuf, process::Stdio, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

const PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct WorkerLimits {
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub request_timeout: Duration,
}

impl Default for WorkerLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(2),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CapabilityReport {
    pub engine: String,
    pub formats: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DecodeResult {
    pub format: String,
    pub message_numbers: Vec<u16>,
}

#[derive(Debug)]
pub enum WorkerError {
    Start(String),
    InputTooLarge { size: usize, limit: usize },
    OutputTooLarge { limit: usize },
    Timeout(Duration),
    Crashed,
    Protocol(String),
    Remote(String),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start(error) => write!(formatter, "worker start failed: {error}"),
            Self::InputTooLarge { size, limit } => {
                write!(formatter, "worker input {size} exceeds limit {limit}")
            }
            Self::OutputTooLarge { limit } => {
                write!(formatter, "worker output exceeds limit {limit}")
            }
            Self::Timeout(duration) => write!(formatter, "worker timed out after {duration:?}"),
            Self::Crashed => formatter.write_str("worker exited unexpectedly"),
            Self::Protocol(error) => write!(formatter, "worker protocol error: {error}"),
            Self::Remote(error) => write!(formatter, "worker rejected request: {error}"),
        }
    }
}

impl std::error::Error for WorkerError {}

pub struct DecoderWorker {
    command: PathBuf,
    args: Vec<String>,
    limits: WorkerLimits,
    process: Option<WorkerProcess>,
    next_id: u64,
}

struct WorkerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

#[derive(Serialize)]
struct WorkerRequest {
    version: u32,
    id: u64,
    operation: &'static str,
    payload_hex: Option<String>,
}

#[derive(Deserialize)]
struct WorkerResponse {
    version: u32,
    id: u64,
    result: WorkerResult,
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
enum WorkerResult {
    Capability(CapabilityReport),
    Decoded(DecodeResult),
    Error(String),
}

impl DecoderWorker {
    pub fn new(command: impl Into<PathBuf>, limits: WorkerLimits) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            limits,
            process: None,
            next_id: 1,
        }
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub async fn capability_probe(&mut self) -> Result<CapabilityReport, WorkerError> {
        match self.exchange("capability_probe", None, 0).await? {
            WorkerResult::Capability(report) => Ok(report),
            _ => Err(WorkerError::Protocol(
                "capability probe returned the wrong result".to_owned(),
            )),
        }
    }

    pub async fn decode(&mut self, payload: &[u8]) -> Result<DecodeResult, WorkerError> {
        if payload.len() > self.limits.max_input_bytes {
            return Err(WorkerError::InputTooLarge {
                size: payload.len(),
                limit: self.limits.max_input_bytes,
            });
        }
        match self
            .exchange("decode", Some(hex_encode(payload)), payload.len())
            .await?
        {
            WorkerResult::Decoded(result) => Ok(result),
            _ => Err(WorkerError::Protocol(
                "decode returned the wrong result".to_owned(),
            )),
        }
    }

    pub async fn stop(&mut self) {
        self.shutdown_process().await;
    }

    async fn exchange(
        &mut self,
        operation: &'static str,
        payload_hex: Option<String>,
        payload_size: usize,
    ) -> Result<WorkerResult, WorkerError> {
        if payload_size > self.limits.max_input_bytes {
            return Err(WorkerError::InputTooLarge {
                size: payload_size,
                limit: self.limits.max_input_bytes,
            });
        }
        self.ensure_process().await?;
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let request = WorkerRequest {
            version: PROTOCOL_VERSION,
            id,
            operation,
            payload_hex,
        };
        let mut frame = serde_json::to_vec(&request)
            .map_err(|error| WorkerError::Protocol(error.to_string()))?;
        frame.push(b'\n');

        let process = self
            .process
            .as_mut()
            .ok_or_else(|| WorkerError::Start("worker is not running".to_owned()))?;
        if process.stdin.write_all(&frame).await.is_err() || process.stdin.flush().await.is_err() {
            self.shutdown_process().await;
            return Err(WorkerError::Crashed);
        }

        let response = match timeout(
            self.limits.request_timeout,
            read_line_bounded(&mut process.stdout, self.limits.max_output_bytes),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(ReadLineError::TooLarge)) => {
                self.shutdown_process().await;
                return Err(WorkerError::OutputTooLarge {
                    limit: self.limits.max_output_bytes,
                });
            }
            Ok(Err(ReadLineError::Eof)) => {
                self.shutdown_process().await;
                return Err(WorkerError::Crashed);
            }
            Ok(Err(ReadLineError::Io(error))) => {
                self.shutdown_process().await;
                return Err(WorkerError::Protocol(format!(
                    "worker read failed: {error}"
                )));
            }
            Err(_) => {
                self.shutdown_process().await;
                return Err(WorkerError::Timeout(self.limits.request_timeout));
            }
        };

        let response: WorkerResponse = match serde_json::from_slice(&response) {
            Ok(response) => response,
            Err(error) => {
                self.shutdown_process().await;
                return Err(WorkerError::Protocol(error.to_string()));
            }
        };
        if response.version != PROTOCOL_VERSION || response.id != id {
            self.shutdown_process().await;
            return Err(WorkerError::Protocol(
                "worker response version or request ID mismatch".to_owned(),
            ));
        }
        if let WorkerResult::Error(error) = response.result {
            return Err(WorkerError::Remote(error));
        }
        Ok(response.result)
    }

    async fn ensure_process(&mut self) -> Result<(), WorkerError> {
        if self.process.is_some() {
            return Ok(());
        }
        let mut child = Command::new(&self.command)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| WorkerError::Start(error.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| WorkerError::Start("worker stdin unavailable".to_owned()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| WorkerError::Start("worker stdout unavailable".to_owned()))?;
        self.process = Some(WorkerProcess {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        });
        Ok(())
    }

    async fn shutdown_process(&mut self) {
        if let Some(mut process) = self.process.take() {
            let _ = process.child.kill().await;
            let _ = process.child.wait().await;
        }
    }
}

impl Drop for DecoderWorker {
    fn drop(&mut self) {
        if let Some(process) = &mut self.process {
            let _ = process.child.start_kill();
        }
    }
}

enum ReadLineError {
    TooLarge,
    Eof,
    Io(std::io::Error),
}

async fn read_line_bounded<R>(reader: &mut R, max_bytes: usize) -> Result<Vec<u8>, ReadLineError>
where
    R: AsyncBufRead + Unpin,
{
    let mut result = Vec::new();
    loop {
        let buffer = reader.fill_buf().await.map_err(ReadLineError::Io)?;
        if buffer.is_empty() {
            return Err(ReadLineError::Eof);
        }
        if let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let take = newline + 1;
            if result.len() + take > max_bytes {
                return Err(ReadLineError::TooLarge);
            }
            result.extend_from_slice(&buffer[..take]);
            reader.consume(take);
            return Ok(result);
        }
        if result.len() + buffer.len() > max_bytes {
            return Err(ReadLineError::TooLarge);
        }
        let take = buffer.len();
        result.extend_from_slice(buffer);
        reader.consume(take);
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
