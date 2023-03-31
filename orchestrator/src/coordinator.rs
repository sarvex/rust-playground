use snafu::prelude::*;
use std::{
    collections::{HashMap, VecDeque},
    convert::{TryFrom, TryInto},
    path::{Path, PathBuf},
    process::Stdio,
    str::from_utf8,
};
use tokio::{
    process::{Child, ChildStdin, ChildStdout, Command},
    select,
    sync::mpsc,
    task::JoinSet,
};
use tokio_util::io::SyncIoBridge;

use crate::{
    message::{CommandId, CoordinatorMessage, Job, JobReport, WorkerMessage},
    sandbox::{CompileRequest, CompileResponse},
};

pub type Container = (
    Child,
    mpsc::Sender<PlaygroundMessage>,
    mpsc::Receiver<ContainerMessage>,
);
pub type RequestId = u64;

#[derive(Debug)]
pub enum PlaygroundMessage {
    Request(RequestId, HighLevelRequest),
    StdinPacket(CommandId, String),
}

#[derive(Debug)]
pub enum ContainerMessage {
    Response(RequestId, HighLevelResponse),
    StdoutPacket(CommandId, String),
    StderrPacket(CommandId, String),
}

#[derive(Debug)]
pub enum HighLevelRequest {
    Compile(CompileRequest),
}

#[derive(Debug)]
pub enum HighLevelResponse {
    Compile(CompileResponse),
}

#[derive(Debug)]
pub struct Coordinator {
    free_containers: VecDeque<Container>,
    worker_project_dir: PathBuf,
}

impl Coordinator {
    pub fn new(project_dir: &Path) -> Self {
        Coordinator {
            free_containers: VecDeque::new(),
            worker_project_dir: project_dir.to_path_buf(),
        }
    }

    pub fn allocate(&mut self) -> Result<Container, Error> {
        if let Some(container) = self.free_containers.pop_front() {
            Ok(container)
        } else {
            spawn_container(self.worker_project_dir.as_path())
        }
    }
}

pub type Result<T, E = Error> = ::std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Reached system process limit"))]
    SpawnWorker { source: std::io::Error },

    #[snafu(display("Worker process's stdin not captured"))]
    WorkerStdinCapture,

    #[snafu(display("Worker process's stdout not captured"))]
    WorkerStdoutCapture,

    #[snafu(display("Failed to flush child stdin"))]
    WorkerStdinFlush { source: std::io::Error },

    #[snafu(display("Failed to deserialize worker message"))]
    WorkerMessageDeserialization { source: bincode::Error },

    #[snafu(display("Failed to serialize coordinator message"))]
    CoordinatorMessageSerialization { source: bincode::Error },

    #[snafu(display("Failed to send worker message through channel"))]
    UnableToSendWorkerMessage {
        source: mpsc::error::SendError<WorkerMessage>,
    },

    #[snafu(display("Failed to receive worker message through channel"))]
    UnableToReceiveWorkerMessage,

    #[snafu(display("Failed to send coordinator message through channel"))]
    UnableToSendCoordinatorMessage {
        source: mpsc::error::SendError<CoordinatorMessage>,
    },

    #[snafu(display("Failed to receive coordinator message through channel"))]
    UnableToReceiveCoordinatorMessage,

    #[snafu(display("Failed to send worker response(job report) through channel"))]
    UnableToSendJobReport,

    #[snafu(display("Failed to send request kind"))]
    UnableToSendRequestKind {
        source: mpsc::error::SendError<(RequestId, RequestKind)>,
    },

    #[snafu(display("Failed to send job"))]
    UnableToSendJob {
        source: mpsc::error::SendError<CoordinatorMessage>,
    },

    #[snafu(display("Failed to send stdin packet"))]
    UnableToSendStdinPacket {
        source: mpsc::error::SendError<CoordinatorMessage>,
    },

    #[snafu(display("Failed to send container respones"))]
    UnableToSendContainerResponse {
        source: mpsc::error::SendError<ContainerMessage>,
    },

    #[snafu(display("Failed to send stdout packet"))]
    UnableToSendStdoutPacket {
        source: mpsc::error::SendError<ContainerMessage>,
    },

    #[snafu(display("Failed to send stderr packet"))]
    UnableToSendStderrPacket {
        source: mpsc::error::SendError<ContainerMessage>,
    },

    #[snafu(display("PlaygroundMessage receiver ended unexpectedly"))]
    PlaygroundMessageReceiverEnded,

    #[snafu(display("WorkerMessage receiver ended unexpectedly"))]
    WorkerMessageReceiverEnded,

    #[snafu(display("RequestKind receiver ended unexpectedly"))]
    RequestKindReceiverEnded,

    #[snafu(display("Worker's project directory path is not valid UTF-8"))]
    WorkerProjectDirNotUTF8,
}

impl TryFrom<HighLevelRequest> for Job {
    type Error = Error;

    fn try_from(value: HighLevelRequest) -> Result<Self, Self::Error> {
        match value {
            HighLevelRequest::Compile(req) => {
                use crate::message::*;
                use crate::sandbox::CompileTarget::*;
                use crate::sandbox::*;

                let mut batch = Vec::new();
                batch.push(Request::WriteFile(WriteFileRequest {
                    path: "src/main.rs".to_owned(),
                    content: req.code.into(),
                }));
                let edition = match req.edition {
                    Some(Edition::Rust2021) => "2021",
                    Some(Edition::Rust2018) => "2018",
                    Some(Edition::Rust2015) => "2015",
                    None => "2021",
                };
                batch.push(Request::WriteFile(WriteFileRequest {
                    path: "Cargo.toml".to_owned(),
                    content: format!(
                        r#"[package]
                                name = "play"
                                version = "0.1.0"
                                edition = "{edition}"
                                "#
                    )
                    .into(),
                }));
                let mut args = if let Wasm = req.target {
                    vec!["wasm", "build"]
                } else {
                    vec!["rustc"]
                };
                if let Mode::Release = req.mode {
                    args.push("--release");
                }
                let output_path: &str = "compilation";
                match req.target {
                    Assembly(flavor, _, _) => {
                        use crate::sandbox::AssemblyFlavor::*;

                        // TODO: No compile-time string formatting.
                        args.extend(&["--", "--emit", "asm=compilation"]);

                        // Enable extra assembly comments for nightly builds
                        if let Channel::Nightly = req.channel {
                            args.push("-Z");
                            args.push("asm-comments");
                        }

                        args.push("-C");
                        match flavor {
                            Att => args.push("llvm-args=-x86-asm-syntax=att"),
                            Intel => args.push("llvm-args=-x86-asm-syntax=intel"),
                        }
                    }
                    LlvmIr => args.extend(&["--", "--emit", "llvm-ir=compilation"]),
                    Mir => args.extend(&["--", "--emit", "mir=compilation"]),
                    Hir => args.extend(&["--", "-Zunpretty=hir", "-o", output_path]),
                    Wasm => args.extend(&["-o", output_path]),
                }
                let mut envs = HashMap::new();
                if req.backtrace {
                    envs.insert("RUST_BACKTRACE".to_owned(), "1".to_owned());
                }
                batch.push(Request::ExecuteCommand(ExecuteCommandRequest {
                    cmd: "cargo".to_owned(),
                    args: args.into_iter().map(|s| s.to_owned()).collect(),
                    envs,
                    cwd: None,
                }));
                batch.push(Request::ReadFile(ReadFileRequest {
                    path: output_path.to_owned(),
                }));
                Ok(Job { reqs: batch })
            }
        }
    }
}

#[derive(Debug)]
pub enum RequestKind {
    Compile,
}

impl RequestKind {
    fn kind(req: &HighLevelRequest) -> Self {
        match req {
            HighLevelRequest::Compile(_) => RequestKind::Compile,
        }
    }
}

impl From<(JobReport, RequestKind)> for HighLevelResponse {
    fn from(value: (JobReport, RequestKind)) -> Self {
        use crate::message::*;

        let (JobReport { resps }, kind) = value;
        let responses = resps;
        match kind {
            RequestKind::Compile => {
                let mut success = false;
                let mut code = String::new();
                if let Some(ResponseResult(Ok(Response::ReadFile(ReadFileResponse(content))))) =
                    responses.last()
                {
                    match from_utf8(content) {
                        Ok(content) => {
                            success = true;
                            code = content.to_owned();
                        }
                        Err(_) => {
                            success = false;
                            code = "Error: Compilation output is not valid UTF-8 string".to_owned();
                        }
                    }
                }
                HighLevelResponse::Compile(CompileResponse {
                    success,
                    code,
                    stdout: "".to_owned(),
                    stderr: "".to_owned(),
                })
            }
        }
    }
}

macro_rules! docker_command {
    ($($arg:expr),* $(,)?) => ({
        let mut cmd = Command::new("docker");
        $( cmd.arg($arg); )*
        cmd
    });
}

fn basic_secure_docker_command() -> Command {
    docker_command!(
        "run",
        "--platform",
        "linux/amd64",
        "--cap-drop=ALL",
        "--net",
        "none",
        "--memory",
        "512m",
        "--memory-swap",
        "640m",
        "--pids-limit",
        "512",
    )
}

fn run_worker_in_background(project_dir: &Path) -> Result<(Child, ChildStdin, ChildStdout)> {
    // For local development.
    const WORKER_FILEPATH: &str = "./target/debug/worker";

    let mut child = Command::new(WORKER_FILEPATH)
        // let mut child = basic_secure_docker_command()
        // .arg("-i")
        // .args(["-a", "stdin", "-a", "stdout", "-a", "stderr"])
        // .arg("adwinw/rust-playground-worker")
        .arg(project_dir.to_str().context(WorkerProjectDirNotUTF8Snafu)?)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context(SpawnWorkerSnafu)?;
    let stdin = child.stdin.take().context(WorkerStdinCaptureSnafu)?;
    let stdout = child.stdout.take().context(WorkerStdoutCaptureSnafu)?;
    Ok((child, stdin, stdout))
}

// Child stdin/out <--> messages.
fn spawn_io_queue(
    tasks: &mut JoinSet<Result<()>>,
    stdin: ChildStdin,
    stdout: ChildStdout,
) -> (
    mpsc::Sender<CoordinatorMessage>,
    mpsc::Receiver<WorkerMessage>,
) {
    use std::io::{prelude::*, BufReader, BufWriter};

    let (tx, worker_msg_rx) = mpsc::channel(8);
    tasks.spawn(async move {
        tokio::task::spawn_blocking(move || {
            let stdout = SyncIoBridge::new(stdout);
            let mut stdout = BufReader::new(stdout);

            loop {
                let worker_msg = bincode::deserialize_from(&mut stdout)
                    .context(WorkerMessageDeserializationSnafu)?;

                tx.blocking_send(worker_msg)
                    .context(UnableToSendWorkerMessageSnafu)?;
            }
        })
        .await
        .unwrap(/* Panic occurred; re-raising */)
    });

    let (coordinator_msg_tx, mut rx) = mpsc::channel(8);
    tasks.spawn(async move {
        tokio::task::spawn_blocking(move || {
            let stdin = SyncIoBridge::new(stdin);
            let mut stdin = BufWriter::new(stdin);

            loop {
                let coordinator_msg = rx
                    .blocking_recv()
                    .context(UnableToReceiveCoordinatorMessageSnafu)?;

                bincode::serialize_into(&mut stdin, &coordinator_msg)
                    .context(CoordinatorMessageSerializationSnafu)?;

                stdin.flush().context(WorkerStdinFlushSnafu)?;
            }
        })
        .await
        .unwrap(/* Panic occurred; re-raising */)
    });
    (coordinator_msg_tx, worker_msg_rx)
}

pub fn spawn_container(project_dir: &Path) -> Result<Container> {
    let mut tasks = JoinSet::new();
    let (child, stdin, stdout) = run_worker_in_background(project_dir)?;
    let (coordinator_msg_tx, worker_msg_rx) = spawn_io_queue(&mut tasks, stdin, stdout);
    let (playground_msg_tx, playground_msg_rx) = mpsc::channel(8);
    let (container_msg_tx, container_msg_rx) = mpsc::channel(8);
    let (kind_tx, kind_rx) = mpsc::channel(8);
    tasks.spawn(async move {
        lower_operations(playground_msg_rx, coordinator_msg_tx, kind_tx).await?;
        Ok(())
    });
    tasks.spawn(async move {
        lift_operation_results(worker_msg_rx, container_msg_tx, kind_rx).await?;
        Ok(())
    });
    tokio::spawn(async move {
        if let Some(task) = tasks.join_next().await {
            eprintln!("{task:?}");

            tasks.abort_all();
            while let Some(task) = tasks.join_next().await {
                eprintln!("{task:?}");
            }
        }
    });
    Ok((child, playground_msg_tx, container_msg_rx))
}

async fn lower_operations(
    mut playground_msg_rx: mpsc::Receiver<PlaygroundMessage>,
    coordinator_msg_tx: mpsc::Sender<CoordinatorMessage>,
    kind_tx: mpsc::Sender<(RequestId, RequestKind)>,
) -> Result<()> {
    loop {
        let playground_msg = playground_msg_rx
            .recv()
            .await
            .context(PlaygroundMessageReceiverEndedSnafu)?;
        match playground_msg {
            PlaygroundMessage::Request(id, req) => {
                let kind = RequestKind::kind(&req);
                kind_tx
                    .send((id, kind))
                    .await
                    .context(UnableToSendRequestKindSnafu)?;
                let job = req.try_into()?;
                let coordinator_msg = CoordinatorMessage::Request(id, job);
                coordinator_msg_tx
                    .send(coordinator_msg)
                    .await
                    .context(UnableToSendJobSnafu)?;
            }
            PlaygroundMessage::StdinPacket(cmd_id, data) => {
                coordinator_msg_tx
                    .send(CoordinatorMessage::StdinPacket(cmd_id, data))
                    .await
                    .context(UnableToSendStdinPacketSnafu)?;
            }
        }
    }
}

async fn lift_operation_results(
    mut worker_msg_rx: mpsc::Receiver<WorkerMessage>,
    container_msg_tx: mpsc::Sender<ContainerMessage>,
    mut kind_rx: mpsc::Receiver<(RequestId, RequestKind)>,
) -> Result<()> {
    let mut request_kinds = HashMap::new();
    loop {
        select! {
            worker_msg = worker_msg_rx.recv() => {
                let worker_msg = worker_msg.context(WorkerMessageReceiverEndedSnafu)?;
                match worker_msg {
                    WorkerMessage::Response(id, job_report) => {
                        if let Some(kind) = request_kinds.remove(&id) {
                            let response = (job_report, kind).into();
                            let container_msg = ContainerMessage::Response(id, response);
                            container_msg_tx.send(container_msg).await.context(UnableToSendContainerResponseSnafu)?;
                        }
                    }
                    WorkerMessage::StdoutPacket(cmd_id, data) => {
                        container_msg_tx.send(ContainerMessage::StdoutPacket(cmd_id, data)).await.context(UnableToSendStdoutPacketSnafu)?;
                    }
                    WorkerMessage::StderrPacket(cmd_id, data) => {
                        container_msg_tx.send(ContainerMessage::StderrPacket(cmd_id, data)).await.context(UnableToSendStderrPacketSnafu)?;
                    }
                }
            }
            kind_msg = kind_rx.recv() => {
                let (id, kind) = kind_msg.context(RequestKindReceiverEndedSnafu)?;
                request_kinds.insert(id, kind);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use tempdir::TempDir;

    use crate::{
        coordinator::{ContainerMessage, Coordinator, HighLevelResponse, PlaygroundMessage},
        sandbox::{Channel, CompileRequest, CompileTarget, CrateType, Edition, Mode},
    };

    fn new_compile_request() -> CompileRequest {
        CompileRequest {
            target: CompileTarget::Mir,
            channel: Channel::Stable,
            crate_type: CrateType::Binary,
            mode: Mode::Release,
            edition: Some(Edition::Rust2021),
            tests: false,
            backtrace: false,
            code: r#"fn main() { println!("Hello World!"); }"#.to_owned(),
        }
    }

    #[tokio::test]
    #[snafu::report]
    async fn test_compile_response() -> super::Result<()> {
        let time_in_nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap() // now can't be earlier than UNIX_EPOCH.
            .as_nanos();
        let project_dir = TempDir::new(&format!("playground-{time_in_nanos}"))
            .expect("Failed to create temporary project directory");
        let mut coordinator = Coordinator::new(project_dir.path());

        let (_child, playground_msg_tx, mut container_msg_rx) = coordinator.allocate()?;
        playground_msg_tx
            .send(PlaygroundMessage::Request(
                0,
                super::HighLevelRequest::Compile(new_compile_request()),
            ))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_millis(5000), async move {
            loop {
                if let Some(container_msg) = container_msg_rx.recv().await {
                    match container_msg {
                        ContainerMessage::Response(id, resp) => {
                            if id == 0 {
                                match resp {
                                    HighLevelResponse::Compile(resp) => {
                                        println!("{}", resp.code);
                                        assert!(resp.success);
                                        break;
                                    }
                                }
                            }
                        }
                        ContainerMessage::StdoutPacket(_, packet) => {
                            println!("Stdout: {packet}");
                        }
                        ContainerMessage::StderrPacket(_, packet) => {
                            println!("Stderr: {packet}");
                        }
                    }
                } else {
                    panic!("Container message receiver ended unexpectedly");
                }
            }
        })
        .await
        .expect("Failed to receive response from container in time");
        Ok(())
    }

    #[tokio::test]
    #[snafu::report]
    async fn test_compile_streaming() -> super::Result<()> {
        let time_in_nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap() // now can't be earlier than UNIX_EPOCH.
            .as_nanos();
        let project_dir = TempDir::new(&format!("playground-{time_in_nanos}"))
            .expect("Failed to create temporary project directory");
        let mut coordinator = Coordinator::new(project_dir.path());

        let (_child, playground_msg_tx, mut container_msg_rx) = coordinator.allocate()?;
        playground_msg_tx
            .send(PlaygroundMessage::Request(
                0,
                super::HighLevelRequest::Compile(new_compile_request()),
            ))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_millis(5000), async move {
            let mut stderr = String::new();
            loop {
                if let Some(container_msg) = container_msg_rx.recv().await {
                    match container_msg {
                        ContainerMessage::Response(_id, resp) => match resp {
                            HighLevelResponse::Compile(resp) => {
                                println!("{}", resp.code);
                            }
                        },
                        ContainerMessage::StdoutPacket(_, packet) => {
                            println!("Stdout: {packet}");
                        }
                        ContainerMessage::StderrPacket(_, packet) => {
                            println!("Stderr: {packet}");
                            stderr.push_str(&packet);
                            if stderr.contains("Compiling") && stderr.contains("Finished") {
                                // Correct output.
                                break;
                            }
                        }
                    }
                } else {
                    panic!("Container message receiver ended unexpectedly");
                }
            }
        })
        .await
        .expect("Failed to receive streaming from container in time");
        Ok(())
    }
}
