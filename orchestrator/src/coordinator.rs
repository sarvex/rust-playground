use snafu::prelude::*;
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    join,
    process::{Child, ChildStdin, ChildStdout, Command},
    select,
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::io::SyncIoBridge;

use crate::{
    message::{
        CoordinatorMessage, JobId, Multiplexed, OneToOneResponse, ReadFileResponse, WorkerMessage,
        WriteFileRequest,
    },
    sandbox::{CompileRequest, CompileResponse, CompileResponse2},
    DropErrorDetailsExt,
};

#[derive(Debug)]
enum DemultiplexCommand {
    Listen(JobId, mpsc::Sender<WorkerMessage>),
    ListenOnce(JobId, oneshot::Sender<WorkerMessage>),
}

#[derive(Debug)]
pub struct Container {
    child: Child,
    commander: Commander,
}

impl Container {
    pub async fn compile(&self, request: CompileRequest) -> Result<CompileResponse> {
        let (result, stdout, stderr) = self.begin_compile(request).await?;

        let stdout = ReceiverStream::new(stdout).collect();
        let stderr = ReceiverStream::new(stderr).collect();

        let (result, stdout, stderr) = join!(result, stdout, stderr);

        let CompileResponse2 { success, code } = result.expect("handle me")?;
        Ok(CompileResponse {
            success,
            code,
            stdout,
            stderr,
        })
    }

    pub async fn begin_compile(
        &self,
        request: CompileRequest,
    ) -> Result<(
        JoinHandle<Result<CompileResponse2>>, // TODO: shouldn't include stdout / stderr
        mpsc::Receiver<String>,
        mpsc::Receiver<String>,
    )> {
        let write_main = WriteFileRequest {
            path: "src/main.rs".to_owned(),
            content: request.code.into(),
        };

        use crate::message::{ExecuteCommandRequest, ReadFileRequest};
        use crate::sandbox::{Channel, CompileTarget::*, Edition, Mode};

        let edition = match request.edition {
            Some(Edition::Rust2021) => "2021",
            Some(Edition::Rust2018) => "2018",
            Some(Edition::Rust2015) => "2015",
            None => "2021",
        };

        let write_cargo_toml = WriteFileRequest {
            path: "Cargo.toml".to_owned(),
            content: format!(
                r#"[package]
                   name = "play"
                   version = "0.1.0"
                   edition = "{edition}"
                   "#
            )
            .into(),
        };

        let mut args = if let Wasm = request.target {
            vec!["wasm", "build"]
        } else {
            vec!["rustc"]
        };
        if let Mode::Release = request.mode {
            args.push("--release");
        }
        let output_path: &str = "compilation";
        match request.target {
            Assembly(flavor, _, _) => {
                use crate::sandbox::AssemblyFlavor::*;

                // TODO: No compile-time string formatting.
                args.extend(&["--", "--emit", "asm=compilation"]);

                // Enable extra assembly comments for nightly builds
                if let Channel::Nightly = request.channel {
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
        if request.backtrace {
            envs.insert("RUST_BACKTRACE".to_owned(), "1".to_owned());
        }

        let execute_cargo = ExecuteCommandRequest {
            cmd: "cargo".to_owned(),
            args: args.into_iter().map(|s| s.to_owned()).collect(),
            envs,
            cwd: None,
        };

        let read_output = ReadFileRequest {
            path: output_path.to_owned(),
        };

        let a = self.commander.one(write_main);
        let b = self.commander.one(write_cargo_toml);

        let (a, b) = join!(a, b);

        // TODO: assert response success

        let (stdout_tx, stdout_rx) = mpsc::channel(8);
        let (stderr_tx, stderr_rx) = mpsc::channel(8);

        let mut from_worker_rx = self.commander.many(execute_cargo).await;

        let x = tokio::spawn({
            let commander = self.commander.clone();
            async move {
                let mut success = false;

                while let Some(container_msg) = from_worker_rx.recv().await {
                    match container_msg {
                        WorkerMessage::WriteFile(..) => todo!("nah"),
                        WorkerMessage::ReadFile(..) => todo!("nah"),
                        WorkerMessage::ExecuteCommand(..) => {
                            success = true;
                            break;
                        }
                        WorkerMessage::StdoutPacket(packet) => {
                            stdout_tx.send(packet).await.ok(/* Receiver gone, that's OK */);
                        }
                        WorkerMessage::StderrPacket(packet) => {
                            stderr_tx.send(packet).await.ok(/* Receiver gone, that's OK */);
                        }
                    }
                }

                let f: ReadFileResponse = commander.one(read_output).await;
                let code = f.0;
                let code = String::from_utf8(code).unwrap();

                // TODO: Stop listening

                Ok(CompileResponse2 { success, code })
            }
        });

        Ok((x, stdout_rx, stderr_rx))
    }
}

#[derive(Debug, Clone)]
struct Commander {
    to_worker_tx: mpsc::Sender<Multiplexed<CoordinatorMessage>>,
    command_tx: mpsc::Sender<DemultiplexCommand>,
    id: Arc<AtomicU64>,
}

impl Commander {
    async fn demultiplex(
        mut command_rx: mpsc::Receiver<DemultiplexCommand>,
        mut rx: mpsc::Receiver<Multiplexed<WorkerMessage>>,
    ) {
        let mut waiting = HashMap::new();
        let mut waiting_once = HashMap::new();

        loop {
            select! {
                command = command_rx.recv() => {
                    let command = command.expect("Handle this");
                    match command {
                        DemultiplexCommand::Listen(id, waiter) => {
                            waiting.insert(id, waiter);
                            // TODO: ensure not replacing
                        }

                        DemultiplexCommand::ListenOnce(id, waiter) => {
                            waiting_once.insert(id, waiter);
                            // TODO: ensure not replacing
                        }
                    }
                },

                msg = rx.recv() => {
                    let msg = msg.expect("Handle this");

                    let Multiplexed(id, data) = msg; // TODO: some uniform way of getting the ID for _any_ message

                    if let Some(waiter) = waiting_once.remove(&id) {
                        waiter.send(data).ok(/* Don't care about it */);
                        continue;
                    }

                    if let Some(waiter) = waiting.get(&id) {
                        waiter.send(data).await.ok(/* Don't care about it */);
                        continue;
                    }

                    // TODO: log unattended messages?
                }
            }
        }
    }

    fn next_id(&self) -> JobId {
        self.id.fetch_add(1, Ordering::SeqCst)
    }

    async fn one<M>(&self, message: M) -> M::Response
    where
        M: Into<CoordinatorMessage>,
        M: OneToOneResponse,
        M::Response: TryFrom<WorkerMessage>,
    {
        let id = self.next_id();

        let (from_worker_tx, from_worker_rx) = oneshot::channel();

        self.command_tx
            .send(DemultiplexCommand::ListenOnce(id, from_worker_tx))
            .await
            .unwrap();

        self.to_worker_tx
            .send(Multiplexed(id, message.into()))
            .await
            .unwrap();

        let msg = from_worker_rx.await.unwrap();
        msg.try_into().ok().unwrap()
    }

    async fn many<M>(&self, message: M) -> mpsc::Receiver<WorkerMessage>
    where
        M: Into<CoordinatorMessage>,
    {
        let id = self.next_id();

        let (from_worker_tx, from_worker_rx) = mpsc::channel(8);

        self.command_tx
            .send(DemultiplexCommand::Listen(id, from_worker_tx))
            .await
            .unwrap();

        self.to_worker_tx
            .send(Multiplexed(id, message.into()))
            .await
            .unwrap();

        from_worker_rx
    }
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
    UnableToSendWorkerMessage { source: mpsc::error::SendError<()> },

    #[snafu(display("Failed to receive worker message through channel"))]
    UnableToReceiveWorkerMessage,

    #[snafu(display("Failed to receive coordinator message through channel"))]
    UnableToReceiveCoordinatorMessage,

    #[snafu(display("Failed to send worker response(job report) through channel"))]
    UnableToSendJobReport,

    #[snafu(display("PlaygroundMessage receiver ended unexpectedly"))]
    PlaygroundMessageReceiverEnded,

    #[snafu(display("WorkerMessage receiver ended unexpectedly"))]
    WorkerMessageReceiverEnded,
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
        .arg(project_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
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
    mpsc::Sender<Multiplexed<CoordinatorMessage>>,
    mpsc::Receiver<Multiplexed<WorkerMessage>>,
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
                    .drop_error_details()
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
    let (to_worker_tx, from_worker_rx) = spawn_io_queue(&mut tasks, stdin, stdout);
    tokio::spawn(async move {
        if let Some(task) = tasks.join_next().await {
            eprintln!("{task:?}");

            tasks.abort_all();
            while let Some(task) = tasks.join_next().await {
                eprintln!("{task:?}");
            }
        }
    });

    let (command_tx, command_rx) = mpsc::channel(8);
    tokio::spawn(Commander::demultiplex(command_rx, from_worker_rx));

    let commander = Commander {
        to_worker_tx,
        command_tx,
        id: Default::default(),
    };

    Ok(Container { child, commander })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tempdir::TempDir;
    use tokio::join;
    use tokio_stream::{wrappers::ReceiverStream, StreamExt};

    use crate::{
        coordinator::Coordinator,
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
        let project_dir =
            TempDir::new("playground").expect("Failed to create temporary project directory");
        let mut coordinator = Coordinator::new(project_dir.path());

        let container = coordinator.allocate()?;
        let response = tokio::time::timeout(
            Duration::from_millis(5000),
            container.compile(new_compile_request()),
        )
        .await
        .expect("Failed to receive streaming from container in time")?;

        assert!(response.success);

        Ok(())
    }

    #[tokio::test]
    #[snafu::report]
    async fn test_compile_streaming() -> super::Result<()> {
        let project_dir =
            TempDir::new("playground").expect("Failed to create temporary project directory");
        let mut coordinator = Coordinator::new(project_dir.path());

        let container = coordinator.allocate()?;
        let (complete, stdout, stderr) = container.begin_compile(new_compile_request()).await?;

        let stdout = ReceiverStream::new(stdout);
        let stdout = stdout.collect::<String>();

        let stderr = ReceiverStream::new(stderr);
        let stderr = stderr.collect::<String>();

        let (_complete, _stdout, stderr) =
            tokio::time::timeout(Duration::from_millis(5000), async move {
                join!(complete, stdout, stderr)
            })
            .await
            .expect("Failed to receive streaming from container in time");

        assert!(stderr.contains("Compiling"));
        assert!(stderr.contains("Finished"));

        Ok(())
    }
}
