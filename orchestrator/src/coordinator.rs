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
        CoordinatorMessage, JobId, Multiplexed, OneToOneResponse, ReadFileRequest,
        ReadFileResponse, WorkerMessage,
    },
    sandbox::{CompileRequest, CompileResponse, CompileResponse2},
    DropErrorDetailsExt, JoinSetExt,
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
    pub async fn compile(&self, request: CompileRequest) -> Result<CompileResponse, CompileError> {
        use compile_error::*;

        let ActiveCompilation {
            task,
            stdout_rx,
            stderr_rx,
        } = self.begin_compile(request).await?;

        let stdout = ReceiverStream::new(stdout_rx).collect();
        let stderr = ReceiverStream::new(stderr_rx).collect();

        let (result, stdout, stderr) = join!(task, stdout, stderr);

        let CompileResponse2 { success, code } = result.context(CompilationTaskPanickedSnafu)??;
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
    ) -> Result<ActiveCompilation, CompileError> {
        use compile_error::*;

        let output_path: &str = "compilation";

        let write_main = request.write_main_request();
        let write_cargo_toml = request.write_cargo_toml_request();
        let execute_cargo = request.execute_cargo_request(output_path);
        let read_output = ReadFileRequest {
            path: output_path.to_owned(),
        };

        let write_main = self.commander.one(write_main);
        let write_cargo_toml = self.commander.one(write_cargo_toml);

        let (write_main, write_cargo_toml) = join!(write_main, write_cargo_toml);

        write_main.context(CouldNotWriteCodeSnafu)?;
        write_cargo_toml.context(CouldNotWriteCargoTomlSnafu)?;

        // TODO: assert response success

        let (stdout_tx, stdout_rx) = mpsc::channel(8);
        let (stderr_tx, stderr_rx) = mpsc::channel(8);

        let mut from_worker_rx = self
            .commander
            .many(execute_cargo)
            .await
            .context(CouldNotStartCompilerSnafu)?;

        let task = tokio::spawn({
            let commander = self.commander.clone();
            async move {
                let mut success = false;

                while let Some(container_msg) = from_worker_rx.recv().await {
                    match container_msg {
                        WorkerMessage::ExecuteCommand(..) => {
                            // TODO: success should from the command response.
                            success = true;
                            break;
                        }
                        WorkerMessage::StdoutPacket(packet) => {
                            stdout_tx.send(packet).await.ok(/* Receiver gone, that's OK */);
                        }
                        WorkerMessage::StderrPacket(packet) => {
                            stderr_tx.send(packet).await.ok(/* Receiver gone, that's OK */);
                        }
                        _ => return UnexpectedMessageSnafu.fail(),
                    }
                }

                let file: ReadFileResponse = commander
                    .one(read_output)
                    .await
                    .context(CouldNotReadCodeSnafu)?;
                let code = String::from_utf8(file.0).context(CodeNotUtf8Snafu)?;

                // TODO: Stop listening

                Ok(CompileResponse2 { success, code })
            }
        });

        Ok(ActiveCompilation {
            task,
            stdout_rx,
            stderr_rx,
        })
    }
}

#[derive(Debug)]
pub struct ActiveCompilation {
    pub task: JoinHandle<Result<CompileResponse2, CompileError>>,
    pub stdout_rx: mpsc::Receiver<String>,
    pub stderr_rx: mpsc::Receiver<String>,
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum CompileError {
    #[snafu(display("The compilation task panicked"))]
    CompilationTaskPanicked { source: tokio::task::JoinError },

    #[snafu(display("Could not write Cargo.toml"))]
    CouldNotWriteCargoToml { source: CommanderError },

    #[snafu(display("Could not write source code"))]
    CouldNotWriteCode { source: CommanderError },

    #[snafu(display("Could not start compiler"))]
    CouldNotStartCompiler { source: CommanderError },

    #[snafu(display("Received an unexpected message"))]
    UnexpectedMessage,

    #[snafu(display("Could not read the compilation output"))]
    CouldNotReadCode { source: CommanderError },

    #[snafu(display("The compilation output was not UTF-8"))]
    CodeNotUtf8 { source: std::string::FromUtf8Error },
}

#[derive(Debug, Clone)]
struct Commander {
    to_worker_tx: mpsc::Sender<Multiplexed<CoordinatorMessage>>,
    to_demultiplexer_tx: mpsc::Sender<DemultiplexCommand>,
    id: Arc<AtomicU64>,
}

impl Commander {
    async fn demultiplex(
        mut command_rx: mpsc::Receiver<DemultiplexCommand>,
        mut from_worker_rx: mpsc::Receiver<Multiplexed<WorkerMessage>>,
    ) -> Result<(), CommanderError> {
        use commander_error::*;

        let mut waiting = HashMap::new();
        let mut waiting_once = HashMap::new();

        loop {
            select! {
                command = command_rx.recv() => {
                    let Some(command) = command else { return Ok(()) };

                    match command {
                        DemultiplexCommand::Listen(job_id, waiter) => {
                            let old = waiting.insert(job_id, waiter);
                            ensure!(old.is_none(), DuplicateDemultiplexerClientSnafu { job_id });
                        }

                        DemultiplexCommand::ListenOnce(job_id, waiter) => {
                            let old = waiting_once.insert(job_id, waiter);
                            ensure!(old.is_none(), DuplicateDemultiplexerClientSnafu { job_id });
                        }
                    }
                },

                msg = from_worker_rx.recv() => {
                    let Multiplexed(job_id, msg) = msg.context(UnableToReceiveFromWorkerSnafu)?;

                    if let Some(waiter) = waiting_once.remove(&job_id) {
                        waiter.send(msg).ok(/* Don't care about it */);
                        continue;
                    }

                    if let Some(waiter) = waiting.get(&job_id) {
                        waiter.send(msg).await.ok(/* Don't care about it */);
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

    async fn send_to_demultiplexer(
        &self,
        command: DemultiplexCommand,
    ) -> Result<(), CommanderError> {
        use commander_error::*;

        self.to_demultiplexer_tx
            .send(command)
            .await
            .drop_error_details()
            .context(UnableToSendToDemultiplexerSnafu)
    }

    async fn send_to_worker(
        &self,
        message: Multiplexed<CoordinatorMessage>,
    ) -> Result<(), CommanderError> {
        use commander_error::*;

        self.to_worker_tx
            .send(message)
            .await
            .drop_error_details()
            .context(UnableToSendToWorkerSnafu)
    }

    async fn one<M>(&self, message: M) -> Result<M::Response, CommanderError>
    where
        M: Into<CoordinatorMessage>,
        M: OneToOneResponse,
        M::Response: TryFrom<WorkerMessage>,
    {
        use commander_error::*;

        let id = self.next_id();
        let (from_demultiplexer_tx, from_demultiplexer_rx) = oneshot::channel();

        self.send_to_demultiplexer(DemultiplexCommand::ListenOnce(id, from_demultiplexer_tx))
            .await?;
        self.send_to_worker(Multiplexed(id, message.into())).await?;
        let msg = from_demultiplexer_rx
            .await
            .context(UnableToReceiveFromDemultiplexerSnafu)?;

        msg.try_into().ok().context(UnexpectedResponseTypeSnafu)
    }

    async fn many<M>(&self, message: M) -> Result<mpsc::Receiver<WorkerMessage>, CommanderError>
    where
        M: Into<CoordinatorMessage>,
    {
        let id = self.next_id();
        let (from_worker_tx, from_worker_rx) = mpsc::channel(8);

        self.send_to_demultiplexer(DemultiplexCommand::Listen(id, from_worker_tx))
            .await?;
        self.send_to_worker(Multiplexed(id, message.into())).await?;

        Ok(from_worker_rx)
    }
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum CommanderError {
    #[snafu(display("Two listeners subscribed to job {job_id}"))]
    DuplicateDemultiplexerClient { job_id: JobId },

    #[snafu(display("Could not send a message to the demultiplexer"))]
    UnableToSendToDemultiplexer { source: mpsc::error::SendError<()> },

    #[snafu(display("Did not receive a response from the demultiplexer"))]
    UnableToReceiveFromDemultiplexer { source: oneshot::error::RecvError },

    #[snafu(display("Could not send a message to the worker"))]
    UnableToSendToWorker { source: mpsc::error::SendError<()> },

    #[snafu(display("Did not receive a response from the worker"))]
    UnableToReceiveFromWorker,

    #[snafu(display("Did not receive the expected response type from the worker"))]
    UnexpectedResponseType,
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

    #[snafu(display("Failed to receive coordinator message through channel"))]
    UnableToReceiveCoordinatorMessage,
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
    tasks.spawn_blocking(move || {
        let stdout = SyncIoBridge::new(stdout);
        let mut stdout = BufReader::new(stdout);

        loop {
            let worker_msg = bincode::deserialize_from(&mut stdout)
                .context(WorkerMessageDeserializationSnafu)?;

            tx.blocking_send(worker_msg)
                .drop_error_details()
                .context(UnableToSendWorkerMessageSnafu)?;
        }
    });

    let (coordinator_msg_tx, mut rx) = mpsc::channel(8);
    tasks.spawn_blocking(move || {
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
        to_demultiplexer_tx: command_tx,
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

    use super::*;

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
    async fn test_compile_response() -> Result<()> {
        let project_dir =
            TempDir::new("playground").expect("Failed to create temporary project directory");
        let mut coordinator = Coordinator::new(project_dir.path());

        let container = coordinator.allocate()?;
        let response = tokio::time::timeout(
            Duration::from_millis(5000),
            container.compile(new_compile_request()),
        )
        .await
        .expect("Failed to receive streaming from container in time")
        .unwrap();

        assert!(response.success);

        Ok(())
    }

    #[tokio::test]
    #[snafu::report]
    async fn test_compile_streaming() -> Result<()> {
        let project_dir =
            TempDir::new("playground").expect("Failed to create temporary project directory");
        let mut coordinator = Coordinator::new(project_dir.path());

        let container = coordinator.allocate()?;
        let ActiveCompilation {
            task,
            stdout_rx,
            stderr_rx,
        } = container
            .begin_compile(new_compile_request())
            .await
            .unwrap();

        let stdout = ReceiverStream::new(stdout_rx);
        let stdout = stdout.collect::<String>();

        let stderr = ReceiverStream::new(stderr_rx);
        let stderr = stderr.collect::<String>();

        let (_complete, _stdout, stderr) =
            tokio::time::timeout(Duration::from_millis(5000), async move {
                join!(task, stdout, stderr)
            })
            .await
            .expect("Failed to receive streaming from container in time");

        assert!(stderr.contains("Compiling"));
        assert!(stderr.contains("Finished"));

        Ok(())
    }
}
