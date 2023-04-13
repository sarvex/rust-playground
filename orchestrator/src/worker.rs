use snafu::prelude::*;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    select,
    sync::mpsc,
    task::JoinSet,
};

use crate::{
    message::{
        CoordinatorMessage, ExecuteCommandRequest, ExecuteCommandResponse, JobId, Multiplexed,
        ReadFileRequest, ReadFileResponse, SerializedError, WorkerMessage, WriteFileRequest,
        WriteFileResponse,
    },
    DropErrorDetailsExt, JoinSetExt,
};

type CommandRequest = (Multiplexed<ExecuteCommandRequest>, MultiplexingSender);

pub async fn listen(project_dir: PathBuf) -> Result<(), Error> {
    let (coordinator_msg_tx, coordinator_msg_rx) = mpsc::channel(8);
    let (worker_msg_tx, worker_msg_rx) = mpsc::channel(8);
    let mut io_tasks = spawn_io_queue(coordinator_msg_tx, worker_msg_rx);

    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let (stdin_tx, stdin_rx) = mpsc::channel(8);
    let process_task = tokio::spawn(manage_processes(stdin_rx, cmd_rx, project_dir.clone()));

    let handler_task = tokio::spawn(handle_coordinator_message(
        coordinator_msg_rx,
        worker_msg_tx,
        project_dir,
        cmd_tx,
        stdin_tx,
    ));

    select! {
        Some(io_task) = io_tasks.join_next() => {
            io_task.context(IoTaskExitedSnafu)?.context(IoTaskFailedSnafu)?;
        }

        process_task = process_task => {
            process_task.context(ProcessTaskExitedSnafu)?.context(ProcessTaskFailedSnafu)?
        }

        handler_task = handler_task => {
            handler_task.context(HandlerTaskExitedSnafu)?.context(HandlerTaskFailedSnafu)?
        }
    }

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    IoTaskExited {
        source: tokio::task::JoinError,
    },

    IoTaskFailed {
        source: IoQueueError,
    },

    ProcessTaskExited {
        source: tokio::task::JoinError,
    },

    ProcessTaskFailed {
        source: ProcessError,
    },

    HandlerTaskExited {
        source: tokio::task::JoinError,
    },

    HandlerTaskFailed {
        source: HandleCoordinatorMessageError,
    },
}

async fn handle_coordinator_message(
    mut coordinator_msg_rx: mpsc::Receiver<Multiplexed<CoordinatorMessage>>,
    worker_msg_tx: mpsc::Sender<Multiplexed<WorkerMessage>>,
    project_dir: PathBuf,
    cmd_tx: mpsc::Sender<CommandRequest>,
    stdin_tx: mpsc::Sender<Multiplexed<String>>,
) -> Result<(), HandleCoordinatorMessageError> {
    use handle_coordinator_message_error::*;

    // TODO: watch this
    let mut msg_tasks = JoinSet::new();

    loop {
        let Multiplexed(job_id, coordinator_msg) = coordinator_msg_rx
            .recv()
            .await
            .context(UnableToReceiveCoordinatorMessageSnafu)?;

        let worker_msg_tx = || MultiplexingSender {
            job_id,
            tx: worker_msg_tx.clone(),
        };

        match coordinator_msg {
            CoordinatorMessage::WriteFile(req) => {
                let project_dir = project_dir.clone();
                let worker_msg_tx = worker_msg_tx();

                msg_tasks.spawn(async move {
                    worker_msg_tx
                        .send(handle_write_file(req, project_dir).await)
                        .await
                        .context(UnableToSendWriteFileResponseSnafu)
                });
            }

            CoordinatorMessage::ReadFile(req) => {
                let project_dir = project_dir.clone();
                let worker_msg_tx = worker_msg_tx();

                msg_tasks.spawn(async move {
                    worker_msg_tx
                        .send(handle_read_file(req, project_dir).await)
                        .await
                        .context(UnableToSendReadFileResponseSnafu)
                });
            }

            CoordinatorMessage::ExecuteCommand(req) => {
                cmd_tx
                    .send((Multiplexed(job_id, req), worker_msg_tx()))
                    .await
                    .drop_error_details()
                    .context(UnableToSendCommandExecutionRequestSnafu)?;
            }

            CoordinatorMessage::StdinPacket(data) => {
                stdin_tx
                    .send(Multiplexed(job_id, data))
                    .await
                    .drop_error_details()
                    .context(UnableToSendStdinPacketSnafu)?;
            }
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum HandleCoordinatorMessageError {
    #[snafu(display("Failed to receive coordinator message from deserialization task"))]
    UnableToReceiveCoordinatorMessage,

    UnableToSendWriteFileResponse {
        source: MultiplexingSenderError,
    },

    UnableToSendReadFileResponse {
        source: MultiplexingSenderError,
    },

    #[snafu(display("Failed to send command execution request"))]
    UnableToSendCommandExecutionRequest {
        source: mpsc::error::SendError<()>,
    },

    #[snafu(display("Failed to send stdin packet"))]
    UnableToSendStdinPacket {
        source: mpsc::error::SendError<()>,
    },
}

#[derive(Debug, Clone)]
struct MultiplexingSender {
    job_id: JobId,
    tx: mpsc::Sender<Multiplexed<WorkerMessage>>,
}

impl MultiplexingSender {
    async fn send(
        &self,
        message: Result<impl Into<WorkerMessage>, impl std::error::Error>,
    ) -> Result<(), MultiplexingSenderError> {
        match message {
            Ok(v) => self.send_ok(v).await,
            Err(e) => self.send_err(e).await,
        }
    }

    async fn send_ok(
        &self,
        message: impl Into<WorkerMessage>,
    ) -> Result<(), MultiplexingSenderError> {
        self.send_raw(message.into()).await
    }

    async fn send_err(&self, e: impl std::error::Error) -> Result<(), MultiplexingSenderError> {
        self.send_raw(WorkerMessage::Error(SerializedError::new(e)))
            .await
    }

    async fn send_raw(&self, message: WorkerMessage) -> Result<(), MultiplexingSenderError> {
        use multiplexing_sender_error::*;

        self.tx
            .send(Multiplexed(self.job_id, message))
            .await
            .drop_error_details()
            .context(UnableToSendWorkerMessageSnafu)
    }
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum MultiplexingSenderError {
    #[snafu(display("Failed to send worker message to serialization task"))]
    UnableToSendWorkerMessage { source: mpsc::error::SendError<()> },
}

async fn handle_write_file(
    req: WriteFileRequest,
    project_dir: PathBuf,
) -> Result<WriteFileResponse, WriteFileError> {
    use write_file_error::*;

    let path = parse_working_dir(Some(req.path), &project_dir);

    // Create intermediate directories.
    if let Some(parent_dir) = path.parent() {
        fs::create_dir_all(parent_dir)
            .await
            .context(UnableToCreateDirSnafu { parent_dir })?;
    }

    fs::write(&path, req.content)
        .await
        .context(UnableToWriteFileSnafu { path })?;

    Ok(WriteFileResponse(()))
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum WriteFileError {
    #[snafu(display("Failed to create parent directory {}", parent_dir.display()))]
    UnableToCreateDir {
        source: std::io::Error,
        parent_dir: PathBuf,
    },

    #[snafu(display("Failed to write file {}", path.display()))]
    UnableToWriteFile {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Failed to send worker message to serialization task"))]
    UnableToSendWorkerMessage { source: mpsc::error::SendError<()> },
}

async fn handle_read_file(
    req: ReadFileRequest,
    project_dir: PathBuf,
) -> Result<ReadFileResponse, ReadFileError> {
    use read_file_error::*;

    let path = parse_working_dir(Some(req.path), &project_dir);

    let content = fs::read(&path)
        .await
        .context(UnableToReadFileSnafu { path })?;

    Ok(ReadFileResponse(content))
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum ReadFileError {
    #[snafu(display("Failed to read file {}", path.display()))]
    UnableToReadFile {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Failed to send worker message to serialization task"))]
    UnableToSendWorkerMessage { source: mpsc::error::SendError<()> },
}

// Current working directory defaults to project dir unless specified otherwise.
fn parse_working_dir(cwd: Option<String>, project_path: &Path) -> PathBuf {
    let mut final_path = project_path.to_path_buf();
    if let Some(path) = cwd {
        // Absolute path will replace final_path.
        final_path.push(path)
    }
    final_path
}

async fn manage_processes(
    mut stdin_rx: mpsc::Receiver<Multiplexed<String>>,
    mut cmd_rx: mpsc::Receiver<CommandRequest>,
    project_path: PathBuf,
) -> Result<(), ProcessError> {
    use process_error::*;

    let mut processes = JoinSet::new();
    let mut stdin_senders = HashMap::new();
    let (stdin_shutdown_tx, mut stdin_shutdown_rx) = mpsc::channel(8);

    loop {
        select! {
            cmd_req = cmd_rx.recv() => {
                let (Multiplexed(job_id, req), worker_msg_tx) = cmd_req.context(CommandRequestReceiverEndedSnafu)?;

                let (child, stdin_rx, stdin, stdout, stderr) = match process_begin(req, &project_path, &mut stdin_senders, job_id) {
                    Ok(v) => v,
                    Err(e) => {
                        // TODO: add message for started vs current stopped
                        worker_msg_tx.send_err(e).await.context(UnableToSendExecuteCommandStartedResponseSnafu)?;
                        continue;
                    }
                };

                let task_set = stream_stdio(worker_msg_tx.clone(), stdin_rx, stdin, stdout, stderr);

                processes.spawn({
                    let stdin_shutdown_tx = stdin_shutdown_tx.clone();
                    async move {
                        worker_msg_tx
                            .send(process_end(child, task_set, stdin_shutdown_tx, job_id).await)
                            .await
                            .context(UnableToSendExecuteCommandResponseSnafu)
                    }
                });
            }

            stdin_packet = stdin_rx.recv() => {
                // Dispatch stdin packet to different child by attached command id.
                let Multiplexed(job_id, packet) = stdin_packet.context(StdinReceiverEndedSnafu)?;

                if let Some(stdin_tx) = stdin_senders.get(&job_id) {
                    stdin_tx.send(packet).await.drop_error_details().context(UnableToSendStdinDataSnafu)?;
                }
            }

            job_id = stdin_shutdown_rx.recv() => {
                let job_id = job_id.context(StdinShutdownReceiverEndedSnafu)?;
                stdin_senders.remove(&job_id);
            }

            Some(process) = processes.join_next() => {
                process.context(ProcessTaskPanickedSnafu)??;
            }
        }
    }
}

fn process_begin(
    req: ExecuteCommandRequest,
    project_path: &Path,
    stdin_senders: &mut HashMap<JobId, mpsc::Sender<String>>,
    job_id: JobId,
) -> Result<
    (
        Child,
        mpsc::Receiver<String>,
        ChildStdin,
        ChildStdout,
        ChildStderr,
    ),
    ProcessError,
> {
    use process_error::*;

    let ExecuteCommandRequest {
        cmd,
        args,
        envs,
        cwd,
    } = req;
    let mut child = Command::new(cmd)
        .args(args)
        .envs(envs)
        .current_dir(parse_working_dir(cwd, project_path))
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context(UnableToSpawnProcessSnafu)?;

    let stdin = child.stdin.take().context(UnableToCaptureStdinSnafu)?;
    let stdout = child.stdout.take().context(UnableToCaptureStdoutSnafu)?;
    let stderr = child.stderr.take().context(UnableToCaptureStderrSnafu)?;

    // Preparing for receiving stdin packet.
    let (stdin_tx, stdin_rx) = mpsc::channel(8);
    stdin_senders.insert(job_id, stdin_tx);

    Ok((child, stdin_rx, stdin, stdout, stderr))
}

async fn process_end(
    mut child: Child,
    mut task_set: JoinSet<Result<(), StdioError>>,
    stdin_shutdown_tx: mpsc::Sender<JobId>,
    job_id: JobId,
) -> Result<ExecuteCommandResponse, ProcessError> {
    use process_error::*;

    let status = child.wait().await.context(WaitChildSnafu)?;

    stdin_shutdown_tx
        .send(job_id)
        .await
        .drop_error_details()
        .context(UnableToSendStdinShutdownSnafu)?;

    while let Some(task) = task_set.join_next().await {
        task.context(StdioTaskPanickedSnafu)?
            .context(StdioTaskFailedSnafu)?;
    }

    let success = status.success();
    Ok(ExecuteCommandResponse { success })
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum ProcessError {
    #[snafu(display("Failed to spawn child process"))]
    UnableToSpawnProcess {
        source: std::io::Error,
    },

    #[snafu(display("Failed to capture child process stdin"))]
    UnableToCaptureStdin,

    #[snafu(display("Failed to capture child process stdout"))]
    UnableToCaptureStdout,

    #[snafu(display("Failed to capture child process stderr"))]
    UnableToCaptureStderr,

    #[snafu(display("Command request receiver ended unexpectedly"))]
    CommandRequestReceiverEnded,

    #[snafu(display("Stdin packet receiver ended unexpectedly"))]
    StdinReceiverEnded,

    #[snafu(display("Failed to send stdin data"))]
    UnableToSendStdinData {
        source: mpsc::error::SendError<()>,
    },

    #[snafu(display("Failed to wait for child process exiting"))]
    WaitChild {
        source: std::io::Error,
    },

    UnableToSendStdinShutdown {
        source: mpsc::error::SendError<()>,
    },

    StdioTaskPanicked {
        source: tokio::task::JoinError,
    },

    StdioTaskFailed {
        source: StdioError,
    },

    UnableToSendExecuteCommandStartedResponse {
        source: MultiplexingSenderError,
    },

    UnableToSendExecuteCommandResponse {
        source: MultiplexingSenderError,
    },

    StdinShutdownReceiverEnded,

    ProcessTaskPanicked {
        source: tokio::task::JoinError,
    },
}

fn stream_stdio(
    coordinator_tx: MultiplexingSender,
    mut stdin_rx: mpsc::Receiver<String>,
    mut stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
) -> JoinSet<Result<(), StdioError>> {
    use stdio_error::*;

    let mut set = JoinSet::new();

    set.spawn(async move {
        loop {
            let Some(data) = stdin_rx.recv().await else { break };
            stdin
                .write_all(data.as_bytes())
                .await
                .context(UnableToWriteStdinSnafu)?;
            stdin.flush().await.context(UnableToFlushStdinSnafu)?;
        }

        Ok(())
    });

    let coordinator_tx_out = coordinator_tx.clone();
    set.spawn(async move {
        let mut stdout_buf = BufReader::new(stdout);
        loop {
            // Must be valid UTF-8.
            let mut buffer = String::new();
            let n = stdout_buf
                .read_line(&mut buffer)
                .await
                .context(UnableToReadStdoutSnafu)?;
            if n == 0 {
                break;
            }

            coordinator_tx_out
                .send_ok(WorkerMessage::StdoutPacket(buffer))
                .await
                .context(UnableToSendStdoutPacketSnafu)?;
        }

        Ok(())
    });

    let coordinator_tx_err = coordinator_tx;
    set.spawn(async move {
        let mut stderr_buf = BufReader::new(stderr);
        loop {
            // Must be valid UTF-8.
            let mut buffer = String::new();
            let n = stderr_buf
                .read_line(&mut buffer)
                .await
                .context(UnableToReadStderrSnafu)?;
            if n == 0 {
                break;
            }

            coordinator_tx_err
                .send_ok(WorkerMessage::StderrPacket(buffer))
                .await
                .context(UnableToSendStderrPacketSnafu)?;
        }

        Ok(())
    });

    set
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum StdioError {
    #[snafu(display("Failed to write stdin data"))]
    UnableToWriteStdin { source: std::io::Error },

    #[snafu(display("Failed to flush stdin data"))]
    UnableToFlushStdin { source: std::io::Error },

    #[snafu(display("Failed to read child process stdout"))]
    UnableToReadStdout { source: std::io::Error },

    #[snafu(display("Failed to read child process stderr"))]
    UnableToReadStderr { source: std::io::Error },

    #[snafu(display("Failed to send stdout packet"))]
    UnableToSendStdoutPacket { source: MultiplexingSenderError },

    #[snafu(display("Failed to send stderr packet"))]
    UnableToSendStderrPacket { source: MultiplexingSenderError },
}

// stdin/out <--> messages.
fn spawn_io_queue(
    coordinator_msg_tx: mpsc::Sender<Multiplexed<CoordinatorMessage>>,
    mut worker_msg_rx: mpsc::Receiver<Multiplexed<WorkerMessage>>,
) -> JoinSet<Result<(), IoQueueError>> {
    use io_queue_error::*;
    use std::io::{prelude::*, BufReader, BufWriter};

    let mut tasks = JoinSet::new();

    tasks.spawn_blocking(move || {
        let stdin = std::io::stdin();
        let mut stdin = BufReader::new(stdin);

        loop {
            let coordinator_msg = bincode::deserialize_from(&mut stdin)
                .context(UnableToDeserializeCoordinatorMessageSnafu)?;

            coordinator_msg_tx
                .blocking_send(coordinator_msg)
                .drop_error_details()
                .context(UnableToSendCoordinatorMessageSnafu)?;
        }
    });

    tasks.spawn_blocking(move || {
        let stdout = std::io::stdout();
        let mut stdout = BufWriter::new(stdout);

        loop {
            let worker_msg = worker_msg_rx
                .blocking_recv()
                .context(UnableToReceiveWorkerMessageSnafu)?;

            bincode::serialize_into(&mut stdout, &worker_msg)
                .context(UnableToSerializeWorkerMessageSnafu)?;

            stdout.flush().context(UnableToFlushStdoutSnafu)?;
        }
    });

    tasks
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum IoQueueError {
    #[snafu(display("Failed to deserialize coordinator message"))]
    UnableToDeserializeCoordinatorMessage { source: bincode::Error },

    #[snafu(display("Failed to serialize worker message"))]
    UnableToSerializeWorkerMessage { source: bincode::Error },

    #[snafu(display("Failed to send coordinator message from deserialization task"))]
    UnableToSendCoordinatorMessage { source: mpsc::error::SendError<()> },

    #[snafu(display("Failed to receive worker message"))]
    UnableToReceiveWorkerMessage,

    #[snafu(display("Failed to flush stdout"))]
    UnableToFlushStdout { source: std::io::Error },
}
