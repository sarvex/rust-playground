use snafu::prelude::*;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    select,
    sync::mpsc,
    task::JoinSet,
};

use crate::{
    message::{
        CoordinatorMessage, ExecuteCommandRequest, ExecuteCommandResponse, JobId, Multiplexed,
        ReadFileRequest, ReadFileResponse, WorkerMessage, WriteFileRequest, WriteFileResponse,
    },
    DropErrorDetailsExt, JoinSetExt,
};

type CommandRequest = (Multiplexed<ExecuteCommandRequest>, MultiplexingSender);

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    UnableToWriteFile {
        source: WriteFileError,
    },

    UnableToReadFile {
        source: ReadFileError,
    },

    #[snafu(display("Failed to send command execution request"))]
    UnableToSendCommandExecutionRequest {
        source: mpsc::error::SendError<()>,
    },

    #[snafu(display("Failed to send stdin packet"))]
    UnableToSendStdinPacket {
        source: mpsc::error::SendError<()>,
    },

    #[snafu(display("Failed to receive coordinator message from deserialization task"))]
    UnableToReceiveCoordinatorMessage,
}

pub async fn listen(project_dir: PathBuf) -> Result<()> {
    let mut tasks = JoinSet::new();

    let (coordinator_msg_tx, mut coordinator_msg_rx) = mpsc::channel(8);
    let (worker_msg_tx, worker_msg_rx) = mpsc::channel(8);
    spawn_io_queue(&mut tasks, coordinator_msg_tx, worker_msg_rx);

    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let (stdin_tx, stdin_rx) = mpsc::channel(8);
    tokio::spawn(manage_processes(
        worker_msg_tx.clone(),
        stdin_rx,
        cmd_rx,
        project_dir.clone(),
    ));

    // TODO: watch this
    tokio::spawn(async move {
        let mut msg_tasks = JoinSet::new(); // TODO check this

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
                    msg_tasks.spawn({
                        handle_write_file(req, project_dir.clone(), worker_msg_tx())
                            .context(UnableToWriteFileSnafu)
                    });
                }

                CoordinatorMessage::ReadFile(req) => {
                    msg_tasks.spawn({
                        handle_read_file(req, project_dir.clone(), worker_msg_tx())
                            .context(UnableToReadFileSnafu)
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

        <Result<_>>::Ok(())
    });

    // Shutdown when any of these critical tasks goes wrong.
    if tasks.join_next().await.is_some() {
        tasks.shutdown().await;
    }

    Ok(())
}

struct MultiplexingSender {
    job_id: JobId,
    tx: mpsc::Sender<Multiplexed<WorkerMessage>>,
}

impl MultiplexingSender {
    async fn send(
        &self,
        message: WorkerMessage,
    ) -> Result<(), mpsc::error::SendError<Multiplexed<WorkerMessage>>> {
        self.tx.send(Multiplexed(self.job_id, message)).await
    }
}

async fn handle_write_file(
    req: WriteFileRequest,
    project_dir: PathBuf,
    worker_msg_tx: MultiplexingSender,
) -> Result<(), WriteFileError> {
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

    worker_msg_tx
        .send(WriteFileResponse(()).into())
        .await
        .drop_error_details()
        .context(UnableToSendWorkerMessageSnafu)
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
    worker_msg_tx: MultiplexingSender,
) -> Result<(), ReadFileError> {
    use read_file_error::*;

    let path = parse_working_dir(Some(req.path), &project_dir);

    let content = fs::read(&path)
        .await
        .context(UnableToReadFileSnafu { path })?;

    worker_msg_tx
        .send(ReadFileResponse(content).into())
        .await
        .drop_error_details()
        .context(UnableToSendWorkerMessageSnafu)
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
    worker_msg_tx: mpsc::Sender<Multiplexed<WorkerMessage>>,
    mut stdin_rx: mpsc::Receiver<Multiplexed<String>>,
    mut cmd_rx: mpsc::Receiver<CommandRequest>,
    project_path: PathBuf,
) -> Result<(), ProcessError> {
    use process_error::*;

    let mut processes = HashMap::new();
    let mut stdin_senders: HashMap<JobId, mpsc::Sender<String>> = HashMap::new();

    loop {
        select! {
            cmd_req = cmd_rx.recv() => {
                let (Multiplexed(job_id, req), response_tx) = cmd_req.context(CommandRequestReceiverEndedSnafu)?;
                let ExecuteCommandRequest {
                    cmd,
                    args,
                    envs,
                    cwd,
                } = req;
                let mut child = Command::new(cmd)
                    .args(args)
                    .envs(envs)
                    .current_dir(parse_working_dir(cwd, project_path.as_path()))
                    .kill_on_drop(true)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn().context(UnableToSpawnProcessSnafu)?;

                // Preparing for receiving stdin packet.
                let (stdin_tx, stdin_rx) = mpsc::channel(8);
                stdin_senders.insert(job_id, stdin_tx);

                let task_set = stream_stdio(worker_msg_tx.clone(), stdin_rx, &mut child, job_id).context(StdioSnafu)?;
                // TODO: watch this spawn
                tokio::spawn(async move {
                    child.wait().await.context(WaitChildSnafu)?;

                    response_tx
                        .send(ExecuteCommandResponse(()).into())
                        .await
                        .drop_error_details()
                        .context(UnableToSendWorkerMessageSnafu)
                });
                processes.insert(job_id, task_set);
            }
            stdin_packet = stdin_rx.recv() => {
                // Dispatch stdin packet to different child by attached command id.
                let Multiplexed(job_id, packet) = stdin_packet.context(StdinReceiverEndedSnafu)?;
                if let Some(stdin_tx) = stdin_senders.get(&job_id) {
                    stdin_tx.send(packet).await.drop_error_details().context(UnableToSendStdinDataSnafu)?;
                }
            }
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum ProcessError {
    #[snafu(display("Failed to spawn child process"))]
    UnableToSpawnProcess {
        source: std::io::Error,
    },

    #[snafu(display("Command request receiver ended unexpectedly"))]
    CommandRequestReceiverEnded,

    #[snafu(display("Stdin packet receiver ended unexpectedly"))]
    StdinReceiverEnded,

    #[snafu(display("Failed to send stdin data"))]
    UnableToSendStdinData {
        source: mpsc::error::SendError<()>,
    },

    Stdio {
        source: StdioError,
    },

    #[snafu(display("Failed to wait for child process exiting"))]
    WaitChild {
        source: std::io::Error,
    },

    #[snafu(display("Failed to send worker message to serialization task"))]
    UnableToSendWorkerMessage {
        source: mpsc::error::SendError<()>,
    },
}

fn stream_stdio(
    coordinator_tx: mpsc::Sender<Multiplexed<WorkerMessage>>,
    mut stdin_rx: mpsc::Receiver<String>,
    child: &mut Child,
    job_id: JobId,
) -> Result<JoinSet<Result<(), StdioError>>, StdioError> {
    use stdio_error::*;

    let mut stdin = child.stdin.take().context(UnableToCaptureStdinSnafu)?;
    let stdout = child.stdout.take().context(UnableToCaptureStdoutSnafu)?;
    let stderr = child.stderr.take().context(UnableToCaptureStderrSnafu)?;

    let mut set = JoinSet::new();

    set.spawn(async move {
        loop {
            let data = stdin_rx
                .recv()
                .await
                .context(UnableToReceiveStdinDataSnafu)?;
            stdin
                .write_all(data.as_bytes())
                .await
                .context(UnableToWriteStdinSnafu)?;
            stdin.flush().await.context(UnableToFlushStdinSnafu)?;
        }
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
            if n != 0 {
                coordinator_tx_out
                    .send(Multiplexed(job_id, WorkerMessage::StdoutPacket(buffer)))
                    .await
                    .drop_error_details()
                    .context(UnableToSendStdoutPacketSnafu)?;
            } else {
                break;
            }
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
            if n != 0 {
                coordinator_tx_err
                    .send(Multiplexed(job_id, WorkerMessage::StderrPacket(buffer)))
                    .await
                    .drop_error_details()
                    .context(UnableToSendStderrPacketSnafu)?;
            } else {
                break;
            }
        }
        Ok(())
    });

    Ok(set)
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum StdioError {
    #[snafu(display("Failed to capture child process stdin"))]
    UnableToCaptureStdin,

    #[snafu(display("Failed to capture child process stdout"))]
    UnableToCaptureStdout,

    #[snafu(display("Failed to capture child process stderr"))]
    UnableToCaptureStderr,

    #[snafu(display("Failed to receive stdin data"))]
    UnableToReceiveStdinData,

    #[snafu(display("Failed to write stdin data"))]
    UnableToWriteStdin { source: std::io::Error },

    #[snafu(display("Failed to flush stdin data"))]
    UnableToFlushStdin { source: std::io::Error },

    #[snafu(display("Failed to read child process stdout"))]
    UnableToReadStdout { source: std::io::Error },

    #[snafu(display("Failed to read child process stderr"))]
    UnableToReadStderr { source: std::io::Error },

    #[snafu(display("Failed to send stdout packet"))]
    UnableToSendStdoutPacket { source: mpsc::error::SendError<()> },

    #[snafu(display("Failed to send stderr packet"))]
    UnableToSendStderrPacket { source: mpsc::error::SendError<()> },
}

// stdin/out <--> messages.
fn spawn_io_queue(
    tasks: &mut JoinSet<Result<(), IoQueueError>>,
    coordinator_msg_tx: mpsc::Sender<Multiplexed<CoordinatorMessage>>,
    mut worker_msg_rx: mpsc::Receiver<Multiplexed<WorkerMessage>>,
) {
    use io_queue_error::*;
    use std::io::{prelude::*, BufReader, BufWriter};

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
