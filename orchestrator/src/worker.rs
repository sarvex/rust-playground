use snafu::prelude::*;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    select,
    sync::{mpsc, Notify},
    task::JoinSet,
};

use crate::message::{
    CoordinatorMessage, ExecuteCommandRequest, ExecuteCommandResponse, JobId, Multiplexed,
    ReadFileRequest, ReadFileResponse, WorkerMessage, WriteFileRequest, WriteFileResponse,
};

type CommandRequest = (JobId, ExecuteCommandRequest, Arc<Notify>);

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create directories"))]
    UnableToCreateDir { source: std::io::Error },

    #[snafu(display("Failed to write file"))]
    UnableToWriteFile { source: std::io::Error },

    #[snafu(display("Failed to read file {}", path.display()))]
    UnableToReadFile {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Failed to send command execution request"))]
    UnableToSendCommandExecutionRequest {
        source: mpsc::error::SendError<CommandRequest>,
    },

    #[snafu(display("Failed to spawn child process"))]
    UnableToSpawnProcess { source: std::io::Error },

    #[snafu(display("Failed to capture child process stdin"))]
    UnableToCaptureStdin,

    #[snafu(display("Failed to capture child process stdout"))]
    UnableToCaptureStdout,

    #[snafu(display("Failed to capture child process stderr"))]
    UnableToCaptureStderr,

    #[snafu(display("Failed to send stdin data"))]
    UnableToSendStdinData {
        source: mpsc::error::SendError<String>,
    },

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

    #[snafu(display("Failed to flush stdout"))]
    UnableToFlushStdout { source: std::io::Error },

    #[snafu(display("Failed to send stdin packet"))]
    UnableToSendStdinPacket {
        source: mpsc::error::SendError<Multiplexed<String>>,
    },

    #[snafu(display("Failed to send stdout packet"))]
    UnableToSendStdoutPacket {
        source: mpsc::error::SendError<Multiplexed<WorkerMessage>>,
    },

    #[snafu(display("Failed to send stderr packet"))]
    UnableToSendStderrPacket {
        source: mpsc::error::SendError<Multiplexed<WorkerMessage>>,
    },

    #[snafu(display("Failed to wait for child process exiting"))]
    WaitChild { source: std::io::Error },

    #[snafu(display("Failed to send coordinator message from deserialization task"))]
    UnableToSendCoordinatorMessage {
        source: mpsc::error::SendError<Multiplexed<CoordinatorMessage>>,
    },

    #[snafu(display("Failed to receive coordinator message from deserialization task"))]
    UnableToReceiveCoordinatorMessage,

    #[snafu(display("Failed to send worker message to serialization task"))]
    UnableToSendWorkerMessage {
        source: mpsc::error::SendError<Multiplexed<WorkerMessage>>,
    },

    #[snafu(display("Failed to receive worker message"))]
    UnableToReceiveWorkerMessage,

    #[snafu(display("Failed to deserialize coordinator message"))]
    UnableToDeserializeCoordinatorMessage { source: bincode::Error },

    #[snafu(display("Failed to serialize worker message"))]
    UnableToSerializeWorkerMessage { source: bincode::Error },

    #[snafu(display("Command request recevier ended unexpectedly"))]
    CommandRequestReceiverEnded,

    #[snafu(display("Stdin packet recevier ended unexpectedly"))]
    StdinReceiverEnded,
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
    tasks.spawn(async move {
        let project_path = project_dir.as_path();
        loop {
            let coordinator_msg = coordinator_msg_rx
                .recv()
                .await
                .context(UnableToReceiveCoordinatorMessageSnafu)?;

            let Multiplexed(job_id, coordinator_msg) = coordinator_msg;

            match coordinator_msg {
                CoordinatorMessage::WriteFile(WriteFileRequest { path, content }) => {
                    let path = parse_working_dir(Some(path), project_path);

                    // Create intermediate directories.
                    if let Some(parent_dir) = path.parent() {
                        fs::create_dir_all(parent_dir)
                            .await
                            .context(UnableToCreateDirSnafu)?;
                    }
                    fs::write(path, content)
                        .await
                        .context(UnableToWriteFileSnafu)?;

                    worker_msg_tx
                        .send(Multiplexed(job_id, WriteFileResponse(()).into()))
                        .await
                        .context(UnableToSendWorkerMessageSnafu)?;
                }

                CoordinatorMessage::ReadFile(ReadFileRequest { path }) => {
                    // TODO: spawn

                    let path = parse_working_dir(Some(path), project_path);
                    let content = fs::read(&path)
                        .await
                        .context(UnableToReadFileSnafu { path })?;

                    worker_msg_tx
                        .send(Multiplexed(job_id, ReadFileResponse(content).into()))
                        .await
                        .context(UnableToSendWorkerMessageSnafu)?;
                }

                CoordinatorMessage::ExecuteCommand(cmd) => {
                    // TODO: spawn
                    let notify = Arc::new(Notify::new());
                    cmd_tx
                        .send((job_id, cmd, notify.clone()))
                        .await
                        .context(UnableToSendCommandExecutionRequestSnafu)?;
                    notify.notified().await;

                    worker_msg_tx
                        .send(Multiplexed(job_id, ExecuteCommandResponse(()).into()))
                        .await
                        .context(UnableToSendWorkerMessageSnafu)?;
                }

                CoordinatorMessage::StdinPacket(data) => {
                    stdin_tx
                        .send(Multiplexed(job_id, data))
                        .await
                        .context(UnableToSendStdinPacketSnafu)?;
                }
            }
        }
    });
    // Shutdown when any of these critical tasks goes wrong.
    if tasks.join_next().await.is_some() {
        tasks.shutdown().await;
    }
    Ok(())
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
) -> Result<()> {
    let mut processes = HashMap::new();
    let mut stdin_senders: HashMap<JobId, mpsc::Sender<String>> = HashMap::new();
    loop {
        select! {
            cmd_req = cmd_rx.recv() => {
                let (cmd_id, req, response_tx) = cmd_req.context(CommandRequestReceiverEndedSnafu)?;
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
                stdin_senders.insert(cmd_id, stdin_tx);

                let mut task_set = stream_stdio(worker_msg_tx.clone(), stdin_rx, &mut child, cmd_id)?;
                task_set.spawn(async move {
                    child.wait().await.context(WaitChildSnafu)?;
                    response_tx.notify_one();
                    Ok(())
                });
                processes.insert(cmd_id, task_set);
            }
            stdin_packet = stdin_rx.recv() => {
                // Dispatch stdin packet to different child by attached command id.
                let Multiplexed(job_id, packet) = stdin_packet.context(StdinReceiverEndedSnafu)?;
                if let Some(stdin_tx) = stdin_senders.get(&job_id) {
                    stdin_tx.send(packet).await.context(UnableToSendStdinDataSnafu)?;
                }
            }
        }
    }
}

fn stream_stdio(
    coordinator_tx: mpsc::Sender<Multiplexed<WorkerMessage>>,
    mut stdin_rx: mpsc::Receiver<String>,
    child: &mut Child,
    job_id: JobId,
) -> Result<JoinSet<Result<()>>> {
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
                    .context(UnableToSendStderrPacketSnafu)?;
            } else {
                break;
            }
        }
        Ok(())
    });
    Ok(set)
}

// stdin/out <--> messages.
fn spawn_io_queue(
    tasks: &mut JoinSet<Result<()>>,
    coordinator_msg_tx: mpsc::Sender<Multiplexed<CoordinatorMessage>>,
    mut worker_msg_rx: mpsc::Receiver<Multiplexed<WorkerMessage>>,
) {
    use std::io::{prelude::*, BufReader, BufWriter};

    tasks.spawn(async move {
        tokio::task::spawn_blocking(move || {
            let stdin = std::io::stdin();
            let mut stdin = BufReader::new(stdin);

            loop {
                let coordinator_msg = bincode::deserialize_from(&mut stdin)
                    .context(UnableToDeserializeCoordinatorMessageSnafu)?;

                coordinator_msg_tx
                    .blocking_send(coordinator_msg)
                    .context(UnableToSendCoordinatorMessageSnafu)?;
            }
        }).await.unwrap(/* Panic occurred; re-raising */)
    });

    tasks.spawn(async move {
        tokio::task::spawn_blocking(move || {
            let stdout = std::io::stdout();
            let mut stdout = BufWriter::new(stdout);

            loop {
                let worker_msg = worker_msg_rx
                    .blocking_recv()
                    .context(UnableToReceiveWorkerMessageSnafu)?;

                bincode::serialize_into(&mut stdout, &worker_msg).context(UnableToSerializeWorkerMessageSnafu)?;

                stdout.flush().context(UnableToFlushStdoutSnafu)?;
            }
        }).await.unwrap(/* Panic occurred; re-raising */)
    });
}
