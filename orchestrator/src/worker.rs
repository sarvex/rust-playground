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
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

use crate::message::{
    CommandId, CoordinatorMessage, ExecuteCommandRequest, ExecuteCommandResponse, JobId, JobReport,
    ReadFileRequest, ReadFileResponse, Request, Response, WorkerMessage, WriteFileRequest,
    WriteFileResponse,
};

type CommandRequest = (CommandId, ExecuteCommandRequest, oneshot::Sender<()>);

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create directories"))]
    UnableToCreateDir { source: std::io::Error },

    #[snafu(display("Failed to write file"))]
    UnableToWriteFile { source: std::io::Error },

    #[snafu(display("Failed to read file"))]
    UnableToReadFile { source: std::io::Error },

    #[snafu(display("Failed to send command execution request"))]
    UnableToSendCommandExecutionRequest {
        source: mpsc::error::SendError<CommandRequest>,
    },

    #[snafu(display("Failed to send stdin_sender"))]
    UnableToSendStdinSender {
        source: mpsc::error::SendError<(CommandId, mpsc::Sender<String>)>,
    },

    #[snafu(display("Failed to receiver command completion signal"))]
    UnableToReceiveCommandCompletion { source: oneshot::error::RecvError },

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
        source: mpsc::error::SendError<(CommandId, String)>,
    },

    #[snafu(display("Failed to send stdout packet"))]
    UnableToSendStdoutPacket {
        source: mpsc::error::SendError<WorkerMessage>,
    },

    #[snafu(display("Failed to send stderr packet"))]
    UnableToSendStderrPacket {
        source: mpsc::error::SendError<WorkerMessage>,
    },

    #[snafu(display("Failed to wait for child process exiting"))]
    WaitChild { source: std::io::Error },

    #[snafu(display("Failed to send command completion signal"))]
    UnableToSendCommandCompletion,

    #[snafu(display("Failed to send coordinator message from deserialization task"))]
    UnableToSendCoordinatorMessage {
        source: mpsc::error::SendError<CoordinatorMessage>,
    },

    #[snafu(display("Failed to receive coordinator message from deserialization task"))]
    UnableToReceiveCoordinatorMessage,

    #[snafu(display("Failed to send worker message to serialization task"))]
    UnableToSendWorkerMessage {
        source: mpsc::error::SendError<WorkerMessage>,
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

    #[snafu(display("Receiver of stdin packet sender ended unexpectedly"))]
    StdinSenderReceiverEnded,
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
        // TODO: may change this to a hashmap to allow multiple concurrent jobs.
        let mut current_job: Option<JoinHandle<Result<()>>> = None;

        let project_path = project_dir.as_path();
        loop {
            let coordinator_msg = coordinator_msg_rx
                .recv()
                .await
                .context(UnableToReceiveCoordinatorMessageSnafu)?;
            match coordinator_msg {
                CoordinatorMessage::Request(job_id, job) => {
                    if let Some(current_job) = current_job {
                        current_job.abort();
                    }

                    let worker_msg_tx = worker_msg_tx.clone();
                    let cmd_tx = cmd_tx.clone();
                    let project_path = project_path.to_path_buf();

                    let task_handle = tokio::spawn(async move {
                        let mut all_responses = Vec::new();
                        for (operation_id, req) in job.reqs.into_iter().enumerate() {
                            let resp = handle_request(
                                req,
                                project_path.as_path(),
                                cmd_tx.clone(),
                                job_id,
                                operation_id as u64,
                            )
                            .await;
                            let success = resp.is_ok();
                            all_responses.push(resp.into());
                            if !success {
                                break;
                            }
                        }
                        let job_report = JobReport {
                            resps: all_responses,
                        };
                        worker_msg_tx
                            .send(WorkerMessage::Response(job_id, job_report))
                            .await
                            .context(UnableToSendWorkerMessageSnafu)?;
                        Ok(())
                    });

                    current_job = Some(task_handle);
                }
                CoordinatorMessage::StdinPacket(cmd_id, data) => {
                    stdin_tx
                        .send((cmd_id, data))
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

async fn handle_request(
    req: Request,
    project_path: &Path,
    cmd_tx: mpsc::Sender<CommandRequest>,
    job_id: JobId,
    operation_id: u64,
) -> Result<Response> {
    match req {
        Request::WriteFile(WriteFileRequest { path, content }) => {
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
            Ok(Response::WriteFile(WriteFileResponse(())))
        }
        Request::ReadFile(ReadFileRequest { path }) => {
            let path = parse_working_dir(Some(path), project_path);
            let content = fs::read(path).await.context(UnableToReadFileSnafu)?;
            Ok(Response::ReadFile(ReadFileResponse(content)))
        }
        Request::ExecuteCommand(cmd) => {
            let (response_tx, response_rx) = oneshot::channel();
            cmd_tx
                .send(((job_id, operation_id), cmd, response_tx))
                .await
                .context(UnableToSendCommandExecutionRequestSnafu)?;
            response_rx
                .await
                .context(UnableToReceiveCommandCompletionSnafu)?;
            Ok(Response::ExecuteCommand(ExecuteCommandResponse(())))
        }
    }
}

async fn manage_processes(
    worker_msg_tx: mpsc::Sender<WorkerMessage>,
    mut stdin_rx: mpsc::Receiver<(CommandId, String)>,
    mut cmd_rx: mpsc::Receiver<CommandRequest>,
    project_path: PathBuf,
) -> Result<()> {
    let mut processes = HashMap::new();
    let mut stdin_senders: HashMap<(u64, u64), mpsc::Sender<String>> = HashMap::new();
    let (stdin_sender_tx, mut stdin_sender_rx) = mpsc::channel(8);
    loop {
        select! {
            cmd_req = cmd_rx.recv() => {
                if let Some((cmd_id, req, response_tx)) = cmd_req {
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
                    stdin_sender_tx.send((cmd_id, stdin_tx)).await.context(UnableToSendStdinSenderSnafu)?;

                    let mut task_set = stream_stdio(worker_msg_tx.clone(), stdin_rx, &mut child, cmd_id).await?;
                    task_set.spawn(async move {
                        child.wait().await.context(WaitChildSnafu)?;
                        response_tx.send(()).map_err(|_e| UnableToSendCommandCompletionSnafu.build())?;
                        Ok(())
                    });
                    processes.insert(cmd_id, task_set);
                } else {
                    return CommandRequestReceiverEndedSnafu.fail();
                }
            }
            stdin_packet = stdin_rx.recv() => {
                // Dispatch stdin packet to different child by attached command id.
                if let Some((cmd_id, packet)) = stdin_packet {
                    if let Some(stdin_tx) = stdin_senders.get(&cmd_id) {
                        stdin_tx.send(packet).await.context(UnableToSendStdinDataSnafu)?;
                    }
                } else {
                    return StdinReceiverEndedSnafu.fail();
                }
            }
            stdin_sender = stdin_sender_rx.recv() => {
                // Store stdin packet senders so you can dispatch packet to them later.
                if let Some((cmd_id, stdin_tx)) = stdin_sender {
                    stdin_senders.insert(cmd_id, stdin_tx);
                } else {
                    return StdinSenderReceiverEndedSnafu.fail();
                }
            }
        }
    }
}

async fn stream_stdio(
    coordinator_tx: mpsc::Sender<WorkerMessage>,
    mut stdin_rx: mpsc::Receiver<String>,
    child: &mut Child,
    cmd_id: CommandId,
) -> Result<JoinSet<Result<()>>> {
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| UnableToCaptureStdinSnafu.build())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| UnableToCaptureStdoutSnafu.build())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| UnableToCaptureStderrSnafu.build())?;

    let mut set = JoinSet::new();

    set.spawn(async move {
        loop {
            let data = stdin_rx
                .recv()
                .await
                .ok_or_else(|| UnableToReceiveStdinDataSnafu.build())?;
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
                    .send(WorkerMessage::StdoutPacket(cmd_id, buffer))
                    .await
                    .context(UnableToSendStdoutPacketSnafu)?;
            } else {
                break;
            }
        }
        Ok::<(), Error>(())
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
                    .send(WorkerMessage::StderrPacket(cmd_id, buffer))
                    .await
                    .context(UnableToSendStderrPacketSnafu)?;
            } else {
                break;
            }
        }
        Ok::<(), Error>(())
    });
    Ok(set)
}

// stdin/out <--> messages.
fn spawn_io_queue(
    tasks: &mut JoinSet<Result<()>>,
    coordinator_msg_tx: mpsc::Sender<CoordinatorMessage>,
    mut worker_msg_rx: mpsc::Receiver<WorkerMessage>,
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
