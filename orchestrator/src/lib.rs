pub mod coordinator;
mod message;
pub mod sandbox;
pub mod worker;

trait DropErrorDetailsExt<T> {
    fn drop_error_details(self) -> Result<T, tokio::sync::mpsc::error::SendError<()>>;
}

impl<T, E> DropErrorDetailsExt<T> for Result<T, tokio::sync::mpsc::error::SendError<E>> {
    fn drop_error_details(self) -> Result<T, tokio::sync::mpsc::error::SendError<()>> {
        self.map_err(|_| tokio::sync::mpsc::error::SendError(()))
    }
}

trait JoinSetExt<T> {
    fn spawn_blocking<F>(&mut self, f: F) -> tokio::task::AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static;
}

impl<T> JoinSetExt<T> for tokio::task::JoinSet<T> {
    fn spawn_blocking<F>(&mut self, f: F) -> tokio::task::AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        self.spawn(async move {
            tokio::task::spawn_blocking(f)
                .await
                .unwrap(/* Panic occurred; re-raising */)
        })
    }
}

fn bincode_input_closed<T>(coordinator_msg: &bincode::Result<T>) -> bool {
    if let Err(e) = coordinator_msg {
        if let bincode::ErrorKind::Io(e) = &**e {
            return e.kind() == std::io::ErrorKind::UnexpectedEof;
        }
    }

    false
}
