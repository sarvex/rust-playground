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
