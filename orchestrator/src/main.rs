use orchestrator::worker::{listen, Error};
use std::env;

#[tokio::main(flavor = "current_thread")]
#[snafu::report]
pub async fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();

    // Panics messages are written to stderr.
    let project_dir = args
        .get(1)
        .expect("Please specify project directory as the first argument");

    listen(project_dir.into()).await
}
