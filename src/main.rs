pub mod dataframe;
pub mod operation;
pub mod parse;
pub mod server;
pub mod store;
pub mod value;

use server::Server;

const REDIS_PORT: &str = "6379";

#[tokio::main]
async fn main() {
    Server::new(REDIS_PORT).listen().await;
}
