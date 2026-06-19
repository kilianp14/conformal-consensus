use crate::{configs::ServerConfig, server::OmniPaxosServer};

mod configs;
mod database;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match ServerConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut server = OmniPaxosServer::new(server_config).await;
    server.run().await;
}
