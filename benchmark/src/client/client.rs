use std::time::Duration;

use crate::{configs::ClientConfig, data_collection::ClientData, network::Network};
use benchmark::common::{ClientId, KVCommand, NodeId, ServerMessage};
use chrono::Utc;
use log::{debug, info, warn};
use rand::{RngExt, SeedableRng, rngs::SmallRng};
use tokio::time::{Instant, sleep_until};

const NETWORK_BATCH_SIZE: usize = 100;
const SEED: u64 = 14;

pub struct Client {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    next_request_id: usize,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let network = Network::new(
            vec![(config.server_id, config.server_address.clone())],
            NETWORK_BATCH_SIZE,
        )
        .await;
        Client {
            id: config.server_id,
            network,
            client_data: ClientData::new(),
            active_server: config.server_id,
            config,
            next_request_id: 0,
        }
    }

    pub async fn run(&mut self) {
        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                Self::wait_until_sync_time(&mut self.config, start_time).await;
            }
            _ => panic!("Error waiting for start signal"),
        }

        let start_instant = Instant::now();
        let max_duration = Duration::from_secs(self.config.max_duration_sec);
        let mut rng = SmallRng::seed_from_u64(SEED);

        // Initialize the stateful load pattern runner
        let mut load_runner = self.config.load_pattern.clone().into_runner();
        let mut next_request_at =
            Instant::now() + load_runner.next_delay(Duration::from_secs(0), &mut rng);

        info!(
            "{}: Starting requests with load pattern: {:?}",
            self.id, self.config.load_pattern
        );
        loop {
            let now = Instant::now();
            let elapsed = now.duration_since(start_instant);

            if elapsed >= max_duration {
                info!(
                    "{}: Max duration reached, stopping request generation",
                    self.id
                );
                break;
            }

            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => self.handle_server_message(msg),
                _ = sleep_until(next_request_at) => {
                    let is_write = rng.random_bool(1.0 - self.config.read_ratio);
                    self.send_request(is_write).await;

                    // Request the dynamic wait duration from the runner
                    next_request_at = Instant::now() + load_runner.next_delay(elapsed, &mut rng);
                },
            }
        }
        let drain_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => self.handle_server_message(msg),
                _ = sleep_until(drain_deadline) => {
                    break;
                }
            }
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        self.network.shutdown();
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) {
        debug!("Recieved {msg:?}");
        match msg {
            ServerMessage::StartSignal(_) => (),
            server_response => {
                let cmd_id = server_response.command_id();
                let acc_status = server_response.accept_status();
                self.client_data.new_response(cmd_id, acc_status);
            }
        }
    }

    async fn send_request(&mut self, is_write: bool) {
        let key = self.next_request_id.to_string();
        let cmd = match is_write {
            true => KVCommand::Put(key.clone(), key),
            false => KVCommand::Get(key),
        };
        let request = (self.next_request_id, cmd);
        debug!("Sending {request:?}");
        self.network.send(self.active_server, request).await;
        self.client_data.new_request(is_write);
        self.next_request_id += 1;
    }

    // Wait until the scheduled start time to synchronize client starts.
    // If start time has already passed, start immediately.
    async fn wait_until_sync_time(config: &mut ClientConfig, scheduled_start_utc_ms: i64) {
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start_utc_ms - now.timestamp_millis();
        config.sync_time = Some(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        } else {
            warn!("Started after synchronization point!");
        }
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        self.client_data.save_summary(self.config.clone())?;
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
        Ok(())
    }
}
