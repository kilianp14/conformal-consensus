use crate::{configs::OmniPaxosKVConfig, database::Database, network::Network};
use benchmark::common::{
    ClientId, ClientMessage, ClusterMessage, Command, CommandId, KVCommand, ServerMessage,
    Timestamp,
};
use chrono::Utc;
use log::*;
use omnipaxos::{
    OmniPaxos, OmniPaxosConfig,
    messages::Message,
    utils::{LogEntry, NodeId},
};
use std::{fs::File, io::Write, time::Duration};
use tokio::signal::unix::{SignalKind, signal};

const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const SEND_OUTGOING_MESSAGES_INTERVAL: Duration = Duration::from_millis(1);

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxos<Command>,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosKVConfig,
    peers: Vec<NodeId>,
    #[cfg(feature = "adaptive")]
    calibration_time: Option<i64>,
    #[cfg(feature = "adaptive")]
    calibration_index: usize,
    #[cfg(feature = "adaptive")]
    is_calibrating: bool,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosKVConfig) -> Self {
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build().unwrap();
        // Waits for client and server network connections to be established
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        OmniPaxosServer {
            id: config.local.server_id,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            config,
            #[cfg(feature = "adaptive")]
            calibration_time: None,
            #[cfg(feature = "adaptive")]
            calibration_index: 0,
            #[cfg(feature = "adaptive")]
            is_calibrating: false,
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        let mut outgoing_interval = tokio::time::interval(SEND_OUTGOING_MESSAGES_INTERVAL);
        loop {
            tokio::select! {
                _ = shutdown_signal() => {
                    self.network.shutdown();
                    #[cfg(feature = "adaptive")]
                    self.omnipaxos.take_fast_path_stats();
                    break;
                }
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    #[cfg(feature = "adaptive")]
                    if self.calibration_time.is_some_and(|t| Utc::now().timestamp_millis() >= t) {
                        let schedule = &self.config.local.calibration_schedule;
                        if !self.is_calibrating {
                            info!("{}: Triggering OmniPaxos calibration start {}/{}",
                                self.id, self.calibration_index + 1, schedule.len());

                            self.omnipaxos.take_fast_path_stats();
                            self.omnipaxos.start_calibration();
                            self.is_calibrating = true;

                            let duration = schedule[self.calibration_index].duration_ms;
                            self.calibration_time = Some(Utc::now().timestamp_millis() + duration as i64);
                        } else {
                            info!("{}: Triggering OmniPaxos calibration end {}/{}",
                                self.id, self.calibration_index + 1, schedule.len());

                            self.omnipaxos.take_fast_path_stats();
                            self.omnipaxos.end_calibration();
                            self.is_calibrating = false;
                            self.calibration_index += 1;

                            if self.calibration_index < schedule.len() {
                                let next_delay = schedule[self.calibration_index].start_delay_ms;
                                self.calibration_time = Some(Utc::now().timestamp_millis() + next_delay as i64);
                            } else {
                                self.calibration_time = None;
                            }
                        }
                    }
                },
                _ = outgoing_interval.tick() => {
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
            }
        }
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, true)) = self.omnipaxos.get_current_leader() && curr_leader == self.id {
                        info!("{}: Leader fully initialized", self.id);
                        let experiment_sync_start = (Utc::now() + Duration::from_secs(5)).timestamp_millis();
                        #[cfg(feature = "adaptive")]
                        if let Some(first_window) = self.config.local.calibration_schedule.first() {
                            self.calibration_time = Some(experiment_sync_start + first_window.start_delay_ms as i64);
                        }
                        self.send_cluster_start_signals(experiment_sync_start);
                        self.send_client_start_signals(experiment_sync_start);
                        break;
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    fn handle_decided_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands);
        }
    }

    fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network.send_to_client(command.client_id, response);
            }
        }
    }

    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, (command_id, kv_command)) in messages.drain(..) {
            self.append_to_log(from, command_id, kv_command)
        }
        self.send_outgoing_msgs();
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries();
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    #[cfg(feature = "adaptive")]
                    if let Some(first_window) = self.config.local.calibration_schedule.first() {
                        self.calibration_time =
                            Some(start_time + first_window.start_delay_ms as i64);
                    }
                    self.send_client_start_signals(start_time);
                }
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        self.omnipaxos.append(command)
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}

async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");

    tokio::select! {
        _ = sigint.recv() => {
            info!("SIGINT received");
        }
        _ = sigterm.recv() => {
            info!("SIGTERM received");
        }
    }
}
