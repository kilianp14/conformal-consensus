use crate::{configs::ServerConfig, database::Database};
use benchmark::common::{
    Command, DaemonToServer, FromClient, IPC_SOCKET_PATH, IpcFromDaemon, IpcToServer,
    ServerToDaemon, ToClient, frame_ipc_server_side,
};
use chrono::Utc;
use futures::{SinkExt, StreamExt, stream::ReadyChunks};
use log::*;
use omnipaxos::{
    OmniPaxos, OmniPaxosConfig,
    messages::Message,
    utils::{AcceptStatus, LogEntry, NodeId, create_logger},
};
use std::{fs::File, io::Write, time::Duration};
use tokio::{
    net::UnixStream,
    signal::unix::{SignalKind, signal},
};

const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const GET_SYSTEM_INFO_INTERVAL: Duration = Duration::from_secs(60);

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    ipc_rx: ReadyChunks<IpcFromDaemon>,
    ipc_tx: IpcToServer,
    omnipaxos: OmniPaxos<Command>,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: ServerConfig,
    start_time: Option<i64>,
    // For overhead eval
    append_count: u64,
    append_total_time_ns: u64,
    // For offline calibration schedule
    #[cfg(feature = "adaptive")]
    calibration_time: Option<i64>,
    #[cfg(feature = "adaptive")]
    calibration_index: usize,
    #[cfg(feature = "adaptive")]
    is_calibrating: bool,
}

impl OmniPaxosServer {
    pub async fn new(config: ServerConfig) -> Self {
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build().unwrap();

        let ipc_stream = loop {
            match UnixStream::connect(IPC_SOCKET_PATH).await {
                Ok(stream) => break stream,
                Err(e) => {
                    warn!(
                        "Waiting for network daemon at {}... ({})",
                        IPC_SOCKET_PATH, e
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        };

        info!("Connected to local network daemon.");
        let (ipc_rx, ipc_tx) = frame_ipc_server_side(ipc_stream);

        OmniPaxosServer {
            id: config.server_id,
            database: Database::new(),
            ipc_rx: ipc_rx.ready_chunks(100),
            ipc_tx,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            config,
            start_time: None,
            append_count: 0,
            append_total_time_ns: 0,
            #[cfg(feature = "adaptive")]
            calibration_time: None,
            #[cfg(feature = "adaptive")]
            calibration_index: 0,
            #[cfg(feature = "adaptive")]
            is_calibrating: false,
        }
    }

    pub async fn run(&mut self) {
        self.save_output().expect("Failed to write to file");
        self.establish_initial_leader().await;

        let mut sigterm = signal(SignalKind::terminate()).expect("Failed install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed install SIGINT handler");

        let logger = create_logger(&self.config.output_filepath);

        let start_time_ms = self
            .start_time
            .expect("Start time should be set by establish_initial_leader");
        let delay_ms = start_time_ms.saturating_sub(Utc::now().timestamp_millis()) as u64;
        let start_instant = tokio::time::Instant::now() + Duration::from_millis(delay_ms);

        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        let mut info_interval = tokio::time::interval_at(
            start_instant + GET_SYSTEM_INFO_INTERVAL,
            GET_SYSTEM_INFO_INTERVAL,
        );

        loop {
            tokio::select! {
                _ = sigint.recv() => {
                    info!("SIGINT received");
                    break;
                }
                _ = sigterm.recv() => {
                    info!("SIGTERM received");
                    break;
                }
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    #[cfg(feature = "adaptive")]
                    if self.calibration_time.is_some_and(|t| Utc::now().timestamp_millis() >= t) {
                        let schedule = &self.config.calibration_schedule;
                        if !self.is_calibrating {
                            info!("{}: Triggering OmniPaxos calibration start {}/{}",
                                self.id, self.calibration_index + 1, schedule.len());

                            // Time start_calibration
                            let start_inst = std::time::Instant::now();
                            self.omnipaxos.start_calibration();
                            let duration = start_inst.elapsed();
                            slog::info!(logger, "start_calibration execution time: {:?}", duration);

                            self.is_calibrating = true;
                            let duration = schedule[self.calibration_index].duration_ms;
                            self.calibration_time = Some(Utc::now().timestamp_millis() + duration as i64);
                        } else {
                            info!("{}: Triggering OmniPaxos calibration end {}/{}",
                                self.id, self.calibration_index + 1, schedule.len());

                            // Time end_calibration
                            let start_inst = std::time::Instant::now();
                            self.omnipaxos.end_calibration();
                            let duration = start_inst.elapsed();
                            slog::info!(logger, "end_calibration execution time: {:?}", duration);

                            self.is_calibrating = false;
                            self.calibration_index += 1;

                            if self.calibration_index < schedule.len() {
                                let next_delay = schedule[self.calibration_index].start_delay_ms;
                                self.calibration_time = Some(self.start_time.unwrap() + next_delay as i64);
                            } else {
                                self.calibration_time = None;
                            }
                        }
                    }
                },
                _ = info_interval.tick() => {
                    // Track fast path utilization and success
                    let fast_path_stats = self.omnipaxos.take_fast_path_stats();
                    slog::info!(logger, "{}", fast_path_stats);

                    // Track execution time of appending
                    let avg_time_ns = if self.append_count > 0 {
                        self.append_total_time_ns / self.append_count
                    } else {
                        0
                    };
                    slog::info!(
                        logger,
                        "Append performance over last minute - Count: {}, Avg Time: {} ns",
                        self.append_count,
                        avg_time_ns
                    );
                    self.append_count = 0;
                    self.append_total_time_ns = 0;

                    // Track latencies (for consistency check)
                    #[cfg(feature = "adaptive")]
                    {
                        let latencies = self.omnipaxos.get_current_latencies();
                        slog::info!(logger, "Current latencies: {:?}", latencies);
                    }
                },
                msg_opt = self.ipc_rx.next() => {
                    if let Some(msgs) = msg_opt {
                        self.handle_ipc_messages(msgs).await;
                    } else {
                        error!("IPC connection to daemon closed unexpectedly. Shutting down.");
                        break;
                    }
                },
            }
            self.send_outgoing_msgs().await;
        }
    }

    async fn establish_initial_leader(&mut self) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.initial_leader == self.id => {
                    if let Some((curr_leader, true)) = self.omnipaxos.get_current_leader() && curr_leader == self.id {
                        info!("{}: Leader fully initialized", self.id);
                        let experiment_sync_start = (Utc::now() + Duration::from_secs(30)).timestamp_millis();
                        self.start_time = Some(experiment_sync_start);
                        #[cfg(feature = "adaptive")]
                        if let Some(first_window) = self.config.calibration_schedule.first() {
                            self.calibration_time = Some(experiment_sync_start + first_window.start_delay_ms as i64);
                        }
                        let msg = ServerToDaemon::StartExperiment(experiment_sync_start);
                        let _ = self.ipc_tx.feed(msg).await;
                        let _ = self.ipc_tx.flush().await;
                        break;
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                },
                msg_opt = self.ipc_rx.next() => {
                    if let Some(msgs) = msg_opt {
                        let recv_start = self.handle_ipc_messages(msgs).await;
                        if recv_start {
                            info!("{}: Start signal received", self.id);
                            break;
                        }
                    } else {
                        panic!("Daemon disconnected during initialization.");
                    }
                },
            }
            self.send_outgoing_msgs().await;
        }
    }

    async fn handle_ipc_messages(
        &mut self,
        messages: Vec<Result<DaemonToServer, std::io::Error>>,
    ) -> bool {
        let mut received_start_signal = false;
        for msg_res in messages {
            match msg_res {
                Ok(DaemonToServer::Cluster(from, bytes)) => {
                    trace!("{}: Received cluster msg from {}", self.id, from);
                    match serde_json::from_slice::<Message<Command>>(&bytes) {
                        Ok(omnipaxos_msg) => {
                            self.omnipaxos.handle_incoming(omnipaxos_msg);
                        }
                        Err(e) => {
                            error!("Server failed to deserialize JSON OmniPaxos payload: {}", e);
                        }
                    }
                    self.handle_decided_entries().await;
                }
                Ok(DaemonToServer::Client(from, FromClient::Command(command_id, kv_command))) => {
                    let command = Command {
                        client_id: from,
                        coordinator_id: self.id,
                        id: command_id,
                        kv_cmd: kv_command,
                    };
                    // Time the append execution
                    let start_append = std::time::Instant::now();
                    self.omnipaxos.append(command);
                    let duration = start_append.elapsed();

                    self.append_count += 1;
                    self.append_total_time_ns += duration.as_nanos() as u64;
                }
                Ok(DaemonToServer::StartExperiment(start_time)) => {
                    received_start_signal = true;
                    self.start_time = Some(start_time);
                    #[cfg(feature = "adaptive")]
                    if let Some(first_window) = self.config.calibration_schedule.first() {
                        self.calibration_time =
                            Some(start_time + first_window.start_delay_ms as i64);
                    }
                }
                Err(e) => {
                    error!("Error decoding IPC message from daemon: {:?}", e);
                }
            }
        }
        received_start_signal
    }

    async fn handle_decided_entries(&mut self) {
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
                    LogEntry::Decided(cmd, accept_status) => Some((cmd, accept_status)),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands).await;
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<(Command, AcceptStatus)>) {
        for (command, accept_status) in commands {
            let read = self.database.handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ToClient::Read(command.id, read_result, accept_status),
                    None => ToClient::Write(command.id, accept_status),
                };

                let out_msg = ServerToDaemon::Client(command.client_id, response);
                if let Err(e) = self.ipc_tx.feed(out_msg).await {
                    error!("Failed to queue client response to daemon: {}", e);
                }
            }
        }
        let _ = self.ipc_tx.flush().await;
    }

    async fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);

        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let msg_bytes = serde_json::to_vec(&msg)
                .expect("Failed to serialize OmniPaxos message to JSON bytes");

            trace!("{}: Sending cluster msg to {}", self.id, to);
            let out_msg = ServerToDaemon::Cluster(to, msg_bytes);

            if let Err(e) = self.ipc_tx.feed(out_msg).await {
                error!("Failed to queue cluster message to daemon: {}", e);
            }
        }
        if let Err(e) = self.ipc_tx.flush().await {
            error!("Failed to flush messages to daemon: {}", e);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}
