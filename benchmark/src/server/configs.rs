use benchmark::common::NodeId;
use config::{Config, ConfigError, File};
use omnipaxos::{
    ClusterConfig as OmnipaxosClusterConfig, OmniPaxosConfig,
    ServerConfig as OmnipaxosServerConfig, utils::Mode,
};
use serde::{Deserialize, Serialize};
use std::env;

#[cfg(feature = "adaptive")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CalibrationWindow {
    pub start_delay_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub nodes: Vec<NodeId>,
    pub initial_leader: NodeId,
    pub server_id: NodeId,
    pub num_clients: usize,
    pub output_filepath: String,
    pub paxos_output_filepath: String,
    pub mode: Mode,
    pub enable_retry: bool,
    #[cfg(feature = "adaptive")]
    pub calibration_schedule: Vec<CalibrationWindow>,
    #[cfg(feature = "adaptive")]
    pub risk_level: f64,
    #[cfg(feature = "adaptive")]
    pub learning_rate: f64,
}

impl From<ServerConfig> for OmniPaxosConfig {
    fn from(config: ServerConfig) -> Self {
        let cluster_config = OmnipaxosClusterConfig {
            nodes: config.nodes,
        };
        let server_config = OmnipaxosServerConfig {
            pid: config.server_id,
            mode: config.mode,
            #[cfg(feature = "adaptive")]
            risk_level: config.risk_level,
            #[cfg(feature = "adaptive")]
            learning_rate: config.learning_rate,
            logger_file_path: Some(config.paxos_output_filepath),
            enable_retry: config.enable_retry,
            ..Default::default()
        };
        Self {
            cluster_config,
            server_config,
        }
    }
}

impl ServerConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let server_config_file = env::var("SERVER_CONFIG_FILE")
            .expect("Requires SERVER_CONFIG_FILE environment variable to be set");
        let node_id = env::var("NODE_ID").expect("Requires NODE_ID environment variable to be set");
        let config = Config::builder()
            .add_source(File::with_name(&server_config_file))
            .set_override("server_id", node_id)?
            .build()?;

        config.try_deserialize()
    }

    pub fn get_peers(&self, node: NodeId) -> Vec<NodeId> {
        self.nodes
            .iter()
            .cloned()
            .filter(|&id| id != node)
            .collect()
    }
}
