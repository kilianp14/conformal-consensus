use std::env;

use benchmark::common::NodeId;
use config::{Config, ConfigError, Environment, File};
use omnipaxos::{
    ClusterConfig as OmnipaxosClusterConfig, OmniPaxosConfig,
    ServerConfig as OmnipaxosServerConfig, utils::Mode,
};
use serde::{Deserialize, Serialize};

#[cfg(feature = "adaptive")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CalibrationWindow {
    pub start_delay_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
    pub initial_leader: NodeId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalConfig {
    pub server_id: NodeId,
    pub listen_address: String,
    pub listen_port: u16,
    pub num_clients: usize,
    pub output_filepath: String,
    pub paxos_output_filepath: String,
    pub mode: Mode,
    pub enable_retry: bool,
    #[cfg(feature = "adaptive")]
    pub calibration_schedule: Vec<CalibrationWindow>,
    #[cfg(feature = "adaptive")]
    pub significance_level: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosKVConfig {
    #[serde(flatten)]
    pub local: LocalConfig,
    #[serde(flatten)]
    pub cluster: ClusterConfig,
}

impl From<OmniPaxosKVConfig> for OmniPaxosConfig {
    fn from(config: OmniPaxosKVConfig) -> Self {
        let cluster_config = OmnipaxosClusterConfig {
            nodes: config.cluster.nodes,
        };
        let server_config = OmnipaxosServerConfig {
            pid: config.local.server_id,
            mode: config.local.mode,
            #[cfg(feature = "adaptive")]
            significance_level: config.local.significance_level,
            logger_file_path: Some(config.local.paxos_output_filepath),
            enable_retry: config.local.enable_retry,
            ..Default::default()
        };
        Self {
            cluster_config,
            server_config,
        }
    }
}

impl OmniPaxosKVConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("SERVER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires SERVER_CONFIG_FILE environment variable to be set"),
        };
        let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .add_source(File::with_name(&cluster_config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(
                Environment::with_prefix("OMNIPAXOS")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        config.try_deserialize()
    }

    pub fn get_peers(&self, node: NodeId) -> Vec<NodeId> {
        self.cluster
            .nodes
            .iter()
            .cloned()
            .filter(|&id| id != node)
            .collect()
    }
}
