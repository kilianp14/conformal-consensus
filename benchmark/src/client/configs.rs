use std::{env, time::Duration};

use benchmark::common::{NodeId, Timestamp};
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub server_id: NodeId,
    pub server_address: String,
    pub load_pattern: LoadPattern,
    pub read_ratio: f64,
    pub max_duration_sec: u64,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum LoadPattern {
    /// A load pattern that oscillates between a high and low RPS.
    Cyclic {
        highest_rps: u64, // Peak requests per second
        lowest_rps: u64,  // Trough requests per second
        period_sec: u64,  // Duration of one full cycle
        offset_sec: u64,  // Time offset for the start of the pattern
    },
}

impl LoadPattern {
    /// Calculates the target RPS at a given point in time.
    pub fn get_rps(&self, elapsed: Duration) -> f64 {
        match self {
            LoadPattern::Cyclic {
                highest_rps,
                lowest_rps,
                period_sec,
                offset_sec,
            } => {
                let h = *highest_rps as f64;
                let l = *lowest_rps as f64;
                let period = *period_sec as f64;
                let offset = *offset_sec as f64;
                let t = elapsed.as_secs_f64();

                let avg = (h + l) / 2.0;
                let amp = (h - l) / 2.0;

                // RPS(t) = avg + amplitude * sin(2*PI * (t + offset) / period)
                avg + amp * (2.0 * std::f64::consts::PI * (t + offset) / period).sin()
            }
        }
    }
}

impl ClientConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let config_file = match env::var("CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&config_file))
            .add_source(Environment::with_prefix("OMNIPAXOS").try_parsing(true))
            .build()?;
        config.try_deserialize()
    }
}
