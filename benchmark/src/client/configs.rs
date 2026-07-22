use benchmark::common::{NodeId, Timestamp};
use config::{Config, ConfigError, File};
use serde::{Deserialize, Serialize};
use std::{env, time::Duration};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub server_id: NodeId,
    pub server_address: String,
    pub load_pattern: LoadPattern,
    pub read_ratio: f64,
    pub max_duration_sec: u64,
    pub seed: u64,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum LoadPattern {
    /// A load pattern where bursts arrive at random intervals (normally distributed) with exponential decay.
    RandomBursts {
        base_rps: f64,             // Baseline RPS when no burst is active
        burst_rps: f64,            // Peak RPS added during a burst
        avg_interval_sec: f64,     // Mean time between successive bursts
        interval_std_dev_sec: f64, // Standard deviation of time between bursts
        decay_rate: f64,           // Exponential decay coefficient (lambda)
        jitter: f64,               // Random jitter factor for the peak height of each burst
    },
}

impl LoadPattern {
    pub fn into_runner(self) -> LoadPatternRunner {
        LoadPatternRunner::new(self)
    }
}

#[derive(Debug, Clone)]
enum RunnerState {
    RandomBursts {
        initialized: bool,
        last_burst_time: f64,
        next_burst_time: f64,
        current_burst_jitter: f64,
    },
}

pub struct LoadPatternRunner {
    pattern: LoadPattern,
    state: RunnerState,
}

impl LoadPatternRunner {
    pub fn new(pattern: LoadPattern) -> Self {
        let state = match &pattern {
            LoadPattern::RandomBursts { .. } => RunnerState::RandomBursts {
                initialized: false,
                last_burst_time: 0.0,
                next_burst_time: 0.0,
                current_burst_jitter: 1.0,
            },
        };
        Self { pattern, state }
    }

    /// Computes the precise duration to wait until generating the next request.
    pub fn next_delay(&mut self, elapsed: Duration, rng: &mut impl rand::RngExt) -> Duration {
        match &self.pattern {
            LoadPattern::RandomBursts {
                base_rps,
                burst_rps,
                avg_interval_sec,
                interval_std_dev_sec,
                decay_rate,
                jitter,
            } => {
                let t = elapsed.as_secs_f64();

                let RunnerState::RandomBursts {
                    initialized,
                    last_burst_time,
                    next_burst_time,
                    current_burst_jitter,
                } = &mut self.state;
                // Initialization of the stochastic burst schedule
                if !*initialized {
                    let first_interval =
                        sample_normal(*avg_interval_sec, *interval_std_dev_sec, rng).max(0.01);
                    let random_fraction = rng.random::<f64>();

                    *last_burst_time = -(first_interval * random_fraction);
                    *next_burst_time = *last_burst_time + first_interval;
                    *current_burst_jitter = 1.0 + (rng.random::<f64>() * 2.0 - 1.0) * jitter;
                    *initialized = true;
                }

                // Advance bursts if the elapsed time passes the scheduled time
                while t >= *next_burst_time {
                    *last_burst_time = *next_burst_time;
                    *current_burst_jitter = 1.0 + (rng.random::<f64>() * 2.0 - 1.0) * jitter;
                    let next_interval =
                        sample_normal(*avg_interval_sec, *interval_std_dev_sec, rng).max(0.01);
                    *next_burst_time += next_interval;
                }

                let time_since_burst = t - *last_burst_time;
                let current_burst =
                    burst_rps * (*current_burst_jitter) * (-decay_rate * time_since_burst).exp();

                let rps = (base_rps + current_burst).max(0.001);
                let delay = (1.0 / rps).min(*next_burst_time - t);
                Duration::from_secs_f64(delay)
            }
        }
    }
}

/// Helper using the Box-Muller transform to sample from a Normal Distribution.
fn sample_normal(mean: f64, std_dev: f64, rng: &mut impl rand::RngExt) -> f64 {
    let u1: f64 = loop {
        let u = rng.random::<f64>();
        if u > 0.0 {
            break u;
        }
    };
    let u2: f64 = rng.random::<f64>();
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    mean + std_dev * z
}

impl ClientConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let config_file =
            env::var("CONFIG_FILE").expect("Requires CONFIG_FILE environment variable to be set");
        let node_id = env::var("NODE_ID").expect("Requires NODE_ID environment variable to be set");
        let config = Config::builder()
            .add_source(File::with_name(&config_file))
            .set_override("server_id", node_id)?
            .build()?;

        config.try_deserialize()
    }
}
