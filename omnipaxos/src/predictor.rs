use crate::utils::Mode;
use std::{
    collections::HashSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// To change mode dynamically during runtime
pub trait ModeSetter: Send {
    /// Returns the new mode to be set
    fn get_new_mode(&mut self) -> Mode;
}

/// Always use conservative path
pub struct AlwaysOmniPaxosMode {}

impl ModeSetter for AlwaysOmniPaxosMode {
    fn get_new_mode(&mut self) -> Mode {
        Mode::OmniPaxos
    }
}

/// Always use fast path
pub struct AlwaysFastPaxosMode {}

impl ModeSetter for AlwaysFastPaxosMode {
    fn get_new_mode(&mut self) -> Mode {
        Mode::FastPaxos
    }
}

/// For testing purposes (safety when nodes have different modes)
pub struct RandomModeSetter {}

impl ModeSetter for RandomModeSetter {
    fn get_new_mode(&mut self) -> Mode {
        // Pseudo-random using nano-seconds
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();

        if nanos.is_multiple_of(2) {
            Mode::OmniPaxos
        } else {
            Mode::FastPaxos
        }
    }
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct Inputs {
    leader_uptime: Duration,
    // For later
}

#[derive(Clone, Debug, Hash)]
pub(crate) enum Label {
    Collision,
    NoCollision,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct DataPoint {
    inputs: Inputs,
    label: Label,
}

pub(crate) struct ConformalModeSetter {
    score_function: fn(Inputs, Label) -> f64,
    lambda_hat: f64,
}

impl ConformalModeSetter {
    fn new(
        calibration_set: HashSet<DataPoint>,
        score_function: fn(Inputs, Label) -> f64,
        alpha: f64,
    ) -> Self {
        match alpha {
            0.0..1.0 => Self {
                score_function,
                lambda_hat: 0.0,
            },
            _ => panic!("Alpha should be between 0 and 1"),
        }
    }

    fn get_prediction_set(&self, inputs: Inputs) -> Vec<Label> {
        let mut labels = vec![];
        if (self.score_function)(inputs.clone(), Label::Collision) <= self.lambda_hat {
            labels.push(Label::Collision);
        }
        if (self.score_function)(inputs, Label::NoCollision) <= self.lambda_hat {
            labels.push(Label::NoCollision);
        }
        labels
    }

    fn false_negative_rate(&self, calibration_set: HashSet<DataPoint>) {
        unimplemented!()
    }
}
