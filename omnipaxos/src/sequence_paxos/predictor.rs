#[cfg(feature = "logging")]
use slog::Logger;

use crate::utils::Mode;

#[derive(Clone, Debug)]
pub(crate) struct Features {
    pub(crate) fast_quorum_latency_in_s: f64,
    pub(crate) other_nodes_proposals_per_s: f64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Label {
    Success,
    NoSuccess,
}

fn model(x: &Features) -> Vec<(Label, f64)> {
    let successful_pred = 1.0
        / (1.0
            + (2.0 * x.fast_quorum_latency_in_s * x.other_nodes_proposals_per_s).powf(5.0 / 4.0));
    vec![
        (Label::Success, successful_pred),
        (Label::NoSuccess, 1.0 - successful_pred),
    ]
}

fn get_prediction_set_with_lambda(features: &Features, lambda: f64) -> Vec<Label> {
    model(features)
        .into_iter()
        .filter_map(|(label, score)| {
            if score >= 1.0 - lambda {
                Some(label)
            } else {
                None
            }
        })
        .collect()
}

fn loss(prediction_set: &[Label], true_label: &Label) -> u32 {
    // Bad case is when fast path does not succeed but this failure is not predicted
    if true_label == &Label::NoSuccess && !prediction_set.contains(&Label::NoSuccess) {
        1
    } else {
        0
    }
}

pub(crate) struct ConformalModePredictor {
    lambda_hat: Option<f64>,
    risk_level: f64,
    learning_rate: f64,
    calibration_set: Vec<(Features, Label)>,
    collecting: bool,
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl ConformalModePredictor {
    pub fn new(
        risk_level: f64,
        learning_rate: f64,
        #[cfg(feature = "logging")] logger: Logger,
    ) -> Self {
        Self {
            lambda_hat: None,
            risk_level,
            learning_rate,
            calibration_set: Vec::with_capacity(10000),
            collecting: false,
            #[cfg(feature = "logging")]
            logger,
        }
    }

    pub fn add_data_point(&mut self, features: Features, label: Label) {
        if self.collecting {
            // Offline calibration phase
            self.calibration_set.push((features, label));
        } else if let Some(current_lambda) = self.lambda_hat {
            // Online risk control
            let prediction_set = get_prediction_set_with_lambda(&features, current_lambda);
            let realized_loss = loss(&prediction_set, &label) as f64;
            let error_signal = realized_loss - self.risk_level;
            self.lambda_hat = Some(current_lambda + self.learning_rate * error_signal);
        }
    }

    pub fn start_data_collection(&mut self) {
        self.collecting = true;
    }

    // Offline calibration with a batch calibration set
    pub fn calibrate(&mut self) {
        let n_calibration = self.calibration_set.len() as f64;
        if n_calibration <= 0.0 {
            #[cfg(feature = "logging")]
            slog::warn!(self.logger, "Calibration failed: Calibration data empty.");
            return;
        }
        let lambda_threshold = |lambda: f64| {
            self.empirical_risk(lambda) - self.risk_level + (1.0 - self.risk_level) / n_calibration
        };
        match find_root(lambda_threshold, 0.0, 1.0) {
            Some(l_hat) => {
                #[cfg(feature = "logging")]
                {
                    let emp_risk = self.empirical_risk(l_hat);
                    slog::info!(
                        self.logger,
                        "Batch Offline Calibration successful! Size: {}. Initialized Lambda: {}. Empirical risk: {}.",
                        n_calibration,
                        l_hat,
                        emp_risk
                    );
                }
                self.lambda_hat = Some(l_hat);
                self.collecting = false;
                self.calibration_set.clear();
            }
            None => {
                #[cfg(feature = "logging")]
                slog::warn!(
                    self.logger,
                    "Offline Calibration failed: No valid lambda found within bounds [0, 1]."
                );
            }
        }
    }

    pub fn get_new_mode(&self, features: &Features) -> Mode {
        let lambda = match self.lambda_hat {
            Some(l_hat) => l_hat,
            None => 1.0 - self.risk_level, // Not yet calibrated -> use plain model
        };
        let labels = get_prediction_set_with_lambda(features, lambda);
        if labels.contains(&Label::NoSuccess) {
            Mode::OmniPaxos
        } else {
            Mode::FastPaxos
        }
    }

    fn empirical_risk(&self, lambda: f64) -> f64 {
        let mut total_loss = 0;
        for (features, true_label) in self.calibration_set.iter() {
            let prediction_set = get_prediction_set_with_lambda(features, lambda);
            total_loss += loss(&prediction_set, true_label);
        }
        total_loss as f64 / self.calibration_set.len() as f64
    }
}

fn find_root<F>(f: F, left: f64, right: f64) -> Option<f64>
where
    F: Fn(f64) -> f64,
{
    if f(right) > 0.0 {
        return None;
    }
    if f(left) <= 0.0 {
        return Some(left);
    }

    let mut low = left;
    let mut high = right;

    for _ in 0..80 {
        let mid = low + (high - low) / 2.0;

        if f(mid) > 0.0 {
            low = mid;
        } else {
            high = mid;
        }
    }
    Some(high)
}
