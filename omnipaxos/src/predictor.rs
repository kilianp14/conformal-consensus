#[cfg(feature = "logging")]
use crate::utils::create_logger;
use crate::{
    utils::{Mode, NodeId},
    OmniPaxosConfig,
};
#[cfg(feature = "logging")]
use slog::{error, warn, Logger};

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `mode`: Operating mode of sequence_paxos (OmniPaxos or FastPaxos)
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct ConformalModePredictorConfig {
    pid: NodeId,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    custom_logger: Option<Logger>,
}

impl From<OmniPaxosConfig> for ConformalModePredictorConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        ConformalModePredictorConfig {
            pid: config.server_config.pid,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,
        }
    }
}

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

fn model(x: Features) -> Vec<(Label, f64)> {
    let successful_pred = 1.0
        / (1.0
            + (2.0 * x.fast_quorum_latency_in_s * x.other_nodes_proposals_per_s).powf(4.0 / 3.0));
    vec![
        (Label::Success, successful_pred),
        (Label::NoSuccess, 1.0 - successful_pred),
    ]
}

pub(crate) struct ConformalModePredictor {
    model: fn(Features) -> Vec<(Label, f64)>, // Softmax/ Sigmoid
    lambda_hat: Option<f64>,                  // Bounded between 0 and 1
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl ConformalModePredictor {
    pub fn with(config: ConformalModePredictorConfig) -> Self {
        Self {
            model,
            lambda_hat: None,
            #[cfg(feature = "logging")]
            logger: {
                if let Some(logger) = config.custom_logger {
                    logger
                } else {
                    let s = config
                        .logger_file_path
                        .unwrap_or_else(|| format!("logs/paxos_{}.log", config.pid));
                    create_logger(s.as_str())
                }
            },
        }
    }

    pub fn calibrate(&mut self, calibration_set: &[(Features, Label)], alpha: f64) {
        let n_calibration = calibration_set.len() as f64;
        if n_calibration <= 0.0 {
            #[cfg(feature = "logging")]
            warn!(
                self.logger,
                "Calibration data empty. No calibration performed"
            );
            return;
        }
        let lambda_threshold = |lambda: f64| {
            self.empirical_risk(calibration_set, lambda)
                - ((n_calibration + 1.0) / n_calibration * alpha - 1.0 / n_calibration)
        };
        self.lambda_hat = brentq(lambda_threshold, 0.0, 1.0, 1e-12);
        if self.lambda_hat == None {
            #[cfg(feature = "logging")]
            error!(self.logger, "No lambda found. Calibration unsuccessful");
        }
    }

    pub fn get_new_mode(&self, features: Features) -> Mode {
        match self.lambda_hat {
            Some(l_hat) => {
                let labels = self.get_prediction_set_with_lambda(features, l_hat);
                if labels.contains(&Label::NoSuccess) {
                    Mode::OmniPaxos
                } else {
                    Mode::FastPaxos
                }
            }
            None => Mode::FastPaxos, // Calibration phase -> always try fast path
        }
    }
    fn empirical_risk(&self, calibration_set: &[(Features, Label)], lambda: f64) -> f64 {
        let mut total_loss = 0.0;
        for (features, true_label) in calibration_set {
            let prediction_set = self.get_prediction_set_with_lambda(features.clone(), lambda);
            // Bad case is when fast path does not succeed but this failure is not predicted
            total_loss +=
                if true_label == &Label::NoSuccess && !prediction_set.contains(&Label::NoSuccess) {
                    1.0
                } else {
                    0.0
                }
        }
        total_loss / calibration_set.len() as f64
    }

    fn get_prediction_set_with_lambda(&self, features: Features, lambda: f64) -> Vec<Label> {
        (self.model)(features)
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
}

fn brentq<F>(f: F, mut a: f64, mut b: f64, tol: f64) -> Option<f64>
where
    F: Fn(f64) -> f64,
{
    let mut fa = f(a);
    let mut fb = f(b);

    if fa == 0.0 {
        return Some(a);
    }
    if fb == 0.0 {
        return Some(b);
    }

    // Root must be bracketed
    if fa * fb > 0.0 {
        return None;
    }

    // Ensure |fa| >= |fb|
    if fa.abs() < fb.abs() {
        std::mem::swap(&mut a, &mut b);
        std::mem::swap(&mut fa, &mut fb);
    }

    let mut c = a;
    let mut fc = fa;

    let mut d = b - a;
    let mut e = d;

    loop {
        if fb.abs() < fc.abs() {
            a = b;
            b = c;
            c = a;

            fa = fb;
            fb = fc;
            fc = fa;
        }

        let tol_act = 2.0 * f64::EPSILON * b.abs() + tol / 2.0;
        let m = 0.5 * (c - b);

        // Convergence check
        if fb == 0.0 || m.abs() <= tol_act {
            return Some(b);
        }

        if e.abs() >= tol_act && fa.abs() > fb.abs() {
            // Attempt interpolation
            let s = fb / fa;

            let (p, q) = if a == c {
                // Secant method
                (2.0 * m * s, 1.0 - s)
            } else {
                // Inverse quadratic interpolation
                let q_ = fa / fc;
                let r = fb / fc;
                (
                    s * (2.0 * m * q_ * (q_ - r) - (b - a) * (r - 1.0)),
                    (q_ - 1.0) * (r - 1.0) * (s - 1.0),
                )
            };

            let mut p = p;
            let mut q = q;

            if p > 0.0 {
                q = -q;
            } else {
                p = -p;
            }

            if (2.0 * p) < (3.0 * m * q - (tol_act * q).abs()) && p < (0.5 * e * q).abs() {
                // Accept interpolation
                e = d;
                d = p / q;
            } else {
                // Fall back to bisection
                d = m;
                e = m;
            }
        } else {
            // Bisection
            d = m;
            e = m;
        }

        a = b;
        fa = fb;

        if d.abs() > tol_act {
            b += d;
        } else {
            b += if m > 0.0 { tol_act } else { -tol_act };
        }

        fb = f(b);

        // Maintain bracketing
        if (fb > 0.0 && fc > 0.0) || (fb < 0.0 && fc < 0.0) {
            c = a;
            fc = fa;
            d = b - a;
            e = d;
        }
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;

    // Helper for floating point comparison
    fn assert_nearly_equal(a: f64, b: f64, tol: f64) {
        assert!(
            (a - b).abs() <= tol,
            "Value {} is not close enough to {}",
            a,
            b
        );
    }

    #[test]
    fn test_linear_function() {
        // x - 1 = 0 => root is 1.0
        let f = |x: f64| x - 1.0;
        match brentq(f, 0.0, 2.0, 1e-12) {
            Some(root) => assert_nearly_equal(root, 1.0, 1e-12),
            None => panic!("No root determined"),
        }
    }

    #[test]
    fn test_quadratic_function() {
        // x^2 - 2 = 0 => root is sqrt(2)
        let f = |x: f64| x * x - 2.0;
        match brentq(f, 0.0, 2.0, 1e-12) {
            Some(root) => assert_nearly_equal(root, 2.0f64.sqrt(), 1e-12),
            None => panic!("No root determined"),
        }
    }

    #[test]
    fn test_transcendental_function() {
        // sin(x) = 0 around pi
        let f = |x: f64| x.sin();
        match brentq(f, 3.0, 4.0, 1e-12) {
            Some(root) => assert_nearly_equal(root, std::f64::consts::PI, 1e-12),
            None => panic!("No root determined"),
        }
    }

    #[test]
    fn test_invalid_bracket() {
        let f = |x: f64| x * x + 1.0; // Never crosses zero
        assert_eq!(None, brentq(f, -1.0, 1.0, 1e-12));
    }

    #[test]
    fn test_root_at_boundary() {
        let f = |x: f64| x - 5.0;
        // The root is exactly at the upper bound 'b'
        match brentq(f, 0.0, 5.0, 1e-12) {
            Some(root) => assert_nearly_equal(root, 5.0, 1e-12),
            None => panic!("No root determined"),
        }
    }
}
