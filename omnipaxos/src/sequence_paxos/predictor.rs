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
            + (2.0 * x.fast_quorum_latency_in_s * x.other_nodes_proposals_per_s).powf(4.0 / 3.0));
    vec![
        (Label::Success, successful_pred),
        (Label::NoSuccess, 1.0 - successful_pred),
    ]
}

pub(crate) struct ConformalModePredictor {
    model: fn(&Features) -> Vec<(Label, f64)>, // Softmax/ Sigmoid
    lambda_hat: Option<f64>,                   // Bounded between 0 and 1
}

impl ConformalModePredictor {
    pub fn new() -> Self {
        Self {
            model,
            lambda_hat: None,
        }
    }

    pub fn calibrate(
        &mut self,
        calibration_set: &[(Features, Label)],
        alpha: f64,
    ) -> Result<(f64, f64), String> {
        let n_calibration = calibration_set.len() as f64;
        if n_calibration <= 0.0 {
            return Err("Calibration data empty. No calibration performed".to_string());
        }
        let lambda_threshold = |lambda: f64| {
            self.empirical_risk(calibration_set, lambda)
                - ((n_calibration + 1.0) / n_calibration * alpha - 1.0 / n_calibration)
        };
        match find_root(lambda_threshold, 0.0, 1.0) {
            Some(l) => {
                self.lambda_hat = Some(l);
                Ok((l, self.empirical_risk(calibration_set, l)))
            }
            None => Err(
                "No valid lambda found within bounds [0, 1]. Calibration unsuccessful".to_string(),
            ),
        }
    }

    pub fn get_new_mode(&self, features: &Features) -> Mode {
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
            let prediction_set = self.get_prediction_set_with_lambda(features, lambda);
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

    fn get_prediction_set_with_lambda(&self, features: &Features, lambda: f64) -> Vec<Label> {
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
