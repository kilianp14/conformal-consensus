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

pub(crate) struct ConformalModePredictor {
    model: fn(&Features) -> Vec<(Label, f64)>, // Softmax/ Sigmoid
    lambda_hat: Option<f64>,                   // Bounded between 0 and 1
    significance_level: f64,
}

impl ConformalModePredictor {
    pub fn new(significance_level: f64) -> Self {
        Self {
            model,
            lambda_hat: None,
            significance_level,
        }
    }

    pub fn calibrate(
        &mut self,
        calibration_set: &[(Features, Label)],
    ) -> Result<(f64, f64), String> {
        let n_calibration = calibration_set.len() as f64;
        if n_calibration <= 0.0 {
            return Err("Calibration data empty. No calibration performed".to_string());
        }
        let lambda_threshold = |lambda: f64| {
            self.empirical_risk(calibration_set, lambda) - self.significance_level
                + (1.0 - self.significance_level) / n_calibration
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
        let lambda = match self.lambda_hat {
            Some(l_hat) => l_hat,
            None => 1.0 - self.significance_level, // Not yet calibrated -> use plain model
        };
        let labels = self.get_prediction_set_with_lambda(features, lambda);
        if labels.contains(&Label::NoSuccess) {
            Mode::OmniPaxos
        } else {
            Mode::FastPaxos
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_crc_empirical_error_rates() {
        let alpha_1 = 0.10;
        let n_cal_1 = 2_000;
        let n_test_1 = 10_000;
        let tries_1 = 40;
        crc_empirical_error_rate(alpha_1, n_cal_1, n_test_1, tries_1);

        let alpha_2 = 0.05;
        let n_cal_2 = 5_000;
        let n_test_2 = 20_000;
        let tries_2 = 30;
        crc_empirical_error_rate(alpha_2, n_cal_2, n_test_2, tries_2);

        let alpha_3 = 0.20;
        let n_cal_3 = 1_000;
        let n_test_3 = 5_000;
        let tries_3 = 50;
        crc_empirical_error_rate(alpha_3, n_cal_3, n_test_3, tries_3);
    }

    fn crc_empirical_error_rate(alpha: f64, n_calibration: i32, n_test: i32, num_tries: i32) {
        let mut rng = rand::thread_rng();

        let se_single =
            (alpha * (1.0 - alpha) * ((1.0 / n_calibration as f64) + (1.0 / n_test as f64))).sqrt();
        let single_try_margin = 5.0 * se_single; // 5-sigma bound for individual iterations

        let se_avg = se_single / (num_tries as f64).sqrt();
        let global_avg_margin = 3.0 * se_avg; // 3-sigma bound for master average

        let mut total_test_error_rate = 0.0;

        for try_idx in 1..=num_tries {
            let mut predictor = ConformalModePredictor::new(alpha);

            let mut generate_data_point = || -> (Features, Label) {
                let features = Features {
                    fast_quorum_latency_in_s: rng.gen_range(0.001..0.005),
                    other_nodes_proposals_per_s: rng.gen_range(100.0..1000.0),
                };

                let true_label = if rng.gen_bool(0.5) {
                    Label::Success
                } else {
                    Label::NoSuccess
                };

                (features, true_label)
            };

            let calibration_set: Vec<(Features, Label)> =
                (0..n_calibration).map(|_| generate_data_point()).collect();

            let (lambda_hat, _emp_risk) = predictor
                .calibrate(&calibration_set)
                .expect("Calibration failed to find a valid lambda");

            let test_set: Vec<(Features, Label)> =
                (0..n_test).map(|_| generate_data_point()).collect();

            let mut test_errors = 0.0;
            for (features, true_label) in &test_set {
                let prediction_set = predictor.get_prediction_set_with_lambda(features, lambda_hat);
                if *true_label == Label::NoSuccess && !prediction_set.contains(&Label::NoSuccess) {
                    test_errors += 1.0;
                }
            }

            let test_error_rate = test_errors / n_test as f64;
            total_test_error_rate += test_error_rate;

            // 3. Individual Guardrail
            assert!(
                test_error_rate <= alpha + single_try_margin,
                "Try #{} failed: individual error rate {} exceeded dynamic 5-sigma limit {}",
                try_idx,
                test_error_rate,
                alpha + single_try_margin
            );
        }

        let average_error_rate = total_test_error_rate / num_tries as f64;

        println!("\n=== ADAPTIVE MULTI-TRY CRC REPORT ===");
        println!("Total Tries (M):      {}", num_tries);
        println!("Calibration Size:     {}", n_calibration);
        println!("Test Size:            {}", n_test);
        println!("Target Alpha:         {}", alpha);
        println!("Calculated Single SE: {:.6}", se_single);
        println!("Calculated Avg SE:    {:.6}", se_avg);
        println!("-------------------------------------");
        println!("Single-Try Threshold: {:.4}", alpha + single_try_margin);
        println!("Global Avg Threshold: {:.4}", alpha + global_avg_margin);
        println!("Empirical Mean Risk:  {:.4}", average_error_rate);
        println!("=====================================\n");

        assert!(
            average_error_rate <= alpha + global_avg_margin,
            "The average CRC error rate ({:.5}) exceeded the dynamic 3-sigma expectation bound ({:.5})",
            average_error_rate, alpha + global_avg_margin
        );
    }
}
