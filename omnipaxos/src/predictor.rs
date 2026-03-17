use crate::utils::Mode;
use std::{
    collections::{HashMap, HashSet},
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

#[derive(Clone, Debug)]
pub(crate) struct Features {
    leader_uptime: Duration,
    // For later
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Label {
    Collision,
    NoCollision,
}

pub(crate) struct ConformalModeSetter {
    model: fn(Features) -> HashMap<Label, f64>, // Has to have softmax
    lambda_hat: f64,                            // Bounded between 0 and 1
}

impl ConformalModeSetter {
    pub fn new(
        model: fn(Features) -> HashMap<Label, f64>,
        calibration_set: Vec<(Features, Label)>,
        loss_function: fn(HashSet<Label>, Label) -> f64,
        alpha: f64,
    ) -> Self {
        let n_calibration = calibration_set.len() as f64;
        if n_calibration <= 0.0 {
            panic!("Calibration data empty");
        }
        let lambda_threshold = |lambda: f64| {
            empirical_risk(model, &calibration_set, loss_function, lambda)
                - ((n_calibration + 1.0) / n_calibration * alpha - 1.0 / n_calibration)
        };
        let lambda_hat = brentq(lambda_threshold, 0.0, 1.0, 1e-12);
        Self { model, lambda_hat }
    }

    pub fn get_new_mode(&self, features: Features) -> Mode {
        let labels = get_prediction_set_with_lambda(self.model, features, self.lambda_hat);
        if labels.contains(&Label::NoCollision) {
            Mode::FastPaxos
        } else {
            Mode::OmniPaxos
        }
    }
}

fn empirical_risk(
    model: fn(Features) -> HashMap<Label, f64>,
    calibration_set: &[(Features, Label)],
    loss_function: fn(HashSet<Label>, Label) -> f64,
    lambda: f64,
) -> f64 {
    let mut total_loss = 0.0;
    for (features, true_label) in calibration_set {
        let prediction_set = get_prediction_set_with_lambda(model, features.clone(), lambda);
        let loss = loss_function(prediction_set, true_label.clone());
        total_loss += loss;
    }
    total_loss / calibration_set.len() as f64
}

fn get_prediction_set_with_lambda(
    model: fn(Features) -> HashMap<Label, f64>,
    features: Features,
    lambda: f64,
) -> HashSet<Label> {
    (model)(features)
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

fn brentq<F>(f: F, mut a: f64, mut b: f64, tol: f64) -> f64
where
    F: Fn(f64) -> f64,
{
    let mut fa = f(a);
    let mut fb = f(b);

    if fa == 0.0 {
        return a;
    }
    if fb == 0.0 {
        return b;
    }

    // Root must be bracketed
    if fa * fb > 0.0 {
        panic!("Root is not bracketed: f(a) and f(b) must have opposite signs.");
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
            return b;
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
        let f = |x: f64| x - 1.0;
        let root = brentq(f, 0.0, 2.0, 1e-7);
        assert_nearly_equal(root, 1.0, 1e-7);
    }

    #[test]
    fn test_quadratic_function() {
        // x^2 - 2 = 0 => root is sqrt(2)
        let f = |x: f64| x * x - 2.0;
        let root = brentq(f, 0.0, 2.0, 1e-12);
        assert_nearly_equal(root, 2.0f64.sqrt(), 1e-12);
    }

    #[test]
    fn test_transcendental_function() {
        // sin(x) = 0 around pi
        let f = |x: f64| x.sin();
        let root = brentq(f, 3.0, 4.0, 1e-12);
        assert_nearly_equal(root, std::f64::consts::PI, 1e-12);
    }

    #[test]
    #[should_panic(expected = "Root is not bracketed")]
    fn test_invalid_bracket_panics() {
        let f = |x: f64| x * x + 1.0; // Never crosses zero
        brentq(f, -1.0, 1.0, 1e-12);
    }

    #[test]
    fn test_root_at_boundary() {
        let f = |x: f64| x - 5.0;
        // The root is exactly at the upper bound 'b'
        let root = brentq(f, 0.0, 5.0, 1e-12);
        assert_nearly_equal(root, 5.0, 1e-12);
    }
}
