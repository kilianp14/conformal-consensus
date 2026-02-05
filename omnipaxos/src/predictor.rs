use crate::utils::Mode;

pub(crate) struct ConformalModePredictor {}

impl ConformalModePredictor {
    pub fn get_new_mode(&mut self) -> Mode {
        Mode::OmniPaxos
    }
}
