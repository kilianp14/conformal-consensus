#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    FastPaxos,
    OmniPaxos,
}

pub(crate) struct ModeChanger {
    pub current_mode: Mode,
}

impl ModeChanger {
    pub fn update_mode(&mut self) {
        self.current_mode = Mode::OmniPaxos
    }
}
