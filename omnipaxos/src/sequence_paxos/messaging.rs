use crate::sequence_paxos::*;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Connectivity {
    pub directly_connected: bool,
    pub qc: bool,
}

impl Connectivity {
    pub fn with(directly_connected: bool, qc: bool) -> Self {
        Self {
            directly_connected,
            qc,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PeerConnectivity {
    connectivity: Vec<Connectivity>,
    quorum: Quorum,
}
impl PeerConnectivity {
    pub fn new(max_pid: NodeId, quorum: Quorum) -> Self {
        let size = max_pid as usize;
        Self {
            connectivity: vec![Connectivity::with(true, true); size],
            quorum,
        }
    }

    pub fn get(&self, pid: NodeId) -> &Connectivity {
        &self.connectivity[pid_to_idx(pid)]
    }

    pub fn get_mut(&mut self, pid: NodeId) -> &mut Connectivity {
        &mut self.connectivity[pid_to_idx(pid)]
    }

    pub fn set_connectivity(&mut self, pid: NodeId, connectivity: Connectivity) {
        self.connectivity[pid_to_idx(pid)] = connectivity;
    }

    pub fn is_qc(&self) -> bool {
        let num_connected = self
            .connectivity
            .iter()
            .filter(|c| c.directly_connected)
            .count();
        self.quorum.is_prepare_quorum(num_connected)
    }

    pub fn get_num_qc(&self) -> usize {
        let qc_peers = self.connectivity.iter().filter(|c| c.qc).count();
        if self.is_qc() {
            qc_peers + 1
        } else {
            qc_peers
        }
    }

    pub fn is_connected_to(&self, pid: NodeId) -> bool {
        self.connectivity[pid_to_idx(pid)].directly_connected
    }

    pub fn get_direct_connections(&self) -> Vec<NodeId> {
        self.connectivity
            .iter()
            .enumerate()
            .filter_map(|(idx, c)| {
                if c.directly_connected {
                    Some(idx as NodeId + 1)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn clear(&mut self) {
        for c in self.connectivity.iter_mut() {
            *c = Connectivity::with(false, false);
        }
    }
}
impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) fn send_msg_to(&mut self, pid: NodeId, msg: PaxosMsg<T>) {
        if self.peer_connectivity.is_connected_to(pid) {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: pid,
                msg,
            });
        }
    }

    pub(crate) fn send_to_all_peers(&mut self, msg: PaxosMsg<T>) {
        let direct_connections = self
            .peer_connectivity
            .get_direct_connections()
            .iter()
            .filter(|x| **x != self.pid)
            .cloned()
            .collect::<Vec<_>>();
        for pid in direct_connections {
            let m = PaxosMessage {
                from: self.pid,
                to: pid,
                msg: msg.clone(),
            };
            self.outgoing.push(m);
        }
    }

    /// Handle an incoming message.
    pub(crate) fn handle(&mut self, m: PaxosMessage<T>) {
        match m.msg {
            PaxosMsg::PrepareReq(prepreq) => self.handle_preparereq(prepreq, m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::NotAccepted(not_acc) => self.handle_notaccepted(not_acc, m.from),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            PaxosMsg::Compaction(c) => self.handle_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => self.handle_accept_stopsign(acc_ss),
            PaxosMsg::ForwardStopSign(f_ss) => self.handle_forwarded_stopsign(f_ss),
        }
    }
}

fn pid_to_idx(pid: NodeId) -> usize {
    pid as usize - 1
}

fn idx_to_pid(idx: usize) -> NodeId {
    idx as NodeId + 1
}
