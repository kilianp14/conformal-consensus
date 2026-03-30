use self::omnireplica::OmniPaxosComponent;
use kompact::{config_keys::system, executors::crossbeam_workstealing_pool, prelude::*};
use omnipaxos::{
    macros::*,
    messages::Message,
    utils::{Ballot, NodeId},
    ClusterConfig, OmniPaxosConfig, ServerConfig,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::{collections::HashMap, error::Error, fs, str, sync::Arc, time::Duration};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_millis(1000);
const REGISTRATION_TIMEOUT: Duration = Duration::from_millis(1000);
const STOP_COMPONENT_TIMEOUT: Duration = Duration::from_millis(1000);
const CHECK_DECIDED_TIMEOUT: Duration = Duration::from_millis(1);

/// Serde deserialize function to deserialize toml milliseconds u64s to std::time::Duration
fn deserialize_duration_millis<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Deserialize::deserialize(deserializer)?;
    Ok(Duration::from_millis(val))
}

pub fn create_proposals(from: u64, to: u64) -> Vec<Value> {
    (from..=to).map(Value::with_id).collect()
}

/// Configuration for `TestSystem`. TestConfig loads the values from
/// the configuration file `/tests/config/test.toml` using toml
#[derive(Deserialize, Clone, Copy)]
#[serde(default)]
pub struct TestConfig {
    pub num_threads: usize,
    pub num_nodes: usize,
    #[serde(rename(deserialize = "wait_timeout_ms"))]
    #[serde(deserialize_with = "deserialize_duration_millis")]
    pub wait_timeout: Duration,
    #[serde(rename(deserialize = "election_timeout_ms"))]
    #[serde(deserialize_with = "deserialize_duration_millis")]
    pub election_timeout: Duration,
    #[serde(rename(deserialize = "resend_message_timeout_ms"))]
    #[serde(deserialize_with = "deserialize_duration_millis")]
    pub resend_message_timeout: Duration,
    pub num_proposals: u64,
    pub num_elections: u64,
    pub trim_idx: usize,
    pub flexible_quorum: Option<(usize, usize)>,
}

impl TestConfig {
    pub fn load(name: &str) -> Result<TestConfig, Box<dyn Error>> {
        let config_file =
            fs::read_to_string("tests/config/test.toml").expect("Couldn't find config file.");
        let mut configs: HashMap<String, TestConfig> = toml::from_str(&config_file)?;
        let config = configs
            .remove(name)
            .unwrap_or_else(|| panic!("Couldnt find config for {}", name));
        Ok(config)
    }

    pub fn into_omnipaxos_config(&self, pid: NodeId) -> OmniPaxosConfig {
        let all_pids: Vec<NodeId> = (1..=self.num_nodes as NodeId).collect();
        let cluster_config = ClusterConfig { nodes: all_pids };
        let server_config = ServerConfig {
            pid,
            election_tick_timeout: 1,
            // Make tick timeouts relative to election timeout
            resend_message_tick_timeout: self.resend_message_timeout.as_millis() as u64
                / self.election_timeout.as_millis() as u64,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            num_threads: 3,
            num_nodes: 3,
            wait_timeout: Duration::from_millis(5000),
            election_timeout: Duration::from_millis(200),
            resend_message_timeout: Duration::from_millis(500),
            num_proposals: 100,
            num_elections: 0,
            trim_idx: 0,
            flexible_quorum: None,
        }
    }
}

pub struct TestSystem {
    pub temp_dir_path: String,
    pub kompact_system: Option<KompactSystem>,
    pub nodes: HashMap<NodeId, Arc<Component<OmniPaxosComponent>>>,
}

impl TestSystem {
    pub fn with(test_config: TestConfig) -> Self {
        let temp_dir_path = create_temp_dir();

        let mut conf = KompactConfig::default();
        conf.set_config_value(&system::LABEL, "KompactSystem".to_string());
        conf.set_config_value(&system::THREADS, test_config.num_threads);
        Self::set_executor_for_threads(test_config.num_threads, &mut conf);

        let mut net = NetworkConfig::default();
        net.set_tcp_nodelay(true);

        conf.system_components(DeadletterBox::new, net.build());
        let system = conf.build().expect("KompactSystem");

        let mut nodes = HashMap::new();
        let mut omni_refs: HashMap<NodeId, ActorRef<Message<Value>>> = HashMap::new();

        for pid in 1..=test_config.num_nodes as NodeId {
            let op_config = test_config.into_omnipaxos_config(pid);
            let (omni_replica, omni_reg_f) = system.create_and_register(|| {
                OmniPaxosComponent::with(
                    pid,
                    op_config.server_config.buffer_size,
                    op_config.build().unwrap(),
                    test_config.election_timeout,
                )
            });
            omni_reg_f.wait_expect(REGISTRATION_TIMEOUT, "ReplicaComp failed to register!");
            omni_refs.insert(pid, omni_replica.actor_ref());
            nodes.insert(pid, omni_replica);
        }

        for omni in nodes.values() {
            omni.on_definition(|o| o.set_peers(omni_refs.clone()));
        }

        Self {
            kompact_system: Some(system),
            nodes,
            temp_dir_path,
        }
    }

    pub fn start_all_nodes(&self) {
        for node in self.nodes.values() {
            self.kompact_system
                .as_ref()
                .expect("No KompactSystem found!")
                .start_notify(node)
                .wait_timeout(START_TIMEOUT)
                .expect("ReplicaComp never started!");
        }
    }

    pub fn stop_all_nodes(&self) {
        for node in self.nodes.values() {
            self.kompact_system
                .as_ref()
                .expect("No KompactSystem found!")
                .stop_notify(node)
                .wait_timeout(STOP_COMPONENT_TIMEOUT)
                .expect("ReplicaComp replica never died!");
        }
    }

    pub fn kill_node(&mut self, id: NodeId) {
        let node = self.nodes.remove(&id).unwrap();
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .kill_notify(node)
            .wait_timeout(STOP_COMPONENT_TIMEOUT)
            .expect("ReplicaComp replica never died!");
        println!("Killed node {}", id);
    }

    pub fn create_node(&mut self, pid: NodeId, test_config: &TestConfig) {
        let mut omni_refs: HashMap<NodeId, ActorRef<Message<Value>>> = HashMap::new();
        let op_config = test_config.into_omnipaxos_config(pid);
        let (omni_replica, omni_reg_f) = self
            .kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .create_and_register(|| {
                OmniPaxosComponent::with(
                    pid,
                    op_config.server_config.buffer_size,
                    op_config.build().unwrap(),
                    test_config.election_timeout,
                )
            });

        omni_reg_f.wait_expect(REGISTRATION_TIMEOUT, "ReplicaComp failed to register!");

        // Insert the new node into vector of peers.
        omni_refs.insert(pid, omni_replica.actor_ref());

        for (other_pid, node) in self.nodes.iter() {
            // Insert each peer node into HashMap as peers to the new node
            omni_refs.insert(*other_pid, node.actor_ref());
            // Also insert the new node as a peer into their Hashmaps
            node.on_definition(|o| o.peers.insert(pid, omni_replica.actor_ref()));
        }

        // Set the peers of the new node, add it to HashMaps of nodes
        omni_replica.on_definition(|o| o.set_peers(omni_refs));
        self.nodes.insert(pid, omni_replica);
    }

    pub fn start_node(&self, pid: NodeId) {
        let node = self
            .nodes
            .get(&pid)
            .unwrap_or_else(|| panic!("Cannot find node {pid}"));
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .start_notify(node)
            .wait_timeout(START_TIMEOUT)
            .expect("ReplicaComp never started!");
    }

    pub fn stop_node(&self, pid: NodeId) {
        let node = self
            .nodes
            .get(&pid)
            .unwrap_or_else(|| panic!("Cannot find node {pid}"));
        self.kompact_system
            .as_ref()
            .expect("No KompactSystem found!")
            .stop_notify(node)
            .wait_timeout(STOP_COMPONENT_TIMEOUT)
            .expect("ReplicaComp never stopped!");
    }

    pub fn set_node_connections(&self, pid: NodeId, connection_status: bool) {
        // set outgoing connections
        let node = self.nodes.get(&pid).expect("Cannot find {pid}");
        node.on_definition(|x| {
            for node_id in self.nodes.keys() {
                x.set_connection(*node_id, connection_status);
            }
        });
        // set incoming connections
        for node_id in self.nodes.keys() {
            let node = self.nodes.get(node_id).expect("Cannot find {pid}");
            node.on_definition(|x| {
                x.set_connection(pid, connection_status);
            });
        }
    }

    /// Return the elected leader from `node`'s viewpoint. If there is no leader yet then
    /// wait until a leader is elected in the allocated time.
    pub fn get_elected_leader(&self, node_id: NodeId, wait_timeout: Duration) -> NodeId {
        let node = self.nodes.get(&node_id).expect("No BLE component found");
        let leader_pid =
            node.on_definition(|x| x.paxos.get_current_leader().map(|(leader, _)| leader));
        leader_pid.unwrap_or_else(|| self.get_next_leader(node_id, wait_timeout))
    }

    /// Return the next new elected leader from `node`'s viewpoint. If there is no leader yet then
    /// wait until a leader is elected in the allocated time.
    pub fn get_next_leader(&self, node_id: NodeId, wait_timeout: Duration) -> NodeId {
        let node = self.nodes.get(&node_id).expect("No BLE component found");
        let (kprom, kfuture) = promise::<Ballot>();
        node.on_definition(|x| x.election_futures.push(Ask::new(kprom, ())));
        let ballot = kfuture
            .wait_timeout(wait_timeout)
            .expect("No leader has been elected in the allocated time!");
        ballot.pid
    }

    /// Forces the cluster to elect `next_leader` as leader of the cluster. Note: This modifies
    /// node connections and results in a fully connected network.
    pub fn force_leader_change(&self, next_leader: NodeId, wait_timeout: Duration) {
        for node in self.nodes.keys() {
            self.set_node_connections(*node, false);
        }
        self.set_node_connections(next_leader, true);
        let current_leader = self.get_elected_leader(next_leader, wait_timeout);
        let next_elected_leader = if current_leader != next_leader {
            self.get_next_leader(next_leader, wait_timeout)
        } else {
            current_leader
        };
        assert_eq!(
            next_leader, next_elected_leader,
            "Failed to force leader change to {}: leader is instead {}",
            next_leader, next_elected_leader
        );
        for node in self.nodes.keys() {
            self.set_node_connections(*node, true);
        }
    }

    /// Use node `proposer` to propose `proposals` then waits for the proposals
    /// to be decided.
    pub fn make_proposals(&self, proposer: NodeId, proposals: Vec<Value>, timeout: Duration) {
        let proposer = self
            .nodes
            .get(&proposer)
            .expect("No SequencePaxos component found");

        let mut proposal_futures = vec![];
        proposer.on_definition(|x| {
            for v in proposals {
                let (kprom, kfuture) = promise::<()>();
                x.paxos.append(v.clone());
                x.insert_decided_future(Ask::new(kprom, v));
                proposal_futures.push(kfuture);
            }
        });

        match FutureCollection::collect_with_timeout::<Vec<_>>(proposal_futures, timeout) {
            Ok(_) => {}
            Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
        }
    }

    fn set_executor_for_threads(threads: usize, conf: &mut KompactConfig) {
        if threads <= 32 {
            conf.executor(crossbeam_workstealing_pool::small_pool)
        } else if threads <= 64 {
            conf.executor(crossbeam_workstealing_pool::large_pool)
        } else {
            conf.executor(crossbeam_workstealing_pool::dyn_pool)
        };
    }
}
pub mod omnireplica {
    use super::*;
    use omnipaxos::{
        messages::Message,
        utils::{Ballot, LogEntry, NodeId},
        OmniPaxos,
    };
    use std::collections::{HashMap, HashSet};

    #[derive(ComponentDefinition)]
    pub struct OmniPaxosComponent {
        ctx: ComponentContext<Self>,
        #[allow(dead_code)]
        pid: NodeId,
        pub peers: HashMap<NodeId, ActorRef<Message<Value>>>,
        pub peer_disconnections: HashSet<NodeId>,
        paxos_timer: Option<ScheduledTimer>,
        tick_timer: Option<ScheduledTimer>,
        tick_timeout: Duration,
        pub paxos: OmniPaxos<Value>,
        decided_futures: HashMap<NodeId, Ask<Value, ()>>,
        pub election_futures: Vec<Ask<(), Ballot>>,
        current_leader_ballot: Ballot,
        decided_idx: usize,
        outgoing_buffer: Vec<Message<Value>>,
    }

    impl ComponentLifecycle for OmniPaxosComponent {
        fn on_start(&mut self) -> Handled {
            self.paxos_timer = Some(self.schedule_periodic(
                CHECK_DECIDED_TIMEOUT,
                CHECK_DECIDED_TIMEOUT,
                move |c, _| {
                    c.send_outgoing_msgs();
                    c.answer_decided_future();
                    Handled::Ok
                },
            ));
            self.tick_timer =
                Some(
                    self.schedule_periodic(self.tick_timeout, self.tick_timeout, move |c, _| {
                        c.paxos.tick();
                        let promise = c.paxos.get_promise();
                        if promise > c.current_leader_ballot {
                            c.current_leader_ballot = promise;
                            c.answer_election_future(promise);
                        }
                        Handled::Ok
                    }),
                );
            Handled::Ok
        }

        fn on_kill(&mut self) -> Handled {
            if let Some(timer) = self.paxos_timer.take() {
                self.cancel_timer(timer);
            }
            Handled::Ok
        }
    }

    impl OmniPaxosComponent {
        pub fn with(
            pid: NodeId,
            buffer_size: usize,
            paxos: OmniPaxos<Value>,
            tick_timeout: Duration,
        ) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                pid,
                peers: HashMap::new(),
                peer_disconnections: HashSet::new(),
                paxos_timer: None,
                tick_timer: None,
                tick_timeout,
                decided_idx: paxos.get_decided_idx(),
                paxos,
                decided_futures: HashMap::new(),
                election_futures: vec![],
                current_leader_ballot: Ballot::default(),
                outgoing_buffer: Vec::with_capacity(buffer_size),
            }
        }

        pub fn read_decided_log(&self) -> Vec<LogEntry<Value>> {
            self.paxos.read_decided_suffix(0).unwrap()
        }

        fn send_outgoing_msgs(&mut self) {
            self.paxos.take_outgoing_messages(&mut self.outgoing_buffer);
            for out in self.outgoing_buffer.drain(..) {
                if !self.peer_disconnections.contains(&out.get_receiver()) {
                    match self.peers.get(&out.get_receiver()) {
                        Some(receiver) => receiver.tell(out),
                        None => warn!(
                            self.ctx.log(),
                            "Peer {} not found! Message: {:?}",
                            out.get_receiver(),
                            out
                        ),
                    }
                }
            }
        }

        pub fn set_peers(&mut self, peers: HashMap<NodeId, ActorRef<Message<Value>>>) {
            self.peers = peers;
        }

        // Used to simulate a network fault to Component `pid`.
        pub fn set_connection(&mut self, pid: NodeId, is_connected: bool) {
            match is_connected {
                true => self.peer_disconnections.remove(&pid),
                false => self.peer_disconnections.insert(pid),
            };
        }

        fn answer_election_future(&mut self, l: Ballot) {
            if !self.election_futures.is_empty() {
                self.election_futures.pop().unwrap().reply(l).unwrap();
            }
        }

        pub fn insert_decided_future(&mut self, a: Ask<Value, ()>) {
            let id = a.request().id;
            let replaced = self.decided_futures.insert(id, a);
            assert!(replaced.is_none(), "Future for {:?} already exists!", id);
        }

        fn try_answer_decided_future(&mut self, id: NodeId) {
            if let Some(ask) = self.decided_futures.remove(&id) {
                ask.reply(()).expect("Failed to reply promise!");
            }
        }

        fn answer_decided_future(&mut self) {
            if let Some(entries) = self.paxos.read_decided_suffix(self.decided_idx) {
                for e in entries {
                    match e {
                        LogEntry::Decided(i) => {
                            self.try_answer_decided_future(i.id);
                        }
                        err => panic!("{}", format!("Got unexpected entry: {:?}", err)),
                    }
                }
                self.decided_idx = self.paxos.get_decided_idx();
            }
        }
    }

    impl Actor for OmniPaxosComponent {
        type Message = Message<Value>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            self.paxos.handle_incoming(msg);
            Handled::Ok
        }

        fn receive_network(&mut self, _: NetMessage) -> Handled {
            unimplemented!()
        }
    }
}

#[derive(Entry, Clone, Default, PartialOrd, PartialEq, Serialize, Deserialize, Eq, Hash, Debug)]
pub struct Value {
    pub id: u64,
}

impl Value {
    pub fn with_id(id: u64) -> Self {
        Self { id }
    }
}

/// Create a temporary directory in /tmp/
pub fn create_temp_dir() -> String {
    let dir = TempDir::new().expect("Failed to create temporary directory");
    let dir_path = dir.path().to_path_buf();
    dir_path.to_string_lossy().to_string()
}

pub mod verification {
    use super::Value;
    use omnipaxos::utils::{LogEntry, NodeId};

    /// Verify that the log matches the proposed values
    /// * All entries are decided, verify the decided entries
    pub fn verify_log(read_log: Vec<LogEntry<Value>>, proposals: Vec<Value>) {
        let num_proposals = proposals.len();
        match &read_log[..] {
            [LogEntry::Decided(_), ..] => verify_entries(&read_log, &proposals, 0, num_proposals),
            [] => assert!(
                proposals.is_empty(),
                "Log is empty but should be {:?}",
                proposals
            ),
            _ => panic!("Unexpected entries in the log: {:?} ", read_log),
        }
    }

    /// Verify that all log entries are decided and matches the proposed entries.
    pub fn verify_entries(
        read_entries: &[LogEntry<Value>],
        exp_entries: &[Value],
        offset: usize,
        decided_idx: usize,
    ) {
        assert_eq!(
            read_entries.len(),
            exp_entries.len(),
            "read: {:?}, expected: {:?}",
            read_entries,
            exp_entries
        );
        for (idx, entry) in read_entries.iter().enumerate() {
            let log_idx = idx + offset;
            match entry {
                LogEntry::Decided(i) if log_idx < decided_idx => assert_eq!(*i, exp_entries[idx]),
                LogEntry::Undecided(_, i, _) if log_idx >= decided_idx => {
                    assert_eq!(*i, exp_entries[idx])
                }
                e => panic!(
                    "{}",
                    format!(
                        "Unexpected entry at idx {}: {:?}, decided_idx: {}",
                        idx, e, decided_idx
                    )
                ),
            }
        }
    }

    /// Verifies that there is a majority when an entry is proposed.
    pub fn check_quorum(
        logs: &[(NodeId, Vec<LogEntry<Value>>)],
        quorum_size: usize,
        proposals: &[Value],
    ) {
        for v in proposals {
            let num_nodes: usize = logs
                .iter()
                .filter(|(_pid, log)| log.contains(&LogEntry::Decided(v.clone())))
                .count();
            let timed_out_proposal = num_nodes == 0;
            if !timed_out_proposal {
                assert!(
                    num_nodes >= quorum_size,
                    "Decided value did NOT have majority quorum! contained: {:?}",
                    num_nodes
                );
            }
        }
    }

    /// Verifies that only proposed values are decided.
    pub fn check_validity(logs: &[(NodeId, Vec<LogEntry<Value>>)], proposals: &[Value]) {
        logs.iter().for_each(|(_pid, log)| {
            for entry in log {
                if let LogEntry::Decided(v) = entry {
                    assert!(
                        proposals.contains(v),
                        "Node decided unproposed value: {:?}",
                        v
                    );
                }
            }
        });
    }

    /// Verifies logs do not diverge. **NOTE**: this check assumes normal execution within one round
    pub fn check_consistent_log_prefixes(logs: &Vec<(NodeId, Vec<LogEntry<Value>>)>) {
        let (_, longest_log) = logs
            .iter()
            .max_by(|(_, sr), (_, other_sr)| sr.len().cmp(&other_sr.len()))
            .expect("Empty log from nodes!");
        for (_, log) in logs {
            assert!(longest_log.starts_with(log.as_slice()));
        }
    }
}
