pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use serial_test::serial;
use utils::{verification::*, TestConfig, TestSystem};

/// Verifies the 3 properties that the Paxos algorithm offers
/// Quorum, Validity, Uniform Agreement
#[test]
#[serial]
fn consensus_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);

    let first_node = sys.nodes.get(&1).unwrap();
    let mut futures = vec![];
    let vec_proposals = utils::create_proposals(1, cfg.num_proposals);
    for v in &vec_proposals {
        let (kprom, kfuture) = promise::<()>();
        first_node.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, v.clone()));
            x.paxos.append(v.clone());
        });
        futures.push(kfuture);
    }

    sys.start_all_nodes();

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    let mut log = vec![];
    for (pid, node) in sys.nodes {
        log.push(node.on_definition(|x| {
            let log = x.read_decided_log();
            (pid, log)
        }));
    }

    let quorum_size = cfg.num_nodes / 2 + 1;
    check_quorum(&log, quorum_size, &vec_proposals);
    check_validity(&log, &vec_proposals);
    check_consistent_log_prefixes(&log);

    let kompact_system =
        std::mem::take(&mut sys.kompact_system).expect("No KompactSystem in memory");
    match kompact_system.shutdown() {
        Ok(_) => {}
        Err(e) => panic!("Error on kompact shutdown: {}", e),
    };
}
