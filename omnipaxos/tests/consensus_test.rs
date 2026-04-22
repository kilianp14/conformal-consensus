pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos::utils::NodeId;
use rand::Rng;
use serial_test::serial;
use std::{thread, time::Duration};
use utils::{verification::*, TestConfig, TestSystem};

/// Verifies the 3 properties that the Paxos algorithm offers
/// Quorum, Validity, Uniform Agreement
#[test]
#[serial]
fn consensus_test() {
    // Setup System
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");
    let mut sys = TestSystem::with(cfg);
    sys.start_all_nodes();

    // Wait for an initial leader to be elected to ensure the cluster is ready
    sys.get_elected_leader(1, cfg.wait_timeout);

    let mut futures = vec![];
    let vec_proposals = utils::create_proposals(1, cfg.num_proposals);
    let mut rng = rand::thread_rng();

    // Propose values to random nodes with random intervals
    for (_i, v) in vec_proposals.iter().enumerate() {
        #[cfg(feature = "adaptive")]
        if _i as u64 == cfg.num_proposals / 2 {
            thread::sleep(Duration::from_millis(5000));
            sys.calibrate_all_nodes(0.1);
        }
        // Pick a random node (1 to num_nodes)
        let random_pid = rng.gen_range(1..=cfg.num_nodes as NodeId);
        let node = sys.nodes.get(&random_pid).expect("Node should exist");

        let (kprom, kfuture) = promise::<()>();

        node.on_definition(|x| {
            x.insert_decided_future(Ask::new(kprom, v.clone()));
            x.paxos.append(v.clone());
        });
        futures.push(kfuture);

        // // Random delay to simulate parallel vs sequential behavior
        // let delay = rng.gen_range(0..15);
        // if delay > 0 {
        //     thread::sleep(Duration::from_millis(delay));
        // }
    }

    // Wait for all proposals to be decided
    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => println!("All proposals decided successfully."),
        Err(e) => panic!("Error collecting decided proposals: {}", e),
    }

    // Consensus Property Verifications
    let mut logs = vec![];
    for (pid, node) in &sys.nodes {
        logs.push(node.on_definition(|x| {
            let log = x.read_decided_log();
            (*pid, log)
        }));
    }
    let quorum_size = cfg.num_nodes / 2 + 1;
    check_quorum(&logs, quorum_size, &vec_proposals);
    check_validity(&logs, &vec_proposals);
    check_consistent_log_prefixes(&logs);
    #[cfg(feature = "adaptive")]
    sys.print_fast_path_success_rates();

    // Graceful Shutdown
    let kompact_system = std::mem::take(&mut sys.kompact_system).expect("No KompactSystem");
    kompact_system.shutdown().expect("Kompact shutdown failed");
}
