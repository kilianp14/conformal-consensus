pub mod utils;

use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos::{storage::Storage, OmniPaxosConfig};
use serial_test::serial;
use utils::{verification::*, StorageType, TestConfig, TestSystem, Value};

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

#[test]
#[serial]
fn read_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    let log: Vec<Value> = [1, 3, 2, 7, 5, 10, 29, 100, 8, 12]
        .iter()
        .map(|v| Value::with_id(*v as u64))
        .collect();
    let decided_idx = 6;

    let mut storage = StorageType::<Value>::with(cfg.storage_type);
    storage
        .append_entries(log.clone())
        .expect("Failed to append entries");
    storage
        .set_decided_idx(decided_idx)
        .expect("Failed to set decided index");

    let mut op_config = OmniPaxosConfig::default();
    op_config.server_config.pid = 1;
    op_config.cluster_config.nodes = vec![1, 2, 3];
    op_config.cluster_config.configuration_id = 1;

    let omni_paxos = op_config.clone().build(storage).unwrap();

    // read decided entries
    let entries = omni_paxos
        .read_decided_suffix(0)
        .expect("No decided entries");
    let expected_entries = log.get(0..decided_idx).unwrap();
    verify_entries(entries.as_slice(), expected_entries, 0, decided_idx);

    // read entry
    let idx = 4;
    let entry = omni_paxos.read(idx).expect("No entry");
    let expected_entries = log.get(idx..=idx).unwrap();
    verify_entries(&[entry], expected_entries, 0, decided_idx);

    // read none
    let idx = log.len();
    let entry = omni_paxos.read(idx);
    assert!(entry.is_none(), "Expected None, got: {:?}", entry);
}

#[test]
#[serial]
fn read_entries_test() {
    let cfg = TestConfig::load("consensus_test").expect("Test config loaded");

    let log: Vec<Value> = [1, 3, 2, 7, 5, 10, 29, 100, 8, 12]
        .iter()
        .map(|v| Value::with_id(*v as u64))
        .collect();
    let decided_idx = 6;

    let mut storage = StorageType::<Value>::with(cfg.storage_type);
    storage
        .append_entries(log.clone())
        .expect("Failed to append entries");
    storage
        .set_decided_idx(decided_idx)
        .expect("Failed to set decided index");
    let mut op_config = OmniPaxosConfig::default();
    op_config.server_config.pid = 1;
    op_config.cluster_config.nodes = vec![1, 2, 3];
    op_config.cluster_config.configuration_id = 1;

    let omni_paxos = op_config.clone().build(storage).unwrap();

    // read snapshot + entries
    let from_idx = 3;
    let to_idx = decided_idx;
    let entries = omni_paxos
        .read_entries(from_idx..to_idx)
        .expect("No entries");
    let expected_entries = log.get(from_idx..to_idx).unwrap();
    verify_entries(&entries, expected_entries, 0, decided_idx);

    // read none
    let from_idx = 0;
    let to_idx = log.len();
    let entries = omni_paxos.read_entries(from_idx..=to_idx);
    assert!(entries.is_none(), "Expected None, got: {:?}", entries);
}
