#[test]
fn build_op_test() {
    use omnipaxos::{macros::Entry, ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};

    #[derive(Clone, Debug, Entry)]
    struct TestEntry {
        pub _field1: u64,
        pub _field2: String,
    }

    let cluster_config = ClusterConfig {
        nodes: vec![1, 2, 3],
        ..Default::default()
    };
    let server_config = ServerConfig {
        pid: 1,
        ..Default::default()
    };
    let config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };

    let _omnipaxos: OmniPaxos<TestEntry> = config.build().unwrap();
}
