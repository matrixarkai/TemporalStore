use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::raft::RaftReplicaRole;
use temporalstore_rust::{
    AddNamespaceRequest, MetaCommand, MetaMutation, ProductionMetaRaftRuntime,
    ProductionMetaRaftRuntimeOptions, ProductionRaftEngineKind, ProductionRaftNode, RaftConfig,
    RaftNodeId, ShardLocation,
};

#[derive(Debug, Clone)]
struct HarnessOptions {
    root: PathBuf,
}

#[derive(Debug, Serialize)]
struct MetaserverRaftHarnessSummary {
    root: String,
    initial_membership: Vec<RaftNodeId>,
    membership_after_add: Vec<RaftNodeId>,
    membership_after_remove: Vec<RaftNodeId>,
    unsupported_role_rejected: bool,
    wait_for_log_applied_index: u64,
    snapshot_index: u64,
    snapshot_restore_read: Option<String>,
    lagging_node_id: RaftNodeId,
    lagging_write_hidden_before_catchup: bool,
    lagging_snapshot_restore_missed_tail: bool,
    lagging_catchup_read: Option<String>,
    leader_before_transfer: RaftNodeId,
    leader_after_transfer: RaftNodeId,
    leader_after_failover: RaftNodeId,
    namespace_after_failover_visible: bool,
    unavailable_without_majority: bool,
    elapsed_ms: u128,
}

fn main() {
    let options = parse_options();
    std::fs::create_dir_all(&options.root).expect("failed to create harness root");
    let started = Instant::now();
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::OpenRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:18110".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:18111".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:18112".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 10,
        election_tick_ms: 5,
        failure_detector_interval_ms: 10,
        stale_server_after_ms: 1_000,
    })
    .expect("metaserver raft runtime should start");
    runtime
        .validate_ready()
        .expect("initial metaserver raft runtime should be ready");

    let initial_membership = runtime.list_membership();
    runtime
        .propose(MetaCommand::ApplyMutation(MetaMutation::AddNamespace(
            AddNamespaceRequest {
                namespace: "meta-raft-before-snapshot".to_string(),
            },
        )))
        .expect("initial namespace should commit");
    let wait_for_log_applied_index = runtime
        .wait_for_log_applied()
        .expect("read-index wait should pass")
        .read_index;

    let add_report = runtime
        .add_node(13, RaftReplicaRole::Voter)
        .expect("metaserver add voter should pass");
    assert_eq!(add_report.live_voters, 4);
    let membership_after_add = runtime.list_membership();
    let unsupported_role_rejected = runtime
        .add_node(14, RaftReplicaRole::Learner)
        .err()
        .map(|err| err.to_string().contains("voter membership only"))
        .unwrap_or(false);
    assert!(unsupported_role_rejected, "learner add must fail closed");

    runtime
        .apply_membership([10, 11, 13])
        .expect("metaserver membership replace should pass");
    let membership_after_remove = runtime.list_membership();

    runtime
        .propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 55,
            server_addr: "meta-snapshot-server".to_string(),
            latest_snapshot: None,
        }))
        .expect("snapshot source route should commit");
    let snapshot = runtime
        .trigger_snapshot()
        .expect("metaserver snapshot trigger should pass");
    let snapshot_index = snapshot.last_included_index;
    let lagging_node_id = 13;
    runtime.cluster().set_alive(lagging_node_id, false).unwrap();
    runtime
        .propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 56,
            server_addr: "meta-after-lag".to_string(),
            latest_snapshot: None,
        }))
        .expect("write while node 13 is down should commit with majority");
    let lagging_write_hidden_before_catchup = runtime
        .cluster()
        .get_shard_location(lagging_node_id, 56)
        .expect("lagging metaserver local read should not fail")
        .is_none();
    runtime.cluster().set_alive(lagging_node_id, true).unwrap();
    runtime
        .cluster()
        .install_snapshot(lagging_node_id, snapshot)
        .expect("snapshot should bootstrap lagging metaserver replica");
    let snapshot_restore_read = runtime
        .cluster()
        .get_shard_location(lagging_node_id, 55)
        .expect("snapshot-restored route read should not fail")
        .map(|location| location.server_addr);
    let lagging_snapshot_restore_missed_tail = runtime
        .cluster()
        .get_shard_location(lagging_node_id, 56)
        .expect("stale snapshot tail read should not fail")
        .is_none();
    runtime
        .cluster()
        .catch_up(lagging_node_id)
        .expect("lagging metaserver voter should catch up from raft log tail");
    let lagging_catchup_read = runtime
        .cluster()
        .get_shard_location(lagging_node_id, 56)
        .expect("caught-up route read should not fail")
        .map(|location| location.server_addr);

    let leader_before_transfer = runtime.status().leader_id;
    runtime
        .transfer_leader(11)
        .expect("leader transfer should pass");
    let leader_after_transfer = runtime.status().leader_id;
    runtime
        .cluster()
        .set_alive(leader_after_transfer, false)
        .unwrap();
    runtime
        .propose(MetaCommand::ApplyMutation(MetaMutation::AddNamespace(
            AddNamespaceRequest {
                namespace: "meta-raft-after-failover".to_string(),
            },
        )))
        .expect("write after metaserver leader failure should commit");
    let leader_after_failover = runtime.status().leader_id;
    let namespace_after_failover_visible = runtime
        .cluster()
        .list_namespaces()
        .namespaces
        .iter()
        .any(|namespace| namespace.namespace == "meta-raft-after-failover");
    assert!(
        namespace_after_failover_visible,
        "post-failover namespace must be visible"
    );

    runtime.cluster().set_alive(10, false).unwrap();
    runtime.cluster().set_alive(13, false).unwrap();
    let unavailable_without_majority = runtime
        .propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 57,
            server_addr: "must-not-commit-without-majority".to_string(),
            latest_snapshot: None,
        }))
        .is_err();
    assert!(
        unavailable_without_majority,
        "metaserver raft must reject writes without majority"
    );

    let summary = MetaserverRaftHarnessSummary {
        root: options.root.display().to_string(),
        initial_membership,
        membership_after_add,
        membership_after_remove,
        unsupported_role_rejected,
        wait_for_log_applied_index,
        snapshot_index,
        snapshot_restore_read,
        lagging_node_id,
        lagging_write_hidden_before_catchup,
        lagging_snapshot_restore_missed_tail,
        lagging_catchup_read,
        leader_before_transfer,
        leader_after_transfer,
        leader_after_failover,
        namespace_after_failover_visible,
        unavailable_without_majority,
        elapsed_ms: started.elapsed().as_millis(),
    };
    assert_eq!(
        summary.snapshot_restore_read.as_deref(),
        Some("meta-snapshot-server")
    );
    assert!(
        summary.lagging_write_hidden_before_catchup,
        "lagging metaserver voter must not see tail write before catch-up"
    );
    assert!(
        summary.lagging_snapshot_restore_missed_tail,
        "stale metaserver snapshot must not contain post-snapshot tail write"
    );
    assert_eq!(
        summary.lagging_catchup_read.as_deref(),
        Some("meta-after-lag")
    );
    assert_ne!(summary.leader_after_failover, summary.leader_after_transfer);
    println!("{}", serde_json::to_string_pretty(&summary).unwrap());
}

fn parse_options() -> HarnessOptions {
    let mut root = std::env::temp_dir().join(format!(
        "temporalstore-metaserver-raft-harness-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
    ));
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--root" => {
                let Some(value) = args.get(index + 1) else {
                    usage_and_exit();
                };
                root = PathBuf::from(value);
                index += 2;
            }
            "--help" | "-h" => usage_and_exit(),
            other => {
                eprintln!("unknown option: {other}");
                usage_and_exit();
            }
        }
    }
    HarnessOptions { root }
}

fn usage_and_exit() -> ! {
    eprintln!("usage: metaserver_raft_harness [--root <path>]");
    std::process::exit(2);
}
