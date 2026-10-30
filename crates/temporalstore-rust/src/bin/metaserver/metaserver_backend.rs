// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// impl MetaBackend, split from metaserver.rs (textual include!, shared flat scope).

impl MetaBackend {
    fn from_env() -> std::io::Result<Self> {
        if env_bool("TS_META_RAFT", false) || std::env::var("TS_META_RAFT_NODES").is_ok() {
            let options = runtime_options_from_env();
            if let Some(warning) = unreplicated_meta_raft_warning(&options.nodes) {
                tracing::warn!(
                    nodes = options.nodes.len(),
                    "{warning}"
                );
            }
            return Ok(Self::Raft(
                ProductionMetaRaftRuntime::start(options)
                    .expect("failed to initialize metaserver raft runtime"),
            ));
        }
        let meta = std::env::var("TS_META_MUTATION_LOG")
            .ok()
            .map(SingleNodeMeta::with_mutation_log)
            .transpose()?
            .unwrap_or_default();
        // With this on, a datanode the metaserver convicted cannot re-register
        // its way back to Normal; an operator has to unfreeze it. Off by default
        // because the automatic recovery it removes is load-bearing wherever the
        // freeze cooldown is left at zero.
        let meta = meta.with_conviction_lock(env_bool(
            "TS_META_FORBID_SELF_CLEARING_CONVICTION",
            false,
        ));
        Ok(Self::Single(meta))
    }

    /// Prometheus text for the background subsystems. Empty for the raft
    /// backend, which does not drive them -- every one of those loops declines
    /// to start against it.
    fn subsystem_prometheus(&self) -> String {
        match self {
            Self::Single(meta) => meta.subsystem_metrics().prometheus(),
            Self::Raft(_) => String::new(),
        }
    }

    /// Record the encoded size of a topology answer.
    ///
    /// Only the single-node backend keeps these: the raft backend drives none of
    /// the subsystems this recorder belongs to, and reports nothing from it.
    fn record_topology_bytes(&self, bytes: usize) {
        if let Self::Single(meta) = self {
            meta.subsystem_metrics().record_topology_bytes(bytes);
        }
    }

    /// Who leads this metaserver's raft group, and where.
    fn raft_leader(&self) -> MetaRaftLeaderResponse {
        match self {
            Self::Single(_) => MetaRaftLeaderResponse {
                status: Status::error("raft_disabled", "meta raft is disabled"),
                leader_id: 0,
                addr: String::new(),
                is_local: false,
            },
            Self::Raft(runtime) => match runtime.leader_endpoint() {
                Some((leader_id, addr)) => MetaRaftLeaderResponse {
                    status: Status::ok(),
                    leader_id,
                    addr,
                    is_local: leader_id == runtime.local_node_id(),
                },
                None => MetaRaftLeaderResponse {
                    status: Status::error(
                        "leader_unavailable",
                        "meta raft has no leader with a configured address",
                    ),
                    leader_id: 0,
                    addr: String::new(),
                    is_local: false,
                },
            },
        }
    }

    fn raft_status(&self) -> Option<RaftClusterStatus> {
        match self {
            Self::Single(_) => None,
            Self::Raft(runtime) => Some(runtime.status()),
        }
    }

    /// This metaserver's answer to a readiness probe, and the HTTP status that goes
    /// with it.
    ///
    /// `/readiness` used to return `production_readiness_report()` with a hardcoded 200.
    /// That report takes no arguments and reads no live state -- it describes what the
    /// codebase supports, not what this process can do, and its own test asserts that it
    /// is never `production_ready`. So the probe answered 200 from the first instant of
    /// startup and could not answer anything else, which defeats the one thing a
    /// readiness probe is for: a metaserver that cannot serve still had traffic sent to
    /// it.
    ///
    /// The signal was already here. A raft-backed metaserver that has lost quorum cannot
    /// serve metadata, and `validate_ready` already says so -- it was just never wired to
    /// the probe. A single-node backend has no quorum to lose, so it is ready once it is
    /// up.
    ///
    /// Deliberately NOT used here: `MetaPreflightReport.degraded_reasons`. Those describe
    /// the CLUSTER (frozen servers, frozen proxies), so one frozen datanode would fail
    /// this probe on every metaserver at once and pull the whole control plane out of
    /// service exactly when it is needed.
    fn readiness(&self) -> (u16, MetaReadinessResponse) {
        let (backend, ready) = match self {
            Self::Single(_) => ("single", Ok(())),
            Self::Raft(runtime) => (
                "raft",
                runtime.validate_ready().map_err(|err| err.to_string()),
            ),
        };
        meta_readiness_response(backend, ready)
    }

    fn raft_ready(&self) -> Status {
        match self {
            Self::Single(_) => Status::error("raft_disabled", "meta raft is disabled"),
            Self::Raft(runtime) => runtime
                .validate_ready()
                .map(|_| Status::ok())
                .unwrap_or_else(|err| Status::error("raft_not_ready", err.to_string())),
        }
    }

    /// The refusal every membership operation gives on the single-node
    /// backend. There is no cluster to change the shape of, and saying so is
    /// better than a route that appears to work.
    fn raft_only() -> Status {
        Status::error("raft_disabled", "meta raft is disabled")
    }

    fn raft_membership(&self) -> MetaRaftMembershipResponse {
        match self {
            Self::Single(_) => MetaRaftMembershipResponse {
                status: Self::raft_only(),
                leader_id: 0,
                members: Vec::new(),
                term: 0,
            },
            Self::Raft(runtime) => {
                let status = runtime.status();
                MetaRaftMembershipResponse {
                    status: Status::ok(),
                    leader_id: status.leader_id,
                    members: runtime.list_membership(),
                    term: status.current_term,
                }
            }
        }
    }

    /// Add a voter, refusing anything that would leave the cluster unable to
    /// reach a majority. The unguarded variant exists but is not what an
    /// operator should be able to reach over HTTP.
    fn raft_add_node(&self, node_id: u64) -> MetaRaftScaleResponse {
        match self {
            Self::Single(_) => MetaRaftScaleResponse {
                status: Self::raft_only(),
                report: None,
            },
            Self::Raft(runtime) => match runtime.cluster().add_node_safely(node_id) {
                Ok(report) => MetaRaftScaleResponse {
                    status: Status::ok(),
                    report: Some(report),
                },
                Err(err) => MetaRaftScaleResponse {
                    status: Status::error("raft_scale_refused", err.to_string()),
                    report: None,
                },
            },
        }
    }

    fn raft_remove_node(&self, node_id: u64) -> MetaRaftScaleResponse {
        match self {
            Self::Single(_) => MetaRaftScaleResponse {
                status: Self::raft_only(),
                report: None,
            },
            Self::Raft(runtime) => match runtime.cluster().remove_node_safely(node_id) {
                Ok(report) => MetaRaftScaleResponse {
                    status: Status::ok(),
                    report: Some(report),
                },
                Err(err) => MetaRaftScaleResponse {
                    status: Status::error("raft_scale_refused", err.to_string()),
                    report: None,
                },
            },
        }
    }

    fn raft_transfer_leader(&self, node_id: u64) -> AckResponse {
        match self {
            Self::Single(_) => AckResponse {
                status: Self::raft_only(),
            },
            Self::Raft(runtime) => AckResponse {
                status: runtime
                    .cluster()
                    .transfer_leader(node_id)
                    .map(|_| Status::ok())
                    .unwrap_or_else(|err| {
                        Status::error("raft_transfer_refused", err.to_string())
                    }),
            },
        }
    }

    fn raft_trigger_snapshot(&self) -> MetaRaftSnapshotTriggerResponse {
        match self {
            Self::Single(_) => MetaRaftSnapshotTriggerResponse {
                status: Self::raft_only(),
                report: None,
            },
            Self::Raft(runtime) => match runtime.cluster().maybe_trigger_snapshot() {
                Ok(report) => MetaRaftSnapshotTriggerResponse {
                    status: Status::ok(),
                    report: Some(report),
                },
                Err(err) => MetaRaftSnapshotTriggerResponse {
                    status: Status::error("raft_snapshot_refused", err.to_string()),
                    report: None,
                },
            },
        }
    }

    fn export_snapshot(&self) -> MetaSnapshotResponse {
        match self {
            Self::Single(meta) => MetaSnapshotResponse {
                status: Status::ok(),
                snapshot: Some(meta.export_snapshot()),
            },
            Self::Raft(runtime) => match runtime.cluster().export_meta_snapshot() {
                Ok(snapshot) => MetaSnapshotResponse {
                    status: Status::ok(),
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotResponse {
                    status: Status::error("raft_snapshot_export_failed", err.to_string()),
                    snapshot: None,
                },
            },
        }
    }

    fn install_snapshot(&self, snapshot: MetaSnapshot) -> AckResponse {
        match self {
            Self::Single(meta) => meta.install_snapshot(snapshot),
            Self::Raft(runtime) => AckResponse {
                status: runtime
                    .cluster()
                    .install_meta_snapshot_on_live_nodes(snapshot)
                    .map(|_| Status::ok())
                    .unwrap_or_else(|err| {
                        Status::error("raft_snapshot_install_failed", err.to_string())
                    }),
            },
        }
    }

    fn save_snapshot(&self, request: MetaSnapshotFileRequest) -> MetaSnapshotFileResponse {
        match self {
            Self::Single(meta) => match meta.save_snapshot(&request.path) {
                Ok(snapshot) => MetaSnapshotFileResponse {
                    status: Status::ok(),
                    path: request.path,
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_save_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
            Self::Raft(runtime) => match runtime.cluster().export_meta_snapshot() {
                Ok(snapshot) => MetaSnapshotFileResponse {
                    status: save_meta_snapshot_file(&request.path, &snapshot)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("snapshot_save_failed", err)),
                    path: request.path,
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_save_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
        }
    }

    fn load_snapshot(&self, request: MetaSnapshotFileRequest) -> MetaSnapshotFileResponse {
        match self {
            Self::Single(meta) => match SingleNodeMeta::load_snapshot_from_file(&request.path) {
                Ok(snapshot) => {
                    let status = meta.install_snapshot(snapshot.clone()).status;
                    MetaSnapshotFileResponse {
                        status,
                        path: request.path,
                        snapshot: Some(snapshot),
                    }
                }
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_load_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
            Self::Raft(runtime) => match SingleNodeMeta::load_snapshot_from_file(&request.path) {
                Ok(snapshot) => {
                    let status = runtime
                        .cluster()
                        .install_meta_snapshot_on_live_nodes(snapshot.clone())
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| {
                            Status::error("raft_snapshot_install_failed", err.to_string())
                        });
                    MetaSnapshotFileResponse {
                        status,
                        path: request.path,
                        snapshot: Some(snapshot),
                    }
                }
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_load_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct MetaReadinessResponse {
    status: Status,
    /// Whether this metaserver can serve metadata right now.
    ready: bool,
    /// Which backend answered: "single" or "raft".
    backend: String,
    /// Why it is not ready, empty when it is. Reported rather than left to be
    /// inferred from a bare 503.
    reason: String,
}

/// Map a backend's own verdict to a readiness answer.
///
/// Split out from `MetaBackend::readiness` so both outcomes are testable without
/// standing up a raft cluster: the interesting half is that a not-ready metaserver
/// answers 503 rather than 200.
fn meta_readiness_response(
    backend: &str,
    ready: Result<(), String>,
) -> (u16, MetaReadinessResponse) {
    match ready {
        Ok(()) => (
            200,
            MetaReadinessResponse {
                status: Status::ok(),
                ready: true,
                backend: backend.to_string(),
                reason: String::new(),
            },
        ),
        Err(reason) => (
            503,
            MetaReadinessResponse {
                status: Status::error("meta_not_ready", reason.clone()),
                ready: false,
                backend: backend.to_string(),
                reason,
            },
        ),
    }
}
