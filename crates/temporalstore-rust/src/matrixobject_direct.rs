// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The direct way into a MatrixObject cluster: no gateway process.
//!
//! A gateway answers S3 requests by calling the cluster SDK against the metadata group and the
//! chunk servers. The direct mode runs that same request handling inside this process: the node
//! builds a [`ClusterGateway`], and each request the networked store would have sent over a
//! socket is handed to it as a function call. Nothing is copied from the gateway, so the append
//! rules, the read-back of an append whose outcome is unknown, the 503 and 412 mapping, ranged
//! reads and listing are the gateway's own; the store above the transport is the same one that
//! talks to a gateway, so the two ways cannot disagree about a request, only about how it travels.
//!
//! # Settings
//!
//! * `TS_MATRIXOBJECT_META`: the metadata group's endpoints, comma separated (1 to 7).
//! * `TS_MATRIXOBJECT_META_GROUP`: the metadata group's numeric id. Optional for a single
//!   endpoint; with several, `1` unless set.
//! * `TS_MATRIXOBJECT_BUCKET`: the bucket the node's objects live in (default `temporalstore`).
//! * `TS_MATRIXOBJECT_TIMEOUT_MS`: bound on one cluster operation (default 10000).
//! * `MATRIXOBJECT_CLIENT_TLS_*`: mutual TLS to the cluster, as the chunk server reads them.
//!
//! The leader of the metadata group is the SDK's to find: it rotates to another endpoint when
//! one refuses, and a write is never sent again after it was submitted.

use std::sync::Arc;
use std::time::Duration;

use matrixobjectstore_rs::cluster::{
    ApiTlsConfig, ObjectWriteOptions, RemoteMetaClient, ReplicaClientOptions,
};
use matrixobjectstore_rs::net::cluster_gateway::{ClusterGateway, GatewayOptions};
use matrixobjectstore_rs::net::InProcessDoor;
use temporalstore_snapshot::object_store::{NetworkedMatrixObject, DEFAULT_HTTP_BUCKET};

/// How a node reaches the cluster directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectConfig {
    pub meta_endpoints: Vec<String>,
    /// `None`: a single endpoint is a plain metadata client, several are a Raft catalog (group 1).
    pub group_id: Option<u64>,
    pub bucket: String,
    pub timeout: Duration,
}

/// The endpoints of a comma separated list, trimmed, empty entries dropped.
pub fn parse_endpoints(list: &str) -> Vec<String> {
    list.split(',')
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .collect()
}

/// Which metadata group to pin the connection to: the one named, else group 1 when there are
/// several endpoints (a Raft catalog), else none (a single endpoint is a plain client).
pub fn catalog_group(group_id: Option<u64>, endpoints: usize) -> Option<u64> {
    group_id.or(if endpoints > 1 { Some(1) } else { None })
}

impl DirectConfig {
    /// Read the settings. `meta` is `TS_MATRIXOBJECT_META`'s value, already resolved by the mode.
    pub fn from_env(meta: &str) -> Result<Self, String> {
        let meta_endpoints = parse_endpoints(meta);
        if meta_endpoints.is_empty() {
            return Err("TS_MATRIXOBJECT_META names no endpoint".to_string());
        }
        if meta_endpoints.len() > 7 {
            return Err(format!(
                "TS_MATRIXOBJECT_META names {} endpoints; a metadata group has at most 7",
                meta_endpoints.len()
            ));
        }
        let number = |name: &str| -> Result<Option<u64>, String> {
            match std::env::var(name) {
                Ok(value) if !value.trim().is_empty() => value
                    .trim()
                    .parse::<u64>()
                    .map(Some)
                    .map_err(|_| format!("{name}={value:?} is not a number")),
                _ => Ok(None),
            }
        };
        let group_id = number("TS_MATRIXOBJECT_META_GROUP")?;
        if group_id == Some(0) {
            return Err("TS_MATRIXOBJECT_META_GROUP must be nonzero".to_string());
        }
        let timeout = Duration::from_millis(number("TS_MATRIXOBJECT_TIMEOUT_MS")?.unwrap_or(10_000));
        let bucket = std::env::var("TS_MATRIXOBJECT_BUCKET")
            .ok()
            .map(|bucket| bucket.trim().to_string())
            .filter(|bucket| !bucket.is_empty())
            .unwrap_or_else(|| DEFAULT_HTTP_BUCKET.to_string());
        Ok(Self {
            meta_endpoints,
            group_id,
            bucket,
            timeout,
        })
    }

    /// Connect to the metadata group and build the direct store. The cluster must answer now: a
    /// node that starts with its metadata quorum unreachable would report ready and fail every
    /// write.
    pub fn connect(&self) -> Result<NetworkedMatrixObject, String> {
        Ok(NetworkedMatrixObject::direct(self.connect_door()?, &self.bucket))
    }

    /// Build the normal direct door for clients that compose a transport.
    /// Metadata readiness, TLS and deadlines follow the regular connection path.
    pub fn connect_door(&self) -> Result<Arc<InProcessDoor>, String> {
        let tls = ApiTlsConfig::from_env("MATRIXOBJECT_CLIENT")
            .map_err(|error| format!("the cluster TLS configuration is invalid: {error}"))?;
        let options = GatewayOptions {
            write: ObjectWriteOptions::default(),
            replica: ReplicaClientOptions {
                tls: tls.clone(),
                connect_timeout: self.timeout,
                read_timeout: self.timeout,
                write_timeout: self.timeout,
                ..ReplicaClientOptions::default()
            },
            operation_timeout: self.timeout,
            ..GatewayOptions::default()
        };
        let mut meta_options = ClusterGateway::meta_options(self.timeout);
        meta_options.tls = tls;
        let endpoints = self.meta_endpoints.clone();
        let group = catalog_group(self.group_id, endpoints.len());
        let open = move || {
            if let Some(group) = group {
                RemoteMetaClient::connect_raft_catalog(&endpoints, group, &meta_options)
            } else {
                RemoteMetaClient::connect(&endpoints[0], &meta_options)
            }
        };
        let open = Arc::new(open);
        // The cluster must answer now: a node that starts with its metadata quorum unreachable
        // would report ready and fail every write.
        open().map_err(|error| {
            format!("no metadata endpoint in {} answered: {error}", self.meta_endpoints.join(","))
        })?;
        let gateway = ClusterGateway::over_chunk_objects(move || (*open)(), options, self.bucket.clone());
        Ok(Arc::new(InProcessDoor::new(Arc::new(gateway))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use bytes::Bytes;
    use matrixobjectstore_rs::net::{ObjectHttpServer, ServerOptions};
    use temporalstore_snapshot::object_store::{MatrixObjectMode, ObjectStore, ObjectStoreError};

    use crate::matrixobject_test_plane::{gateway_over as plane_gateway, MemoryPlane, Shared};
    use matrixobjectstore_rs::net::cluster_gateway::ObjectPlane;

    const BUCKET: &str = "temporalstore";

    fn gateway_over(shared: &Arc<Mutex<Shared>>) -> ClusterGateway {
        plane_gateway(shared, BUCKET)
    }

    /// Both ways in, each over its own cluster: a gateway served on a socket and read through
    /// the S3 store, and the same gateway code run in this process.
    struct Pair {
        gateway: NetworkedMatrixObject,
        direct: NetworkedMatrixObject,
        gateway_cluster: Arc<Mutex<Shared>>,
        direct_cluster: Arc<Mutex<Shared>>,
        _server: matrixobjectstore_rs::net::ServerHandle,
    }

    fn pair() -> Pair {
        let gateway_cluster = Arc::new(Mutex::new(Shared::default()));
        let direct_cluster = Arc::new(Mutex::new(Shared::default()));
        let server = ObjectHttpServer::bind_handler(
            "127.0.0.1:0",
            Arc::new(gateway_over(&gateway_cluster)),
            ServerOptions {
                worker_threads: 2,
                read_timeout: Duration::from_secs(10),
                write_timeout: Duration::from_secs(10),
                ..ServerOptions::default()
            },
        )
        .expect("binding loopback on an ephemeral port")
        .spawn()
        .expect("the accept threads start");
        let uri = format!("matrixobject://{}/{BUCKET}", server.local_addr);
        Pair {
            gateway: NetworkedMatrixObject::connect(MatrixObjectMode::Gateway, &uri).unwrap(),
            direct: NetworkedMatrixObject::direct(
                Arc::new(InProcessDoor::new(Arc::new(gateway_over(&direct_cluster)))),
                BUCKET,
            ),
            gateway_cluster,
            direct_cluster,
            _server: server,
        }
    }

    fn cluster_bytes(cluster: &Arc<Mutex<Shared>>) -> BTreeMap<String, (u64, Vec<u8>)> {
        cluster
            .lock()
            .unwrap()
            .objects
            .iter()
            .filter(|((bucket, _), _)| bucket == BUCKET)
            .map(|((_, key), (m, bytes))| (key.clone(), (m.generation, bytes.clone())))
            .collect()
    }

    /// One sequence of operations through any store, reporting everything a caller can see.
    async fn drive<S: ObjectStore>(store: &S) -> Vec<String> {
        let mut seen = Vec::new();
        seen.push(format!("absent get: {:?}", store.get("wal").await.map_err(|e| e.to_string())));
        for record in [&b"alpha"[..], b"bravo-bravo", b"c"] {
            seen.push(match store.append_blob("wal", Bytes::copy_from_slice(record)).await {
                Ok(r) => format!(
                    "append {} -> start {} end {} written {} length {}",
                    record.len(),
                    r.start_offset,
                    r.end_offset,
                    r.bytes_written,
                    r.object_length
                ),
                Err(e) => format!("append {} -> error {e}", record.len()),
            });
        }
        seen.push(format!("get {:?}", store.get("wal").await.map_err(|e| e.to_string())));
        seen.push(format!("range 3..8 {:?}", store.get_range("wal", 3, 5).await.map_err(|e| e.to_string())));
        seen.push(format!("range past {:?}", store.get_range("wal", 15, 5).await.map_err(|e| e.to_string())));
        seen.push(format!("range_from 5 {:?}", store.get_range_from("wal", 5).await.map_err(|e| e.to_string())));
        seen.push(format!("range_from end {:?}", store.get_range_from("wal", 17).await.map_err(|e| e.to_string())));
        seen.push(format!("range_from past {:?}", store.get_range_from("wal", 18).await.map_err(|e| e.to_string())));
        seen.push(format!("put {:?}", store.put("a/1", Bytes::from_static(b"one")).await.map_err(|e| e.to_string())));
        seen.push(format!("put {:?}", store.put("a/2", Bytes::from_static(b"two")).await.map_err(|e| e.to_string())));
        seen.push(format!("replace {:?}", store.put("a/1", Bytes::from_static(b"uno!")).await.map_err(|e| e.to_string())));
        seen.push(format!("get a/1 {:?}", store.get("a/1").await.map_err(|e| e.to_string())));
        seen.push(format!("list {:?}", store.list("a/").await.map_err(|e| e.to_string())));
        seen.push(format!("list_after {:?}", store.list_after("a/", "a/1").await.map_err(|e| e.to_string())));
        seen.push(format!("delete {:?}", store.delete("a/2").await.map_err(|e| e.to_string())));
        seen.push(format!("delete again {:?}", store.delete("a/2").await.map_err(|e| e.to_string())));
        seen.push(format!("get deleted {:?}", store.get("a/2").await.map_err(|e| e.to_string())));
        seen
    }

    #[test]
    fn several_endpoints_are_a_catalog_and_one_is_a_plain_client() {
        assert_eq!(catalog_group(None, 1), None);
        assert_eq!(catalog_group(None, 3), Some(1));
        assert_eq!(catalog_group(Some(7), 1), Some(7));
        assert_eq!(catalog_group(Some(7), 3), Some(7));
    }

    #[test]
    fn the_endpoint_list_is_trimmed_and_empty_entries_are_dropped() {
        assert_eq!(parse_endpoints(" m1:7000 , ,m2:7000,"), ["m1:7000", "m2:7000"]);
        assert!(parse_endpoints(" , ").is_empty());
        assert!(DirectConfig::from_env(" ").is_err());
        assert!(DirectConfig::from_env("1:1,2:2,3:3,4:4,5:5,6:6,7:7,8:8").is_err());
    }

    #[tokio::test]
    async fn conditional_writes_preserve_values_in_both_modes() {
        let pair = pair();
        for store in [&pair.gateway, &pair.direct] {
            store.compare_and_swap("lease", None, Bytes::from_static(b"owner-1")).await.unwrap();
            assert!(matches!(store.compare_and_swap("lease", None, Bytes::from_static(b"bad")).await,
                Err(ObjectStoreError::ConditionFailed { .. })));
            assert!(matches!(store.compare_and_swap("lease", Some(Bytes::from_static(b"other")), Bytes::from_static(b"bad")).await,
                Err(ObjectStoreError::ConditionFailed { .. })));
            assert_eq!(store.get("lease").await.unwrap(), Bytes::from_static(b"owner-1"));
            store.compare_and_swap("lease", Some(Bytes::from_static(b"owner-1")), Bytes::from_static(b"owner-2")).await.unwrap();
            assert!(matches!(store.compare_and_swap("lease", Some(Bytes::from_static(b"owner-1")), Bytes::from_static(b"bad")).await,
                Err(ObjectStoreError::ConditionFailed { .. })));
            assert_eq!(store.get("lease").await.unwrap(), Bytes::from_static(b"owner-2"));
        }
    }

    #[tokio::test]
    async fn identical_bytes_through_a_gateway_and_directly() {
        let pair = pair();
        let through_gateway = drive(&pair.gateway).await;
        let through_direct = drive(&pair.direct).await;
        assert_eq!(through_gateway, through_direct);
        // What each cluster durably holds: the same keys, the same generations, the same bytes.
        assert_eq!(cluster_bytes(&pair.gateway_cluster), cluster_bytes(&pair.direct_cluster));
        assert_eq!(cluster_bytes(&pair.direct_cluster)["wal"].1, b"alphabravo-bravoc");
        // The control: the drive reached the cluster and answered, so two empty runs would fail.
        assert!(through_gateway.iter().any(|line| line.starts_with("append 5 -> start 0 end 5")), "{through_gateway:?}");
        assert_eq!(pair.direct.wire_bytes_sent(), 0, "the direct way put nothing on a wire");
        assert!(pair.gateway.wire_bytes_sent() > 0);
    }

    /// The gateway's own read-back resolves an append whose publication was not confirmed, in
    /// either way in: the bytes are stored once, and the receipt says where they landed.
    #[tokio::test]
    async fn an_unconfirmed_publication_is_resolved_by_reading_in_both_ways() {
        let pair = pair();
        // The first append to a missing object creates it; the fault applies to a real append.
        pair.gateway.append_blob("wal", Bytes::from_static(b"seed")).await.unwrap();
        pair.direct.append_blob("wal", Bytes::from_static(b"seed")).await.unwrap();
        pair.gateway_cluster.lock().unwrap().commit_then_unconfirmed = true;
        pair.direct_cluster.lock().unwrap().commit_then_unconfirmed = true;
        let a = pair.gateway.append_blob("wal", Bytes::from_static(b"record-one")).await.unwrap();
        let b = pair.direct.append_blob("wal", Bytes::from_static(b"record-one")).await.unwrap();
        assert_eq!((a.start_offset, a.end_offset, a.object_length), (4, 14, 14));
        assert_eq!((b.start_offset, b.end_offset, b.object_length), (4, 14, 14));
        assert_eq!(cluster_bytes(&pair.gateway_cluster), cluster_bytes(&pair.direct_cluster));
        assert_eq!(cluster_bytes(&pair.direct_cluster)["wal"].1, b"seedrecord-one");
        assert_eq!(pair.direct_cluster.lock().unwrap().appends, 1, "the append was not sent again");
        assert_eq!(pair.gateway_cluster.lock().unwrap().appends, 1);
    }

    /// The reply to an append is lost AND the append never committed: the case a read cannot
    /// settle, because the object ends exactly where the append was to start and a late attempt
    /// could still commit. The resolve fences the lost attempt with an unchanged publication and
    /// answers absent; the retry lands once. Same bytes and same generation, both ways in.
    #[tokio::test]
    async fn an_append_lost_before_it_committed_is_fenced_and_lands_once_in_both_ways() {
        let pair = pair();
        pair.gateway.append_blob("wal", Bytes::from_static(b"seed")).await.unwrap();
        pair.direct.append_blob("wal", Bytes::from_static(b"seed")).await.unwrap();
        let before = |cluster: &Arc<Mutex<Shared>>| {
            cluster.lock().unwrap().objects.get(&(BUCKET.to_string(), "wal".to_string())).unwrap().0.clone()
        };
        let stale = [before(&pair.gateway_cluster), before(&pair.direct_cluster)];
        pair.gateway_cluster.lock().unwrap().lose_then_unconfirmed = true;
        pair.direct_cluster.lock().unwrap().lose_then_unconfirmed = true;
        let a = pair.gateway.append_blob("wal", Bytes::from_static(b"record-one")).await.unwrap();
        let b = pair.direct.append_blob("wal", Bytes::from_static(b"record-one")).await.unwrap();
        assert_eq!((a.start_offset, a.end_offset, a.object_length), (4, 14, 14));
        assert_eq!((b.start_offset, b.end_offset, b.object_length), (4, 14, 14));
        assert_eq!(cluster_bytes(&pair.gateway_cluster), cluster_bytes(&pair.direct_cluster));
        assert_eq!(cluster_bytes(&pair.direct_cluster)["wal"].1, b"seedrecord-one", "stored once");
        for (cluster, stale) in [&pair.gateway_cluster, &pair.direct_cluster].into_iter().zip(stale) {
            {
                let shared = cluster.lock().unwrap();
                // The gateway fences the lost attempt when it decides the append, and again when the
                // client asks it to resolve that attempt: two fences, and the object is unchanged by both.
                assert_eq!(shared.fences, 2, "the lost attempt was fenced by the gateway and by the resolve");
                assert_eq!(shared.appends, 2, "the lost attempt, then the retry");
            }
            // The attempt that was lost can never commit now: it builds on a generation the fence passed.
            let late = MemoryPlane(cluster.clone()).append(stale, b"record-one");
            assert!(late.is_err(), "a late attempt on the old generation must be refused");
            assert_eq!(cluster.lock().unwrap().objects.get(&(BUCKET.to_string(), "wal".to_string())).unwrap().1, b"seedrecord-one");
        }
    }

    /// A long run of sequential appends, in both ways, against the gateway's real append path:
    /// none wedges, every receipt agrees, and the two clusters end up byte for byte equal.
    #[tokio::test]
    async fn three_thousand_sequential_appends_agree_through_a_gateway_and_directly() {
        const APPENDS: usize = 3000;
        const SIZE: usize = 1024;
        let pair = pair();
        for index in 0..APPENDS {
            let record = vec![(index % 251) as u8; SIZE];
            let a = pair
                .gateway
                .append_blob("wal", Bytes::from(record.clone()))
                .await
                .unwrap_or_else(|e| panic!("gateway append {index}: {e}"));
            let b = pair
                .direct
                .append_blob("wal", Bytes::from(record))
                .await
                .unwrap_or_else(|e| panic!("direct append {index}: {e}"));
            assert_eq!(
                (a.start_offset, a.end_offset, a.object_length),
                (b.start_offset, b.end_offset, b.object_length),
                "receipts diverged at append {index}"
            );
            assert_eq!(a.start_offset, (index * SIZE) as u64, "append {index} landed off the end");
        }
        assert_eq!(cluster_bytes(&pair.direct_cluster)["wal"].1.len(), APPENDS * SIZE);
        assert!(
            cluster_bytes(&pair.gateway_cluster) == cluster_bytes(&pair.direct_cluster),
            "the stored objects differ"
        );
    }

    #[test]
    fn direct_mode_connects_or_says_why_not() {
        // No metadata endpoint is listening on this port, so the node refuses to start the way
        // the gateway binary does, naming the endpoint, instead of reporting ready.
        let config = DirectConfig {
            meta_endpoints: vec!["127.0.0.1:1".to_string()],
            group_id: None,
            bucket: BUCKET.to_string(),
            timeout: Duration::from_millis(500),
        };
        let error = config.connect().expect_err("nothing answers");
        assert!(error.contains("127.0.0.1:1"), "{error}");
    }
}
