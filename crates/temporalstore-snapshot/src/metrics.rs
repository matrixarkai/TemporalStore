use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicI64;

use crate::types::SnapshotStatus;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SnapshotStatusLabels {
    shard_id: String,
    status: StatusLabel,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ShardLabels {
    shard_id: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum StatusLabel {
    Success,
    Failure,
}

impl From<SnapshotStatus> for StatusLabel {
    fn from(value: SnapshotStatus) -> Self {
        match value {
            SnapshotStatus::Success => StatusLabel::Success,
            SnapshotStatus::Failure => StatusLabel::Failure,
        }
    }
}

#[derive(Clone)]
pub struct SnapshotMetrics {
    create_total: Family<SnapshotStatusLabels, Counter>,
    upload_total: Family<SnapshotStatusLabels, Counter>,
    download_total: Family<SnapshotStatusLabels, Counter>,
    restore_total: Family<SnapshotStatusLabels, Counter>,
    snapshot_bytes: Family<ShardLabels, Gauge<i64, AtomicI64>>,
    upload_seconds: Family<ShardLabels, Gauge<i64, AtomicI64>>,
    download_seconds: Family<ShardLabels, Gauge<i64, AtomicI64>>,
    last_success_log_index: Family<ShardLabels, Gauge<i64, AtomicI64>>,
}

impl SnapshotMetrics {
    pub fn register(registry: &mut Registry) -> Self {
        let create_total = Family::<SnapshotStatusLabels, Counter>::default();
        let upload_total = Family::<SnapshotStatusLabels, Counter>::default();
        let download_total = Family::<SnapshotStatusLabels, Counter>::default();
        let restore_total = Family::<SnapshotStatusLabels, Counter>::default();
        let snapshot_bytes = Family::<ShardLabels, Gauge<i64, AtomicI64>>::default();
        let upload_seconds = Family::<ShardLabels, Gauge<i64, AtomicI64>>::default();
        let download_seconds = Family::<ShardLabels, Gauge<i64, AtomicI64>>::default();
        let last_success_log_index = Family::<ShardLabels, Gauge<i64, AtomicI64>>::default();

        registry.register(
            "temporalstore_snapshot_create",
            "Total local snapshot creation attempts.",
            create_total.clone(),
        );
        registry.register(
            "temporalstore_snapshot_upload",
            "Total snapshot upload attempts.",
            upload_total.clone(),
        );
        registry.register(
            "temporalstore_snapshot_download",
            "Total snapshot download attempts.",
            download_total.clone(),
        );
        registry.register(
            "temporalstore_snapshot_restore",
            "Total snapshot restore attempts.",
            restore_total.clone(),
        );
        registry.register(
            "temporalstore_snapshot_bytes",
            "Byte size of the latest observed snapshot.",
            snapshot_bytes.clone(),
        );
        registry.register(
            "temporalstore_snapshot_upload_seconds",
            "Upload duration for the latest snapshot upload.",
            upload_seconds.clone(),
        );
        registry.register(
            "temporalstore_snapshot_download_seconds",
            "Download duration for the latest snapshot download.",
            download_seconds.clone(),
        );
        registry.register(
            "temporalstore_snapshot_last_success_log_index",
            "Last successfully uploaded snapshot log index.",
            last_success_log_index.clone(),
        );

        Self {
            create_total,
            upload_total,
            download_total,
            restore_total,
            snapshot_bytes,
            upload_seconds,
            download_seconds,
            last_success_log_index,
        }
    }

    pub fn observe_create(&self, shard_id: u64, status: SnapshotStatus) {
        self.create_total
            .get_or_create(&status_labels(shard_id, status))
            .inc();
    }

    pub fn observe_upload(&self, shard_id: u64, status: SnapshotStatus, bytes: u64, seconds: u64) {
        self.upload_total
            .get_or_create(&status_labels(shard_id, status))
            .inc();
        let labels = shard_labels(shard_id);
        self.snapshot_bytes
            .get_or_create(&labels)
            .set(to_i64(bytes));
        self.upload_seconds
            .get_or_create(&labels)
            .set(to_i64(seconds));
    }

    pub fn observe_download(&self, shard_id: u64, status: SnapshotStatus, seconds: u64) {
        self.download_total
            .get_or_create(&status_labels(shard_id, status))
            .inc();
        self.download_seconds
            .get_or_create(&shard_labels(shard_id))
            .set(to_i64(seconds));
    }

    pub fn observe_restore(&self, shard_id: u64, status: SnapshotStatus) {
        self.restore_total
            .get_or_create(&status_labels(shard_id, status))
            .inc();
    }

    pub fn set_last_success_log_index(&self, shard_id: u64, index: u64) {
        self.last_success_log_index
            .get_or_create(&shard_labels(shard_id))
            .set(to_i64(index));
    }
}

fn status_labels(shard_id: u64, status: SnapshotStatus) -> SnapshotStatusLabels {
    SnapshotStatusLabels {
        shard_id: shard_id.to_string(),
        status: status.into(),
    }
}

fn shard_labels(shard_id: u64) -> ShardLabels {
    ShardLabels {
        shard_id: shard_id.to_string(),
    }
}

fn to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}
