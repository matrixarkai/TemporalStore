use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::engine::TemporalEngine;
use crate::types::{
    BatchExecuteRequest, Command, CommandResponse, ExecuteResponse, ShardId, Status,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IngestionSource {
    Api {
        request_id: String,
    },
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
        #[serde(default)]
        key: Option<String>,
        #[serde(default)]
        timestamp_ms: Option<u64>,
    },
    Flink {
        job_id: String,
        operator_uid: String,
        subtask_index: u32,
        checkpoint_id: u64,
        record_index: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionRecord {
    pub source: IngestionSource,
    pub shard_id: ShardId,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionBatchRequest {
    pub records: Vec<IngestionRecord>,
    #[serde(default)]
    pub stop_on_error: bool,
    #[serde(default)]
    pub kafka_high_watermarks: Vec<KafkaHighWatermark>,
    #[serde(default)]
    pub flink_checkpoints: Vec<FlinkCheckpointUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionRecordResult {
    pub index: usize,
    pub source: IngestionSource,
    pub shard_id: ShardId,
    pub status: Status,
    pub response: CommandResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionBatchReport {
    pub status: Status,
    pub accepted_count: usize,
    pub failed_count: usize,
    pub duplicate_count: usize,
    #[serde(default)]
    pub dead_letters: Vec<IngestionDeadLetter>,
    #[serde(default)]
    pub kafka_offsets: Vec<KafkaOffsetLedgerEntry>,
    #[serde(default)]
    pub flink_checkpoints: Vec<FlinkCheckpointState>,
    #[serde(default)]
    pub max_kafka_lag: i64,
    #[serde(default = "Status::ok")]
    pub state_persist_status: Status,
    pub results: Vec<IngestionRecordResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaHighWatermark {
    pub topic: String,
    pub partition: i32,
    pub high_watermark_offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaOffsetLedgerEntry {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: i64,
    pub updated_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FlinkCheckpointAction {
    Precommit,
    Commit,
    Abort,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlinkCheckpointUpdate {
    pub job_id: String,
    pub operator_uid: String,
    pub subtask_index: u32,
    pub checkpoint_id: u64,
    pub action: FlinkCheckpointAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FlinkCheckpointStatus {
    Precommitted,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlinkCheckpointState {
    pub job_id: String,
    pub operator_uid: String,
    pub subtask_index: u32,
    pub checkpoint_id: u64,
    pub status: FlinkCheckpointStatus,
    pub updated_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionDeadLetter {
    pub index: usize,
    pub source: IngestionSource,
    pub shard_id: ShardId,
    pub status: Status,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestionStats {
    pub accepted_total: u64,
    pub failed_total: u64,
    pub duplicate_total: u64,
    pub dead_letter_total: u64,
    pub kafka_committed_total: u64,
    pub flink_precommit_total: u64,
    pub flink_commit_total: u64,
    pub flink_abort_total: u64,
    pub max_kafka_lag: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestionStateReport {
    pub status: Status,
    pub stats: IngestionStats,
    pub kafka_offsets: Vec<KafkaOffsetLedgerEntry>,
    pub flink_checkpoints: Vec<FlinkCheckpointState>,
    pub dead_letters: Vec<IngestionDeadLetter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestionReadinessReport {
    pub production_ready: bool,
    pub covered: Vec<String>,
    pub missing: Vec<String>,
    pub blocker_count: usize,
}

impl Default for IngestionStateReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            stats: IngestionStats::default(),
            kafka_offsets: Vec::new(),
            flink_checkpoints: Vec::new(),
            dead_letters: Vec::new(),
        }
    }
}

pub fn ingestion_readiness_report() -> IngestionReadinessReport {
    let covered = vec![
        "proxy/table ingestion route accepts API, Kafka, and Flink sourced records".to_string(),
        "durable Kafka offset ledger rejects duplicate committed offsets before executing writes"
            .to_string(),
        "Flink checkpoint precommit, commit, and abort lifecycle is durably tracked".to_string(),
        "dead-letter records preserve source, shard, status, and failed index".to_string(),
        "Kafka lag and ingestion/dead-letter counters are exposed through state reports"
            .to_string(),
        "ingestion state is persisted through atomic temp-file rename".to_string(),
    ];
    let missing = vec![
        "network Kafka consumer group runtime with partition assignment, rebalance, and backpressure"
            .to_string(),
        "network Flink sink/source connector with checkpoint handshake over the production API"
            .to_string(),
        "Raft failover and restart harness that proves offset/checkpoint idempotence end-to-end"
            .to_string(),
        "Prometheus ingestion lag/dead-letter metrics from live proxy and data-node processes"
            .to_string(),
    ];
    IngestionReadinessReport {
        production_ready: missing.is_empty(),
        blocker_count: missing.len(),
        covered,
        missing,
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
struct DurableIngestionState {
    #[serde(default)]
    kafka_offsets: BTreeMap<String, KafkaOffsetLedgerEntry>,
    #[serde(default)]
    flink_checkpoints: BTreeMap<String, FlinkCheckpointState>,
    #[serde(default)]
    dead_letters: Vec<IngestionDeadLetter>,
    #[serde(default)]
    stats: IngestionStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IngestionValidationReport {
    status: Status,
    duplicate_indexes: BTreeSet<usize>,
}

impl TemporalEngine {
    pub fn ingest_batch(&self, request: IngestionBatchRequest) -> IngestionBatchReport {
        let validation = validate_ingestion_batch(&request.records);
        let mut state = load_ingestion_state(&self.ingestion_dir()).unwrap_or_default();
        apply_flink_checkpoint_updates(&mut state, &request.flink_checkpoints);
        let mut results = Vec::with_capacity(request.records.len());
        let mut accepted_count = 0usize;
        let mut failed_count = 0usize;
        let mut durable_duplicate_count = 0usize;
        let mut dead_letters = Vec::new();
        let now = now_unix_ms();

        for (index, record) in request.records.into_iter().enumerate() {
            let durable_duplicate = durable_kafka_duplicate(&state, &record.source);
            let batch_duplicate = validation.duplicate_indexes.contains(&index);
            if batch_duplicate || durable_duplicate {
                if durable_duplicate && !batch_duplicate {
                    durable_duplicate_count = durable_duplicate_count.saturating_add(1);
                }
                failed_count += 1;
                state.stats.duplicate_total = state.stats.duplicate_total.saturating_add(1);
                let status = Status::error(
                    "duplicate_ingestion_record",
                    "duplicate Kafka topic/partition/offset in ingestion ledger",
                );
                dead_letters.push(IngestionDeadLetter {
                    index,
                    source: record.source.clone(),
                    shard_id: record.shard_id,
                    status: status.clone(),
                });
                results.push(IngestionRecordResult {
                    index,
                    source: record.source,
                    shard_id: record.shard_id,
                    status,
                    response: CommandResponse::Empty,
                });
                if request.stop_on_error {
                    break;
                }
                continue;
            }

            let batch = self.batch_execute(BatchExecuteRequest {
                shard_id: record.shard_id,
                commands: vec![record.command],
            });
            let response = batch
                .responses
                .into_iter()
                .next()
                .unwrap_or(ExecuteResponse {
                    status: Status::error(
                        "missing_ingestion_response",
                        "engine returned no response",
                    ),
                    response: CommandResponse::Empty,
                });
            if response.status.ok {
                accepted_count += 1;
                commit_kafka_offset(&mut state, &record.source, now);
            } else {
                failed_count += 1;
                dead_letters.push(IngestionDeadLetter {
                    index,
                    source: record.source.clone(),
                    shard_id: record.shard_id,
                    status: response.status.clone(),
                });
            }
            let failed = !response.status.ok;
            results.push(IngestionRecordResult {
                index,
                source: record.source,
                shard_id: record.shard_id,
                status: response.status,
                response: response.response,
            });
            if failed && request.stop_on_error {
                break;
            }
        }

        let status = if !validation.status.ok {
            validation.status
        } else if failed_count == 0 {
            Status::ok()
        } else {
            Status::error(
                "partial_ingestion_failure",
                format!("{failed_count} ingestion records failed"),
            )
        };
        state.stats.accepted_total = state
            .stats
            .accepted_total
            .saturating_add(accepted_count as u64);
        state.stats.failed_total = state.stats.failed_total.saturating_add(failed_count as u64);
        state.stats.dead_letter_total = state
            .stats
            .dead_letter_total
            .saturating_add(dead_letters.len() as u64);
        state.dead_letters.extend(dead_letters.clone());
        state.stats.max_kafka_lag = compute_max_kafka_lag(&state, &request.kafka_high_watermarks);
        let state_persist_status = persist_ingestion_state(&self.ingestion_dir(), &state);
        IngestionBatchReport {
            status,
            accepted_count,
            failed_count,
            duplicate_count: validation
                .duplicate_indexes
                .len()
                .saturating_add(durable_duplicate_count),
            dead_letters,
            kafka_offsets: state.kafka_offsets.values().cloned().collect(),
            flink_checkpoints: state.flink_checkpoints.values().cloned().collect(),
            max_kafka_lag: state.stats.max_kafka_lag,
            state_persist_status,
            results,
        }
    }

    pub fn ingestion_state_report(&self) -> IngestionStateReport {
        match load_ingestion_state(&self.ingestion_dir()) {
            Ok(state) => IngestionStateReport {
                status: Status::ok(),
                stats: state.stats,
                kafka_offsets: state.kafka_offsets.values().cloned().collect(),
                flink_checkpoints: state.flink_checkpoints.values().cloned().collect(),
                dead_letters: state.dead_letters,
            },
            Err(status) => IngestionStateReport {
                status,
                ..IngestionStateReport::default()
            },
        }
    }
}

fn validate_ingestion_batch(records: &[IngestionRecord]) -> IngestionValidationReport {
    let mut kafka_offsets = BTreeSet::new();
    let mut flink_indexes = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for (index, record) in records.iter().enumerate() {
        match &record.source {
            IngestionSource::Kafka {
                topic,
                partition,
                offset,
                ..
            } => {
                if *partition < 0 || *offset < 0 {
                    return IngestionValidationReport {
                        status: Status::error(
                            "invalid_kafka_position",
                            "Kafka partition and offset must be non-negative",
                        ),
                        duplicate_indexes: duplicates,
                    };
                }
                if !kafka_offsets.insert((topic.clone(), *partition, *offset)) {
                    duplicates.insert(index);
                }
            }
            IngestionSource::Flink {
                job_id,
                operator_uid,
                checkpoint_id,
                record_index,
                ..
            } => {
                if job_id.is_empty() || operator_uid.is_empty() {
                    return IngestionValidationReport {
                        status: Status::error(
                            "invalid_flink_source",
                            "Flink job_id and operator_uid are required",
                        ),
                        duplicate_indexes: duplicates,
                    };
                }
                if !flink_indexes.insert((
                    job_id.clone(),
                    operator_uid.clone(),
                    *checkpoint_id,
                    *record_index,
                )) {
                    duplicates.insert(index);
                }
            }
            IngestionSource::Api { request_id } if request_id.is_empty() => {
                return IngestionValidationReport {
                    status: Status::error("invalid_api_source", "API request_id is required"),
                    duplicate_indexes: duplicates,
                };
            }
            IngestionSource::Api { .. } => {}
        }
    }
    IngestionValidationReport {
        status: Status::ok(),
        duplicate_indexes: duplicates,
    }
}

fn ingestion_state_path(root: &Path) -> PathBuf {
    root.join("state.json")
}

fn load_ingestion_state(root: &Path) -> Result<DurableIngestionState, Status> {
    let path = ingestion_state_path(root);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice::<DurableIngestionState>(&bytes)
            .map_err(|err| Status::error("bad_ingestion_state", err.to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            Ok(DurableIngestionState::default())
        }
        Err(err) => Err(Status::error("ingestion_state_io", err.to_string())),
    }
}

fn persist_ingestion_state(root: &Path, state: &DurableIngestionState) -> Status {
    if let Err(err) = fs::create_dir_all(root) {
        return Status::error("ingestion_state_io", err.to_string());
    }
    let path = ingestion_state_path(root);
    let temp_path = path.with_extension("json.tmp");
    let bytes = match serde_json::to_vec_pretty(state) {
        Ok(bytes) => bytes,
        Err(err) => return Status::error("ingestion_state_serialize", err.to_string()),
    };
    if let Err(err) = fs::write(&temp_path, bytes) {
        return Status::error("ingestion_state_io", err.to_string());
    }
    if let Err(err) = fs::rename(&temp_path, &path) {
        let _ = fs::remove_file(&temp_path);
        return Status::error("ingestion_state_io", err.to_string());
    }
    Status::ok()
}

fn kafka_offset_key(topic: &str, partition: i32) -> String {
    format!("{topic}:{partition}")
}

fn flink_checkpoint_key(
    job_id: &str,
    operator_uid: &str,
    subtask_index: u32,
    checkpoint_id: u64,
) -> String {
    format!("{job_id}:{operator_uid}:{subtask_index}:{checkpoint_id}")
}

fn durable_kafka_duplicate(state: &DurableIngestionState, source: &IngestionSource) -> bool {
    let IngestionSource::Kafka {
        topic,
        partition,
        offset,
        ..
    } = source
    else {
        return false;
    };
    state
        .kafka_offsets
        .get(&kafka_offset_key(topic, *partition))
        .map(|entry| *offset <= entry.committed_offset)
        .unwrap_or(false)
}

fn commit_kafka_offset(state: &mut DurableIngestionState, source: &IngestionSource, now: u64) {
    let IngestionSource::Kafka {
        topic,
        partition,
        offset,
        ..
    } = source
    else {
        return;
    };
    let key = kafka_offset_key(topic, *partition);
    let should_update = state
        .kafka_offsets
        .get(&key)
        .map(|entry| *offset > entry.committed_offset)
        .unwrap_or(true);
    if should_update {
        state.kafka_offsets.insert(
            key,
            KafkaOffsetLedgerEntry {
                topic: topic.clone(),
                partition: *partition,
                committed_offset: *offset,
                updated_unix_ms: now,
            },
        );
        state.stats.kafka_committed_total = state.stats.kafka_committed_total.saturating_add(1);
    }
}

fn apply_flink_checkpoint_updates(
    state: &mut DurableIngestionState,
    updates: &[FlinkCheckpointUpdate],
) {
    let now = now_unix_ms();
    for update in updates {
        let status = match update.action {
            FlinkCheckpointAction::Precommit => {
                state.stats.flink_precommit_total =
                    state.stats.flink_precommit_total.saturating_add(1);
                FlinkCheckpointStatus::Precommitted
            }
            FlinkCheckpointAction::Commit => {
                state.stats.flink_commit_total = state.stats.flink_commit_total.saturating_add(1);
                FlinkCheckpointStatus::Committed
            }
            FlinkCheckpointAction::Abort => {
                state.stats.flink_abort_total = state.stats.flink_abort_total.saturating_add(1);
                FlinkCheckpointStatus::Aborted
            }
        };
        state.flink_checkpoints.insert(
            flink_checkpoint_key(
                &update.job_id,
                &update.operator_uid,
                update.subtask_index,
                update.checkpoint_id,
            ),
            FlinkCheckpointState {
                job_id: update.job_id.clone(),
                operator_uid: update.operator_uid.clone(),
                subtask_index: update.subtask_index,
                checkpoint_id: update.checkpoint_id,
                status,
                updated_unix_ms: now,
            },
        );
    }
}

fn compute_max_kafka_lag(
    state: &DurableIngestionState,
    high_watermarks: &[KafkaHighWatermark],
) -> i64 {
    high_watermarks
        .iter()
        .filter_map(|watermark| {
            let committed = state
                .kafka_offsets
                .get(&kafka_offset_key(&watermark.topic, watermark.partition))?
                .committed_offset;
            Some(watermark.high_watermark_offset.saturating_sub(committed))
        })
        .max()
        .unwrap_or_default()
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::LoadShardRequest;
    use crate::types::{CommandResponse, SequenceFeatureRow};

    fn loaded_engine() -> TemporalEngine {
        let engine = TemporalEngine::default();
        let response = engine.load_shard_with(LoadShardRequest {
            shard_id: 7,
            load_version: 1,
            local_node_id: Some(1),
            shard_uri: "memory://ingestion".to_string(),
            start_routing_slot: 0,
            end_routing_slot: 1023,
            readonly: false,
            table_name: "ingestion_table".to_string(),
        });
        assert!(response.status.ok);
        engine
    }

    #[test]
    fn ingestion_batch_executes_api_kafka_and_flink_records() {
        let engine = loaded_engine();
        let report = engine.ingest_batch(IngestionBatchRequest {
            stop_on_error: false,
            kafka_high_watermarks: Vec::new(),
            flink_checkpoints: Vec::new(),
            records: vec![
                IngestionRecord {
                    source: IngestionSource::Api {
                        request_id: "api-1".to_string(),
                    },
                    shard_id: 7,
                    command: Command::StringSet {
                        key: "api-key".to_string(),
                        value: b"api-value".to_vec(),
                    },
                },
                IngestionRecord {
                    source: IngestionSource::Kafka {
                        topic: "feature-topic".to_string(),
                        partition: 0,
                        offset: 1,
                        key: Some("feature-key".to_string()),
                        timestamp_ms: Some(10),
                    },
                    shard_id: 7,
                    command: Command::FeatureAppend {
                        key: "feature-key".to_string(),
                        points: vec![crate::FeaturePoint {
                            timestamp_ms: 10,
                            value: b"feature".to_vec(),
                        }],
                    },
                },
                IngestionRecord {
                    source: IngestionSource::Flink {
                        job_id: "job-a".to_string(),
                        operator_uid: "op-a".to_string(),
                        subtask_index: 2,
                        checkpoint_id: 11,
                        record_index: 1,
                    },
                    shard_id: 7,
                    command: Command::SequenceAdd {
                        key: "sequence-key".to_string(),
                        rows: vec![SequenceFeatureRow {
                            timestamp_ms: 11,
                            gid: 9,
                            action_type: 3,
                            duration: 12,
                            author_id: 42,
                        }],
                    },
                },
            ],
        });

        assert!(report.status.ok, "{report:?}");
        assert_eq!(report.accepted_count, 3);
        assert_eq!(report.failed_count, 0);
        assert_eq!(report.results.len(), 3);

        let read = engine.execute(crate::ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "api-key".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"api-value".to_vec())
            }
        );
    }

    #[test]
    fn ingestion_batch_reports_duplicate_kafka_offsets_without_nooping_valid_records() {
        let engine = loaded_engine();
        let report = engine.ingest_batch(IngestionBatchRequest {
            stop_on_error: false,
            kafka_high_watermarks: Vec::new(),
            flink_checkpoints: Vec::new(),
            records: vec![
                IngestionRecord {
                    source: IngestionSource::Kafka {
                        topic: "topic-a".to_string(),
                        partition: 1,
                        offset: 9,
                        key: None,
                        timestamp_ms: None,
                    },
                    shard_id: 7,
                    command: Command::StringSet {
                        key: "first".to_string(),
                        value: b"one".to_vec(),
                    },
                },
                IngestionRecord {
                    source: IngestionSource::Kafka {
                        topic: "topic-a".to_string(),
                        partition: 1,
                        offset: 9,
                        key: None,
                        timestamp_ms: None,
                    },
                    shard_id: 7,
                    command: Command::StringSet {
                        key: "duplicate".to_string(),
                        value: b"two".to_vec(),
                    },
                },
            ],
        });

        assert_eq!(report.status.code, "partial_ingestion_failure");
        assert_eq!(report.accepted_count, 1);
        assert_eq!(report.failed_count, 1);
        assert_eq!(report.duplicate_count, 1);
        assert_eq!(report.results[1].status.code, "duplicate_ingestion_record");

        let duplicate = engine.execute(crate::ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "duplicate".to_string(),
            },
        });
        assert_eq!(duplicate.response, CommandResponse::Bytes { value: None });
    }

    #[test]
    fn ingestion_persists_kafka_ledger_dead_letters_lag_and_flink_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine =
            TemporalEngine::with_local_dirs(1024 * 1024, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(7);

        let report = engine.ingest_batch(IngestionBatchRequest {
            stop_on_error: false,
            kafka_high_watermarks: vec![KafkaHighWatermark {
                topic: "topic-a".to_string(),
                partition: 0,
                high_watermark_offset: 7,
            }],
            flink_checkpoints: vec![FlinkCheckpointUpdate {
                job_id: "job-a".to_string(),
                operator_uid: "sink".to_string(),
                subtask_index: 0,
                checkpoint_id: 42,
                action: FlinkCheckpointAction::Precommit,
            }],
            records: vec![IngestionRecord {
                source: IngestionSource::Kafka {
                    topic: "topic-a".to_string(),
                    partition: 0,
                    offset: 5,
                    key: None,
                    timestamp_ms: None,
                },
                shard_id: 7,
                command: Command::StringSet {
                    key: "ledger-key".to_string(),
                    value: b"ledger-value".to_vec(),
                },
            }],
        });
        assert!(report.status.ok, "{report:?}");
        assert_eq!(report.accepted_count, 1);
        assert_eq!(report.kafka_offsets[0].committed_offset, 5);
        assert_eq!(report.max_kafka_lag, 2);
        assert_eq!(
            report.flink_checkpoints[0].status,
            FlinkCheckpointStatus::Precommitted
        );
        assert!(report.state_persist_status.ok);

        let restarted =
            TemporalEngine::with_local_dirs(1024 * 1024, &cache_dir, &page_dir, &index_dir);
        restarted.load_shard(7);
        let duplicate = restarted.ingest_batch(IngestionBatchRequest {
            stop_on_error: false,
            kafka_high_watermarks: vec![KafkaHighWatermark {
                topic: "topic-a".to_string(),
                partition: 0,
                high_watermark_offset: 7,
            }],
            flink_checkpoints: vec![FlinkCheckpointUpdate {
                job_id: "job-a".to_string(),
                operator_uid: "sink".to_string(),
                subtask_index: 0,
                checkpoint_id: 42,
                action: FlinkCheckpointAction::Commit,
            }],
            records: vec![IngestionRecord {
                source: IngestionSource::Kafka {
                    topic: "topic-a".to_string(),
                    partition: 0,
                    offset: 5,
                    key: None,
                    timestamp_ms: None,
                },
                shard_id: 7,
                command: Command::StringSet {
                    key: "should-not-write".to_string(),
                    value: b"duplicate".to_vec(),
                },
            }],
        });
        assert_eq!(duplicate.status.code, "partial_ingestion_failure");
        assert_eq!(duplicate.duplicate_count, 1);
        assert_eq!(duplicate.dead_letters.len(), 1);
        assert_eq!(
            duplicate.flink_checkpoints[0].status,
            FlinkCheckpointStatus::Committed
        );

        let state = restarted.ingestion_state_report();
        assert!(state.status.ok);
        assert_eq!(state.kafka_offsets[0].committed_offset, 5);
        assert_eq!(state.dead_letters.len(), 1);
        assert_eq!(state.stats.duplicate_total, 1);
        assert_eq!(state.stats.dead_letter_total, 1);

        let missing = restarted.execute(crate::ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "should-not-write".to_string(),
            },
        });
        assert_eq!(missing.response, CommandResponse::Bytes { value: None });
    }

    #[test]
    fn ingestion_readiness_report_tracks_done_and_remaining_production_gaps() {
        let report = ingestion_readiness_report();
        assert!(!report.production_ready);
        assert_eq!(report.blocker_count, report.missing.len());
        assert!(report
            .covered
            .iter()
            .any(|item| item.contains("durable Kafka offset ledger")));
        assert!(report
            .covered
            .iter()
            .any(|item| item.contains("Flink checkpoint precommit")));
        assert!(report
            .missing
            .iter()
            .any(|item| item.contains("consumer group runtime")));
        assert!(report
            .missing
            .iter()
            .any(|item| item.contains("Raft failover")));
    }
}
