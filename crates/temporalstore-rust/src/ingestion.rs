use std::collections::BTreeSet;

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
    pub results: Vec<IngestionRecordResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IngestionValidationReport {
    status: Status,
    duplicate_indexes: BTreeSet<usize>,
}

impl TemporalEngine {
    pub fn ingest_batch(&self, request: IngestionBatchRequest) -> IngestionBatchReport {
        let validation = validate_ingestion_batch(&request.records);
        let mut results = Vec::with_capacity(request.records.len());
        let mut accepted_count = 0usize;
        let mut failed_count = 0usize;

        for (index, record) in request.records.into_iter().enumerate() {
            if validation.duplicate_indexes.contains(&index) {
                failed_count += 1;
                results.push(IngestionRecordResult {
                    index,
                    source: record.source,
                    shard_id: record.shard_id,
                    status: Status::error(
                        "duplicate_ingestion_record",
                        "duplicate Kafka topic/partition/offset in ingestion batch",
                    ),
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
            } else {
                failed_count += 1;
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
        IngestionBatchReport {
            status,
            accepted_count,
            failed_count,
            duplicate_count: validation.duplicate_indexes.len(),
            results,
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
}
