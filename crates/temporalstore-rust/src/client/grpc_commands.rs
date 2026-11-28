// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::sdk::v1::{self, command::Kind, CommandResultKind as ResultKind};
use std::future::Future;

impl TemporalStoreTable {
    pub(super) fn execute_native_grpc(
        &self,
        command: &Command,
    ) -> Result<ExecuteResponse, ClientError> {
        let command = encode_command(command)?;
        let table = self.clone();
        let response = match run_grpc(async move { table.execute_grpc(command).await }) {
            Err(ClientError::WriteNotStarted(message)) => {
                return Ok(ExecuteResponse {
                    status: Status::error("raft_write_not_started", message),
                    response: CommandResponse::Empty,
                });
            }
            result => result?,
        };
        let status = decode_status(response.status)?;
        if !status.ok {
            return Ok(ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            });
        }
        decode_response(response.response.ok_or_else(invalid_response)?)
    }

    pub(super) fn batch_execute_native_grpc(
        &self,
        commands: Vec<Command>,
    ) -> Result<BatchExecuteResponse, ClientError> {
        let commands = commands
            .iter()
            .map(encode_command)
            .collect::<Result<Vec<_>, _>>()?;
        let table = self.clone();
        let response = run_grpc(async move { table.batch_execute_grpc(commands).await })?;
        Ok(BatchExecuteResponse {
            status: decode_status(response.status)?,
            responses: response
                .responses
                .into_iter()
                .map(decode_response)
                .collect::<Result<_, _>>()?,
        })
    }
}

fn run_grpc<T: Send + 'static>(
    future: impl Future<Output = Result<T, ClientError>> + Send + 'static,
) -> Result<T, ClientError> {
    let runtime = super::metadata_grpc::runtime()?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    runtime.spawn(async move {
        let _ = sender.send(future.await);
    });
    receiver
        .recv()
        .map_err(|error| ClientError::Status(error.to_string()))?
}

fn encode_command(command: &Command) -> Result<v1::Command, ClientError> {
    let kind = match command {
        Command::StringLen { key } => Kind::StringLen(v1::StringLen { key: key.clone() }),
        Command::CommonTtl { key } => Kind::CommonTtl(v1::CommonTtl { key: key.clone() }),
        Command::HashGetAll { key } => Kind::HashGetAll(v1::HashGetAll { key: key.clone() }),
        Command::StringGetRange { key, start, end } => Kind::StringGetRange(v1::StringGetRange {
            key: key.clone(),
            start: *start,
            end: *end,
        }),
        Command::FeatureAggQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
            count,
        } => Kind::FeatureAggQuery(v1::FeatureAggregateQuery {
            key: key.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            aggregator: aggregator.clone(),
            count: count.map(|count| count as u64),
        }),
        Command::FeatureProtoAggQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
            count,
        } => Kind::FeatureProtoAggQuery(v1::FeatureAggregateQuery {
            key: key.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            aggregator: aggregator.clone(),
            count: count.map(|count| count as u64),
        }),
        _ => {
            return crate::shared_store::command_to_sdk_proto(command).ok_or_else(|| {
                ClientError::InvalidRequest("command has no gRPC representation".to_owned())
            })
        }
    };
    Ok(v1::Command { kind: Some(kind) })
}

fn invalid_response() -> ClientError {
    ClientError::Status("invalid or unsupported gRPC command response".to_owned())
}

#[cfg(test)]
#[path = "feature_reduce_rpc_tests.rs"]
mod feature_reduce_rpc_tests;

fn decode_status(status: Option<v1::Status>) -> Result<Status, ClientError> {
    let status = status.ok_or_else(invalid_response)?;
    Ok(Status {
        ok: status.ok,
        code: status.code,
        message: status.message,
    })
}

fn decode_response(response: v1::CommandResponse) -> Result<ExecuteResponse, ClientError> {
    let status = decode_status(response.status)?;
    if !status.ok {
        return Ok(ExecuteResponse {
            status,
            response: CommandResponse::Empty,
        });
    }
    let kind = ResultKind::try_from(response.result_kind).map_err(|_| invalid_response())?;
    let response = match kind {
        ResultKind::Empty => CommandResponse::Empty,
        ResultKind::Bytes => CommandResponse::Bytes {
            value: response
                .value_present
                .ok_or_else(invalid_response)?
                .then_some(response.value),
        },
        ResultKind::Integer => CommandResponse::Integer {
            value: response.integer.ok_or_else(invalid_response)?,
        },
        ResultKind::Aggregate => CommandResponse::Aggregate {
            value: response.integer.ok_or_else(invalid_response)?,
        },
        ResultKind::Members => CommandResponse::Members {
            members: response.values,
        },
        ResultKind::Values => {
            let count = response.values.len();
            let presence = response
                .values_present
                .filter(|bits| bits.len() == count.div_ceil(8))
                .ok_or_else(invalid_response)?;
            let values = response
                .values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    (presence[index / 8] & (1 << (index % 8)) != 0).then_some(value)
                })
                .collect();
            CommandResponse::Values { values }
        }
        ResultKind::HashEntries if response.entry_names.len() == response.values.len() => {
            CommandResponse::HashEntries {
                entries: response
                    .entry_names
                    .into_iter()
                    .zip(response.values)
                    .collect(),
            }
        }
        ResultKind::FeaturePoints => CommandResponse::FeaturePoints {
            points: response
                .feature_points
                .into_iter()
                .map(|point| crate::types::FeaturePoint {
                    timestamp_ms: point.timestamp_ms,
                    value: point.value,
                })
                .collect(),
        },
        ResultKind::SequenceRows => CommandResponse::SequenceRows {
            rows: response
                .sequence_rows
                .into_iter()
                .map(|row| crate::types::SequenceFeatureRow {
                    timestamp_ms: row.timestamp_ms,
                    gid: row.gid,
                    action_type: row.action_type,
                    duration: row.duration,
                    author_id: row.author_id,
                })
                .collect(),
        },
        ResultKind::CellMergeReceipt => CommandResponse::CellMergeReceipt {
            samples_written: response.count,
            cells_created: response.cells_created,
            cells_merged: response.cells_merged,
            replayed: response.replayed,
        },
        ResultKind::WindowReduceRollupReceipt => CommandResponse::WindowReduceRollupReceipt {
            samples_before: response.samples_before,
            samples_after: response.count,
            cells_before: response.cells_before,
            cells_after: response.cells_after,
            cells_dropped: response.cells_dropped,
            buckets_folded: response.buckets_folded,
            buckets_remaining: response.buckets_remaining,
        },
        ResultKind::WindowReduceRemoveReceipt => CommandResponse::WindowReduceRemoveReceipt {
            samples_before: response.samples_before,
            samples_after: response.count,
            cells_before: response.cells_before,
            cells_removed: response.cells_removed,
        },
        _ => return Err(invalid_response()),
    };
    Ok(ExecuteResponse { status, response })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_grpc_response_preserves_empty_values_and_result_kinds() {
        let variants = [
            CommandResponse::Bytes { value: None },
            CommandResponse::Bytes {
                value: Some(Vec::new()),
            },
            CommandResponse::Values {
                values: vec![None, Some(Vec::new()), Some(vec![0, 255])],
            },
            CommandResponse::HashEntries {
                entries: Vec::new(),
            },
            CommandResponse::Aggregate { value: -1 },
            CommandResponse::Integer { value: -1 },
            CommandResponse::Members {
                members: vec![vec![0, 255]],
            },
        ];
        for expected in variants {
            let reply =
                decode_response(crate::sdk::types_command_response_to_sdk(expected.clone()))
                    .unwrap();
            assert_eq!(reply.response, expected);
        }
    }
}
