// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

use crate::http::{post_json_with_options, HttpError, HttpRequestOptions};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, CommandResponse, ExecuteRequest, ExecuteResponse,
    Status,
};

// Keep this adapter on the HTTP boundary, not on the public response type:
// tagged-enum buffering otherwise allocates a temporary value for every byte.
#[derive(Deserialize)]
struct ExecuteJson {
    status: Status,
    #[serde(deserialize_with = "deserialize_response")]
    response: CommandResponse,
}

#[derive(Deserialize)]
struct BatchJson {
    status: Status,
    responses: Vec<ExecuteJson>,
}

fn deserialize_response<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<CommandResponse, D::Error> {
    if !deserializer.is_human_readable() {
        return CommandResponse::deserialize(deserializer);
    }
    let raw = Box::<RawValue>::deserialize(deserializer)?;
    #[derive(Deserialize)]
    struct Kind {
        kind: String,
    }
    let header: Kind = serde_json::from_str(raw.get()).map_err(D::Error::custom)?;
    if header.kind == "hash_entries" {
        #[derive(Deserialize)]
        struct HashEntries {
            entries: Vec<(String, Vec<u8>)>,
        }
        let body: HashEntries = serde_json::from_str(raw.get()).map_err(D::Error::custom)?;
        Ok(CommandResponse::HashEntries {
            entries: body.entries,
        })
    } else {
        serde_json::from_str(raw.get()).map_err(D::Error::custom)
    }
}

impl From<ExecuteJson> for ExecuteResponse {
    fn from(value: ExecuteJson) -> Self {
        Self {
            status: value.status,
            response: value.response,
        }
    }
}

impl From<BatchJson> for BatchExecuteResponse {
    fn from(value: BatchJson) -> Self {
        Self {
            status: value.status,
            responses: value.responses.into_iter().map(Into::into).collect(),
        }
    }
}

pub(super) fn post_execute_json(
    addr: &str,
    path: &str,
    request: &ExecuteRequest,
    options: HttpRequestOptions,
) -> Result<ExecuteResponse, HttpError> {
    post_json_with_options::<_, ExecuteJson>(addr, path, request, options).map(Into::into)
}

pub(super) fn post_batch_json(
    addr: &str,
    path: &str,
    request: &BatchExecuteRequest,
    options: HttpRequestOptions,
) -> Result<BatchExecuteResponse, HttpError> {
    post_json_with_options::<_, BatchJson>(addr, path, request, options).map(Into::into)
}

#[cfg(test)]
#[path = "response_json_tests.rs"]
mod tests;
