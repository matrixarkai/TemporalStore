// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Shard load/reload/unload helpers, extracted from data_node.rs.

use super::*;
use crate::control::{LoadShardRequest, LoadShardResponse, UnloadShardRequest, UnloadShardResponse};
use crate::types::Status;

pub(super) fn load_shard_with_inner(
    inner: &DataNodeRuntimeInner,
    request: LoadShardRequest,
) -> LoadShardResponse {
    if let Err(status) =
        validate_lifecycle_token_inner(inner, request.shard_id, "load", request.load_version)
    {
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "load",
            request.load_version,
            Some(status.clone()),
        );
        return LoadShardResponse { status };
    }
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        "loading",
        "load",
        request.load_version,
        None,
    );
    let response = inner.engine.load_shard_with(request.clone());
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        final_loaded_lifecycle_state(request.readonly, &response.status),
        "load",
        request.load_version,
        Some(response.status.clone()),
    );
    response
}

pub(super) fn reload_shard_with_inner(
    inner: &DataNodeRuntimeInner,
    request: LoadShardRequest,
) -> LoadShardResponse {
    if let Err(status) =
        validate_lifecycle_token_inner(inner, request.shard_id, "reload", request.load_version)
    {
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "reload",
            request.load_version,
            Some(status.clone()),
        );
        return LoadShardResponse { status };
    }
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        "reloading",
        "reload",
        request.load_version,
        None,
    );
    let response = inner.engine.reload_shard_with(request.clone());
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        final_loaded_lifecycle_state(request.readonly, &response.status),
        "reload",
        request.load_version,
        Some(response.status.clone()),
    );
    response
}

pub(super) fn unload_shard_with_inner(
    inner: &DataNodeRuntimeInner,
    request: UnloadShardRequest,
    reject_when_busy: bool,
) -> UnloadShardResponse {
    let load_version = inner
        .engine
        .get_info(request.shard_id)
        .info
        .map(|info| info.load_version)
        .unwrap_or_default();
    if reject_when_busy && shard_has_queued_or_running_work(inner, request.shard_id) {
        let status = Status::error(
            "shard_busy",
            format!(
                "cannot unload shard {} while data node work is queued or running",
                request.shard_id
            ),
        );
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "unload",
            load_version,
            Some(status.clone()),
        );
        return UnloadShardResponse { status };
    }
    if let Err(status) =
        validate_lifecycle_token_inner(inner, request.shard_id, "unload", load_version)
    {
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "unload",
            load_version,
            Some(status.clone()),
        );
        return UnloadShardResponse { status };
    }
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        "unloading",
        "unload",
        load_version,
        None,
    );
    let response = inner.engine.unload_shard_with(request.clone());
    let state = if response.status.ok {
        "unloaded"
    } else {
        "failed"
    };
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        state,
        "unload",
        load_version,
        Some(response.status.clone()),
    );
    response
}

