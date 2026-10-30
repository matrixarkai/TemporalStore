// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::TemporalEngine;
use crate::http::{json_response, parse_json, serve};
use crate::meta::{GetShardResponse, ShardLocation, TableMetaInfo, TableShard};

#[path = "tests/client_test_support.rs"]
mod helpers;
#[allow(unused_imports)]
use helpers::*;
#[path = "tests/routing_and_preflight.rs"]
mod part1;
#[path = "tests/topology_sync_and_read_policy.rs"]
mod part2;
#[path = "tests/retry_and_pipeline_routing.rs"]
mod part3;
