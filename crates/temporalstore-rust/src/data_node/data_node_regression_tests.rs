// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use tempfile::tempdir;

#[path = "tests/data_node_test_support.rs"]
mod helpers;
#[allow(unused_imports)]
use helpers::*;
#[path = "tests/worker_lifecycle_and_streaming.rs"]
mod part1;
#[path = "tests/admission_and_unload_ordering.rs"]
mod part2;
#[path = "tests/maintenance_and_lifecycle_recovery.rs"]
mod part3;
#[path = "tests/exported_storage_terms.rs"]
mod exported_storage_terms;
#[path = "tests/evict_live.rs"]
mod evict_live;
#[path = "tests/evict_dump_drop.rs"]
mod evict_dump_drop;
