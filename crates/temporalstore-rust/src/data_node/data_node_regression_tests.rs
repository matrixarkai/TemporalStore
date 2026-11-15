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
#[path = "tests/loop_report.rs"]
mod loop_report;
#[path = "tests/evict_dump_drop.rs"]
mod evict_dump_drop;
#[path = "tests/the_wal_reclaim_stage_waits_for_the_byte_gate.rs"]
mod the_wal_reclaim_stage_waits_for_the_byte_gate;
#[path = "tests/a_restart_after_loop_rounds_finds_every_value.rs"]
mod a_restart_after_loop_rounds_finds_every_value;
#[path = "tests/a_reopen_after_rounds_with_deadlines_serves_every_key_and_list_element.rs"]
mod a_reopen_after_rounds_with_deadlines_serves_every_key_and_list_element;
#[path = "tests/the_loop_reports_its_backlog_and_its_dumps.rs"]
mod the_loop_reports_its_backlog_and_its_dumps;
