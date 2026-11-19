#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
#
# Regenerate the protobuf messages and the Rust converters for the metaserver's snapshot files.
# Run from the repository root after changing any of the types named below.
set -eu
python3 tools/gen_proto_converters.py --repo . --prefix Mf \
  --roots MetaSnapshot,TaskSchedulerSnapshot,Status,SchedulerLifecycleToken,DataNodeShardLifecycleState,RaftMembershipChangeReport,SchedulerTaskResult \
  --proto-marker metaserver-files \
  --out-rust crates/temporalstore-rust/src/metaserver_files_proto.rs \
  --path-map "crate::meta::proxy_groups=crate::meta,crate::raft::raft_membership=crate::raft,crate::command_protocol_types=crate::types"
