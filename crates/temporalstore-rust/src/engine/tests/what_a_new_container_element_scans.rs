// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::atomic::Ordering;

#[test]
fn what_a_new_container_element_scans() {
    for kind in ["hash", "set", "zset", "list"] {
        for elements in [64, 128, 256] {
            let dir = tempfile::tempdir().unwrap();
            let engine = TemporalEngine::with_local_dirs(
                64 << 20,
                dir.path().join("cache"),
                dir.path().join("blocks"),
                dir.path().join("index"),
            );
            engine.load_shard(1);
            state::SIBLING_SCAN_CALLS.store(0, Ordering::Relaxed);
            state::SIBLING_SCAN_ELEMENTS.store(0, Ordering::Relaxed);
            for index in 0..elements {
                let member = format!("member-{index:04}").into_bytes();
                let command = match kind {
                    "hash" => Command::HashSet {
                        key: "container".into(),
                        field: format!("field-{index:04}"),
                        value: member,
                    },
                    "set" => Command::SetAdd {
                        key: "container".into(),
                        member,
                    },
                    "zset" => Command::ZSetAdd {
                        key: "container".into(),
                        member,
                        score: index as f64,
                    },
                    _ => Command::ListPush {
                        key: "container".into(),
                        member,
                        left: false,
                    },
                };
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command,
                });
                assert!(response.status.ok, "{kind}: {:?}", response.status);
            }
            let calls = state::SIBLING_SCAN_CALLS.load(Ordering::Relaxed);
            let examined = state::SIBLING_SCAN_ELEMENTS.load(Ordering::Relaxed);
            assert_eq!(
                calls, 2,
                "{kind}: one empty lookup and one cold populated lookup"
            );
            assert_eq!(
                examined, 1,
                "{kind}: appends must not repeat the sibling scan"
            );
            println!("{kind} elements={elements} scans={calls} siblings={examined}");
        }
    }
}
