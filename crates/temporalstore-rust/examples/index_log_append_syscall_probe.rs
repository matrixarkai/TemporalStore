// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Append N delta records into a fresh index log, and nothing else.
//!
//! Run under `strace -f -c` at two record counts; every fixed cost -- process start, the
//! dynamic loader, the tempdir -- is identical in both arms and cancels in the difference.
//!
//!     cargo run --release --example index_log_append_syscall_probe -- <dir> <records>

use temporalstore_rust::index_log::{IndexItem, IndexItemKind, LocalIndexLogStore};

fn small_item(bucket: u32, key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Page,
        routing_bucket: bucket,
        block_ref_key: key.to_string(),
        object_key: key.to_string(),
        model_id: "m".to_string(),
        component: None,
        object_id: 1,
        block_id: 0,
        address: None,
        size: 8,
        in_log: false,
        deleted: false,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: <dir> <records>");
    let records: usize = args.next().expect("usage: <dir> <records>").parse().unwrap();

    // Build every record BEFORE the store exists, so the fixture's own allocation and
    // formatting are outside the region the append arm is compared on. They are identical
    // per record in both arms anyway; this keeps the difference to the appends alone.
    let mut batches = Vec::with_capacity(records);
    let mut value = 0usize;
    while value < records {
        // ZERO-PADDED: an unpadded key is one character longer at 10,000 than at 1,000, which
        // would put fixture bytes into a number that is measuring the code.
        batches.push(vec![small_item(
            (value % 64) as u32,
            &format!("tenant/1/object/{value:08}/00"),
        )]);
        value += 1;
    }

    let store = LocalIndexLogStore::new(&dir);
    let mut taken = 0usize;
    for items in batches {
        store
            .append_delta(1, items, Vec::new(), None, None, false, false)
            .unwrap();
        taken += 1;
    }
    println!("appended {taken}");
}
