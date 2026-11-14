// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A TIMESTAMPED POINT'S STAGED NAME IS BUILT ONCE, AND MOVED INTO THE RECORD THAT CARRIES IT.
//!
//! Every point a feature, sequence or context write appends stages one outcome item naming the
//! instant it landed at, and every point a trim or a replace drops stages one saying it went. Both
//! built the name as a `String`, put a CLONE of it in the item, and dropped the original at the end
//! of the iteration: a second allocation and a second free of the same characters, per point, on
//! every timestamped write. The name is now moved.
//!
//! WHAT MUST NOT MOVE: the items themselves -- kind, object key, element name, object id, routing
//! bucket, address, and the `deleted` flag -- so the bytes a record carries are the bytes it carried.
//!
//! TWO ARMS.
//!   * `the_staged_items_are_what_they_were` -- every field of every item, for a point written under
//!     a plain instant, under an instant with an identity, and for a removal.
//!   * `probe::a_staged_point_does_not_copy_its_name` needs `alloc-probe` and `--ignored`.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::block_store::ElementEntry;
use crate::engine::block_in_wal::take_outcomes;
use crate::engine::packed_blocks::{
    stage_timestamped_outcomes, stage_timestamped_removal, timestamped_element_name,
};
use crate::engine::stable_block_object_id;
use crate::wal::WalOutcomeItem;

#[test]
fn the_staged_items_are_what_they_were() {
    let object_id = stable_block_object_id(7, "feature", "the-key");
    let refs: Vec<(u64, ElementEntry)> = [5u64, 1_787_270_070_123, u64::MAX]
        .into_iter()
        .map(|timestamp_ms| (timestamp_ms, ElementEntry::default()))
        .collect();
    let _ = take_outcomes();

    stage_timestamped_outcomes("feature", "the-key", object_id, 3, &refs, None);
    let plain = take_outcomes();
    stage_timestamped_outcomes("feature", "the-key", object_id, 3, &refs, Some(0xABCD));
    let identified = take_outcomes();
    stage_timestamped_removal("feature", "the-key", object_id, 3, 1_787_270_070_123);
    let removal = take_outcomes();

    assert_eq!(3, plain.len());
    assert_eq!(3, identified.len());
    assert_eq!(1, removal.len());
    for (item, (timestamp_ms, address)) in plain.iter().zip(&refs) {
        assert_eq!(
            &WalOutcomeItem {
                kind: "feature".to_string(),
                object_key: "the-key".to_string(),
                element_name: Some(timestamp_ms.to_string()),
                object_id,
                routing_bucket: 3,
                address: Some(address.clone()),
                value: None,
                ttl: None,
                deleted: false,
                meta: false,
            },
            item
        );
    }
    // The spelling under an identity is sixteen hex digits of each, and is the contract between the
    // writer and the apply path, so it is stated here by hand and not read back from the builder.
    assert_eq!(
        Some("0000000000000005000000000000abcd".to_string()),
        identified[0].element_name
    );
    for (item, (timestamp_ms, _)) in identified.iter().zip(&refs) {
        assert_eq!(
            Some(timestamped_element_name(*timestamp_ms, Some(0xABCD))),
            item.element_name
        );
        assert!(!item.deleted);
    }
    assert_eq!(
        WalOutcomeItem {
            kind: "feature".to_string(),
            object_key: "the-key".to_string(),
            element_name: Some("1787270070123".to_string()),
            object_id,
            routing_bucket: 3,
            address: None,
            value: None,
            ttl: None,
            deleted: true,
            meta: false,
        },
        removal[0]
    );
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const POINTS: usize = 200;

    fn refs(points: usize) -> Vec<(u64, ElementEntry)> {
        (0..points as u64)
            .map(|index| (1_787_270_070_000 + index, ElementEntry::default()))
            .collect()
    }

    /// Allocations to stage `POINTS` points, divided by the points.
    fn per_point_staged() -> f64 {
        let refs = refs(POINTS);
        let _ = take_outcomes();
        let probe = Probe::start();
        stage_timestamped_outcomes("feature", "the-key", 1, 0, &refs, None);
        let counts = probe.stop();
        let staged = take_outcomes();
        assert_eq!(POINTS, staged.len());
        counts.allocs as f64 / POINTS as f64
    }

    /// The same staging written the way it used to be, as the CONTROL: it must come out dearer by
    /// the clone, or the probe cannot see a second copy of a name.
    fn per_point_staged_with_a_clone() -> f64 {
        let refs = refs(POINTS);
        let _ = take_outcomes();
        let probe = Probe::start();
        for (timestamp_ms, address) in &refs {
            let element_name = timestamped_element_name(*timestamp_ms, None);
            crate::engine::block_in_wal::stage_outcome(WalOutcomeItem {
                kind: "feature".to_string(),
                object_key: "the-key".to_string(),
                element_name: Some(element_name.clone()),
                object_id: 1,
                routing_bucket: 0,
                address: Some(address.clone()),
                value: None,
                ttl: None,
                deleted: false,
                meta: false,
            });
        }
        let counts = probe.stop();
        let staged = take_outcomes();
        assert_eq!(POINTS, staged.len());
        counts.allocs as f64 / POINTS as f64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_staged_point_does_not_copy_its_name() {
        let moved = per_point_staged();
        let cloned = per_point_staged_with_a_clone();
        println!("staged: {moved:.2} allocations a point; with a clone (control): {cloned:.2}");
        assert!(
            cloned - moved >= 0.9,
            "the control cost {cloned:.2} a point against {moved:.2}, so the probe cannot see a \
             second copy of the name"
        );
        // Three strings to build the item (kind, object key, element name) and the amortised growth
        // of the staging buffer; a fourth per point is the clone.
        assert!(
            moved <= 3.2,
            "staging a point cost {moved:.2} allocations; the name is being copied again"
        );
    }
}
