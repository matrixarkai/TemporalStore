// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LIVE TOTAL EQUALS THE SUM OF THE TALLY AFTER EVERY WAY THE TALLY CAN CHANGE.
//!
//! The block-GC gate reads one number instead of walking the per-slab map, so the number has to be
//! the sum the walk would have produced. Each arm moves the tally one way: a charge, a discharge,
//! a discharge of a slab that holds less than the address says, a discharge on a slab with no
//! tally, a wholesale replacement, and a clear. The check after every step is the same: the total
//! against the sum taken the slow way.
#![allow(clippy::all)]
use crate::block_store::ElementEntry;
use crate::engine::state::{BlockSlabLiveIndex, SlabLiveTally, StaleCause};
use std::collections::BTreeMap;

fn entry(block_slab_id: u64, offset: u64, length: u64) -> ElementEntry {
    ElementEntry::from_parts(block_slab_id, offset, length, Some(0), None)
}

fn walked(live: &BlockSlabLiveIndex) -> u64 {
    live.iter().map(|(_, tally)| tally.bytes).sum()
}

#[test]
fn the_live_total_follows_every_charge_discharge_replacement_and_clear() {
    let mut live = BlockSlabLiveIndex::default();
    assert_eq!(live.total_live_bytes(), 0);

    // Charges across several slabs.
    for slab in 1..=4_u64 {
        for block in 0..5_u64 {
            live.add_address(&entry(slab, block * 100, 100 + slab));
        }
    }
    assert_eq!(live.total_live_bytes(), walked(&live));
    assert!(live.total_live_bytes() > 0, "DENOMINATOR: the arms below move a real total");

    // Discharges, recorded and unrecorded.
    live.remove_address(&entry(2, 0, 102), StaleCause::Overwrite);
    live.remove_address_unrecorded(&entry(3, 100, 103));
    assert_eq!(live.total_live_bytes(), walked(&live));

    // A discharge larger than the slab holds gives up only what the slab has.
    live.remove_address_unrecorded(&entry(1, 0, 1_000_000));
    assert_eq!(live.total_live_bytes(), walked(&live));

    // A discharge on a slab that has no tally changes nothing.
    let before = live.total_live_bytes();
    live.remove_address_unrecorded(&entry(99, 0, 50));
    assert_eq!(live.total_live_bytes(), before);
    assert_eq!(live.total_live_bytes(), walked(&live));

    // A slab discharged to nothing leaves the total at the remaining slabs.
    for block in 0..5_u64 {
        live.remove_address_unrecorded(&entry(4, block * 100, 104));
    }
    assert_eq!(live.tally(4), SlabLiveTally::default());
    assert_eq!(live.total_live_bytes(), walked(&live));

    // A wholesale replacement recomputes it.
    let replacement = BTreeMap::from([
        (10_u64, SlabLiveTally { block_refs: 2, bytes: 700 }),
        (11_u64, SlabLiveTally { block_refs: 1, bytes: 25 }),
    ]);
    live.reset_from(replacement);
    assert_eq!(live.total_live_bytes(), 725);
    assert_eq!(live.total_live_bytes(), walked(&live));

    // And a clear empties it and un-derives the tally.
    live.clear();
    assert_eq!(live.total_live_bytes(), 0);
    assert!(!live.is_ready());
}
