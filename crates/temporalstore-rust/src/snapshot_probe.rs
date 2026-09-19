// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a snapshot walks, reads, copies and rebuilds -- counted inside the primitives that do it.
//!
//! A snapshot spans four modules (`raft`, `raft::cluster_snapshot`, `engine`, `block_store`), so a
//! counter bumped at the call sites in `cluster_snapshot.rs` would have measured the call sites
//! rather than the work. Every counter here is bumped INSIDE the primitive -- inside
//! `BlockStore::read_slab`, inside `BlockStore::install_slab`, inside `collect_live_block_slab_ids`,
//! inside `Clone for RaftSnapshotStateImage` -- so a new call site cannot reach the work without
//! being counted, and no list of call sites has to be maintained.
//!
//! Thread-local, for the reason `wal::WAL_SEGMENT_LISTINGS` is: the suite runs tests in parallel
//! by default and a great many of them touch a block store, so a process-global counter would
//! report their work as this one's.
//!
//! `GuardMark` is armed for exactly the span in which this thread holds the raft cluster WRITE
//! guard and disarms in `Drop`, so an early return cannot leave it set and charge later work to a
//! guard that has been released. That guard is the one every `propose` needs the other half of, so
//! "under guard" here means "the cluster could not accept a write".

#![cfg(test)]

use std::cell::Cell;

thread_local! {
    static GUARD_DEPTH: Cell<u32> = const { Cell::new(0) };

    static IMAGE_BUILDS: Cell<u64> = const { Cell::new(0) };

    static LIVE_SLAB_SCANS: Cell<u64> = const { Cell::new(0) };
    static LIVE_SLAB_SCAN_ADDRESSES: Cell<u64> = const { Cell::new(0) };

    static SLAB_DIR_LISTINGS: Cell<u64> = const { Cell::new(0) };

    static SLAB_READS: Cell<u64> = const { Cell::new(0) };
    static SLAB_READ_BYTES: Cell<u64> = const { Cell::new(0) };

    static SLAB_INSTALLS: Cell<u64> = const { Cell::new(0) };
    static SLAB_INSTALL_BYTES: Cell<u64> = const { Cell::new(0) };
    static SLAB_INSTALL_BYTES_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };

    static IMAGE_CLONES: Cell<u64> = const { Cell::new(0) };
    static IMAGE_CLONE_BYTES: Cell<u64> = const { Cell::new(0) };
    static IMAGE_CLONE_BYTES_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };

    static ENGINE_REBUILDS: Cell<u64> = const { Cell::new(0) };
    static ENGINE_REBUILDS_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };

    static ENGINE_PUBLISHES: Cell<u64> = const { Cell::new(0) };

    static WINDOW_ARMED: Cell<Option<InstallWindow>> = const { Cell::new(None) };
    static WINDOW_FIRINGS: Cell<u64> = const { Cell::new(0) };
}

/// An interleaving a test asks to happen in the window where the install loop builds its engines
/// holding NO cluster guard.
///
/// A test cannot get inside that window by racing for it -- the window is microseconds wide and a
/// lost race reads exactly like a check that fired -- so the interleaving is REQUESTED here and
/// performed by the install path itself, once, at the point where it holds nothing. The arm being
/// attacked can only fail in one direction, so it is constructed rather than waited for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstallWindow {
    /// Apply one more entry, so every node's commit and applied indexes pass the snapshot's.
    /// The snapshot is now OLDER than the nodes it was about to be published into.
    ApplyOneMoreEntry,
    /// Bring a node back, so it qualifies for the snapshot without an engine having been built
    /// for it.
    ReviveNode(u64),
}

/// Ask the next install window on this thread to perform `window`. Consumed by the first window
/// that reaches it, so one arming is one interleaving.
pub fn arm_install_window(window: InstallWindow) {
    WINDOW_ARMED.with(|armed| armed.set(Some(window)));
}

/// Taken by the install path at the point where it holds no cluster guard.
pub fn take_install_window() -> Option<InstallWindow> {
    WINDOW_ARMED.with(|armed| armed.take()).inspect(|_| {
        bump(&WINDOW_FIRINGS, 1);
    })
}

/// How many armed interleavings were actually performed. A test that armed one and reads zero
/// here did not test what it thinks it tested.
pub fn install_window_firings() -> u64 {
    WINDOW_FIRINGS.with(Cell::get)
}

/// Armed while this thread holds the raft cluster write guard.
pub struct GuardMark;

impl GuardMark {
    pub fn new() -> Self {
        GUARD_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for GuardMark {
    fn drop(&mut self) {
        GUARD_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

fn under_guard() -> bool {
    GUARD_DEPTH.with(|depth| depth.get() > 0)
}

fn bump(counter: &'static std::thread::LocalKey<Cell<u64>>, by: u64) {
    counter.with(|value| value.set(value.get().saturating_add(by)));
}

pub fn note_image_build() {
    bump(&IMAGE_BUILDS, 1);
}

/// One walk of a shard's whole index to collect the live slab ids, and how many block ADDRESSES
/// that walk visited. The address count is the per-record denominator: it is what makes the walk
/// O(corpus) rather than O(slabs), and it is invisible in the small set the walk returns.
pub fn note_live_slab_scan(addresses: u64) {
    bump(&LIVE_SLAB_SCANS, 1);
    bump(&LIVE_SLAB_SCAN_ADDRESSES, addresses);
}

pub fn note_slab_dir_listing() {
    bump(&SLAB_DIR_LISTINGS, 1);
}

pub fn note_slab_read(bytes: u64) {
    bump(&SLAB_READS, 1);
    bump(&SLAB_READ_BYTES, bytes);
}

pub fn note_slab_install(bytes: u64) {
    bump(&SLAB_INSTALLS, 1);
    bump(&SLAB_INSTALL_BYTES, bytes);
    if under_guard() {
        bump(&SLAB_INSTALL_BYTES_UNDER_GUARD, bytes);
    }
}

pub fn note_image_clone(bytes: u64) {
    bump(&IMAGE_CLONES, 1);
    bump(&IMAGE_CLONE_BYTES, bytes);
    if under_guard() {
        bump(&IMAGE_CLONE_BYTES_UNDER_GUARD, bytes);
    }
}

pub fn note_engine_rebuild() {
    bump(&ENGINE_REBUILDS, 1);
    if under_guard() {
        bump(&ENGINE_REBUILDS_UNDER_GUARD, 1);
    }
}

/// One engine moved onto a node. Counted separately from the rebuild because the two stopped
/// being the same event: an engine can be built and then NOT published, which is what happens
/// when the eligibility check finds the node moved on while the engine was building. Without
/// this counter, "built nothing" and "built and discarded everything" are the same reading.
pub fn note_engine_publish() {
    bump(&ENGINE_PUBLISHES, 1);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotCounts {
    pub image_builds: u64,
    pub live_slab_scans: u64,
    pub live_slab_scan_addresses: u64,
    pub slab_dir_listings: u64,
    pub slab_reads: u64,
    pub slab_read_bytes: u64,
    pub slab_installs: u64,
    pub slab_install_bytes: u64,
    pub slab_install_bytes_under_guard: u64,
    pub image_clones: u64,
    pub image_clone_bytes: u64,
    pub image_clone_bytes_under_guard: u64,
    pub engine_rebuilds: u64,
    pub engine_rebuilds_under_guard: u64,
    pub engine_publishes: u64,
}

pub fn reset() {
    for counter in [
        &IMAGE_BUILDS,
        &LIVE_SLAB_SCANS,
        &LIVE_SLAB_SCAN_ADDRESSES,
        &SLAB_DIR_LISTINGS,
        &SLAB_READS,
        &SLAB_READ_BYTES,
        &SLAB_INSTALLS,
        &SLAB_INSTALL_BYTES,
        &SLAB_INSTALL_BYTES_UNDER_GUARD,
        &IMAGE_CLONES,
        &IMAGE_CLONE_BYTES,
        &IMAGE_CLONE_BYTES_UNDER_GUARD,
        &ENGINE_REBUILDS,
        &ENGINE_REBUILDS_UNDER_GUARD,
        &ENGINE_PUBLISHES,
        &WINDOW_FIRINGS,
    ] {
        counter.with(|value| value.set(0));
    }
    WINDOW_ARMED.with(|armed| armed.set(None));
}

pub fn counts() -> SnapshotCounts {
    SnapshotCounts {
        image_builds: IMAGE_BUILDS.with(Cell::get),
        live_slab_scans: LIVE_SLAB_SCANS.with(Cell::get),
        live_slab_scan_addresses: LIVE_SLAB_SCAN_ADDRESSES.with(Cell::get),
        slab_dir_listings: SLAB_DIR_LISTINGS.with(Cell::get),
        slab_reads: SLAB_READS.with(Cell::get),
        slab_read_bytes: SLAB_READ_BYTES.with(Cell::get),
        slab_installs: SLAB_INSTALLS.with(Cell::get),
        slab_install_bytes: SLAB_INSTALL_BYTES.with(Cell::get),
        slab_install_bytes_under_guard: SLAB_INSTALL_BYTES_UNDER_GUARD.with(Cell::get),
        image_clones: IMAGE_CLONES.with(Cell::get),
        image_clone_bytes: IMAGE_CLONE_BYTES.with(Cell::get),
        image_clone_bytes_under_guard: IMAGE_CLONE_BYTES_UNDER_GUARD.with(Cell::get),
        engine_rebuilds: ENGINE_REBUILDS.with(Cell::get),
        engine_rebuilds_under_guard: ENGINE_REBUILDS_UNDER_GUARD.with(Cell::get),
        engine_publishes: ENGINE_PUBLISHES.with(Cell::get),
    }
}
