// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Snapshot the locator, not the payload, before an unlocked read can race its retirement.
//! Reclaimed records still cause the fast path to retry through the guarded current index.

use super::*;
use std::marker::PhantomData;
use std::rc::Rc;

#[derive(Clone)]
struct PinnedLocation {
    location: Option<Registration>,
    check_address: bool,
}

struct ReadPins {
    owner: usize,
    shard: ShardId,
    first: Option<(physical::PhysicalKey, PinnedLocation)>,
    rest: Option<HashMap<physical::PhysicalKey, PinnedLocation, ahash::RandomState>>,
}

impl ReadPins {
    fn get(&self, key: &physical::PhysicalKey) -> Option<&PinnedLocation> {
        if let Some((held, location)) = &self.first {
            if held == key {
                return Some(location);
            }
        }
        self.rest.as_ref()?.get(key)
    }

    fn insert(&mut self, key: physical::PhysicalKey, location: PinnedLocation) {
        if self.first.is_none() {
            self.first = Some((key, location));
        } else {
            self.rest.get_or_insert_with(HashMap::default).insert(key, location);
        }
    }
}

thread_local! {
    static READ_PINS: RefCell<Option<ReadPins>> = const { RefCell::new(None) };
}

// A point read stays inline. Wide reads retain one locator per distinct frame, not per field.
pub(crate) struct ReadSnapshotGuard {
    previous: Option<ReadPins>,
    owner: usize,
    shard: ShardId,
    _thread_bound: PhantomData<Rc<()>>,
}

impl ReadSnapshotGuard {
    pub(crate) fn new(blocks: &crate::block_store::BlockStore, shard: ShardId) -> Self {
        let owner = blocks.store_id();
        let previous = READ_PINS.with(|slot| slot.replace(Some(ReadPins {
            owner, shard, first: None, rest: None,
        })));
        Self { previous, owner, shard, _thread_bound: PhantomData }
    }

    /// Called while the shard index is locked. Missing locators are captured too: a later
    /// registration must not supply a newer record for an address from this snapshot.
    pub(crate) fn pin(
        &mut self,
        blocks: &crate::block_store::BlockStore,
        object: u64,
        element: Option<&str>,
        address: &crate::block_store::ElementEntry,
    ) {
        debug_assert!(crate::wal_record::is_wal_resident(address.block_slab_id()));
        let key = (object, crate::engine::live_block_key(address));
        READ_PINS.with(|slot| {
            let mut held = slot.borrow_mut();
            let pins = held.as_mut().expect("read snapshot is active");
            assert_eq!((pins.owner, pins.shard), (self.owner, self.shard));
            assert_eq!(blocks.store_id(), self.owner);
            if pins.get(&key).is_some() {
                return;
            }
            let physical = physical::lookup(blocks, self.shard, object, key.1);
            let captured = match physical {
                Some(Some(location)) => PinnedLocation { location: Some(location), check_address: false },
                missing => {
                    let location = registry().lock().ok().and_then(|map| {
                        let entries = map.shard(self.owner, self.shard)?;
                        entries.get(&block_key(object, element))
                            .or_else(|| entries.get(&block_key(object, None))).cloned()
                    });
                    PinnedLocation { location, check_address: missing.is_some() }
                }
            };
            pins.insert(key, captured);
        });
    }
}

impl Drop for ReadSnapshotGuard {
    fn drop(&mut self) {
        READ_PINS.with(|slot| {
            let finished = slot.replace(self.previous.take());
            drop(finished);
        });
    }
}

pub(super) fn try_read(
    blocks: &crate::block_store::BlockStore,
    shard: ShardId,
    object: u64,
    element: Option<&str>,
    address: &crate::block_store::ElementEntry,
) -> Option<Option<Vec<u8>>> {
    let captured = READ_PINS.with(|slot| {
        let held = slot.borrow();
        let pins = held.as_ref()?;
        if pins.owner != blocks.store_id() || pins.shard != shard {
            return None;
        }
        pins.get(&(object, crate::engine::live_block_key(address))).cloned()
    })?;
    let PinnedLocation { location, check_address } = captured;
    Some(location.and_then(|location| {
        if check_address {
            read_logical_registration_at(&location, shard, object, element, address)
        } else {
            read_registration(&location, shard, object, element)
        }
    }))
}

#[cfg(test)]
#[path = "wal_read_snapshot_tests.rs"]
mod tests;
