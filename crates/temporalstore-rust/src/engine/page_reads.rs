use std::collections::{HashMap, HashSet};

use matrixcache::{CacheEntryInfo, CacheKey, MultiLayerCache};

use crate::page_store::{LocalPageStore, PageAddress};
use crate::types::ShardId;

pub(super) fn single_non_empty_page_address(
    addresses: &[Option<PageAddress>],
) -> Option<&PageAddress> {
    let mut single_address = None;
    for address in addresses.iter().flatten() {
        match single_address {
            Some(existing) if existing != address => return None,
            Some(_) => {}
            None => single_address = Some(address),
        }
    }
    single_address
}

pub(super) struct BatchPageReadEntry {
    pub(super) key: CacheKey,
    pub(super) address: PageAddress,
    pub(super) first_index: usize,
    pub(super) extra_indexes: Vec<usize>,
}

impl BatchPageReadEntry {
    pub(super) fn push_index(&mut self, index: usize) {
        self.extra_indexes.push(index);
    }
}

pub(super) struct BatchPageReadFillEntry {
    pub(super) key: CacheKey,
    pub(super) first_index: usize,
    pub(super) extra_indexes: Vec<usize>,
}

pub(super) fn fill_page_read_values(
    values: &mut [Option<Vec<u8>>],
    entry: &BatchPageReadEntry,
    bytes: &[u8],
) {
    values[entry.first_index] = Some(bytes.to_vec());
    for index in &entry.extra_indexes {
        values[*index] = Some(bytes.to_vec());
    }
}

pub(super) fn fill_page_read_values_from_fill_entry(
    values: &mut [Option<Vec<u8>>],
    entry: &BatchPageReadFillEntry,
    bytes: &[u8],
) {
    values[entry.first_index] = Some(bytes.to_vec());
    for index in &entry.extra_indexes {
        values[*index] = Some(bytes.to_vec());
    }
}

pub(super) fn fill_page_read_values_owned(
    values: &mut [Option<Vec<u8>>],
    entry: &BatchPageReadEntry,
    bytes: Vec<u8>,
) {
    for index in &entry.extra_indexes {
        values[*index] = Some(bytes.clone());
    }
    values[entry.first_index] = Some(bytes);
}

pub(super) fn duplicate_page_read_values(
    len: usize,
    bytes: Option<Vec<u8>>,
) -> Vec<Option<Vec<u8>>> {
    let Some(bytes) = bytes else {
        return vec![None; len];
    };
    let mut values = Vec::with_capacity(len);
    let mut remaining = len;
    while remaining > 0 {
        if remaining == 1 {
            values.push(Some(bytes));
            break;
        } else {
            values.push(Some(bytes.clone()));
            remaining = remaining.saturating_sub(1);
        }
    }
    values
}

pub(super) fn duplicate_sparse_page_read_values(
    addresses: &[Option<PageAddress>],
    bytes: Option<Vec<u8>>,
) -> Vec<Option<Vec<u8>>> {
    let Some(bytes) = bytes else {
        return vec![None; addresses.len()];
    };
    let last_value_index = addresses
        .iter()
        .rposition(|address| address.is_some())
        .unwrap_or_default();
    let mut bytes = Some(bytes);
    let mut values = Vec::with_capacity(addresses.len());
    for (index, address) in addresses.iter().enumerate() {
        if address.is_some() {
            if index == last_value_index {
                values.push(bytes.take());
                continue;
            }
            values.push(bytes.as_ref().cloned());
        } else {
            values.push(None);
        }
    }
    values
}

pub(super) fn read_page_bytes_cold(
    page_store: &LocalPageStore,
    address: &PageAddress,
) -> Option<Vec<u8>> {
    page_store.read_cold(address).ok()
}

pub(super) fn page_address_cache_key(shard_id: ShardId, address: &PageAddress) -> CacheKey {
    CacheKey::page_with_slot_generation(
        shard_id,
        address.page_segment_id,
        address.offset,
        address.length,
        address.routing_slot,
        address.generation,
    )
}

pub(super) fn read_page_bytes(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    address: &PageAddress,
) -> Option<Vec<u8>> {
    let cache_key = page_address_cache_key(shard_id, address);
    if let Ok(Some(bytes)) = cache.get(&cache_key) {
        return Some(bytes);
    }
    let bytes = page_store.read(address).ok()?;
    let _ = cache.put(cache_key, bytes.clone());
    Some(bytes)
}

pub(super) fn read_page_bytes_batch(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    addresses: &[Option<PageAddress>],
) -> Vec<Option<Vec<u8>>> {
    if addresses.is_empty() {
        return Vec::new();
    }
    if addresses.len() == 1 {
        return vec![addresses[0]
            .as_ref()
            .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))];
    }
    if addresses.windows(2).all(|window| window[0] == window[1]) {
        return match &addresses[0] {
            Some(address) => duplicate_page_read_values(
                addresses.len(),
                read_page_bytes(cache, page_store, shard_id, address),
            ),
            None => vec![None; addresses.len()],
        };
    }
    if let Some(address) = single_non_empty_page_address(addresses) {
        return duplicate_sparse_page_read_values(
            addresses,
            read_page_bytes(cache, page_store, shard_id, address),
        );
    }

    let mut values = vec![None; addresses.len()];
    let mut unique_entries = Vec::<BatchPageReadEntry>::with_capacity(addresses.len());
    let mut unique_index_by_key = HashMap::<CacheKey, usize>::with_capacity(addresses.len());
    for (index, address) in addresses.iter().enumerate() {
        let Some(address) = address else {
            continue;
        };
        let key = page_address_cache_key(shard_id, address);
        if let Some(entry_index) = unique_index_by_key.get(&key).copied() {
            unique_entries[entry_index].push_index(index);
        } else {
            let entry_index = unique_entries.len();
            unique_index_by_key.insert(key.clone(), entry_index);
            unique_entries.push(BatchPageReadEntry {
                key,
                address: address.clone(),
                first_index: index,
                extra_indexes: Vec::new(),
            });
        }
    }
    if unique_entries.is_empty() {
        return values;
    }
    if unique_entries.len() == 1 {
        let entry = &unique_entries[0];
        values[entry.first_index] = read_page_bytes(cache, page_store, shard_id, &entry.address);
        for index in &entry.extra_indexes {
            values[*index] = values[entry.first_index].clone();
        }
        return values;
    }

    let unique_keys = unique_entries
        .iter()
        .map(|entry| entry.key.clone())
        .collect::<Vec<_>>();
    let cached = cache
        .get_batch(&unique_keys)
        .unwrap_or_else(|_| vec![None; unique_keys.len()]);
    let mut miss_entries = Vec::with_capacity(unique_entries.len());
    for (entry, cached_value) in unique_entries.into_iter().zip(cached.into_iter()) {
        if let Some(bytes) = cached_value {
            fill_page_read_values_owned(&mut values, &entry, bytes);
            continue;
        }
        miss_entries.push(entry);
    }
    if miss_entries.is_empty() {
        return values;
    }
    if miss_entries.len() == 1 {
        let entry = miss_entries
            .pop()
            .expect("single miss entry is present after length check");
        if let Ok(bytes) = page_store.read(&entry.address) {
            fill_page_read_values(&mut values, &entry, &bytes);
            let _ = cache.put(entry.key, bytes);
        }
        return values;
    }

    let mut miss_addresses = Vec::with_capacity(miss_entries.len());
    let mut miss_fill_entries = Vec::with_capacity(miss_entries.len());
    for entry in miss_entries {
        miss_addresses.push(entry.address);
        miss_fill_entries.push(BatchPageReadFillEntry {
            key: entry.key,
            first_index: entry.first_index,
            extra_indexes: entry.extra_indexes,
        });
    }
    let miss_reads = page_store.read_batch(&miss_addresses);
    let mut refills = Vec::with_capacity(miss_fill_entries.len());
    for (entry, read_result) in miss_fill_entries.into_iter().zip(miss_reads) {
        let Ok(bytes) = read_result else {
            continue;
        };
        fill_page_read_values_from_fill_entry(&mut values, &entry, &bytes);
        refills.push((entry.key, bytes));
    }
    if !refills.is_empty() {
        let _ = cache.put_batch(refills);
    }
    values
}

pub(super) fn read_page_bytes_batch_owned(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    addresses: Vec<Option<PageAddress>>,
) -> Vec<Option<Vec<u8>>> {
    if addresses.is_empty() {
        return Vec::new();
    }
    if addresses.len() == 1 {
        return vec![addresses[0]
            .as_ref()
            .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))];
    }
    if addresses.windows(2).all(|window| window[0] == window[1]) {
        return match &addresses[0] {
            Some(address) => duplicate_page_read_values(
                addresses.len(),
                read_page_bytes(cache, page_store, shard_id, address),
            ),
            None => vec![None; addresses.len()],
        };
    }
    if let Some(address) = single_non_empty_page_address(&addresses) {
        return duplicate_sparse_page_read_values(
            &addresses,
            read_page_bytes(cache, page_store, shard_id, address),
        );
    }

    let mut values = vec![None; addresses.len()];
    let mut unique_entries = Vec::<BatchPageReadEntry>::with_capacity(addresses.len());
    let mut unique_index_by_key = HashMap::<CacheKey, usize>::with_capacity(addresses.len());
    for (index, address) in addresses.into_iter().enumerate() {
        let Some(address) = address else {
            continue;
        };
        let key = page_address_cache_key(shard_id, &address);
        if let Some(entry_index) = unique_index_by_key.get(&key).copied() {
            unique_entries[entry_index].push_index(index);
        } else {
            let entry_index = unique_entries.len();
            unique_index_by_key.insert(key.clone(), entry_index);
            unique_entries.push(BatchPageReadEntry {
                key,
                address,
                first_index: index,
                extra_indexes: Vec::new(),
            });
        }
    }
    if unique_entries.is_empty() {
        return values;
    }
    if unique_entries.len() == 1 {
        let entry = &unique_entries[0];
        values[entry.first_index] = read_page_bytes(cache, page_store, shard_id, &entry.address);
        for index in &entry.extra_indexes {
            values[*index] = values[entry.first_index].clone();
        }
        return values;
    }

    let unique_keys = unique_entries
        .iter()
        .map(|entry| entry.key.clone())
        .collect::<Vec<_>>();
    let cached = cache
        .get_batch(&unique_keys)
        .unwrap_or_else(|_| vec![None; unique_keys.len()]);
    let mut miss_entries = Vec::with_capacity(unique_entries.len());
    for (entry, cached_value) in unique_entries.into_iter().zip(cached.into_iter()) {
        if let Some(bytes) = cached_value {
            fill_page_read_values_owned(&mut values, &entry, bytes);
            continue;
        }
        miss_entries.push(entry);
    }
    if miss_entries.is_empty() {
        return values;
    }
    if miss_entries.len() == 1 {
        let entry = miss_entries
            .pop()
            .expect("single miss entry is present after length check");
        if let Ok(bytes) = page_store.read(&entry.address) {
            fill_page_read_values(&mut values, &entry, &bytes);
            let _ = cache.put(entry.key, bytes);
        }
        return values;
    }

    let mut miss_addresses = Vec::with_capacity(miss_entries.len());
    let mut miss_fill_entries = Vec::with_capacity(miss_entries.len());
    for entry in miss_entries {
        miss_addresses.push(entry.address);
        miss_fill_entries.push(BatchPageReadFillEntry {
            key: entry.key,
            first_index: entry.first_index,
            extra_indexes: entry.extra_indexes,
        });
    }
    let miss_reads = page_store.read_batch(&miss_addresses);
    let mut refills = Vec::with_capacity(miss_fill_entries.len());
    for (entry, read_result) in miss_fill_entries.into_iter().zip(miss_reads) {
        let Ok(bytes) = read_result else {
            continue;
        };
        fill_page_read_values_from_fill_entry(&mut values, &entry, &bytes);
        refills.push((entry.key, bytes));
    }
    if !refills.is_empty() {
        let _ = cache.put_batch(refills);
    }
    values
}

pub(super) fn dedupe_nonzero_u64_preserve_order(values: Vec<u64>) -> Vec<u64> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| *value != 0 && seen.insert(*value))
        .collect()
}

pub(super) fn cache_entry_routing_slot(entry: &CacheEntryInfo) -> Option<u32> {
    entry
        .selector
        .strip_prefix("slot-")?
        .split(':')
        .next()?
        .parse()
        .ok()
}

pub(super) fn parse_i64(bytes: &Vec<u8>) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}
