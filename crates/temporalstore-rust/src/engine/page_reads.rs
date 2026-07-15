use std::collections::HashSet;

use rustmtcache::{CacheEntryInfo, CacheKey};

use crate::page_store::{LocalPageStore, PageAddress};

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
