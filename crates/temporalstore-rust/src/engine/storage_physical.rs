use crate::page_store::PageAddress;

use super::reports::{
    StorageBlockAddressSample, StoragePageAddressSample, StoragePhysicalPageIndex,
    StoragePhysicalSlotNode,
};
use super::storage_model::storage_model_code;

pub(super) const CPP_PACKED_PAGE_INDEX_SIZE: usize = 17;
pub(super) const CPP_PACKED_SLOT_NODE_SIZE: usize = 24;

fn physical_address_word(address: &PageAddress) -> u64 {
    address.page_segment_id.wrapping_shl(32) | (address.offset & u32::MAX as u64)
}

pub(super) fn storage_page_address_sample(
    shard_id: u64,
    address: &PageAddress,
) -> StoragePageAddressSample {
    StoragePageAddressSample {
        shard_id,
        zone_id: address.extent_id.unwrap_or(address.page_segment_id),
        segment_id: address.page_segment_id,
        page_id: address.page_id.unwrap_or(address.page_segment_id),
        offset: address.offset,
        length: address.length,
        generation: address.object_id.unwrap_or(0),
    }
}

pub(super) fn storage_block_address_sample(
    shard_id: u64,
    address: &PageAddress,
) -> StorageBlockAddressSample {
    StorageBlockAddressSample {
        shard_id,
        zone_id: address.extent_id.unwrap_or(address.page_segment_id),
        block_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
        checksum: address.sha256.clone().unwrap_or_default(),
    }
}

pub(super) fn cpp_packed_page_index_bytes(
    page: &StoragePhysicalPageIndex,
) -> [u8; CPP_PACKED_PAGE_INDEX_SIZE] {
    let mut bytes = [0u8; CPP_PACKED_PAGE_INDEX_SIZE];
    bytes[0] = page.object_id.unwrap_or_default() as u8;
    bytes[1] = storage_model_code(&page.model_id);
    bytes[2..4].copy_from_slice(&(page.page_id.unwrap_or_default() as u16).to_le_bytes());
    bytes[4] = u8::from(page.dirty) | (u8::from(page.log_backed) << 1);
    let page_size = if page.deleted { 0 } else { page.length as u32 };
    bytes[5..9].copy_from_slice(&page_size.to_le_bytes());
    let address = physical_address_word(&PageAddress {
        page_segment_id: page.page_segment_id,
        offset: page.offset,
        length: page.length,
        page_id: page.page_id,
        object_id: page.object_id,
        routing_slot: Some(page.routing_slot),
        generation: page.page_id.or(page.object_id),
        extent_id: page.zone_id,
        sha256: page.checksum.clone(),
    });
    bytes[9..17].copy_from_slice(&address.to_le_bytes());
    bytes
}

pub(super) fn cpp_packed_slot_node_bytes(
    slot: &StoragePhysicalSlotNode,
) -> [u8; CPP_PACKED_SLOT_NODE_SIZE] {
    let mut bytes = [0u8; CPP_PACKED_SLOT_NODE_SIZE];
    let page_in_log = slot.page_indexes.iter().any(|page| page.log_backed);
    let trivial_page = slot.page_ref_count <= 1;
    let page_deleted = slot.page_ref_count == 0;
    let mut flags = 0u32;
    flags |= (slot.ttl_ms.is_some() as u32) << 1;
    flags |= (slot.dirty as u32) << 2;
    flags |= (slot.loading as u32) << 4;
    flags |= (slot.in_memory as u32) << 5;
    flags |= (slot.dirty as u32) << 6;
    flags |= (page_deleted as u32) << 7;
    flags |= (page_in_log as u32) << 8;
    flags |= (trivial_page as u32) << 9;
    let flag_bytes = flags.to_le_bytes();
    bytes[0..3].copy_from_slice(&flag_bytes[0..3]);
    bytes[3..7].copy_from_slice(&(slot.physical_bytes as u32).to_le_bytes());
    let model_code = slot
        .page_indexes
        .first()
        .map(|page| storage_model_code(&page.model_id))
        .unwrap_or_default();
    bytes[7] = model_code;
    bytes[8..16].copy_from_slice(&slot.ttl_ms.unwrap_or_default().to_le_bytes());
    let address = slot
        .page_indexes
        .first()
        .map(|page| page.page_segment_id.wrapping_shl(32) | (page.offset & u32::MAX as u64))
        .unwrap_or_default();
    bytes[16..24].copy_from_slice(&address.to_le_bytes());
    bytes
}
