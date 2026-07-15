use crate::types::ShardId;

pub(super) fn routing_slot_count(start_routing_slot: u32, end_routing_slot: u32) -> u32 {
    if end_routing_slot < start_routing_slot {
        return 0;
    }
    end_routing_slot
        .saturating_sub(start_routing_slot)
        .saturating_add(1)
}

pub(super) fn slot_for_object(key: &str, start_routing_slot: u32, routing_slot_count: u32) -> u32 {
    if routing_slot_count == 0 {
        return start_routing_slot;
    }
    start_routing_slot + (stable_object_hash(key) % routing_slot_count as u64) as u32
}

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(super) fn stable_object_hash(key: &str) -> u64 {
    stable_object_hash_bytes(key.as_bytes())
}

pub(super) fn stable_object_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = FNV1A64_OFFSET_BASIS;
    stable_object_hash_update(&mut hash, bytes);
    hash
}

pub(super) fn stable_object_hash_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= *byte as u64;
        *hash = hash.wrapping_mul(FNV1A64_PRIME);
    }
}

pub(super) fn stable_object_hash_update_u64_decimal(hash: &mut u64, mut value: u64) {
    let mut buf = [0_u8; 20];
    let mut pos = buf.len();
    if value == 0 {
        pos -= 1;
        buf[pos] = b'0';
    } else {
        while value > 0 {
            pos -= 1;
            buf[pos] = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }
    stable_object_hash_update(hash, &buf[pos..]);
}

pub(super) fn stable_page_object_id(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    component: Option<&str>,
) -> u64 {
    stable_page_object_id_from_prefix(stable_page_object_id_prefix(shard_id, kind, key), component)
}

pub(super) fn stable_page_object_id_prefix(shard_id: ShardId, kind: &str, key: &str) -> u64 {
    let mut hash = FNV1A64_OFFSET_BASIS;
    stable_object_hash_update_u64_decimal(&mut hash, shard_id as u64);
    stable_object_hash_update(&mut hash, b":");
    stable_object_hash_update(&mut hash, kind.as_bytes());
    stable_object_hash_update(&mut hash, b":");
    stable_object_hash_update(&mut hash, key.as_bytes());
    hash
}

pub(super) fn stable_page_object_id_from_prefix(mut hash: u64, component: Option<&str>) -> u64 {
    if let Some(component) = component {
        stable_object_hash_update(&mut hash, b":");
        stable_object_hash_update(&mut hash, component.as_bytes());
    }
    hash
}

pub(super) fn page_routing_slot(key: &str, start_routing_slot: u32, end_routing_slot: u32) -> u32 {
    slot_for_object(
        key,
        start_routing_slot,
        routing_slot_count(start_routing_slot, end_routing_slot),
    )
}
