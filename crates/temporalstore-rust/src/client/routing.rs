use crate::types::ShardId;

pub fn shard_id_for_key(
    key: &str,
    first_shard_id: ShardId,
    shard_count: u64,
    default_shard_id: ShardId,
) -> ShardId {
    if shard_count == 0 {
        return default_shard_id;
    }
    first_shard_id + slot_id_for_key(key) % shard_count
}

pub fn slot_id_for_key(key: &str) -> u64 {
    crc64_jones(key.as_bytes()) >> 34
}

pub fn crc64_jones(bytes: &[u8]) -> u64 {
    let mut crc = 0_u64;
    for byte in bytes {
        let mut entry = (crc ^ u64::from(*byte)) & 0xff;
        for _ in 0..8 {
            if entry & 1 == 1 {
                entry = (entry >> 1) ^ 0x95ac9329ac4bc9b5;
            } else {
                entry >>= 1;
            }
        }
        crc = entry ^ (crc >> 8);
    }
    crc
}

pub fn stable_key_hash(key: &str) -> u64 {
    crc64_jones(key.as_bytes())
}

pub fn key_is_dropped_by_percent(key: &str, drop_percent: u8) -> bool {
    let drop_percent = drop_percent.min(100);
    drop_percent > 0 && (stable_key_hash(key) % 100) < u64::from(drop_percent)
}
