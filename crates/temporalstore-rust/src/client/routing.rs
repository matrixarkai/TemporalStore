// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

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
    first_shard_id + bucket_id_for_key(key) % shard_count
}

pub fn bucket_id_for_key(key: &str) -> u64 {
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

#[cfg(test)]
mod bucket_ownership_tests {
    use super::*;

    /// Where a key goes is decided by the bucket MODULO the shard count, not by the bucket
    /// ranges the metaserver publishes.
    ///
    /// Both exist. `bucket_id_for_key` puts a key in one of `1 << 30` buckets, the same slot
    /// space the metaserver uses, and `build_shards` gives every shard a contiguous
    /// `start_bucket..=end_bucket` slice of it (`bucket_count * offset / shard_count`). Nothing
    /// routes by those ranges: this function takes the bucket modulo the count instead, so the
    /// two describe different owners for the same key -- at four shards they disagree for about
    /// three quarters of the bucket space.
    ///
    /// That is not currently a fault, because the ranges are only reported (in the client's
    /// partition-set report, where they fall back to the same even split) and the data node
    /// serves whatever shard it is asked for. It matters for anything built on them later: a
    /// range split moves a contiguous slice of buckets, and this mapping would not follow it.
    ///
    /// This pins the mapping that is real, so that a change to range routing is a deliberate one
    /// and not something discovered afterwards.
    #[test]
    fn a_key_is_routed_by_bucket_modulo_count_not_by_the_published_range() {
        let shard_count = 4_u64;
        let first_shard_id = 100_u64;
        let bucket_count = 1_u64 << 30;

        // The range the metaserver publishes for the first shard, computed as `build_shards`
        // does it.
        let shard0_end = bucket_count / shard_count - 1;

        // A key whose bucket falls inside that range, but which does not route there.
        let mut found = None;
        for candidate in 0..10_000_u64 {
            let key = format!("k{candidate}");
            let bucket = bucket_id_for_key(&key);
            if bucket <= shard0_end && bucket % shard_count != 0 {
                found = Some((key, bucket));
                break;
            }
        }
        let (key, bucket) = found.expect("some key lands in the first shard's published range");

        let routed = shard_id_for_key(&key, first_shard_id, shard_count, 0);
        assert_eq!(
            routed,
            first_shard_id + bucket % shard_count,
            "routing is modulo the shard count"
        );
        assert_ne!(
            routed, first_shard_id,
            "and it is NOT the shard whose published range contains bucket {bucket} -- if this              now holds, routing became range-based and the report and this comment need updating"
        );
    }
}
