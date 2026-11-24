// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::path::Path;

use matrixcache::{CacheCompression, CacheOptions, MultiLayerCache};
use temporalstore_rust::env_flag::{env_bool, env_value};

pub(super) fn open_replica_cache(
    cache_dir: &Path,
    memory_bytes: usize,
    disk_cache_tier: bool,
) -> Result<MultiLayerCache, String> {
    if let Some(cache) = open_configured_cache(cache_dir, memory_bytes, disk_cache_tier)? {
        return Ok(cache);
    }

    let mut options = CacheOptions::default();
    options.dram_capacity = memory_bytes;
    if !disk_cache_tier {
        options.pmem_capacity = 0;
        options.ssd_capacity = 0;
    }
    options.ssd_paths.push(cache_dir.to_owned());
    MultiLayerCache::try_with_options(options).map_err(|error| error.to_string())
}

pub(super) fn open_configured_cache(
    cache_dir: &Path,
    memory_bytes: usize,
    disk_cache_tier: bool,
) -> Result<Option<MultiLayerCache>, String> {
    let pmem_bytes = env_value("TS_CACHE_PMEM_BYTES");
    let ssd_bytes = env_value("TS_CACHE_SSD_BYTES");
    let memory_limit = env_value("TS_CACHE_MAX_MEMORY_BLOCK_BYTES");
    let pmem_limit = env_value("TS_CACHE_MAX_PMEM_BLOCK_BYTES");
    let ssd_limit = env_value("TS_CACHE_MAX_SSD_BLOCK_BYTES");
    let recover = env_value("TS_CACHE_RECOVER_ON_START");
    let compression = env_value("TS_CACHE_COMPRESSION_ENABLED");
    let configure_disk = disk_cache_tier
        && [
            &pmem_bytes,
            &ssd_bytes,
            &memory_limit,
            &pmem_limit,
            &ssd_limit,
            &recover,
        ]
        .iter()
        .any(|setting| setting.is_some());
    if !configure_disk && compression.is_none() {
        return Ok(None);
    }

    let mut options = CacheOptions::default();
    options.dram_capacity = memory_bytes;
    if disk_cache_tier {
        options.pmem_capacity = byte_count(pmem_bytes.as_deref(), "TS_CACHE_PMEM_BYTES")?
            .unwrap_or(options.pmem_capacity);
        options.ssd_capacity =
            byte_count(ssd_bytes.as_deref(), "TS_CACHE_SSD_BYTES")?.unwrap_or(options.ssd_capacity);
        options.max_memory_block_bytes =
            byte_count(memory_limit.as_deref(), "TS_CACHE_MAX_MEMORY_BLOCK_BYTES")?;
        options.max_pmem_block_bytes =
            byte_count(pmem_limit.as_deref(), "TS_CACHE_MAX_PMEM_BLOCK_BYTES")?;
        options.max_ssd_block_bytes =
            byte_count(ssd_limit.as_deref(), "TS_CACHE_MAX_SSD_BLOCK_BYTES")?;
    } else {
        options.pmem_capacity = 0;
        options.ssd_capacity = 0;
    }
    options.ssd_paths.push(cache_dir.to_owned());
    if options.pmem_capacity > 0 && pmem_bytes.is_some() {
        options.pmem_paths.push(cache_dir.join("pmem"));
    }
    options.auto_recover_on_start = (options.pmem_capacity > 0 || options.ssd_capacity > 0)
        && env_bool("TS_CACHE_RECOVER_ON_START", false);
    if !env_bool("TS_CACHE_COMPRESSION_ENABLED", true) {
        options.block_options.compression = CacheCompression::None;
    }
    MultiLayerCache::try_with_options(options)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn byte_count(value: Option<&str>, name: &str) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|_| format!("{name} must be a nonnegative byte count"))
        })
        .transpose()
}
