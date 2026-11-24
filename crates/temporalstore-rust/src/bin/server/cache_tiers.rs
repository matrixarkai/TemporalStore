// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::path::Path;

use matrixcache::{CacheCompression, CacheOptions, MultiLayerCache};
use temporalstore_rust::env_flag::{env_bool, env_value};

pub(super) fn open_configured_cache(
    cache_dir: &Path,
    memory_bytes: usize,
    disk_cache_tier: bool,
) -> Result<Option<MultiLayerCache>, String> {
    let pmem_bytes = env_value("TS_CACHE_PMEM_BYTES");
    let ssd_bytes = env_value("TS_CACHE_SSD_BYTES");
    let recover = env_value("TS_CACHE_RECOVER_ON_START");
    let compression = env_value("TS_CACHE_COMPRESSION_ENABLED");
    let configure_disk =
        disk_cache_tier && (pmem_bytes.is_some() || ssd_bytes.is_some() || recover.is_some());
    if !configure_disk && compression.is_none() {
        return Ok(None);
    }

    let mut options = CacheOptions::default();
    options.dram_capacity = memory_bytes;
    options.pmem_capacity = if disk_cache_tier {
        capacity(
            pmem_bytes.as_deref(),
            "TS_CACHE_PMEM_BYTES",
            options.pmem_capacity,
        )?
    } else {
        0
    };
    options.ssd_capacity = if disk_cache_tier {
        capacity(
            ssd_bytes.as_deref(),
            "TS_CACHE_SSD_BYTES",
            options.ssd_capacity,
        )?
    } else {
        0
    };
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

fn capacity(value: Option<&str>, name: &str, default: usize) -> Result<usize, String> {
    match value {
        Some(value) => value
            .parse()
            .map_err(|_| format!("{name} must be a nonnegative byte count")),
        None => Ok(default),
    }
}
