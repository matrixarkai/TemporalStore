// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gflags/gflags.h>

DEFINE_uint64(stream_max_blob_size, 1UL << 30, "max blob size in bytes, 1GB by default");
DEFINE_uint64(stream_blob_deletion_min_age, 24 * 3600, "least age in seconds for blob deletion");
DEFINE_uint64(stream_blob_deletion_min_gap, 10UL << 30, "least gap for blob deletion");
DEFINE_uint64(stream_blob_switch_retry_interval_us, 1000000UL,
              "retry interval for blob switch failure");

DEFINE_int32(store_fiu_hang_interval_ms, 1, "hang interval ms");

// Aggregate flush batches oplog writes by size or time, whichever comes first.
// Profiles:
//   default      : 2ms  or 512KB
//   low_latency  : 1ms  or 256KB
//   throughput   : 5ms  or 1MB
//   batch_ingest : 50ms or 4MB
//   custom       : use stream_aggregate_flush_loop_interval_ms and
//                  stream_aggregate_flush_batch_size_byte directly
DEFINE_bool(stream_aggregate_flush, true, "stream aggregate flush");
DEFINE_string(stream_aggregate_flush_profile, "default",
              "aggregate flush profile: default, low_latency, throughput, batch_ingest, custom");
DEFINE_uint64(stream_aggregate_flush_loop_interval_ms, 2, "aggregate flush loop interval ms");
DEFINE_uint64(stream_aggregate_flush_batch_size_byte, 512 * 1024,
              "aggregate flush batch size byte");

static bool ValidateStreamAggregateFlushProfile(const char*, const std::string& value) {
    return value == "default" || value == "low_latency" || value == "throughput" ||
           value == "batch_ingest" || value == "custom";
}

DEFINE_validator(stream_aggregate_flush_profile, &ValidateStreamAggregateFlushProfile);
