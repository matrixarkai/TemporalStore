// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gflags/gflags.h>

DEFINE_uint64(stream_max_blob_size, 1UL << 30, "max blob size in bytes, 1GB by default");
DEFINE_uint64(stream_blob_deletion_min_age, 24 * 3600, "least age in seconds for blob deletion");
DEFINE_uint64(stream_blob_deletion_min_gap, 10UL << 30, "least gap for blob deletion");
DEFINE_uint64(stream_blob_switch_retry_interval_us, 1000000UL,
              "retry interval for blob switch failure");

DEFINE_int32(store_fiu_hang_interval_ms, 1, "hang interval ms");

// aggregate flush Flags, default 300ms, 3872byte=4096-24-200
DEFINE_bool(stream_aggregate_flush, false, "stream aggregate flush");
DEFINE_uint64(stream_aggregate_flush_loop_interval_ms, 300, "aggregate flush loop interval ms");
DEFINE_uint64(stream_aggregate_flush_batch_size_byte, 3872, "aggregate flush batch size byte");
