// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gflags/gflags.h>

#include "brpc/reloadable_flags.h"

DEFINE_uint64(model_max_page_id, 128, "max page size");
BRPC_VALIDATE_GFLAG(model_max_page_id, brpc::PassValidate);

DEFINE_bool(model_deny_full_dump, false, "just for UT");
BRPC_VALIDATE_GFLAG(model_deny_full_dump, brpc::PassValidate);

DEFINE_double(model_max_space_amplification, 2, "max space amplification for model");
BRPC_VALIDATE_GFLAG(model_max_space_amplification, brpc::PassValidate);

DEFINE_uint64(model_size_tiered_compaction_min_bucket_size, 1024,
              "all pages smaller than this number of bytes are put into the same bucket");
BRPC_VALIDATE_GFLAG(model_size_tiered_compaction_min_bucket_size, brpc::PassValidate);

DEFINE_uint64(model_size_tiered_compaction_max_ignore_bucket_size, 200 * 1024,
              "we do not compact pages which size greater than max_bucket_size");
BRPC_VALIDATE_GFLAG(model_size_tiered_compaction_max_ignore_bucket_size, brpc::PassValidate);

DEFINE_uint64(model_size_tiered_compaction_bucket_step, 2,
              "bucket size is min_page_size * bucket_step^level");
BRPC_VALIDATE_GFLAG(model_size_tiered_compaction_bucket_step, brpc::PassValidate);

DEFINE_uint64(model_size_tiered_compaction_max_threshold, 2,
              "maximum number of SSTables to allow in a bucket for compaction");
BRPC_VALIDATE_GFLAG(model_size_tiered_compaction_max_threshold, brpc::PassValidate);
