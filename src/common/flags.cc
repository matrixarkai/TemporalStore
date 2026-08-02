// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gflags/gflags.h>

#include "brpc/reloadable_flags.h"

DEFINE_uint64(log_sample_count, 1000, "log sample count");
BRPC_VALIDATE_GFLAG(log_sample_count, brpc::PassValidate);
