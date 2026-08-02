// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gflags/gflags.h>

#include "brpc/reloadable_flags.h"

DEFINE_uint64(feature_max_size, 5000UL, "feature max size");
BRPC_VALIDATE_GFLAG(feature_max_size, brpc::PassValidate);
