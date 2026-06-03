// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gflags/gflags.h>

#include "brpc/reloadable_flags.h"

DEFINE_int32(load_slot_retry_times, 3, "retry times for load slot");
BRPC_VALIDATE_GFLAG(load_slot_retry_times, brpc::PassValidate);
