// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#ifdef FIU_ENABLE

#include <thread>

#include "common/coclosure.h"
#include "common/fiu_local.h"
#include "common/logging.h"
#include "libfiu/libfiu/fiu.h"

DECLARE_int32(store_fiu_hang_interval_ms);

#define PARTITION_FAULT_INJECT_HANG(path)                     \
    fiu_do_on(                                                \
        path, do {                                            \
            LOG_INFO("Inject hang").put("Path", path);        \
            CoSleep(FLAGS_store_fiu_hang_interval_ms * 1000); \
        } while (false))

#define PARTITION_FAULT_INJECT_CRASH(path)                 \
    fiu_do_on(                                             \
        path, do {                                         \
            LOG_WARNING("Inject crash").put("Path", path); \
            LOG_FLUSH();                                   \
            _Exit(0);                                      \
        } while (false))

// inject hang and crash
#define PARTITION_FAULT_INJECT(path)                 \
    do {                                             \
        PARTITION_FAULT_INJECT_HANG(path "/hang");   \
        PARTITION_FAULT_INJECT_CRASH(path "/crash"); \
    } while (false)

#else

#define PARTITION_FAULT_INJECT_HANG(path)
#define PARTITION_FAULT_INJECT_CRASH(path)
#define PARTITION_FAULT_INJECT(path)

#endif
