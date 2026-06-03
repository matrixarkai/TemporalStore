// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>

#define ALLOW_COPY_AND_ASSIGN(class) void CopyAndAssignHint##class()

#define INITIALIZE_WITH_NAME(name, body) BYTE_INITIALIZER(name, body)
#define INITIALIZE(body) INITIALIZE_WITH_NAME(__COUNTER__, body)
#define RESTORE_FLAGS(flags)  \
    auto tmp_##flags = flags; \
    BYTE_DEFER({ flags = tmp_##flags; });

#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
    do {                     \
    } while (0)
#endif

namespace bcache2 {}  // namespace bcache2
