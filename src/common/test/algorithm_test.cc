// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/algorithm.h"

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gtest/gtest.h>

#include "common/data.h"

namespace bcache2 {

TEST(NextPowerOfTwo, 32Bit) {
    EXPECT_EQ(0U, NextPowerOfTwo(0U));
    EXPECT_EQ(1U, NextPowerOfTwo(1U));
    EXPECT_EQ(2U, NextPowerOfTwo(2U));
    EXPECT_EQ(4U, NextPowerOfTwo(3U));

    EXPECT_EQ(1U << 16, NextPowerOfTwo((1U << 16) - 1));
    EXPECT_EQ(1U << 16, NextPowerOfTwo(1U << 16));
    EXPECT_EQ(1U << 17, NextPowerOfTwo((1U << 16) + 1));

    EXPECT_EQ(1U << 31, NextPowerOfTwo((1U << 31) - 1));
    EXPECT_EQ(1U << 31, NextPowerOfTwo(1U << 31));

    EXPECT_EQ(0U, NextPowerOfTwo((1U << 31) + 1));
}

TEST(NextPowerOfTwo, 64Bit) {
    EXPECT_EQ(0UL, NextPowerOfTwo(0UL));
    EXPECT_EQ(1UL, NextPowerOfTwo(1UL));

    EXPECT_EQ(1UL << 44, NextPowerOfTwo((1UL << 44) - 1));
    EXPECT_EQ(1UL << 44, NextPowerOfTwo(1UL << 44));
    EXPECT_EQ(1UL << 45, NextPowerOfTwo((1UL << 44) + 1));

    EXPECT_EQ(1UL << 63, NextPowerOfTwo((1UL << 63) - 1));
    EXPECT_EQ(1UL << 63, NextPowerOfTwo(1UL << 63));

    EXPECT_EQ(0UL, NextPowerOfTwo((1UL << 63) + 1));
}

}  // namespace bcache2
