// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/bits.h"

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gtest/gtest.h>

namespace bcache2 {

TEST(BitsTest, SetAndCheck) {
    uint64_t bits = 0;
    uint64_t bits_after_set;

    bits_after_set = SetBit(bits, 0);
    for (uint32_t i = 0 ; i < 100 ; ++i) {
        if (i == 0) {
            ASSERT_TRUE(IsBitSet(bits_after_set, i));
        } else {
            ASSERT_FALSE(IsBitSet(bits_after_set, i));
        }
    }

    bits_after_set = SetBit(bits_after_set, 5);
    for (uint32_t i = 0 ; i < 100 ; ++i) {
        if (i == 0 || i == 5) {
            ASSERT_TRUE(IsBitSet(bits_after_set, i));
        } else {
            ASSERT_FALSE(IsBitSet(bits_after_set, i));
        }
    }
}

}  // namespace bcache2
