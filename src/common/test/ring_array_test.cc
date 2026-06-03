// Copyright (c) 2023-present, ByteDance Inc. All rights reserved.

#include "common/ring_array.h"

#include <gtest/gtest.h>
#include <string>

#include "common/algorithm.h"

namespace bcache2 {

TEST(RingArray, resize) {
    RingArray<std::string> ring_array(0);
    std::string item("test_resize");
    auto index = 0UL;
    while (index++ < 1023UL) {
        ring_array.Push(std::string("test_resize_").append(std::to_string(index)));
    }

    EXPECT_EQ(ring_array.Size(), 1023UL);
    EXPECT_EQ(ring_array.size_, NextPowerOfTwo(index));

    // 341 = 1024 / 3
    while (ring_array.Size() > 341) {
        ring_array.Pop();
    }
    EXPECT_EQ(ring_array.size_, 1024UL);
    ring_array.Pop();
    EXPECT_EQ(ring_array.size_, 512UL);
}

}  // namespace bcache2
