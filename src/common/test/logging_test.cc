// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/logging.h"

#include <gtest/gtest.h>

namespace bcache2 {

TEST(LoggingTest, SimpleTest) {
    byte::SetByteLogDir("./");
    byte::SetByteLogMaxFileNum(10);
    byte::SetByteLogMaxFileSize(1UL << 30);
    LOG_CALL_WARNING().put("Key1", "Value1").put("Key2", "Value2");
    LOG_DEBUG("Write debug log").put("Key1", "Value1");
    LOG_INFO("Write info log").put("Key1", "Value1");
    LOG_WARNING("Write warning log").put("Key1", "Value1");
    LOG_ERROR("Write error log").put("Key1", "Value1");
    // LOG_FATAL("Write fatal log").put("Key1", "Value1");

    LOG_ERROR("Append data log")
        .put("Key1", 500)
        .put("key2", "Yeasgsdfg")
        .put("key3", "AAAAAAAAAAAAAAAAAAAAAAA")
        .put("XXXXXXXXXXXXXXXXXXX", "dgdgdg");
}

}  // namespace bcache2
