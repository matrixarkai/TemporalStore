// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>
#include <stdint.h>

#include <string>

namespace bcache2 {

class TempDir final {
 public:
    TempDir();
    TempDir(const std::string& prefix);
    virtual ~TempDir();

    std::string GetDir() const;

 private:
    std::string dir_;

    DISALLOW_COPY_AND_ASSIGN(TempDir);
};

uint32_t GetIdlePort();

}  // namespace bcache2
