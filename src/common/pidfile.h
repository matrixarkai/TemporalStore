// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <string>

namespace bcache2 {
class PidFile {
 public:
    explicit PidFile(const std::string& filepath) : filepath_(filepath) {}
    ~PidFile();

    bool TryLock();

 private:
    const std::string filepath_;
    int fd_{-1};
    bool f_{false};
};

}  // namespace bcache2

