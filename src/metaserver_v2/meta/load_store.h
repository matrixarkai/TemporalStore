// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

namespace bcache2 {
namespace metaserver {

struct LoadPoint {
    int cpu;
    int memory;
};

class LoadStore {
 public:
    LoadStore() = default;
    ~LoadStore() = default;

 private:
};

}  // namespace metaserver
}  // namespace bcache2

