// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "stream/stream.h"
#include "stream/tools/action/action.h"

namespace bcache2 {
namespace stream {
namespace tool {

class StatAction : public Action {
 public:
    explicit StatAction(Stream* stream) : stream_(stream) {}
    ~StatAction() {}

    Status Run() override {
        Stats stats = stream_->Stat();
        std::cout << "start_record_id: " << stats.start_record_id << "\n";
        std::cout << "usage_bytes: " << stats.usage_bytes << "\n";
        std::cout << "length: " << stats.length << "\n";
        std::cout << "persistent_length: " << stats.persistent_length << "\n";
        return Status::OK();
    }

 private:
    Stream* stream_ = nullptr;
};

}  // namespace tool
}  // namespace stream
}  // namespace bcache2
