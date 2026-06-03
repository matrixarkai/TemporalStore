// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>
#include <stdint.h>

#include <unordered_map>
#include <vector>

#include "common/ring_array.h"

namespace bcache2 {

class KvChecker {
 public:
    KvChecker();
    virtual ~KvChecker();

    uint64_t NewWrite(uint64_t* value);
    uint64_t NewRead();
    void FinishWrite(uint64_t handle, bool success);
    bool FinishRead(uint64_t handle, bool success, uint64_t value);

 private:
    struct WriteOp {
        uint64_t version = 0;
        uint64_t start_time = 0;
        uint64_t end_time = 0;
    };

    struct ReadOp {
        uint64_t version = 0;
        uint64_t start_time = 0;
        std::vector<uint64_t> candidate_values;
    };

    void ClearOldWrites(uint64_t time, uint64_t retain_version);

    uint64_t version_ = 0;
    uint64_t inflight_reads_count_ = 0;
    std::unordered_map<uint64_t, WriteOp> inflight_writes_;
    RingArray<WriteOp> candidate_writes_{0};

    DISALLOW_COPY_AND_ASSIGN(KvChecker);
};

}  // namespace bcache2
