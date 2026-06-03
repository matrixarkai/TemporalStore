// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/concurrent/count_down_latch.h>
#include <byte/thread/async_thread.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "bench/model/model.h"
#include "common/macros.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

class ConsistencyChecker {
 public:
    struct Options {
        uint64_t worker_num = 0;
        bool eventual_consistency_mode = false;
        uint64_t eventual_consistency_history_time_us = 0;
        uint64_t max_ambiguous_time_ms = 0;
        uint64_t max_expire_ambiguous_time_ms = 0;
        uint64_t timeout_ms = 0;
    };

    ConsistencyChecker() {}
    ~ConsistencyChecker() {
        if (checker_countdown_ != nullptr) {
            checker_countdown_->Wait();
        }
        worker_pool_.Stop();
    }

    void Init(Options opts);

    void CheckConsistency(std::vector<std::vector<Operation>> ops);

    bool Checking() const {
        if (checker_countdown_ == nullptr) {
            return false;
        }
        return checker_countdown_->GetCount();
    }
    bool Consistency() const { return consistency_; }
    bool Timeout() const { return timeout_; }
    void PrintStats();

 private:
    struct CheckContext {
        std::string key;
        std::vector<Operation> ops;
        std::vector<const Model*> states_history;
        std::multiset<uint64_t> end_time_us_heap;  // for prune
        byte::CountDownLatch* countdown = nullptr;
    };

    void CheckInternal(CheckContext* context);
    bool WG(CheckContext* context, size_t depth, uint64_t max_start_time_us,
            const Model* current_state, uint64_t start_time_us);
    Status TryApplyOperation(CheckContext* context, size_t depth, const Operation& op,
                             std::vector<std::unique_ptr<Model>>* next_states);

    Options opts_;
    Model::ApplyOptions apply_opts_;
    byte::AsyncThreadPool worker_pool_;

    bool consistency_ = false;
    bool timeout_ = false;
    uint64_t total_checker_ = 0;
    std::unique_ptr<byte::CountDownLatch> checker_countdown_;

    DISALLOW_COPY_AND_ASSIGN(ConsistencyChecker);
};

}  // namespace bench
}  // namespace bcache2
