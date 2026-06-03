// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/concurrent/count_down_latch.h>
#include <byte/include/macros.h>
#include <byte/thread/async_thread.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/controller.h"

namespace bcache2 {

class Operation {
 public:
    virtual ~Operation() {}

    virtual void Run(Controller* ctrl, Closure<void>* callback) = 0;
};

class Verifier {
 public:
    virtual ~Verifier() {}

    virtual Operation* GeneratorOp(int op_type) = 0;
    virtual void FinishOp(Operation* op) = 0;
};

class Bench {
 public:
    struct Options {
        byte::AsyncThreadPool* thread_pool = nullptr;
        int jobs = 1;
        Verifier* verifier = nullptr;
    };

    struct Stat {
        std::string name;
        uint64_t succ_count = 0;
        uint64_t succ_avg_latency_ns = 0;
        uint64_t fail_count = 0;
    };

    Bench();
    virtual ~Bench();

    void Init(const Options& options);
    void RegisterOp(int op_type, const std::string& name, uint32_t depth, uint64_t interval_ms);
    void RegisterOp(int op_type, const std::string& name, const std::string& freq);

    void Start();
    void Stop();
    std::vector<Stat> GetStats();
    void ShowStats();

 private:
    struct OpInfo;
    struct OpContext {
        OpInfo* op_info = nullptr;
        byte::AsyncThread* thread = nullptr;
        Operation* operation = nullptr;
        Controller ctrl;
        uint64_t start_time_ns = 0;
        uint64_t end_time_ns = 0;

        uint64_t succ_count = 0;
        uint64_t succ_sum_latency = 0;
        uint64_t fail_count = 0;
        uint64_t fail_sum_latency = 0;
    } __attribute__((aligned(64)));

    struct OpInfo {
        int op_type = 0;
        std::string name;
        uint32_t depth = 1;
        uint64_t interval_us = 0;
        std::unique_ptr<OpContext[]> contexts;

        uint64_t last_succ_count = 0;
        uint64_t last_succ_sum_latency = 0;
        uint64_t last_fail_count = 0;
        uint64_t last_fail_sum_latency = 0;
    };

    void RunOp(OpContext* context);
    void OnRunOpDone(OpContext* context);

    Options options_;
    std::unordered_map<int, std::unique_ptr<OpInfo>> op_info_map_;
    bool stop_ = false;
    std::unique_ptr<byte::CountDownLatch> count_latch_;

    DISALLOW_COPY_AND_ASSIGN(Bench);
};

}  // namespace bcache2
