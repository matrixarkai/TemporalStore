// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/btree_map.h>
#include <bvar/bvar.h>
#include <byte/concurrent/count_down_latch.h>
#include <byte/include/assert.h>
#include <byte/thread/async_thread.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bench/flags.h"
#include "common/metrics.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

class Client;
class WorkloadsBunch;

class Bench {
 public:
    struct Options {
        Client* client = nullptr;
        WorkloadsBunch* workloads = nullptr;
        uint64_t jobs = 0;
        uint64_t depth = 0;
        uint64_t key_ttl_ms = 0;
        uint64_t delay_start_ms = 0;
        bool stay_operations = false;
    };

    Bench() = default;
    ~Bench() { Stop(); }

    void Init(Options opts);

    void Start();
    void Stop();

    std::vector<CommandStats> GetCmdStats() const;
    CommandStats GetTotalStats() const;
    void PrintStats() const;
    uint64_t GetTotalOperationsNum() const { return total_operations_num_; }
    bool Stopped() const { return stopped_; }

    std::vector<std::vector<Operation>> ExtractOperations() {
        std::vector<std::vector<Operation>> ret;
        for (auto& worker : worker_contextes_) {
            ret.emplace_back(std::move(worker->operations));
        }
        return ret;
    }

 private:
    struct Stats {
        bool effective = false;
        bvar::Adder<uint64_t> send_bytes;
        bvar::Adder<uint64_t> receive_bytes;
        bvar::PerSecond<bvar::Adder<uint64_t>> send_throughput{&send_bytes};
        bvar::PerSecond<bvar::Adder<uint64_t>> receive_throughput{&receive_bytes};
        bvar::LatencyRecorder succ_latency_us;
        bvar::LatencyRecorder fail_latency_us;
        bvar::LatencyRecorder total_latency_us;
    };
    struct WorkerContext {
        uint64_t id = 0;
        std::vector<Operation> operations;

        explicit WorkerContext(uint64_t id) : id(id) {}
    };

    void MainLoop(WorkerContext* worker);
    void Wakeup(WorkerContext* worker);
    void Postpone(WorkerContext* worker);
    void StatOperation(WorkerContext* worker, const Operation& operation);

    Options opts_;
    bool stopped_ = false;

    std::unique_ptr<byte::CountDownLatch> worker_countdown_;
    byte::AsyncThreadPool work_pool_;
    std::vector<std::unique_ptr<WorkerContext>> worker_contextes_;
    std::atomic<uint64_t> total_operations_num_{0};

    std::atomic<uint64_t> wakeup_worker_num_{0};

    Stats total_stats_;
    std::unordered_map<uint64_t, Stats> cmd_stats_;

    DISALLOW_COPY_AND_ASSIGN(Bench);
};

}  // namespace bench
}  // namespace bcache2
