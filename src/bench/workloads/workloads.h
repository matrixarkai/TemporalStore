// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/ratio_dice.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

class Workload {
 public:
    Workload() = default;
    virtual ~Workload() = default;

    // workload name
    virtual std::string Name() const = 0;

    // gen operation
    virtual Operation NextOperation(const std::string& key, const std::string& value) = 0;
};

class WorkloadsBunch {
 public:
    enum class KeyPattern {
        kSequential,
    };

    enum class KeyDist {
        kUniform,
        kZipfian,
    };

    struct Options {
        std::string id;

        uint64_t key_count = 0;
        uint64_t key_size = 0;
        KeyPattern key_pattern = KeyPattern::kSequential;
        KeyDist key_dis = KeyDist::kUniform;

        uint64_t value_count = 0;
        uint64_t value_min_size = 0;
        uint64_t value_max_size = 0;

        double zipfian_alpha = 0.0;
    };

    WorkloadsBunch() = default;
    ~WorkloadsBunch() {
        for (auto& workload : workloads_) {
            delete workload;
        }
    }

    void Init(Options opts);

    void RegisterWorkload(Workload* workload, uint64_t ratio) {
        BYTE_ASSERT(ratio > 0);
        workloads_.emplace_back(workload);
        workloads_dice_.AddProperty(ratio, workloads_.size() - 1);
    }

    // thread safe
    Operation NextOperation() {
        total_count_++;

        std::string key = RandomKey();
        std::string value = RandomValue();

        std::lock_guard<std::mutex> _(mutex_);
        size_t idx = workloads_dice_.Roll();
        return workloads_[idx]->NextOperation(key, value);
    }

    void SetRound(uint64_t round) { round_ = round; }
    uint64_t GetRound() const { return round_; }

    void SetReusedKeys(std::vector<std::string> reused_keys) {
        reused_keys_ = std::move(reused_keys);
    }
    const std::vector<std::string>& GetReusedKeys() const { return reused_keys_; }
    const std::vector<std::string>& GetRawKeys() const { return random_raw_keys_; }
    std::string GetRoundKey(const std::string& raw_key) const {
        return "workloads_" + opts_.id + ":" + "round_" + std::to_string(round_) + ":" + "key_" +
               raw_key;
    }

    uint64_t GetTotalCount() const { return total_count_; }

 private:
    std::string RandomKey() const;
    std::string RandomValue() const;

    Options opts_;
    uint64_t round_ = 0;
    std::atomic<uint64_t> total_count_{0};
    std::vector<std::string> random_raw_keys_;
    std::vector<std::string> random_values_;
    std::vector<std::string> reused_keys_;

    std::mutex mutex_;
    std::vector<Workload*> workloads_;
    RatioDice<size_t> workloads_dice_;  // to roll a idx in workloads_
};

}  // namespace bench
}  // namespace bcache2
