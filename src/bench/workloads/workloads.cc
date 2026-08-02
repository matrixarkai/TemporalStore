// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/workloads/workloads.h"

#include <byte/include/assert.h>

#include <random>
#include <string>

#include "bench/dist_utils.h"

namespace bcache2 {
namespace bench {

static thread_local std::random_device dev;
static thread_local std::mt19937 rng(dev());

inline std::string RandomString(size_t length) {
    auto randchar = []() -> char {
        const char charset[] =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz";
        static std::uniform_int_distribution<int> dist(0, sizeof(charset) - 2);
        return charset[dist(rng)];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

void WorkloadsBunch::Init(Options opts) {
    opts_ = std::move(opts);

    random_raw_keys_.resize(opts_.key_count);
    for (size_t i = 0; i < random_raw_keys_.size(); ++i) {
        switch (opts.key_pattern) {
        case KeyPattern::kSequential:
            random_raw_keys_[i] = "key_" + std::to_string(i);
            size_t plug = opts.key_size >= random_raw_keys_[i].size()
                              ? opts.key_size - random_raw_keys_[i].size()
                              : 0;
            random_raw_keys_[i] = "key_" + std::string(plug, '0') + std::to_string(i);
            break;
        }
    }

    random_values_.resize(opts_.value_count);
    for (size_t i = 0; i < random_values_.size(); ++i) {
        std::uniform_int_distribution<uint64_t> uniform_dist(opts_.value_min_size,
                                                             opts_.value_max_size);
        random_values_[i] = RandomString(uniform_dist(rng));
    }
}

std::string WorkloadsBunch::RandomKey() const {
    uint64_t all_key_count = random_raw_keys_.size() + reused_keys_.size();
    std::uniform_int_distribution<int> uniform_dist(0, all_key_count - 1);

    size_t idx = 0;
    switch (opts_.key_dis) {
    case KeyDist::kUniform:
        idx = uniform_dist(rng);
        break;
    case KeyDist::kZipfian:
        idx = Zipf(all_key_count, opts_.zipfian_alpha) - 1;
        break;
    }

    if (idx >= random_raw_keys_.size()) {
        BYTE_ASSERT(!reused_keys_.empty());
        idx -= random_raw_keys_.size();
        return reused_keys_[idx];
    }

    return GetRoundKey(random_raw_keys_[idx]);
}

std::string WorkloadsBunch::RandomValue() const {
    std::uniform_int_distribution<int> idx_dist(0, random_values_.size() - 1);
    return "workloads_" + opts_.id + ":" + "round_" + std::to_string(round_) + ":" +
           random_values_[idx_dist(rng)];
}

}  // namespace bench
}  // namespace bcache2
