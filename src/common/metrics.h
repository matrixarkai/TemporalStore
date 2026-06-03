// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/embedded_metrics/metrics_holder.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/coclosure.h"
#include "common/status.h"
#include "common/time.h"

namespace bcache2 {

class MetricsEnv : public std::enable_shared_from_this<MetricsEnv> {
 public:
    using Counter = byte::embedded_metrics::Counter;
    using Guage = byte::embedded_metrics::Guage;
    using Histogram = byte::embedded_metrics::Histogram;
    using HistogramType = byte::embedded_metrics::HistogramType;
    using MetricsTags = byte::embedded_metrics::Tags;

    template <typename MetricClass>
    using Holder = typename byte::embedded_metrics::MetricHolder<MetricClass>;

    using CounterHolder = Holder<Counter>;
    using GuageHolder = Holder<Guage>;
    using HistogramHolder = Holder<Histogram>;

    struct Options {
        MetricsTags common_tags;
        std::string prefix = "bcache2.default";
        byte::AsyncThreadPool* background_pool = nullptr;
        int base_metrics_update_interval_ms = 1000;
    };

    MetricsEnv() = default;
    ~MetricsEnv() = default;

    void Init(const Options& options);
    void Stop() { stoped_ = true; }

 private:
    void BaseMetricsSchedule();

    std::atomic<bool> initialized_{false};
    std::atomic<bool> stoped_{false};
    Options options_;
    std::unique_ptr<GuageHolder> cpu_metrics_;
    std::unique_ptr<GuageHolder> memory_metrics_;
    std::unique_ptr<GuageHolder> input_metrics_;
    std::unique_ptr<GuageHolder> output_metrics_;

    DISALLOW_COPY_AND_ASSIGN(MetricsEnv);
};

class MetricsManager {
 public:
    MetricsManager(MetricsEnv::MetricsTags common_tags, std::string module_name)
        : common_tags_(std::move(common_tags)), module_name_(module_name) {}
    ~MetricsManager() {}

    template <typename MetricClass>
    std::unique_ptr<MetricsEnv::Holder<MetricClass>> Get(const std::string& name,
                                                         const MetricsEnv::MetricsTags& tags) {
        MetricsEnv::MetricsTags merge_tags = tags;
        merge_tags.insert(merge_tags.end(), common_tags_.begin(), common_tags_.end());
        return std::unique_ptr<MetricsEnv::Holder<MetricClass>>(
            new MetricsEnv::Holder<MetricClass>(module_name_ + "." + name, merge_tags));
    }

 private:
    MetricsEnv::MetricsTags common_tags_;
    std::string module_name_;

    DISALLOW_COPY_AND_ASSIGN(MetricsManager);
};

class ScopedLatency {
 public:
    explicit ScopedLatency(MetricsEnv::Histogram* histogram) : histogram_(histogram) {}

    ~ScopedLatency() { histogram_->Set(cost_.GetElapsedInUs()); }

 private:
    TimeCost cost_;
    MetricsEnv::Histogram* histogram_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(ScopedLatency);
};

class RequestMetrics {
 public:
    RequestMetrics(MetricsManager* manager, const std::string& name,
                   const MetricsEnv::MetricsTags& tags) {
        BYTE_ASSERT(manager != nullptr);
        success_ = manager->Get<MetricsEnv::Counter>(name + ".success", tags);
#define DEFINE_CODE(code_name, code_value)                                                   \
    do {                                                                                     \
        MetricsEnv::MetricsTags new_tags = tags;                                             \
        new_tags.push_back({"code", std::to_string(code_value)});                            \
        new_tags.push_back({"code_name", #code_name});                                       \
        failed_.emplace_back(manager->Get<MetricsEnv::Counter>(name + ".failed", new_tags)); \
    } while (false);
#include "common/code.inc"  // NOLINT
#undef DEFINE_CODE
        latency_ = manager->Get<MetricsEnv::Histogram>(name + ".latency", tags);
        request_bytes_ = manager->Get<MetricsEnv::Counter>(name + ".request_bytes", tags);
        response_bytes_ = manager->Get<MetricsEnv::Counter>(name + ".response_bytes", tags);
        request_size_ = manager->Get<MetricsEnv::Histogram>(name + ".request_size", tags);
        response_size_ = manager->Get<MetricsEnv::Histogram>(name + ".response_size", tags);
    }
    ~RequestMetrics() = default;

    void Set(bool success, int64_t latency, uint64_t request_bytes, uint64_t response_bytes,
             int error_code) {
        success ? success_->get()->Increment() : SetFailedMetrics(error_code);
        latency_->get()->Set(latency);
        request_bytes_->get()->Add(request_bytes);
        response_bytes_->get()->Add(response_bytes);
        request_size_->get()->Set(request_bytes);
        response_size_->get()->Set(response_bytes);
    }

 private:
    void SetFailedMetrics(int error_code) {
        if (static_cast<size_t>(error_code) >= failed_.size()) {
            error_code = kUnknown;
        }
        failed_[error_code]->get()->Increment();
    }
    std::unique_ptr<MetricsEnv::CounterHolder> success_;
    std::vector<std::unique_ptr<MetricsEnv::CounterHolder>> failed_;
    std::unique_ptr<MetricsEnv::HistogramHolder> latency_;
    std::unique_ptr<MetricsEnv::HistogramHolder> request_size_;
    std::unique_ptr<MetricsEnv::HistogramHolder> response_size_;
    std::unique_ptr<MetricsEnv::CounterHolder> request_bytes_;
    std::unique_ptr<MetricsEnv::CounterHolder> response_bytes_;

    DISALLOW_COPY_AND_ASSIGN(RequestMetrics);
};

struct ModuleMetrics {
    // dummy struct for polymorphism only
    explicit ModuleMetrics(MetricsManager*) {}
    virtual ~ModuleMetrics() {}
};

}  // namespace bcache2
