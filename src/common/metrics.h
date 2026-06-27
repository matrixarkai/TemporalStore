// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <byte/include/macros.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "bvar/bvar.h"
#include "common/status.h"
#include "common/time.h"

namespace byte {
class AsyncThreadPool;
}  // namespace byte

namespace bcache2 {

class MetricsEnv : public std::enable_shared_from_this<MetricsEnv> {
 public:
    using MetricsTags = std::vector<std::pair<std::string, std::string>>;

    class Counter {
     public:
        Counter() = default;
        Counter(const std::string& name, const MetricsTags& tags) { Expose(name, tags); }

        void Expose(const std::string& name, const MetricsTags& tags);
        void Increment() { value_ << 1; }
        void Add(uint64_t value) { value_ << static_cast<int64_t>(value); }
        void Set(double value) { value_ << static_cast<int64_t>(value); }
        int64_t GetValue() const { return value_.get_value(); }

     private:
        bvar::Adder<int64_t> value_;
    };

    class Guage {
     public:
        Guage() = default;
        Guage(const std::string& name, const MetricsTags& tags) { Expose(name, tags); }

        void Expose(const std::string& name, const MetricsTags& tags);
        void Set(double value) { value_.set_value(value); }
        void Add(double value) { value_.set_value(value_.get_value() + value); }
        void Increment() { Add(1); }

     private:
        bvar::Status<double> value_{0};
    };

    class Histogram {
     public:
        Histogram() = default;
        Histogram(const std::string& name, const MetricsTags& tags) { Expose(name, tags); }

        void Expose(const std::string& name, const MetricsTags& tags);
        void Set(double value) { latency_ << static_cast<int64_t>(value); }
        void Add(double value) { Set(value); }
        void Increment() { Set(1); }

     private:
        bvar::LatencyRecorder latency_;
    };

    template <typename MetricClass>
    class Holder {
     public:
        Holder() = default;
        Holder(const std::string& name, const MetricsTags& tags) { Initialize(name, tags); }

        void Initialize(const std::string& name, const MetricsTags& tags) {
            metric_.reset(new MetricClass(name, tags));
        }
        MetricClass* get() { return metric_.get(); }
        const MetricClass* get() const { return metric_.get(); }

     private:
        std::unique_ptr<MetricClass> metric_;
    };

    using CounterHolder = Holder<Counter>;
    using GuageHolder = Holder<Guage>;
    using HistogramHolder = Holder<Histogram>;

    struct Options {
        MetricsTags common_tags;
        std::string prefix = "bcache2.default";
        // Kept for API compatibility. Metrics are exposed through bvar/Prometheus now, so no
        // background embedded-metrics reporter is started here.
        byte::AsyncThreadPool* background_pool = nullptr;
        int base_metrics_update_interval_ms = 1000;
    };

    MetricsEnv() = default;
    ~MetricsEnv() = default;

    void Init(const Options& options);
    void Stop() { stopped_ = true; }

    static std::string MetricName(const std::string& name, const MetricsTags& tags);
    static MetricsTags CommonTags();

 private:
    std::atomic<bool> initialized_{false};
    std::atomic<bool> stopped_{false};
    Options options_;
    std::unique_ptr<GuageHolder> ready_metrics_;

    DISALLOW_COPY_AND_ASSIGN(MetricsEnv);
};

class MetricsManager {
 public:
    MetricsManager(MetricsEnv::MetricsTags common_tags, std::string module_name)
        : common_tags_(std::move(common_tags)), module_name_(std::move(module_name)) {}
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

    ~ScopedLatency() {
        if (histogram_ != nullptr) {
            histogram_->Set(cost_.GetElapsedInUs());
        }
    }

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
    explicit ModuleMetrics(MetricsManager*) {}
    virtual ~ModuleMetrics() {}
};

}  // namespace bcache2
