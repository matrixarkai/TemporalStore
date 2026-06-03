// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <brpc/server.h>
#include <bthread/timer_thread.h>

#include "common/metrics.h"
#include "common/logging.h"

namespace bcache2 {

const char kCpuMetrics[] = "cpu";
const char kMemoryMetrics[] = "memory";
const char kThroughputInputMetrics[] = "throughput.input";
const char kThroughputOutputMetrics[] = "throughput.output";

void MetricsEnv::Init(const Options& options) {
    bool set_initialized = false;
    if (!initialized_.compare_exchange_strong(set_initialized, true)) {
        return;
    }

    options_ = options;
    byte::embedded_metrics::MetricsOptions registry_options;
    registry_options.enable_logging = false;
    byte::embedded_metrics::MetricsRegistry::Registry().Initialize(
        options_.prefix, options_.common_tags, registry_options, nullptr);
    cpu_metrics_.reset(new GuageHolder(kCpuMetrics, {}));
    memory_metrics_.reset(new GuageHolder(kMemoryMetrics, {}));
    input_metrics_.reset(new GuageHolder(kThroughputInputMetrics, {}));
    output_metrics_.reset(new GuageHolder(kThroughputOutputMetrics, {}));
    if (options_.background_pool != nullptr) {
        auto guard = shared_from_this();
        auto func = [this, guard] { BaseMetricsSchedule(); };
        options_.background_pool->PushTask(NewFuncClosure(func),
                                           options_.base_metrics_update_interval_ms);
    }
    LOG_INFO("Init MetricsEnv success").put("prefix", options_.prefix);
}

void MetricsEnv::BaseMetricsSchedule() {
    if (stoped_) {
        return;
    }

    double cpu = atof(bvar::Variable::describe_exposed("process_cpu_usage").data()) * 100;
    int64_t memory = atoll(bvar::Variable::describe_exposed("process_memory_resident").data());
    int64_t input = atoll(bvar::Variable::describe_exposed("process_io_read_bytes_second").data());
    int64_t output =
        atoll(bvar::Variable::describe_exposed("process_io_write_bytes_second").data());
    cpu_metrics_->get()->Set(cpu);
    memory_metrics_->get()->Set(memory);
    input_metrics_->get()->Set(input);
    output_metrics_->get()->Set(output);
    auto guard = shared_from_this();
    options_.background_pool->PushTask(NewFuncClosure([this, guard] { BaseMetricsSchedule(); }),
                                       options_.base_metrics_update_interval_ms);
}

}  // namespace bcache2
