// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/metrics.h"
#include "common/time.h"

namespace bcache2 {
namespace metaserver {

void InitMetrics(const std::string& prefix, const MetricsEnv::MetricsTags& tags);
void QuitMetrics();

struct Metrics;
using MetricsRef = std::shared_ptr<Metrics>;
extern MetricsRef g_metrics;
#define MS_METRIC(name) (g_metrics->name)

class LatencyMetricsRecord {
 public:
    explicit LatencyMetricsRecord(MetricsEnv::HistogramHolder* holder)
        : histogram_(holder == nullptr ? nullptr : holder->get()) {}
    explicit LatencyMetricsRecord(MetricsEnv::Histogram* histogram) : histogram_(histogram) {}
    ~LatencyMetricsRecord() {
        if (histogram_ != nullptr) {
            histogram_->Set(cost_.GetElapsedInUs());
        }
    }

 private:
    TimeCost cost_;
    MetricsEnv::Histogram* histogram_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(LatencyMetricsRecord);
};

struct Metrics {
    using Guage = MetricsEnv::Guage;
    using Histogram = MetricsEnv::Histogram;
    using Counter = MetricsEnv::Counter;
    using Tags = MetricsEnv::MetricsTags;
    template <typename M>
    using Holder = MetricsEnv::Holder<M>;

    // ha
    Holder<Counter> reboot_server_count;
    Holder<Counter> missing_partition_count;
    Holder<Counter> long_time_loading_partition_count;
    Holder<Counter> replicator_error_partition_count;
    Holder<Counter> freeze_partition_count;
    Holder<Counter> server_heartbeat_count;

    // balance
    Holder<Counter> balance_partition_count;

    // placement
    Holder<Counter> placement_fail_count;

    // services
    Holder<Counter> meta_query_count;
    Holder<Counter> meta_query_bytes;
    Holder<Counter> meta_query_fail_count;
    Holder<Histogram> meta_query_latency_us;

    // misc
    Holder<Counter> fsm_apply_fail_count;
    Holder<Guage> event_harbor_queue_length;

    void EmitCounter(const std::string& name, double value, const Tags& tags);
    void EmitStore(const std::string& name, double value, const Tags& tags);
    void EmitTimer(const std::string& name, double value, const Tags& tags);

    bool inited{false};
    Tags common_tags;

    void Init(const Tags& tags);
};

}  // namespace metaserver
}  // namespace bcache2
