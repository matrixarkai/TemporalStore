// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "byte/embedded_metrics/metrics.h"
#include "byte/embedded_metrics/metrics_holder.h"
#include "byte/metrics/metrics.h"  // metrics2

namespace bcache2 {
namespace metaserver {

void InitMetrics(const std::string& prefix, const byte::embedded_metrics::Tags& tags);
void QuitMetrics();

struct Metrics;
using MetricsRef = std::shared_ptr<Metrics>;
using LatencyMetricsRecord = byte::embedded_metrics::OneLatencyGuard;
extern MetricsRef g_metrics;
#define MS_METRIC(name) (g_metrics->name)

struct Metrics {
    using Guage = byte::embedded_metrics::Guage;
    using Histogram = byte::embedded_metrics::Histogram;
    using Counter = byte::embedded_metrics::Counter;
    using Tags = byte::embedded_metrics::Tags;  // vector<pair<string, string>>
    template <typename M>
    using Holder = byte::embedded_metrics::MetricHolder<M>;

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

    // mics
    Holder<Counter> fsm_apply_fail_count;
    Holder<Guage> event_harbor_queue_length;

    void EmitCounter(const std::string& name, double value, const Tags& tags);
    void EmitStore(const std::string& name, double value, const Tags& tags);
    void EmitTimer(const std::string& name, double value, const Tags& tags);

    ////////

    bool inited{false};
    Tags common_tags;

    void Init(const Tags& tags);
    Tags CombineTags(const Tags& tags);
};

}  // namespace metaserver
}  // namespace bcache2

