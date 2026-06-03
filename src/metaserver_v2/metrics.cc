// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/metrics.h"

#include "byte/embedded_metrics/metrics_registry.h"

namespace bcache2 {
namespace metaserver {

MetricsRef g_metrics;
void InitMetrics(const std::string& prefix, const byte::embedded_metrics::Tags& tags) {
    byte::embedded_metrics::MetricsOptions opts;
    opts.enable_logging = false;
    opts.report_interval_secs = 30;
    byte::embedded_metrics::MetricsRegistry::Registry().Initialize(prefix, tags, opts, nullptr);
    g_metrics = std::make_shared<Metrics>();
    g_metrics->Init(tags);
}

void QuitMetrics() { g_metrics.reset(); }

void Metrics::Init(const Tags& tags) {
    if (inited) {
        return;
    }
    inited = true;
    common_tags = tags;

    reboot_server_count.Initialize("reboot_server_count", {});
    missing_partition_count.Initialize("missing_partition_count", {});
    long_time_loading_partition_count.Initialize("long_time_loading_partition_count", {});
    replicator_error_partition_count.Initialize("replicator_error_partition_count", {});
    freeze_partition_count.Initialize("freeze_partition_count", {});

    server_heartbeat_count.Initialize("server_heartbeat_count", {});

    balance_partition_count.Initialize("balance_partition_count", {});

    placement_fail_count.Initialize("placement_fail_count", {});

    meta_query_count.Initialize("meta_query_count", {});
    meta_query_bytes.Initialize("meta_query_bytes", {});
    meta_query_fail_count.Initialize("meta_query_fail_count", {});
    meta_query_latency_us.Initialize("meta_query_latency_us", {});

    fsm_apply_fail_count.Initialize("fsm_apply_fail_count", {});
    event_harbor_queue_length.Initialize("event_harbor_queue_length", {});
}

void Metrics::EmitCounter(const std::string& name, double value, const Tags& tags) {
    byte::metrics2::Metrics::emit_counter(name, value, CombineTags(tags));
}

void Metrics::EmitStore(const std::string& name, double value, const Tags& tags) {
    byte::metrics2::Metrics::emit_store(name, value, CombineTags(tags));
}

void Metrics::EmitTimer(const std::string& name, double value, const Tags& tags) {
    byte::metrics2::Metrics::emit_timer(name, value, CombineTags(tags));
}

Metrics::Tags Metrics::CombineTags(const Tags& tags) {
    Tags full_tags = common_tags;
    for (auto& tag : tags) {
        full_tags.push_back(tag);
    }
    return full_tags;
}

}  // namespace metaserver
}  // namespace bcache2
