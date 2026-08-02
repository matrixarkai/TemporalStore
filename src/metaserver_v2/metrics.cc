// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/metrics.h"

#include <mutex>
#include <unordered_map>

namespace bcache2 {
namespace metaserver {
namespace {

std::shared_ptr<MetricsEnv> g_metrics_env;
std::mutex g_dynamic_metrics_mu;
std::unordered_map<std::string, std::unique_ptr<MetricsEnv::CounterHolder>> g_dynamic_counters;
std::unordered_map<std::string, std::unique_ptr<MetricsEnv::GuageHolder>> g_dynamic_gauges;
std::unordered_map<std::string, std::unique_ptr<MetricsEnv::HistogramHolder>> g_dynamic_histograms;

std::string DynamicKey(const std::string& name, const Metrics::Tags& tags) {
    return MetricsEnv::MetricName("metaserver.dynamic." + name, tags);
}

MetricsEnv::CounterHolder* DynamicCounter(const std::string& name, const Metrics::Tags& tags) {
    const std::string key = DynamicKey(name, tags);
    std::lock_guard<std::mutex> lock(g_dynamic_metrics_mu);
    auto it = g_dynamic_counters.find(key);
    if (it == g_dynamic_counters.end()) {
        auto holder = std::make_unique<MetricsEnv::CounterHolder>(
            "metaserver.dynamic." + name, tags);
        it = g_dynamic_counters.emplace(key, std::move(holder)).first;
    }
    return it->second.get();
}

MetricsEnv::GuageHolder* DynamicGauge(const std::string& name, const Metrics::Tags& tags) {
    const std::string key = DynamicKey(name, tags);
    std::lock_guard<std::mutex> lock(g_dynamic_metrics_mu);
    auto it = g_dynamic_gauges.find(key);
    if (it == g_dynamic_gauges.end()) {
        auto holder = std::make_unique<MetricsEnv::GuageHolder>("metaserver.dynamic." + name, tags);
        it = g_dynamic_gauges.emplace(key, std::move(holder)).first;
    }
    return it->second.get();
}

MetricsEnv::HistogramHolder* DynamicHistogram(const std::string& name, const Metrics::Tags& tags) {
    const std::string key = DynamicKey(name, tags);
    std::lock_guard<std::mutex> lock(g_dynamic_metrics_mu);
    auto it = g_dynamic_histograms.find(key);
    if (it == g_dynamic_histograms.end()) {
        auto holder = std::make_unique<MetricsEnv::HistogramHolder>(
            "metaserver.dynamic." + name, tags);
        it = g_dynamic_histograms.emplace(key, std::move(holder)).first;
    }
    return it->second.get();
}

}  // namespace

MetricsRef g_metrics;

void InitMetrics(const std::string& prefix, const MetricsEnv::MetricsTags& tags) {
    g_metrics_env = std::make_shared<MetricsEnv>();
    MetricsEnv::Options opts;
    opts.prefix = prefix;
    opts.common_tags = tags;
    g_metrics_env->Init(opts);

    g_metrics = std::make_shared<Metrics>();
    g_metrics->Init(tags);
}

void QuitMetrics() {
    g_metrics.reset();
    if (g_metrics_env != nullptr) {
        g_metrics_env->Stop();
        g_metrics_env.reset();
    }
    std::lock_guard<std::mutex> lock(g_dynamic_metrics_mu);
    g_dynamic_counters.clear();
    g_dynamic_gauges.clear();
    g_dynamic_histograms.clear();
}

void Metrics::Init(const Tags& tags) {
    if (inited) {
        return;
    }
    inited = true;
    common_tags = tags;

    reboot_server_count.Initialize("metaserver.ha.reboot_server_count", {});
    missing_partition_count.Initialize("metaserver.ha.missing_partition_count", {});
    long_time_loading_partition_count.Initialize(
        "metaserver.ha.long_time_loading_partition_count", {});
    replicator_error_partition_count.Initialize(
        "metaserver.ha.replicator_error_partition_count", {});
    freeze_partition_count.Initialize("metaserver.ha.freeze_partition_count", {});

    server_heartbeat_count.Initialize("metaserver.ha.server_heartbeat_count", {});

    balance_partition_count.Initialize("metaserver.balance.partition_count", {});

    placement_fail_count.Initialize("metaserver.placement.fail_count", {});

    meta_query_count.Initialize("metaserver.service.meta_query_count", {});
    meta_query_bytes.Initialize("metaserver.service.meta_query_bytes", {});
    meta_query_fail_count.Initialize("metaserver.service.meta_query_fail_count", {});
    meta_query_latency_us.Initialize("metaserver.service.meta_query_latency_us", {});

    fsm_apply_fail_count.Initialize("metaserver.fsm.apply_fail_count", {});
    event_harbor_queue_length.Initialize("metaserver.event_harbor.queue_length", {});
}

void Metrics::EmitCounter(const std::string& name, double value, const Tags& tags) {
    DynamicCounter(name, tags)->get()->Add(static_cast<uint64_t>(value));
}

void Metrics::EmitStore(const std::string& name, double value, const Tags& tags) {
    DynamicGauge(name, tags)->get()->Set(value);
}

void Metrics::EmitTimer(const std::string& name, double value, const Tags& tags) {
    DynamicHistogram(name, tags)->get()->Set(value);
}

}  // namespace metaserver
}  // namespace bcache2
