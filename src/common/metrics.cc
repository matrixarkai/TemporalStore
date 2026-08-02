// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/metrics.h"

#include <algorithm>
#include <cctype>
#include <mutex>

#include "common/logging.h"

namespace bcache2 {
namespace {

std::mutex g_metrics_mu;
std::string g_metrics_prefix = "bcache2.default";
MetricsEnv::MetricsTags g_common_tags;

std::string SanitizeMetricPart(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    bool last_was_underscore = false;
    for (char ch : value) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (std::isalnum(c)) {
            out.push_back(static_cast<char>(std::tolower(c)));
            last_was_underscore = false;
        } else if (!last_was_underscore) {
            out.push_back('_');
            last_was_underscore = true;
        }
    }
    while (!out.empty() && out.front() == '_') {
        out.erase(out.begin());
    }
    while (!out.empty() && out.back() == '_') {
        out.pop_back();
    }
    if (out.empty()) {
        return "unknown";
    }
    return out;
}

MetricsEnv::MetricsTags MergeCommonTags(const MetricsEnv::MetricsTags& tags) {
    MetricsEnv::MetricsTags merged = MetricsEnv::CommonTags();
    merged.insert(merged.end(), tags.begin(), tags.end());
    return merged;
}

}  // namespace

void MetricsEnv::Init(const Options& options) {
    bool set_initialized = false;
    if (!initialized_.compare_exchange_strong(set_initialized, true)) {
        return;
    }

    options_ = options;
    {
        std::lock_guard<std::mutex> lock(g_metrics_mu);
        g_metrics_prefix = options_.prefix;
        g_common_tags = options_.common_tags;
    }
    ready_metrics_.reset(new GuageHolder("process.ready", {}));
    ready_metrics_->get()->Set(1);
    LOG_INFO("Init Prometheus/bvar MetricsEnv success").put("prefix", options_.prefix);
}

MetricsEnv::MetricsTags MetricsEnv::CommonTags() {
    std::lock_guard<std::mutex> lock(g_metrics_mu);
    return g_common_tags;
}

std::string MetricsEnv::MetricName(const std::string& name, const MetricsTags& tags) {
    std::string prefix;
    {
        std::lock_guard<std::mutex> lock(g_metrics_mu);
        prefix = g_metrics_prefix;
    }

    std::string metric_name = SanitizeMetricPart(prefix) + "_" + SanitizeMetricPart(name);
    for (const auto& tag : tags) {
        metric_name += "_" + SanitizeMetricPart(tag.first) + "_" + SanitizeMetricPart(tag.second);
    }
    return metric_name;
}

void MetricsEnv::Counter::Expose(const std::string& name, const MetricsTags& tags) {
    value_.expose(MetricName(name, MergeCommonTags(tags)));
}

void MetricsEnv::Guage::Expose(const std::string& name, const MetricsTags& tags) {
    value_.expose(MetricName(name, MergeCommonTags(tags)));
}

void MetricsEnv::Histogram::Expose(const std::string& name, const MetricsTags& tags) {
    latency_.expose(MetricName(name, MergeCommonTags(tags)));
}

}  // namespace bcache2
