// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>

#include "common/metrics.h"

namespace bcache2 {
namespace stream {

struct BlobMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> read_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> read_throughput;
    std::unique_ptr<MetricsEnv::HistogramHolder> read_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> append_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> append_throughput;
    std::unique_ptr<MetricsEnv::HistogramHolder> append_latency;

    void Init(MetricsManager* metrics_manager, std::string uri) {
        read_qps = metrics_manager->Get<MetricsEnv::Counter>("blob.read_qps", {{"uri", uri}});
        read_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("blob.read_throughput", {{"uri", uri}});
        read_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("blob.read_latency", {{"uri", uri}});
        append_qps = metrics_manager->Get<MetricsEnv::Counter>("blob.append_qps", {{"uri", uri}});
        append_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("blob.append_throughput", {{"uri", uri}});
        append_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("blob.append_latency", {{"uri", uri}});
    }
};

struct StreamMetrics {
    std::unique_ptr<MetricsEnv::GuageHolder> blob_count;
    std::unique_ptr<MetricsEnv::GuageHolder> obsolete_blob_count;
    std::unique_ptr<MetricsEnv::GuageHolder> usage_size;
    std::unique_ptr<MetricsEnv::GuageHolder> incoming_size;
    std::unique_ptr<MetricsEnv::GuageHolder> inflight_size;
    std::unique_ptr<MetricsEnv::GuageHolder> physical_size;
    std::unique_ptr<MetricsEnv::GuageHolder> buffer_size;
    std::unique_ptr<MetricsEnv::CounterHolder> commit_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> commit_latency;

    void Init(MetricsManager* metrics_manager, std::string uri) {
        blob_count = metrics_manager->Get<MetricsEnv::Guage>("stream.blob_count", {{"uri", uri}});
        obsolete_blob_count =
            metrics_manager->Get<MetricsEnv::Guage>("stream.obsolete_blob_count", {{"uri", uri}});
        usage_size = metrics_manager->Get<MetricsEnv::Guage>("stream.usage_size", {{"uri", uri}});
        incoming_size =
            metrics_manager->Get<MetricsEnv::Guage>("stream.incoming_size", {{"uri", uri}});
        inflight_size =
            metrics_manager->Get<MetricsEnv::Guage>("stream.inflight_size", {{"uri", uri}});
        physical_size =
            metrics_manager->Get<MetricsEnv::Guage>("stream.physical_size", {{"uri", uri}});
        buffer_size = metrics_manager->Get<MetricsEnv::Guage>("stream.buffer_size", {{"uri", uri}});
        commit_qps = metrics_manager->Get<MetricsEnv::Counter>("stream.commit_qps", {{"uri", uri}});
        commit_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("stream.commit_latency", {{"uri", uri}});
    }
};

}  // namespace stream
}  // namespace bcache2
