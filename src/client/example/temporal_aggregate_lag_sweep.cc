#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client/client.h"
#include "client/client_impl.h"
#include "common/cmd_manager.h"
#include "common/controller.h"
#include "common/status.h"
#include "common/sync_closure.h"
#include "extension/modules.pb.h"
#include "extension/temporal_aggregate/interface.pb.h"

namespace {

uint64_t NowMs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
}

bool OpenTable(const std::string& metaserver, const std::string& idc,
               const std::string& namespace_name, const std::string& table_name,
               bool pin_primary, std::unique_ptr<bcache2::client::Client>* client,
               std::unique_ptr<bcache2::client::Table>* table,
               bcache2::client::TableCore** table_core) {
    bcache2::client::ClientOptions client_options;
    client_options.af = bcache2::client::AddressFamily::kIp4;
    client_options.master_addr = metaserver;
    client_options.idc = idc;
    client_options.host = "127.0.0.1";
    client_options.psm = "temporal.aggregate.lag.sweep";
    client_options.log_level = bcache2::client::LogLevel::kWarning;
    if (pin_primary) {
        client_options.partition_pick_opts.policy =
            bcache2::client::PartitionPickOptions::Policy::kPrimary;
    }

    bcache2::client::Client* raw_client = nullptr;
    bcache2::Status status = bcache2::client::Client::Create(client_options, &raw_client);
    if (!status.ok()) {
        std::cerr << "Client::Create failed: " << status.ToString() << std::endl;
        return false;
    }
    client->reset(raw_client);

    bcache2::client::TableOptions table_options;
    table_options.io_timeout_ms = 2000;
    table_options.connect_timeout_ms = 2000;
    bcache2::client::Table* raw_table = nullptr;
    status = (*client)->OpenTable(namespace_name, table_name, table_options, &raw_table);
    if (!status.ok()) {
        std::cerr << "OpenTable failed: " << status.ToString() << std::endl;
        return false;
    }
    table->reset(raw_table);
    *table_core = dynamic_cast<bcache2::client::TableCore*>(table->get());
    return *table_core != nullptr;
}

template <typename Request, typename Response>
bool ExecuteRaw(bcache2::client::TableCore* table, uint16_t module_id, uint16_t function_id,
                const std::string& partition_key, const Request& request, Response* response) {
    bcache2::client::TableCore::Request raw_request;
    bcache2::client::TableCore::Response raw_response;
    bcache2::Controller ctrl;
    bcache2::CoSyncClosure sync;

    raw_request.cmd_id = bcache2::MakeCmdId(module_id, function_id);
    raw_request.key = partition_key;
    raw_request.input.set_module_id(module_id);
    raw_request.input.set_function_id(function_id);

    std::string request_bytes;
    if (!request.SerializeToString(&request_bytes)) {
        return false;
    }
    raw_request.input.set_request_bytes(std::move(request_bytes));

    table->Execute(&ctrl, &raw_request, &raw_response, &sync, nullptr,
                   bcache2::client::RequestOptions());
    sync.Wait();
    if (!ctrl.status().ok()) {
        return false;
    }
    return response->ParseFromString(raw_response.output->response_bytes());
}

void FillDimensions(int feature_idx, int dimension_count,
                    google::protobuf::RepeatedPtrField<bcache2::temporal_aggregate::Dimension>*
                        dimensions) {
    dimensions->Clear();
    for (int i = 0; i < std::max(1, dimension_count); ++i) {
        auto* dim = dimensions->Add();
        dim->set_name("dim" + std::to_string(i));
        dim->set_value("v" + std::to_string((feature_idx + i * 131) % 1000000));
    }
}

bool Incr(bcache2::client::TableCore* table, const std::string& key, const std::string& metric,
          int feature_idx, int dimension_count, uint64_t timestamp_ms, uint64_t bucket_width_ms,
          std::string* error) {
    bcache2::temporal_aggregate::IncrRequest request;
    request.set_key(key);
    request.set_metric(metric);
    FillDimensions(feature_idx, dimension_count, request.mutable_dimensions());
    request.set_timestamp_ms(timestamp_ms);
    request.set_bucket_width_ms(bucket_width_ms);
    request.set_value(1);
    request.set_ttl_ms(24ULL * 60ULL * 60ULL * 1000ULL);
    request.set_op(bcache2::temporal_aggregate::COUNT);
    bcache2::temporal_aggregate::IncrResponse response;
    bcache2::client::TableCore::Request raw_request;
    bcache2::client::TableCore::Response raw_response;
    bcache2::Controller ctrl;
    bcache2::CoSyncClosure sync;

    raw_request.cmd_id =
        bcache2::MakeCmdId(bcache2::Module::TEMPORAL_AGGREGATE,
                           bcache2::temporal_aggregate::INCR);
    raw_request.key = key;
    raw_request.input.set_module_id(bcache2::Module::TEMPORAL_AGGREGATE);
    raw_request.input.set_function_id(bcache2::temporal_aggregate::INCR);

    std::string request_bytes;
    if (!request.SerializeToString(&request_bytes)) {
        if (error != nullptr) {
            *error = "SerializeToString failed";
        }
        return false;
    }
    raw_request.input.set_request_bytes(std::move(request_bytes));

    table->Execute(&ctrl, &raw_request, &raw_response, &sync, nullptr,
                   bcache2::client::RequestOptions());
    sync.Wait();
    if (!ctrl.status().ok()) {
        if (error != nullptr) {
            *error = ctrl.status().ToString();
        }
        return false;
    }
    if (!response.ParseFromString(raw_response.output->response_bytes())) {
        if (error != nullptr) {
            *error = "response parse failed";
        }
        return false;
    }
    return true;
}

bool QueryVisible(bcache2::client::TableCore* table, const std::string& key,
                  const std::string& metric, int feature_idx, int dimension_count,
                  uint64_t start_ms, uint64_t end_ms, uint64_t bucket_width_ms, int expected) {
    bcache2::temporal_aggregate::QueryRequest request;
    request.set_key(key);
    request.set_metric(metric);
    FillDimensions(feature_idx, dimension_count, request.mutable_dimensions());
    request.set_start_timestamp_ms(start_ms);
    request.set_end_timestamp_ms(end_ms);
    request.set_bucket_width_ms(bucket_width_ms);
    request.set_op(bcache2::temporal_aggregate::COUNT);
    bcache2::temporal_aggregate::QueryResponse response;
    return ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                      bcache2::temporal_aggregate::QUERY, key, request, &response) &&
           response.has_value() && response.value() == expected &&
           response.buckets_size() == expected;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5 || argc > 13) {
        std::cout << "usage: " << argv[0]
                  << " <metaserver_host:port> <idc> <namespace> <table> [features=10000]"
                     " [keys=1000] [buckets=12] [threads=1] [dimension_count=2]"
                     " [bucket_width_ms=60000] [poll_ms=100] [max_wait_ms=30000]"
                  << std::endl;
        return 2;
    }

    const std::string metaserver = argv[1];
    const std::string idc = argv[2];
    const std::string namespace_name = argv[3];
    const std::string table_name = argv[4];
    const int features = argc > 5 ? std::atoi(argv[5]) : 10000;
    const int keys = argc > 6 ? std::atoi(argv[6]) : 1000;
    const int buckets = argc > 7 ? std::atoi(argv[7]) : 12;
    const int threads = argc > 8 ? std::atoi(argv[8]) : 16;
    const int dimension_count = argc > 9 ? std::atoi(argv[9]) : 2;
    const uint64_t bucket_width_ms = argc > 10 ? std::strtoull(argv[10], nullptr, 10) : 60000;
    const int poll_ms = argc > 11 ? std::atoi(argv[11]) : 100;
    const int max_wait_ms = argc > 12 ? std::atoi(argv[12]) : 30000;

    const std::string prefix = "lag_" + std::to_string(NowMs()) + "_" +
                               std::to_string(static_cast<unsigned long long>(std::rand()));
    const uint64_t start_ms = NowMs();
    const uint64_t end_ms = start_ms + static_cast<uint64_t>(buckets) * bucket_width_ms;

    std::cout << "prefix=" << prefix << std::endl;
    std::cout << "features=" << features << std::endl;
    std::cout << "keys=" << keys << std::endl;
    std::cout << "buckets=" << buckets << std::endl;
    std::cout << "threads=" << threads << std::endl;
    std::cout << "dimensions=" << dimension_count << std::endl;
    std::cout << "poll_ms=" << poll_ms << std::endl;
    std::cout << "max_wait_ms=" << max_wait_ms << std::endl;

    const auto ingest_begin = std::chrono::steady_clock::now();
    std::atomic<int> ingest_next{0};
    std::atomic<int> ingest_errors{0};
    std::mutex first_error_mu;
    std::string first_error;
    std::vector<std::thread> ingest_workers;
    for (int t = 0; t < threads; ++t) {
        ingest_workers.emplace_back([&]() {
            std::unique_ptr<bcache2::client::Client> primary_client;
            std::unique_ptr<bcache2::client::Table> primary_table_holder;
            bcache2::client::TableCore* primary_table = nullptr;
            if (!OpenTable(metaserver, idc, namespace_name, table_name, true, &primary_client,
                           &primary_table_holder, &primary_table)) {
                ingest_errors.fetch_add(1);
                return;
            }
            while (true) {
                const int op = ingest_next.fetch_add(1);
                if (op >= features * buckets) {
                    break;
                }
                const int feature_idx = op / buckets;
                const int bucket = op % buckets;
                const std::string key = prefix + ":entity:" + std::to_string(feature_idx % keys);
                const std::string metric = "feature_" + std::to_string(feature_idx);
                std::string error;
                if (!Incr(primary_table, key, metric, feature_idx, dimension_count,
                          start_ms + static_cast<uint64_t>(bucket) * bucket_width_ms,
                          bucket_width_ms, &error)) {
                    ingest_errors.fetch_add(1);
                    std::lock_guard<std::mutex> lock(first_error_mu);
                    if (first_error.empty()) {
                        first_error = error;
                    }
                }
            }
        });
    }
    for (auto& worker : ingest_workers) {
        worker.join();
    }
    const auto ingest_end = std::chrono::steady_clock::now();
    const int64_t ingest_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(ingest_end - ingest_begin).count();
    std::cout << "ingest,ops,errors,elapsed_ms" << std::endl;
    std::cout << "ingest," << (features * buckets) << "," << ingest_errors.load() << ","
              << ingest_ms << std::endl;
    if (!first_error.empty()) {
        std::cout << "first_ingest_error=" << first_error << std::endl;
    }

    std::vector<int64_t> visible_ms(static_cast<size_t>(features), -1);
    const auto poll_begin = std::chrono::steady_clock::now();
    int visible = 0;
    int last_visible = -1;
    while (true) {
        const int64_t elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::steady_clock::now() - poll_begin)
                                       .count();
        std::atomic<int> poll_next{0};
        std::atomic<int> newly_visible{0};
        std::vector<std::thread> poll_workers;
        for (int t = 0; t < threads; ++t) {
            poll_workers.emplace_back([&]() {
                std::unique_ptr<bcache2::client::Client> secondary_client;
                std::unique_ptr<bcache2::client::Table> secondary_table_holder;
                bcache2::client::TableCore* secondary_table = nullptr;
                if (!OpenTable(metaserver, idc, namespace_name, table_name, false,
                               &secondary_client, &secondary_table_holder, &secondary_table)) {
                    return;
                }
                while (true) {
                    const int feature_idx = poll_next.fetch_add(1);
                    if (feature_idx >= features) {
                        break;
                    }
                    if (visible_ms[static_cast<size_t>(feature_idx)] >= 0) {
                        continue;
                    }
                    const std::string key =
                        prefix + ":entity:" + std::to_string(feature_idx % keys);
                    const std::string metric = "feature_" + std::to_string(feature_idx);
                    if (QueryVisible(secondary_table, key, metric, feature_idx, dimension_count,
                                     start_ms, end_ms, bucket_width_ms, buckets)) {
                        visible_ms[static_cast<size_t>(feature_idx)] = elapsed_ms;
                        newly_visible.fetch_add(1);
                    }
                }
            });
        }
        for (auto& worker : poll_workers) {
            worker.join();
        }
        visible += newly_visible.load();
        if (visible != last_visible || elapsed_ms >= max_wait_ms || visible == features) {
            std::cout << "lag_poll,elapsed_ms,visible,missing" << std::endl;
            std::cout << "lag_poll," << elapsed_ms << "," << visible << ","
                      << (features - visible) << std::endl;
            last_visible = visible;
        }
        if (visible == features || elapsed_ms >= max_wait_ms) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(std::max(1, poll_ms)));
    }

    std::vector<int64_t> seen;
    seen.reserve(static_cast<size_t>(features));
    for (int64_t value : visible_ms) {
        if (value >= 0) {
            seen.push_back(value);
        }
    }
    std::sort(seen.begin(), seen.end());
    auto pct = [&](double p) -> int64_t {
        if (seen.empty()) {
            return -1;
        }
        return seen[static_cast<size_t>(p * static_cast<double>(seen.size() - 1) + 0.5)];
    };

    std::cout << "lag_summary,features,visible,missing,p50_ms,p95_ms,p99_ms,max_ms" << std::endl;
    std::cout << "lag_summary," << features << "," << visible << "," << (features - visible)
              << "," << pct(0.50) << "," << pct(0.95) << "," << pct(0.99) << ","
              << (seen.empty() ? -1 : seen.back()) << std::endl;
    return ingest_errors.load() == 0 && visible == features ? 0 : 1;
}
