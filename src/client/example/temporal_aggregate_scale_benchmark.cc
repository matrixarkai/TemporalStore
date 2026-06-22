#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "client/client.h"
#include "client/client_impl.h"
#include "common/cmd_manager.h"
#include "common/controller.h"
#include "common/status.h"
#include "common/sync_closure.h"
#include "extension/common/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/temporal_aggregate/interface.pb.h"

namespace {

struct Summary {
    std::string phase;
    int ops = 0;
    int threads = 0;
    int errors = 0;
    double qps = 0;
    int64_t avg_us = 0;
    int64_t p50_us = 0;
    int64_t p95_us = 0;
    int64_t p99_us = 0;
    int64_t min_us = 0;
    int64_t max_us = 0;
    int64_t total_ms = 0;
};

enum class ReadMode {
    kPrimary,
    kDefault,
    kSecondary,
};

struct QueryDiagnostics {
    std::atomic<int> rpc_or_parse_errors{0};
    std::atomic<int> no_value{0};
    std::atomic<int> wrong_value{0};
    std::atomic<int> wrong_bucket_count{0};
    std::mutex sample_mu;
    std::vector<std::string> samples;
};

ReadMode ParseReadMode(const char* value, bool pin_primary_reads) {
    if (value == nullptr) {
        return pin_primary_reads ? ReadMode::kPrimary : ReadMode::kDefault;
    }
    const std::string mode(value);
    if (mode == "primary") {
        return ReadMode::kPrimary;
    }
    if (mode == "secondary" || mode == "force_secondary" || mode == "force-secondary") {
        return ReadMode::kSecondary;
    }
    return ReadMode::kDefault;
}

const char* ReadModeName(ReadMode mode) {
    switch (mode) {
        case ReadMode::kPrimary:
            return "primary";
        case ReadMode::kSecondary:
            return "secondary";
        case ReadMode::kDefault:
        default:
            return "default";
    }
}

int64_t Percentile(const std::vector<int64_t>& sorted, double pct) {
    if (sorted.empty()) {
        return 0;
    }
    const double pos = pct * static_cast<double>(sorted.size() - 1);
    return sorted[static_cast<size_t>(pos + 0.5)];
}

Summary Summarize(const std::string& phase, std::vector<int64_t> samples, int threads,
                  int errors, int64_t total_ms) {
    std::sort(samples.begin(), samples.end());
    Summary s;
    s.phase = phase;
    s.ops = static_cast<int>(samples.size());
    s.threads = threads;
    s.errors = errors;
    s.total_ms = total_ms;
    s.qps = total_ms > 0 ? static_cast<double>(s.ops) * 1000.0 / static_cast<double>(total_ms) : 0;
    const int64_t sum = std::accumulate(samples.begin(), samples.end(), int64_t{0});
    s.avg_us = samples.empty() ? 0 : sum / static_cast<int64_t>(samples.size());
    s.p50_us = Percentile(samples, 0.50);
    s.p95_us = Percentile(samples, 0.95);
    s.p99_us = Percentile(samples, 0.99);
    s.min_us = samples.empty() ? 0 : samples.front();
    s.max_us = samples.empty() ? 0 : samples.back();
    return s;
}

void PrintSummary(const Summary& s, int keys, int features, int buckets, int dimensions) {
    std::cout << "system,phase,ops,threads,keys,features,buckets,dimensions,errors,qps,avg_us,"
                 "p50_us,p95_us,p99_us,min_us,max_us,total_ms"
              << std::endl;
    std::cout << "TemporalStore," << s.phase << "," << s.ops << "," << s.threads << ","
              << keys << "," << features << "," << buckets << "," << dimensions << ","
              << s.errors << "," << static_cast<int64_t>(s.qps) << "," << s.avg_us << ","
              << s.p50_us << "," << s.p95_us << "," << s.p99_us << "," << s.min_us << ","
              << s.max_us << "," << s.total_ms << std::endl;
}

bool OpenTable(const std::string& metaserver, const std::string& idc,
               const std::string& namespace_name, const std::string& table_name,
               ReadMode read_mode, std::unique_ptr<bcache2::client::Client>* client,
               std::unique_ptr<bcache2::client::Table>* table,
               bcache2::client::TableCore** table_core) {
    bcache2::client::ClientOptions client_options;
    client_options.af = bcache2::client::AddressFamily::kIp4;
    client_options.master_addr = metaserver;
    client_options.idc = idc;
    client_options.host = "127.0.0.1";
    client_options.psm = "temporal.aggregate.scale.benchmark";
    client_options.log_level = bcache2::client::LogLevel::kWarning;
    if (read_mode == ReadMode::kPrimary) {
        client_options.partition_pick_opts.policy =
            bcache2::client::PartitionPickOptions::Policy::kPrimary;
    } else if (read_mode == ReadMode::kSecondary) {
        client_options.partition_pick_opts.policy =
            bcache2::client::PartitionPickOptions::Policy::kVdcAffinity;
        client_options.partition_pick_opts.affinity_vdc = "force-secondary-read";
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
    if (*table_core == nullptr) {
        std::cerr << "OpenTable did not return TableCore" << std::endl;
        return false;
    }
    return true;
}

template <typename Request, typename Response>
bool ExecuteRaw(bcache2::client::TableCore* table, uint16_t module_id, uint16_t function_id,
                const std::string& partition_key, const Request& request, Response* response,
                std::string* error = nullptr) {
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
        if (error != nullptr) {
            *error = "serialize_failed";
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
    if (!response->ParseFromString(raw_response.output->response_bytes())) {
        if (error != nullptr) {
            *error = "parse_failed";
        }
        return false;
    }
    return true;
}

void AddSample(QueryDiagnostics* diag, const std::string& sample) {
    std::lock_guard<std::mutex> lock(diag->sample_mu);
    if (diag->samples.size() < 12) {
        diag->samples.push_back(sample);
    }
}

void PrintQueryDiagnostics(const QueryDiagnostics& diag) {
    std::cout << "query_diagnostics,rpc_or_parse_errors,no_value,wrong_value,wrong_bucket_count"
              << std::endl;
    std::cout << "query_diagnostics," << diag.rpc_or_parse_errors.load() << ","
              << diag.no_value.load() << "," << diag.wrong_value.load() << ","
              << diag.wrong_bucket_count.load() << std::endl;
    std::cout << "query_diagnostic_samples,count" << std::endl;
    std::cout << "query_diagnostic_samples," << diag.samples.size() << std::endl;
    for (const auto& sample : diag.samples) {
        std::cout << "query_diagnostic_sample," << sample << std::endl;
    }
}

void FillDimensions(int feature_idx, int dimension_count,
                    google::protobuf::RepeatedPtrField<bcache2::temporal_aggregate::Dimension>*
                        dimensions) {
    dimensions->Clear();
    const int safe_dimension_count = std::max(1, dimension_count);
    for (int i = 0; i < safe_dimension_count; ++i) {
        auto* dim = dimensions->Add();
        dim->set_name("dim" + std::to_string(i));
        dim->set_value("v" + std::to_string((feature_idx + i * 131) % 1000000));
    }
}

bool WaitForReplica(bcache2::client::TableCore* table, const std::string& key,
                    const std::string& metric, int feature_idx, int dimension_count,
                    uint64_t start_ms, uint64_t end_ms, uint64_t bucket_width_ms,
                    int expected, int max_wait_ms, int* attempts, int64_t* elapsed_ms) {
    const auto begin = std::chrono::steady_clock::now();
    const auto deadline = begin + std::chrono::milliseconds(max_wait_ms);
    *attempts = 0;
    do {
        ++(*attempts);
        bcache2::temporal_aggregate::QueryRequest query;
        query.set_key(key);
        query.set_metric(metric);
        FillDimensions(feature_idx, dimension_count, query.mutable_dimensions());
        query.set_start_timestamp_ms(start_ms);
        query.set_end_timestamp_ms(end_ms);
        query.set_bucket_width_ms(bucket_width_ms);
        query.set_op(bcache2::temporal_aggregate::COUNT);
        bcache2::temporal_aggregate::QueryResponse response;
        if (ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                       bcache2::temporal_aggregate::QUERY, key, query, &response) &&
            response.has_value() && response.value() == expected) {
            *elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - begin)
                              .count();
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    *elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - begin)
                      .count();
    return false;
}

template <typename Fn>
Summary RunParallel(const std::string& phase, int ops, int threads, Fn fn) {
    std::vector<int64_t> samples(static_cast<size_t>(ops));
    std::atomic<int> next{0};
    std::atomic<int> errors{0};
    const auto begin = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&]() {
            while (true) {
                const int i = next.fetch_add(1);
                if (i >= ops) {
                    break;
                }
                const auto op_begin = std::chrono::steady_clock::now();
                if (!fn(i)) {
                    errors.fetch_add(1);
                }
                samples[static_cast<size_t>(i)] =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - op_begin)
                        .count();
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    const int64_t total_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                              begin)
            .count();
    return Summarize(phase, std::move(samples), threads, errors.load(), total_ms);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5 || argc > 14) {
        std::cout << "usage: " << argv[0]
                  << " <metaserver_host:port> <idc> <namespace> <table> [features=1000]"
                     " [keys=100] [buckets=12] [threads=16] [pin_primary_reads=1]"
                     " [dimension_count=2] [replica_wait_ms=5000] [bucket_width_ms=60000]"
                     " [read_mode=primary|default|secondary]"
                  << std::endl;
        return 2;
    }

    const std::string metaserver = argv[1];
    const std::string idc = argv[2];
    const std::string namespace_name = argv[3];
    const std::string table_name = argv[4];
    const int features = argc > 5 ? std::atoi(argv[5]) : 1000;
    const int keys = argc > 6 ? std::atoi(argv[6]) : 100;
    const int buckets = argc > 7 ? std::atoi(argv[7]) : 12;
    const int threads = argc > 8 ? std::atoi(argv[8]) : 16;
    const bool pin_primary_reads = argc > 9 ? std::atoi(argv[9]) != 0 : true;
    const int dimension_count = argc > 10 ? std::atoi(argv[10]) : 2;
    const int replica_wait_ms = argc > 11 ? std::atoi(argv[11]) : 5000;
    const uint64_t bucket_width_ms = argc > 12 ? std::strtoull(argv[12], nullptr, 10) : 60000;
    const ReadMode read_mode = ParseReadMode(argc > 13 ? argv[13] : nullptr, pin_primary_reads);

    if (features <= 0 || keys <= 0 || buckets <= 0 || threads <= 0 || bucket_width_ms == 0) {
        std::cerr << "features, keys, buckets, threads, and bucket_width_ms must be positive"
                  << std::endl;
        return 2;
    }

    std::unique_ptr<bcache2::client::Client> write_client;
    std::unique_ptr<bcache2::client::Table> write_table_holder;
    bcache2::client::TableCore* write_table = nullptr;
    if (!OpenTable(metaserver, idc, namespace_name, table_name, ReadMode::kPrimary, &write_client,
                   &write_table_holder, &write_table)) {
        return 1;
    }

    std::unique_ptr<bcache2::client::Client> read_client;
    std::unique_ptr<bcache2::client::Table> read_table_holder;
    bcache2::client::TableCore* read_table = nullptr;
    if (!OpenTable(metaserver, idc, namespace_name, table_name, read_mode, &read_client,
                   &read_table_holder, &read_table)) {
        return 1;
    }

    const std::string prefix =
        "tagg_" + std::to_string(std::time(nullptr)) + "_" +
        std::to_string(static_cast<unsigned long long>(std::random_device{}()));
    const uint64_t now_ms =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count());
    const uint64_t start_ms = now_ms - static_cast<uint64_t>(buckets - 1) * bucket_width_ms;
    const uint64_t end_ms = now_ms + bucket_width_ms;

    std::cout << "prefix=" << prefix << std::endl;
    std::cout << "features=" << features << std::endl;
    std::cout << "keys=" << keys << std::endl;
    std::cout << "buckets=" << buckets << std::endl;
    std::cout << "dimensions=" << dimension_count << std::endl;
    std::cout << "pin_primary_reads=" << (pin_primary_reads ? 1 : 0) << std::endl;
    std::cout << "read_mode=" << ReadModeName(read_mode) << std::endl;

    const int ingest_ops = features * buckets;
    Summary ingest = RunParallel("temporal_aggregate_incr", ingest_ops, threads, [&](int i) {
        const int feature_idx = i / buckets;
        const int bucket_idx = i % buckets;
        const std::string key = prefix + ":entity:" + std::to_string(feature_idx % keys);
        const std::string metric = "feature_" + std::to_string(feature_idx);
        bcache2::temporal_aggregate::IncrRequest incr;
        incr.set_key(key);
        incr.set_metric(metric);
        FillDimensions(feature_idx, dimension_count, incr.mutable_dimensions());
        incr.set_timestamp_ms(start_ms + static_cast<uint64_t>(bucket_idx) * bucket_width_ms);
        incr.set_bucket_width_ms(bucket_width_ms);
        incr.set_value(1);
        incr.set_ttl_ms(24ULL * 60ULL * 60ULL * 1000ULL);
        incr.set_op(bcache2::temporal_aggregate::COUNT);
        bcache2::temporal_aggregate::IncrResponse response;
        return ExecuteRaw(write_table, bcache2::Module::TEMPORAL_AGGREGATE,
                          bcache2::temporal_aggregate::INCR, key, incr, &response);
    });
    PrintSummary(ingest, keys, features, buckets, dimension_count);

    if (read_mode != ReadMode::kPrimary) {
        std::this_thread::sleep_for(std::chrono::milliseconds(replica_wait_ms));
    }

    QueryDiagnostics query_diag;
    Summary query = RunParallel("temporal_aggregate_query", features, threads, [&](int feature_idx) {
        const std::string key = prefix + ":entity:" + std::to_string(feature_idx % keys);
        const std::string metric = "feature_" + std::to_string(feature_idx);
        bcache2::temporal_aggregate::QueryRequest request;
        request.set_key(key);
        request.set_metric(metric);
        FillDimensions(feature_idx, dimension_count, request.mutable_dimensions());
        request.set_start_timestamp_ms(start_ms);
        request.set_end_timestamp_ms(end_ms);
        request.set_bucket_width_ms(bucket_width_ms);
        request.set_op(bcache2::temporal_aggregate::COUNT);
        bcache2::temporal_aggregate::QueryResponse response;
        std::string error;
        if (!ExecuteRaw(read_table, bcache2::Module::TEMPORAL_AGGREGATE,
                        bcache2::temporal_aggregate::QUERY, key, request, &response, &error)) {
            query_diag.rpc_or_parse_errors.fetch_add(1);
            AddSample(&query_diag, "feature=" + std::to_string(feature_idx) + ",key=" + key +
                                       ",metric=" + metric + ",error=" + error);
            return false;
        }
        if (!response.has_value()) {
            query_diag.no_value.fetch_add(1);
            AddSample(&query_diag, "feature=" + std::to_string(feature_idx) + ",key=" + key +
                                       ",metric=" + metric + ",has_value=0,buckets=" +
                                       std::to_string(response.buckets_size()));
            return false;
        }
        if (response.value() != buckets) {
            query_diag.wrong_value.fetch_add(1);
            AddSample(&query_diag, "feature=" + std::to_string(feature_idx) + ",key=" + key +
                                       ",metric=" + metric + ",value=" +
                                       std::to_string(response.value()) + ",expected=" +
                                       std::to_string(buckets) + ",buckets=" +
                                       std::to_string(response.buckets_size()));
            return false;
        }
        if (response.buckets_size() != buckets) {
            query_diag.wrong_bucket_count.fetch_add(1);
            AddSample(&query_diag, "feature=" + std::to_string(feature_idx) + ",key=" + key +
                                       ",metric=" + metric + ",value=" +
                                       std::to_string(response.value()) + ",bucket_count=" +
                                       std::to_string(response.buckets_size()) + ",expected=" +
                                       std::to_string(buckets));
            return false;
        }
        return true;
    });
    PrintSummary(query, keys, features, buckets, dimension_count);
    PrintQueryDiagnostics(query_diag);

    if (read_mode != ReadMode::kPrimary) {
        int attempts = 0;
        int64_t lag_ms = 0;
        std::unique_ptr<bcache2::client::Client> lag_client;
        std::unique_ptr<bcache2::client::Table> lag_table_holder;
        bcache2::client::TableCore* lag_table = nullptr;
        if (!OpenTable(metaserver, idc, namespace_name, table_name, read_mode, &lag_client,
                       &lag_table_holder, &lag_table)) {
            return 1;
        }
        const bool lag_ok = WaitForReplica(lag_table, prefix + ":entity:0", "feature_0", 0,
                                           dimension_count, start_ms, end_ms, bucket_width_ms,
                                           buckets, std::max(replica_wait_ms, 30000), &attempts,
                                           &lag_ms);
        std::cout << "replica_lag_probe,ok,attempts,elapsed_ms" << std::endl;
        std::cout << "replica_lag_probe," << (lag_ok ? 1 : 0) << "," << attempts << ","
                  << lag_ms << std::endl;
    }

    return ingest.errors == 0 && query.errors == 0 ? 0 : 1;
}
