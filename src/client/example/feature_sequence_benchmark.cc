#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "client/client.h"
#include "client/client_impl.h"
#include "common/cmd_manager.h"
#include "common/controller.h"
#include "common/status.h"
#include "common/sync_closure.h"
#include "extension/feature/interface.pb.h"
#include "extension/modules.pb.h"

namespace {

constexpr uint64_t kBaseTsMs = 1700000000000ULL;
constexpr uint64_t kTsStepMs = 1000ULL;
constexpr uint64_t kKeyTsStrideMs = 1000000000ULL;

struct Summary {
    std::string phase;
    int ops = 0;
    int threads = 0;
    int keys = 0;
    int rows_per_key = 0;
    int window_rows = 0;
    int filters = 0;
    int errors = 0;
    int64_t total_points = 0;
    int64_t total_ms = 0;
    double qps = 0;
    double points_per_sec = 0;
    int64_t avg_us = 0;
    int64_t p50_us = 0;
    int64_t p95_us = 0;
    int64_t p99_us = 0;
    int64_t min_us = 0;
    int64_t max_us = 0;
};

struct QueryCase {
    std::string name;
    int window_rows = 0;
    int count = 0;
    std::vector<std::string> filters;
    std::function<bool(const bcache2::feature2::FeaturePoint&)> validate;
};

int64_t Percentile(const std::vector<int64_t>& sorted, double pct) {
    if (sorted.empty()) {
        return 0;
    }
    const double pos = pct * static_cast<double>(sorted.size() - 1);
    return sorted[static_cast<size_t>(pos + 0.5)];
}

Summary Summarize(const std::string& phase, std::vector<int64_t> samples, int threads, int keys,
                  int rows_per_key, int window_rows, int filters, int errors,
                  int64_t total_points, int64_t total_ms) {
    std::sort(samples.begin(), samples.end());
    Summary s;
    s.phase = phase;
    s.ops = static_cast<int>(samples.size());
    s.threads = threads;
    s.keys = keys;
    s.rows_per_key = rows_per_key;
    s.window_rows = window_rows;
    s.filters = filters;
    s.errors = errors;
    s.total_points = total_points;
    s.total_ms = total_ms;
    s.qps = total_ms > 0 ? static_cast<double>(s.ops) * 1000.0 / total_ms : 0;
    s.points_per_sec =
        total_ms > 0 ? static_cast<double>(total_points) * 1000.0 / total_ms : 0;
    const int64_t sum = std::accumulate(samples.begin(), samples.end(), int64_t{0});
    s.avg_us = samples.empty() ? 0 : sum / static_cast<int64_t>(samples.size());
    s.p50_us = Percentile(samples, 0.50);
    s.p95_us = Percentile(samples, 0.95);
    s.p99_us = Percentile(samples, 0.99);
    s.min_us = samples.empty() ? 0 : samples.front();
    s.max_us = samples.empty() ? 0 : samples.back();
    return s;
}

void PrintHeader() {
    std::cout << "system,phase,ops,threads,keys,rows_per_key,window_rows,filters,errors,"
                 "total_points,total_ms,qps,points_per_sec,avg_us,p50_us,p95_us,p99_us,min_us,"
                 "max_us"
              << std::endl;
}

void PrintSummary(const Summary& s) {
    std::cout << "TemporalStore," << s.phase << "," << s.ops << "," << s.threads << ","
              << s.keys << "," << s.rows_per_key << "," << s.window_rows << ","
              << s.filters << "," << s.errors << "," << s.total_points << "," << s.total_ms
              << "," << static_cast<int64_t>(s.qps) << ","
              << static_cast<int64_t>(s.points_per_sec) << "," << s.avg_us << "," << s.p50_us
              << "," << s.p95_us << "," << s.p99_us << "," << s.min_us << "," << s.max_us
              << std::endl;
}

std::string KeyFor(const std::string& prefix, int key_idx) {
    return prefix + ":sequence:" + std::to_string(key_idx);
}

uint64_t BaseTsForKey(int key_idx) {
    return kBaseTsMs + static_cast<uint64_t>(key_idx) * kKeyTsStrideMs;
}

bcache2::feature2::FeaturePoint MakeFeaturePoint(int key_idx, int row_idx) {
    bcache2::feature2::FeaturePoint point;
    point.set_gid(1000000ULL + static_cast<uint64_t>(row_idx % 4096));
    point.set_action_type(static_cast<uint32_t>(row_idx % 5));
    point.set_duration(static_cast<uint32_t>((row_idx * 17 + key_idx) % 300));
    point.set_author_id(500000ULL + static_cast<uint64_t>(key_idx * 1000 + row_idx % 113));
    return point;
}

std::string SerializeFeaturePoint(int key_idx, int row_idx) {
    return MakeFeaturePoint(key_idx, row_idx).SerializeAsString();
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
    client_options.psm = "feature.sequence.benchmark";
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
    table_options.io_timeout_ms = 10000;
    table_options.connect_timeout_ms = 2000;
    bcache2::client::Table* raw_table = nullptr;
    status = (*client)->OpenTable(namespace_name, table_name, table_options, &raw_table);
    if (!status.ok()) {
        std::cerr << "OpenTable failed: " << status.ToString() << std::endl;
        return false;
    }
    table->reset(raw_table);
    *table_core = dynamic_cast<bcache2::client::TableCore*>(raw_table);
    if (*table_core == nullptr) {
        std::cerr << "OpenTable returned non-TableCore table" << std::endl;
        return false;
    }
    return true;
}

template <typename Request, typename Response>
bool ExecuteRaw(bcache2::client::TableCore* table, uint16_t module_id, uint16_t function_id,
                const std::string& partition_key, const Request& request, Response* response,
                const std::string& op) {
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
        std::cerr << op << " request serialization failed" << std::endl;
        return false;
    }
    raw_request.input.set_request_bytes(std::move(request_bytes));

    table->Execute(&ctrl, &raw_request, &raw_response, &sync, nullptr,
                   bcache2::client::RequestOptions());
    sync.Wait();
    if (!ctrl.status().ok()) {
        std::cerr << op << " failed: " << ctrl.status().ToString() << std::endl;
        return false;
    }
    if (!response->ParseFromString(raw_response.output->response_bytes())) {
        std::cerr << op << " response parse failed" << std::endl;
        return false;
    }
    return true;
}

template <typename Fn>
bool RunPhase(const std::string& phase, const std::string& metaserver, const std::string& idc,
              const std::string& namespace_name, const std::string& table_name, bool pin_primary,
              int ops, int threads, int keys, int rows_per_key, int window_rows, int filters,
              Fn fn, Summary* summary) {
    std::vector<int64_t> samples(static_cast<size_t>(ops), 0);
    std::atomic<int> errors{0};
    std::atomic<int64_t> total_points{0};
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threads));

    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            std::unique_ptr<bcache2::client::Client> client;
            std::unique_ptr<bcache2::client::Table> table;
            bcache2::client::TableCore* table_core = nullptr;
            if (!OpenTable(metaserver, idc, namespace_name, table_name, pin_primary, &client,
                           &table, &table_core)) {
                errors.fetch_add(1);
                ready.fetch_add(1);
                return;
            }
            ready.fetch_add(1);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (int i = t; i < ops; i += threads) {
                int64_t points = 0;
                auto begin = std::chrono::steady_clock::now();
                if (!fn(table_core, i, &points)) {
                    errors.fetch_add(1);
                }
                auto end = std::chrono::steady_clock::now();
                total_points.fetch_add(points);
                samples[static_cast<size_t>(i)] =
                    std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto wall_begin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto wall_end = std::chrono::steady_clock::now();
    const int64_t total_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(wall_end - wall_begin).count();

    *summary = Summarize(phase, std::move(samples), threads, keys, rows_per_key, window_rows,
                         filters, errors.load(), total_points.load(), total_ms);
    return summary->errors == 0;
}

bool AddRows(bcache2::client::TableCore* table, const std::string& prefix, int key_idx,
             int rows_per_key, int64_t* points_written) {
    const std::string key = KeyFor(prefix, key_idx);
    bcache2::feature2::AddRequest request;
    request.set_key(key);
    request.set_format("protobuf");
    request.set_policy(bcache2::feature2::UPSERT);
    request.mutable_point_list()->Reserve(rows_per_key);

    const uint64_t base_ts = BaseTsForKey(key_idx);
    for (int row = 0; row < rows_per_key; ++row) {
        auto* point = request.add_point_list();
        point->set_ts(base_ts + static_cast<uint64_t>(row) * kTsStepMs);
        point->set_value(SerializeFeaturePoint(key_idx, row));
    }

    bcache2::feature2::AddResponse response;
    if (!ExecuteRaw(table, bcache2::Module::FEATURE, bcache2::feature2::ADD, key, request,
                    &response, "FEATURE Add")) {
        return false;
    }
    *points_written = rows_per_key;
    return true;
}

template <typename Fn>
bool RetryUntil(int max_wait_ms, int sleep_ms, Fn fn) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(std::max(0, max_wait_ms));
    do {
        if (fn()) {
            return true;
        }
        if (max_wait_ms <= 0) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    } while (std::chrono::steady_clock::now() < deadline);
    return fn();
}

bool QueryRows(bcache2::client::TableCore* table, const std::string& prefix, int query_idx,
               int keys, int rows_per_key, const QueryCase& query_case,
               int retry_ms, int64_t* points_returned) {
    const int key_idx = query_idx % keys;
    const int usable_window = std::max(1, std::min(query_case.window_rows, rows_per_key));
    const int max_start = std::max(0, rows_per_key - usable_window);
    const int start_row = max_start == 0 ? 0 : (query_idx * 37 + key_idx * 13) % (max_start + 1);
    const int end_row = std::min(rows_per_key, start_row + usable_window);

    const std::string key = KeyFor(prefix, key_idx);
    bcache2::feature2::QueryRequest request;
    request.set_key(key);
    request.set_start_ts(BaseTsForKey(key_idx) + static_cast<uint64_t>(start_row) * kTsStepMs);
    request.set_end_ts(BaseTsForKey(key_idx) + static_cast<uint64_t>(end_row) * kTsStepMs);
    request.set_count(query_case.count > 0 ? query_case.count : usable_window);
    request.set_format("protobuf");
    for (const auto& filter : query_case.filters) {
        request.add_filters(filter);
    }

    bcache2::feature2::QueryResponse response;
    const bool queried = RetryUntil(retry_ms, 20, [&]() {
        response.Clear();
        if (!ExecuteRaw(table, bcache2::Module::FEATURE, bcache2::feature2::QUERY, key, request,
                        &response, "FEATURE Query")) {
            return false;
        }
        return !query_case.filters.empty() || response.point_list_size() > 0;
    });
    if (!queried) {
        return false;
    }

    for (const auto& point : response.point_list()) {
        if (point.ts() < request.start_ts() || point.ts() >= request.end_ts()) {
            std::cerr << "FEATURE Query returned point outside requested window" << std::endl;
            return false;
        }
        bcache2::feature2::FeaturePoint decoded;
        if (!decoded.ParseFromString(point.value())) {
            std::cerr << "FEATURE Query returned undecodable protobuf point" << std::endl;
            return false;
        }
        if (query_case.validate && !query_case.validate(decoded)) {
            std::cerr << "FEATURE Query returned point that violates filter" << std::endl;
            return false;
        }
    }

    if (query_case.filters.empty() && response.point_list_size() == 0) {
        std::cerr << "FEATURE Query returned no points for unfiltered window" << std::endl;
        return false;
    }

    *points_returned = response.point_list_size();
    return true;
}

std::vector<QueryCase> BuildQueryCases(int rows_per_key) {
    const int small_window = std::min(100, rows_per_key);
    const int medium_window = std::min(1000, rows_per_key);
    const int full_window = rows_per_key;

    return {
        QueryCase{"window_100_no_filter", small_window, small_window, {},
                  [](const bcache2::feature2::FeaturePoint&) { return true; }},
        QueryCase{"window_1000_action_eq_3", medium_window, medium_window, {"action_type = 3"},
                  [](const bcache2::feature2::FeaturePoint& point) {
                      return point.action_type() == 3;
                  }},
        QueryCase{
            "window_1000_complex_filters",
            medium_window,
            medium_window,
            {"action_type = 3", "duration > 120", "gid < 1002048"},
            [](const bcache2::feature2::FeaturePoint& point) {
                return point.action_type() == 3 && point.duration() > 120 &&
                       point.gid() < 1002048ULL;
            }},
        QueryCase{
            "full_window_complex_filters",
            full_window,
            full_window,
            {"action_type != 1", "duration < 250", "author_id > 500050"},
            [](const bcache2::feature2::FeaturePoint& point) {
                return point.action_type() != 1 && point.duration() < 250 &&
                       point.author_id() > 500050ULL;
            }},
    };
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5 || argc > 11) {
        std::cout << "usage: " << argv[0]
                  << " <metaserver_host:port> <idc> <namespace> <table> [keys=8]"
                     " [rows_per_key=5000] [query_ops=2000] [threads=16]"
                     " [pin_primary_reads=1] [replica_wait_ms=5000]"
                  << std::endl;
        return 2;
    }

    const std::string metaserver = argv[1];
    const std::string idc = argv[2];
    const std::string namespace_name = argv[3];
    const std::string table_name = argv[4];
    const int keys = argc > 5 ? std::atoi(argv[5]) : 8;
    const int rows_per_key = argc > 6 ? std::atoi(argv[6]) : 5000;
    const int query_ops = argc > 7 ? std::atoi(argv[7]) : 2000;
    const int threads = argc > 8 ? std::atoi(argv[8]) : 16;
    const bool pin_primary_reads = argc > 9 ? std::atoi(argv[9]) != 0 : true;
    const int replica_wait_ms = argc > 10 ? std::atoi(argv[10]) : 5000;

    if (keys <= 0 || rows_per_key <= 0 || query_ops <= 0 || threads <= 0) {
        std::cerr << "keys, rows_per_key, query_ops, and threads must be positive" << std::endl;
        return 2;
    }
    if (rows_per_key > 5000) {
        std::cerr << "warning: default feature_max_size is 5000; larger rows_per_key may be "
                     "truncated unless the server flag is raised"
                  << std::endl;
    }

    const std::string prefix =
        "seq_" + std::to_string(std::time(nullptr)) + "_" +
        std::to_string(static_cast<unsigned long long>(std::rand()));

    std::cout << "prefix=" << prefix << std::endl;
    std::cout << "keys=" << keys << std::endl;
    std::cout << "rows_per_key=" << rows_per_key << std::endl;
    std::cout << "query_ops_per_case=" << query_ops << std::endl;
    PrintHeader();

    Summary ingest_summary;
    if (!RunPhase("ingest_add_sequence_rows", metaserver, idc, namespace_name, table_name, true,
                  keys, std::min(keys, threads), keys, rows_per_key, 0, 0,
                  [&](bcache2::client::TableCore* table, int key_idx, int64_t* points) {
                      return AddRows(table, prefix, key_idx, rows_per_key, points);
                  },
                  &ingest_summary)) {
        PrintSummary(ingest_summary);
        return 1;
    }
    PrintSummary(ingest_summary);

    if (!pin_primary_reads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(replica_wait_ms));
    }

    const auto query_cases = BuildQueryCases(rows_per_key);
    for (const auto& query_case : query_cases) {
        Summary query_summary;
        if (!RunPhase(query_case.name, metaserver, idc, namespace_name, table_name,
                      pin_primary_reads, query_ops, threads, keys, rows_per_key,
                      query_case.window_rows, static_cast<int>(query_case.filters.size()),
                      [&](bcache2::client::TableCore* table, int query_idx, int64_t* points) {
                          const int retry_ms = pin_primary_reads ? 0 : std::max(replica_wait_ms, 30000);
                          return QueryRows(table, prefix, query_idx, keys, rows_per_key,
                                           query_case, retry_ms, points);
                      },
                      &query_summary)) {
            PrintSummary(query_summary);
            return 1;
        }
        PrintSummary(query_summary);
    }

    return 0;
}
