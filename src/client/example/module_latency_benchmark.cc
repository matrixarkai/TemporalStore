#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "client/client.h"
#include "client/client_impl.h"
#include "common/cmd_manager.h"
#include "common/controller.h"
#include "common/status.h"
#include "common/sync_closure.h"
#include "extension/feature/interface.pb.h"
#include "extension/ips/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/risk/interface.pb.h"
#include "extension/set/interface.pb.h"

namespace {

struct LatencyStats {
    std::string module;
    std::string operation;
    std::vector<int64_t> samples_us;
};

bool CheckStatus(const bcache2::Status& status, const std::string& op) {
    if (!status.ok()) {
        std::cerr << "FAIL " << op << ": " << status.ToString() << std::endl;
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
        std::cerr << "FAIL " << op << ": request serialization failed" << std::endl;
        return false;
    }
    raw_request.input.set_request_bytes(std::move(request_bytes));

    table->Execute(&ctrl, &raw_request, &raw_response, &sync, nullptr,
                   bcache2::client::RequestOptions());
    sync.Wait();
    if (!CheckStatus(ctrl.status(), op)) {
        return false;
    }
    if (!response->ParseFromString(raw_response.output->response_bytes())) {
        std::cerr << "FAIL " << op << ": response parse failed" << std::endl;
        return false;
    }
    return true;
}

template <typename Fn>
bool Measure(const std::string& module, const std::string& operation, int ops, Fn fn,
             std::vector<LatencyStats>* results) {
    LatencyStats stats;
    stats.module = module;
    stats.operation = operation;
    stats.samples_us.reserve(ops);

    for (int i = 0; i < ops; ++i) {
        auto begin = std::chrono::steady_clock::now();
        if (!fn(i)) {
            return false;
        }
        auto end = std::chrono::steady_clock::now();
        stats.samples_us.push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count());
    }
    results->push_back(std::move(stats));
    return true;
}

int64_t Percentile(const std::vector<int64_t>& sorted, double pct) {
    if (sorted.empty()) {
        return 0;
    }
    const double pos = pct * static_cast<double>(sorted.size() - 1);
    return sorted[static_cast<size_t>(pos + 0.5)];
}

void PrintResults(const std::vector<LatencyStats>& results) {
    std::cout << "module,operation,count,avg_us,p50_us,p95_us,p99_us,min_us,max_us" << std::endl;
    for (const auto& stats : results) {
        std::vector<int64_t> sorted = stats.samples_us;
        std::sort(sorted.begin(), sorted.end());
        const int64_t sum = std::accumulate(sorted.begin(), sorted.end(), int64_t{0});
        const int64_t avg = sorted.empty() ? 0 : sum / static_cast<int64_t>(sorted.size());
        std::cout << stats.module << "," << stats.operation << "," << sorted.size() << ","
                  << avg << "," << Percentile(sorted, 0.50) << ","
                  << Percentile(sorted, 0.95) << "," << Percentile(sorted, 0.99) << ","
                  << (sorted.empty() ? 0 : sorted.front()) << ","
                  << (sorted.empty() ? 0 : sorted.back()) << std::endl;
    }
}

std::string Prefix() {
    std::ostringstream os;
    os << "latency_" << static_cast<unsigned long long>(std::time(nullptr)) << "_"
       << static_cast<unsigned long long>(std::rand());
    return os.str();
}

bool BenchmarkStringAndCommon(bcache2::client::Table* table, const std::string& prefix, int ops,
                              std::vector<LatencyStats>* results) {
    const std::string payload = R"({"country":"US","score":0.91,"segment":"active"})";

    if (!Measure("STRING", "ingest_set", ops,
                 [&](int i) {
                     return CheckStatus(table->Set(prefix + ":string:" + std::to_string(i),
                                                   payload),
                                        "STRING Set");
                 },
                 results)) {
        return false;
    }

    if (!Measure("STRING", "query_get", ops,
                 [&](int i) {
                     std::string value;
                     if (!CheckStatus(table->Get(prefix + ":string:" + std::to_string(i), &value),
                                      "STRING Get")) {
                         return false;
                     }
                     return value == payload;
                 },
                 results)) {
        return false;
    }

    if (!Measure("COMMON", "ingest_expire", ops,
                 [&](int i) {
                     return CheckStatus(table->Expire(prefix + ":string:" + std::to_string(i),
                                                      60000),
                                        "COMMON Expire");
                 },
                 results)) {
        return false;
    }

    return Measure("COMMON", "query_ttl", ops,
                   [&](int i) {
                       uint64_t ttl_ms = 0;
                       if (!CheckStatus(table->Ttl(prefix + ":string:" + std::to_string(i),
                                                   &ttl_ms),
                                        "COMMON Ttl")) {
                           return false;
                       }
                       return ttl_ms > 0;
                   },
                   results);
}

bool BenchmarkHash(bcache2::client::Table* table, const std::string& prefix, int ops,
                   std::vector<LatencyStats>* results) {
    if (!Measure("HASH", "ingest_hset", ops,
                 [&](int i) {
                     return CheckStatus(table->HSet(prefix + ":hash:" + std::to_string(i),
                                                    "ctr_7d", "0.042"),
                                        "HASH HSet");
                 },
                 results)) {
        return false;
    }

    return Measure("HASH", "query_hget", ops,
                   [&](int i) {
                       std::string value;
                       if (!CheckStatus(table->HGet(prefix + ":hash:" + std::to_string(i),
                                                    "ctr_7d", &value),
                                        "HASH HGet")) {
                           return false;
                       }
                       return value == "0.042";
                   },
                   results);
}

bool BenchmarkSet(bcache2::client::TableCore* table, const std::string& prefix, int ops,
                  std::vector<LatencyStats>* results) {
    if (!Measure("SET", "ingest_sadd", ops,
                 [&](int i) {
                     const std::string key = prefix + ":set:" + std::to_string(i);
                     bcache2::set::SAddRequest request;
                     request.set_key(key);
                     request.set_member("campaign_" + std::to_string(i));
                     bcache2::set::SAddResponse response;
                     return ExecuteRaw(table, bcache2::Module::SET, bcache2::set::SADD, key,
                                       request, &response, "SET SAdd");
                 },
                 results)) {
        return false;
    }

    return Measure("SET", "query_smembers", ops,
                   [&](int i) {
                       const std::string key = prefix + ":set:" + std::to_string(i);
                       bcache2::set::SMembersRequest request;
                       request.set_key(key);
                       bcache2::set::SMembersResponse response;
                       if (!ExecuteRaw(table, bcache2::Module::SET, bcache2::set::SMEMBERS, key,
                                       request, &response, "SET SMembers")) {
                           return false;
                       }
                       return response.members_size() == 1;
                   },
                   results);
}

bool BenchmarkFeature(bcache2::client::TableCore* table, const std::string& prefix, int ops,
                      std::vector<LatencyStats>* results) {
    const uint64_t base_ts = 1700000000000ULL;
    if (!Measure("FEATURE", "ingest_add_point", ops,
                 [&](int i) {
                     const std::string key = prefix + ":feature:" + std::to_string(i);
                     bcache2::feature2::AddRequest request;
                     request.set_key(key);
                     request.set_format("protobuf");
                     auto* point = request.add_point_list();
                     point->set_ts(base_ts + static_cast<uint64_t>(i));
                     point->set_value("item_id=" + std::to_string(900 + i) + ",action=click");
                     bcache2::feature2::AddResponse response;
                     return ExecuteRaw(table, bcache2::Module::FEATURE, bcache2::feature2::ADD,
                                       key, request, &response, "FEATURE Add");
                 },
                 results)) {
        return false;
    }

    return Measure("FEATURE", "query_window_one_point", ops,
                   [&](int i) {
                       const std::string key = prefix + ":feature:" + std::to_string(i);
                       bcache2::feature2::QueryRequest request;
                       request.set_key(key);
                       request.set_start_ts(base_ts + static_cast<uint64_t>(i));
                       request.set_end_ts(base_ts + static_cast<uint64_t>(i) + 1);
                       request.set_count(1);
                       request.set_format("protobuf");
                       bcache2::feature2::QueryResponse response;
                       if (!ExecuteRaw(table, bcache2::Module::FEATURE,
                                       bcache2::feature2::QUERY, key, request, &response,
                                       "FEATURE Query")) {
                           return false;
                       }
                       return response.point_list_size() == 1;
                   },
                   results);
}

bool BenchmarkIps(bcache2::client::TableCore* table, const std::string& prefix, int ops,
                  std::vector<LatencyStats>* results) {
    const int64_t base_uid = 9100000 + static_cast<int64_t>(std::time(nullptr) % 1000000);
    const int64_t base_ts = static_cast<int64_t>(std::time(nullptr)) * 1000LL * 1000LL;

    if (!Measure("IPS", "ingest_add_instance", ops,
                 [&](int i) {
                     const int64_t uid = base_uid + i;
                     bcache2::ips::AddRequest request;
                     request.set_table("table_compress");
                     request.set_enable_server_aggregator(true);
                     auto* instance = request.add_instance_list();
                     instance->set_uid(uid);
                     instance->set_ts(base_ts + i);
                     instance->set_action_type(0);
                     instance->set_table(0);
                     auto* stat = instance->add_feature_stat32_list();
                     stat->set_slot(23);
                     stat->set_has_slot(true);
                     stat->set_type(0);
                     stat->set_id(456);
                     stat->mutable_int_pair()->set_v1(12);
                     stat->mutable_int_pair()->set_v2(34);
                     bcache2::ips::AddResponse response;
                     if (!ExecuteRaw(table, bcache2::Module::IPS, bcache2::ips::ADD,
                                     std::to_string(uid), request, &response, "IPS Add")) {
                         return false;
                     }
                     return response.err_code() == bcache2::ips::SUCCESS;
                 },
                 results)) {
        return false;
    }

    return Measure("IPS", "query_last_instance", ops,
                   [&](int i) {
                       const int64_t uid = base_uid + i;
                       bcache2::ips::BatchQueryRequest request;
                       auto* query = request.add_reqs();
                       query->set_uid(uid);
                       query->set_decoupled(false);
                       query->set_table("table_compress");
                       query->mutable_data_range()->set_type(bcache2::ips::LAST_INSTANCES);
                       query->mutable_data_range()->set_range_val(10);
                       query->mutable_filter()->set_table(0);
                       query->mutable_filter()->set_action_type(0);
                       query->mutable_filter()->set_slot(23);
                       query->mutable_filter()->set_top_k(20);
                       query->mutable_filter()->set_optor(bcache2::ips::SORT_BY_TS);
                       bcache2::ips::BatchQueryResponse response;
                       if (!ExecuteRaw(table, bcache2::Module::IPS, bcache2::ips::BATCH_QUERY,
                                       std::to_string(uid), request, &response, "IPS Query")) {
                           return false;
                       }
                       return response.err_code() == bcache2::ips::SUCCESS &&
                              response.rsps_size() == 1 &&
                              response.rsps(0).feature_stat32_list_size() >= 1;
                   },
                   results);
}

bool BenchmarkRisk(bcache2::client::TableCore* table, const std::string& prefix, int ops,
                   std::vector<LatencyStats>* results) {
    const uint64_t now = static_cast<uint64_t>(std::time(nullptr));

    if (!Measure("RISK", "ingest_hset_count", ops,
                 [&](int i) {
                     const std::string key = prefix + ":risk:" + std::to_string(i);
                     bcache2::risk::HsetRequest request;
                     request.set_key(key);
                     request.set_value("1");
                     request.set_ttl(24 * 3600);
                     request.set_htype(bcache2::risk::COUNT);
                     request.set_precision(bcache2::risk::OneMinute);
                     request.set_occur_time(now);
                     request.set_uuid(prefix + ":risk_uuid:" + std::to_string(i));
                     bcache2::risk::HsetResponse response;
                     if (!ExecuteRaw(table, bcache2::Module::RISK, bcache2::risk::HSET, key,
                                     request, &response, "RISK Hset")) {
                         return false;
                     }
                     return response.err_code() == 0;
                 },
                 results)) {
        return false;
    }

    return Measure("RISK", "query_hquery_1h_count", ops,
                   [&](int i) {
                       const std::string key = prefix + ":risk:" + std::to_string(i);
                       bcache2::risk::HqueryRequest request;
                       request.set_key(key);
                       request.set_precision(bcache2::risk::OneMinute);
                       request.set_htype(bcache2::risk::COUNT);
                       auto* window = request.add_windows();
                       window->set_start(-1);
                       window->set_end(0);
                       window->set_unit(bcache2::risk::Hour);
                       bcache2::risk::HqueryResponse response;
                       if (!ExecuteRaw(table, bcache2::Module::RISK, bcache2::risk::HQUERY, key,
                                       request, &response, "RISK Hquery")) {
                           return false;
                       }
                       return response.err_code() == 0 && response.result_list_size() == 1 &&
                              response.result_list(0).result() == 1;
                   },
                   results);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5 || argc > 6) {
        std::cout << "usage: " << argv[0]
                  << " <metaserver_host:port> <idc> <namespace> <table> [ops_per_case]"
                  << std::endl;
        return 2;
    }

    const int ops = argc == 6 ? std::max(1, std::atoi(argv[5])) : 300;
    std::srand(static_cast<unsigned int>(std::time(nullptr)));

    bcache2::client::ClientOptions options;
    options.log_level = bcache2::client::LogLevel::kWarning;
    options.af = bcache2::client::AddressFamily::kIp4;
    options.master_addr = argv[1];
    options.idc = argv[2];
    options.host = "127.0.0.1";
    options.psm = "module.latency.benchmark";
    options.partition_pick_opts.policy = bcache2::client::PartitionPickOptions::Policy::kPrimary;

    bcache2::client::Client* raw_client = nullptr;
    bcache2::Status status = bcache2::client::Client::Create(options, &raw_client);
    if (!CheckStatus(status, "Client::Create")) {
        return 1;
    }
    std::unique_ptr<bcache2::client::Client> client(raw_client);

    bcache2::client::Table* raw_table = nullptr;
    status = client->OpenTable(argv[3], argv[4], bcache2::client::TableOptions(), &raw_table);
    if (!CheckStatus(status, "OpenTable")) {
        return 1;
    }
    std::unique_ptr<bcache2::client::Table> table(raw_table);
    auto* table_core = dynamic_cast<bcache2::client::TableCore*>(raw_table);
    if (table_core == nullptr) {
        std::cerr << "FAIL: opened table is not TableCore" << std::endl;
        return 1;
    }

    const std::string prefix = Prefix();
    std::vector<LatencyStats> results;

    bool ok = true;
    ok = ok && BenchmarkStringAndCommon(table.get(), prefix, ops, &results);
    ok = ok && BenchmarkHash(table.get(), prefix, ops, &results);
    ok = ok && BenchmarkSet(table_core, prefix, ops, &results);
    ok = ok && BenchmarkFeature(table_core, prefix, ops, &results);
    ok = ok && BenchmarkIps(table_core, prefix, ops, &results);
    ok = ok && BenchmarkRisk(table_core, prefix, ops, &results);

    bcache2::Status close_status = client->CloseTable(table.get());
    if (!CheckStatus(close_status, "CloseTable")) {
        ok = false;
    }

    if (!ok) {
        return 1;
    }

    std::cout << "prefix=" << prefix << std::endl;
    std::cout << "ops_per_case=" << ops << std::endl;
    PrintResults(results);
    return 0;
}
