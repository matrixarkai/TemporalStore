#include <chrono>
#include <ctime>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>

#include "client/client.h"
#include "client/client_impl.h"
#include "common/cmd_manager.h"
#include "common/controller.h"
#include "common/status.h"
#include "common/sync_closure.h"
#include "extension/common/interface.pb.h"
#include "extension/feature/interface.pb.h"
#include "extension/hash/interface.pb.h"
#include "extension/ips/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/risk/interface.pb.h"
#include "extension/set/interface.pb.h"
#include "extension/string/interface.pb.h"
#include "extension/temporal_aggregate/interface.pb.h"

namespace {

bool Expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        return false;
    }
    return true;
}

bool ExpectStatus(const bcache2::Status& status, const std::string& op) {
    if (!status.ok()) {
        std::cerr << "FAIL: " << op << ": " << status.ToString() << std::endl;
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
        std::cerr << "FAIL: " << op << ": request serialization failed" << std::endl;
        return false;
    }
    raw_request.input.set_request_bytes(std::move(request_bytes));

    table->Execute(&ctrl, &raw_request, &raw_response, &sync, nullptr,
                   bcache2::client::RequestOptions());
    sync.Wait();
    if (!ExpectStatus(ctrl.status(), op)) {
        return false;
    }
    if (!response->ParseFromString(raw_response.output->response_bytes())) {
        std::cerr << "FAIL: " << op << ": response parse failed" << std::endl;
        return false;
    }
    return true;
}

std::string UniquePrefix() {
    const auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::random_device rd;
    std::ostringstream os;
    os << "module_smoke_" << static_cast<unsigned long long>(std::time(nullptr)) << "_"
       << static_cast<unsigned long long>(now) << "_" << static_cast<unsigned long long>(rd());
    return os.str();
}

bool TestStringAndCommon(bcache2::client::Table* table, const std::string& prefix) {
    const std::string key = prefix + ":string:profile";
    const std::string value = R"({"uid":1001,"country":"US","score":0.91})";

    if (!ExpectStatus(table->Set(key, value), "STRING Set")) {
        return false;
    }

    std::string got;
    if (!ExpectStatus(table->Get(key, &got), "STRING Get")) {
        return false;
    }
    if (!Expect(got == value, "STRING Get returned unexpected value")) {
        return false;
    }

    if (!ExpectStatus(table->Expire(key, 60000), "COMMON Expire")) {
        return false;
    }

    uint64_t ttl_ms = 0;
    if (!ExpectStatus(table->Ttl(key, &ttl_ms), "COMMON Ttl")) {
        return false;
    }
    if (!Expect(ttl_ms > 0, "COMMON Ttl should be positive after Expire")) {
        return false;
    }

    if (!ExpectStatus(table->Del(key), "COMMON DelObject")) {
        return false;
    }

    std::cout << "PASS STRING: set/get JSON profile" << std::endl;
    std::cout << "PASS COMMON: expire/ttl/del on same object, ttl_ms=" << ttl_ms << std::endl;
    return true;
}

bool TestHash(bcache2::client::Table* table, const std::string& prefix) {
    const std::string key = prefix + ":hash:user_features";
    if (!ExpectStatus(table->HSet(key, "ctr_7d", "0.042"), "HASH HSet ctr_7d")) {
        return false;
    }
    if (!ExpectStatus(table->HSet(key, "cart_count_1h", "3"), "HASH HSet cart_count_1h")) {
        return false;
    }

    std::string ctr;
    if (!ExpectStatus(table->HGet(key, "ctr_7d", &ctr), "HASH HGet ctr_7d")) {
        return false;
    }
    if (!Expect(ctr == "0.042", "HASH HGet returned unexpected value")) {
        return false;
    }

    std::cout << "PASS HASH: hset/hget feature fields" << std::endl;
    return true;
}

bool TestSet(bcache2::client::TableCore* table, const std::string& prefix) {
    const std::string key = prefix + ":set:campaigns";

    const char* members[] = {"campaign_100", "campaign_200"};
    for (const char* member : members) {
        bcache2::set::SAddRequest request;
        request.set_key(key);
        request.set_member(member);
        bcache2::set::SAddResponse response;
        if (!ExecuteRaw(table, bcache2::Module::SET, bcache2::set::SADD, key, request, &response,
                        std::string("SET SAdd ") + member)) {
            return false;
        }
    }

    bcache2::set::SMembersRequest request;
    request.set_key(key);
    bcache2::set::SMembersResponse response;
    if (!ExecuteRaw(table, bcache2::Module::SET, bcache2::set::SMEMBERS, key, request, &response,
                    "SET SMembers")) {
        return false;
    }
    if (!Expect(response.members_size() == 2, "SET SMembers should return 2 members")) {
        return false;
    }

    std::cout << "PASS SET: sadd/smembers campaign set, members=" << response.members_size()
              << std::endl;
    return true;
}

bool TestFeature(bcache2::client::TableCore* table, const std::string& prefix) {
    const std::string key = prefix + ":feature:click_sequence";
    const uint64_t start_ts = 1700000000000ULL;

    bcache2::feature2::AddRequest add;
    add.set_key(key);
    add.set_format("protobuf");
    for (int i = 0; i < 5; ++i) {
        auto* point = add.add_point_list();
        point->set_ts(start_ts + i * 1000);
        point->set_value("item_id=" + std::to_string(900 + i) + ",action=click");
    }
    bcache2::feature2::AddResponse add_response;
    if (!ExecuteRaw(table, bcache2::Module::FEATURE, bcache2::feature2::ADD, key, add,
                    &add_response, "FEATURE Add sequence points")) {
        return false;
    }

    bcache2::feature2::QueryRequest query;
    query.set_key(key);
    query.set_start_ts(start_ts);
    query.set_end_ts(start_ts + 5000);
    query.set_count(10);
    query.set_format("protobuf");
    bcache2::feature2::QueryResponse query_response;
    if (!ExecuteRaw(table, bcache2::Module::FEATURE, bcache2::feature2::QUERY, key, query,
                    &query_response, "FEATURE Query sequence window")) {
        return false;
    }
    if (!Expect(query_response.point_list_size() == 5,
                "FEATURE Query should return 5 sequence points")) {
        return false;
    }

    std::cout << "PASS FEATURE: add/query time sequence, points="
              << query_response.point_list_size() << std::endl;
    return true;
}

bool TestIps(bcache2::client::TableCore* table, const std::string& prefix) {
    const int64_t uid = 8800001 + static_cast<int64_t>(std::hash<std::string>{}(prefix) % 1000000);
    const int64_t ts = static_cast<int64_t>(std::time(nullptr)) * 1000LL * 1000LL;
    const std::string partition_key = prefix + ":ips:" + std::to_string(uid);

    bcache2::ips::AddRequest add;
    add.set_table("table_compress");
    add.set_enable_server_aggregator(true);
    auto* instance = add.add_instance_list();
    instance->set_uid(uid);
    instance->set_ts(ts);
    instance->set_action_type(0);
    instance->set_table(0);
    auto* stat = instance->add_feature_stat32_list();
    stat->set_slot(23);
    stat->set_has_slot(true);
    stat->set_type(0);
    stat->set_id(456);
    stat->mutable_int_pair()->set_v1(12);
    stat->mutable_int_pair()->set_v2(34);

    bcache2::ips::AddResponse add_response;
    if (!ExecuteRaw(table, bcache2::Module::IPS, bcache2::ips::ADD, std::to_string(uid), add,
                    &add_response, "IPS Add instance")) {
        return false;
    }
    if (!Expect(add_response.err_code() == bcache2::ips::SUCCESS,
                "IPS AddResponse should be SUCCESS")) {
        return false;
    }

    bcache2::ips::BatchQueryRequest query;
    auto* req = query.add_reqs();
    req->set_uid(uid);
    req->set_decoupled(false);
    req->set_table("table_compress");
    req->mutable_data_range()->set_type(bcache2::ips::LAST_INSTANCES);
    req->mutable_data_range()->set_range_val(10);
    req->mutable_filter()->set_table(0);
    req->mutable_filter()->set_action_type(0);
    req->mutable_filter()->set_slot(23);
    req->mutable_filter()->set_top_k(20);
    req->mutable_filter()->set_optor(bcache2::ips::SORT_BY_TS);

    bcache2::ips::BatchQueryResponse query_response;
    if (!ExecuteRaw(table, bcache2::Module::IPS, bcache2::ips::BATCH_QUERY, std::to_string(uid),
                    query, &query_response, "IPS BatchQuery")) {
        return false;
    }
    if (!Expect(query_response.err_code() == bcache2::ips::SUCCESS,
                "IPS BatchQueryResponse should be SUCCESS, got err_code=" +
                    std::to_string(query_response.err_code()) + ", desc=" +
                    query_response.error_desc())) {
        return false;
    }
    if (!Expect(query_response.rsps_size() == 1, "IPS BatchQuery should return one response")) {
        return false;
    }
    if (!Expect(query_response.rsps(0).feature_stat32_list_size() >= 1,
                "IPS BatchQuery should return at least one feature stat")) {
        return false;
    }
    const auto& got = query_response.rsps(0).feature_stat32_list(0).int_pair();
    if (!Expect(got.v1() == 12 && got.v2() == 34, "IPS returned unexpected int_pair")) {
        return false;
    }

    std::cout << "PASS IPS: add/query instance feature, v1=" << got.v1() << ", v2=" << got.v2()
              << ", partition_key=" << partition_key << std::endl;
    return true;
}

bool TestRisk(bcache2::client::TableCore* table, const std::string& prefix) {
    const std::string key = prefix + ":risk:purchase_count";

    for (int i = 0; i < 3; ++i) {
        bcache2::risk::HsetRequest hset;
        hset.set_key(key);
        hset.set_value("1");
        hset.set_ttl(24 * 3600);
        hset.set_htype(bcache2::risk::COUNT);
        hset.set_precision(bcache2::risk::OneMinute);
        hset.set_occur_time(static_cast<uint64_t>(std::time(nullptr)));
        hset.set_uuid(prefix + ":risk_uuid_" + std::to_string(i));
        bcache2::risk::HsetResponse hset_response;
        if (!ExecuteRaw(table, bcache2::Module::RISK, bcache2::risk::HSET, key, hset,
                        &hset_response, "RISK Hset count")) {
            return false;
        }
        if (!Expect(hset_response.err_code() == 0, "RISK HsetResponse err_code should be 0")) {
            return false;
        }
    }

    bcache2::risk::HqueryRequest query;
    query.set_key(key);
    query.set_precision(bcache2::risk::OneMinute);
    query.set_htype(bcache2::risk::COUNT);
    auto* window = query.add_windows();
    window->set_start(-1);
    window->set_end(0);
    window->set_unit(bcache2::risk::Hour);

    bcache2::risk::HqueryResponse response;
    if (!ExecuteRaw(table, bcache2::Module::RISK, bcache2::risk::HQUERY, key, query, &response,
                    "RISK Hquery 1h count")) {
        return false;
    }
    if (!Expect(response.err_code() == 0, "RISK HqueryResponse err_code should be 0")) {
        return false;
    }
    if (!Expect(response.result_list_size() == 1, "RISK Hquery should return one result")) {
        return false;
    }
    if (!Expect(response.result_list(0).result() == 3, "RISK 1h count should be 3")) {
        return false;
    }

    std::cout << "PASS RISK: hset/hquery count window, count="
              << response.result_list(0).result() << std::endl;
    return true;
}

bool TestTemporalAggregate(bcache2::client::TableCore* table, const std::string& prefix) {
    const std::string key = prefix + ":temporal_aggregate:device_1";
    const uint64_t base_ts_ms = 1700000000000ULL;
    constexpr uint64_t kMinuteMs = 60 * 1000ULL;

    for (int i = 0; i < 3; ++i) {
        bcache2::temporal_aggregate::IncrRequest incr;
        incr.set_key(key);
        incr.set_metric("failed_login_count");
        auto* country = incr.add_dimensions();
        country->set_name("country");
        country->set_value("US");
        auto* result = incr.add_dimensions();
        result->set_name("result");
        result->set_value("failed");
        incr.set_timestamp_ms(base_ts_ms + static_cast<uint64_t>(i) * kMinuteMs);
        incr.set_bucket_width_ms(kMinuteMs);
        incr.set_value(1);
        incr.set_ttl_ms(24 * 3600 * 1000ULL);
        incr.set_op(bcache2::temporal_aggregate::COUNT);

        bcache2::temporal_aggregate::IncrResponse response;
        if (!ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                        bcache2::temporal_aggregate::INCR, key, incr, &response,
                        "TEMPORAL_AGGREGATE Incr failed_login_count")) {
            return false;
        }
    }

    {
        bcache2::temporal_aggregate::IncrRequest incr;
        incr.set_key(key);
        incr.set_metric("failed_login_count");
        auto* country = incr.add_dimensions();
        country->set_name("country");
        country->set_value("CA");
        auto* result = incr.add_dimensions();
        result->set_name("result");
        result->set_value("failed");
        incr.set_timestamp_ms(base_ts_ms + kMinuteMs);
        incr.set_bucket_width_ms(kMinuteMs);
        incr.set_value(1);
        incr.set_op(bcache2::temporal_aggregate::COUNT);

        bcache2::temporal_aggregate::IncrResponse response;
        if (!ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                        bcache2::temporal_aggregate::INCR, key, incr, &response,
                        "TEMPORAL_AGGREGATE Incr different dimension")) {
            return false;
        }
    }

    bcache2::temporal_aggregate::QueryRequest query;
    query.set_key(key);
    query.set_metric("failed_login_count");
    auto* result = query.add_dimensions();
    result->set_name("result");
    result->set_value("failed");
    auto* country = query.add_dimensions();
    country->set_name("country");
    country->set_value("US");
    query.set_start_timestamp_ms(base_ts_ms);
    query.set_end_timestamp_ms(base_ts_ms + 5 * kMinuteMs);
    query.set_bucket_width_ms(kMinuteMs);
    query.set_op(bcache2::temporal_aggregate::COUNT);

    bcache2::temporal_aggregate::QueryResponse response;
    if (!ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                    bcache2::temporal_aggregate::QUERY, key, query, &response,
                    "TEMPORAL_AGGREGATE Query failed_login_count")) {
        return false;
    }
    if (!Expect(response.has_value(), "TEMPORAL_AGGREGATE Query should have a value")) {
        return false;
    }
    if (!Expect(response.value() == 3, "TEMPORAL_AGGREGATE 5m count should be 3")) {
        return false;
    }
    if (!Expect(response.buckets_size() == 3, "TEMPORAL_AGGREGATE should return 3 buckets")) {
        return false;
    }

    bcache2::temporal_aggregate::QueryRequest ca_query = query;
    ca_query.mutable_dimensions(1)->set_value("CA");
    bcache2::temporal_aggregate::QueryResponse ca_response;
    if (!ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                    bcache2::temporal_aggregate::QUERY, key, ca_query, &ca_response,
                    "TEMPORAL_AGGREGATE Query CA failed_login_count")) {
        return false;
    }
    if (!Expect(ca_response.has_value() && ca_response.value() == 1,
                "TEMPORAL_AGGREGATE dimension filter should isolate CA count")) {
        return false;
    }

    bcache2::temporal_aggregate::QueryRequest empty_query = query;
    empty_query.set_start_timestamp_ms(base_ts_ms + 10 * kMinuteMs);
    empty_query.set_end_timestamp_ms(base_ts_ms + 11 * kMinuteMs);
    bcache2::temporal_aggregate::QueryResponse empty_response;
    if (!ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                    bcache2::temporal_aggregate::QUERY, key, empty_query, &empty_response,
                    "TEMPORAL_AGGREGATE Query empty window")) {
        return false;
    }
    if (!Expect(!empty_response.has_value() && empty_response.buckets_size() == 0,
                "TEMPORAL_AGGREGATE empty window should have no value")) {
        return false;
    }

    const std::string user_key = prefix + ":temporal_aggregate:user_1";
    struct AggregateCase {
        const char* metric;
        bcache2::temporal_aggregate::AggregateOp op;
        int64_t values[3];
        int64_t expected;
    };
    const AggregateCase cases[] = {
        {"purchase_amount_sum", bcache2::temporal_aggregate::SUM, {20, 30, -5}, 45},
        {"purchase_amount_min", bcache2::temporal_aggregate::MIN, {20, 30, -5}, -5},
        {"purchase_amount_max", bcache2::temporal_aggregate::MAX, {20, 30, -5}, 30},
    };

    for (const auto& test_case : cases) {
        for (int i = 0; i < 3; ++i) {
            bcache2::temporal_aggregate::IncrRequest incr;
            incr.set_key(user_key);
            incr.set_metric(test_case.metric);
            auto* merchant = incr.add_dimensions();
            merchant->set_name("merchant_type");
            merchant->set_value("grocery");
            incr.set_timestamp_ms(base_ts_ms + static_cast<uint64_t>(i) * kMinuteMs);
            incr.set_bucket_width_ms(kMinuteMs);
            incr.set_value(test_case.values[i]);
            incr.set_op(test_case.op);

            bcache2::temporal_aggregate::IncrResponse incr_response;
            if (!ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                            bcache2::temporal_aggregate::INCR, user_key, incr, &incr_response,
                            std::string("TEMPORAL_AGGREGATE Incr ") + test_case.metric)) {
                return false;
            }
        }

        bcache2::temporal_aggregate::QueryRequest aggregate_query;
        aggregate_query.set_key(user_key);
        aggregate_query.set_metric(test_case.metric);
        auto* merchant = aggregate_query.add_dimensions();
        merchant->set_name("merchant_type");
        merchant->set_value("grocery");
        aggregate_query.set_start_timestamp_ms(base_ts_ms);
        aggregate_query.set_end_timestamp_ms(base_ts_ms + 3 * kMinuteMs);
        aggregate_query.set_bucket_width_ms(kMinuteMs);
        aggregate_query.set_op(test_case.op);

        bcache2::temporal_aggregate::QueryResponse aggregate_response;
        if (!ExecuteRaw(table, bcache2::Module::TEMPORAL_AGGREGATE,
                        bcache2::temporal_aggregate::QUERY, user_key, aggregate_query,
                        &aggregate_response,
                        std::string("TEMPORAL_AGGREGATE Query ") + test_case.metric)) {
            return false;
        }
        if (!Expect(aggregate_response.has_value() &&
                        aggregate_response.value() == test_case.expected,
                    std::string("TEMPORAL_AGGREGATE unexpected result for ") +
                        test_case.metric + ", got " +
                        std::to_string(aggregate_response.value()))) {
            return false;
        }
    }

    std::cout << "PASS TEMPORAL_AGGREGATE: count window with dimensions, count="
              << response.value() << ", buckets=" << response.buckets_size() << std::endl;
    std::cout << "PASS TEMPORAL_AGGREGATE: sum/min/max aggregated feature windows" << std::endl;
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cout << "usage: " << argv[0] << " <metaserver_host:port> <idc> <namespace> <table>"
                  << std::endl;
        return 2;
    }

    bcache2::client::ClientOptions options;
    options.log_level = bcache2::client::LogLevel::kWarning;
    options.af = bcache2::client::AddressFamily::kIp4;
    options.master_addr = argv[1];
    options.idc = argv[2];
    options.host = "127.0.0.1";
    options.psm = "module.ingest.query.example";
    options.partition_pick_opts.policy = bcache2::client::PartitionPickOptions::Policy::kPrimary;

    bcache2::client::Client* raw_client = nullptr;
    bcache2::Status status = bcache2::client::Client::Create(options, &raw_client);
    if (!ExpectStatus(status, "Client::Create")) {
        return 1;
    }
    std::unique_ptr<bcache2::client::Client> client(raw_client);

    bcache2::client::Table* raw_table = nullptr;
    status = client->OpenTable(argv[3], argv[4], bcache2::client::TableOptions(), &raw_table);
    if (!ExpectStatus(status, "OpenTable")) {
        return 1;
    }
    std::unique_ptr<bcache2::client::Table> table(raw_table);

    auto* table_core = dynamic_cast<bcache2::client::TableCore*>(raw_table);
    if (!Expect(table_core != nullptr, "opened table is not a TableCore")) {
        return 1;
    }

    const std::string prefix = UniquePrefix();
    std::cout << "Using key prefix: " << prefix << std::endl;

    bool ok = true;
    ok = ok && TestStringAndCommon(table.get(), prefix);
    ok = ok && TestHash(table.get(), prefix);
    ok = ok && TestSet(table_core, prefix);
    ok = ok && TestFeature(table_core, prefix);
    ok = ok && TestIps(table_core, prefix);
    ok = ok && TestRisk(table_core, prefix);
    ok = ok && TestTemporalAggregate(table_core, prefix);

    bcache2::Status close_status = client->CloseTable(table.get());
    if (!ExpectStatus(close_status, "CloseTable")) {
        ok = false;
    }

    if (!ok) {
        return 1;
    }

    std::cout << "PASS ALL MODULE INGEST+QUERY TESTS" << std::endl;
    return 0;
}
