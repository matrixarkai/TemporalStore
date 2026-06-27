#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/client.h"
#include "common/status.h"

namespace bcache2 {
namespace client {

struct TemporalStoreClientOptions {
    std::string metaserver_addr;
    std::string metaserver_consul;
    std::string namespace_name;
    std::string table_name;
    std::string idc = "vdc1";
    std::string host = "127.0.0.1";
    std::string psm = "temporalstore.customer.client";
    std::string log_dir = "./";
    LogLevel log_level = LogLevel::kWarning;
    AddressFamily address_family = AddressFamily::kIp4;
    int64_t meta_sync_interval_ms = 1000 * 60 * 10;
    int64_t topo_error_retry_interval_ms = 1000 * 5;
    int64_t meta_fetch_timeout_ms = 2000;
    int64_t io_timeout_ms = 1000;
    int64_t connect_timeout_ms = 1000;
    int64_t continuous_failed_time_ms = 10000;
    uint64_t request_timeout_ms = 5000;
    int max_read_retries = 1;
    int max_write_retries = 0;  // Enable only for idempotent writes.
    int retry_backoff_ms = 2;
    int max_feature_points_per_request = 1000;
    uint64_t max_feature_query_count = 5000;
    uint64_t max_key_bytes = 4096;
    uint64_t max_value_bytes = 16ULL * 1024ULL * 1024ULL;
    bool pin_primary = true;
};

struct TemporalFeaturePoint {
    uint64_t timestamp = 0;
    std::string value;
};

struct MatrixArkHashRecord {
    std::string key;
    std::string field;
    std::string value;
};

enum class TemporalFeatureWritePolicy {
    kUpsert,
    kBlock,
    kFirst,
    kUpdate,
};

enum class TemporalFeatureFilterOp {
    kEqual,
    kNotEqual,
    kGreaterThan,
    kLessThan,
};

struct TemporalFeatureFilter {
    std::string field;
    TemporalFeatureFilterOp op = TemporalFeatureFilterOp::kEqual;
    uint64_t value = 0;
};

struct TemporalFeatureQuery {
    uint64_t start_ts = 0;
    uint64_t end_ts = 0;
    uint64_t count = 0;
    std::vector<TemporalFeatureFilter> filters;
};

struct SequenceFeatureRow {
    uint64_t timestamp = 0;
    uint64_t gid = 0;
    uint32_t action_type = 0;
    uint32_t duration = 0;
    uint64_t author_id = 0;
};

struct IpsFeatureStat {
    int64_t id = 0;
    int32_t slot = 0;
    bool has_slot = true;
    int32_t type = 0;
    int32_t v1 = 0;
    int32_t v2 = 0;
};

struct IpsInstance {
    std::string table = "table_compress";
    int64_t uid = 0;
    int64_t timestamp_us = 0;
    int32_t action_type = 0;
    int32_t logical_table = 0;
    bool enable_server_aggregator = true;
    bool enable_idempotent = false;
    std::vector<IpsFeatureStat> features;
};

struct IpsLastQuery {
    std::string table = "table_compress";
    int64_t uid = 0;
    int32_t action_type = 0;
    int32_t logical_table = 0;
    int32_t slot = 0;
    int32_t top_k = 20;
    int64_t last_instances = 10;
};

enum class RiskPrecision {
    kOneSecond,
    kFiveSeconds,
    kTenSeconds,
    kOneMinute,
    kFiveMinutes,
    kTenMinutes,
    kOneHour,
    kOneDay,
    kOneMonth,
};

enum class RiskWindowUnit {
    kSecond,
    kMinute,
    kHour,
    kDay,
};

struct RiskWindow {
    int64_t start = -1;
    int64_t end = 0;
    RiskWindowUnit unit = RiskWindowUnit::kHour;
};

struct HashEntry {
    std::string key;
    std::string field;
    std::string value;
    std::string route_json;
};

struct MatrixArkBatchAppendOptions {
    std::string append_options_json;
};

class TemporalStoreClient {
 public:
    static Status Connect(const TemporalStoreClientOptions& options,
                          std::unique_ptr<TemporalStoreClient>* client);

    ~TemporalStoreClient();
    TemporalStoreClient(TemporalStoreClient&&) noexcept;
    TemporalStoreClient& operator=(TemporalStoreClient&&) noexcept;

    TemporalStoreClient(const TemporalStoreClient&) = delete;
    TemporalStoreClient& operator=(const TemporalStoreClient&) = delete;

    Status Close();

    Status PutString(const std::string& key, const std::string& value);
    Status PutStringWithTtl(const std::string& key, const std::string& value, uint64_t ttl_ms);
    Status GetString(const std::string& key, std::string* value);
    Status DeleteObject(const std::string& key);
    Status Expire(const std::string& key, uint64_t ttl_ms);
    Status Ttl(const std::string& key, uint64_t* ttl_ms);

    Status HSet(const std::string& key, const std::string& field, const std::string& value);
    Status HGet(const std::string& key, const std::string& field, std::string* value);
    Status HGetAll(const std::string& key, std::vector<HashEntry>* entries);
    Status HDel(const std::string& key, const std::string& field);
    Status MatrixArkBatchAppendRecords(const std::vector<HashEntry>& entries,
                                       const std::string& count_key = "",
                                       const std::string& count_value = "",
                                       const MatrixArkBatchAppendOptions& options = {});
    Status MatrixArkRetrieveContextPack(const std::string& request_json,
                                        std::string* response_json);

    Status SAdd(const std::string& key, const std::string& member);
    Status SMembers(const std::string& key, std::vector<std::string>* members);

    Status AddFeaturePoints(const std::string& key, const std::vector<TemporalFeaturePoint>& points,
                            TemporalFeatureWritePolicy policy = TemporalFeatureWritePolicy::kUpsert);
    Status QueryFeaturePoints(const std::string& key, uint64_t start_ts, uint64_t end_ts,
                              uint64_t count, std::vector<TemporalFeaturePoint>* points);
    Status QueryFeaturePoints(const std::string& key, const TemporalFeatureQuery& query,
                              std::vector<TemporalFeaturePoint>* points);
    Status AddSequenceFeatureRows(
        const std::string& key, const std::vector<SequenceFeatureRow>& rows,
        TemporalFeatureWritePolicy policy = TemporalFeatureWritePolicy::kUpsert);
    Status QuerySequenceFeatureRows(const std::string& key, const TemporalFeatureQuery& query,
                                    std::vector<SequenceFeatureRow>* rows);

    Status AddIpsInstance(const IpsInstance& instance);
    Status QueryIpsLastInstances(const IpsLastQuery& query, std::vector<IpsFeatureStat>* features);

    Status RiskIncrement(const std::string& key, int64_t amount, uint64_t ttl_seconds,
                         RiskPrecision precision, const std::string& uuid = "",
                         uint64_t occur_time_seconds = 0);
    Status RiskCount(const std::string& key, RiskPrecision precision, const RiskWindow& window,
                     int64_t* count);

 private:
    struct Impl;

    explicit TemporalStoreClient(std::unique_ptr<Impl> impl);
    Status CheckInitialized() const;

    std::unique_ptr<Impl> impl_;
};

}  // namespace client
}  // namespace bcache2
