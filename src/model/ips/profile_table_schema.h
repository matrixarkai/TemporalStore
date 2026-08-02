// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <rapidjson/document.h>
// #include <butil/third_party/rapidjson/document.h>
// #include <butil/third_party/rapidjson/filereadstream.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// #include "bcache/common/status.h"
// #include "bcache/server/ips_interface/ips_define.h"
// #include "bcache/server/ips_interface/profile_slot_manager.h"
// #include "bcache/server/ips_interface/profile_table_schema.h"
// #include "bcache/server/ips_interface/profile_time_dimension.h"
#include "common/status.h"
#include "model/ips/ips_define.h"
#include "model/ips/profile_slot_manager.h"
#include "model/ips/profile_table_schema.h"
#include "model/ips/profile_time_dimension.h"

namespace bcache2 {
namespace ips {

enum class CompressCompactType {
    FREGMENT = 0,
    OneTime = 1,
};

class ProfileTableSchema {
 public:
    using SlotTtlMap = std::unordered_map<SlotID, int64_t>;
    using SlotTtlMapPtr = std::shared_ptr<SlotTtlMap>;

    ProfileTableSchema() = default;
    ~ProfileTableSchema() = default;
    void Init(const char* table_name, const rapidjson::Value& val);

    const std::string& TableName() const { return table_name_; }

    // Compaction
    int64_t MinSnapCountAfterTruncate() const { return min_snap_count_after_truncate_.load(); }
    int64_t TriggerCompactSnapCount() const { return trigger_compact_snap_count_.load(); }
    const std::string& CompactType() const { return compact_type_; }

    // Truncate
    TruncateType GetTruncateType() const { return truncate_type_; }
    int64_t TruncateRangeMicros() const { return truncate_range_micros_.load(); }
    void SetTruncateRangeMicros(int64_t truncate_range_micros) {
        truncate_range_micros_.store(truncate_range_micros);
    }
    int64_t TruncateByTimeMaxSnapCount() const { return truncate_by_time_max_snap_count_; }

    // Shrink
    bool OpenShrink() const { return open_shrink_.load(); }
    double ProtectedLatestFidRatio() const { return protected_latest_fid_ratio_.load(); }
    const std::string& DeleteSequence() const { return delete_sequence_; }
    int64_t GetSortKeyIndex() const { return sort_key_index_.load(); }
    void SetShrink(bool shrink_conf) { open_shrink_.store(shrink_conf); }

    bool EnableTtl() const { return enable_table_ttl_; }
    int64_t GetSlotTtlConf(SlotID slot) const;
    int64_t GetTableMaxTtlConf() const;

    // Abase
    const std::string& AbaseZone() const { return abase_zone_; }
    const std::string& AbaseCluster() const { return abase_cluster_; }
    const std::string& AbaseConsul() const { return abase_consul_; }
    const std::string& AbaseTable() const { return abase_table_; }
    const std::string& AbaseNamespace() const { return abase_namespace_; }
    bool IsAbase2() const { return !abase_namespace_.empty(); }
    bool CheckAbaseConf() const {
        // check base param
        if (abase_consul_.empty() || abase_table_.empty()) {
            return false;
        }

        // check consul format
        if (IsAbase2()) {
            static std::string abase2_prefix = "bytedance.abase2.";
            return abase_consul_.compare(0, abase2_prefix.length(), abase2_prefix) == 0;
        } else {
            static std::vector<std::string> abase_prefixs = {"toutiao.abase.", "bytedance.abase."};
            for (const auto& prefix : abase_prefixs) {
                if (abase_consul_.compare(0, prefix.length(), prefix) == 0) return true;
            }
            return false;
        }
    }
    bool AbaseAllowWriteData() const { return abase_allow_write_data_.load(); }
    int64_t AbaseQueryTimeOutMills() const { return abase_query_time_out_mills_.load(); }
    int64_t AbaseRetryMaxCount() const { return abase_retry_max_count_.load(); }
    int64_t AbaseRetryMaxWaitMs() const { return abase_retry_max_wait_ms_.load(); }
    int64_t AbaseRetryWaitMs() const { return abase_retry_wait_ms_.load(); }
    int64_t abase_reload_interval_ms() const { return abase_reload_interval_ms_.load(); }
    void set_AbaseAllowWriteData(bool allow_write) { abase_allow_write_data_.store(allow_write); }
    void SetInsertAsTimestampPoint(bool insert_as_timestamp_point) {
        insert_as_timestamp_point_.store(insert_as_timestamp_point);
    }
    void SetStoreTimestampHistory(bool store_timestamp_history) {
        store_timestamp_history_.store(store_timestamp_history);
    }
    // Schema
    const TimeDimension& GetTimeDimension() const { return time_dimension_; }
    TimeDimension* GetMutableTimeDimension() { return &time_dimension_; }

    SlotManager& GetSlotManager() { return slot_manager_; }

    const SlotManager& GetSlotManager() const { return slot_manager_; }

    // Other
    bool StoreTimestampHistory() const { return store_timestamp_history_.load(); }
    int64_t AccessQuotaPerUser() const { return access_quota_per_user_.load(); }
    bool InsertAsTimestampPoint() const { return insert_as_timestamp_point_.load(); }
    ReduceType GetReduceType() const { return reduce_type_; }
    TableType GetTableType() const { return table_type_; }

    bool GetEnableAbaseKeyPrefix() const { return enable_abase_key_prefix_; }
    const std::string& GetAbaseKeyPrefix() const { return abase_key_prefix_; }

    CompressCompactType GetCompressCompactType() const { return compress_compact_type_; }

    void SetCompressCompactType(CompressCompactType type) { compress_compact_type_ = type; }
    int64_t GetCompactInterval() const { return compact_interval_; }

    const std::string& GetCompressStartTimeType() const { return compress_start_time_type_; }

    const int64_t GetTtlScanLimit() const { return ttl_scan_limit_; }

    void AppendSlotTtlConf(const SlotTtlMap& append_conf);

    void GetTableSlotTtlConf(SlotTtlMapPtr* slot_ttl_conf) const;

 private:
    void ParseSlotTtlConf(const rapidjson::Value& slot_ttl_conf);

 private:
    std::string table_name_;
    // Compaction
    std::atomic<int64_t> min_snap_count_after_truncate_;
    std::atomic<int64_t> trigger_compact_snap_count_;
    // invalid type:
    // "compress":执行聚合函数
    // "truncate":保留最近若干个
    std::string compact_type_;

    // "absolute"、"relative" or "count", truncate 默认按照snap数量进行截断,
    // "absolute"和"relative"类型，由truncate_range_micros_决定保留多长时间的snap
    TruncateType truncate_type_;
    std::atomic<int64_t> truncate_range_micros_;  // microseconds
    // truncate by time时，需要避免极端case保存过多snap问题
    int64_t truncate_by_time_max_snap_count_;

    // valid type: absolute/relative
    std::string compress_start_time_type_;

    // Shrink
    std::atomic<bool> open_shrink_;
    std::atomic<double> protected_latest_fid_ratio_;
    std::string delete_sequence_;
    std::atomic<int64_t> sort_key_index_;
    // Abase
    std::string abase_zone_;
    std::string abase_cluster_;
    std::string abase_consul_;
    std::string abase_namespace_;
    std::string abase_table_;
    std::atomic<bool> abase_allow_write_data_;
    std::atomic<bool> abase_readonly_;
    std::atomic<int64_t> abase_query_time_out_mills_;
    std::atomic<int64_t> abase_retry_max_count_;
    std::atomic<int64_t> abase_retry_max_wait_ms_;
    std::atomic<int64_t> abase_retry_wait_ms_;
    std::atomic<int64_t> abase_reload_interval_ms_;

    // Schema
    TimeDimension time_dimension_;
    SlotManager slot_manager_;

    // Ttl
    bool enable_table_ttl_;
    int64_t table_ttl_us_;
    SlotTtlMapPtr slots_ttl_conf_us_;
    int64_t ttl_scan_limit_;

    // Other
    std::atomic<bool> store_timestamp_history_;
    std::atomic<int64_t> access_quota_per_user_;
    std::atomic<bool> insert_as_timestamp_point_;
    ReduceType reduce_type_;
    std::string reduce_func_;
    TableType table_type_;

    bool enable_abase_key_prefix_;
    std::string abase_key_prefix_;
    CompressCompactType compress_compact_type_;
    int64_t compact_interval_;
};

}  // namespace ips
}  // namespace bcache2
