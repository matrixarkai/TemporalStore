// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <byte/include/macros.h>
#include <rapidjson/document.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "model/ips/ips_define.h"
#include "model/ips/profile_table_schema.h"
#include "common/logging.h"

namespace bcache2 {
namespace ips {

const char default_test_table_conf[] = R"({
    "table_compress": {
      "trigger_compact_snap_count": 1,
      "compact_type": "compress",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": false,
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "pair",
      "enable_table_ttl": true,
      "table_ttl_conf": "180d",
      "slot_ttl_conf": {
          "1": "30d",
          "2": "30d"
      },
      "time_dimension": {
        "1m": [
          "0s",
          "10m"
        ],
        "10m": [
          "10m",
          "1h"
        ],
        "1h": [
          "1h",
          "24h"
        ],
        "1d": [
          "24h",
          "30d"
        ],
        "30d": [
          "30d",
          "365d"
        ]
      },
      "slot": {
        "0": 100
      }
    },
    "table_truncate_by_count": {
      "trigger_compact_snap_count": 66666,
      "min_snap_count_after_truncate": 66660,
      "compact_type": "truncate",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": false,
      "reduce_func": "sum",
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "pair",
      "time_dimension": {
        "1000d": [
          "0s",
          "100000000d"
        ]
      },
      "slot": {
        "140": 500,
        "141": 1000,
        "142": 2000,
        "143": 2000,
        "228": 500
      }
    },
    "table_truncate_by_count_list": {
      "trigger_compact_snap_count": 66666,
      "min_snap_count_after_truncate": 66660,
      "compact_type": "truncate",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": false,
      "reduce_func": "sum",
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "list",
      "time_dimension": {
        "1000d": [
          "0s",
          "100000000d"
        ]
      },
      "slot": {
        "140": 500,
        "141": 1000,
        "142": 2000,
        "143": 2000,
        "228": 500
      }
    },
    "test_table_truncate_time": {
      "trigger_compact_snap_count": 1,
      "compact_type": "truncate",
      "truncate_type": "relative",
      "truncate_range": "2d",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": true,
      "reduce_func": "sum",
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "pair",
      "time_dimension": {
        "1000d": [
          "0s",
          "100000000d"
        ]
      },
      "slot": {
        "140": 500,
        "141": 1000,
        "142": 2000,
        "143": 2000,
        "228": 500
      }
    },
    "table_ttl": {
      "trigger_compact_snap_count": 1000000,
      "compact_type": "truncate",
      "truncate_type": "relative",
      "truncate_range": "2000d",
      "reduce_func": "none",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": false,
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "pair",
      "enable_table_ttl": true,
      "table_ttl_conf": "1h",
      "slot_ttl_conf": {
        "0": "5h"
      },
      "time_dimension": {
        "1m": [
          "0s",
          "10m"
        ],
        "10m": [
          "10m",
          "1h"
        ],
        "1h": [
          "1h",
          "24h"
        ],
        "1d": [
          "24h",
          "30d"
        ],
        "30d": [
          "30d",
          "365d"
        ]
      },
      "slot": {
        "0": 500
      }
    }
  })";

// using TreeFactoryPtr = std::shared_ptr<server::OrderedTreeFactory>;

class IpsTableSchemaManager {
 public:
    IpsTableSchemaManager() {}

    ~IpsTableSchemaManager() = default;

    std::shared_ptr<ProfileTableSchema> GetTreeFactoryAndSchema(const std::string& table) {
        auto it = table_schema_map_ptr_->find(table);
        if (LIKELY(it == table_schema_map_ptr_->end())) {
            LOG_ERROR("Not found table schema").put("TableName", table);
            return nullptr;
        }
        return it->second;
    }
    Status Init(const std::string&);

 private:
    struct TableConf {
     public:
        std::unordered_map<SlotID, int64_t> slot_conf{};
        std::shared_ptr<std::vector<std::pair<int64_t, int64_t>>> time_dimension_conf{};
        ProfileTableSchema::SlotTtlMap slot_ttl_conf{};
    };


    Status TableSchemaAndTreeCacheInit(const std::string& table_name, const rapidjson::Value& val,
                                       std::shared_ptr<ProfileTableSchema>* table_schema);

    void AppendNewTable(const std::string& table_name,
                        std::shared_ptr<ProfileTableSchema> table_schema);

    bool IsTableExist(const std::string& table_name);

    size_t TableCount();

    void CheckAndUpdateTableConf(const std::string& table__conf);

    void UpdateTableConf(ProfileTableSchema* table_schema, const TableConf& table_conf);

    void UpdateTableSlotConf(ProfileTableSchema* table_schema,
                             const std::unordered_map<SlotID, int64_t>& slot_conf);

    void UpdateTableTimeDimensionConf(
        ProfileTableSchema* table_schema,
        std::shared_ptr<std::vector<std::pair<int64_t, int64_t>>> time_dimension_conf);

    void UpdateTableSlotTtlConf(ProfileTableSchema* table_schema,
                                const std::unordered_map<SlotID, int64_t>& slot_ttl_map);

    void ParseTableConf(const rapidjson::Value& table_doc, const ProfileTableSchema& table_schema,
                        TableConf* table_conf);

    std::unordered_map<SlotID, int64_t> ParseTableSlotConf(const rapidjson::Value& slot_conf);

    bool ParseTableTimeDimensionConf(
        const rapidjson::Value& time_dimension_conf, const ProfileTableSchema& table_schema,
        std::shared_ptr<std::vector<std::pair<int64_t, int64_t>>>* compact_conf);

    bool ParseTableSlotTtlConf(const rapidjson::Value& slot_ttl_conf,
                               const ProfileTableSchema& table_schema,
                               std::unordered_map<SlotID, int64_t>* slot_ttl_map);

 private:
    using TableSchemaPtr = std::shared_ptr<ProfileTableSchema>;
    using TableSchemaMap = std::unordered_map<std::string, TableSchemaPtr>;
    std::shared_ptr<TableSchemaMap> table_schema_map_ptr_ = std::make_shared<TableSchemaMap>();

    friend class IPSInterfaceTest;
    friend class IPSTccTest;
    friend class TccConfig;
};

}  // namespace ips
}  // namespace bcache2
