// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
// #include "bcache/server/ips_interface/profile_table_schema.h"

#include <utility>

// #include "bcache/common/flags.h"
// #include "bcache/server/ips_interface/profile_parse_time_range.h"
#include "model/ips/profile_parse_time_range.h"
#include "model/ips/profile_table_schema.h"

namespace bcache2 {
namespace ips {

void ProfileTableSchema::Init(const char* table_name, const rapidjson::Value& val) {
    table_name_ = table_name;

    // Compaction Options
    IP_TABLE_GET_ATOMIC_CONFIG(val, min_snap_count_after_truncate, Int64, 100);
    IP_TABLE_GET_ATOMIC_CONFIG(val, trigger_compact_snap_count, Int64, 150);
    IP_TABLE_GET_CONFIG(val, truncate_by_time_max_snap_count, Int64, 600);
    IP_TABLE_GET_CONFIG(val, compact_interval, Int64, 100);
    IP_TABLE_GET_CONFIG(val, compact_type, String, "compress");
    IP_TABLE_GET_CONFIG(val, compress_start_time_type, String, "absolute");
    IP_TABLE_GET_CONFIG(val, enable_abase_key_prefix, Bool, false);
    IP_TABLE_GET_CONFIG(val, ttl_scan_limit, Int64, 500);
    // TTL
    IP_TABLE_GET_CONFIG(val, enable_table_ttl, Bool, false);
    if (enable_table_ttl_) {
        rapidjson::Value::ConstMemberIterator iter_ttl = val.FindMember("table_ttl_conf");
        if (iter_ttl != val.MemberEnd()) {
            table_ttl_us_ = ParseTimeName(iter_ttl->value.GetString());
        } else {
            table_ttl_us_ = -1;
        }

        // BC_INFO("table {} enable ttl, ttl range: {}", table_name, table_ttl_us_);
        // parse slot ttl conf
        auto slot_ttl_conf_iter = val.FindMember("slot_ttl_conf");
        if (slot_ttl_conf_iter == val.MemberEnd()) {
            if (table_ttl_us_ == -1) {
                // BC_FATAL(
                //     "ttl is enable, but both table_ttl_conf and slot_ttl_conf aren't not setted,
                //     " "table_name: {}", table_name);
            }
        } else {
            ParseSlotTtlConf(slot_ttl_conf_iter->value);
            SlotTtlMapPtr table_slot_ttl_conf;
            GetTableSlotTtlConf(&table_slot_ttl_conf);
            if (table_slot_ttl_conf != nullptr) {
                for (auto const& ttl_pair_conf : *table_slot_ttl_conf) {
                    // BC_INFO("slot ttl conf: slot: {}, ttl_us: {}", ttl_pair_conf.first,
                    //         ttl_pair_conf.second);
                }
            }
        }
    }

    rapidjson::Value::ConstMemberIterator itr_compress_type =
        val.FindMember("compress_compact_type");
    if (itr_compress_type == val.MemberEnd()) {
        compress_compact_type_ = CompressCompactType::FREGMENT;
    } else {
        const std::string& compress_compact_type_str = itr_compress_type->value.GetString();
        if (compress_compact_type_str == "fragment") {
            SetCompressCompactType(CompressCompactType::FREGMENT);
        } else if (compress_compact_type_str == "one_time") {
            SetCompressCompactType(CompressCompactType::OneTime);
        } else {
            // BC_FATAL("invalid compress_compact_type: {}", compress_compact_type_);
        }
    }

    // BC_INFO("compress_compact_type: {}",
    //         compress_compact_type_ == CompressCompactType::OneTime ? "one_time" : "fragment");

    rapidjson::Value::ConstMemberIterator itr_truncate = val.FindMember("truncate_type");
    if (itr_truncate == val.MemberEnd()) {
        truncate_type_ = IP_COUNT_TRUNCATE;
    } else {
        const std::string& truncate_type = itr_truncate->value.GetString();

        if (IsCountTruncateType(truncate_type)) {
            truncate_type_ = IP_COUNT_TRUNCATE;
        } else if (IsAbsoluteTimeTruncateType(truncate_type)) {
            truncate_type_ = IP_ABSOLUTE_TRUNCATE;
        } else if (IsRelativeTimeTruncateType(truncate_type)) {
            truncate_type_ = IP_RELATIVE_TRUNCATE;
        } else {
            // BC_FATAL("invalid truncate_type conf: {}", truncate_type.c_str());
        }
    }

    itr_truncate = val.FindMember("truncate_range");
    if (itr_truncate == val.MemberEnd()) {
        // 这个配置会造成数据的截断丢失，不提供有效的默认值，必须显式配置
        truncate_range_micros_.store(-1);
    } else {
        truncate_range_micros_.store(ParseTimeName(itr_truncate->value.GetString()));
    }

    // Shrink
    IP_TABLE_GET_ATOMIC_CONFIG(val, open_shrink, Bool, true);
    IP_TABLE_GET_ATOMIC_CONFIG(val, protected_latest_fid_ratio, Double, 0.30);
    IP_TABLE_GET_CONFIG(val, delete_sequence, String, "v1");

    // table type: pair or list
    if (open_shrink_.load() &&
        (delete_sequence_ == "v1" || delete_sequence_ == "v2" || delete_sequence_ == "wilson")) {
        table_type_ = PAIR;
    } else if (open_shrink_.load() && delete_sequence_ == "vx") {
        table_type_ = LIST;
    } else {
        rapidjson::Value::ConstMemberIterator iter = val.FindMember("table_type");
        if (iter == val.MemberEnd()) {
            // BC_FATAL(
            //     "when close shrink or delete_sequence_ not in [v1, v2, vx], "
            //     "table_type conf must be provided, table name: %s",
            //     table_name_.c_str());
        } else if (IsPairTable(iter->value.GetString())) {
            table_type_ = PAIR;
        } else if (IsListTable(iter->value.GetString())) {
            table_type_ = LIST;
        } else {
            // BC_FATAL("invalid table type conf: {}, table name: {}", iter->value.GetString(),
            //          table_name_.c_str());
        }
    }
    // delete_sequence为"vx"时，shrink时使用list[sort_key_index]进行排序；
    // delete_sequence为"customize"，并且shrink存在"SlotId:
    // fid_max_num"格式的配置时，
    // list类型shrink将使用list[sort_key_index]排序；pair类型:
    // sort_key_index值为0时，
    // 使用v1进行排序；sort_key_index值为1时，将使用v2进行排序；sort_key_index值为其他时，
    // shrink使用v1进行排序。建议delete_sequence为"customize"时，显示配置sort_key_index
    IP_TABLE_GET_ATOMIC_CONFIG(val, sort_key_index, Int64, 0);

    // Abase
    IP_TABLE_GET_CONFIG(val, abase_zone, String, "online");
    IP_TABLE_GET_CONFIG(val, abase_cluster, String, "abase_instance_profile_test");
    IP_TABLE_GET_CONFIG(val, abase_consul, String, "");
    IP_TABLE_GET_CONFIG(val, abase_namespace, String, "");
    IP_TABLE_GET_CONFIG(val, abase_table, String, "sandbox");
    IP_TABLE_GET_ATOMIC_CONFIG(val, abase_allow_write_data, Bool, true);
    IP_TABLE_GET_ATOMIC_CONFIG(val, abase_readonly, Bool, false);
    IP_TABLE_GET_ATOMIC_CONFIG(val, abase_query_time_out_mills, Int64, 50);
    IP_TABLE_GET_ATOMIC_CONFIG(val, abase_retry_max_count, Int64, 0);
    IP_TABLE_GET_ATOMIC_CONFIG(val, abase_retry_max_wait_ms, Int64, 500);
    IP_TABLE_GET_ATOMIC_CONFIG(val, abase_retry_wait_ms, Int64, 1);
    IP_TABLE_GET_ATOMIC_CONFIG(val, abase_reload_interval_ms, Int64, 300000);
    if (!CheckAbaseConf()) {
        // BC_FATAL("invalid store conf, consul:{}, namespace:{}, table:{}", abase_consul_.c_str(),
        //          abase_namespace_.c_str(), abase_table_.c_str());
    }

    // Other
    IP_TABLE_GET_ATOMIC_CONFIG(val, store_timestamp_history, Bool, false);
    IP_TABLE_GET_ATOMIC_CONFIG(val, access_quota_per_user, Int64, 100000000);
    IP_TABLE_GET_ATOMIC_CONFIG(val, insert_as_timestamp_point, Bool, false);
    IP_TABLE_GET_CONFIG(val, reduce_func, String, "sum");

    if (reduce_func_ == "sum") {
        reduce_type_ = IP_REDUCE_SUM;
    } else if (reduce_func_ == "max") {
        reduce_type_ = IP_REDUCE_MAX;
    } else if (reduce_func_ == "none") {
        reduce_type_ = IP_REDUCE_NONE;
    } else if (reduce_func_ == "sum_max") {
        reduce_type_ = IP_REDUCE_SUM_MAX;
    } else {
        // BC_FATAL("invalid reduce func {}", reduce_func_.c_str());
    }

    // Check truncate conf
    if (IsTruncateCompactType(compact_type_)) {
        if (truncate_type_ == IP_COUNT_TRUNCATE) {
            if (min_snap_count_after_truncate_ >= trigger_compact_snap_count_) {
                // BC_FATAL(
                //     "min_snap_count_after_truncate {} should less than rigger_compact_snap_count
                //     {} in " "truncate compact table config", min_snap_count_after_truncate_,
                //     trigger_compact_snap_count_);
            }
        } else if (truncate_range_micros_ == -1) {
            // BC_FATAL("missing truncate_range_micros conf");
        }
    } else if (!IsCompressCompactType(compact_type_)) {
        // BC_FATAL("invalid compact type: {}", compact_type_.c_str());
    }

    // Schema
    rapidjson::Value::ConstMemberIterator itr = val.FindMember("time_dimension");
    if (IsCompressCompactType(compact_type_) && itr == val.MemberEnd()) {
        // BC_FATAL("empty time_dimension config");
    } else if (IsCompressCompactType(compact_type_)) {
        // BC_WARN("cur init table {} time_dimension", table_name_);
        bool ret = time_dimension_.Init(itr->value, compress_start_time_type_);
        if (!ret) {
            // BC_FATAL("init timedimension failed");
        } else {
            const std::shared_ptr<std::vector<std::pair<int64_t, int64_t>>> time_dimension =
                time_dimension_.GetCompactRange();
            // BC_INFO("finish time dimension init, range list: ");
            for (size_t i = 0; i < time_dimension->size(); ++i) {
                const std::pair<int64_t, int64_t>& range = time_dimension->at(i);
                // BC_INFO("[{}, {}]", range.first, range.second);
            }
        }
    }

    itr = val.FindMember("slot");
    if (open_shrink_.load() && itr == val.MemberEnd()) {
        // BC_FATAL("empty slot config when enable shrink");
    } else if (open_shrink_.load() && itr != val.MemberEnd()) {
        slot_manager_.Init(itr->value);
    }

    if (!CheckSortOptor(delete_sequence_.c_str())) {
        // BC_FATAL("Failed to parse_sort_optor: {}", delete_sequence_.c_str());
    }
}

void ProfileTableSchema::ParseSlotTtlConf(const rapidjson::Value& slot_ttl_conf) {
    SlotTtlMap new_slot_ttl_conf;
    for (rapidjson::Value::ConstMemberIterator it = slot_ttl_conf.MemberBegin();
         it != slot_ttl_conf.MemberEnd(); ++it) {
        SlotID slot = std::stoi(it->name.GetString());
        int64_t cur_ttl_val = ParseTimeName(it->value.GetString());

        new_slot_ttl_conf[slot] = cur_ttl_val;
    }
    AppendSlotTtlConf(new_slot_ttl_conf);
}

void ProfileTableSchema::AppendSlotTtlConf(const SlotTtlMap& append_conf) {
    if (slots_ttl_conf_us_ == nullptr) {
        slots_ttl_conf_us_ = std::make_shared<SlotTtlMap>();
    }

    for (auto const& iter : append_conf) {
        (*slots_ttl_conf_us_)[iter.first] = iter.second;
    }

    return;
}

void ProfileTableSchema::GetTableSlotTtlConf(SlotTtlMapPtr* slot_ttl_conf) const {
    *slot_ttl_conf = slots_ttl_conf_us_;
}

int64_t ProfileTableSchema::GetSlotTtlConf(SlotID slot) const {
    if (!enable_table_ttl_) {
        return -1;
    }
    int64_t res = table_ttl_us_;

    SlotTtlMapPtr table_slot_ttl_conf;
    GetTableSlotTtlConf(&table_slot_ttl_conf);
    if (table_slot_ttl_conf == nullptr) {
        return res;
    }
    auto iter = table_slot_ttl_conf->find(slot);
    if (iter != table_slot_ttl_conf->end()) {
        res = iter->second;
    }

    return res;
}

int64_t ProfileTableSchema::GetTableMaxTtlConf() const {
    if (!enable_table_ttl_ || table_ttl_us_ == -1) {
        return -1;
    }
    int64_t res = table_ttl_us_;

    SlotTtlMapPtr table_slot_ttl_conf;
    GetTableSlotTtlConf(&table_slot_ttl_conf);
    if (table_slot_ttl_conf == nullptr) {
        return res;
    }
    for (auto const& slot_ttl : *table_slot_ttl_conf) {
        if (slot_ttl.second > res) {
            res = slot_ttl.second;
        }
    }
    return res;
}

}  // namespace ips
}  // namespace bcache2
