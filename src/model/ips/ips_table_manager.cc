// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
// #include "bcache/server/ips_interface/tree_manager.h"

#include "model/ips/ips_table_manager.h"


#include <butil/strings/string_split.h>
#include <butil/strings/string_util.h>
#include <byte/include/assert.h>

#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "model/ips/profile_parse_time_range.h"
#include "model/ips/utils.h"
#include "common/logging.h"

namespace bcache2 {
namespace ips {
static const int64_t kDefaultJsonBufferSize = 65536;
static const int64_t kDefaultEvictBatchSize = 100;
static constexpr char kTableConfTccKey[] = "table";
static constexpr char kSlotConfTccKey[] = "slot";
static constexpr char kTimeDimensionTccKey[] = "time_dimension";
static constexpr char kTtlSlotConf[] = "slot_ttl_conf";

Status IpsTableSchemaManager::Init(const std::string& raw_json) {
    rapidjson::Document doc;
    if (raw_json.empty()) {
        doc.Parse(default_test_table_conf);
    } else {
        doc.Parse(raw_json.c_str());
    }

    for (rapidjson::Value::ConstMemberIterator it = doc.MemberBegin(); it != doc.MemberEnd();
         it++) {
        std::string table_name(it->name.GetString());
        if (IsTableExist(table_name)) {
            LOG_ERROR("Duplicate user table").put("TableName", table_name);
        }
        LOG_INFO("IPS table start init").put("TableName", table_name);

        std::shared_ptr<ProfileTableSchema> ips_schema;
        Status ret = TableSchemaAndTreeCacheInit(table_name, it->value, &ips_schema);
        if (!ret.IsOK()) {
             LOG_ERROR("Init table failed").put("TableName", table_name)
                       .put("ErrorMsg", ret.ToString());
             return ret;
        }
        AppendNewTable(table_name, std::move(ips_schema));
        LOG_INFO("Table finished init").put("TableName", table_name);
    }

    size_t table_count = TableCount();
    if (table_count == 0) {
        LOG_ERROR("Empty table list");
        return Status::Internal("Empty ips table");
    }
    return Status::OK();
}

Status IpsTableSchemaManager::TableSchemaAndTreeCacheInit(
    const std::string& table_name, const rapidjson::Value& val,
    std::shared_ptr<ProfileTableSchema>* table_schema) {
    std::shared_ptr<ProfileTableSchema> ips_schema = std::make_shared<ProfileTableSchema>();
    ips_schema->Init(table_name.c_str(), val);
    *table_schema = std::move(ips_schema);
    return Status::OK();
}

void IpsTableSchemaManager::AppendNewTable(const std::string& table_name,
                                           std::shared_ptr<ProfileTableSchema> table_schema) {
    BYTE_ASSERT(table_schema_map_ptr_->find(table_name) == table_schema_map_ptr_->end());
    table_schema_map_ptr_->emplace(table_name, table_schema);
    return;
}

bool IpsTableSchemaManager::IsTableExist(const std::string& table_name) {
    return table_schema_map_ptr_->find(table_name) != table_schema_map_ptr_->end();
}

size_t IpsTableSchemaManager::TableCount() {
    return table_schema_map_ptr_->size();
}

void IpsTableSchemaManager::ParseTableConf(const rapidjson::Value& table_doc,
                                           const ProfileTableSchema& table_schema,
                                           TableConf* table_conf) {
    if (table_doc.HasMember(kSlotConfTccKey)) {
        table_conf->slot_conf = ParseTableSlotConf(table_doc[kSlotConfTccKey]);
    }

    if (table_doc.HasMember(kTimeDimensionTccKey)) {
        std::shared_ptr<std::vector<std::pair<int64_t, int64_t>>> compact_conf;
        bool ret = ParseTableTimeDimensionConf(table_doc[kTimeDimensionTccKey], table_schema,
                                               &compact_conf);
        if (ret) {
            assert(compact_conf->size() > 0);
            table_conf->time_dimension_conf = std::move(compact_conf);
        } else {
            LOG_ERROR("Invalid time diemnsion conf");
        }
    }

    if (table_doc.HasMember(kTtlSlotConf)) {
        if (!table_schema.EnableTtl()) {
            // BC_WARN(
            //     "table ttl is disabled, but tcc conf contains slot_ttl_conf conf. please set "
            //     "enable_table_ttl true first. table_name: {}",
            //     table_schema.TableName());
        } else {
            std::unordered_map<SlotID, int64_t> slot_ttl_map;
            bool ret = ParseTableSlotTtlConf(table_doc[kTtlSlotConf], table_schema, &slot_ttl_map);
            assert(ret);
            table_conf->slot_ttl_conf = std::move(slot_ttl_map);
        }
    }
}

std::unordered_map<SlotID, int64_t> IpsTableSchemaManager::ParseTableSlotConf(
    const rapidjson::Value& slot_conf) {
    std::unordered_map<SlotID, int64_t> res;
    for (rapidjson::Value::ConstMemberIterator it = slot_conf.MemberBegin();
         it != slot_conf.MemberEnd(); ++it) {
        SlotID slot = std::stoi(it->name.GetString());
        int64_t fid_num = it->value.GetInt();
        res[slot] = fid_num;
    }
    return res;
}

bool IpsTableSchemaManager::ParseTableTimeDimensionConf(
    const rapidjson::Value& time_dimension_conf, const ProfileTableSchema& table_schema,
    std::shared_ptr<std::vector<std::pair<int64_t, int64_t>>>* compact_conf) {
    TimeDimension time_dimension;
    bool ret = time_dimension.Init(time_dimension_conf, table_schema.GetCompressStartTimeType());
    if (UNLIKELY(!ret)) {
        return false;
    }
    *compact_conf = std::make_shared<std::vector<std::pair<int64_t, int64_t>>>(
        *(time_dimension.GetCompactRange()));
    return true;
}

bool IpsTableSchemaManager::ParseTableSlotTtlConf(
    const rapidjson::Value& slot_ttl_conf, const ProfileTableSchema& table_schema,
    std::unordered_map<SlotID, int64_t>* slot_ttl_map) {
    slot_ttl_map->clear();
    for (rapidjson::Value::ConstMemberIterator it = slot_ttl_conf.MemberBegin();
         it != slot_ttl_conf.MemberEnd(); ++it) {
        SlotID slot = std::stoi(it->name.GetString());
        int64_t cur_ttl_val = ParseTimeName(it->value.GetString());

        (*slot_ttl_map)[slot] = cur_ttl_val;
    }
    return true;
}

void IpsTableSchemaManager::UpdateTableSlotConf(
    ProfileTableSchema* table_schema, const std::unordered_map<SlotID, int64_t>& new_slot_conf) {
    const std::unordered_map<SlotID, int64_t> old_slot_conf =
        table_schema->GetSlotManager().GetSlotConf();
    if (LIKELY(old_slot_conf == new_slot_conf)) {
        return;
    }

    std::unordered_map<SlotID, int64_t> insert_slots;
    std::vector<SlotID> del_slots;
    for (auto const& new_slot_pair : new_slot_conf) {
        SlotID slot = new_slot_pair.first;
        int64_t fid_num = new_slot_pair.second;

        auto iter = old_slot_conf.find(slot);
        if (iter == old_slot_conf.end() || iter->second != fid_num) {
            insert_slots[slot] = fid_num;
        }
    }

    for (auto const& old_slot_pair : old_slot_conf) {
        SlotID slot = old_slot_pair.first;
        int64_t fid_num = old_slot_pair.second;

        if (new_slot_conf.find(slot) == new_slot_conf.end()) {
            del_slots.emplace_back(slot);
        }
    }

    if (!insert_slots.empty()) {
        table_schema->GetSlotManager().InsertSlotFidLimit(insert_slots);
        // BC_INFO("insert or update slot conf from tcc, table name: {}",
        // table_schema->TableName());
        for (auto const& slot_pair : insert_slots) {
            // BC_INFO("slot id: {}, fid num: {}", slot_pair.first, slot_pair.second);
        }
    }

    if (!del_slots.empty()) {
        table_schema->GetSlotManager().DelSlotFidLimit(del_slots);

        // BC_INFO("delete slot conf from tcc, table name: {}", table_schema->TableName());
        for (SlotID slot : del_slots) {
            // BC_INFO("slot id: {}", slot);
        }
    }
}

void IpsTableSchemaManager::UpdateTableSlotTtlConf(
    ProfileTableSchema* table_schema,
    const std::unordered_map<SlotID, int64_t>& new_slot_ttl_conf) {
    ProfileTableSchema::SlotTtlMapPtr old_slot_ttl_conf;
    table_schema->GetTableSlotTtlConf(&old_slot_ttl_conf);
    if (old_slot_ttl_conf != nullptr && *old_slot_ttl_conf == new_slot_ttl_conf) {
        return;
    }

    ProfileTableSchema::SlotTtlMap delta_map{};
    for (auto iter = new_slot_ttl_conf.cbegin(); iter != new_slot_ttl_conf.end(); ++iter) {
        SlotID slot_id = iter->first;
        int64_t ttl_us = iter->second;

        bool cur_slot_exist = old_slot_ttl_conf != nullptr &&
                              old_slot_ttl_conf->find(slot_id) != old_slot_ttl_conf->end();
        if (!cur_slot_exist || old_slot_ttl_conf->at(slot_id) != ttl_us) {
            delta_map[slot_id] = ttl_us;

            std::string old_ttl_str_val;
            if (!cur_slot_exist) {
                old_ttl_str_val = "not_set";
            } else {
                old_ttl_str_val = std::to_string(old_slot_ttl_conf->at(slot_id));
            }
            // BC_INFO(
            //     "update or insert slot ttl conf, table_name: {}, slot_id: {}, old_ttl_conf: {},
            //     updated " "ttl_conf: {}", table_schema->TableName(), slot_id, old_ttl_str_val,
            //     ttl_us);
        }
    }

    if (!delta_map.empty()) {
        table_schema->AppendSlotTtlConf(delta_map);
        // BC_INFO("finish update slot ttl conf, table_name: {}", table_schema->TableName());
    }
}

void IpsTableSchemaManager::UpdateTableTimeDimensionConf(
    ProfileTableSchema* table_schema,
    std::shared_ptr<std::vector<std::pair<int64_t, int64_t>>> time_dimension_conf) {
    TimeDimension* old_time_dimension = table_schema->GetMutableTimeDimension();
    auto const old_conf = old_time_dimension->GetCompactRange();
    if (*time_dimension_conf.get() == *old_conf.get()) {
        return;
    }
    old_time_dimension->ReplaceCompactIntervals(time_dimension_conf);
    // BC_INFO("update time dimension from tcc, table name: {}, range list: ",
    // table_schema->TableName());
    for (size_t i = 0; i < time_dimension_conf->size(); ++i) {
        const std::pair<int64_t, int64_t>& range = time_dimension_conf->at(i);
        // BC_INFO("[{}, {}]", range.first, range.second);
    }
}

void IpsTableSchemaManager::UpdateTableConf(ProfileTableSchema* table_schema,
                                            const TableConf& table_conf) {
    if (!table_conf.slot_conf.empty()) {
        UpdateTableSlotConf(table_schema, table_conf.slot_conf);
    }

    if (table_conf.time_dimension_conf != nullptr && !table_conf.time_dimension_conf->empty()) {
        UpdateTableTimeDimensionConf(table_schema, table_conf.time_dimension_conf);
    }

    if (!table_conf.slot_ttl_conf.empty()) {
        UpdateTableSlotTtlConf(table_schema, table_conf.slot_ttl_conf);
    }
}

void IpsTableSchemaManager::CheckAndUpdateTableConf(const std::string& tcc_table_conf) {
    rapidjson::Document tcc_table_doc;
    tcc_table_doc.Parse(tcc_table_conf.c_str());
    for (rapidjson::Value::ConstMemberIterator it = tcc_table_doc.MemberBegin();
         it != tcc_table_doc.MemberEnd(); ++it) {
        std::string table_name(it->name.GetString());
        if (!IsTableExist(table_name)) {
            // BC_INFO("table: {} start init from tcc", table_name);
            std::shared_ptr<ProfileTableSchema> ips_schema;
            Status ret = TableSchemaAndTreeCacheInit(table_name, it->value, &ips_schema);
            if (!ret.ok()) {
                // BC_ERROR("Init table failed, table_name: {}, ret: {}", table_name,
                // ret.ToString());
                return;
            }
            assert(ips_schema != nullptr);
            // assert(tree_cache != nullptr);
            // TreeFactoryPtr cur_tree_factory =
            // std::make_shared<server::OrderedTreeFactoryImpl>(tree_cache);

            // ret = evicter_->AddTreeCache(tree_cache);
            if (UNLIKELY(!ret.ok())) {
                // BC_ERROR("failed to append treecache to evict, table_name: {}", table_name);
                continue;
            }
            AppendNewTable(table_name, std::move(ips_schema));
            // BC_INFO("table: {} finish init from tcc", table_name);
        } else {
            // table exist
            // std::shared_ptr<ProfileTableSchema>* table_schema;
            // bool ret = GetTableSchema(table_name, &table_schema);
            // assert(ret);

            TableConf table_conf;
            // ParseTableConf(it->value, *table_schema.get(), &table_conf);
            // UpdateTableConf(table_schema.get(), table_conf);
        }
    }
}

}  // namespace ips
}  // namespace bcache2
