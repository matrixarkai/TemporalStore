// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <byte/include/macros.h>

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/slice.h"
#include "common/status.h"
#include "model/ips/profile_find_top_k.h"
#include "model/ips/profile_table_schema.h"
#include "model/ips/profile_time_dimension.h"
#include "model/ips/time_cost.h"
#include "model/ips/utils.h"
#include "model/ips_model.h"
#include "model/persistent_map.h"
#include "partition/cmd_context.h"

// #include "model/model.h"

namespace bcache2 {
namespace ips {

using Tag = std::pair<std::string, std::string>;
using TagList = std::vector<std::pair<std::string, std::string>>;
using model::PersistentMap;
using partition::CmdContext;

// 在IPS场景下，key是ts，当range key取 kMinIPSKey和kMaxIPSKey时，
// 表示需要需所有时间区间数据

static const int64_t kMinIPSKey = INT64_MIN;
static const int64_t kMaxIPSKey = INT64_MAX;
using MergeFunc =
    std::function<Status(const Slice& old_val, const Slice& insert_val, std::string* merged_res)>;

class IPSOperator {
 public:
    // caller by query, convert data in range to IPSFeatureData format
    static Status RangeGet(model::IpsModel* ordered_tree, int64_t base_ts, uint8_t tree_version,
                           int64_t min_data_ts_micros, int64_t max_data_ts_micros,
                           std::vector<IPSFeatureData>* res, bool is_sort_by_ts,
                           int64_t* range_min_ts, int64_t* range_max_ts, ReduceType reduce_type);

    // query reverse scan last cnt
    static Status RangeGet(model::IpsModel* ordered_tree, int64_t base_ts, uint8_t tree_version,
                           int64_t cnt, std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                           int64_t* range_max_ts, ReduceType reduce_type);

    // FilterByVx with last instance
    static Status RangeGetFilterByVx(model::IpsModel* ordered_tree, int64_t base_ts,
                                     uint8_t tree_version, int64_t cnt, int64_t vx, int64_t val,
                                     std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                                     int64_t* range_max_ts, ReduceType reduce_type);

    // FilterByVx with timerange
    static Status RangeGetFilterByVx(model::IpsModel* ordered_tree, int64_t base_ts,
                                     uint8_t tree_version, int64_t min_data_ts_micros,
                                     int64_t max_data_ts_micros, int64_t cnt, int64_t vx,
                                     int64_t val, std::vector<IPSFeatureData>* res,
                                     int64_t* range_min_ts, int64_t* range_max_ts,
                                     ReduceType reduce_type);

    // filter by fid with time range query
    static Status RangeGetFilterByFid(model::IpsModel* ordered_tree, int64_t base_ts,
                                      uint8_t tree_version, int64_t min_data_ts_micros,
                                      int64_t max_data_ts_micros,
                                      const std::unordered_set<int64_t>& fid_set,
                                      std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                                      int64_t* range_max_ts, ReduceType reduce_type);

    //  filter by fid with last instance query
    static Status RangeGetFilterByFid(model::IpsModel* ordered_tree, int64_t base_ts,
                                      uint8_t tree_version, int64_t cnt,
                                      const std::unordered_set<int64_t>& fid_set,
                                      std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                                      int64_t* range_max_ts, ReduceType reduce_type);

    // strong filter with range query
    static Status RangeGetStrongFilter(model::IpsModel* ordered_tree, int64_t base_ts,
                                       uint8_t tree_version, int64_t min_data_ts_micros,
                                       int64_t max_data_ts_micros,
                                       std::vector<int32_t>* index_strong,
                                       std::vector<int32_t>* min_index_count, int64_t top_k,
                                       std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                                       int64_t* range_max_ts, ReduceType reduce_type);

    static Status RangeGetLastCntEle(ReduceType reduce_type, model::IpsModel* ordered_tree,
                                     int64_t base_ts, uint8_t tree_version,
                                     std::vector<IPSFeatureData>* res, int64_t cnt,
                                     int64_t* range_min_ts, int64_t* range_max_ts);

    // 返回完整覆盖在[min_data_ts_micros, max_data_ts_micros)区间的tree kv
    static Status RangeGetByTime(ReduceType reduce_type, model::IpsModel* ordered_tree,
                                 int64_t base_ts, uint8_t tree_version, int64_t min_data_ts_micros,
                                 int64_t max_data_ts_micros,
                                 std::vector<std::pair<Slice, Slice>>* res, bool key_only);

    static void ReserveKElements(std::vector<IPSFeatureData>* res, uint64_t top_k, bool first_k);

    static Status InsertFeatureStat(CmdContext* ctx, model::IpsModel* ordered_tree, int64_t ts,
                                    const FeatureStat32& fs, TableType table_type,
                                    ReduceType reduce_type, bool idempotent_add,
                                    uint8_t cur_tree_version, int64_t base_ts);

    static Status InsertFeatureStatWithMaxTsAndMinTs(CmdContext* ctx,
                                      model::IpsModel* ordered_tree,
                                      int64_t max_ts, int64_t min_ts,
                                      const FeatureStat32& fs, TableType table_type,
                                      ReduceType reduce_type, bool idempotent_add,
                                      uint8_t cur_tree_version, int64_t base_ts);

    static Status TruncateTreeDataByTimeRange(CmdContext* ctx, ReduceType reduce_type,
                                              model::IpsModel* ordered_tree, int64_t base_ts,
                                              uint8_t tree_version, int64_t min_snap_ts_micros,
                                              int64_t max_snap_ts_micros, uint64_t max_snap_cnt);

    static Status TruncateTreeDataByCount(CmdContext* ctx, ReduceType reduce_type,
                                          model::IpsModel* ordered_tree, int64_t base_ts,
                                          uint8_t tree_version, uint64_t trigger_cnt,
                                          uint64_t reserve_cnt);

    static Status CompressCompact(CmdContext* ctx, model::IpsModel* ordered_tree, int64_t base_ts,
                                  uint8_t tree_version, const TimeDimension& time_dimension,
                                  ReduceType reduce_type, TableType table_type,
                                  const TagList& cur_tag,
                                  CompressCompactType compress_compact_type);

    static bool DecodeIpsData(const char* encode_key, const char* encode_val, size_t val_size,
                              uint8_t tree_version, int64_t base_ts, ReduceType reduce_type,
                              int64_t* min_ts, std::vector<int64_t>* feature_vec);

    static Status GetTreeMinKeyTs(int64_t* min_ts, model::IpsModel* ordered_tree, int64_t base_ts,
                                  uint8_t tree_version, ReduceType reduce_type);

    static Status GetTreeMaxKeyTs(int64_t* max_ts, model::IpsModel* ordered_tree,
                                  uint8_t tree_version);

    static bool IsInvalidTreeKV(const Slice& key, const Slice& value, uint8_t tree_version);

    static Status GetQueryTreeVersionAndBaseTs(model::IpsModel* ordered_tree, uint8_t* tree_version,
                                               int64_t* base_ts);

    static Status GetInsertTreeVersionAndBaseTs(ReduceType reduce_type,
                                                model::IpsModel* ordered_tree, int64_t insert_ts,
                                                uint8_t* tree_version, int64_t* base_ts);

    static inline void CompactReachEndHandler(int64_t* compact_start_ts,
                                              int32_t* compact_range_index, int64_t max_ts) {
        int64_t cur_ts = GetCurTsMicros();
        *compact_start_ts = std::min(cur_ts, max_ts + 1);
        *compact_range_index = 0;
    }

    static Status TtlHandler(CmdContext* ctx, model::IpsModel* ordered_tree,
                             int64_t min_valid_ts_us, int64_t* ttl_cnt, int64_t max_scan_ttl);

 private:
    struct IPSSlice {
     public:
        explicit IPSSlice(std::string str) {
            str_ = std::move(str);
            slice_ = Slice(str_);
        }

        explicit IPSSlice(const Slice& slice) {
            str_ = slice.ToString();
            slice_ = Slice(str_);
        }

        explicit IPSSlice(IPSSlice&& other) {
            str_ = std::move(other.str_);
            slice_ = Slice(str_);
        }

        IPSSlice& operator=(IPSSlice&& other) {
            str_ = std::move(other.str_);
            slice_ = Slice(str_);

            return *this;
        }

        std::string ToString(bool hex = false) const { return str_; }

        size_t size() const { return str_.size(); }

        const Slice& GetSlice() const { return slice_; }

        explicit IPSSlice(const IPSSlice& other) = delete;
        IPSSlice& operator=(const IPSSlice& other) = delete;

     private:
        std::string str_;
        Slice slice_;
    };

    static std::string EncodeIpsData(ReduceType reduce_type, int64_t insert_ts, int64_t base_ts,
                                     const std::vector<int64_t>& feature_vec);

    static std::string GetInt56BigEndianStringVal(int64_t value);

    static std::string GetRangeStrKey(int64_t int_key);

    static std::string GenerateIpsTreeKey(int64_t ts, int64_t fid);

    // call by truncate by count compct
    static Status RangeGetByCnt(ReduceType reduce_type, model::IpsModel* ordered_tree,
                                int64_t base_ts, uint8_t tree_version, int64_t min_data_ts_micros,
                                uint64_t cnt, std::vector<Slice>* res);

    static Status TreeValMerge(const Slice& old_key, const Slice& old_val, const Slice& insert_key,
                               const Slice& insert_val, std::string* merge_res,
                               ReduceType reduce_type, int64_t base_ts, uint8_t tree_version);

    static Status TreeKeyMerge(const Slice& key1, const Slice& key2, std::string* merged_res);

    static Status CompactDataMerge_(ReduceType reduce_type, int64_t base_ts, uint8_t tree_version,
                                    const std::vector<std::pair<Slice, Slice>>& compact_data,
                                    std::vector<std::pair<IPSSlice, IPSSlice>>* compact_res,
                                    std::vector<IPSSlice>* kv_gc);

    static Status CompactResHandle(CmdContext* ctx,
                                   const std::vector<std::pair<IPSSlice, IPSSlice>>& compact_res,
                                   const std::vector<IPSSlice>& gc_set,
                                   model::IpsModel* ordered_tree);

    static Status GetCurCompactTsRange(const TimeDimension& time_dimension,
                                       model::IpsModel* ordered_tree, IpsTimeRange* dimension_range,
                                       int64_t min_ts, int64_t max_ts, int64_t* compact_start_ts,
                                       int32_t* compact_range_index);

    static Status DataMerge(int64_t val1, int64_t val2, size_t index, size_t max_size,
                            ReduceType reduce_type, int64_t* merged_res);

    static Status UptateTreeValToExpectedVersion(model::IpsModel* ordered_tree,
                                                 uint8_t tree_version, int64_t base_ts,
                                                 ReduceType reduce_type);

    static bool DecodeIpsVersionOneData(const char* encode_key, const char* encode_val,
                                        size_t val_size, uint8_t tree_version, int64_t base_ts,
                                        ReduceType reduce_type, int64_t* min_ts,
                                        std::vector<int64_t>* feature_vec);

    static bool DecodeIpsVersionZeroData(const char* encode_val, uint8_t tree_version,
                                         int64_t* min_ts, std::vector<int64_t>* feature_vec);

    static Status GetTreeValTs(ReduceType reduce_type, const Slice& tree_key,
                               const Slice& tree_value, int64_t base_ts, uint8_t tree_version,
                               int64_t* val_ts);

    static std::string EncodeIpsDataVerisonOne(ReduceType reduce_type, int64_t insert_ts,
                                               int64_t base_ts,
                                               const std::vector<int64_t>& feature_vec);

    static std::string EncodeIpsDataVerisonZero(int64_t ts,
                                                const std::vector<int64_t>& feature_vec);

    static Status UpdateTreeMetaFromZeroVersionToOne(CmdContext* ctx, model::IpsModel* ordered_tree,
                                                     uint8_t cur_version, int64_t base_ts);
    static Status UpdateTreeMetaFromOneVersionToZero(CmdContext* ctx, model::IpsModel* ordered_tree,
                                                     uint8_t cur_version);
    static Status UpdateTreeMetaToExpectedVersion(CmdContext* ctx, model::IpsModel* ordered_tree,
                                                  uint8_t cur_tree_version, int64_t base_ts);

    static Status CheckAndUpdateBaseTsMeta(CmdContext* ctx, model::IpsModel* ordered_tree,
                                           uint8_t cur_tree_version, int64_t base_ts);
    static size_t GetVersionOneTreeValTs(ReduceType reduce_type, uint8_t tree_version,
                                         int64_t base_ts, const char* encode_key,
                                         const char* encode_val, int64_t* ts);

    static Status CompressCompactOnce(CmdContext* ctx, model::IpsModel* ordered_tree,
                                      int64_t base_ts, uint8_t tree_version,
                                      const TimeDimension& time_dimension, ReduceType reduce_type,
                                      TableType table_type, const TagList& cur_tag);

    static void GetTreeKvMinAndMaxTs(ReduceType reduce_type, int64_t base_ts, uint8_t tree_version,
                                     const Slice& tree_k, const Slice& tree_val, int64_t* min_ts,
                                     int64_t* max_ts);

    static int64_t GetTreeKvMaxTs(const Slice& tree_k, const Slice& tree_val) {
        return DecodeInt56FromBigEndian(tree_k.data());
    }

 public:
    template <typename T>
    static void SortByWeightTopk(std::vector<IPSFeatureData>* range_res, int64_t top_k,
                                 const SortContext& context) {
        int64_t feature_size = range_res->size();
        if (top_k >= feature_size) {
            return;
        }

        T sort_helper;
        sort_helper.SetSortContext(context);
        std::vector<std::pair<IPSFeatureData, int64_t>> sort_data;
        sort_data.reserve(feature_size);
        for (auto iter = range_res->begin(); iter != range_res->end(); ++iter) {
            IPSFeatureData& cur_data = *iter;
            int64_t cur_weight = sort_helper.GetCurrentFeatureSortWeight(cur_data);
            sort_data.emplace_back(std::make_pair(std::move(cur_data), cur_weight));
        }

        range_res->clear();
        // get topk feature
        std::nth_element(sort_data.begin(), sort_data.begin() + top_k, sort_data.end(),
                         CustomsizeComparator<std::pair<IPSFeatureData, int64_t>>());
        for (int64_t i = 0; i < top_k; ++i) {
            range_res->emplace_back(std::move(sort_data[i].first));
        }
    }
};

template <typename T>
static Status DoShrink(CmdContext* ctx, ShrinkSortContext context, model::IpsModel* ordered_tree,
                       int64_t tree_size, int64_t start_cnt, int64_t reserve_cnt,
                       const TagList& cur_tag) {
    assert(tree_size > reserve_cnt);
    assert(reserve_cnt > start_cnt);
    int64_t topk_reserve = reserve_cnt - start_cnt;
    T sort_helper;
    sort_helper.SetSortContext(context);

    std::vector<std::pair<Slice, int64_t>> shrink_scan_vec;
    shrink_scan_vec.reserve(tree_size - start_cnt + 1);
    int64_t tmp_start_cnt = start_cnt;

    uint8_t tree_version = context.tree_version;
    PersistentMap<std::string, std::string>::IterateFunc shrink_iter =
        [&shrink_scan_vec, &start_cnt, &sort_helper, tree_version](
            const std::string& key_str, const std::string& value_str) -> bool {
        const Slice key(key_str), value(value_str);
        if (UNLIKELY(IPSOperator::IsInvalidTreeKV(key, value, tree_version))) {
            return true;
        }

        if (start_cnt > 0) {
            --start_cnt;
            return true;
        }
        int64_t cur_weight = sort_helper.GetCurrentFeatureSortWeight(key, value);
        shrink_scan_vec.emplace_back(key, cur_weight);
        return true;
    };
    TimeCost tc_shrink;
    Status ret = ordered_tree->OrSet().ScanBackward("", shrink_iter);
    if (UNLIKELY(!ret.ok())) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("tree scan failed, ret: {}", ret.ToString());
        return ret;
    }
    assert(shrink_scan_vec.size() + tmp_start_cnt == tree_size);
    // Metrics::GetInstance()->Emit<kMetricTimer>("shrink.collect_latency", tc_shrink.GetElapsed(),
    // cur_tag,
    //                                            FLAGS_metrics_sample);
    // Metrics::GetInstance()->Emit<kMetricTimer>("shrink.collect_size", shrink_scan_vec.size(),
    // cur_tag,
    //                                            FLAGS_metrics_sample);
    FindShrinkTopkMax(shrink_scan_vec.begin(), topk_reserve, shrink_scan_vec.end());
    tc_shrink.Reset();
    for (size_t i = topk_reserve; i < shrink_scan_vec.size(); ++i) {
        ret = ordered_tree->OrSet().Del(ctx, shrink_scan_vec[i].first.ToString());
        if (UNLIKELY(!ret.ok())) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("batch delete tree key failed, tree_root: {}, ret: {}",
            //                             ordered_tree->OrSet().GetTreeRootKey(), ret.ToString());
            return ret;
        }
    }
    // Metrics::GetInstance()->Emit<kMetricTimer>("shrink.data_delete_latency",
    // tc_shrink.GetElapsed(),
    //                                            cur_tag, FLAGS_metrics_sample);
    // Metrics::GetInstance()->Emit<kMetricTimer>(
    //     "shrink.delete_kv_num", shrink_scan_vec.size() - topk_reserve, cur_tag,
    //     FLAGS_metrics_sample);
    return ret;
}

template <typename T>
static Status Shrink(CmdContext* ctx, SlotID slot, int64_t reserved_cnt,
                     model::IpsModel* ordered_tree, const ProfileTableSchema& table_schema,
                     const TagList& cur_tag) {
    if (table_schema.GetReduceType() == ReduceType::IP_REDUCE_NONE) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("enable shrink when reduce_func is none. table_name: {}",
        //                             table_schema.TableName());
        // TODO(chenyanbin): change GetCurrentFeatureSortWeight impl logic to support shrink when
        // reduce func is none
        return Status::Unimplemented("invalid table conf: shrink is true but reduce_type is none");
    }

    double start_ratio = table_schema.ProtectedLatestFidRatio();

    if (UNLIKELY(reserved_cnt < 0 || start_ratio > 1 || start_ratio < 0)) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("invalid shrink arg, reserved_cnt: {}, start_ratio: {}",
        // reserved_cnt,
        //                             start_ratio);
        return Status::InvalidArgument("invalid shrink conf");
    }

    Status ret;
    if (UNLIKELY(reserved_cnt == 0)) {
        ret = ordered_tree->OrSet().DeleteTree();
        if (UNLIKELY(!ret.ok())) {
            LOG_ERROR("TreeDelete failed").put("ErrorMsg", ret.ToString());
            // BC_ERROR_DEFAULT_RATE_LIMIT("TreeDelete failed, ret: {}", ret.ToString());
        }
        return ret;
    }

    std::vector<uint64_t> feature_index, feature_weight;
    const auto& slot_manager = table_schema.GetSlotManager();
    slot_manager.GetSlotIndexAndWeightConf(slot, &feature_index, &feature_weight);
    ShrinkSortContext context;
    context.feature_index = &feature_index;
    context.feature_weight = &feature_weight;
    context.sort_by_vx = table_schema.GetSortKeyIndex();
    context.table_type = table_schema.GetTableType();

    uint8_t tree_version;
    int64_t base_ts;
    ret = IPSOperator::GetQueryTreeVersionAndBaseTs(ordered_tree, &tree_version, &base_ts);
    if (!ret.ok()) {
        return ret;
    }
    context.tree_version = tree_version;

    uint64_t tree_size = 0;
    ret = ordered_tree->OrSet().Size(&tree_size);
    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Get tree size failed").put("ErrorMsg", ret.ToString());
        // BC_ERROR_DEFAULT_RATE_LIMIT("get tree size failed, ret: {}", ret.ToString());
        return ret;
    }
    if (UNLIKELY(tree_size <= reserved_cnt)) {
        LOG_WARNING("shrink no action").put("TreeSize", tree_size)
            .put("ReservedCnt", reserved_cnt);
        // BC_WARN_DEFAULT_RATE_LIMIT("shrink no action, tree-size: {}, reserved_cnt: {},
        // table_name: {}",
        //                            tree_size, reserved_cnt, table_schema.TableName());
        return Status::NoAction("");
    }
    int64_t start_cnt = (int64_t)(reserved_cnt * start_ratio);
    return DoShrink<T>(ctx, context, ordered_tree, tree_size, start_cnt, reserved_cnt, cur_tag);
}

std::string Instance2IPSKey(std::string ips_table, const ips::Instance& ins);

}  // namespace ips
}  // namespace bcache2
