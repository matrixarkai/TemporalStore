// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "model/ips/ips_interface.h"

#include <byte/include/macros.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/slice.h"
#include "common/status.h"
#include "common/logging.h"

#include "model/ips/ips_define.h"
#include "model/ips/ips_operator.h"
#include "model/ips/random.h"
#include "model/ips/utils.h"
#include "partition/cmd_context.h"
#include "partition/compute/cmd.h"

namespace bcache2 {
namespace ips {
using partition::CmdContext;
namespace {

static const int64_t kFidBitNum = 64;
static const int64_t kSlotBitNum = 10;
// 2天的微秒数
static const int64_t kMicrosOfTwoDays = (int64_t)2 * 24L * 60 * 60 * 1000 * 1000;
static const int64_t kMicrosOfFiveYear = (int64_t)5 * 365 * 24L * 60 * 60 * 1000 * 1000;
}  // namespace

Status IPSInterface::GetQueryTimeRange(model::IpsModel* ordered_tree, uint8_t tree_version,
                                       const ips::DataRange& data_range, int64_t* start_micros,
                                       int64_t* end_micros) {
    Status ret;
    switch (data_range.type()) {
    // 使用绝对时间（墙上时钟）
    case DataRangeType::ABSOLUTE_TIME_MICROS:
        *end_micros = GetCurTsMicros();
        *start_micros = *end_micros - data_range.range_val();
        *start_micros = *start_micros < 0 ? 0 : *start_micros;
        break;
    // 使用相对时间，相对整个用户所有slot、action_type最后一次写入的时间
    case DataRangeType::RELATIVE_TIME_MICROS:
    // 使用相对时间，相对当前用户指定slot、action_type最后一次写入的时间
    case DataRangeType::RELATIVE_TIME_MICROS_ACTION_TYPE: {
        ret = IPSOperator::GetTreeMaxKeyTs(end_micros, ordered_tree, tree_version);
        if (UNLIKELY(!ret.ok())) {
            return ret;
        }
        *end_micros += 1;
        *start_micros = *end_micros - data_range.range_val();
        if (*start_micros < 0) {
            *start_micros = 0;
        }
        break;
    }
    // 最后写入的若干个instance
    case DataRangeType::LAST_INSTANCES:
        return Status::NoAction("");
    // 指定起始时间，收集指定时间范围内的snap
    case DataRangeType::START_TIME_MICROS:
        *start_micros = data_range.start_time_micros();
        *end_micros = *start_micros + data_range.range_val();
        break;
    default:
        return Status::InvalidArgument("invalid data_range.type val");
    }
    return Status::OK();
}

template <typename T>
inline Status IPSInterface::RangeByWeightHandler(
    const QueryRequest& req, int64_t top_k, const SortContext& context,
    std::vector<IPSFeatureData>* query_result, model::IpsModel* ordered_tree, int64_t* range_min_ts,
    int64_t* range_max_ts, ReduceType reduce_type, uint8_t tree_version, int64_t base_ts) {
    query_result->clear();
    int64_t start_micros = 0, end_micros = 0;
    Status ret =
        GetQueryTimeRange(ordered_tree, tree_version, req.data_range(), &start_micros, &end_micros);
    if (ret.ok()) {  // time range query
        ret = IPSOperator::RangeGet(ordered_tree, base_ts, tree_version, start_micros, end_micros,
                                    query_result, false, range_min_ts, range_max_ts, reduce_type);
    } else if (ret.IsNoAction()) {  // last instance query
        ret =
            IPSOperator::RangeGet(ordered_tree, base_ts, tree_version, req.data_range().range_val(),
                                  query_result, range_min_ts, range_max_ts, reduce_type);
    } else {
        LOG_ERROR("GetQueryTimeRange Failed").put("ErrorMsg", ret.ToString());
        return ret;
    }

    if (UNLIKELY(!ret.ok())) {
        if (!ret.IsNotFound()) {
            LOG_ERROR("Range get failed").put("ErrorMsg", ret.ToString());
        }
        return ret;
    } else {
        IPSOperator::SortByWeightTopk<T>(query_result, top_k, context);
        return Status::OK();
    }
}

Status IPSInterface::FilterByVxHandler(const QueryRequest& req, int64_t top_k,
                                       std::vector<IPSFeatureData>* query_result,
                                       model::IpsModel* ordered_tree, int64_t* range_min_ts,
                                       int64_t* range_max_ts, ReduceType reduce_type,
                                       uint8_t tree_version, int64_t base_ts) {
    auto const& filter = req.filter();
    if (!filter.has_filter_by_vx() || !filter.has_filter_by_vx_value()) {
        LOG_ERROR("filter_by_vx or filter_by_vx_value is not set")
            .put("filter_by_vx", filter.filter_by_vx())
            .put("filter_by_vx_value", filter.filter_by_vx_value());
        return Status::InvalidArgument("filter_by_vx or filter_by_vx_value is not set");
    }
    int64_t vx = filter.filter_by_vx();
    int64_t val = filter.filter_by_vx_value();

    query_result->clear();
    int64_t start_micros = 0, end_micros = 0;
    Status ret =
        GetQueryTimeRange(ordered_tree, tree_version, req.data_range(), &start_micros, &end_micros);
    if (ret.ok()) {  // time range query
        ret = IPSOperator::RangeGetFilterByVx(ordered_tree, base_ts, tree_version, start_micros,
                                              end_micros, top_k, vx, val, query_result,
                                              range_min_ts, range_max_ts, reduce_type);
    } else if (ret.IsNoAction()) {  // last instance query
        ret =
            IPSOperator::RangeGetFilterByVx(ordered_tree, base_ts, tree_version, top_k, vx, val,
                                            query_result, range_min_ts, range_max_ts, reduce_type);
    } else {
        LOG_ERROR("Get Query Time Range failed").put("ErrorMsg", ret.ToString());
        return ret;
    }

    if (UNLIKELY(!ret.ok())) {
        if (!ret.IsNotFound()) {
            LOG_ERROR("Range get failed tree key").put("ErrorMsg", ret.ToString());
            // BC_ERROR_DEFAULT_RATE_LIMIT("range_get failed, tree_key: {}, ret: {}",
            //                             ordered_tree->OrSet().GetTreeRootKey(), ret.ToString());
        }
    }
    return ret;
}

inline Status IPSInterface::FilterByFidHandler(const QueryRequest& req, const Filter& filter,
                                               const SortContext& context,
                                               std::vector<IPSFeatureData>* query_result,
                                               model::IpsModel* ordered_tree, int64_t* range_min_ts,
                                               int64_t* range_max_ts, ReduceType reduce_type,
                                               uint8_t tree_version, int64_t base_ts) {
    if (UNLIKELY(!filter.has_fid_list() || filter.fid_list().empty())) {
        LOG_WARNING("Query invailed parameter, filter.fid_list is not set");
        return Status::InvalidArgument("fid_list is empty or fid_list is not set");
    }

    query_result->clear();
    std::unordered_set<int64_t> fid_set(filter.fid_list().begin(), filter.fid_list().end());
    int64_t start_micros = 0, end_micros = 0;
    Status ret =
        GetQueryTimeRange(ordered_tree, tree_version, req.data_range(), &start_micros, &end_micros);
    if (ret.ok()) {  // time range
        return IPSOperator::RangeGetFilterByFid(ordered_tree, base_ts, tree_version, start_micros,
                                                end_micros, fid_set, query_result, range_min_ts,
                                                range_max_ts, reduce_type);
    } else if (ret.IsNoAction()) {  // last instance query
        return IPSOperator::RangeGetFilterByFid(ordered_tree, base_ts, tree_version,
                                                req.data_range().range_val(), fid_set, query_result,
                                                range_min_ts, range_max_ts, reduce_type);
    } else {
        LOG_ERROR("GetQueryTimeRange failed").put("ErrorMsg", ret.ToString());
        return ret;
    }
}

inline Status IPSInterface::RangeByTsHandler(const QueryRequest& req, int64_t top_k,
                                             const Filter& filter, const SortContext& context,
                                             std::vector<IPSFeatureData>* query_result,
                                             model::IpsModel* ordered_tree, int64_t* range_min_ts,
                                             int64_t* range_max_ts, ReduceType reduce_type,
                                             uint8_t tree_version, int64_t base_ts) {
    int64_t start_micros = 0, end_micros = 0;
    Status ret =
        GetQueryTimeRange(ordered_tree, tree_version, req.data_range(), &start_micros, &end_micros);
    if (UNLIKELY(!ret.IsNoAction() && !ret.ok())) {
        LOG_ERROR("GetQueryTimeRnage failed").put("ErrorMsg", ret.ToString());
        return ret;
    }

    if (!filter.has_min_index_count() || !filter.has_index()) {
        if (ret.IsNoAction()) {
            // last instance query
            assert(req.data_range().type() == DataRangeType::LAST_INSTANCES);
            int64_t cnt = req.data_range().range_val();
            ret = IPSOperator::RangeGetLastCntEle(reduce_type, ordered_tree, base_ts, tree_version,
                                                  query_result, req.data_range().range_val(),
                                                  range_min_ts, range_max_ts);
        } else {
            ret =
                IPSOperator::RangeGet(ordered_tree, base_ts, tree_version, start_micros, end_micros,
                                      query_result, true, range_min_ts, range_max_ts, reduce_type);
        }
        if (UNLIKELY(!ret.ok())) {
            if (!ret.IsNotFound()) {
                LOG_ERROR("range get failed").put("ErrorMsg", ret.ToString());
            }
            return ret;
        } else {
            IPSOperator::ReserveKElements(query_result, top_k,
                                          req.data_range().type() == DataRangeType::LAST_INSTANCES);
            return Status::OK();
        }
    } else {
        std::vector<int32_t> index_strong(req.filter().index().begin(), req.filter().index().end());
        std::vector<int32_t> min_index_count(req.filter().min_index_count().begin(),
                                             req.filter().min_index_count().end());
        if (UNLIKELY(index_strong.size() != min_index_count.size())) {
            LOG_ERROR("filter.index != filter.min_index_count")
                    .put("MinIndexCountSize", min_index_count.size())
                    .put("IndexStrongSize", index_strong.size());
            return Status::InvalidArgument("filter.index != filter.min_index_count");
        }

        for (int32_t index : index_strong) {
            if (UNLIKELY(index < 0)) {
                LOG_ERROR("Index strong include <0 index");
                return Status::InvalidArgument("filter.index exist < 0 value");
            }
        }

        for (int32_t count : min_index_count) {
            if (UNLIKELY(count <= 0)) {
                LOG_ERROR("Min_index_count include <0 count");
                return Status::InvalidArgument("filter.min_index_count exist <= 0 value");
            }
        }
        if (ret.IsNoAction()) {
            // last instance query
            return IPSOperator::RangeGetStrongFilter(
                ordered_tree, base_ts, tree_version, INT64_MIN, INT64_MAX, &index_strong,
                &min_index_count, top_k, query_result, range_min_ts, range_max_ts, reduce_type);
        } else {  // time range query
            return IPSOperator::RangeGetStrongFilter(
                ordered_tree, base_ts, tree_version, start_micros, end_micros, &index_strong,
                &min_index_count, top_k, query_result, range_min_ts, range_max_ts, reduce_type);
        }
    }
}

bool IPSInterface::CheckParameterForCustomizeWeight(const Filter& filter, TableType table_type) {
    if (!filter.has_feature_indexes() || !filter.has_feature_weights() ||
        filter.feature_indexes().size() == 0 ||
        filter.feature_indexes().size() != filter.feature_weights().size()) {
        // BC_ERROR_DEFAULT_RATE_LIMIT(
        //     "feature_indexes' size {} is not equal to feature_weights"
        //     "size {} or feature_indexes or feature_weights is not set",
        //     filter.feature_indexes.size(), filter.feature_weights.size());
        return false;
    }
    for (int idx = 0; idx < filter.feature_indexes_size(); idx++) {
        auto index = filter.feature_indexes(idx);
        if (UNLIKELY(index < 0)) {
            // BC_ERROR_DEFAULT_RATE_LIMIT(
            //    "feature_indexes {} is less than zero", index);
            return false;
        } else if (table_type == TableType::PAIR && index >= 2) {
            // BC_ERROR_DEFAULT_RATE_LIMIT(
            //    "feature_indexes {} is grate than 2 in pair table", index);
            return false;
        }
    }

    for (int idx = 0; idx < filter.feature_weights_size(); idx++) {
        auto weight = filter.feature_weights(idx);
        if (UNLIKELY(weight < 0)) {
            // BC_ERROR_DEFAULT_RATE_LIMIT(
            //    "feature_weight {} must greater or equal than 0",weight);
            return false;
        }
    }

    return true;
}
FeatureStatOperator IPSInterface::GetSortOptor(const std::string& optor) {
    if (optor == "id") {
        return FeatureStatOperator::SORT_BY_ID;
    } else if (optor == "v1") {
        return FeatureStatOperator::SORT_BY_V1;
    } else if (optor == "v2") {
        return FeatureStatOperator::SORT_BY_V2;
    } else if (optor == "ratio") {
        return FeatureStatOperator::SORT_BY_RATIO;
    } else if (optor == "vx") {
        return FeatureStatOperator::SORT_BY_VX;
    } else if (optor == "customize") {
        return FeatureStatOperator::SORT_BY_CUSTOMIZE_WEIGHT;
    } else if (optor == "wilson") {
        return FeatureStatOperator::SORT_BY_WILSON_SCORE;
    } else {
        LOG_ERROR("Get_sort_optor get invalid sort optor").put("optor", optor);
        return FeatureStatOperator::SORT_BY_V1;
    }
}

Status IPSInterface::SubQueryReqHandler(const QueryRequest& req, QueryResponse* rsp,
                                        model::IpsModel* ordered_tree, TableType table_type,
                                        ReduceType reduce_type, uint8_t tree_version,
                                        int64_t base_ts) {
    const Filter& filter = req.filter();
    int32_t top_k = filter.top_k();
    if (UNLIKELY(top_k <= 0)) {
        if (filter.optor() == FeatureStatOperator::SORT_BY_TS && filter.has_top_k_fid()) {
            top_k = std::max(top_k, filter.top_k_fid());
        } else if (filter.optor() != FeatureStatOperator::FILTER_BY_FID) {
            LOG_ERROR("invalid filter").put("topk", filter.top_k())
                .put("top_k_fid", filter.top_k_fid())
                .put("optor", filter.optor());
            return Status::InvalidArgument("invalid topk value: top_k need > 0");
        }
    }

    SortContext context;
    FeatureStatOperator optor = filter.optor();
    std::vector<IPSFeatureData> query_result;
    int64_t range_min_ts = 0, range_max_ts = 0;
    Status ret;
    switch (optor) {
    case FeatureStatOperator::SORT_BY_V1:
        IPS_DATA_TYPE_CHECK(table_type, TableType::PAIR, Status::Unmatched("DataTypeConflict"));
        ret = RangeByWeightHandler<GetSortWeightV1>(req, top_k, context, &query_result,
                                                    ordered_tree, &range_min_ts, &range_max_ts,
                                                    reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::SORT_BY_V2:
        IPS_DATA_TYPE_CHECK(table_type, TableType::PAIR, Status::Unmatched("DataTypeConflict"));
        ret = RangeByWeightHandler<GetSortWeightV2>(req, top_k, context, &query_result,
                                                    ordered_tree, &range_min_ts, &range_max_ts,
                                                    reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::SORT_BY_VX:
        IPS_DATA_TYPE_CHECK(table_type, TableType::LIST, Status::Unmatched("DataTypeConflict"));
        if (filter.has_sort_by_vx()) {
            if (UNLIKELY(filter.sort_by_vx() < 0)) {
                std::string msg =
                    fmt::format("invalid filter.sort_by_vx value: {}", filter.sort_by_vx());
                LOG_ERROR(msg.c_str());
                return Status::InvalidArgument(msg);
            } else {
                context.sort_by_vx = filter.sort_by_vx();
            }
        } else {
            context.sort_by_vx = 0;
        }
        ret = RangeByWeightHandler<GetSortWeightVX>(req, top_k, context, &query_result,
                                                    ordered_tree, &range_min_ts, &range_max_ts,
                                                    reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::SORT_BY_RATIO:
        IPS_DATA_TYPE_CHECK(table_type, TableType::PAIR, Status::Unmatched("DataTypeConflict"));
        ret = RangeByWeightHandler<GetSortWeightRatio>(req, top_k, context, &query_result,
                                                       ordered_tree, &range_min_ts, &range_max_ts,
                                                       reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::SORT_BY_ID:
        ret = RangeByWeightHandler<GetSortWeightID>(req, top_k, context, &query_result,
                                                    ordered_tree, &range_min_ts, &range_max_ts,
                                                    reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::SORT_BY_TS:
        ret = RangeByTsHandler(req, top_k, filter, context, &query_result, ordered_tree,
                               &range_min_ts, &range_max_ts, reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::FILTER_BY_VX:
        ret = FilterByVxHandler(req, top_k, &query_result, ordered_tree, &range_min_ts,
                                &range_max_ts, reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::SORT_BY_CUSTOMIZE_WEIGHT:
        if (UNLIKELY(!CheckParameterForCustomizeWeight(req.filter(), table_type))) {
            LOG_ERROR("not support SORT_BY_CUSTOMIZE_WEIGHT");
            return Status::InvalidArgument("InvalidArgument");
        } else {
            // context.feature_index = &(filter.feature_indexes);
            // context.feature_weight = &(filter.feature_weights);
        }
        ret = RangeByWeightHandler<GetSortWeightCustomizeWeight>(
            req, top_k, context, &query_result, ordered_tree, &range_min_ts, &range_max_ts,
            reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::FILTER_BY_FID:
        ret = FilterByFidHandler(req, filter, context, &query_result, ordered_tree, &range_min_ts,
                                 &range_max_ts, reduce_type, tree_version, base_ts);
        break;
    case FeatureStatOperator::SORT_BY_WILSON_SCORE:
        IPS_DATA_TYPE_CHECK(table_type, TableType::PAIR, Status::Unmatched("DataTypeConflict"));
        if (filter.has_wilson_z()) {
            if (filter.wilson_z() > 0) {
                context.wilson_z = filter.wilson_z();
            }
        }
        ret = RangeByWeightHandler<GetSortWeightWilson>(req, top_k, context, &query_result,
                                                        ordered_tree, &range_min_ts, &range_max_ts,
                                                        reduce_type, tree_version, base_ts);
        break;
    default:
        std::string msg = fmt::format("invalid filter.optor value: {}", static_cast<int>(optor));
        LOG_ERROR(msg.c_str());
        return Status::InvalidArgument(msg);
    }

    if (LIKELY(ret.ok() || ret.IsNotFound())) {
        ret =
            BuildSubQueryResponse(&query_result, rsp, req, table_type, range_min_ts, range_max_ts);
        return ret;
    } else {
        return ret;
    }
}

Status IPSInterface::GetFeatureStat(FeatureStat32* fs, IPSFeatureData* feature_data,
                                    TableType table_type) {
    fs->set_type(table_type);
    fs->set_id(feature_data->GetFid());

    switch (table_type) {
    case IP_INT_PAIR: {
        size_t feature_size = feature_data->GetFeatureDataSize();
        if (feature_size > 2) {
            LOG_ERROR("get invalid feature_stat list in pair ips table")
            .put("List size", feature_size);
            return Status::Unmatched("DataTypeConflict");
        }
        fs->mutable_int_pair()->set_v1(feature_data->GetDataAtIndex(0));
        fs->mutable_int_pair()->set_v2(feature_data->GetDataAtIndex(1));
        fs->mutable_int_pair()->set_timestamp(feature_data->GetMaxTs());
        break;
    }
    case IP_INT_LIST: {
        std::vector<int64_t>* feature_data_vec = feature_data->GetMutableFeatureDataVec();
        *fs->mutable_int_list()->mutable_v_list() = {feature_data_vec->begin(),
                                                     feature_data_vec->end()};

        fs->mutable_int_list()->set_timestamp(feature_data->GetMaxTs());
        break;
    }
    default:
        LOG_ERROR("get invalid feature_stat type").put("TableType", table_type);
        return Status::InvalidArgument("InvalidArgument");
    }
    return Status::OK();
}

Status IPSInterface::BuildSubQueryResponse(std::vector<IPSFeatureData>* query_result,
                                           QueryResponse* rsp, const QueryRequest& req,
                                           TableType table_type, int64_t range_min_ts,
                                           int64_t range_max_ts) {
    rsp->set_uid(req.uid());
    if (req.has_decoupled() && req.decoupled()) {
        *(rsp->mutable_query_filter()) = req.filter();
        *(rsp->mutable_query_range()) = req.data_range();
    }

    const size_t rsp_len = query_result->size();
    if (rsp_len == 0) {
        rsp->set_start_ts(0);
        rsp->set_end_ts(0);

        rsp->clear_feature_stat32_list();
        // Metrics::GetInstance()->Emit<kMetricCounter>("query.not_found.qps", 1,
        // FLAGS_metrics_sample);
        return Status::NotFound("not found");
    }

    rsp->set_start_ts(range_min_ts);
    rsp->set_end_ts(range_max_ts);

    std::vector<FeatureStat32> rst_list;

    LOG_DEBUG("IPS rsp len").put("Len", rsp_len);
    const auto& filter = req.filter();

    SnapsInfo* snaps_info = nullptr;
    if (filter.optor() == FeatureStatOperator::SORT_BY_TS && filter.has_need_snaps_info() &&
        filter.need_snaps_info()) {
        snaps_info = rsp->mutable_snaps_info();
    }

    for (size_t i = 0; i < rsp_len; ++i) {
        IPSFeatureData& data = (*query_result)[i];
        if (snaps_info != nullptr) {
            snaps_info->add_snaps_start_ts(data.GetMinTs());
            snaps_info->add_snaps_end_ts(data.GetMaxTs());
            snaps_info->add_snaps_fid_cnt(1);
        }

        rst_list.emplace_back();
        FeatureStat32* cur_fs = &(rst_list.back());
        cur_fs->set_slot(req.filter().slot());
        Status ret = GetFeatureStat(cur_fs, &data, table_type);
        if (UNLIKELY(!ret.ok())) {
            rst_list.pop_back();
        }
    }
    rsp->mutable_feature_stat32_list()->CopyFrom({rst_list.begin(), rst_list.end()});
    return Status::OK();
}

inline int64_t IPSInterface::GetSlotReserveSize(const ProfileTableSchema& schema, SlotID slot) {
    int64_t max_size = 0;
    if (schema.OpenShrink()) {
        Status ret = schema.GetSlotManager().GetSlotCntLimit(slot, &max_size);
        if (!ret.ok()) {
            max_size = 1000;
        }
    }
    return max_size;
}

bool IPSInterface::ValidInstance(const ips::Instance& ins, int64_t cur_ts) {
    // 五年前的ts || 超前当前时间2天以上的ts视为非法
    // 空instance也视为非法
    return ins.ts() > cur_ts - kMicrosOfFiveYear && ins.ts() < cur_ts + kMicrosOfTwoDays &&
           ins.feature_stat32_list_size() != 0;
}

bool IPSInterface::ValidInstance(const ips::LoadInstance& ins, int64_t cur_ts) {
    // 五年前的ts || 超前当前时间2天以上的ts视为非法
    // 空instance也视为非法
    return ins.max_ts() > cur_ts - kMicrosOfFiveYear && ins.max_ts() < cur_ts + kMicrosOfTwoDays &&
           ins.feature_stat32_list_size() != 0;
}

// require: all FeatureStat32 in ins_list must has same
// uid, slot, table, action_type parameter

Status IPSInterface::Add(CmdContext* ctx, const std::vector<ips::Instance>& ins_list,
                         bool idempotent_add, model::IpsModel* ordered_tree,
                         const ProfileTableSchema& schema, bool is_replace) {
    // assert(hf_mgr != nullptr);
    if (UNLIKELY(ins_list.empty())) {
        return Status::OK();
    }
    int64_t uid = ins_list[0].uid();
    SlotID slot = CalcSlot(ins_list[0].feature_stat32_list(0));
    int16_t table = ins_list[0].table(), action_type = ins_list[0].action_type();
    Status ret;
    int64_t cur_ts = GetCurTsMicros();

    const TableType table_type = schema.GetTableType();
    const ReduceType reduce_type = schema.GetReduceType();

    uint8_t cur_tree_version = kCurExpectedTreeVersion;
    int64_t base_ts;

    for (auto const& ins : ins_list) {
        if (UNLIKELY(!ValidInstance(ins, cur_ts))) {
            LOG_ERROR("invalid ins.ts or fid list is empty")
                    .put("Uid", ins.uid()).put("Ts", ins.ts());
            return Status::InvalidArgument(
                "invalid ins.ts (ts need be us unit) or feature_stat32_list empty");
        }

        for (int f_idx = 0; f_idx < ins.feature_stat32_list_size(); f_idx++) {
            auto const& fs = ins.feature_stat32_list(f_idx);
            if (UNLIKELY(fs.type() != table_type)) {
                LOG_ERROR("Data type conflict").put("TableType", table_type)
                .put("RequestType", fs.type());
                return Status::InvalidArgument(
                    fmt::format("instance type: {} not equal table type: {}",
                                (fs.type() == 0) ? "pair" : "list",
                                (schema.GetTableType() == TableType::PAIR) ? "pair" : "list"));
            }
            SlotID cur_slot = CalcSlot(fs);
            if (UNLIKELY(ins.uid() != uid || cur_slot != slot || ins.table() != table ||
                         ins.action_type() != action_type)) {
                LOG_ERROR("invalid parameter").put("CurrentUid", ins.uid())
                .put("OriginUid", uid)
                .put("CurrentSlot", cur_slot).put("OriginSlot", slot)
                .put("CurrentAction", ins.action_type()).put("OriginAction", action_type);
                return Status::InvalidArgument("unexpected error, contact oncall");
            }
            ret = IPSOperator::InsertFeatureStat(ctx, ordered_tree, ins.ts(), fs, table_type,
                                                 reduce_type, idempotent_add, cur_tree_version,
                                                 base_ts);
            if (UNLIKELY(!ret.ok())) {
                if (!ret.IsInternal()) {
                    LOG_ERROR("InsertFeatureStat failed").put("ErrorMsg", ret.ToString());
                }
                return ret;
            }
            if (cur_tree_version != kCurExpectedTreeVersion) {
                cur_tree_version = kCurExpectedTreeVersion;
            }
        }
    }

    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Add failed").put("ErrorMsg", ret.ToString());
        return ret;
    }

    Status ttl_ret = Ttlhandler(ctx, ordered_tree, schema, slot, action_type);
    if (!ttl_ret.ok() && !ttl_ret.IsNoAction()) {
        LOG_ERROR("Ttl Handler failed").put("ErrorMsg", ret.ToString());
        return ttl_ret;
    }

    Status compact_ret =
        is_replace ? Status::OK() : TryCompactOneTree(ctx, slot, action_type, ordered_tree, schema);
    if (!compact_ret.ok() && !compact_ret.IsNoAction()) {
        LOG_ERROR("Try compact Failed").put("ErrorMsg", compact_ret.ToString());
        return compact_ret;
    }
    Status shrink_ret =
        is_replace ? Status::OK() : TryShrinkOneUser(ctx, slot, action_type, ordered_tree, schema);
    if (!shrink_ret.ok() && !shrink_ret.IsNoAction()) {
        LOG_ERROR("Shrink failed").put("ErrorMsg", ret.ToString());
        return shrink_ret;
    } else {
        return ret;
    }
}

Status IPSInterface::Load(CmdContext* ctx, const std::vector<ips::LoadInstance>& ins_list,
                         bool idempotent_add, model::IpsModel* ordered_tree,
                         const ProfileTableSchema& schema, bool is_replace) {
    // assert(hf_mgr != nullptr);
    if (UNLIKELY(ins_list.empty())) {
        return Status::OK();
    }
    int64_t uid = ins_list[0].uid();
    SlotID slot = CalcSlot(ins_list[0].feature_stat32_list(0));
    int16_t table = ins_list[0].table(), action_type = ins_list[0].action_type();
    Status ret;
    int64_t cur_ts = GetCurTsMicros();

    const TableType table_type = schema.GetTableType();
    const ReduceType reduce_type = schema.GetReduceType();

    uint8_t cur_tree_version = kCurExpectedTreeVersion;
    int64_t base_ts;

    for (auto const& ins : ins_list) {
        if (UNLIKELY(!ValidInstance(ins, cur_ts))) {
            LOG_ERROR("invalid ins.ts or fid list is empty")
                    .put("Uid", ins.uid()).put("Ts", ins.max_ts());
            return Status::InvalidArgument(
                "invalid ins.ts (ts need be us unit) or feature_stat32_list empty");
        }

        for (int f_idx = 0; f_idx < ins.feature_stat32_list_size(); f_idx++) {
            auto const& fs = ins.feature_stat32_list(f_idx);
            if (UNLIKELY(fs.type() != table_type)) {
                LOG_ERROR("Data type conflict").put("TableType", table_type)
                .put("RequestType", fs.type());
                return Status::InvalidArgument(
                    fmt::format("instance type: {} not equal table type: {}",
                                (fs.type() == 0) ? "pair" : "list",
                                (schema.GetTableType() == TableType::PAIR) ? "pair" : "list"));
            }
            SlotID cur_slot = CalcSlot(fs);
            if (UNLIKELY(ins.uid() != uid || cur_slot != slot || ins.table() != table ||
                         ins.action_type() != action_type)) {
                LOG_ERROR("invalid parameter").put("CurrentUid", ins.uid())
                .put("OriginUid", uid)
                .put("CurrentSlot", cur_slot).put("OriginSlot", slot)
                .put("CurrentAction", ins.action_type()).put("OriginAction", action_type);
                return Status::InvalidArgument("unexpected error, contact oncall");
            }
            ret = IPSOperator::InsertFeatureStatWithMaxTsAndMinTs(ctx,
                                    ordered_tree, ins.max_ts(), ins.min_ts(), fs, table_type,
                                    reduce_type, idempotent_add, cur_tree_version, base_ts);
            if (UNLIKELY(!ret.ok())) {
                if (!ret.IsInternal()) {
                    LOG_ERROR("InsertFeatureStatWithMaxTsAndMinTs failed")
                    .put("ErrorMsg", ret.ToString());
                }
                return ret;
            }
            if (cur_tree_version != kCurExpectedTreeVersion) {
                cur_tree_version = kCurExpectedTreeVersion;
            }
        }
    }

    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Add failed").put("ErrorMsg", ret.ToString());
        return ret;
    }

    Status ttl_ret = Ttlhandler(ctx, ordered_tree, schema, slot, action_type);
    if (!ttl_ret.ok() && !ttl_ret.IsNoAction()) {
        LOG_ERROR("Ttl Handler failed").put("ErrorMsg", ret.ToString());
        return ttl_ret;
    }

    Status compact_ret =
        is_replace ? Status::OK() : TryCompactOneTree(ctx, slot, action_type, ordered_tree, schema);
    if (!compact_ret.ok() && !compact_ret.IsNoAction()) {
        LOG_ERROR("Try compact Failed").put("ErrorMsg", compact_ret.ToString());
        return compact_ret;
    }
    Status shrink_ret =
        is_replace ? Status::OK() : TryShrinkOneUser(ctx, slot, action_type, ordered_tree, schema);
    if (!shrink_ret.ok() && !shrink_ret.IsNoAction()) {
        LOG_ERROR("Shrink failed").put("ErrorMsg", ret.ToString());
        return shrink_ret;
    } else {
        return ret;
    }
}

int16_t IPSInterface::CalcSlot(const FeatureStat32& fs) {
    if (fs.has_slot()) {
        return fs.slot();
    }
    return fs.id() >> (kFidBitNum - kSlotBitNum);
}

std::string IPSInterface::GenEmptyTreeMeta() {
    std::string empty_tree_meta;
    if (kCurExpectedTreeVersion == 0) {
        empty_tree_meta.resize(kVersionZeroMetaSize, 0);
    } else if (kCurExpectedTreeVersion == 1) {
        empty_tree_meta.resize(kVersionOneMetaSize, 0);
    } else {
        LOG_ERROR("Invaild Version").put("Meta Version", kCurExpectedTreeVersion);
    }
    empty_tree_meta[kTreeMetaVersionOffset] = kCurExpectedTreeVersion;
    return empty_tree_meta;
}

// inline Status IPSInterface::GetOrCreateTree(server::OrderedTreeFactory* hf_mgr,
//                                             std::shared_ptr<server::OrderedTree>* ordered_tree,
//                                             const std::string& root_key, bool is_replace) {
//     Metrics::GetInstance()->Emit<kMetricCounter>("tree_get.qps", 1, FLAGS_metrics_sample);
//     *ordered_tree = nullptr;
//     if (is_replace) {
//         Status ret = hf_mgr->CreateTreeForReplace(&ordered_tree, root_key);
//         if (LIKELY(ret.ok())) {
//             std::string user_meta = GenEmptyTreeMeta();
//             ret = (*ordered_tree)->UpdateUserMeta(user_meta);
//             if (!ret.ok()) {
//                 BC_ERROR_DEFAULT_RATE_LIMIT("init user_meta failed, root_key: {}, ret: {}",
//                 root_key,
//                                             ret.ToString());
//             }
//         }
//         return ret;
//     }

//     Status ret = hf_mgr->GetMutableTree(&ordered_tree, root_key);
//     if (!ret.ok()) {
//         if (LIKELY(ret.IsNotFound())) {
//             ret = hf_mgr->CreateTree(&ordered_tree, root_key);
//             if (UNLIKELY(!ret.ok())) {
//                 BC_ERROR_DEFAULT_RATE_LIMIT("create tree failed, root_key: {}, ret: {}",
//                 root_key,
//                                             ret.ToString());
//                 return ret;
//             }
//             std::string user_meta = GenEmptyTreeMeta();
//             ret = (*ordered_tree)->UpdateUserMeta(user_meta);
//             if (!ret.ok()) {
//                 BC_ERROR_DEFAULT_RATE_LIMIT("init user_meta failed, root_key: {}, ret: {}",
//                 root_key,
//                                             ret.ToString());
//                 return ret;
//             }
//         } else {
//             if (ret.IsOutOfMemory()) {
//                 Metrics::GetInstance()->Emit<kMetricCounter>("oom.error.cnt", 1,
//                 FLAGS_metrics_sample);
//             } else {
//                 BC_ERROR_DEFAULT_RATE_LIMIT("get mutable failed, root_key: {}, ret: {}",
//                 root_key,
//                                             ret.ToString());
//             }
//             return ret;
//         }
//     }
//     return Status::OK();
// }

// // 从内存中随机获取指定用户在指定slot、action_type下的数据
// // 没有指定action_type则随机获取指定用户的随机slot和action_type下的数据
// Status IPSInterface::GetInstances(GetInstancesResponse* rsp, const GetInstancesRequest& req,
//                                   server::OrderedTreeFactory* hf_mgr, TableType table_type,
//                                   ReduceType reduce_type) {
//     if (UNLIKELY(!req.__isset.filter)) {
//         BC_ERROR_DEFAULT_RATE_LIMIT("GetInstancesRequest.filter is not set");
//         return Status::InvalidArgument("req.__isset.filter is false");
//     }

//     std::string root_key =
//         IPSKeyToTreeKey(req.uid, req.filter.slot, req.filter.instance_table,
//         req.filter.action_type);
//     std::shared_ptr<const server::OrderedTree> ordered_tree = nullptr;
//     Status ret = hf_mgr->GetImmutableTree(&&ordered_tree, root_key);
//     if (UNLIKELY(!ret.ok())) {
//         return ret;
//     }
//     assert(ordered_tree != nullptr);

//     // get tree meta
//     uint8_t tree_version;
//     int64_t base_ts;
//     ret = IPSOperator::GetQueryTreeVersionAndBaseTs(&ordered_tree, &tree_version, &base_ts);
//     if (UNLIKELY(!ret.ok())) {
//         return ret;
//     }

//     std::vector<IPSFeatureData> res;
//     int64_t range_min_ts = 0, range_max_ts = 0;
//     ret = IPSOperator::RangeGet(&ordered_tree, base_ts, tree_version, kMinIPSKey, kMaxIPSKey,
//     &res, true,
//                                 &range_min_ts, &range_max_ts, reduce_type);
//     if (UNLIKELY(!ret.ok())) {
//         return ret;
//     } else {
//         return BuildGetInstanceRsp(&res, rsp, req, table_type);
//     }
// }

// // 从cache中随机获取指定数量的用户在指定的slot、action_type下的数据
// Status IPSInterface::DumpUserData(DumpUserDataResponse* rsp, const DumpUserDataRequest& req,
//                                   server::OrderedTreeFactory* hf_mgr) {
//     BC_ERROR_DEFAULT_RATE_LIMIT("not support: DumpUserData");
//     return Status::NotSupported("dump interface is offline, please use get instance interface");
// }

// Status IPSInterface::ProfileDataDelete(ProfileDataDeleteResponse* rsp, const
// ProfileDataDeleteRequest& req,
//                                        server::OrderedTreeFactory* hf_mgr) {
//     std::string root_key = IPSKeyToTreeKey(req.uid, req.slot, req.instance_table,
//     req.action_type); std::shared_ptr<server::OrderedTree> ordered_tree; Status ret =
//     hf_mgr->GetMutableTree(&&ordered_tree, root_key); if (UNLIKELY(!ret.ok())) {
//         return ret;
//     }
//     ret = ordered_tree->OrSet().DeleteTree();
//     if (UNLIKELY(!ret.ok())) {
//         return ret;
//     }
//     return ordered_tree->OrSet().Commit();
// }

// Status IPSInterface::Remove(RemoveResponse* rsp, const RemoveRequest& req,
//                             server::OrderedTreeFactory* hf_mgr, ReduceType reduce_type) {
//     int64_t start_ts = req.time_range.start_micros;
//     int64_t end_ts = req.time_range.end_micros;
//     if (UNLIKELY(end_ts <= start_ts)) {
//         BC_ERROR_DEFAULT_RATE_LIMIT("invalid argument, end_ts: {} <= start_ts: {}", end_ts,
//         start_ts); return Status::InvalidArgument("req.time_range.end_micros <=
//         req.time_range.start_micros");
//     }

//     std::string root_key =
//         IPSKeyToTreeKey(req.uid, req.filter.slot, req.filter.instance_table,
//         req.filter.action_type);
//     std::shared_ptr<server::OrderedTree> ordered_tree = nullptr;
//     Status ret = hf_mgr->GetMutableTree(&&ordered_tree, root_key);
//     if (UNLIKELY(!ret.ok())) {
//         return ret;
//     }
//     // get tree meta
//     uint8_t tree_version;
//     int64_t base_ts;
//     ret = IPSOperator::GetQueryTreeVersionAndBaseTs(&ordered_tree, &tree_version, &base_ts);
//     if (UNLIKELY(!ret.ok())) {
//         return ret;
//     }

//     std::vector<std::pair<Slice, Slice>> res{};
//     IPSOperator::RangeGetByTime(reduce_type, &ordered_tree, base_ts, tree_version, start_ts,
//     end_ts, &res,
//                                 true);
//     if (!res.empty()) {
//         for (auto const& cur_key : res) {
//             ret = ordered_tree->OrSet().Del(cur_key.first.ToString());
//             if (UNLIKELY(!ret.ok())) {
//                 return ret;
//             }
//         }
//         return ordered_tree->OrSet().Commit();
//     }
//     return Status::OK();
// }

Status IPSInterface::Ttlhandler(CmdContext* ctx, model::IpsModel* ordered_tree,
                                const ProfileTableSchema& table_schema, SlotID slot,
                                ActionType action_type) {
    if (!table_schema.EnableTtl()) {
        return Status::NoAction("");
    }
    int64_t ttl_us = table_schema.GetSlotTtlConf(slot);
    if (ttl_us == -1) {
        return Status::NoAction("");
    }
    int64_t cur_ts = GetCurTsMicros();

    int64_t min_valid_ts_us = cur_ts - ttl_us;
    assert(min_valid_ts_us > 0);
    int64_t ttl_cnt = 0;
    int64_t ttl_scan_limit = table_schema.GetTtlScanLimit();
    Status ret =
        IPSOperator::TtlHandler(ctx, ordered_tree, min_valid_ts_us, &ttl_cnt, ttl_scan_limit);

    // TagKvList tag = {{"table_name", table_schema.TableName()},
    //                  {"slot", std::to_string(slot)},
    //                  {"action_type", std::to_string(action_type)},
    //                  {"ret", ret.CodeStr()}};
    // if (ret.IsNoAction()) {
    //     Metrics::GetInstance()->Emit<kMetricCounter>("ttl.no_action.qps", 1, std::move(tag),
    //                                                  FLAGS_metrics_sample);
    // } else if (ret.ok()) {
    //     Metrics::GetInstance()->Emit<kMetricTimer>("ttl.success.cnt", ttl_cnt, std::move(tag),
    //                                                FLAGS_metrics_sample);
    // } else {
    //     Metrics::GetInstance()->Emit<kMetricCounter>("ttl.fail.qps", 1, std::move(tag),
    //                                                  FLAGS_metrics_sample);
    // }
    return ret;
}

Status IPSInterface::NeedShrink(model::IpsModel* ordered_tree,
                                const ProfileTableSchema& table_schema, int64_t reserve_num,
                                bool* need_shrink) {
    *need_shrink = false;
    uint64_t tree_size;
    Status ret = ordered_tree->OrSet().Size(&tree_size);
    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Failed get tree size").put("ErrorMsg", ret.ToString());
        return ret;
    }
    thread_local const int64_t delta_shrink_size = Rand::randi64(100, 200);
    if (static_cast<int64_t>(tree_size) - reserve_num >= delta_shrink_size) {
        *need_shrink = true;
    }
    return Status::OK();
}

Status IPSInterface::NeedCompact(model::IpsModel* ordered_tree,
                                 const ProfileTableSchema& table_schema, SlotID slot,
                                 uint64_t* cur_tree_size, bool* need_compact) {
    Status ret = ordered_tree->OrSet().Size(cur_tree_size);
    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Get tree size failed");
        return ret;
    }
    uint64_t max_tree_size = table_schema.TriggerCompactSnapCount();
#ifndef NDEBUG
    if (*cur_tree_size >= max_tree_size) {
        *need_compact = true;
    } else {
        *need_compact = false;
    }
    return Status::OK();
#endif
    *need_compact = false;
    const std::string& compact_type = table_schema.CompactType();
    if (IsCompressCompactType(compact_type) &&
        table_schema.GetCompressCompactType() == CompressCompactType::OneTime) {
        int64_t cur_ts = GetCurTsMicros();

        int64_t compact_interval = table_schema.GetCompactInterval();
        if (cur_ts % compact_interval == 0) {
            *need_compact = true;
        }
        return Status::OK();
    } else {
        if (*cur_tree_size >= max_tree_size) {
            *need_compact = true;
            return Status::OK();
        } else if (IsTruncateCompactType(compact_type)) {
            return Status::OK();
        }
    }

    std::string tree_meta;
    ret = ordered_tree->OrSet().GetUserMeta(&tree_meta);
    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Get user meta failed").put("ErrorMsg", ret.ToString());
        return ret;
    }

    uint8_t tree_version = tree_meta[kTreeMetaVersionOffset];
    assert(tree_version == 0 || tree_version == 1);
    size_t expect_meta_size = (tree_version == 0) ? kVersionZeroMetaSize : kVersionOneMetaSize;
    if (UNLIKELY(tree_meta.size() != expect_meta_size)) {
        LOG_ERROR("Invaild IPS header meta");
        // BC_WARN_DEFAULT_RATE_LIMIT(
        //     "invalid ips header meta: root_key: {}, meta_size: {}, meta_data: {}, expected size:
        //     {}, " "ret: "
        //     "{}",
        //     ordered_tree->OrSet().GetTreeRootKey(), tree_meta.size(), tree_meta,
        // expect_meta_size,
        //     ret.ToString());
        assert(0 != 0);
        // BC_FATAL("invalid tree meta");
        return Status::InvalidArgument("invalid tree meta");
    }

    char* tree_meta_data = &tree_meta.front();
    if (UNLIKELY(tree_version == 0 && tree_meta_data[0] != 0)) {
        LOG_ERROR("invaild meta head version").put("TreeVersion", tree_version)
            .put("TreeVersionData", tree_meta_data[0]);
        assert(0 != 0);
        return Status::InvalidArgument("invalid tree meta version");
    }
    int64_t last_compact_ts = DecodeInt64FromBigEndian(tree_meta_data + 1);
    int64_t now_ts = GetCurTsMicros();
    thread_local int64_t compact_interval = table_schema.GetCompactInterval();
#ifndef NDEBUG
    compact_interval = 2;
#endif

    thread_local const int64_t compact_delta_ts =
        Rand::randi64(compact_interval / 2, compact_interval * 1.5);
    if (now_ts - last_compact_ts >= compact_delta_ts) {
        *need_compact = true;
    }
    return Status::OK();
}

Status UpdateCompactTsMeta(CmdContext* ctx, model::IpsModel* ordered_tree) {
    std::string tree_meta;
    Status ret = ordered_tree->OrSet().GetUserMeta(&tree_meta);
    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Get tree meta failed").put("ErrorMsg", ret.ToString());
        return ret;
    }
    char* tree_meta_data = &tree_meta.front();
    int64_t cur_ts = GetCurTsMicros();
    EncodeInt64ToBigEndian(cur_ts, tree_meta_data + 1);
    ret = ordered_tree->OrSet().UpdateUserMeta(ctx, tree_meta);

    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Update meta failed").put("ErrorMsg", ret.ToString());
    }
    return ret;
}

Status IPSInterface::TryCompactOneTree(CmdContext* ctx, SlotID slot, ActionType action_type,
                                       model::IpsModel* ordered_tree,
                                       const ProfileTableSchema& table_schema) {
    uint64_t pre_tree_size = 0;
    bool need_compact;
    Status ret = NeedCompact(ordered_tree, table_schema, slot, &pre_tree_size, &need_compact);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    if (LIKELY(!need_compact)) {
        return Status::NoAction("");
    }
    TagList cur_tag = {{"slot", std::to_string(slot)},
                       {"action_type", std::to_string(action_type)},
                       {"table_name", table_schema.TableName()}};
    // Metrics::GetInstance()->Emit<kMetricCounter>("compact.qps", 1, FLAGS_metrics_sample);
    TimeCost tc_compact;

    // get tree meta
    uint8_t tree_version;
    int64_t base_ts;
    ret = IPSOperator::GetQueryTreeVersionAndBaseTs(ordered_tree, &tree_version, &base_ts);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    assert(tree_version == kCurExpectedTreeVersion);

    // start compact
    const std::string& compact_type = table_schema.CompactType();
    if (IsCompressCompactType(compact_type)) {
        ret = IPSOperator::CompressCompact(
            ctx, ordered_tree, base_ts, tree_version, table_schema.GetTimeDimension(),
            table_schema.GetReduceType(), table_schema.GetTableType(), cur_tag,
            table_schema.GetCompressCompactType());
    } else if (IsTruncateCompactType(compact_type)) {
        // 3种不同的truncate：按照snap数量截断、按照绝对时间截断、按照相对时间截断
        TruncateType truncate_type = table_schema.GetTruncateType();
        if (truncate_type == IP_COUNT_TRUNCATE) {
            // 按照snap数量截断
            ret = IPSOperator::TruncateTreeDataByCount(
                ctx, table_schema.GetReduceType(), ordered_tree, base_ts, tree_version,
                table_schema.TriggerCompactSnapCount(), table_schema.MinSnapCountAfterTruncate());
        } else {
            int64_t max_snap_ts_micros = 0;
            Status ret =
                IPSOperator::GetTreeMaxKeyTs(&max_snap_ts_micros, ordered_tree, tree_version);
            if (UNLIKELY(!ret.ok())) {
                LOG_ERROR("Get MaxKey failed").put("ErrorMsg", ret.ToString());
                return ret;
            }
            if (truncate_type == IP_ABSOLUTE_TRUNCATE) {
                // 按照绝对时间截断
                max_snap_ts_micros = std::max(GetCurTsMicros(), max_snap_ts_micros);
            }
            int64_t range_ = table_schema.TruncateRangeMicros();
            int64_t truncate_snap_max_ts_micros = max_snap_ts_micros - range_;
            if (UNLIKELY(truncate_snap_max_ts_micros < 0)) {
                // BC_WARN_DEFAULT_RATE_LIMIT(
                //     "truncate by time failed, "
                //     "truncate_type: %d, max_snap_ts_micros: %lld, "
                //     "truncate_range_micros : %lld, truncate_snap_ts_micros: %lld",
                //     truncate_type, max_snap_ts_micros, table_schema.TruncateRangeMicros(),
                //     truncate_snap_max_ts_micros);
                return Status::NoAction("");
            } else {
                ret = IPSOperator::TruncateTreeDataByTimeRange(
                    ctx, table_schema.GetReduceType(), ordered_tree, base_ts, tree_version,
                    kMinIPSKey, truncate_snap_max_ts_micros,
                    table_schema.TruncateByTimeMaxSnapCount());
            }
        }
    } else {
        LOG_ERROR("Invaild compact type").put("CompactType", compact_type);
        return Status::InvalidArgument("InvalidArgument");
    }

    if (LIKELY(ret.ok())) {
        if (!IsCompressCompactType(table_schema.CompactType()) ||
            table_schema.GetCompressCompactType() != CompressCompactType::OneTime) {
            ret = UpdateCompactTsMeta(ctx, ordered_tree);
            if (!ret.ok()) {
                return ret;
            }
        }

        // Metrics::GetInstance()->Emit<kMetricTimer>("compact.latency", tc_compact.GetElapsed(),
        // cur_tag,
        //                                            FLAGS_metrics_sample);
        // if (UNLIKELY(NeedEmitMetrics())) {
        //     uint64_t cur_tree_size;
        //     ret = ordered_tree->OrSet().Size(&cur_tree_size);
        //     if (UNLIKELY(!ret.ok())) {
        //         BC_ERROR_DEFAULT_RATE_LIMIT("fail to get get tree size, tree_root: {}, ret: {}",
        //                                     ordered_tree->OrSet().GetTreeRootKey(),
        //                                     ret.ToString());
        //         return ret;
        //     }
        //     Metrics::GetInstance()->Emit<kMetricTimer>(
        //         "compact.size_descrease", pre_tree_size - cur_tree_size, cur_tag,
        //         FLAGS_metrics_sample);
        // }
    } else if (ret.IsNoAction()) {
        // Metrics::GetInstance()->Emit<kMetricCounter>("compact.no_action", 1, cur_tag,
        // FLAGS_metrics_sample);
    } else {
        LOG_ERROR("Compact failed").put("ErrorMsg", ret.ToString());
    }
    return ret;
}

Status IPSInterface::TryShrinkOneUser(CmdContext* ctx, SlotID slot, ActionType action_type,
                                      model::IpsModel* ordered_tree,
                                      const ProfileTableSchema& table_schema) {
    if (!table_schema.OpenShrink()) {
        return Status::NoAction("");
    }
    int64_t reserved_cnt = GetSlotReserveSize(table_schema, slot);
    bool cur_need_shrink;
    Status ret = NeedShrink(ordered_tree, table_schema, reserved_cnt, &cur_need_shrink);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    if (!cur_need_shrink) {
        return Status::NoAction("");
    }
    TagList cur_tag = {{"slot", std::to_string(slot)},
                       {"action_type", std::to_string(action_type)},
                       {"table_name", table_schema.TableName()}};
    // Metrics::GetInstance()->Emit<kMetricCounter>("shrink.qps", 1, cur_tag, FLAGS_metrics_sample);
    TimeCost tc_shrink;
    auto optor = GetSortOptor(table_schema.DeleteSequence());
    TableType table_type = table_schema.GetTableType();
    switch (optor) {
    case FeatureStatOperator::SORT_BY_V1:
        // SORT_BY_V1, SORT_BY_V2, SORT_BY_RATIO是pair特有的,检查一下合法性
        IPS_DATA_TYPE_CHECK(table_type, TableType::PAIR, Status::Unmatched("DataTypeConflict"));
        ret = Shrink<ShrinkGetSortWeightV1>(ctx, slot, reserved_cnt, ordered_tree, table_schema,
                                            cur_tag);
        break;
    case FeatureStatOperator::SORT_BY_V2:
        IPS_DATA_TYPE_CHECK(table_type, TableType::PAIR, Status::Unmatched("DataTypeConflict"));
        ret = Shrink<ShrinkGetSortWeightV2>(ctx, slot, reserved_cnt, ordered_tree, table_schema,
                                            cur_tag);
        break;
    case FeatureStatOperator::SORT_BY_RATIO:
        IPS_DATA_TYPE_CHECK(table_type, TableType::PAIR, Status::Unmatched("DataTypeConflict"));
        ret = Shrink<ShrinkGetSortWeightRatio>(ctx, slot, reserved_cnt, ordered_tree, table_schema,
                                               cur_tag);
        break;
    case FeatureStatOperator::SORT_BY_ID:
        ret = Shrink<ShrinkGetSortWeightID>(ctx, slot, reserved_cnt, ordered_tree, table_schema,
                                            cur_tag);
        break;
    case FeatureStatOperator::SORT_BY_VX:
        IPS_DATA_TYPE_CHECK(table_type, TableType::LIST, Status::Unmatched("DataTypeConflict"));
        ret = Shrink<ShrinkGetSortWeightVX>(ctx, slot, reserved_cnt, ordered_tree, table_schema,
                                            cur_tag);
        break;
    case FeatureStatOperator::SORT_BY_CUSTOMIZE_WEIGHT:
        ret = Shrink<ShrinkGetSortWeightCustomizeWeight>(ctx, slot, reserved_cnt, ordered_tree,
                                                         table_schema, cur_tag);
        break;
    case FeatureStatOperator::SORT_BY_WILSON_SCORE:
        ret = Shrink<ShrinkGetSortWeightWilsonWeight>(ctx, slot, reserved_cnt, ordered_tree,
                                                      table_schema, cur_tag);
        break;
    default:
        LOG_ERROR("Invaild try shrink one user invaild optor").put("optor", optor);
        return Status::InvalidArgument("InvalidArgument");
    }

    // if (LIKELY(ret.ok())) {
    //     Metrics::GetInstance()->Emit<kMetricTimer>("shrink.latency", tc_shrink.GetElapsed(),
    //                                                std::move(cur_tag), FLAGS_metrics_sample);
    // }
    return ret;
}

}  // namespace ips
}  // namespace bcache2
