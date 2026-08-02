// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <brpc/traceprintf.h>
#include <bthread/bthread.h>
#include <bthread/types.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/slice.h"
#include "common/status.h"
#include "common/logging.h"
#include "model/ips/ips_define.h"
#include "model/ips/ips_operator.h"
#include "model/ips/profile_find_top_k.h"
#include "model/ips/profile_table_schema.h"
#include "model/ips/time_cost.h"
#include "model/ips/utils.h"
#include "partition/cmd_context.h"
#include "extension/ips/interface.pb.h"

namespace bcache2 {
namespace ips {

using partition::CmdContext;
class IPSInterface {
 public:
    // 单表query
    // static Status Query(const std::vector<const QueryRequest*>& reqs, size_t size,
    //                     std::vector<QueryResponse*>* rsps, server::OrderedTreeFactory* hf_mgr,
    //                     TableType table_type, ReduceType reduce_type);

    static Status Add(CmdContext* ctx, const std::vector<ips::Instance>& ins_list,
                      bool idempotent_add, model::IpsModel* ordered_tree,
                      const ProfileTableSchema& schema, bool is_replace);
    static Status Load(CmdContext* ctx, const std::vector<ips::LoadInstance>& ins_list,
                         bool idempotent_add, model::IpsModel* ordered_tree,
                         const ProfileTableSchema& schema, bool is_replace);

    static int16_t CalcSlot(const FeatureStat32& fs);

    static std::string GenEmptyTreeMeta();

    static Status SubQueryReqHandler(const QueryRequest& req, QueryResponse* rsp,
                                     model::IpsModel* ordered_tree, TableType table_type,
                                     ReduceType reduce_type, uint8_t tree_version, int64_t base_ts);

 private:
    static Status GetQueryTimeRange(model::IpsModel* ordered_tree, uint8_t tree_version,
                                    const DataRange& data_range, int64_t* start_micros,
                                    int64_t* end_micros);

    static Status TryCompactOneTree(CmdContext* ctx, SlotID slot, ActionType action_type,
                                    model::IpsModel* ordered_tree,
                                    const ProfileTableSchema& table_schema);

    static Status TryShrinkOneUser(CmdContext* ctx, SlotID slot, ActionType action_type,
                                   model::IpsModel* ordered_tree,
                                   const ProfileTableSchema& table_schema);

    template <typename T>
    static Status RangeByWeightHandler(const QueryRequest& req, int64_t top_k,
                                       const SortContext& context,
                                       std::vector<IPSFeatureData>* query_result,
                                       model::IpsModel* ordered_tree, int64_t* range_min_ts,
                                       int64_t* range_max_ts, ReduceType reduce_type,
                                       uint8_t tree_version, int64_t base_ts);

    static Status FilterByFidHandler(const QueryRequest& req, const Filter& filter,
                                     const SortContext& context,
                                     std::vector<IPSFeatureData>* query_result,
                                     model::IpsModel* ordered_tree, int64_t* range_min_ts,
                                     int64_t* range_max_ts, ReduceType reduce_type,
                                     uint8_t tree_version, int64_t base_ts);

    static Status RangeByTsHandler(const QueryRequest& req, int64_t top_k, const Filter& filter,
                                   const SortContext& context,
                                   std::vector<IPSFeatureData>* query_result,
                                   model::IpsModel* ordered_tree, int64_t* range_min_ts,
                                   int64_t* range_max_ts, ReduceType reduce_type,
                                   uint8_t tree_version, int64_t base_ts);

    static Status FilterByVxHandler(const QueryRequest& req, int64_t top_k,
                                    std::vector<IPSFeatureData>* query_result,
                                    model::IpsModel* ordered_tree, int64_t* range_min_ts,
                                    int64_t* range_max_ts, ReduceType reduce_type,
                                    uint8_t tree_version, int64_t base_ts);

    static Status BuildSubQueryResponse(std::vector<IPSFeatureData>* query_result,
                                        QueryResponse* rsp, const QueryRequest& req,
                                        TableType table_type, int64_t range_min_ts,
                                        int64_t range_max_ts);

    static Status GetFeatureStat(FeatureStat32* fs, IPSFeatureData* feature_data,
                                 TableType table_type);

    static Status NeedCompact(model::IpsModel* ordered_tree, const ProfileTableSchema& table_schema,
                              SlotID slot, uint64_t* cur_tree_size, bool* need_compact);

    static Status NeedShrink(model::IpsModel* ordered_tree, const ProfileTableSchema& table_schema,
                             int64_t reserve_num, bool* need_shrink);

    static Status Ttlhandler(CmdContext* ctx, model::IpsModel* ordered_tree,
                             const ProfileTableSchema& table_schema, SlotID slot,
                             ActionType action_type);

    static bool CheckParameterForCustomizeWeight(const Filter& filter, TableType table_type);

    static int64_t GetSlotReserveSize(const ProfileTableSchema& schema, SlotID slot);

    static FeatureStatOperator GetSortOptor(const std::string& optor);

    static bool ValidInstance(const ips::Instance& ins, int64_t cur_ts);

    static bool ValidInstance(const ips::LoadInstance& ins, int64_t cur_ts);
};

}  // namespace ips
}  // namespace bcache2
