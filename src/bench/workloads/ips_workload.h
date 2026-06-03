// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "bench/workloads/workloads.h"
#include "extension/modules.pb.h"
#include "extension/ips/interface.pb.h"
#include "common/time.h"

namespace bcache2 {
namespace bench {

class IpsWorkload : public Workload {
 public:
    struct Options {
        uint64_t freq_add = 0;
        uint64_t freq_query = 0;
    };

    IpsWorkload() {}
    ~IpsWorkload() {}

    void Init(Options options) {
        if (options.freq_add > 0) {
            function_dice_.AddProperty(options.freq_add, ips::Function::ADD);
        }
        if (options.freq_query > 0) {
            function_dice_.AddProperty(options.freq_query, ips::Function::BATCH_QUERY);
        }
        opts_ = options;
    }

    std::string Name() const override { return "IpsWorkload"; }

    Operation NextOperation(const std::string& key, const std::string& value) override;

 private:
    unsigned int RandomUid() const {
        static std::random_device dev;
        return dev();
    }

    void FillAddRequestForPair(ips::AddRequest* request, std::string table_str,
                                int32_t v1, int32_t v2, int64_t uid, int64_t ts,
                                int64_t action_type, int64_t slot) {
        request->set_enable_server_aggregator(true);
        request->set_table(table_str);
        auto instance = request->add_instance_list();
        instance->set_uid(uid);
        instance->set_ts(ts);
        instance->set_action_type(action_type);
        instance->set_table(0);
        auto featurestat32 = instance->add_feature_stat32_list();
        featurestat32->set_slot(slot);
        featurestat32->set_type(0);
        auto featurestat32intpair = featurestat32->mutable_int_pair();
        featurestat32intpair->set_v1(v1);
        featurestat32intpair->set_v2(v2);
    }

    void FillTimeQueryRequest(ips::BatchQueryRequest* request, std::string table_str,
                            int64_t uid, int64_t slot, int64_t action_type, int64_t time_range) {
        auto query_request = request->add_reqs();
        query_request->set_uid(uid);
        query_request->set_table(table_str);
        ips::DataRange* data_range = query_request->mutable_data_range();
        ips::Filter* filter = query_request->mutable_filter();
        data_range->set_type(ips::DataRangeType::ABSOLUTE_TIME_MICROS);
        data_range->set_range_val(time_range);
        filter->set_table(0);
        filter->set_slot(slot);
        filter->set_action_type(action_type);
        filter->set_top_k(30000);
        filter->set_has_top_k(true);
        filter->set_optor(ips::FeatureStatOperator::SORT_BY_TS);
    }

    Options opts_;
    RatioDice<ips::Function> function_dice_;
};

inline Operation IpsWorkload::NextOperation(const std::string& key, const std::string& value) {
    Operation operation;
    operation.set_module_id(Module::IPS);
    operation.set_function_id(function_dice_.Roll());

    switch (operation.function_id()) {
    case ips::Function::ADD: {
        ips::AddRequest request;
        int32_t v1 = 1, v2 = 0;
        int64_t uid = RandomUid() % 1000, ts = GetCurrentTimeInUs(), slot = 0, action_type = 0;
        FillAddRequestForPair(&request, std::string("table_compress"), v1, v2,
                              uid, ts, action_type, slot);
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(std::to_string(uid));
        break;
    }
    case ips::Function::BATCH_QUERY: {
        ips::BatchQueryRequest request;
        int64_t uid = RandomUid() % 1000, action_type = 0, slot = 0,
                time_range = static_cast<int64_t>(24) * 60 * 60 * 1000 * 1000;
        FillTimeQueryRequest(&request, std::string("table_compress"), uid,
                             slot, action_type, time_range);
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(std::to_string(uid));
        break;
    }
    }

    return operation;
}

}  // namespace bench
}  // namespace bcache2
