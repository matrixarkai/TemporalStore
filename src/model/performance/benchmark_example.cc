// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <benchmark/benchmark.h>
#include <byte/include/assert.h>
#include <time.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <thread>

#include "absl/container/btree_map.h"
#include "common/logging.h"
#include "common/status.h"
#include "model/feature_model.h"
#include "partition/allocator_manager.h"
#include "partition/cmd_context.h"
#include "partition/storage/page_store.h"
#include "protocol/feature_module.pb.h"

void bench_feature_query(benchmark::State& state) {
    int64_t i = 0;
    bcache2::model::FeatureModel** feature_model =
        reinterpret_cast<bcache2::model::FeatureModel**>(state.range(0));
    int64_t table_cnt = state.range(1);
    uint64_t end = state.range(2);
    for (auto _ : state) {
        std::unique_ptr<bcache2::feature::QueryResponse> response_ptr(
            new bcache2::feature::QueryResponse);
        auto response = response_ptr.get();
        auto filter_func = [response](const uint64_t key, const std::string& entry) mutable {
            auto pt = response->add_point_list();
            pt->set_ts(key);
            pt->set_value(std::move(entry));
        };
        i++;
        auto j = i % table_cnt;
        auto st = feature_model[j]->OrSet().Query(0, end, end, filter_func);
        BYTE_ASSERT_TRUE(st.ok());
        BYTE_ASSERT_TRUE((uint64_t)(response->point_list_size()) == end);
    }
}

// void bench_feature_add(benchmark::State& state) {
//     std::string val(128, 'a');
//     uint64_t ts_start = 1000000, i = 0;
//     bcache2::model::FeatureModel** feature_model =
//         reinterpret_cast<bcache2::model::FeatureModel**>(state.range(0));
//     int64_t table_cnt = state.range(1);
//
//     for (auto _ : state) {
//         i++;
//         auto j = i % table_cnt;
//         auto st = feature_model[j]->OrSet().Add((ts_start + i), val);
//         BYTE_ASSERT_TRUE(st.ok());
//     }
// }

void bench_pb_serialize(benchmark::State& state) {
    bcache2::feature::FeaturePoint feature_point;
    std::string feature_val;

    for (auto _ : state) {
        feature_point.set_gid(1UL);
        feature_point.set_action_type(1U);
        feature_point.set_duration(1U);
        feature_point.set_author_id(1U);

        feature_point.SerializeToString(&feature_val);
    }
}

void bench_pb_all(benchmark::State& state) {
    bcache2::feature::FeaturePoint feature_point;
    std::string feature_val;

    for (auto _ : state) {
        feature_point.set_gid(1UL);
        feature_point.set_action_type(1U);
        feature_point.set_duration(1U);
        feature_point.set_author_id(1U);

        feature_point.SerializeToString(&feature_val);
        if (!feature_point.ParseFromString(feature_val)) {
            LOG_ERROR("ParseFromString fail");
            continue;
        }
    }
}

bcache2::Status construct_feature_model(bcache2::Allocator** allocators,
                                        bcache2::model::FeatureModel** feature_model,
                                        uint64_t table_cnt) {
    bcache2::Status st;

    for (uint64_t i = 0; i < table_cnt; i++) {
        allocators[i] = new bcache2::Allocator();
        feature_model[i] = new bcache2::model::FeatureModel;

        std::vector<bcache2::partition::PageInfo> pages;
        std::string page;
        uint64_t item_cnt = 11000;
        uint32_t cluster_id_start = 1000;
        uint64_t ts_start = 0;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);

        std::string val(40, 'a');
        for (uint64_t j = 0; j < item_cnt; j++) {
            std::string key = bcache2::model::SerializeToString<uint64_t>(ts_start + j);
            std::string value = bcache2::model::SerializeToString<std::string>(val);
            bcache2::model::WriteKvItemToStream(&stream, key, value, cluster_id_start + j, 0,
                                                ts_start);
        }
        stream.Trim();
        bcache2::partition::PageInfo page_info;
        page_info.data = std::move(page);
        pages.emplace_back(page_info);

        st = feature_model[i]->Init(allocators[i], pages);
        if (!st.ok()) {
            std::cout << "feature_model init fail" << std::endl;
            return st;
        }
    }
    return st;
}

int main(int argc, char** argv) {
    int64_t table_cnt = 10;
    bcache2::Allocator* allocator[table_cnt];
    bcache2::model::FeatureModel* feature_model[table_cnt];

    auto st = construct_feature_model(allocator, feature_model, table_cnt);
    if (!st.ok()) {
        std::cout << "construct_feature_model fail" << std::endl;
        return -1;
    }

    ::benchmark::RegisterBenchmark("bench_pb_serialize", &bench_pb_serialize);
    ::benchmark::RegisterBenchmark("bench_pb_all", &bench_pb_all);

    // TODO(wangtai.10): impl
    ::benchmark::RegisterBenchmark("bench_feature_query", &bench_feature_query)
        ->Args({reinterpret_cast<int64_t>(feature_model), table_cnt, 3000});
    ::benchmark::RegisterBenchmark("bench_feature_query", &bench_feature_query)
        ->Args({reinterpret_cast<int64_t>(feature_model), table_cnt, 5000});
    ::benchmark::RegisterBenchmark("bench_feature_query", &bench_feature_query)
        ->Args({reinterpret_cast<int64_t>(feature_model), table_cnt, 8000});
    // ::benchmark::RegisterBenchmark("bench_feature_add", &bench_feature_add)
    //    ->Args({reinterpret_cast<int64_t>(feature_model), table_cnt});

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    for (int64_t i = 0; i < table_cnt; i++) {
        delete feature_model[i];
        delete allocator[i];
    }

    return 0;
}
