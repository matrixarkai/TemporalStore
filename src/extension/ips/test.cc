// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/light_client.h"
#include "extension/ips/interface.pb.h"
#include "extension/modules.pb.h"
#include "test/smoketest/base_smoketest.h"

#include "model/ips/ips_operator.h"
#include "model/ips/random.h"
#include "model/ips/utils.h"

namespace bcache2 {
namespace swig {

class IpsModuleTest : public ::testing::Test {
 public:
    void SetUp() override {
        MiniCluster::Options options;
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        bcache2::Status internal_status = cluster_.Start();
        ASSERT_TRUE(internal_status.ok());

        MasteWrapper* master = cluster_.GetMaster();

        master->CreateSimpleTable("ns", "table");
        ASSERT_TRUE(internal_status.ok());

        // New client
        Client* tmp_client = nullptr;
        Status status = Client::Create(ClientOptions(), &tmp_client);
        ASSERT_TRUE(status.ok());
        std::unique_ptr<Client> client(tmp_client);

        // Open table
        status = client->OpenTable(
            "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) +
                "/ns/table",
            TableOptions(), &table);
        ASSERT_TRUE(status.ok());
        light_table = new LightTable{table};
    }

    void TearDown() override {
        std::unique_ptr<Table> table_release_gurad(table);
        std::unique_ptr<LightTable> light_table_release_gurad(light_table);

        cluster_.Stop();
    }

 protected:
    TempDir temp_dir_;
    MiniCluster cluster_;

    Table* table = nullptr;
    LightTable* light_table = nullptr;
};

uint32_t MakeCmdId(int module_id, int function_id) {
    return (static_cast<uint32_t>(module_id) << 16) | static_cast<uint32_t>(function_id);
}

TEST_F(IpsModuleTest, IpsModuleSimpleTest) {
    /*
     * Query not-exist-table
     */
    uint64_t test_ts = GetCurrentTimeInUs();
    {
        Controller ctrl;
        ips::AddRequest request;
        ips::AddResponse response;

        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table("not-exist-table");

        auto instance = ips_add_request->add_instance_list();
        instance->set_uid(1234567);
        instance->set_ts(test_ts);
        instance->set_action_type(0);
        instance->set_table(0);
        auto featurestat32 = instance->add_feature_stat32_list();
        featurestat32->set_slot(23);
        featurestat32->set_has_slot(true);
        featurestat32->set_type(0);
        auto featurestat32intpair = featurestat32->mutable_int_pair();
        featurestat32intpair->set_v1(1234);
        featurestat32intpair->set_v2(123456);

        light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(1234567), request,
                             &response);
        ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::NOT_FOUND));
        // ASSERT_EQ(response.err_code(), ips::ErrorCode::USER_NO_DATA);
    }
    /*
     * add data
     */
    {
        Controller ctrl;
        ips::AddRequest request;
        ips::AddResponse response;

        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table("table_compress");
        auto instance = ips_add_request->add_instance_list();
        instance->set_uid(1234567);
        instance->set_ts(test_ts);
        instance->set_action_type(0);
        instance->set_table(0);
        auto featurestat32 = instance->add_feature_stat32_list();
        featurestat32->set_slot(23);
        featurestat32->set_has_slot(true);
        featurestat32->set_type(0);
        auto featurestat32intpair = featurestat32->mutable_int_pair();
        featurestat32intpair->set_v1(1234);
        featurestat32intpair->set_v2(123456);

        light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(1234567), request,
                             &response);
        // std::cout << response.DebugString() << std::endl;
        ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        ASSERT_EQ(response.err_code(), ips::ErrorCode::SUCCESS);
    }
    /*
     * query data
     */
    {
        Controller ctrl;
        ips::BatchQueryRequest request;
        ips::BatchQueryResponse response;

        auto ips_batch_query_request = &request;

        ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
        query_request->set_uid(1234567);
        query_request->set_decoupled(false);
        query_request->set_table("table_compress");

        auto data_range = query_request->mutable_data_range();
        auto filter = query_request->mutable_filter();
        data_range->set_type(ips::DataRangeType::LAST_INSTANCES);
        data_range->set_range_val(10);

        filter->set_table(0);
        filter->set_action_type(0);
        filter->set_slot(23);
        filter->set_top_k(20);
        filter->set_optor(ips::FeatureStatOperator::SORT_BY_TS);

        light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY), std::to_string(1234567),
                             request, &response);
        // std::cout << "starting output test info" << std::endl;
        // std::cout << response.ips_response().DebugString() << std::endl;
        // std::cout << response.ips_response().batch_query_response().err_code() <<
        // std::endl; std::cout <<
        // response.ips_response().batch_query_response().error_desc()
        //           << std::endl;
        ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        // std::cout << response.DebugString() << std::endl;
        ASSERT_EQ(response.rsps_size(), 1);
        auto v1 = response.rsps(0).feature_stat32_list(0).int_pair().v1();
        auto v2 = response.rsps(0).feature_stat32_list(0).int_pair().v2();
        ASSERT_EQ(v1, 1234);
        ASSERT_EQ(v2, 123456);
    }
}

static const char kCompress[] = "table_compress";
static const char kTruncateByCount[] = "table_truncate_by_count";
static const char kTruncateByCountList[] = "table_truncate_by_count_list";
static const char kTruncateByTime[] = "table_truncate_by_time";
static const char kTtl[] = "table_ttl";

TEST_F(IpsModuleTest, TestOnceTimeCompressCompact) {
    // int loop_cnt = ips::Rand::randi64(100, 200);
    int loop_cnt = 10;
    while (loop_cnt > 0) {
        --loop_cnt;
        std::vector<std::pair<int64_t, int64_t>> wilson_weight;
        int64_t cur_uid = ips::GetCurTsMicros();
        // instance init
        std::vector<ips::Instance> ins_vce;

        size_t feature_count = 0;
        int64_t inner_loop = ips::Rand::randi64(10, 100);
        for (int64_t in_loop = 0; in_loop < inner_loop; ++in_loop) {
            // std::cout << in_loop << std::endl;
            const int64_t ins_cnt = ips::Rand::randi64(1, 50);
            int64_t loop_ts = ips::GetCurTsMicros();
            for (int64_t i = 0; i < ins_cnt; ++i) {
                // ins_vce.clear();
                int64_t cur_ts = loop_ts;
                if (ips::Rand::randi64(0, 100) > 50) {
                    cur_ts -= (ips::Rand::randi64(0, 100) * 3600 * 1000 * 1000);
                }

                /*
                    build add request
                */

                Controller ctrl;
                ips::AddRequest request;
                ips::AddResponse response;

                auto ips_add_request = &request;
                ips_add_request->set_enable_server_aggregator(true);
                ips_add_request->set_table(kCompress);
                auto instance = ips_add_request->add_instance_list();
                instance->set_table(0);
                instance->set_action_type(0);
                instance->set_uid(cur_uid);
                instance->set_ts(cur_ts);

                // init a FeatureStat32
                auto fs = instance->add_feature_stat32_list();
                fs->set_id(0);
                fs->set_type(0);

                auto int_pair = fs->mutable_int_pair();
                int_pair->set_v1(1);
                int_pair->set_v2(1);

                fs->set_slot(0);
                fs->set_has_slot(true);

                // // make a FeatureStat32 list
                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid),
                                     request, &response);
                // std::cout << response.ips_response().DebugString() << std::endl;
                ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
                ++feature_count;
            }
        }

        {
            Controller ctrl;
            ips::BatchQueryRequest request;
            ips::BatchQueryResponse response;

            auto ips_batch_query_request = &request;
            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kCompress);
            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();
            data_range->set_type(ips::DataRangeType::ABSOLUTE_TIME_MICROS);
            data_range->set_range_val(ips::GetCurTsMicros());
            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_top_k(feature_count);
            filter->set_optor(ips::FeatureStatOperator::SORT_BY_V1);

            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY), std::to_string(cur_uid),
                                 request, &response);
            // std::cout << "starting output test info loop_cnt:" <<loop_cnt<<  std::endl;
            // std::cout << response.ips_response().DebugString() << std::endl;
            // std::cout << response.ips_response().batch_query_response().err_code() <<
            // std::endl;
            // std::cout << response.ips_response().batch_query_response().error_desc()
            //         << std::endl;
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
            // std::cout << response.DebugString() << std::endl;
            ASSERT_TRUE(response.rsps(0).uid() == cur_uid);
            ASSERT_EQ(response.rsps(0).feature_stat32_list_size(), 1);
            for (int idx = 0; idx < response.rsps(0).feature_stat32_list_size(); idx++) {
                auto fs32 = response.rsps(0).feature_stat32_list(idx);
                ASSERT_EQ(fs32.type(), 0);
                ASSERT_EQ(fs32.int_pair().v1(), feature_count);
                ASSERT_EQ(fs32.int_pair().v2(), feature_count);
            }
        }
    }
}

TEST_F(IpsModuleTest, DISABLED_TestTableAndSlotTtl) {
    // int loop_cnt = ips::Rand::randi64(100, 200);
    int loop_cnt = 10;
    while (loop_cnt-- > 0) {
        int64_t cur_uid = ips::GetCurTsMicros();
        // instances init
        // std::vector<IPSInterface::Instance> ins_vce;
        std::unordered_set<int64_t> insert_ins_ts;
        int64_t inner_loop = ips::Rand::randi64(10, 50);
        for (int64_t in_loop = 0; in_loop < inner_loop; ++in_loop) {
            const int64_t ins_cnt = ips::Rand::randi64(10, 100);
            for (int64_t i = 0; i < ins_cnt; ++i) {
                // ins_vce.clear();
                int64_t cur_ts = static_cast<int64_t>(ips::GetCurTsMicros() / 1000) * 1000;
                if (i != 0) {
                    cur_ts -= (ips::Rand::randi64(0, 24 * 60 * 60) * 1000 * 1000);
                }
                while (insert_ins_ts.find(cur_ts) != insert_ins_ts.end()) {
                    cur_ts -= 1000L * 1000;
                }
                insert_ins_ts.insert(cur_ts);
                /*
                    build instance
                */
                Controller ctrl;
                ips::AddRequest request;
                ips::AddResponse response;

                auto ips_add_request = &request;
                ips_add_request->set_enable_server_aggregator(true);
                ips_add_request->set_table(kTtl);
                auto instance = ips_add_request->add_instance_list();

                instance->set_table(0);
                instance->set_action_type(0);
                instance->set_uid(cur_uid);
                instance->set_ts(cur_ts);

                // init a FeatureStat32
                auto fs = instance->add_feature_stat32_list();
                fs->set_has_slot(true);
                fs->set_id(0);
                fs->set_type(0);

                auto int_pair = fs->mutable_int_pair();
                int_pair->set_v1(1);
                int_pair->set_v2(1);

                fs->set_slot(0);
                fs->set_has_slot(true);
                // make a FeatureStat32 list

                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid),
                                     request, &response);
                ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
                fs->set_slot(1);
                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid),
                                     request, &response);
                ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
            }
        }

        {
            /* query */
            Controller ctrl;
            ips::BatchQueryRequest request;

            auto ips_batch_query_request = &request;
            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kTtl);
            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();
            data_range->set_type(ips::DataRangeType::RELATIVE_TIME_MICROS);
            data_range->set_range_val(ips::GetCurTsMicros());
            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_top_k(100000);
            filter->set_optor(ips::FeatureStatOperator::SORT_BY_TS);
            const int64_t max_insert_ts =
                *std::max_element(insert_ins_ts.begin(), insert_ins_ts.end());

            // /* check slot 0 */
            {
                ips::BatchQueryResponse response;
                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY),
                                     std::to_string(cur_uid), request, &response);
                // ASSERT_EQ(ctrl.status.code(), 0);

                const int64_t slot0_min_valid_ts = max_insert_ts - 5L * 3600 * 1000 * 1000;
                const int64_t slot0_valid_ts_cnt =
                    std::count_if(insert_ins_ts.begin(), insert_ins_ts.end(),
                                  [=](int64_t ts) { return ts > slot0_min_valid_ts; });

                ASSERT_GE(response.rsps(0).start_ts(), slot0_min_valid_ts);
                ASSERT_GE(response.rsps(0).end_ts(), slot0_min_valid_ts);

                int64_t delta_size =
                    static_cast<int64_t>(response.rsps(0).feature_stat32_list_size()) -
                    slot0_valid_ts_cnt;
                ASSERT_TRUE(delta_size == 0 || delta_size == 1 || delta_size == -1);
                ASSERT_LE(delta_size, 1);

                ASSERT_TRUE(response.rsps(0).uid() == cur_uid);
                for (int idx = 0; idx < response.rsps(0).feature_stat32_list_size(); idx++) {
                    auto fs32 = response.rsps(0).feature_stat32_list(idx);
                    ASSERT_EQ(fs32.type(), 0);
                    ASSERT_GE(fs32.int_pair().timestamp(), slot0_min_valid_ts);
                }
            }

            {
                /*
                check slot 1
                */
                filter->set_slot(1);

                ips::BatchQueryResponse response;
                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY),
                                     std::to_string(cur_uid), request, &response);
                ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));

                const int64_t slot1_min_valid_ts = max_insert_ts - 1L * 3600 * 1000 * 1000;
                const int64_t slot1_valid_ts_cnt =
                    std::count_if(insert_ins_ts.begin(), insert_ins_ts.end(),
                                  [=](int64_t ts) { return ts > slot1_min_valid_ts; });

                // std::cout << ips_resp.DebugString() << std::endl;
                ASSERT_GE(response.rsps(0).start_ts(), slot1_min_valid_ts);
                ASSERT_GE(response.rsps(0).end_ts(), slot1_min_valid_ts);

                int64_t delta_size =
                    static_cast<int64_t>(response.rsps(0).feature_stat32_list_size()) -
                    slot1_valid_ts_cnt;
                // ASSERT_TRUE(delta_size == 0 || delta_size == 1 || delta_size == -1);
                ASSERT_LE(delta_size, 1);
                ASSERT_TRUE(response.rsps(0).uid() == cur_uid);
                for (int idx = 0; idx < response.rsps(0).feature_stat32_list_size(); idx++) {
                    auto fs32 = response.rsps(0).feature_stat32_list(idx);
                    ASSERT_EQ(fs32.type(), 0);
                    ASSERT_GE(fs32.int_pair().timestamp(), slot1_min_valid_ts);
                }
            }
        }
    }
}

TEST_F(IpsModuleTest, TestQuerySortByWilson) {
    int loop_cnt = 5;
    // int loop_cnt = ips::Rand::randi64(100, 400);
    while (loop_cnt >= 0) {
        --loop_cnt;
        std::vector<std::pair<int64_t, int64_t>> wilson_weight;
        int64_t cur_uid = ips::GetCurTsMicros();
        // instance init
        // std::vector<IPSInterface::Instance> ins_vce;

        // ins_vce.reserve(ins_cnt);
        int ins_cnt = 5;

        Controller ctrl;
        ips::AddRequest request;

        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table(kTruncateByCount);

        for (int64_t i = 0; i < ins_cnt; ++i) {
            int64_t cur_ts = ips::GetCurTsMicros();
            auto instance = ips_add_request->add_instance_list();
            instance->set_table(0);
            instance->set_action_type(0);
            instance->set_uid(cur_uid);
            instance->set_ts(cur_ts);

            auto fs = instance->add_feature_stat32_list();
            fs->set_id(i);
            fs->set_type(0);

            auto int_pair = fs->mutable_int_pair();

            fs->set_slot(0);
            fs->set_has_slot(true);

            int64_t v1, v2;
            int64_t cur_score;
            do {
                v1 = ips::Rand::randi64(0, 10000);
                v2 = ips::Rand::randi64(0, 10000);
                cur_score = ips::GetWilsonScore(v1, v2);
                bool is_repeat = false;
                for (auto const& e : wilson_weight) {
                    if (e.second == cur_score) {
                        is_repeat = true;
                        break;
                    }
                }
                if (is_repeat) {
                    continue;
                }
                wilson_weight.emplace_back(std::piecewise_construct, std::forward_as_tuple(i),
                                           std::forward_as_tuple(cur_score));
                int_pair->set_v1(v1);
                int_pair->set_v2(v2);
                break;
            } while (true);
        }

        // Add
        {
            ips::AddResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid), request,
                                 &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        }

        // Query Request
        {
            ips::BatchQueryRequest request;
            auto ips_batch_query_request = &request;

            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kTruncateByCount);

            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();
            data_range->set_type(ips::DataRangeType::ABSOLUTE_TIME_MICROS);
            data_range->set_range_val(ips::GetCurTsMicros());

            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);

            filter->set_optor(ips::FeatureStatOperator::SORT_BY_WILSON_SCORE);
            for (int64_t topk = 1; topk <= ins_cnt; ++topk) {
                filter->set_top_k(topk);

                ips::BatchQueryResponse response;
                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY),
                                     std::to_string(cur_uid), request, &response);
                ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));

                auto cur_rsp = response.rsps(0);
                // std::cout << response.DebugString() << std::endl;
                auto fs32_vec_size = cur_rsp.feature_stat32_list_size();

                ASSERT_EQ(fs32_vec_size, filter->top_k());
                ASSERT_TRUE(cur_rsp.uid() == cur_uid);

                std::sort(wilson_weight.begin(), wilson_weight.end(),
                          [](const std::pair<int64_t, int64_t>& a,
                             const std::pair<int64_t, int64_t>& b) { return a.second > b.second; });
                std::unordered_set<int64_t> expected_res;

                for (size_t i = 0; i < (size_t)filter->top_k(); ++i) {
                    expected_res.insert(wilson_weight[i].first);
                }
                for (int fs_idx = 0; fs_idx < cur_rsp.feature_stat32_list_size(); fs_idx++) {
                    auto fs32 = cur_rsp.feature_stat32_list(fs_idx);
                    ASSERT_EQ(fs32.type(), 0);

                    bool is_find = expected_res.find(fs32.id()) != expected_res.end();
                    if (!is_find) {
                        std::cout << "ins_cnt: " << ins_cnt << std::endl;
                        std::cout << "cur id: " << fs32.id() << std::endl;
                        for (auto id : expected_res) {
                            std::cout << "expected id: " << id << std::endl;
                        }
                        for (auto w : wilson_weight) {
                            std::cout << "id: " << w.first << " , weight: " << w.second
                                      << std::endl;
                        }
                    }
                    ASSERT_TRUE(is_find);
                }
            }
        }
    }
}

TEST_F(IpsModuleTest, TestLastInstanceWeightQuery) {
    int loop_cnt = 3;
    // int loop_cnt = ips::Rand::randi64(100, 200);
    while (loop_cnt >= 0) {
        --loop_cnt;

        Controller ctrl;
        ips::AddRequest request;

        int64_t cur_uid = ips::GetCurTsMicros();
        // instance init
        int64_t fid_num = ips::Rand::randi64(10, 50);
        int64_t feature_val = ips::Rand::randi64(10, 50);
        int64_t ins_cnt = fid_num * feature_val;

        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table(kCompress);

        for (int64_t i = 0; i < ins_cnt; ++i) {
            int64_t cur_ts = ips::GetCurTsMicros();

            auto instance = ips_add_request->add_instance_list();
            instance->set_table(0);
            instance->set_action_type(0);
            instance->set_uid(cur_uid);
            instance->set_ts(cur_ts);

            // init a FeatureStat32

            auto fs = instance->add_feature_stat32_list();
            fs->set_id(i % fid_num);
            fs->set_type(0);

            auto int_pair = fs->mutable_int_pair();

            fs->set_slot(0);
            fs->set_has_slot(true);

            int_pair->set_v1(1);
            int_pair->set_v2(1);
        }

        /* add */
        {
            ips::AddResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid), request,
                                 &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        }

        /* query */

        {
            ips::BatchQueryRequest request;
            ips::BatchQueryResponse response;

            auto ips_batch_query_request = &request;
            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kCompress);

            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();
            data_range->set_type(ips::DataRangeType::LAST_INSTANCES);
            data_range->set_range_val(ips::GetCurTsMicros());

            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_top_k(ins_cnt);
            filter->set_optor(ips::FeatureStatOperator::SORT_BY_V1);

            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY), std::to_string(cur_uid),
                                 request, &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));

            auto cur_rsp = response.rsps(0);
            for (int fs_idx = 0; fs_idx < cur_rsp.feature_stat32_list_size(); fs_idx++) {
                auto fs32 = cur_rsp.feature_stat32_list(fs_idx);
                ASSERT_EQ(fs32.type(), 0);
                ASSERT_EQ(fs32.int_pair().v1(), feature_val);
                ASSERT_EQ(fs32.int_pair().v2(), feature_val);
            }
        }
    }
}

// server端无法满足任何一个强约束 kTruncateByCountList
TEST_F(IpsModuleTest, TestStrongFilterQueryNoData) {
    int loop_cnt = 3;
    // int loop_cnt = ips::Rand::randi64(100, 200);

    while (loop_cnt >= 0) {
        --loop_cnt;
        int64_t cur_uid = ips::GetCurTsMicros();

        Controller ctrl;
        ips::AddRequest request;
        ips::AddResponse response;

        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table(kTruncateByCountList);

        int ins_cnt = ips::Rand::randi64(20, 50);
        for (int64_t i = 0; i < ins_cnt; ++i) {
            int64_t cur_ts = ips::GetCurTsMicros();
            auto instance = ips_add_request->add_instance_list();
            instance->set_table(0);
            instance->set_action_type(0);
            instance->set_uid(cur_uid);
            instance->set_ts(cur_ts);

            // init a FeatureStat32

            auto fs = instance->add_feature_stat32_list();
            fs->set_id(i);
            fs->set_type(1);

            auto int_list = fs->mutable_int_list();

            fs->set_slot(0);
            fs->set_has_slot(true);

            int_list->add_v_list(0);
            int_list->add_v_list(0);
            int_list->add_v_list(0);
            int_list->add_v_list(0);
        }

        // Add
        {
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid), request,
                                 &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        }

        // Query Request
        {
            Controller ctrl;
            ips::BatchQueryRequest request;
            ips::BatchQueryResponse response;

            auto ips_batch_query_request = &request;

            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kTruncateByCountList);

            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();
            int64_t type = ips::Rand::randi64(0, 100);
            if (type > 50) {
                data_range->set_type(ips::DataRangeType::ABSOLUTE_TIME_MICROS);
            } else {
                data_range->set_type(ips::DataRangeType::LAST_INSTANCES);
            }

            data_range->set_range_val(ips::GetCurTsMicros());

            filter->set_top_k(ins_cnt);
            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_optor(ips::FeatureStatOperator::SORT_BY_TS);

            filter->add_index(0);
            filter->add_index(1);
            filter->add_index(3);
            filter->set_has_index(true);

            filter->add_min_index_count(1);
            filter->add_min_index_count(3);
            filter->add_min_index_count(2);
            // filter->mutable_min_index_count()->CopyFrom({1,3,2});
            filter->set_has_min_index_count(true);

            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY), std::to_string(cur_uid),
                                 request, &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));

            ASSERT_EQ(response.err_code(), 0);
            auto cur_rsp = response.rsps(0);

            auto fs32_vec_size = cur_rsp.feature_stat32_list_size();
            ASSERT_EQ(fs32_vec_size, 1);  // 预期返回1个非强约束的fid
            ASSERT_EQ(cur_rsp.feature_stat32_list(0).id(), ins_cnt - 1);
        }
    }
}

// server端遍历的前topk个fid都满足强约束 kTruncateByCountList
TEST_F(IpsModuleTest, TestStrongFilterQueryFullData) {
    int loop_cnt = 3;
    // int loop_cnt = ips::Rand::randi64(100, 200);
    while (loop_cnt >= 0) {
        --loop_cnt;
        int64_t cur_uid = ips::GetCurTsMicros();
        // instance init
        int ins_cnt = ips::Rand::randi64(20, 50);

        Controller ctrl;
        ips::AddRequest request;
        ips::AddResponse response;

        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table(kTruncateByCountList);

        for (int64_t i = 0; i < ins_cnt; ++i) {
            int64_t cur_ts = ips::GetCurTsMicros();

            auto instance = ips_add_request->add_instance_list();
            instance->set_table(0);
            instance->set_action_type(0);
            instance->set_uid(cur_uid);
            instance->set_ts(cur_ts);

            // init a FeatureStat32
            auto fs = instance->add_feature_stat32_list();
            fs->set_id(i);
            fs->set_type(1);

            auto int_list = fs->mutable_int_list();

            fs->set_slot(0);
            fs->set_has_slot(true);

            int_list->add_v_list(1);
            int_list->add_v_list(1);
            int_list->add_v_list(1);
            int_list->add_v_list(1);
        }

        // Add
        {
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid), request,
                                 &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        }

        {
            Controller ctrl;
            ips::BatchQueryRequest request;
            ips::BatchQueryResponse response;

            auto ips_batch_query_request = &request;

            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kTruncateByCountList);

            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();
            int64_t type = ips::Rand::randi64(0, 100);
            if (type > 50) {
                data_range->set_type(ips::DataRangeType::ABSOLUTE_TIME_MICROS);
            } else {
                data_range->set_type(ips::DataRangeType::LAST_INSTANCES);
            }

            data_range->set_range_val(ips::GetCurTsMicros());

            filter->set_top_k(ins_cnt);
            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_optor(ips::FeatureStatOperator::SORT_BY_TS);

            filter->add_index(0);
            filter->add_index(1);
            filter->add_index(3);
            filter->set_has_index(true);

            filter->add_min_index_count(1);
            filter->add_min_index_count(3);
            filter->add_min_index_count(2);
            // filter->mutable_min_index_count()->CopyFrom({1,3,2});
            filter->set_has_min_index_count(true);

            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY), std::to_string(cur_uid),
                                 request, &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));

            ASSERT_EQ(response.err_code(), 0);
            auto cur_rsp = response.rsps(0);
            auto fs32_vec_size = cur_rsp.feature_stat32_list_size();

            ASSERT_EQ(fs32_vec_size, 3);

            int64_t expected_fid = ins_cnt - 1;
            for (int fs_idx = 0; fs_idx < cur_rsp.feature_stat32_list_size(); fs_idx++) {
                ASSERT_EQ(cur_rsp.feature_stat32_list(fs_idx).id(), expected_fid);
                --expected_fid;
            }

            // filter->mutable_min_index_count()->CopyFrom({1,3,2});
            {
                filter->clear_min_index_count();
                filter->add_min_index_count(1);
                filter->add_min_index_count(-3);
                filter->add_min_index_count(2);

                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY),
                                     std::to_string(cur_uid), request, &response);
                // std::cout << "992" << response.ShortDebugString() << std::endl;
                // std::cout << ctrl.status.code() << std::endl;
                // ASSERT_EQ(response.err_code(), 8);
                ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::INVALID_ARGUMENT));
            }

            {
                filter->clear_index();
                filter->add_index(1);
                filter->add_index(-3);
                filter->add_index(2);
                filter->clear_min_index_count();
                filter->add_min_index_count(0);
                filter->add_min_index_count(2);
                filter->add_min_index_count(-3);

                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY),
                                     std::to_string(cur_uid), request, &response);
                // std::cout << response.ShortDebugString() << std::endl;
                // ASSERT_EQ(response.err_code(), 8);
                ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::INVALID_ARGUMENT));
            }

            {
                filter->clear_min_index_count();
                filter->add_min_index_count(1);
                filter->add_min_index_count(0);
                filter->add_min_index_count(2);

                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY),
                                     std::to_string(cur_uid), request, &response);
                // std::cout << response.ShortDebugString() << std::endl;
                // ASSERT_EQ(response.err_code(), 8);
                ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::INVALID_ARGUMENT));
            }
        }
    }
}

// server端只能满足client要求的部分强约束
TEST_F(IpsModuleTest, TestStrongFilterQueryPartialData) {
    /* init */
    int loop_cnt = 50;
    // int loop_cnt = ips::Rand::randi64(100, 200);

    while (loop_cnt >= 0) {
        --loop_cnt;

        int64_t cur_uid = ips::GetCurTsMicros();
        // instance init
        int ins_cnt = ips::Rand::randi64(20, 50);

        Controller ctrl;
        ips::AddRequest request;
        // instance init
        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table(kTruncateByCountList);

        for (int64_t i = 0; i < ins_cnt; ++i) {
            int64_t cur_ts = ips::GetCurTsMicros();

            auto instance = ips_add_request->add_instance_list();
            instance->set_table(0);
            instance->set_action_type(0);
            instance->set_uid(cur_uid);
            instance->set_ts(cur_ts);

            // init a FeatureStat32
            auto fs = instance->add_feature_stat32_list();
            fs->set_id(i);
            fs->set_type(1);

            auto int_list = fs->mutable_int_list();

            fs->set_slot(0);
            fs->set_has_slot(true);

            int_list->add_v_list(1);
            int_list->add_v_list(0);
            int_list->add_v_list(0);
            int_list->add_v_list(3);
        }

        // Add
        {
            ips::AddResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid), request,
                                 &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        }

        {
            ips::BatchQueryRequest request;
            auto ips_batch_query_request = &request;

            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kTruncateByCountList);

            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();
            int64_t type = ips::Rand::randi64(0, 100);
            if (type > 50) {
                data_range->set_type(ips::DataRangeType::ABSOLUTE_TIME_MICROS);
            } else {
                data_range->set_type(ips::DataRangeType::LAST_INSTANCES);
            }

            data_range->set_range_val(ips::GetCurTsMicros());

            filter->set_top_k(ins_cnt);
            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_optor(ips::FeatureStatOperator::SORT_BY_TS);

            filter->add_index(0);
            filter->add_index(1);
            filter->add_index(3);
            filter->set_has_index(true);

            filter->add_min_index_count(1);
            filter->add_min_index_count(3);
            filter->add_min_index_count(2);
            // filter->mutable_min_index_count()->CopyFrom({1,3,2});
            filter->set_has_min_index_count(true);

            ips::BatchQueryResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY), std::to_string(cur_uid),
                                 request, &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));

            ASSERT_EQ(response.err_code(), 0);
            auto cur_rsp = response.rsps(0);
            auto fs32_vec_size = cur_rsp.feature_stat32_list_size();
            ASSERT_EQ(fs32_vec_size, 3);
            int64_t expected_fid = ins_cnt - 1;
            for (int fs_idx = 0; fs_idx < cur_rsp.feature_stat32_list_size(); fs_idx++) {
                ASSERT_EQ(cur_rsp.feature_stat32_list(fs_idx).id(), expected_fid);
                --expected_fid;
            }
        }
    }
}

void GenInstance(ips::Instance* instance, int64_t uid, int64_t fid, int64_t ts,
                 const std::vector<int64_t>& list_feat) {  // ins 1
    instance->set_table(0);
    instance->set_action_type(0);
    instance->set_uid(uid);
    instance->set_ts(ts);

    // init a FeatureStat32
    auto fs = instance->add_feature_stat32_list();
    fs->set_id(fid);
    fs->set_type(1);
    fs->mutable_int_list()->mutable_v_list()->CopyFrom({list_feat.begin(), list_feat.end()});
    fs->set_slot(0);
    fs->set_has_slot(true);
}

// // /*
// // 存在重复的fid满足强约束, 写入后的时序数据如下：
// // strong_index:    [0, 1, 3]
// // min_index_count: [1, 3, 2]
// // ts递减写入的数据：
// // fid2: [1, 1, 0, 1] <0, 1>  <1, 1>  <3, 1>
// // fid4: [1, 0, 1, 1]         <1, 1>  <3, 2>
// // fid2: [1, 1, 0, 0]
// // fid3: [0, 1, 0, 1]         <1, 2>  <3, 3>
// // fid1: [0, 0, 1, 0]         random
// // fid4: [0, 1, 0, 0]         <1, 3>

// // 预期返回的结果(ts递减)
// // expect result:
// // fid2: [1, 1, 0, 1]
// // fid4: [1, 0, 1, 1]
// // fid3: [0, 1, 0, 1]
// // fid1: [0, 0, 1, 0]
// // fid4: [0, 1, 0, 0]
// // */
TEST_F(IpsModuleTest, TestStrongFilterQueryRepeatData) {
    int loop_cnt = ips::Rand::randi64(100, 200);
    while (loop_cnt >= 0) {
        --loop_cnt;

        int64_t cur_uid = ips::GetCurTsMicros();

        ips::AddRequest request;
        Controller ctrl;
        // instance init

        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table(kTruncateByCountList);
        // ins 1
        int64_t ts = ips::GetCurTsMicros();
        GenInstance(ips_add_request->add_instance_list(), cur_uid, 4, ts + 1, {0, 1, 0, 0});
        GenInstance(ips_add_request->add_instance_list(), cur_uid, 1, ts + 2, {0, 0, 1, 0});
        GenInstance(ips_add_request->add_instance_list(), cur_uid, 3, ts + 3, {0, 1, 0, 1});
        GenInstance(ips_add_request->add_instance_list(), cur_uid, 2, ts + 4, {1, 1, 0, 0});
        GenInstance(ips_add_request->add_instance_list(), cur_uid, 4, ts + 5, {1, 0, 1, 1});
        GenInstance(ips_add_request->add_instance_list(), cur_uid, 2, ts + 6, {1, 1, 0, 1});

        // instance init

        // Add
        {
            ips::AddResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid), request,
                                 &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        }

        // Query Request

        {
            ips::BatchQueryRequest request;
            auto ips_batch_query_request = &request;

            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kTruncateByCountList);

            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();
            int64_t type = ips::Rand::randi64(0, 100);
            if (type > 50) {
                data_range->set_type(ips::DataRangeType::ABSOLUTE_TIME_MICROS);
            } else {
                data_range->set_type(ips::DataRangeType::LAST_INSTANCES);
            }

            data_range->set_range_val(ips::GetCurTsMicros());

            filter->set_top_k(10);
            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_optor(ips::FeatureStatOperator::SORT_BY_TS);

            filter->add_index(0);
            filter->add_index(1);
            filter->add_index(3);
            filter->set_has_index(true);

            filter->add_min_index_count(1);
            filter->add_min_index_count(3);
            filter->add_min_index_count(2);
            // filter->mutable_min_index_count()->CopyFrom({1,3,2});
            filter->set_has_min_index_count(true);

            ips::BatchQueryResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY), std::to_string(cur_uid),
                                 request, &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));

            ASSERT_EQ(response.err_code(), 0);
            auto cur_rsp = response.rsps(0);
            auto fs32_vec_size = cur_rsp.feature_stat32_list_size();
            ASSERT_EQ(fs32_vec_size, 5);

            ASSERT_EQ(cur_rsp.feature_stat32_list(0).id(), 2);
            // expected_res = {1, 1, 0, 1};
            // ASSERT_EQ(fs32_vec[0].int_list.v_list, expected_res);

            ASSERT_EQ(cur_rsp.feature_stat32_list(1).id(), 4);
            // expected_res = {1, 0, 1, 1};
            // ASSERT_EQ(fs32_vec[1].int_list.v_list, expected_res);

            ASSERT_EQ(cur_rsp.feature_stat32_list(2).id(), 3);
            // expected_res = {0, 1, 0, 1};
            // ASSERT_EQ(fs32_vec[2].int_list.v_list, expected_res);

            ASSERT_EQ(cur_rsp.feature_stat32_list(3).id(), 1);
            // expected_res = {0, 0, 1, 0};
            // ASSERT_EQ(fs32_vec[3].int_list.v_list, expected_res);

            ASSERT_EQ(cur_rsp.feature_stat32_list(4).id(), 4);
            // expected_res = {0, 1, 0, 0};
            // ASSERT_EQ(fs32_vec[4].int_list.v_list, expected_res);
        }
    }
}

TEST_F(IpsModuleTest, TestStrongFilterRangeQueryFullData) {
    int loop_cnt = 50;
    // int loop_cnt = ips::Rand::randi64(100, 200);
    while (loop_cnt >= 0) {
        --loop_cnt;
        int64_t cur_uid = ips::GetCurTsMicros();

        ips::AddRequest request;
        Controller ctrl;
        // instance init
        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table(kTruncateByCountList);
        // instance init
        // int ins_cnt =ips::Rand::randi64(20, 50);
        int ins_cnt = 10;
        if (ins_cnt % 2 != 0) {
            ins_cnt += 1;
        }

        std::vector<int64_t> ins_ts_vec;
        ins_ts_vec.reserve(ins_cnt);
        for (int64_t i = 0; i < ins_cnt; ++i) {
            // int64_t cur_ts = ips::GetCurTsMicros();
            int64_t cur_ts = cur_uid + i;
            ins_ts_vec.emplace_back(cur_ts);

            auto instance = ips_add_request->add_instance_list();
            instance->set_table(0);
            instance->set_action_type(0);
            instance->set_uid(cur_uid);
            instance->set_ts(cur_ts);
            // init a FeatureStat32
            auto fs = instance->add_feature_stat32_list();
            fs->set_id(i);
            fs->set_type(1);

            auto int_list = fs->mutable_int_list();

            fs->set_slot(0);
            fs->set_has_slot(true);
            int_list->add_v_list(1);
            int_list->add_v_list(1);
            int_list->add_v_list(1);
            int_list->add_v_list(1);
        }

        // Add
        {
            ips::AddResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid), request,
                                 &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        }

        {
            ips::BatchQueryRequest request;
            auto ips_batch_query_request = &request;

            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kTruncateByCountList);

            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();

            data_range->set_type(ips::DataRangeType::RELATIVE_TIME_MICROS_ACTION_TYPE);

            int64_t mid_ins = ins_cnt / 2;
            int64_t mid_ts = ins_ts_vec[mid_ins];
            data_range->set_range_val(ins_ts_vec.back() - ins_ts_vec[mid_ins] + 1);

            filter->set_top_k(ins_cnt);
            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_optor(ips::FeatureStatOperator::SORT_BY_TS);

            filter->add_index(0);
            filter->add_index(1);
            filter->add_index(3);
            filter->set_has_index(true);

            filter->add_min_index_count(ins_cnt);
            filter->add_min_index_count(ins_cnt);
            filter->add_min_index_count(ins_cnt);
            // filter->mutable_min_index_count()->CopyFrom({1,3,2});
            filter->set_has_min_index_count(true);

            ips::BatchQueryResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY), std::to_string(cur_uid),
                                 request, &response);
            auto cur_rsp = response.rsps(0);
            // std::cout << cur_rsp.DebugString() << std::endl;
            auto fs32_vec_size = cur_rsp.feature_stat32_list_size();
            ASSERT_EQ(fs32_vec_size, mid_ins);
            int64_t expected_fid = ins_cnt - 1;
            for (int fs_idx = 0; fs_idx < cur_rsp.feature_stat32_list_size(); fs_idx++) {
                ASSERT_EQ(cur_rsp.feature_stat32_list(fs_idx).id(), expected_fid);
                ASSERT_GE(cur_rsp.feature_stat32_list(fs_idx).int_list().timestamp(), mid_ts);
                --expected_fid;
            }
        }
    }
}

TEST_F(IpsModuleTest, TestFilterByFidQuery) {
    int loop_cnt = 50;
    // int loop_cnt = ips::Rand::randi64(100, 200);
    while (loop_cnt >= 0) {
        --loop_cnt;

        int64_t cur_uid = ips::GetCurTsMicros();
        // instance init
        int64_t fid_num = ips::Rand::randi64(10, 50);
        int64_t feature_val = ips::Rand::randi64(10, 50);
        int64_t ins_cnt = fid_num * feature_val;

        ips::AddRequest request;
        Controller ctrl;
        // instance init
        auto ips_add_request = &request;
        ips_add_request->set_enable_server_aggregator(true);
        ips_add_request->set_table(kCompress);

        for (int64_t i = 0; i < ins_cnt; ++i) {
            int64_t cur_ts = ips::GetCurTsMicros();
            auto instance = ips_add_request->add_instance_list();
            instance->set_table(0);
            instance->set_action_type(0);
            instance->set_uid(cur_uid);
            instance->set_ts(cur_ts);

            // init a FeatureStat32

            auto fs = instance->add_feature_stat32_list();
            fs->set_id(i % fid_num);
            fs->set_type(0);

            auto int_pair = fs->mutable_int_pair();

            fs->set_slot(0);
            fs->set_has_slot(true);
            int_pair->set_v1(1);
            int_pair->set_v2(1);
        }

        // Add
        {
            ips::AddResponse response;
            light_table->Execute(&ctrl, MakeCmdId(IPS, ips::ADD), std::to_string(cur_uid), request,
                                 &response);
            ASSERT_EQ(ctrl.status.code(), static_cast<int>(Code::OK));
        }

        {
            ips::BatchQueryRequest request;
            auto ips_batch_query_request = &request;
            ips::QueryRequest* query_request = ips_batch_query_request->add_reqs();
            query_request->set_uid(cur_uid);
            query_request->set_decoupled(false);
            query_request->set_table(kCompress);

            auto data_range = query_request->mutable_data_range();
            auto filter = query_request->mutable_filter();

            data_range->set_type(ips::DataRangeType::LAST_INSTANCES);
            data_range->set_range_val(ips::GetCurTsMicros());

            filter->set_top_k(ins_cnt);
            filter->set_table(0);
            filter->set_action_type(0);
            filter->set_slot(0);
            filter->set_optor(ips::FeatureStatOperator::FILTER_BY_FID);
            filter->set_has_fid_list(true);
            for (size_t i = 0; i < (size_t)fid_num; ++i) {
                filter->add_fid_list(i);

                ips::BatchQueryResponse response;
                light_table->Execute(&ctrl, MakeCmdId(IPS, ips::BATCH_QUERY),
                                     std::to_string(cur_uid), request, &response);

                auto cur_rsp = response.rsps(0);
                auto fs32_vec_size = cur_rsp.feature_stat32_list_size();
                ASSERT_EQ(fs32_vec_size, filter->fid_list_size());
                ASSERT_TRUE(cur_rsp.uid() == cur_uid);
                for (int fs_idx = 0; fs_idx < cur_rsp.feature_stat32_list_size(); fs_idx++) {
                    ASSERT_EQ(cur_rsp.feature_stat32_list(fs_idx).type(), 0);
                    ASSERT_LE(cur_rsp.feature_stat32_list(fs_idx).id(), filter->fid_list_size());
                    ASSERT_EQ(cur_rsp.feature_stat32_list(fs_idx).int_pair().v1(), feature_val);
                    ASSERT_EQ(cur_rsp.feature_stat32_list(fs_idx).int_pair().v2(), feature_val);
                }
            }
        }
    }
}

}  // namespace swig
}  // namespace bcache2
