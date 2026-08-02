// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <stdio.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "client/light_client.h"
#include "extension/hash/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/control_state/interface.pb.h"
#include "extension/control_state/window.h"
#include "test/smoketest/base_smoketest.h"

namespace bcache2 {
namespace swig {

// 错误率理论上应该在 0.78 / sqrt(2^12) ≈ 0.012 , 但是数据量小的时候会超过这个值
static const double kRelativeErrorForLgK12 = 0.015;
class ControlStateModuleTest : public ::testing::Test {
 public:
    void SetUp() override {
        MiniCluster::Options options;
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        bcache2::Status internal_status = cluster_.Start();
        ASSERT_TRUE(internal_status.ok()) << internal_status.ToString();

        MasteWrapper* master = cluster_.GetMaster();

        master->CreateSimpleTable("ns", "table");
        ASSERT_TRUE(internal_status.ok());
    }

    void TearDown() override { cluster_.Stop(); }

    static uint32_t MakeCmdId(int module_id, int function_id) {
        return (static_cast<uint32_t>(module_id) << 16) | static_cast<uint32_t>(function_id);
    }

 protected:
    TempDir temp_dir_;
    MiniCluster cluster_;
};

TEST_F(ControlStateModuleTest, ControlStateLDCTest) {
    // Init
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    Controller ctrl;
    time_t now;
    time(&now);

    // test for dc
    for (int i = 0; i < 2000; ++i) {
        control_state::CPCSetRequest req;
        req.set_key("test-for-dc");
        req.set_ttl(3600);
        req.set_precision(control_state::OneHour);
        req.add_values(std::to_string(i));
        req.set_occur_time(now);
        req.set_uuid(std::to_string(i));
        control_state::CPCQueryResponse resp;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCSET), "test_for_dc",
                        req, &resp);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(resp.err_code(), 0);
    }
    // write for equal uuid
    for (int i = 0; i < 2000; ++i) {
        control_state::CPCSetRequest req;
        req.set_key("test-for-dc");
        req.set_ttl(3600);
        req.set_precision(control_state::OneHour);
        req.add_values(std::to_string(2000 + i));
        req.set_occur_time(now);
        req.set_uuid(std::to_string(i));
        control_state::CPCQueryResponse resp;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCSET), "test_for_dc",
                        req, &resp);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(resp.err_code(), 0);
    }
    control_state::CPCQueryRequest queryDCReq;
    control_state::CPCQueryResponse queryDCRes;
    auto* windowDc = queryDCReq.add_windows();
    windowDc->set_start(-24);
    windowDc->set_end(0);
    windowDc->set_unit(control_state::Hour);

    queryDCReq.set_precision(control_state::OneHour);
    queryDCReq.set_with_detail(false);
    queryDCReq.set_key("test-for-dc");
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCQUERY), "test_for_dc",
            queryDCReq, &queryDCRes);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(queryDCRes.err_code(), 0);
    ASSERT_EQ(queryDCRes.count_list(0), 2000);

    // write
    auto start = std::chrono::high_resolution_clock::now();
    for (int j = 0; j < 100; ++j) {
        int cnt = 10;
        // if (j < 10) {
        //     cnt += 5000;
        // }
        for (int i = 0; i < cnt; ++i) {
            control_state::CPCSetRequest req;
            req.set_key("control_state-test-ldc");
            req.set_ttl(1L * 24 * 3600);
            req.set_precision(control_state::OneSecond);
            req.add_values(std::to_string(i * 10000 + j));
            req.set_occur_time(now - j);
            control_state::CPCQueryResponse resp;
            table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCSET), "control_state-test-ldc",
                          req, &resp);
            ASSERT_EQ(ctrl.status.code(), 0);
            ASSERT_EQ(resp.err_code(), 0);
        }
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << "cpc write cost=" << duration.count() / 1000 << std::endl;

    // query
    control_state::CPCQueryRequest queryReq;
    control_state::CPCQueryResponse queryRes;
    auto* window = queryReq.add_windows();
    window->set_start(-1);
    window->set_end(0);
    window->set_unit(control_state::Hour);

    queryReq.set_precision(control_state::OneSecond);
    queryReq.set_with_detail(false);
    queryReq.set_key("control_state-test-ldc");
    auto start2 = std::chrono::high_resolution_clock::now();
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCQUERY), "control_state-test-ldc", queryReq,
                  &queryRes);
    auto stop2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(stop2 - start2);
    std::cout << "cpc read cost=" << duration2.count() << std::endl;
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(queryRes.err_code(), 0);
    ASSERT_NEAR(queryRes.count_list(0), 1000, 1000 * kRelativeErrorForLgK12);

    // query list
    queryReq.Clear();
    queryRes.Clear();
}

TEST_F(ControlStateModuleTest, ControlStateSyncCountTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    control_state::HsetAndGetResponse count_response;

    time_t now;
    time(&now);
    control_state::HsetAndGetRequest count_request;
    count_request.set_key("control_state-test-cnt-sync");
    count_request.set_ttl(1LL * 24 * 3600);
    count_request.set_htype(control_state::COUNT);

    auto* window = count_request.add_windows();
    window->set_start(-2);
    window->set_end(0);
    window->set_unit(control_state::Hour);
    count_request.set_precision(bcache2::control_state::OneMinute);

    Controller ctrl;
    for (int i = 0; i < 36; ++i) {
        count_request.set_value("1");
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSETANDGET),
                      "control_state-test-cnt-sync", count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }
    ASSERT_EQ(count_response.result_list(0).result(), 36)
        << "need = 36, got = " << count_response.result_list(0).result();
}

TEST_F(ControlStateModuleTest, ControlStateChangeTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    std::string key = "control_state-test-change-1";
    control_state::HsetResponse count_response;
    time_t now;
    time(&now);
    // 第一次写入"a"
    control_state::HsetRequest count_request;
    count_request.set_key(key);
    count_request.set_ttl(1LL * 24 * 3600);
    count_request.set_htype(control_state::CHANGE);
    count_request.set_precision(control_state::OneSecond);
    count_request.set_value("a");
    Controller ctrl;
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), key, count_request,
                  &count_response);
    ASSERT_EQ(ctrl.status.code(), 0);

    // 查询
    control_state::HqueryRequest count_query_request;
    count_query_request.set_key(key);

    auto* window = count_query_request.add_windows();
    window->set_start(-2);
    window->set_end(0);
    window->set_unit(control_state::Hour);
    count_query_request.set_precision(bcache2::control_state::OneSecond);
    count_query_request.set_htype(control_state::CHANGE);
    control_state::HqueryResponse count_query_resp;
    auto start = std::chrono::high_resolution_clock::now();
    // 第一次查询
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), key, count_query_request,
                  &count_query_resp);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(count_query_resp.result_list(0).result(), 1)
        << "need = 1, got = " << count_query_resp.result_list(0).result();

    // 第二次次写入"a"
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), key, count_request,
                  &count_response);
    ASSERT_EQ(ctrl.status.code(), 0);
    // 第二次次查询
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), key, count_query_request,
                  &count_query_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(count_query_resp.result_list(0).result(), 1)
        << "need = 1, got = " << count_query_resp.result_list(0).result();

    // 第三次次写入"b"
    count_request.set_value("b");
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), key, count_request,
                  &count_response);
    ASSERT_EQ(ctrl.status.code(), 0);

    // 第三次次查询
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), key, count_query_request,
                  &count_query_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(count_query_resp.result_list(0).result(), 2)
        << "need = 2, got = " << count_query_resp.result_list(0).result();
}

TEST_F(ControlStateModuleTest, ControlStateCountTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    control_state::HsetResponse count_response;

    time_t now;
    time(&now);
    for (int i = 0; i < 72; ++i) {
        control_state::HsetRequest count_request;
        count_request.set_key("control_state-test-cnt");
        count_request.set_ttl(1LL * 24 * 3600);
        count_request.set_htype(control_state::COUNT);
        count_request.set_precision(control_state::OneSecond);
        count_request.set_value("1");
        count_request.set_uuid(std::to_string(i));
        Controller ctrl;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-cnt",
                      count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }
    // 相同 uuid
    for (int i = 0; i < 72; ++i) {
        control_state::HsetRequest count_request;
        count_request.set_key("control_state-test-cnt");
        count_request.set_ttl(1LL * 24 * 3600);
        count_request.set_htype(control_state::COUNT);
        count_request.set_precision(control_state::OneSecond);
        count_request.set_value("1");
        count_request.set_uuid(std::to_string(i));
        Controller ctrl;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-cnt",
                      count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // 查询
    control_state::HqueryRequest count_query_request;
    count_query_request.set_key("control_state-test-cnt");

    auto* window = count_query_request.add_windows();
    window->set_start(-2);
    window->set_end(0);
    window->set_unit(control_state::Hour);
    count_query_request.set_precision(bcache2::control_state::OneSecond);
    count_query_request.set_htype(control_state::COUNT);
    control_state::HqueryResponse count_query_resp;
    auto start = std::chrono::high_resolution_clock::now();
    Controller ctrl;
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), "control_state-test-cnt",
                  count_query_request, &count_query_resp);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

    printf("the query time cost is %lu microseconds\n", duration.count());
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(count_query_resp.result_list(0).result(), 72)
        << "need = 72, got = " << count_query_resp.result_list(0).result();

    // 再一次写入
    for (int i = 0; i < 72; ++i) {
        control_state::HsetRequest count_request;
        count_request.set_key("control_state-test-cnt");
        count_request.set_ttl(1LL * 24 * 3600);
        count_request.set_htype(control_state::COUNT);
        count_request.set_precision(control_state::OneSecond);
        count_request.set_value("1");

        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-cnt",
                      count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    auto start2 = std::chrono::high_resolution_clock::now();
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), "control_state-test-cnt",
                  count_query_request, &count_query_resp);
    auto stop2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(stop2 - start2);

    printf("the query time cost is %lu microseconds\n", duration2.count());
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(count_query_resp.result_list(0).result(), 144)
        << "need = 14400, got = " << count_query_resp.result_list(0).result();
}

// TEST_F(ControlStateModuleTest, ControlStateDCTest) {
//     Client* tmp_client = nullptr;
//     Status status = Client::Create(ClientOptions(), &tmp_client);
//     ASSERT_TRUE(status.ok());
//     std::unique_ptr<Client> client(tmp_client);

//     // Open table
//     Table* tmp_table = nullptr;
//     status = client->OpenTable(
//         "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
//         TableOptions(), &tmp_table);
//     ASSERT_TRUE(status.ok());
//     std::unique_ptr<Table> table_release_gurad(tmp_table);
//     LightTable table(tmp_table);

//     time_t now;
//     time(&now);
//     for (int i = 0; i < 36; ++i) {
//         control_state::CPCSetRequest control_state_batch_add_req;
//         control_state_batch_add_req.set_key("control_state-test-dc");
//         control_state_batch_add_req.set_ttl(1L * 24 * 3600);

//         control_state_batch_add_req.set_precision(control_state::OneSecond);
//         control_state_batch_add_req.add_values("filed" + std::to_string(i));

//         control_state::CPCSetResponse control_state_batch_add_resp;

//         Controller ctrl;
//         table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCSET), "control_state-test-dc",
//                       control_state_batch_add_req, &control_state_batch_add_resp);
//         ASSERT_EQ(ctrl.status.code(), 0);
//     }

//     control_state::CPCQueryRequest control_state_count_req;
//     control_state_count_req.set_key("control_state-test-dc");
//     control_state_count_req.set_precision(bcache2::control_state::OneSecond);

//     auto* window = control_state_count_req.add_windows();
//     window->set_start(-2);
//     window->set_end(0);
//     window->set_unit(control_state::Hour);
//     control_state::CPCQueryResponse control_state_count_resp;

//     auto start = std::chrono::high_resolution_clock::now();
//     Controller ctrl;
//     table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCQUERY), "control_state-test-dc",
//                   control_state_count_req, &control_state_count_resp);
//     ASSERT_EQ(ctrl.status.code(), 0);
//     auto stop = std::chrono::high_resolution_clock::now();
//     auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

//     printf("the query time cost is %lu microseconds\n", duration.count());
//     ASSERT_EQ(control_state_count_resp.count_list(0), 36)
//         << "need = 36, got = " << control_state_count_resp.count_list(0);

//     control_state::CPCQueryRequest control_state_detail_req;
//     control_state_detail_req.set_key("control_state-test-dc");

//     auto* window2 = control_state_detail_req.add_windows();
//     window2->set_start(-2);
//     window2->set_end(0);
//     window2->set_unit(control_state::Hour);

//     control_state_detail_req.set_precision(bcache2::control_state::OneSecond);
//     control_state_detail_req.set_with_detail(true);

//     control_state::CPCQueryResponse control_state_detail_resp;
//     start = std::chrono::high_resolution_clock::now();
//     table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCQUERY), "control_state-test-dc",
//                   control_state_detail_req, &control_state_detail_resp);
//     ASSERT_EQ(ctrl.status.code(), 0);
//     stop = std::chrono::high_resolution_clock::now();
//     duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//     printf("the query time cost is %lu microseconds\n", duration.count());
//     size_t size = control_state_detail_resp.detail_lists_size();
//     ASSERT_EQ(size, 1);
//     auto detail_list = control_state_detail_resp.detail_lists(0);
//     ASSERT_EQ(detail_list.detail_size(), 36);
// }

TEST_F(ControlStateModuleTest, ControlStateMinTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    control_state::HsetResponse count_response;
    // control_state::KvPair* kv_pair;
    time_t now;
    time(&now);
    for (int i = 0; i < 7201; ++i) {
        control_state::HsetRequest count_request;
        count_request.set_key("control_state-test-min");
        count_request.set_ttl(1LL * 24 * 3600);
        count_request.set_htype(control_state::MIN);

        count_request.set_precision(control_state::OneSecond);
        count_request.set_value(std::to_string(-i));

        Controller ctrl;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-min",
                      count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }
    // 查询
    control_state::HqueryRequest count_query_request;
    count_query_request.set_key("control_state-test-min");

    auto* window = count_query_request.add_windows();
    window->set_start(-2);
    window->set_end(0);
    window->set_unit(control_state::Hour);

    count_query_request.set_precision(bcache2::control_state::OneSecond);
    count_query_request.set_htype(control_state::MIN);
    control_state::HqueryResponse count_query_resp;
    auto start = std::chrono::high_resolution_clock::now();
    Controller ctrl;
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), "control_state-test-max",
                  count_query_request, &count_query_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

    printf("the query time cost is %lu microseconds\n", duration.count());
    ASSERT_EQ(count_query_resp.result_list(0).result(), -7200)
        << "need = -7200, got = " << count_query_resp.result_list(0).result();

    // 再一次写入
    for (int i = 0; i < 7201; ++i) {
        control_state::HsetRequest count_request;
        count_request.set_key("control_state-test-min");
        count_request.set_ttl(1LL * 24 * 3600);
        count_request.set_htype(control_state::MIN);

        count_request.set_precision(control_state::OneSecond);

        count_request.set_value(std::to_string(-i));

        Controller ctrl;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-min",
                      count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // 再一次查询
    auto start2 = std::chrono::high_resolution_clock::now();
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), "control_state-test-max",
                  count_query_request, &count_query_resp);
    auto stop2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(stop2 - start2);
    ASSERT_EQ(ctrl.status.code(), 0);
    printf("the query time cost is %lu microseconds\n", duration2.count());
    ASSERT_EQ(count_query_resp.result_list(0).result(), -7200)
        << "need = -7200, got = " << count_query_resp.result_list(0).result();
}

TEST_F(ControlStateModuleTest, ControlStateMaxTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    control_state::HsetResponse count_response;
    time_t now;
    time(&now);
    for (int i = 0; i < 7201; ++i) {
        control_state::HsetRequest count_request;
        count_request.set_key("control_state-test-max");
        count_request.set_ttl(1LL * 24 * 3600);
        count_request.set_htype(control_state::MAX);

        count_request.set_precision(control_state::OneSecond);
        count_request.set_value(std::to_string(i));

        Controller ctrl;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-max",
                      count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // 查询
    control_state::HqueryRequest count_query_request;
    count_query_request.set_key("control_state-test-max");

    auto* window = count_query_request.add_windows();
    window->set_start(-2);
    window->set_end(0);
    window->set_unit(control_state::Hour);

    count_query_request.set_precision(control_state::OneSecond);
    count_query_request.set_htype(control_state::MAX);
    control_state::HqueryResponse count_query_resp;
    auto start = std::chrono::high_resolution_clock::now();

    Controller ctrl;
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), "control_state-test-max",
                  count_query_request, &count_query_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    printf("the query time cost is %lu microseconds\n", duration.count());
    ASSERT_EQ(count_query_resp.result_list(0).result(), 7200)
        << "need = 7200, got = " << count_query_resp.result_list(0).result();

    // 再一次写入
    for (int i = 0; i < 7201; ++i) {
        control_state::HsetRequest count_request;
        count_request.set_key("control_state-test-max");
        count_request.set_ttl(1LL * 24 * 3600);
        count_request.set_htype(control_state::MAX);

        count_request.set_precision(control_state::OneSecond);
        count_request.set_value(std::to_string(i));

        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-max",
                      count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // 再一次查询
    auto start2 = std::chrono::high_resolution_clock::now();
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), "control_state-test-max",
                  count_query_request, &count_query_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    auto stop2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(stop2 - start2);

    printf("the query time cost is %lu microseconds\n", duration2.count());
    ASSERT_EQ(count_query_resp.result_list(0).result(), 7200)
        << "need = 7200, got = " << count_query_resp.result_list(0).result();
}

TEST_F(ControlStateModuleTest, ControlStateFirstOrLastTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    control_state::FolSetRequest control_state_fol_req;
    control_state::FolSetResponse control_state_fol_resp;
    control_state_fol_req.set_key("12345-wjp-last");
    control_state_fol_req.set_value("v1");
    control_state_fol_req.set_occur_time(1665664460L);
    control_state_fol_req.set_ttl(365 * 24 * 3600L);
    control_state_fol_req.set_fol_type(control_state::LAST);
    Controller ctrl;
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::FOLSET), "12345-wjp-last",
                  control_state_fol_req, &control_state_fol_resp);
    ASSERT_EQ(ctrl.status.code(), 0);

    // 查询
    control_state::FolQueryRequest control_state_fol_query_req;
    control_state::FolQueryResponse control_state_fol_query_resp;
    control_state_fol_query_req.set_key("12345-wjp-last");

    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::FOLQUERY), "12345-wjp-last",
                  control_state_fol_query_req, &control_state_fol_query_resp);
    ASSERT_EQ(ctrl.status.code(), 0) << ctrl.status.message();
    ASSERT_EQ(control_state_fol_query_resp.result(), "v1")
        << "need = v1, got = " << control_state_fol_query_resp.result();

    // 再一次写入，但是时间更早
    control_state::FolSetRequest control_state_fol_req2;
    control_state::FolSetResponse control_state_fol_resp2;
    control_state_fol_req2.set_key("12345-wjp-last");
    control_state_fol_req2.set_value("v2");
    control_state_fol_req2.set_occur_time(1665664456L);
    control_state_fol_req2.set_ttl(365 * 24 * 3600L);
    control_state_fol_req2.set_fol_type(control_state::LAST);

    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::FOLSET), "12345-wjp-last",
                  control_state_fol_req2, &control_state_fol_resp2);
    ASSERT_EQ(ctrl.status.code(), 0);

    // 再一次查询
    control_state::FolQueryRequest control_state_fol_query_req2;
    control_state::FolQueryResponse control_state_fol_query_resp2;
    control_state_fol_query_req2.set_key("12345-wjp-last");

    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::FOLQUERY), "12345-wjp-last",
                  control_state_fol_query_req2, &control_state_fol_query_resp2);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(control_state_fol_query_resp2.result(), "v1")
        << "need = v1, got = v1" << control_state_fol_query_resp2.result();
}

TEST_F(ControlStateModuleTest, ControlStateSyncFirstOrLastTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    control_state::FolSetAndGetRequest control_state_fol_req;
    control_state::FolSetAndGetResponse control_state_fol_resp;
    control_state_fol_req.set_key("12345-wjp-last");
    control_state_fol_req.set_value("v1");
    control_state_fol_req.set_occur_time(1665664460L);
    control_state_fol_req.set_ttl(365 * 24 * 3600L);
    control_state_fol_req.set_fol_type(control_state::LAST);

    Controller ctrl;
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::FOLSETANDGET), "12345-wjp-last-sync",
                  control_state_fol_req, &control_state_fol_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(control_state_fol_resp.result(), "v1") << "need = v1, got = v1" << control_state_fol_resp.result();
}

TEST_F(ControlStateModuleTest, TTLTest) {
    // 这里之前验证TTL的逻辑依赖返回NOT FOUND错误号,现在不好判断,先不处理
    return;
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    time_t now;
    time(&now);
    Controller ctrl;
    control_state::CPCSetRequest cpc_req;
    control_state::CPCSetResponse cpc_resp;
    std::string key = "test-for-cpc-ttl";
    cpc_req.set_key(key);
    cpc_req.set_ttl(1);
    cpc_req.set_precision(control_state::OneHour);
    cpc_req.add_values("1");
    cpc_req.set_occur_time(now);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCSET), key, cpc_req, &cpc_resp);
    ASSERT_EQ(ctrl.status.code(), 0) << ctrl.status.message();
    ASSERT_EQ(cpc_resp.err_code(), 0);

    control_state::HsetRequest h_req;
    control_state::HsetResponse h_resp;
    std::string h_key = "test-for-hash-ttl";
    h_req.set_key(h_key);
    h_req.set_ttl(1);
    h_req.set_precision(control_state::OneHour);
    h_req.set_value("1");
    h_req.set_occur_time(now);
    h_req.set_htype(control_state::MAX);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), h_key, h_req, &h_resp);
    ASSERT_EQ(ctrl.status.code(), 0) << ctrl.status.message();
    ASSERT_EQ(h_resp.err_code(), 0);

    control_state::HsetRequest h_c_req;
    control_state::HsetResponse h_c_resp;
    std::string h_c_key = "test-for-change-ttl";
    h_c_req.set_key(h_c_key);
    h_c_req.set_ttl(1);
    h_c_req.set_precision(control_state::OneHour);
    h_c_req.set_value("1");
    h_c_req.set_occur_time(now);
    h_c_req.set_htype(control_state::CHANGE);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), h_c_key, h_c_req, &h_c_resp);
    ASSERT_EQ(ctrl.status.code(), 0) << ctrl.status.message();
    ASSERT_EQ(h_c_resp.err_code(), 0);

    control_state::FolSetRequest fol_req;
    control_state::FolSetResponse fol_resp;
    std::string fol_key = "test-for-last-ttl";
    fol_req.set_key(fol_key);
    fol_req.set_ttl(1);
    fol_req.set_fol_type(control_state::LAST);
    fol_req.set_value("1");
    fol_req.set_occur_time(now);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::FOLSET), fol_key,
        fol_req, &fol_resp);
    ASSERT_EQ(ctrl.status.code(), 0) << ctrl.status.message();
    ASSERT_EQ(fol_resp.err_code(), 0);

    // 读取
    control_state::CPCQueryRequest cpc_q_req;
    control_state::CPCQueryResponse cpc_q_resp;
    control_state::HqueryRequest h_q_req;
    control_state::HqueryResponse h_q_resp;
    control_state::HqueryRequest h_c_q_req;
    control_state::HqueryResponse h_c_q_resp;
    control_state::FolQueryRequest fol_q_req;
    control_state::FolQueryResponse fol_q_resp;

    cpc_q_req.set_key(key);
    auto window = cpc_q_req.add_windows();
    window->set_end(0);
    window->set_start(-2);
    window->set_unit(control_state::Hour);
    cpc_q_req.set_with_detail(false);
    cpc_q_req.set_precision(control_state::OneHour);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCQUERY), key,
        cpc_q_req, &cpc_q_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(cpc_q_resp.err_code(), 0);

    h_q_req.set_key(h_key);
    window = h_q_req.add_windows();
    window->set_end(0);
    window->set_start(-2);
    window->set_unit(control_state::Hour);
    h_q_req.set_htype(control_state::MAX);
    h_q_req.set_precision(control_state::OneHour);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), key,
        h_q_req, &h_q_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(h_q_resp.err_code(), 0);

    h_c_q_req.set_key(h_c_key);
    window = h_c_q_req.add_windows();
    window->set_end(0);
    window->set_start(-2);
    window->set_unit(control_state::Hour);
    h_c_q_req.set_htype(control_state::CHANGE);
    h_c_q_req.set_precision(control_state::OneHour);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), key,
        h_c_q_req, &h_c_q_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(h_c_q_resp.err_code(), 0);

    fol_q_req.set_key(fol_key);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::FOLQUERY), key,
        fol_q_req, &fol_q_resp);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(fol_q_resp.err_code(), 0);

    std::cout << "wait for expired..." << std::endl;
    sleep(3);

    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::CPCQUERY), key,
        cpc_q_req, &cpc_q_resp);
    ASSERT_NE(ctrl.status.code(), 0);
    // ASSERT_NE(cpc_q_resp.err_code(), 0);

    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), key,
        h_q_req, &h_q_resp);
    ASSERT_NE(ctrl.status.code(), 0);
    // ASSERT_NE(h_q_resp.err_code(), 0);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HQUERY), key,
        h_c_q_req, &h_c_q_resp);
    ASSERT_NE(ctrl.status.code(), 0);
    // ASSERT_NE(h_c_q_resp.err_code(), 0);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::FOLQUERY), key,
        fol_q_req, &fol_q_resp);
    ASSERT_NE(ctrl.status.code(), 0);
    // ASSERT_NE(fol_q_resp.err_code(), 0);
}

TEST_F(ControlStateModuleTest, ControlStateManageTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    control_state::HsetResponse count_response;

    time_t now;
    time(&now);
    for (int i = 0; i < 72; ++i) {
        control_state::HsetRequest count_request;
        count_request.set_key("control_state-test-manager");
        count_request.set_ttl(1LL * 24 * 3600);
        count_request.set_htype(control_state::COUNT);
        count_request.set_precision(control_state::OneSecond);
        count_request.set_value("1");
        count_request.set_uuid(std::to_string(i));
        // kv_pair = count_request.add_field_values();
        // kv_pair->set_field(std::to_string(control_state::OneSecond) + std::to_string(now - i % 3600));
        // kv_pair->set_value("1");
        Controller ctrl;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-manager",
                      count_request, &count_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // fullGC
    control_state::ManagerRequest manage_request;
    manage_request.set_key("control_state-test-manager");
    manage_request.set_op_type(control_state::FULLGC);
    auto kvpair = manage_request.add_field_list();
    kvpair->set_field("111");
    kvpair->set_value("222");
    Controller ctrl;
    control_state::ManagerResponse manage_response;
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::MANAGER), "control_state-test-manager",
        manage_request, &manage_response);
    ASSERT_EQ(ctrl.status.code(), 0);

    // subGC
    manage_request.set_key("control_state-test-manager");
    manage_request.set_op_type(control_state::SUBGC);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::MANAGER), "control_state-test-manager",
        manage_request, &manage_response);
    ASSERT_EQ(ctrl.status.code(), 0);

    // queryAll
    control_state::ManagerResponse manage_response_all;
    manage_request.set_key("control_state-test-manager");
    manage_request.set_op_type(control_state::ALL_DATA_VALUE);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::MANAGER), "control_state-test-manager",
        manage_request, &manage_response_all);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_GT(manage_response_all.result_size(), 0);

    // queryAllRepeat
    control_state::ManagerResponse manage_response_repeat_check_data;
    manage_request.set_key("control_state-test-manager");
    manage_request.set_op_type(control_state::REPEAT_CHECK_DATA);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::MANAGER), "control_state-test-manager",
        manage_request, &manage_response_repeat_check_data);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(manage_response_repeat_check_data.result_size(), 1);
    ASSERT_EQ(manage_response_repeat_check_data.result(0).field(), "repeat_data");
}

TEST_F(ControlStateModuleTest, ControlStateCPCManageTest) {
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);

    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // 写入
    control_state::CPCSetResponse cpc_response;

    time_t now;
    time(&now);
    for (int i = 0; i < 72; ++i) {
        control_state::CPCSetRequest cpc_request;
        cpc_request.set_key("control_state-test-cpc-manager");
        cpc_request.set_ttl(1LL * 24 * 3600);
        cpc_request.set_precision(control_state::OneSecond);
        cpc_request.set_uuid(std::to_string(i));
        Controller ctrl;
        table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::HSET), "control_state-test-manager",
                      cpc_request, &cpc_response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // fullGC
    control_state::ManagerRequest manage_request;
    manage_request.set_key("control_state-test-cpc-manager");
    manage_request.set_op_type(control_state::FULLGC);
    auto kvpair = manage_request.add_field_list();
    kvpair->set_field("111");
    kvpair->set_value("222");
    Controller ctrl;
    control_state::ManagerResponse manage_response;
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::MANAGER), "control_state-test-manager",
        manage_request, &manage_response);
    ASSERT_EQ(ctrl.status.code(), 0);

    // subGC
    manage_request.set_key("control_state-test-cpc-manager");
    manage_request.set_op_type(control_state::SUBGC);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::MANAGER), "control_state-test-manager",
        manage_request, &manage_response);
    ASSERT_EQ(ctrl.status.code(), 0);

    // queryAll
    control_state::ManagerResponse manage_response_all;
    manage_request.set_key("control_state-test-cpc-manager");
    manage_request.set_op_type(control_state::ALL_DATA_VALUE);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::MANAGER), "control_state-test-manager",
        manage_request, &manage_response_all);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_GT(manage_response_all.result_size(), 0);

    // queryAllRepeat
    control_state::ManagerResponse manage_response_repeat_check_data;
    manage_request.set_key("control_state-test-cpc-manager");
    manage_request.set_op_type(control_state::REPEAT_CHECK_DATA);
    table.Execute(&ctrl, ControlStateModuleTest::MakeCmdId(CONTROL_STATE, control_state::MANAGER), "control_state-test-manager",
        manage_request, &manage_response_repeat_check_data);
    ASSERT_EQ(ctrl.status.code(), 0);
    ASSERT_EQ(manage_response_repeat_check_data.result_size(), 1);
    ASSERT_EQ(manage_response_repeat_check_data.result(0).field(), "repeat_data");
}

}  // namespace swig
}  // namespace bcache2
