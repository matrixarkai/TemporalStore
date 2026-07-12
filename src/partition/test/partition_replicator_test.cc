// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <random>

#include "bench/bench.h"
#include "bench/client/brpc_client.h"
#include "bench/workloads/common_workload.h"
#include "bench/workloads/hash_workload.h"
#include "bench/workloads/string_workload.h"
#include "common/ratio_dice.h"
#include "common/slot.h"
#include "extension/string/interface.pb.h"
#include "model/hash_model.h"
#include "model/string_model.h"
#include "partition/partition.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/replicator.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/storage_manager.h"
#include "server/server.h"
#include "server/service.h"
#include "stream/log_based_stream.h"
#include "stream/log_based_util.h"
#include "stream/stream.h"
#include "test/common/temp_dir.h"
#include "test/common/util.h"

DECLARE_double(index_gc_usage_trigger);
DECLARE_uint64(index_gc_max_num_per_round);
DECLARE_uint64(index_gc_bytes_threshold);
DECLARE_uint64(stream_max_blob_size);
DECLARE_uint64(stream_blob_deletion_min_age);
DECLARE_uint64(stream_blob_deletion_min_gap);
DECLARE_int32(storage_zone_size);
DECLARE_double(storage_gc_space_utility_threshold);
DECLARE_uint64(storage_gc_max_bytes_per_round);
DECLARE_uint64(storage_gc_max_slots_per_round);
DECLARE_uint64(storage_gc_zone_destroy_delay_ms);
DECLARE_uint64(replicator_loop_interval_us);
DECLARE_uint64(replicator_loop_interval_us);
DECLARE_uint64(replicator_max_oplog_per_loop);
DECLARE_uint64(replicator_max_indexlog_per_loop);
DECLARE_uint64(replicator_out_of_sync_s);
DECLARE_uint64(replicator_update_remote_interval_ms);
DECLARE_uint64(storage_oplog_delay_dump_length);
DECLARE_uint64(server_stopping_wait_s);

namespace bcache2 {
namespace partition {
namespace test {

static thread_local std::random_device rd;
static thread_local std::mt19937 rng(rd());

struct FlagsSetter {
    uint64_t stream_max_blob_size = 0;
    uint64_t stream_blob_deletion_min_age = 0;
    uint64_t stream_blob_deletion_min_gap = 0;
    uint64_t storage_zone_size = 0;
    uint64_t storage_gc_zone_destroy_delay_ms = 0;
    uint64_t replicator_out_of_sync_s = 0;
    uint64_t index_gc_bytes_threshold = 0;
    double storage_gc_space_utility_threshold = 0.0;
    double index_gc_usage_trigger = 0.0;

    FlagsSetter() {
        stream_max_blob_size = FLAGS_stream_max_blob_size;
        stream_blob_deletion_min_age = FLAGS_stream_blob_deletion_min_age;
        stream_blob_deletion_min_gap = FLAGS_stream_blob_deletion_min_gap;
        storage_zone_size = FLAGS_storage_zone_size;
        storage_gc_zone_destroy_delay_ms = FLAGS_storage_gc_zone_destroy_delay_ms;
        replicator_out_of_sync_s = FLAGS_replicator_out_of_sync_s;
        storage_gc_space_utility_threshold = FLAGS_storage_gc_space_utility_threshold;
        index_gc_usage_trigger = FLAGS_index_gc_usage_trigger;
        index_gc_bytes_threshold = FLAGS_index_gc_bytes_threshold;

        FLAGS_stream_max_blob_size = stream::kBlockSize + 512 * 1024;
        FLAGS_stream_blob_deletion_min_gap = 100;
        FLAGS_storage_zone_size = FLAGS_stream_max_blob_size * 1.1;
        FLAGS_replicator_out_of_sync_s = 5;
        FLAGS_storage_gc_zone_destroy_delay_ms = 10 * 1000;
        FLAGS_stream_blob_deletion_min_age = 10;
        FLAGS_storage_gc_space_utility_threshold = 0.8;
        FLAGS_index_gc_usage_trigger = 10000;
        FLAGS_index_gc_bytes_threshold = 102400;
        FLAGS_enable_blockcache = true;
        FLAGS_blockcache_dram_capacity = 134217728;  // 128 MB
        FLAGS_blockcache_ssd_capacity = 134217728;   // 128 MB
        FLAGS_server_stopping_wait_s = 0;
    }

    ~FlagsSetter() {
        FLAGS_stream_max_blob_size = stream_max_blob_size;
        FLAGS_stream_blob_deletion_min_age = stream_blob_deletion_min_age;
        FLAGS_stream_blob_deletion_min_gap = stream_blob_deletion_min_gap;
        FLAGS_storage_zone_size = storage_zone_size;
        FLAGS_storage_gc_zone_destroy_delay_ms = storage_gc_zone_destroy_delay_ms;
        FLAGS_replicator_out_of_sync_s = replicator_out_of_sync_s;
        FLAGS_storage_gc_space_utility_threshold = storage_gc_space_utility_threshold;
        FLAGS_index_gc_bytes_threshold = index_gc_bytes_threshold;
        FLAGS_index_gc_usage_trigger = index_gc_usage_trigger;
    }
};

class PartitionReplicatorTest : public testing::Test {
 public:
    void SetUp() {
        FLAGS_storage_oplog_delay_dump_length = 0;

        matrixobjectstore_init();
        partition_id_ = butil::fast_rand();

        StartServer(&master_, RandomPort());
        LoadPartition(&master_, &master_partition_, false);

        StartServer(&slave_, RandomPort());
        LoadPartition(&slave_, &slave_partition_, true);
    }

    void TearDown() {
        matrixobjectstore_shutdown();
        master_.Stop();
        slave_.Stop();
        sleep(1);
    }

    void SetString(server::Server* server, const std::string& key, const std::string& value,
                   Status* status) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        ExecuteCmdRequest request;
        ExecuteCmdResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        request.set_load_version(UINT64_MAX);
        request.mutable_request()->mutable_string_request()->mutable_set_request()->set_key(key);
        request.mutable_request()->mutable_string_request()->mutable_set_request()->set_value(
            value);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.ExecuteCmd(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();

        *status = Status::FromRpcStatus(response.response().status());
        if (!status->ok()) {
            return;
        }
        *status = Status::FromRpcStatus(response.response().response_status());
        if (!status->ok()) {
            return;
        }
    }

    void HSet(server::Server* server, const std::string& key, const std::string& field,
              const std::string& value, Status* status) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        ExecuteCmdRequest request;
        ExecuteCmdResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        request.set_load_version(UINT64_MAX);
        request.mutable_request()->mutable_hash_request()->mutable_set_request()->set_key(key);
        request.mutable_request()->mutable_hash_request()->mutable_set_request()->set_field(field);
        request.mutable_request()->mutable_hash_request()->mutable_set_request()->set_value(value);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.ExecuteCmd(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();

        *status = Status::FromRpcStatus(response.response().status());
        if (!status->ok()) {
            return;
        }
        *status = Status::FromRpcStatus(response.response().response_status());
        if (!status->ok()) {
            return;
        }
    }

    void HGet(server::Server* server, const std::string& key, const std::string& field,
              std::string* value, Status* status) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        ExecuteCmdRequest request;
        ExecuteCmdResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        request.set_load_version(UINT64_MAX);
        request.mutable_request()->mutable_hash_request()->mutable_get_request()->set_key(key);
        request.mutable_request()->mutable_hash_request()->mutable_get_request()->set_field(field);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.ExecuteCmd(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();

        *status = Status::FromRpcStatus(response.response().status());
        if (!status->ok()) {
            return;
        }
        *status = Status::FromRpcStatus(response.response().response_status());
        if (!status->ok()) {
            return;
        }
        *value = response.response().hash_response().get_response().value();
    }

    void Delete(server::Server* server, const std::string& key, Status* status) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        ExecuteCmdRequest request;
        ExecuteCmdResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        request.set_load_version(UINT64_MAX);
        request.mutable_request()->mutable_common_request()->mutable_del_object_request()->set_key(
            key);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.ExecuteCmd(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();

        *status = Status::FromRpcStatus(response.response().status());
        if (!status->ok()) {
            return;
        }
        *status = Status::FromRpcStatus(response.response().response_status());
        if (!status->ok()) {
            return;
        }
    }

    void Expire(server::Server* server, const std::string& key, uint64_t ttl_ms, Status* status) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        ExecuteCmdRequest request;
        ExecuteCmdResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        request.set_load_version(UINT64_MAX);
        request.mutable_request()->mutable_common_request()->mutable_expire_request()->set_key(key);
        request.mutable_request()->mutable_common_request()->mutable_expire_request()->set_ttl_ms(
            ttl_ms);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.ExecuteCmd(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();

        *status = Status::FromRpcStatus(response.response().status());
        if (!status->ok()) {
            return;
        }
        *status = Status::FromRpcStatus(response.response().response_status());
        if (!status->ok()) {
            return;
        }
    }

    void Ttl(server::Server* server, const std::string& key, uint64_t* ttl_ms, Status* status) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        ExecuteCmdRequest request;
        ExecuteCmdResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        request.set_load_version(UINT64_MAX);
        request.mutable_request()->mutable_common_request()->mutable_ttl_request()->set_key(key);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.ExecuteCmd(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();

        *status = Status::FromRpcStatus(response.response().status());
        if (!status->ok()) {
            return;
        }
        *status = Status::FromRpcStatus(response.response().response_status());
        if (!status->ok()) {
            return;
        }
        *ttl_ms = response.response().common_response().ttl_response().ttl_ms();
    }

    void GetString(server::Server* server, const std::string& key, std::string* value,
                   Status* status) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        ExecuteCmdRequest request;
        ExecuteCmdResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        request.set_load_version(UINT64_MAX);
        request.mutable_request()->mutable_string_request()->mutable_get_request()->set_key(key);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.ExecuteCmd(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();

        *status = Status::FromRpcStatus(response.response().status());
        if (!status->ok()) {
            return;
        }
        *status = Status::FromRpcStatus(response.response().response_status());
        if (!status->ok()) {
            return;
        }
        *value = response.response().string_response().get_response().value();
    }

    Status GetStringWithRetry(
        server::Server* server, const std::string& key, std::string* value,
        std::function<bool(const Status& status, const std::string& key, const std::string& value)>
            check_func) {
        Status status;
        for (int i = 0; i < 10; ++i) {
            GetString(server, key, value, &status);
            if (!check_func(status, key, *value)) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
            return status;
        }
        return status;
    }

    std::string RandomString(size_t length) {
        auto randchar = []() -> char {
            const char charset[] =
                "0123456789"
                "abcdefghijklmnopqrstuvwxyz";
            static std::uniform_int_distribution<int> dist(0, sizeof(charset) - 2);
            return charset[dist(rng)];
        };
        std::string str(length, 0);
        std::generate_n(str.begin(), length, randchar);
        return str;
    }

    void StartServer(server::Server* server, int port) {
        InitHostSpec(temp_dir_.GetDir() + "/host_spec_" + std::to_string(port), port);

        server::Server::Options options;
        options.service_thread_num = 1;
        options.worker_thread_num = 1;
        options.background_thread_num = 1;
        options.port = port;
        options.host = "127.0.0.1";
        options.host_v6 = "::1";
        options.heart_beat_interval = 1 * 1000 * 1000;

        server->Init(options);
        if (FLAGS_enable_blockcache) {
            FLAGS_blockcache_metric_id_prefix = std::to_string(port) + "PartitionReplicatorTest";
        }
        Status status = server->Start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void LoadPartition(server::Server* server, Partition** partition, bool readonly) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        LoadRequest request;
        LoadResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        request.set_load_version(load_version++);
        request.set_partition_uri("file://" + temp_dir_.GetDir() + "/cluster/public/partition");
        request.set_sync(true);
        request.set_readonly(readonly);
        // TODO(wuzhenyu) with membership
        // TODO(wuzhenyu) partition id
        ctrl.set_timeout_ms(30000);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.Load(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();
        ASSERT_EQ(response.status().code(), kOK) << response.status().message();
        *partition =
            server->partition_manager_->thread_infos_[0].partition_map.begin()->second.get();
    }

    void UnloadPartition(server::Server* server, Partition** partition) {
        std::string server_addr = server->GetHost() + ":" + std::to_string(server->GetListenPort());
        brpc::Channel channel;
        brpc::Controller ctrl;
        UnloadRequest request;
        UnloadResponse response;
        ServerService_Stub stub(&channel);
        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(partition_id_);
        ctrl.set_timeout_ms(30000);
        ASSERT_EQ(channel.Init(server_addr.c_str(), nullptr), 0);
        stub.Unload(&ctrl, &request, &response, nullptr);
        ASSERT_FALSE(ctrl.Failed()) << ctrl.ErrorText();
        ASSERT_EQ(response.status().code(), kOK) << response.status().message();
        *partition = nullptr;
    }

    int RandomPort() {
        static thread_local std::random_device rd;
        static thread_local std::mt19937 rng(rd());
        return std::uniform_int_distribution<int>(2000, 9000)(rng);
    }

    std::string partition_uri;
    uint64_t partition_id_ = 0;
    server::Server master_;
    Partition* master_partition_ = nullptr;
    server::Server slave_;
    Partition* slave_partition_ = nullptr;
    TempDir temp_dir_;
    FlagsSetter flag_setter_;
    uint32_t load_version = 0;
};

TEST_F(PartitionReplicatorTest, RejectSecondaryWriteWithoutPinPrimary) {
    Status status;
    SetString(&master_, "primary_write_guard_key", "primary_value", &status);
    ASSERT_TRUE(status.ok()) << status;

    SetString(&slave_, "secondary_write_guard_key", "secondary_value", &status);
    ASSERT_TRUE(status.IsTopomError()) << status;
}

TEST_F(PartitionReplicatorTest, SimpleGetSetDelete) {
    // set key
    Status status;
    SetString(&master_, "test_key", "test_value", &status);
    ASSERT_TRUE(status.ok());

    // get key from slave
    std::string value;
    status = GetStringWithRetry(&slave_, "test_key", &value,
                                [](const Status& status, const std::string& key,
                                   const std::string& value) { return status.ok(); });
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(value, "test_value");

    // delete key
    Delete(&master_, "test_key", &status);
    ASSERT_TRUE(status.ok()) << status;

    // get key from slave
    status = GetStringWithRetry(&slave_, "test_key", &value,
                                [](const Status& status, const std::string& key,
                                   const std::string& value) { return status.IsNotFound(); });
    ASSERT_TRUE(status.IsNotFound());
}

TEST_F(PartitionReplicatorTest, SimpleExpire) {
    // set key
    Status status;
    SetString(&master_, "test_key", "test_value", &status);
    ASSERT_TRUE(status.ok()) << status;

    // get key from slave
    std::string value;
    status = GetStringWithRetry(&slave_, "test_key", &value,
                                [](const Status& status, const std::string& key,
                                   const std::string& value) { return status.ok(); });
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(value, "test_value");

    // set ttl
    uint64_t ttl_s = 10;
    Expire(&master_, "test_key", ttl_s * 1000, &status);
    ASSERT_TRUE(status.ok()) << status;

    // get key from slave
    sleep(1);
    GetString(&slave_, "test_key", &value, &status);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(value, "test_value");

    // key expired at slave
    sleep(10);
    GetString(&slave_, "test_key", &value, &status);
    ASSERT_TRUE(status.IsNotFound()) << status;
}

TEST_F(PartitionReplicatorTest, UpdateConditionWhenRemoteFailed) {
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    std::unique_ptr<brpc::Channel> bad_channel(new brpc::Channel);
    ASSERT_EQ(bad_channel->Init("192.168.4.2", 1023, nullptr), 0);
    slave_partition_->replicator_->remote_channel_.reset(bad_channel.release());

    PartitionInfo info;
    Status status = slave_partition_->replicator_->UpdateRemoteInfo();
    ASSERT_FALSE(status.ok());

    slave_partition_->replicator_->need_update_remote_ = true;
    slave_partition_->replicator_->MainLoop();

    status = slave_partition_->replicator_->UpdateRemoteInfo();
    ASSERT_TRUE(status.ok()) << status;
}

TEST_F(PartitionReplicatorTest, PageStore) {
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();

    {
        // UpdateZones
        // add zones in index meta
        master_partition_->page_store_->PrepareNewZone(true);  // zone 2
        master_partition_->page_store_->PrepareNewZone(true);  // zone 3

        // replay index meta and zones of page store
        for (int i = 0; i < 100; ++i) {
            slave_partition_->replicator_->need_update_remote_ = true;
            slave_partition_->replicator_->MainLoop();
            ASSERT_TRUE(slave_partition_->replicator_->status_.ok());
        }
        ASSERT_EQ(slave_partition_->index_->MetaInfo().version(),
                  master_partition_->index_->MetaInfo().version());

        // check page store state
        ASSERT_EQ(master_partition_->page_store_->zones_.size(),
                  slave_partition_->page_store_->zones_.size());
        ASSERT_EQ(master_partition_->page_store_->writing_zone_id_,
                  slave_partition_->page_store_->writing_zone_id_);

        for (uint32_t zone_id = 1; zone_id < master_partition_->page_store_->zones_.size();
             ++zone_id) {
            if (master_partition_->page_store_->zones_[zone_id] == nullptr) {
                ASSERT_EQ(slave_partition_->page_store_->zones_[zone_id], nullptr);
            } else {
                ASSERT_NE(slave_partition_->page_store_->zones_[zone_id], nullptr);
                ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
                    master_partition_->page_store_->zones_[zone_id]->stream->GetInfo(),
                    slave_partition_->page_store_->zones_[zone_id]->stream->GetInfo()))
                    << zone_id << "\n"
                    << master_partition_->page_store_->zones_[zone_id]
                           ->stream->GetInfo()
                           .ShortDebugString()
                    << "\n"
                    << slave_partition_->page_store_->zones_[zone_id]
                           ->stream->GetInfo()
                           .ShortDebugString()
                    << "\n";
            }
        }
    }

    {
        // UpdateZones
        // remove zones not in index meta

        // recycled zone
        ASSERT_NE(master_partition_->page_store_->writing_zone_id_, 2);
        master_partition_->page_store_->RecycledZone(2);

        // replay index meta and zones of page store
        for (int i = 0; i < 100; ++i) {
            slave_partition_->replicator_->need_update_remote_ = true;
            slave_partition_->replicator_->MainLoop();
            ASSERT_TRUE(slave_partition_->replicator_->status_.ok());
        }
        ASSERT_EQ(slave_partition_->index_->MetaInfo().version(),
                  master_partition_->index_->MetaInfo().version());

        // destroy zone
        master_partition_->page_store_->DestroyZone(2);

        // replay index meta and zones of page store
        for (int i = 0; i < 100; ++i) {
            slave_partition_->replicator_->need_update_remote_ = true;
            slave_partition_->replicator_->MainLoop();
            ASSERT_TRUE(slave_partition_->replicator_->status_.ok());
        }
        ASSERT_EQ(slave_partition_->index_->MetaInfo().version(),
                  master_partition_->index_->MetaInfo().version());

        // check page store state
        ASSERT_EQ(master_partition_->page_store_->zones_.size(),
                  slave_partition_->page_store_->zones_.size());
        ASSERT_EQ(master_partition_->page_store_->writing_zone_id_,
                  slave_partition_->page_store_->writing_zone_id_);
        for (uint32_t zone_id = 1; zone_id < master_partition_->page_store_->zones_.size();
             ++zone_id) {
            if (master_partition_->page_store_->zones_[zone_id] == nullptr) {
                ASSERT_EQ(slave_partition_->page_store_->zones_[zone_id], nullptr);
            } else {
                ASSERT_NE(slave_partition_->page_store_->zones_[zone_id], nullptr);
                ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
                    master_partition_->page_store_->zones_[zone_id]->stream->GetInfo(),
                    slave_partition_->page_store_->zones_[zone_id]->stream->GetInfo()))
                    << zone_id << "\n"
                    << master_partition_->page_store_->zones_[zone_id]
                           ->stream->GetInfo()
                           .ShortDebugString()
                    << "\n"
                    << slave_partition_->page_store_->zones_[zone_id]
                           ->stream->GetInfo()
                           .ShortDebugString()
                    << "\n";
            }
        }
    }
}

TEST_F(PartitionReplicatorTest, ReplayOplog) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    {
        // replay kvlog (slot in memory)
        Status status;
        HSet(&master_, "test_key", "test_field", "test_value", &status);
        ASSERT_TRUE(status.ok()) << status;

        std::string value;
        HGet(&slave_, "test_key", "test_field", &value, &status);
        ASSERT_TRUE(status.IsNotFound());
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(10);
        ASSERT_TRUE(status.ok()) << status;
        HGet(&slave_, "test_key", "test_field", &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "test_value");
        uint64_t slot_id = CallHash("test_key");
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 1);
    }

    {
        // replay meta log (slot in memory)
        uint64_t slot_id = CallHash("test_key");

        Status status;
        Expire(&master_, "test_key", 100000, &status);
        ASSERT_TRUE(status.ok()) << status;

        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(10);
        ASSERT_TRUE(status.ok()) << status;
        uint64_t ttl_ms = 0;
        Ttl(&slave_, "test_key", &ttl_ms, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(ttl_ms > 90000 && ttl_ms <= 100000);
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 2);
    }

    {
        // replay delete log (slot in memory)
        uint64_t slot_id = CallHash("test_key");

        Status status;
        Delete(&master_, "test_key", &status);
        ASSERT_TRUE(status.ok()) << status;

        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(10);
        ASSERT_TRUE(status.ok()) << status;
        std::string value;
        HGet(&slave_, "test_key", "test_field", &value, &status);
        ASSERT_TRUE(status.IsNotFound()) << status;
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 3);
    }

    {
        // replay page log (slot in memory)
        uint64_t slot_id = CallHash("test_key");

        Status status;
        SetString(&master_, "test_string_key", "test_value1", &status);
        ASSERT_TRUE(status.ok()) << status;
        Expire(&master_, "test_string_key", 100000, &status);
        ASSERT_TRUE(status.ok()) << status;
        SetString(&master_, "test_string_key", "test_value2", &status);
        ASSERT_TRUE(status.ok()) << status;

        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(10);
        ASSERT_TRUE(status.ok()) << status;
        std::string value;
        GetString(&slave_, "test_string_key", &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        uint64_t ttl_ms = 0;
        Ttl(&slave_, "test_string_key", &ttl_ms, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_NE(ttl_ms, 0);
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 3);
    }

    {
        // replay oplog (slot not in memory)
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);

        Status status;
        HSet(&master_, key, "test_field", "test_value", &status);
        ASSERT_TRUE(status.ok()) << status;
        master_partition_->storage_manager_->ReclaimOpLog();

        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        // add oplog
        HSet(&master_, key, "test_field", "test_value2", &status);
        ASSERT_TRUE(status.ok()) << status;

        // slave reload
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok()) << status;

        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        slave_partition_->index_->EvictSlot(slot_id);

        // replay oplog
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 1);
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->InMemory());

        // get object from slave (trigger load pages)
        std::string value;
        HGet(&slave_, key, "test_field", &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "test_value2") << value;
    }

    {
        // replay page in log (slot not in memory)
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);

        Status status;
        SetString(&master_, key, "test_value", &status);
        ASSERT_TRUE(status.ok()) << status;
        master_partition_->storage_manager_->ReclaimOpLog();

        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        // add oplog ( page in log )
        SetString(&master_, key, "test_value2", &status);
        ASSERT_TRUE(status.ok()) << status;

        // slave reload
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 1);
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        slave_partition_->index_->EvictSlot(slot_id);

        // replay oplog
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 1);
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->InMemory());

        // get object from slave (trigger load pages)
        std::string value;
        GetString(&slave_, key, &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "test_value2") << value;
    }

    {
        // oplog version less than slot version
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);

        Status status;
        HSet(&master_, key, "test_field", "test_value", &status);
        ASSERT_TRUE(status.ok()) << status;
        master_partition_->storage_manager_->ReclaimOpLog();

        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        // slave reload
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 1);
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        slave_partition_->index_->EvictSlot(slot_id);

        // replay oplog that oplog version less than slot version
        size_t log_num = slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size();
        {
            slave_partition_->slot_context_manager_->SetSlotVersion(slot_id, UINT64_MAX);
            storage::OpLog oplog;
            storage::OpLog::LogItem* item = oplog.add_item();
            item->set_page_log(false);
            item->set_slot_id(slot_id);
            oplog.set_sequence(slave_partition_->op_logger_->CurrentSequence() + 1);
            Status status = slave_partition_->object_manager_->ReplayOplog(100, 10, oplog);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(),
                      log_num);
        }

        {
            slave_partition_->slot_context_manager_->SetSlotVersion(slot_id, 0);
            storage::OpLog oplog;
            storage::OpLog::LogItem* item = oplog.add_item();
            item->set_page_log(false);
            item->set_slot_id(slot_id);
            oplog.set_sequence(slave_partition_->op_logger_->CurrentSequence() + 1);
            status = slave_partition_->object_manager_->ReplayOplog(100, 10, oplog);
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(),
                      log_num + 1);
        }
    }
}

TEST_F(PartitionReplicatorTest, IndexLogTrim) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    std::string key = RandomString(12);
    uint64_t slot_id = CallHash(key);

    // master add oplog
    Status status;
    HSet(&master_, key, "test_field", "test_value", &status);
    ASSERT_TRUE(status.ok()) << status;

    HSet(&master_, key, "test_field", "test_value2", &status);
    ASSERT_TRUE(status.ok()) << status;

    HSet(&master_, key, "test_field", "test_value3", &status);
    ASSERT_TRUE(status.ok()) << status;

    // slave replicate
    slave_partition_->replicator_->MainLoop();
    ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 3);
    ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);
    ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());

    // master dump index log
    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    // slave replciate
    slave_partition_->replicator_->MainLoop();
    slave_partition_->replicator_->MainLoop();
    slave_partition_->replicator_->MainLoop();
    ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 0);
    ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 0);
    ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
}

TEST_F(PartitionReplicatorTest, IndexLogDataLoss) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    std::string key = RandomString(12);
    uint64_t slot_id = CallHash(key);

    // master add oplog
    Status status;
    HSet(&master_, key, "test_field", "test_value", &status);
    ASSERT_TRUE(status.ok()) << status;

    HSet(&master_, key, "test_field", "test_value2", &status);
    ASSERT_TRUE(status.ok()) << status;

    HSet(&master_, key, "test_field", "test_value3", &status);
    ASSERT_TRUE(status.ok()) << status;

    // slave replicate
    slave_partition_->replicator_->MainLoop();
    ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 3);
    ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);
    ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());

    // master dump index log
    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    // slave replicate
    slave_partition_->replicator_->MainLoop();

    uint64_t index_log_sequence = slave_partition_->index_->current_sequence_;
    ASSERT_GE(index_log_sequence, 1);

    {
        // log sequence not match
        storage::IndexLog log;
        log.mutable_meta_item();
        log.set_sequence(index_log_sequence);
        status = slave_partition_->index_->ReplayIndexLog(log, log.ByteSize());
        ASSERT_TRUE(status.IsDataLoss());
    }

    {
        // log sequence match
        storage::IndexLog log;
        log.mutable_meta_item();
        log.set_sequence(index_log_sequence + 1);
        status = slave_partition_->index_->ReplayIndexLog(log, log.ByteSize());
        ASSERT_TRUE(status.IsZoneChanged()) << status;
        ASSERT_EQ(slave_partition_->index_->current_sequence_, index_log_sequence + 1);
    }
}

TEST_F(PartitionReplicatorTest, ReplayIndexObjectMetaLog) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    {
        // index log fast
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);

        Status status;
        HSet(&master_, key, "test_field", "test_value", &status);
        ASSERT_TRUE(status.ok()) << status;
        Expire(&master_, key, 100000, &status);
        ASSERT_TRUE(status.ok()) << status;
        Expire(&master_, key, 200000, &status);
        ASSERT_TRUE(status.ok()) << status;

        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        Expire(&master_, key, 300000, &status);
        ASSERT_TRUE(status.ok()) << status;

        // we have 4 oplog
        // 1. hset
        // 2. expire 100s
        // 3. expire 200s
        // 4. expire 300s

        uint64_t ttl_ms = 0;
        Ttl(&slave_, key, &ttl_ms, &status);
        ASSERT_TRUE(status.IsNotFound()) << status;

        // replay index log
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());

        Ttl(&slave_, key, &ttl_ms, &status);
        ASSERT_TRUE(status.IsNotFound()) << status;

        // replay 1st oplog (hset) and 2nd oplog (ttl 100s)
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(2);
        ASSERT_TRUE(status.ok());
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        // index log not applied yet
        ASSERT_TRUE(!slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());

        Ttl(&slave_, key, &ttl_ms, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(ttl_ms > 90000 && ttl_ms <= 190000);

        // replay 3rd oplog
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok());
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_TRUE(!slave_partition_->index_->GetSlot(slot_id)->MetaDirty());

        Ttl(&slave_, key, &ttl_ms, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(ttl_ms > 190000 && ttl_ms <= 200000);

        // replay 4th oplog
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(!slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());

        Ttl(&slave_, key, &ttl_ms, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(ttl_ms > 290000 && ttl_ms <= 300000);
    }

    {
        // oplog fast
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);

        Status status;
        HSet(&master_, key, "test_field", "test_value", &status);
        ASSERT_TRUE(status.ok()) << status;
        Expire(&master_, key, 100000, &status);
        ASSERT_TRUE(status.ok()) << status;
        Expire(&master_, key, 200000, &status);
        ASSERT_TRUE(status.ok()) << status;

        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        Expire(&master_, key, 300000, &status);
        ASSERT_TRUE(status.ok()) << status;

        // we have 4 oplog
        // 1. hset
        // 2. expire 100s
        // 3. expire 200s
        // 4. expire 300s

        // replay all oplog
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(10);
        ASSERT_TRUE(status.ok());

        uint64_t ttl_ms = 0;
        Ttl(&slave_, key, &ttl_ms, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(ttl_ms > 290000 && ttl_ms <= 300000);

        // replay index log
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 1);

        Ttl(&slave_, key, &ttl_ms, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(ttl_ms > 290000 && ttl_ms <= 300000);
    }
}

TEST_F(PartitionReplicatorTest, ReplayIndexMetaLog) {
    storage::IndexLog log;
    log.set_sequence(slave_partition_->index_->CurrentSequence() + 1);
    log.mutable_meta_item()->set_zone_version(10000);
    Status status = slave_partition_->index_->ReplayIndexLog(log, log.ByteSize());
    ASSERT_TRUE(status.IsZoneChanged());
}

TEST_F(PartitionReplicatorTest, ReplayIndexPageLog) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);

    auto index_gc_usage_trigger = FLAGS_index_gc_usage_trigger;
    FLAGS_index_gc_usage_trigger = 1.0;
    BYTE_DEFER(FLAGS_index_gc_usage_trigger = index_gc_usage_trigger);
    auto index_gc_bytes_threshold = FLAGS_index_gc_bytes_threshold;
    FLAGS_index_gc_bytes_threshold = 0;
    BYTE_DEFER(FLAGS_index_gc_bytes_threshold = index_gc_bytes_threshold);

    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    {
        // key not exists before slave loading
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);

        Status status;
        HSet(&master_, key, "test_field", "test_value1", &status);
        ASSERT_TRUE(status.ok()) << status;
        master_partition_->storage_manager_->ReclaimOpLog();

        HSet(&master_, key, "test_field", "test_value2", &status);
        ASSERT_TRUE(status.ok()) << status;
        master_partition_->storage_manager_->ReclaimOpLog();

        HSet(&master_, key, "test_field", "test_value3", &status);
        ASSERT_TRUE(status.ok()) << status;
        master_partition_->storage_manager_->ReclaimOpLog();

        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        // we have 3 oplog
        // 1. hset test_value1
        // 2. hset test_value2
        // 3. hset test_value3

        // replay index log
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());

        std::string value;
        HGet(&slave_, key, "test_field", &value, &status);
        // index log can not be newer than oplog, so no index log is applied
        ASSERT_TRUE(status.IsNotFound()) << status;

        // replay 1st, 2nd oplog
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(2);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(!slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());

        HGet(&slave_, key, "test_field", &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "test_value2");

        // replay index log again
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        // trim oplog successfully
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_TRUE(!slave_partition_->index_->GetSlot(slot_id)->Dirty());

        // replay 3rd oplog successfuly
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(!slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());

        HGet(&slave_, key, "test_field", &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "test_value3");

        // replay index log finally
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        // trim oplog successfully
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_TRUE(!slave_partition_->index_->GetSlot(slot_id)->Dirty());
    }

    {
        // replay index log generated by index rewrite or page gc
        // while the according oplog of the slot was trimed before slave loading
        // and the slot is loaded in memory by user query
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);

        Status status;
        HSet(&master_, key, "test_field", "test_value1", &status);
        ASSERT_TRUE(status.ok()) << status;
        HSet(&master_, key, "test_field", "test_value2", &status);
        ASSERT_TRUE(status.ok()) << status;

        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        // slave reload
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(2);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 2);
        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        slave_partition_->index_->EvictSlot(slot_id);

        std::string value;
        HGet(&slave_, key, "test_field", &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "test_value2");

        // then, rewrite all index logs
        bool dirty_slot = false;
        uint64_t truncate_id = 0;
        master_partition_->index_->TryGc(&dirty_slot, &truncate_id);
        ASSERT_FALSE(dirty_slot);
        if (truncate_id != 0) {
            master_partition_->index_->Truncate(truncate_id);
        }

        // master modify the key again
        HSet(&master_, key, "test_field", "test_value3", &status);
        ASSERT_TRUE(status.ok()) << status;

        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayOpLog(1);
        ASSERT_TRUE(status.ok());

        while (true) {
            slave_partition_->replicator_->UpdateRemoteInfo();
            status = slave_partition_->replicator_->ReplayIndexLog(10);
            if (status.IsZoneChanged()) {
                slave_partition_->page_store_->UpdateZones();
                continue;
            }
            ASSERT_TRUE(status.ok()) << status;
            break;
        }
        ASSERT_TRUE(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
        ASSERT_TRUE(!slave_partition_->index_->GetSlot(slot_id)->Dirty());

        HGet(&slave_, key, "test_field", &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "test_value3");
    }
}

TEST_F(PartitionReplicatorTest, IndexLogStage) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    std::string key = RandomString(12);

    Status status;
    HSet(&master_, key, "test_field", "test_value1", &status);
    ASSERT_TRUE(status.ok()) << status;
    HSet(&master_, key, "test_field", "test_value2", &status);
    ASSERT_TRUE(status.ok()) << status;

    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    HSet(&master_, key, "test_field", "test_value3", &status);
    ASSERT_TRUE(status.ok()) << status;

    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    // replay 1st oplog (create slot in memory)
    slave_partition_->replicator_->UpdateRemoteInfo();
    status = slave_partition_->replicator_->ReplayOpLog(1);
    ASSERT_TRUE(status.ok());

    // replay index log failed
    ASSERT_FALSE(slave_partition_->replicator_->index_log_staged_);
    do {
        status = slave_partition_->page_store_->UpdateZones();
        ASSERT_TRUE(status.ok());
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayIndexLog(10);
    } while (status.IsZoneChanged());
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(slave_partition_->replicator_->index_log_staged_);

    uint64_t log_id = slave_partition_->replicator_->index_log_iter_->GetLogId();
    storage::IndexLog log = slave_partition_->replicator_->index_log_iter_->GetLog();

    // replay index (we expected index log iter not move forward)
    for (int i = 0; i < 10; ++i) {
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayIndexLog(10);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(slave_partition_->replicator_->index_log_staged_);
        ASSERT_EQ(log_id, slave_partition_->replicator_->index_log_iter_->GetLogId());
        ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
            log, slave_partition_->replicator_->index_log_iter_->GetLog()));
    }

    // replay remain oplog (oplog catched up index log)
    slave_partition_->replicator_->UpdateRemoteInfo();
    status = slave_partition_->replicator_->ReplayOpLog(10);
    ASSERT_TRUE(status.ok());

    // replay index log succ (using staged log, so index log iter not move forward)
    ASSERT_TRUE(slave_partition_->replicator_->index_log_staged_);
    do {
        status = slave_partition_->page_store_->UpdateZones();
        ASSERT_TRUE(status.ok());
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayIndexLog(1);
    } while (status.IsZoneChanged());
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(slave_partition_->replicator_->index_log_staged_);
    ASSERT_EQ(log_id, slave_partition_->replicator_->index_log_iter_->GetLogId());
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        log, slave_partition_->replicator_->index_log_iter_->GetLog()));

    // replay index log succ (move forward index log)
    do {
        status = slave_partition_->page_store_->UpdateZones();
        ASSERT_TRUE(status.ok());
        slave_partition_->replicator_->UpdateRemoteInfo();
        status = slave_partition_->replicator_->ReplayIndexLog(1);
    } while (status.IsZoneChanged());
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(slave_partition_->replicator_->index_log_staged_);
    ASSERT_LE(log_id, slave_partition_->replicator_->index_log_iter_->GetLogId());
    ASSERT_EQ(log.sequence() + 1,
              slave_partition_->replicator_->index_log_iter_->GetLog().sequence());
}

TEST_F(PartitionReplicatorTest, ReplicateFailed) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);

    slave_partition_->op_logger_->current_sequence_ = 100000;

    Status status;
    HSet(&master_, "test_key", "test_field", "test_value1", &status);
    ASSERT_TRUE(status.ok()) << status;

    // waiting for slave partition replicate failed
    usleep(FLAGS_replicator_loop_interval_us * 3);
    ASSERT_FALSE(slave_partition_->replicator_->GetStatus().ok());
}

TEST_F(PartitionReplicatorTest, OutOfSync) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    Status status;
    HSet(&master_, "test_key", "test_field", "test_value1", &status);
    ASSERT_TRUE(status.ok()) << status;

    // waiting for slave partition out of sync
    sleep(FLAGS_replicator_out_of_sync_s * 2);
    slave_partition_->replicator_->MainLoop();
    ASSERT_FALSE(slave_partition_->replicator_->GetStatus().ok());
}

TEST_F(PartitionReplicatorTest, SlaveDirty) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    {
        // clear page dirty
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);
        Status status;
        SetString(&master_, key, "test_value", &status);
        ASSERT_TRUE(status.ok());

        for (int i = 0; i < 10; ++i) {
            slave_partition_->replicator_->MainLoop();
        }
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].dirty);
        ASSERT_TRUE(!slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);

        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        slave_partition_->replicator_->MainLoop();
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 0);
    }

    {
        // clear data dirty
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);
        Status status;
        HSet(&master_, key, "test_value", "test_value", &status);
        ASSERT_TRUE(status.ok());

        slave_partition_->replicator_->MainLoop();
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 0);
        ASSERT_TRUE(!slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);

        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        slave_partition_->replicator_->MainLoop();
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 0);
    }

    {
        // clear meta dirty
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);
        Status status;
        SetString(&master_, key, "test_value", &status);
        ASSERT_TRUE(status.ok());
        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        slave_partition_->replicator_->MainLoop();
        slave_partition_->replicator_->MainLoop();
        slave_partition_->replicator_->MainLoop();
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 0);

        Expire(&master_, key, 10000, &status);
        ASSERT_TRUE(status.ok());

        slave_partition_->replicator_->MainLoop();
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);

        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        slave_partition_->replicator_->MainLoop();
        slave_partition_->replicator_->MainLoop();
        slave_partition_->replicator_->MainLoop();
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 0);
    }

    {
        // clear page dirty
        // clear data dirty
        auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 9041; };
        SetHashFunc(hash_func);
        BYTE_DEFER(SetHashFunc(CallHash));

        uint64_t slot_id = 9041;
        Status status;
        SetString(&master_, RandomString(12), "test_value", &status);
        ASSERT_TRUE(status.ok());
        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        HSet(&master_, RandomString(12), "test_value", "test_value", &status);
        ASSERT_TRUE(status.ok());
        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        slave_partition_->replicator_->UpdateRemoteInfo();
        slave_partition_->replicator_->ReplayOpLog(10);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].dirty);
        ASSERT_TRUE(!slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);

        uint64_t before_meta_version = 0;
        uint64_t after_meta_version = 0;
        // 1. clear page dirty
        do {
            status = slave_partition_->page_store_->UpdateZones();
            ASSERT_TRUE(status.ok());
            slave_partition_->replicator_->UpdateRemoteInfo();
            before_meta_version = slave_partition_->index_->MetaInfo().version();
            status = slave_partition_->replicator_->ReplayIndexLog(1);
            after_meta_version = slave_partition_->index_->MetaInfo().version();
        } while (status.IsZoneChanged() || before_meta_version != after_meta_version);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].dirty);
        ASSERT_TRUE(!slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);

        // 2. clear data dirty (and slot dirty)
        do {
            status = slave_partition_->page_store_->UpdateZones();
            ASSERT_TRUE(status.ok());
            slave_partition_->replicator_->UpdateRemoteInfo();
            before_meta_version = slave_partition_->index_->MetaInfo().version();
            status = slave_partition_->replicator_->ReplayIndexLog(1);
            after_meta_version = slave_partition_->index_->MetaInfo().version();
        } while (status.IsZoneChanged() || before_meta_version != after_meta_version);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 0);
    }

    {
        // clear meta dirty
        // clear data dirty
        // clear page dirty
        auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 4821; };
        SetHashFunc(hash_func);
        BYTE_DEFER(SetHashFunc(CallHash));
        uint64_t slot_id = 4821;

        Status status;
        HSet(&master_, "test_key2", "test_value", "test_value", &status);
        ASSERT_TRUE(status.ok());
        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        SetString(&master_, "test_key1", "test_value", &status);
        ASSERT_TRUE(status.ok());
        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        Expire(&master_, "test_key2", 10000, &status);
        ASSERT_TRUE(status.ok());
        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        slave_partition_->replicator_->UpdateRemoteInfo();
        slave_partition_->replicator_->ReplayOpLog(10);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].dirty);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);

        uint64_t before_meta_version = 0;
        uint64_t after_meta_version = 0;
        // 1. clear data dirty
        do {
            status = slave_partition_->page_store_->UpdateZones();
            ASSERT_TRUE(status.ok());
            slave_partition_->replicator_->UpdateRemoteInfo();
            before_meta_version = slave_partition_->index_->MetaInfo().version();
            status = slave_partition_->replicator_->ReplayIndexLog(1);
            after_meta_version = slave_partition_->index_->MetaInfo().version();
        } while (status.IsZoneChanged() || before_meta_version != after_meta_version);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 2);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].dirty);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);

        // 2. clear page dirty
        do {
            status = slave_partition_->page_store_->UpdateZones();
            ASSERT_TRUE(status.ok());
            slave_partition_->replicator_->UpdateRemoteInfo();
            before_meta_version = slave_partition_->index_->MetaInfo().version();
            status = slave_partition_->replicator_->ReplayIndexLog(1);
            after_meta_version = slave_partition_->index_->MetaInfo().version();
        } while (status.IsZoneChanged() || before_meta_version != after_meta_version);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 2);
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].dirty);
        ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 1);

        // 3. clear meta dirty (and slot dirty)
        do {
            status = slave_partition_->page_store_->UpdateZones();
            ASSERT_TRUE(status.ok());
            slave_partition_->replicator_->UpdateRemoteInfo();
            before_meta_version = slave_partition_->index_->MetaInfo().version();
            status = slave_partition_->replicator_->ReplayIndexLog(1);
            after_meta_version = slave_partition_->index_->MetaInfo().version();
        } while (status.IsZoneChanged() || before_meta_version != after_meta_version);
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->Dirty());
        ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->MetaDirty());
        ASSERT_EQ(slave_partition_->index_->slot_context_manager_->DirtySlotsNum(), 0);
    }
}

TEST_F(PartitionReplicatorTest, SlaveDeleteSlot) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    {
        // delete slot
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);
        Status status;
        SetString(&master_, key, "test_value", &status);
        ASSERT_TRUE(status.ok());
        Delete(&master_, key, &status);
        ASSERT_TRUE(status.ok());

        slave_partition_->replicator_->MainLoop();
        ASSERT_NE(slave_partition_->index_->GetSlot(slot_id), nullptr);

        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }

        slave_partition_->replicator_->MainLoop();
        slave_partition_->replicator_->MainLoop();
        slave_partition_->replicator_->MainLoop();
        ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id), nullptr);
    }

    {
        // can not delete when slot dirty
        std::string key = RandomString(12);
        uint64_t slot_id = CallHash(key);
        Status status;
        HSet(&master_, key, "test_field", "test_value", &status);
        ASSERT_TRUE(status.ok());
        Delete(&master_, key, &status);
        ASSERT_TRUE(status.ok());
        master_partition_->storage_manager_->ReclaimOpLog();
        {
            Controller ctrl;
            CoSyncClosure sync;
            master_partition_->index_->Commit(&ctrl, &sync);
            sync.Wait();
        }
        HSet(&master_, key, "test_field", "test_value", &status);
        ASSERT_TRUE(status.ok());

        slave_partition_->replicator_->UpdateRemoteInfo();
        slave_partition_->replicator_->ReplayOpLog(10);
        ASSERT_NE(slave_partition_->index_->GetSlot(slot_id), nullptr);

        slave_partition_->replicator_->MainLoop();
        slave_partition_->replicator_->MainLoop();
        slave_partition_->replicator_->MainLoop();
        ASSERT_NE(slave_partition_->index_->GetSlot(slot_id), nullptr);

        std::string value;
        HGet(&slave_, key, "test_field", &value, &status);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "test_value");
    }
}

TEST_F(PartitionReplicatorTest, MultiObjectPage) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 12322; };
    SetHashFunc(hash_func);
    BYTE_DEFER(SetHashFunc(CallHash));
    uint64_t slot_id = 12322;

    Status status;
    SetString(&master_, "test_key1", "test_value1", &status);
    ASSERT_TRUE(status.ok());
    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    SetString(&master_, "test_key1", "test_value3", &status);
    ASSERT_TRUE(status.ok());
    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    Delete(&master_, "test_key1", &status);
    ASSERT_TRUE(status.ok());
    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    HSet(&master_, "test_key1", "test_field", "test_value", &status);
    ASSERT_TRUE(status.ok());
    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    slave_partition_->replicator_->UpdateRemoteInfo();
    slave_partition_->replicator_->ReplayOpLog(10);
    ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
    PageIndex page_index = slave_partition_->index_->GetSlot(slot_id)->GetPages()[0];

    uint64_t before_meta_version = 0;
    uint64_t after_meta_version = 0;
    // 1. replay 1 index log, do not update page index
    do {
        status = slave_partition_->page_store_->UpdateZones();
        ASSERT_TRUE(status.ok());
        slave_partition_->replicator_->UpdateRemoteInfo();
        before_meta_version = slave_partition_->index_->MetaInfo().version();
        status = slave_partition_->replicator_->ReplayIndexLog(1);
        after_meta_version = slave_partition_->index_->MetaInfo().version();
    } while (status.IsZoneChanged() || before_meta_version != after_meta_version);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
    ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].dirty);
    ASSERT_EQ(memcmp(&page_index, slave_partition_->index_->GetSlot(slot_id)->GetPages().data(),
                     sizeof(PageIndex)),
              0);

    // 2. replay 2 index log, do not update page index
    do {
        status = slave_partition_->page_store_->UpdateZones();
        ASSERT_TRUE(status.ok());
        slave_partition_->replicator_->UpdateRemoteInfo();
        before_meta_version = slave_partition_->index_->MetaInfo().version();
        status = slave_partition_->replicator_->ReplayIndexLog(1);
        after_meta_version = slave_partition_->index_->MetaInfo().version();
    } while (status.IsZoneChanged() || before_meta_version != after_meta_version);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
    ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].dirty);
    ASSERT_EQ(memcmp(&page_index, slave_partition_->index_->GetSlot(slot_id)->GetPages().data(),
                     sizeof(PageIndex)),
              0);

    // 3. replay 3,4 index log (delete will dump 2 index log), update page index
    do {
        status = slave_partition_->page_store_->UpdateZones();
        ASSERT_TRUE(status.ok());
        slave_partition_->replicator_->UpdateRemoteInfo();
        before_meta_version = slave_partition_->index_->MetaInfo().version();
        status = slave_partition_->replicator_->ReplayIndexLog(1);
        after_meta_version = slave_partition_->index_->MetaInfo().version();
    } while (status.IsZoneChanged() || before_meta_version != after_meta_version);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(slave_partition_->index_->GetSlot(slot_id)->GetPages().empty());
}

TEST_F(PartitionReplicatorTest, DeleteDirtyMeta) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();
    slave_partition_->page_store_->UpdateZones();

    Status status;
    SetString(&master_, "test_key", "test_value", &status);
    ASSERT_TRUE(status.ok());
    Expire(&master_, "test_key", 200000, &status);
    master_partition_->storage_manager_->ReclaimOpLog();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }
    ASSERT_TRUE(status.ok());
    Delete(&master_, "test_key", &status);
    ASSERT_TRUE(status.ok());
    SetString(&master_, "test_key", "test_value", &status);
    ASSERT_TRUE(status.ok());

    slave_partition_->replicator_->UpdateRemoteInfo();
    status = slave_partition_->replicator_->ReplayOpLog(10);
    ASSERT_TRUE(status.ok());

    slave_partition_->replicator_->MainLoop();
    slave_partition_->replicator_->MainLoop();
    slave_partition_->replicator_->MainLoop();

    uint64_t ttl_ms = 0;
    Ttl(&slave_, "test_key", &ttl_ms, &status);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(ttl_ms, 0);
}

TEST_F(PartitionReplicatorTest, StringModuleLoop) {
    bench::BrpcClient::Options client_opts;
    client_opts.server_addr = master_.GetHost() + ":" + std::to_string(master_.GetListenPort());
    client_opts.partition_id = master_partition_->GetPartitionID();
    std::unique_ptr<bench::BrpcClient> client(new bench::BrpcClient());
    Status status = client->Init(std::move(client_opts));
    ASSERT_TRUE(status.ok()) << status;

    bench::WorkloadsBunch workloads;
    bench::WorkloadsBunch::Options base_opts;
    base_opts.id =
        FLAGS_bench_id.empty() ? std::to_string(bcache2::GetCurrentTimeInNs()) : FLAGS_bench_id;
    base_opts.key_count = 100000;
    base_opts.key_size = 32;
    base_opts.value_count = 1000;
    base_opts.value_min_size = 128;
    base_opts.value_max_size = 256;
    base_opts.key_pattern = bench::WorkloadsBunch::KeyPattern::kSequential;
    base_opts.key_dis = bench::WorkloadsBunch::KeyDist::kUniform;
    workloads.Init(base_opts);

    {
        // string workload
        std::unique_ptr<bench::StringWorkload> string_workloads(new bench::StringWorkload());
        bench::StringWorkload::Options opts;
        opts.freq_set = 1;
        opts.freq_setex = 1;
        opts.freq_get = 1;
        opts.setex_min_ttl_ms = 1000;
        opts.setex_max_ttl_ms = 5000;
        string_workloads->Init(std::move(opts));
        workloads.RegisterWorkload(string_workloads.release(), 10);
    }

    {
        // common workload
        std::unique_ptr<bench::CommonWorkload> common_workloads(new bench::CommonWorkload());
        bench::CommonWorkload::Options opts;
        opts.freq_del = 1;
        opts.freq_expire = 1;
        opts.freq_ttl = 1;
        opts.expire_min_ttl_ms = 1000;
        opts.expire_max_ttl_ms = 5000;
        common_workloads->Init(std::move(opts));
        workloads.RegisterWorkload(common_workloads.release(), 1);
    }

    bench::Bench bench;
    bench::Bench::Options bench_opts;
    bench_opts.client = client.get();
    bench_opts.workloads = &workloads;
    bench_opts.jobs = 1;
    bench_opts.depth = 10;
    bench_opts.stay_operations = false;
    bench_opts.key_ttl_ms = 1000000;
    bench.Init(bench_opts);
    bench.Start();

    for (int i = 0; i < 120; ++i) {
        bench.PrintStats();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    bench.Stop();

    // waiting for slave catch up
    master_partition_->storage_manager_->Stop();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->op_logger_->Commit(&ctrl, &sync);
        sync.Wait();
    }
    for (int i = 0; i < 120; ++i) {
        if (slave_partition_->index_->current_sequence_ ==
                master_partition_->index_->current_sequence_ &&
            slave_partition_->op_logger_->current_sequence_ ==
                master_partition_->op_logger_->current_sequence_) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    ASSERT_EQ(slave_partition_->index_->current_sequence_,
              master_partition_->index_->current_sequence_);
    ASSERT_EQ(slave_partition_->op_logger_->current_sequence_,
              master_partition_->op_logger_->current_sequence_);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // check index
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(master_partition_->index_->meta_,
                                                                   slave_partition_->index_->meta_))
        << master_partition_->index_->meta_.ShortDebugString() << "\n"
        << slave_partition_->index_->meta_.ShortDebugString() << "\n";
    ASSERT_EQ(master_partition_->index_->slot_map_.size(),
              slave_partition_->index_->slot_map_.size());
    auto master_slot_iter = master_partition_->index_->slot_map_.begin();
    auto slave_slot_iter = slave_partition_->index_->slot_map_.begin();
    while (master_slot_iter != master_partition_->index_->slot_map_.end()) {
        const auto& master_slot_node = master_slot_iter->second;
        const auto& slave_slot_node = slave_slot_iter->second;
        ASSERT_EQ(master_slot_iter->first, slave_slot_iter->first);
        ASSERT_EQ(master_slot_node.Dirty(), slave_slot_node.Dirty());
        ASSERT_EQ(master_slot_node.MetaDirty(), slave_slot_node.MetaDirty());
        ASSERT_EQ(master_slot_node.GetPageNum(), slave_slot_node.GetPageNum());
        ASSERT_EQ(master_slot_node.GetObjectNum(), slave_slot_node.GetObjectNum());
        ASSERT_EQ(master_slot_node.GetObjectTtls().size(), slave_slot_node.GetObjectTtls().size());
        for (size_t i = 0; i < master_slot_node.GetPages().size(); ++i) {
            ASSERT_EQ(memcmp(&(master_slot_node.GetPages()[i]), &(slave_slot_node.GetPages()[i]),
                             sizeof(PageIndex)),
                      0);
        }
        for (size_t i = 0; i < master_slot_node.GetObjects().size(); ++i) {
            ASSERT_EQ(master_slot_node.GetObjects()[i].ObjectId(),
                      slave_slot_node.GetObjects()[i].ObjectId());
            ASSERT_EQ(master_slot_node.GetObjectTtl(master_slot_node.GetObjects()[i].ObjectId()),
                      slave_slot_node.GetObjectTtl(slave_slot_node.GetObjects()[i].ObjectId()));
            ASSERT_EQ(master_slot_node.GetObjects()[i].ModelId(),
                      model::ModelManager::GetModelId<model::StringModel>());
            ASSERT_EQ(slave_slot_node.GetObjects()[i].ModelId(),
                      model::ModelManager::GetModelId<model::StringModel>());
            ASSERT_EQ(master_slot_node.GetObjects()[i].Model<model::StringModel>()->GetValue(),
                      slave_slot_node.GetObjects()[i].Model<model::StringModel>()->GetValue());
        }
        ++master_slot_iter;
        ++slave_slot_iter;
    }

    // check page store
    ASSERT_EQ(master_partition_->page_store_->zones_.size(),
              slave_partition_->page_store_->zones_.size());
    ASSERT_EQ(master_partition_->page_store_->writing_zone_id_,
              slave_partition_->page_store_->writing_zone_id_);
}

TEST_F(PartitionReplicatorTest, HashModuleLoop) {
    bench::BrpcClient::Options client_opts;
    client_opts.server_addr = master_.GetHost() + ":" + std::to_string(master_.GetListenPort());
    client_opts.partition_id = master_partition_->GetPartitionID();
    std::unique_ptr<bench::BrpcClient> client(new bench::BrpcClient());
    Status status = client->Init(std::move(client_opts));
    ASSERT_TRUE(status.ok()) << status;

    bench::WorkloadsBunch workloads;
    bench::WorkloadsBunch::Options base_opts;
    base_opts.id =
        FLAGS_bench_id.empty() ? std::to_string(bcache2::GetCurrentTimeInNs()) : FLAGS_bench_id;
    base_opts.key_count = 100000;
    base_opts.key_size = 32;
    base_opts.value_count = 1000;
    base_opts.value_min_size = 128;
    base_opts.value_max_size = 256;
    base_opts.key_pattern = bench::WorkloadsBunch::KeyPattern::kSequential;
    base_opts.key_dis = bench::WorkloadsBunch::KeyDist::kUniform;
    workloads.Init(base_opts);

    {
        // hash workload
        std::unique_ptr<bench::HashWorkload> hash_workloads(new bench::HashWorkload());
        bench::HashWorkload::Options opts;
        opts.freq_hset = 1;
        opts.freq_hdel = 1;
        opts.freq_hget = 1;
        opts.field_count = 20;
        hash_workloads->Init(std::move(opts));
        workloads.RegisterWorkload(hash_workloads.release(), 10);
    }

    {
        // common workload
        std::unique_ptr<bench::CommonWorkload> common_workloads(new bench::CommonWorkload());
        bench::CommonWorkload::Options opts;
        opts.freq_del = 1;
        opts.freq_expire = 1;
        opts.freq_ttl = 1;
        opts.expire_min_ttl_ms = 1000;
        opts.expire_max_ttl_ms = 5000;
        common_workloads->Init(std::move(opts));
        workloads.RegisterWorkload(common_workloads.release(), 1);
    }

    bench::Bench bench;
    bench::Bench::Options bench_opts;
    bench_opts.client = client.get();
    bench_opts.workloads = &workloads;
    bench_opts.jobs = 1;
    bench_opts.depth = 10;
    bench_opts.stay_operations = false;
    bench_opts.key_ttl_ms = 1000000;
    bench.Init(bench_opts);
    bench.Start();

    for (int i = 0; i < 120; ++i) {
        bench.PrintStats();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    bench.Stop();

    // waiting for slave catch up
    master_partition_->storage_manager_->Stop();
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->op_logger_->Commit(&ctrl, &sync);
        sync.Wait();
    }
    for (int i = 0; i < 10; ++i) {
        if (slave_partition_->index_->current_sequence_ ==
                master_partition_->index_->current_sequence_ &&
            slave_partition_->op_logger_->current_sequence_ ==
                master_partition_->op_logger_->current_sequence_) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    ASSERT_EQ(slave_partition_->index_->current_sequence_,
              master_partition_->index_->current_sequence_);
    ASSERT_EQ(slave_partition_->op_logger_->current_sequence_,
              master_partition_->op_logger_->current_sequence_);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    for (const auto& iter : master_partition_->index_->slot_map_) {
        if (slave_partition_->index_->slot_map_.find(iter.first) ==
            slave_partition_->index_->slot_map_.end()) {
            std::cout << iter.first << "\n";
        }
    }

    // check index
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(master_partition_->index_->meta_,
                                                                   slave_partition_->index_->meta_))
        << master_partition_->index_->meta_.ShortDebugString() << "\n"
        << slave_partition_->index_->meta_.ShortDebugString() << "\n";
    ASSERT_EQ(master_partition_->index_->slot_map_.size(),
              slave_partition_->index_->slot_map_.size());

    auto master_slot_iter = master_partition_->index_->slot_map_.begin();
    auto slave_slot_iter = slave_partition_->index_->slot_map_.begin();
    while (master_slot_iter != master_partition_->index_->slot_map_.end()) {
        const auto& master_slot_node = master_slot_iter->second;
        const auto& slave_slot_node = slave_slot_iter->second;
        ASSERT_EQ(master_slot_iter->first, slave_slot_iter->first);
        ASSERT_EQ(master_slot_node.Dirty(), slave_slot_node.Dirty()) << master_slot_iter->first;
        ASSERT_EQ(master_slot_node.MetaDirty(), slave_slot_node.MetaDirty())
            << master_slot_iter->first;
        ASSERT_EQ(master_slot_node.GetPageNum(), slave_slot_node.GetPageNum())
            << master_slot_iter->first;
        ASSERT_EQ(master_slot_node.GetObjectNum(), slave_slot_node.GetObjectNum())
            << master_slot_iter->first;
        ASSERT_EQ(master_slot_node.GetObjectTtls().size(), slave_slot_node.GetObjectTtls().size())
            << master_slot_iter->first;
        for (size_t i = 0; i < master_slot_node.GetPages().size(); ++i) {
            ASSERT_EQ(memcmp(&(master_slot_node.GetPages()[i]), &(slave_slot_node.GetPages()[i]),
                             sizeof(PageIndex)),
                      0)
                << master_slot_iter->first;
        }
        for (size_t i = 0; i < master_slot_node.GetObjects().size(); ++i) {
            ASSERT_EQ(master_slot_node.GetObjects()[i].ObjectId(),
                      slave_slot_node.GetObjects()[i].ObjectId())
                << master_slot_iter->first;
            ASSERT_EQ(master_slot_node.GetObjectTtl(master_slot_node.GetObjects()[i].ObjectId()),
                      slave_slot_node.GetObjectTtl(slave_slot_node.GetObjects()[i].ObjectId()))
                << master_slot_iter->first;
            ASSERT_EQ(master_slot_node.GetObjects()[i].ModelId(),
                      model::ModelManager::GetModelId<model::HashModel>())
                << master_slot_iter->first;
            ASSERT_EQ(slave_slot_node.GetObjects()[i].ModelId(),
                      model::ModelManager::GetModelId<model::HashModel>())
                << master_slot_iter->first;

            auto master_pmap = &(master_slot_node.GetObjects()[i].Model<model::HashModel>()->data_);
            auto slave_pmap = &(slave_slot_node.GetObjects()[i].Model<model::HashModel>()->data_);
            ASSERT_EQ(master_pmap->size(), slave_pmap->size());
            auto master_pmap_iter = master_pmap->begin();
            auto slave_pmap_iter = slave_pmap->begin();
            while (master_pmap_iter != master_pmap->end()) {
                ASSERT_EQ(master_pmap_iter->first, slave_pmap_iter->first);
                ASSERT_EQ(master_pmap_iter->second.second, slave_pmap_iter->second.second);
                ++master_pmap_iter;
                ++slave_pmap_iter;
            }
        }
        ++master_slot_iter;
        ++slave_slot_iter;
    }

    // check page store
    ASSERT_EQ(master_partition_->page_store_->zones_.size(),
              slave_partition_->page_store_->zones_.size());
    ASSERT_EQ(master_partition_->page_store_->writing_zone_id_,
              slave_partition_->page_store_->writing_zone_id_);
}

TEST_F(PartitionReplicatorTest, Issue97) {}

TEST_F(PartitionReplicatorTest, MasterDown) {
    slave_partition_->replicator_->Stop();

    Status status;
    HSet(&master_, "test_key", "test_field", "test_value", &status);
    ASSERT_TRUE(status.ok()) << status;

    for (int i = 0; i < 10; ++i) {
        slave_partition_->replicator_->MainLoop();
        if (slave_partition_->replicator_->last_replay_time_ms_ != 0) {
            break;
        }
        sleep(1);
    }
    ASSERT_NE(slave_partition_->replicator_->last_replay_time_ms_, 0);

    master_partition_->Unload();

    ASSERT_TRUE(slave_partition_->replicator_->GetStatus().ok());

    sleep(FLAGS_replicator_out_of_sync_s + 1);

    slave_partition_->replicator_->MainLoop();
    ASSERT_FALSE(slave_partition_->replicator_->GetStatus().ok());
}

TEST_F(PartitionReplicatorTest, SlaveReload) {
    auto storage_async = FLAGS_storage_async;
    FLAGS_storage_async = false;
    BYTE_DEFER(FLAGS_storage_async = storage_async);

    FLAGS_replicator_out_of_sync_s = 1000000;
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();

    std::string key = "test_key";
    uint64_t slot_id = CallHash(key);

    // oplog(value1)
    Status status;
    SetString(&master_, key, "value1", &status);
    ASSERT_TRUE(status.ok()) << status;

    // dump
    {
        master_partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        Controller ctrl;
        SYNC_CALL(master_partition_->index_->Commit, &ctrl);
        ASSERT_TRUE(ctrl.status().ok());
    }

    // reload slave
    UnloadPartition(&slave_, &slave_partition_);
    LoadPartition(&slave_, &slave_partition_, true);
    ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->InMemory());
    ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 0);
    slave_partition_->replicator_->Stop();

    // oplog(value2)
    SetString(&master_, key, "value2", &status);
    ASSERT_TRUE(status.ok()) << status;

    // dump
    {
        master_partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        Controller ctrl;
        SYNC_CALL(master_partition_->index_->Commit, &ctrl);
        ASSERT_TRUE(ctrl.status().ok());
    }

    // oplog(value3)
    SetString(&master_, key, "value3", &status);
    ASSERT_TRUE(status.ok()) << status;

    // replicate oplog
    for (int i = 0; i < 10; ++i) {
        slave_partition_->replicator_->UpdateRemoteInfo();
        slave_partition_->replicator_->ReplayOpLog(100);
    }
    ASSERT_FALSE(slave_partition_->index_->GetSlot(slot_id)->InMemory());
    ASSERT_EQ(slave_partition_->slot_context_manager_->GetSlotLogs(slot_id).size(), 2);

    // get string(trigger slot load)
    std::string value;
    GetString(&slave_, key, &value, &status);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(value, "value3");

    // replicate index log
    for (int i = 0; i < 10; ++i) {
        slave_partition_->replicator_->UpdateRemoteInfo();
        slave_partition_->replicator_->ReplayIndexLog(100);
    }

    // get string
    GetString(&slave_, key, &value, &status);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(value, "value3");

    // compare master&slave
    ASSERT_EQ(master_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
    ASSERT_EQ(slave_partition_->index_->GetSlot(slot_id)->GetPages().size(), 1);
    ASSERT_EQ(master_partition_->index_->GetSlot(slot_id)->GetPages()[0].address,
              slave_partition_->index_->GetSlot(slot_id)->GetPages()[0].address);
}

TEST_F(PartitionReplicatorTest, WriteSlave) {
    {
        // get
        str2::GetRequest request;
        str2::GetResponse response;
        Controller ctrl;
        Status status;
        request.set_key("test_key");
        SYNC_CALL(slave_partition_->ExecuteCmd, &ctrl, Module::STRING, str2::Function::GET,
                  &request, &response, &status);
        ASSERT_TRUE(ctrl.status().ok());
        ASSERT_TRUE(status.IsNotFound());
    }

    {
        // set
        str2::SetRequest request;
        str2::SetResponse response;
        Controller ctrl;
        Status status;
        request.set_key("test_key");
        SYNC_CALL(slave_partition_->ExecuteCmd, &ctrl, Module::STRING, str2::Function::SET,
                  &request, &response, &status);
        ASSERT_TRUE(ctrl.status().IsPermissionDenied());
    }
}

TEST_F(PartitionReplicatorTest, Issue144PageStoreReuse) {
    FLAGS_replicator_out_of_sync_s = 1000000;

    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();

    // write data in zone 1
    ASSERT_EQ(master_partition_->page_store_->writing_zone_id_, 1);
    for (int i = 0; i < 1000; ++i) {
        Status status;
        HSet(&master_, std::to_string(i), "test_field", std::string(4096, 'a'), &status);
        ASSERT_TRUE(status.ok()) << status;
        if (i % 100 == 0) {
            master_partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        }
    }

    // zone 2, 3
    Status status = master_partition_->page_store_->PrepareNewZone(true);
    BYTE_ASSERT(status.ok());
    master_partition_->page_store_->PrepareNewZone(true);
    BYTE_ASSERT(status.ok());

    // recycle zone 1
    storage::IndexLog::ZoneInfo zone_info;
    ASSERT_TRUE(master_partition_->index_->GetZoneInfo(1, &zone_info));
    ASSERT_EQ(zone_info.state(), storage::IndexLog::FROZEN);
    master_partition_->page_store_->RecycledZone(1);

    // sync page store
    for (int i = 0; i < 100; ++i) {
        slave_partition_->replicator_->MainLoop();
    }
    ASSERT_TRUE(slave_partition_->replicator_->GetStatus().ok())
        << slave_partition_->replicator_->GetStatus();
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        slave_partition_->page_store_->GetInfo(), master_partition_->page_store_->GetInfo()))
        << slave_partition_->page_store_->GetInfo().ShortDebugString() << "\n"
        << master_partition_->page_store_->GetInfo().ShortDebugString() << "\n";
    ASSERT_TRUE(slave_partition_->index_->GetZoneInfo(1, &zone_info));
    ASSERT_EQ(zone_info.state(), storage::IndexLog::RECYCLED);

    // destroy zone 1 and new zone 1
    master_partition_->page_store_->DestroyZone(1);
    master_partition_->page_store_->PrepareNewZone(true);
    ASSERT_TRUE(master_partition_->index_->GetZoneInfo(1, &zone_info));
    ASSERT_EQ(zone_info.state(), storage::IndexLog::CREATED);

    // write data in zone 1
    for (int i = 0; i < 1000; ++i) {
        Status status;
        HSet(&master_, std::to_string(i), "test_field", std::string(4096, 'a'), &status);
        ASSERT_TRUE(status.ok()) << status;
        if (i % 100 == 0) {
            master_partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        }
    }

    // sync page store
    for (int i = 0; i < 100; ++i) {
        slave_partition_->replicator_->MainLoop();
    }
    ASSERT_TRUE(slave_partition_->replicator_->GetStatus().ok())
        << slave_partition_->replicator_->GetStatus();
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        slave_partition_->page_store_->GetInfo(), master_partition_->page_store_->GetInfo()))
        << slave_partition_->page_store_->GetInfo().ShortDebugString() << "\n"
        << master_partition_->page_store_->GetInfo().ShortDebugString() << "\n";
}

TEST_F(PartitionReplicatorTest, MultiPage) {
    master_partition_->storage_manager_->Stop();
    slave_partition_->replicator_->Stop();

    std::unordered_map<std::string, std::string> origin_data;

    std::string key = "key";

    Status status;
    HSet(&master_, key, "big_field", std::string(200 * 1024, 'a'), &status);
    ASSERT_TRUE(status.ok());
    master_partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    HSet(&master_, key, "small_field", "value", &status);
    ASSERT_TRUE(status.ok());
    master_partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    // more pages
    for (int i = 0; i < 10; ++i) {
        HSet(&master_, key, "small_field" + std::to_string(i), "value" + std::to_string(i),
             &status);
        ASSERT_TRUE(status.ok());
        master_partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        origin_data["small_field" + std::to_string(i)] = "value" + std::to_string(i);
    }

    // compact all small pages to one
    master_partition_->storage_manager_->CompactPages();

    // more pages
    for (int i = 100; i < 110; ++i) {
        origin_data["small_field" + std::to_string(i)] = "value" + std::to_string(i);
        HSet(&master_, key, "small_field" + std::to_string(i), "value" + std::to_string(i),
             &status);
        ASSERT_TRUE(status.ok());
        if (i < 105) {
            master_partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        }
    }

    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->index_->Commit(&ctrl, &sync);
        sync.Wait();
    }
    {
        Controller ctrl;
        CoSyncClosure sync;
        master_partition_->op_logger_->Commit(&ctrl, &sync);
        sync.Wait();
    }

    // slave replicate
    for (int i = 0; i < 100; ++i) {
        slave_partition_->replicator_->MainLoop();
    }

    // check data correctness
    for (auto& origin_field_value : origin_data) {
        std::string value;
        HGet(&slave_, key, origin_field_value.first, &value, &status);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, origin_field_value.second);
    }
}

}  // namespace test
}  // namespace partition
}  // namespace bcache2
