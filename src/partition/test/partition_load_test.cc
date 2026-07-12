// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <byte/algorithm/crc64.h>
#include <byte/thread/async_thread.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <iostream>

#include "blockcache/blockcache.h"
#include "common/coclosure.h"
#include "common/function_closure.h"
#include "common/ring_array.h"
#include "common/slot.h"
#include "common/sync_closure.h"
#include "model/feature_model.h"
#include "model/hash_model.h"
#include "model/string_model.h"
#include "partition/partition.h"
#include "partition/storage/evicter.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/page_store.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/storage_manager.h"
#include "partition/test/cmd.h"
#include "protocol/server.pb.h"
#include "stream/log_based_env.h"
#include "stream/log_based_stream_base.h"
#include "test/common/temp_dir.h"

DECLARE_uint64(storage_dump_slots_per_round);
DECLARE_uint64(storage_gc_max_slots_per_round);
DECLARE_uint64(stream_max_blob_size);
DECLARE_uint64(storage_oplog_delay_dump_length);
DECLARE_uint64(evicter_max_memory_usage);
DECLARE_uint64(evict_count_limit);
DECLARE_uint64(evict_batch_size);

namespace bcache2 {
namespace partition {

class PartitionLoadTest : public testing::Test {
 public:
    void SetUp() {
        FLAGS_storage_oplog_delay_dump_length = 0;
        FLAGS_enable_blockcache = true;
        FLAGS_blockcache_dram_capacity = 134217728;  // 128 MB
        FLAGS_blockcache_pmem_capacity = 0;
        FLAGS_blockcache_ssd_capacity = 0;
        FLAGS_blockcache_enable_metrics = false;
        matrixobjectstore_set_flag("matrixobjectstore_client_log_level", "1");
        matrixobjectstore_init();
        byte::AsyncThreadPoolOptions tp_options;
        tp_options.name_ = "test";
        background_pool_.reset(new byte::AsyncThreadPool());
        ASSERT_TRUE(background_pool_->Init(tp_options));
        ASSERT_TRUE(background_pool_->Start());

        store_layer_.reset(new stream::StoreLayer(background_pool_.get()));

        env_.reset(new stream::LogBasedEnv());
        stream::LogBasedEnv::Options env_options;
        env_options.background_pool = background_pool_.get();
        env_options.store_layer = store_layer_.get();
        env_->Init(env_options);

        uri_ = "file://" + temp_dir_.GetDir() + "/cluster/public/partition";

        ReloadPartition();
    }

    void TearDown() {
        if (partition_.get() != nullptr) {
            partition_->Unload();
        }

        if (FLAGS_enable_blockcache) {
            blockcache_->Stop();
        }

        matrixobjectstore_shutdown();
    }

    void ReloadPartition() {
        if (partition_ != nullptr) {
            partition_->Unload();
        }
        if (FLAGS_enable_blockcache) {
            blockcache_.reset(new bcache2::blockcache::BlockCache());
            auto start_status = blockcache_->Start();
            ASSERT_TRUE(start_status.ok());
        }
        Partition::Options options;
        options.host = "127.0.0.1";
        options.port = 8231;
        options.persistent_type = PersistentType::PERSISTENT_SYNC;
        options.env = env_.get();
        options.uri = uri_;
        options.load_version = ++load_version_;
        options.blockcache = blockcache_.get();
        partition_.reset(new Partition(options));
        auto status = partition_->Load();
        ASSERT_TRUE(status.ok()) << status.ToString();
        partition_->storage_manager_->Stop();
        partition_->storage_manager_->Prepare();
    }

 protected:
    std::unique_ptr<byte::AsyncThreadPool> background_pool_;
    std::unique_ptr<stream::StoreLayer> store_layer_;
    std::unique_ptr<stream::LogBasedEnv> env_;
    TempDir temp_dir_;
    std::string uri_;
    std::unique_ptr<Partition> partition_;
    std::unique_ptr<bcache2::blockcache::BlockCache> blockcache_{nullptr};
    uint32_t load_version_ = 0;
};

TEST_F(PartitionLoadTest, KvLogWithIndexItem) {
    {
        // Append kv log.
        auto status = PartitionHSet(partition_.get(), "key", "field", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        // Dump. Add index item.
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLog();
        BYTE_ASSERT(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }
    {
        // Append more kv log.
        auto status = PartitionHSet(partition_.get(), "key", "field", "value2");
        ASSERT_TRUE(status.ok());
    }
    {
        // Reload. Expect partition retriggers dump and slot holds logs.
        uint64_t slot_id = CallHash("key");
        ReloadPartition();
        ASSERT_FALSE(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        auto slot = partition_->index_->GetSlot(slot_id);
        ASSERT_TRUE(slot != nullptr);
        ASSERT_TRUE(!partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
    }
    {
        // Expect to get value2.
        std::string value;
        auto status = PartitionHGet(partition_.get(), "key", "field", &value);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(value, "value2");
    }
    {
        // Dump. Expect slot erases logs.
        uint64_t slot_id = CallHash("key");
        partition_->storage_manager_->ReclaimOpLog();
        BYTE_ASSERT(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        auto slot = partition_->index_->GetSlot(slot_id);
        ASSERT_TRUE(slot != nullptr);
        ASSERT_TRUE(partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
    }
    {
        // Expect to get value2.
        std::string value;
        auto status = PartitionHGet(partition_.get(), "key", "field", &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value2");
    }
}

TEST_F(PartitionLoadTest, KvLogWithoutIndexItem) {
    {
        // Append kv log.
        auto status = PartitionHSet(partition_.get(), "key", "field", "value1");
        ASSERT_TRUE(status.ok());
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }
    {
        // Reload. Expect partition retriggers dump and slot directly applies logs.
        uint64_t slot_id = CallHash("key");
        ReloadPartition();
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        auto slot = partition_->index_->GetSlot(slot_id);
        ASSERT_TRUE(slot != nullptr);
        ASSERT_TRUE(slot->InMemory());
    }
    {
        // Expect to get value1.
        std::string value;
        auto status = PartitionHGet(partition_.get(), "key", "field", &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value1");
    }
    {
        // Dump. Add index item.
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLog();
        BYTE_ASSERT(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }
    {
        // Get after dump.
        std::string value;
        auto status = PartitionHGet(partition_.get(), "key", "field", &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value1");
    }
}

TEST_F(PartitionLoadTest, PageLogWithIndexItem) {
    {
        // Append page log.
        auto status = PartitionSet(partition_.get(), "key", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        // Dump. Add index item.
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLog();
        BYTE_ASSERT(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }
    {
        // Reload. Expect slot exists.
        ReloadPartition();
        auto slot = partition_->index_->GetSlot(CallHash("key"));
        ASSERT_TRUE(slot != nullptr);
    }
    {
        // Expect to get value1.
        std::string value;
        auto status = PartitionGet(partition_.get(), "key", &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value1");
    }
    {
        // Append more page log.
        auto status = PartitionSet(partition_.get(), "key", "value2");
        ASSERT_TRUE(status.ok());
    }
    {
        // Reload. Expect slot exists.(page update)
        ReloadPartition();
        auto slot = partition_->index_->GetSlot(CallHash("key"));
        ASSERT_TRUE(slot != nullptr);
    }
    {
        // Expect to get value2.
        std::string value;
        auto status = PartitionGet(partition_.get(), "key", &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value2");
    }
}

TEST_F(PartitionLoadTest, LoadEvictedObjectChecksBlockCacheBeforePersistentStore) {
    struct ScopedEvicterFlags {
        uint64_t max_memory_usage = FLAGS_evicter_max_memory_usage;
        uint64_t count_limit = FLAGS_evict_count_limit;
        uint64_t batch_size = FLAGS_evict_batch_size;
        ~ScopedEvicterFlags() {
            FLAGS_evicter_max_memory_usage = max_memory_usage;
            FLAGS_evict_count_limit = count_limit;
            FLAGS_evict_batch_size = batch_size;
        }
    } scoped_evicter_flags;

    FLAGS_evicter_max_memory_usage = 1;  // 1 MB, low enough to force eviction for the object below.
    FLAGS_evict_count_limit = 10;
    FLAGS_evict_batch_size = 1;

    constexpr char kKey[] = "key";
    const std::string kValue(2 * 1024 * 1024, 'v');

    {
        auto status = PartitionSet(partition_.get(), kKey, kValue);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }
    {
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLog();
        BYTE_ASSERT(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }

    ReloadPartition();
    auto slot_id = CallHash(kKey);
    auto slot = partition_->index_->GetSlot(slot_id);
    ASSERT_TRUE(slot != nullptr);
    ASSERT_FALSE(slot->InMemory());

    PageStore::ReadPathTestCounters counters;
    partition_->page_store_->SetReadPathTestCounters(&counters);

    {
        std::string value;
        auto status = PartitionGet(partition_.get(), kKey, &value);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(kValue, value);
        EXPECT_EQ(1, counters.blockcache_gets);
        EXPECT_EQ(0, counters.blockcache_hits);
        EXPECT_EQ(1, counters.persistent_reads);
    }

    ASSERT_TRUE(partition_->index_->GetSlot(slot_id)->InMemory());

    auto evict_status = partition_->evicter_->TryEvict();
    ASSERT_TRUE(evict_status.ok()) << evict_status.ToString();
    ASSERT_FALSE(partition_->index_->GetSlot(slot_id)->InMemory());

    counters = PageStore::ReadPathTestCounters();
    {
        std::string value;
        auto status = PartitionGet(partition_.get(), kKey, &value);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(kValue, value);
        EXPECT_EQ(1, counters.blockcache_gets);
        EXPECT_EQ(1, counters.blockcache_hits);
        EXPECT_EQ(0, counters.persistent_reads);
    }

    partition_->page_store_->SetReadPathTestCounters(nullptr);
}

TEST_F(PartitionLoadTest, PageLogWithoutIndexItem) {
    {
        // Append page log.
        auto status = PartitionSet(partition_.get(), "key", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        // Reload. Expect partition triggers dump ans reconstructs slots.
        ReloadPartition();
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        auto slot = partition_->index_->GetSlot(CallHash("key"));
        ASSERT_TRUE(slot != nullptr);
    }
    {
        // Expect to get value1.
        std::string value;
        auto status = PartitionGet(partition_.get(), "key", &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value1");
    }
    {
        // Dump. Add index item.
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLog();
        BYTE_ASSERT(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }
    {
        // Get after dump.
        std::string value;
        auto status = PartitionGet(partition_.get(), "key", &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value1");
    }
}

TEST_F(PartitionLoadTest, ObjectDeleteLogWithIndexItem) {
    {
        // Append kv log.
        auto status = PartitionHSet(partition_.get(), "key", "field", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        // Dump. Add index item.
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLog();
        BYTE_ASSERT(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }
    {
        // Append more kv log.
        auto status = PartitionHSet(partition_.get(), "key", "field", "value2");
        ASSERT_TRUE(status.ok());
    }
    {
        // Delete.
        auto status = PartitionDel(partition_.get(), "key");
        ASSERT_TRUE(status.ok());
    }
    {
        // Reload. Expect slot holds delete log.
        uint64_t slot_id = CallHash("key");
        ReloadPartition();
        auto slot = partition_->index_->GetSlot(CallHash("key"));
        ASSERT_TRUE(slot != nullptr);
        ASSERT_TRUE(!partition_->slot_context_manager_->GetSlotLogs(slot_id).empty());
    }
    {
        // Expect to return not_found.
        std::string value;
        auto status = PartitionHGet(partition_.get(), "key", "field", &value);
        ASSERT_TRUE(status.IsNotFound()) << status.ToString();
    }
}

TEST_F(PartitionLoadTest, ObjectDeleteLogWithoutIndexItem) {
    {
        // Append kv log.
        auto status = PartitionHSet(partition_.get(), "key", "field", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        // Delete.
        auto status = PartitionDel(partition_.get(), "key");
        ASSERT_TRUE(status.ok());
    }
    {
        // Reload.
        ReloadPartition();
    }
    {
        // Expect to return not_found.
        std::string value;
        auto status = PartitionHGet(partition_.get(), "key", "field", &value);
        ASSERT_TRUE(status.IsNotFound());
    }
}

TEST_F(PartitionLoadTest, SimpleCheckpoint) {
    uint64_t slot_id_key1 = CallHash("key1");
    uint64_t slot_id_key2 = CallHash("key2");
    BYTE_ASSERT(slot_id_key1 != slot_id_key2);
    {
        // Set key1 and append kv log.
        auto status = PartitionHSet(partition_.get(), "key1", "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        // Set key2 and append kv log.
        auto status = PartitionHSet(partition_.get(), "key2", "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        // Set key1 and append more kv log
        auto status = PartitionHSet(partition_.get(), "key1", "field2", "value2");
        ASSERT_TRUE(status.ok());
    }
    {
        // Just dump key1.
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLogWithLimit(1);
        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }
    {
        // Reload. Expect key1 not in dirty slots(skip oplog replay).
        ReloadPartition();
        auto* dirty_slots_list = &partition_->index_->slot_context_manager_->dirty_slot_list_;
        BYTE_ASSERT(!dirty_slots_list->empty());
        for (auto iter = dirty_slots_list->begin(); iter != dirty_slots_list->end(); iter++) {
            BYTE_ASSERT(slot_id_key1 != iter->slot_id);
        }
    }
    {
        // Expect that partition does not lose key2.
        std::string value;
        auto status = PartitionHGet(partition_.get(), "key2", "field1", &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value1");
    }
}

TEST_F(PartitionLoadTest, Issues18) {
    RESTORE_FLAGS(FLAGS_stream_max_blob_size);
    RESTORE_FLAGS(FLAGS_storage_zone_size);
    RESTORE_FLAGS(FLAGS_storage_gc_max_slots_per_round);
    FLAGS_stream_max_blob_size = 1024 * 256;
    FLAGS_storage_zone_size = 1024;
    FLAGS_storage_gc_max_slots_per_round = 100000;
    partition_->storage_manager_->Stop();

    {
        // Append some kv log.
        for (int i = 0; i < 1000; ++i) {
            auto status =
                PartitionHSet(partition_.get(), "key" + std::to_string(i), "field", "value1");
            ASSERT_TRUE(status.ok());
            status = PartitionSet(partition_.get(), "key_set" + std::to_string(i), "value1");
            ASSERT_TRUE(status.ok());
        }
    }
    {
        // Dump. Add index item.
        FLAGS_storage_dump_slots_per_round = 100000;
        ASSERT_FALSE(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLog();
        ASSERT_TRUE(partition_->index_->slot_context_manager_->DirtySlotsEmpty());
    }
    {
        // Page&Oplog GC and append kv for persistent oplogger
        ASSERT_EQ(partition_->op_logger_->Start(), 0);
        partition_->storage_manager_->ReclaimPage();
        partition_->storage_manager_->ReclaimPage();
        auto status = PartitionHSet(partition_.get(), "key", "field", "value2");
        ASSERT_TRUE(status.ok());
        ASSERT_NE(partition_->op_logger_->Start(), 0);
    }
    {
        // Append some kv log.
        for (int i = 0; i < 1000; ++i) {
            auto status =
                PartitionHSet(partition_.get(), "key" + std::to_string(i), "field", "value1");
            ASSERT_TRUE(status.ok());
        }
    }
    {
        // Reload and invoke Page&Oplog GC
        ReloadPartition();
        ASSERT_GE(partition_->index_->meta_.start_oplog_id(), partition_->op_logger_->Start());
        partition_->storage_manager_->ReclaimOpLog();
        partition_->storage_manager_->ReclaimPage();
    }
    {
        // check kv
        for (int i = 0; i < 1000; ++i) {
            std::string value;
            auto status =
                PartitionHGet(partition_.get(), "key" + std::to_string(i), "field", &value);
            ASSERT_EQ(value, "value1");
        }
    }
}

}  // namespace partition
}  // namespace bcache2
