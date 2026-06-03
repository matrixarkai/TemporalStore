// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/partition.h"

#include <byte/algorithm/crc64.h>
#include <byte/string/format.h>
#include <byte/thread/async_thread.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <set>

#include "blockcache/blockcache.h"
#include "common/cmd_manager.h"
#include "common/coclosure.h"
#include "common/fiu_local.h"
#include "common/function_closure.h"
#include "common/macros.h"
#include "common/ring_array.h"
#include "common/slot.h"
#include "common/sync_closure.h"
#include "extension/feature/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/string/interface.pb.h"
#include "model/feature_model.h"
#include "model/hash_model.h"
#include "model/string_model.h"
#include "partition/allocator_manager.h"
#include "partition/condition.h"
#include "partition/storage/evicter.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/storage_manager.h"
#include "partition/test/cmd.h"
#include "partition/test/ips_test_config.h"
#include "partition/test/mem_stream.h"
#include "protocol/config.pb.h"
#include "protocol/server.pb.h"
#include "stream/log_based_env.h"
#include "stream/log_based_stream.h"
#include "test/common/temp_dir.h"

DECLARE_bool(model_deny_full_dump);
DECLARE_double(model_max_space_amplification);
DECLARE_uint64(model_size_tiered_compaction_min_bucket_size);
DECLARE_double(index_gc_usage_trigger);
DECLARE_uint64(evicter_max_memory_usage);
DECLARE_uint64(index_gc_max_num_per_round);
DECLARE_uint64(index_gc_bytes_threshold);
DECLARE_uint32(store_fiu_hang_interval_ms);
DECLARE_uint64(stream_max_blob_size);
DECLARE_uint64(storage_gc_max_bytes_per_round);
DECLARE_uint64(storage_gc_max_slots_per_round);
DECLARE_double(storage_gc_space_utility_threshold);
DECLARE_uint64(storage_gc_zone_destroy_delay_ms);
DECLARE_uint32(storage_zone_size);
DECLARE_uint64(expirer_scan_slots_per_round);
DECLARE_uint64(expirer_scan_in_memory_slots_per_round);
DECLARE_uint64(storage_dump_slots_per_round);
DECLARE_uint64(storage_oplog_delay_dump_length);
DECLARE_uint64(evict_count_limit);
DECLARE_uint64(evict_batch_size);
DECLARE_bool(storage_async);
DECLARE_bool(partition_commit_oplog);
DECLARE_uint64(storage_gc_zone_destroy_delay_ms);
DECLARE_uint64(stream_blob_deletion_min_age);
DECLARE_uint64(stream_blob_deletion_min_gap);
DECLARE_uint64(stream_max_blob_size);
DECLARE_uint64(feature_max_size);
DECLARE_uint64(page_store_compress_trigger_threshold);

namespace bcache2 {
namespace partition {

using model::StringModel;

class PartitionTest : public testing::Test {
 public:
    void SetUp() {
        ASSERT_EQ(fiu_init(0), 0);
        FLAGS_storage_async = false;
        FLAGS_storage_gc_zone_destroy_delay_ms = 0;
        FLAGS_stream_blob_deletion_min_age = 0;
        FLAGS_stream_blob_deletion_min_gap = 0;
        FLAGS_storage_gc_max_bytes_per_round = 1024 * 1024;
        FLAGS_storage_gc_max_slots_per_round = 1000;
        FLAGS_storage_gc_space_utility_threshold = 0.8;
        FLAGS_storage_gc_zone_destroy_delay_ms = 0;
        FLAGS_storage_zone_size = 64 * 1024;
        FLAGS_stream_max_blob_size = 1024 * 1024;
        FLAGS_start_storage_manager_when_loading = false;
        FLAGS_storage_dump_slots_per_round = 10000;
        FLAGS_storage_oplog_delay_dump_length = 0;
        FLAGS_page_store_compress_trigger_threshold = 100;
        FLAGS_enable_blockcache = true;
        FLAGS_blockcache_dram_capacity = 134217728;  // 128 MB
        FLAGS_blockcache_ssd_capacity = 0;

        bytestore_set_flag("bytestore_client_log_level", "1");
        bytestore_init();
        SetHashFunc(CallHash);
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

        if (FLAGS_enable_blockcache) {
            auto thread_id = std::this_thread::get_id();
            std::stringstream ss;
            ss << thread_id;
            FLAGS_blockcache_ssd_path = "./" + ss.str();
            blockcache_.reset(new bcache2::blockcache::BlockCache());
            auto start_status = blockcache_->Start();
            ASSERT_TRUE(start_status.ok()) << start_status.errorcode();
        }

        Partition::Options options;
        options.host = "127.0.0.1";
        options.port = 8888;
        options.env = env_.get();
        options.uri = uri_;
        options.load_version = ++load_version_;
        options.blockcache = blockcache_.get();
        partition_.reset(new Partition(options));
        Status status = partition_->Load();
        ASSERT_TRUE(status.ok()) << status;
        status = partition_->storage_manager_->Prepare();
        ASSERT_TRUE(status.ok());
        partition_->page_gc_->recycled_oplog_length_ = partition_->page_gc_->op_logger_->Start();
    }

    void TearDown() {
        if (partition_.get() != nullptr) {
            partition_->Unload();
        }
        bytestore_shutdown();

        if (FLAGS_enable_blockcache) {
            blockcache_->Stop();
        }
    }

    void ReloadPartitionWithOption(const Config& config) {
        partition_->Unload();
        partition_.reset();
        Partition::Options options;
        options.host = "127.0.0.1";
        options.port = 8888;
        options.env = env_.get();
        options.uri = uri_;
        options.load_version = ++load_version_;
        options.config = config;
        if (FLAGS_enable_blockcache) {
            blockcache_.reset(new bcache2::blockcache::BlockCache());
            auto start_status = blockcache_->Start();
            ASSERT_TRUE(start_status.ok());
        }
        options.blockcache = blockcache_.get();
        partition_.reset(new Partition(options));
        Status status = partition_->Load();
        ASSERT_TRUE(status.ok());
        status = partition_->storage_manager_->Prepare();
        ASSERT_TRUE(status.ok());
        partition_->page_gc_->recycled_oplog_length_ = partition_->page_gc_->op_logger_->Start();
    }

    void ReloadPartition() {
        partition_->Unload();

        partition_.reset();
        Partition::Options options;
        options.host = "127.0.0.1";
        options.port = 8888;
        options.env = env_.get();
        options.uri = uri_;
        options.load_version = ++load_version_;
        if (FLAGS_enable_blockcache) {
            blockcache_.reset(new bcache2::blockcache::BlockCache());
            auto start_status = blockcache_->Start();
            ASSERT_TRUE(start_status.ok());
        }
        options.blockcache = blockcache_.get();
        partition_.reset(new Partition(options));
        Status status = partition_->Load();
        ASSERT_TRUE(status.ok());
        status = partition_->storage_manager_->Prepare();
        ASSERT_TRUE(status.ok());
        partition_->page_gc_->recycled_oplog_length_ = partition_->page_gc_->op_logger_->Start();
    }

    void ScanIndexLog() {
        Index* index = partition_->index_.get();
        uint64_t offset = index->stream_->Stat().start_record_id;
        stream::ScopedIterator iter = index->stream_->NewIterator(offset, UINT64_MAX);
        std::cout << "BeginScanIndexLog===========: " << offset << std::endl;
        while (true) {
            std::cout << std::endl;
            Status status = iter->Next();
            if (!status.ok()) {
                std::cout << "EndScanIndexLog=====: " << status.ToString() << std::endl;
                break;
            }

            absl::string_view data = iter->Data();
            storage::IndexLog log;

            ASSERT_TRUE(log.ParseFromArray(data.data(), data.size()));
            std::cout << "{ slot_id: " << log.slot_id()
                      << ", oplog_sequence: " << log.oplog_sequence() << ", id: " << iter->Id()
                      << "}" << std::endl;

            if (UNLIKELY(log.has_meta_item())) {
                std::cout << "MetaLog: {version: " << log.meta_item().version()
                          << ", start_oplog_id: " << log.meta_item().start_oplog_id() << "}"
                          << std::endl;
                continue;
            }

            if (log.object_item_size() > 0) {
                for (int i = 0; i < log.object_item_size(); ++i) {
                    std::cout << "IndexObjectMetaLog: {"
                              << "version: " << log.object_item(i).version()
                              << ", object_id: " << log.object_item(i).object_id()
                              << ", ttl: " << log.object_item(i).ttl() << "}" << std::endl;
                }
                std::cout << "}" << std::endl;
            }

            for (int i = 0; i < log.item_size(); ++i) {
                const storage::IndexLog::IndexItem& item = log.item(i);
                std::cout << "IndexLog: {"
                          << ", pageid: " << item.page_id() << ", address: " << item.address()
                          << ", size: " << item.size() << ", object_id: " << item.object_id()
                          << ", inlog: " << item.in_log() << ", del: " << item.deleted() << "}"
                          << std::endl;
            }
        }
    }

    void ScanOpLog() {
        Index* index = partition_->index_.get();
        auto iter = partition_->op_logger_.get()->NewIterator(index->GetDumpedLogId());
        std::cout << "BeginScanOpLog===========: " << index->GetDumpedLogId() << std::endl;
        Status status;
        while ((status = iter->Next()).ok()) {
            const uint64_t log_id = iter->GetLogId();
            // const uint32_t log_size = iter->GetSize();
            const auto& oplog = iter->GetLog();
            std::cout << std::endl;
            for (int i = 0; i < oplog.item_size(); ++i) {
                const auto& log_item = oplog.item(i);
                std::cout << "{slot_id: " << log_item.slot_id() << ", log_id: " << log_id
                          << ", object_key: " << log_item.object_key()
                          << ", key: " << std::string(log_item.key())
                          << ", value: " << std::string(log_item.value())
                          << ", timestamp_ms: " << log_item.timestamp_ms()
                          << ", deleted: " << log_item.deleted()
                          << ", object_deleted: " << log_item.object_deleted()
                          << ", page_log: " << log_item.page_log()
                          << ", object_id: " << log_item.object_id()
                          << ", page_id: " << log_item.page_id() << ", ttl: " << log_item.ttl()
                          << ", meta_log: " << log_item.meta_log() << "}" << std::endl;
            }
        }
        std::cout << "EndScanOplog========: " << status.ToString() << std::endl;
    }

 protected:
    std::unique_ptr<byte::AsyncThreadPool> background_pool_;
    std::unique_ptr<stream::StoreLayer> store_layer_;
    std::unique_ptr<stream::LogBasedEnv> env_;
    TempDir temp_dir_;
    std::string uri_;
    std::unique_ptr<Partition> partition_;
    std::unique_ptr<bcache2::blockcache::BlockCache> blockcache_{nullptr};
    uint64_t load_version_ = 0;
};

#if 0
TEST_F(PartitionTest, GcCursorFullNormal) {
    for (size_t i = 0; i < 10; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->ReclaimOpLog();
        auto index = storage_manager->index_;
        auto cursor = index->WithGcReadCursor(1, false);
        std::set<std::string> ids;
        while (true) {
            uint64_t slot_id = 0;
            std::vector<PageIndex> indices;
            bool page_dirty = false;
            bool ok = cursor->Next(&slot_id, &indices, &page_dirty);
            if (!ok) {
                break;
            }
            for (auto& index : indices) {
                ids.emplace(byte::StringPrint("%lld:%d", slot_id, index.object_id));
                ASSERT_TRUE(!index.page_in_log);
            }
        }
        ASSERT_EQ(ids.size(), 10) << ids.size();
    }
}

TEST_F(PartitionTest, GcCursorBreakNormal) {
    for (size_t i = 0; i < 10; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    Index::GcReadCursor* cursor;
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->ReclaimOpLog();
        auto index = storage_manager->index_;
        cursor = index->WithGcReadCursor(1, false);
        std::set<std::string> ids;
        size_t count = 5;
        while (count-- > 0) {
            uint64_t slot_id = 0;
            std::vector<PageIndex> indices;
            bool page_dirty = false;
            bool ok = cursor->Next(&slot_id, &indices, &page_dirty);
            if (!ok) {
                break;
            }
            for (auto& index : indices) {
                ids.emplace(byte::StringPrint("%lld:%d", slot_id, index.object_id));
                ASSERT_TRUE(!index.page_in_log);
            }
        }
    }
    for (size_t i = 0; i < 10; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        storage_manager->ReclaimOpLog();
        std::set<std::string> ids;
        while (true) {
            uint64_t slot_id = 0;
            std::vector<PageIndex> indices;
            bool page_dirty = false;
            bool ok = cursor->Next(&slot_id, &indices, &page_dirty);
            if (!ok) {
                break;
            }
            for (auto& index : indices) {
                ids.emplace(byte::StringPrint("%lld:%d", slot_id, index.object_id));
                ASSERT_TRUE(!index.page_in_log);
            }
        }
        ASSERT_EQ(ids.size(), 0) << ids.size();
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        auto index = storage_manager->index_;
        auto cursor = index->WithGcReadCursor(2, false);  // new zone
        std::set<std::string> ids;
        while (true) {
            uint64_t slot_id = 0;
            std::vector<PageIndex> indices;
            bool page_dirty = false;
            bool ok = cursor->Next(&slot_id, &indices, &page_dirty);
            if (!ok) {
                break;
            }
            for (auto& index : indices) {
                ids.emplace(byte::StringPrint("%lld:%d", slot_id, index.object_id));
                ASSERT_TRUE(!index.page_in_log);
            }
        }
        ASSERT_EQ(ids.size(), 10);
    }
}
#endif

TEST_F(PartitionTest, GcPickNormal) {
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ false);
    }
    for (size_t i = 0; i < 10; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->ReclaimOpLog();
        auto gc = storage_manager->page_gc_;
        bool ok = gc->PickNextZone();
        ASSERT_TRUE(!ok);
    }
    for (size_t i = 0; i < 6; i++) {  // partial overwrite
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        storage_manager->ReclaimOpLog();
        auto gc = storage_manager->page_gc_;
        bool ok = gc->PickNextZone();
        ASSERT_TRUE(ok);
        ASSERT_EQ(gc->current_gc_zone_.zone_id, 1);
        // ASSERT_EQ(gc->current_gc_zone_.empty, false);  // non-empty
        ASSERT_TRUE(!gc->current_gc_zone_.page_log);
    }
    for (size_t i = 6; i < 10; i++) {  // continue overwrite
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->ReclaimOpLog();
        auto gc = storage_manager->page_gc_;
        bool ok = gc->PickNextZone();
        ASSERT_TRUE(ok);
        ASSERT_EQ(gc->current_gc_zone_.zone_id, 1);
        // ASSERT_EQ(gc->current_gc_zone_.empty, true);  // empty
        ASSERT_TRUE(!gc->current_gc_zone_.page_log);
    }
}

TEST_F(PartitionTest, GcMoveEmptyNormal) {
    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ false);
        storage_manager->ReclaimOpLog();
    }
    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        storage_manager->ReclaimOpLog();
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        auto gc = storage_manager->page_gc_;
        ASSERT_TRUE(gc->PickNextZone());
        // ASSERT_TRUE(gc->current_gc_zone_->empty);
        ASSERT_TRUE(gc->GcCurrentZone());
        // ASSERT_GT(gc->compacted_zones_.count(0), 0);
    }
}

TEST_F(PartitionTest, GcMoveNonEmptyNormal) {
    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ false);
        storage_manager->ReclaimOpLog();
    }
    for (size_t i = 0; i < 50; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        storage_manager->ReclaimOpLog();
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        auto gc = storage_manager->page_gc_;
        ASSERT_TRUE(gc->PickNextZone());
        // ASSERT_TRUE(!gc->current_gc_zone_->empty);
        // ASSERT_TRUE(!gc->GcCurrentZone());
        // ASSERT_EQ(gc->compacted_zones_.count(0), 0);
        ASSERT_TRUE(gc->GcCurrentZone());
        // ASSERT_GT(gc->compacted_zones_.count(0), 0);
    }
}
// UT for verify uniqueID
TEST_F(PartitionTest, UniqueIDforGCedZone) {
    std::string value1 = "";
    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        // Insert keys to the partition with hash set model
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "");
        ASSERT_TRUE(status.ok());
    }
    Index* index = partition_->index_.get();
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ false);
        auto pg = storage_manager->page_store_;
        ASSERT_EQ(pg->writing_zone_id_, 1);
        storage_manager->ReclaimOpLog();
    }
    // find the slot of the inserted first key, use the page on the slot to get its unique_id
    uint64_t slot_id = hash_func("key000000", 9);
    auto page_indexes = index->GetSlotPages(slot_id, false);
    std::string old_unique_id = partition_->page_store_->GetUniqueID(&page_indexes[0]);

    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        auto pg = storage_manager->page_store_;
        ASSERT_EQ(pg->writing_zone_id_, 2);
        storage_manager->ReclaimOpLog();
        auto gc = storage_manager->page_gc_;
        bool ok = gc->PickNextZone();
        ASSERT_TRUE(ok);
        ok = gc->GcCurrentZone();
        ASSERT_TRUE(ok);
        gc->PurgeCompactedZones();
        /*
        auto stat = storage_manager->gc_stat_;
        auto zone = stat->ZoneOf(0, false);
        ASSERT_TRUE(zone == nullptr);
        */
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        ASSERT_EQ(pg->writing_zone_id_, 1);  // recycle zone id
    }
    // Get the unique_id on the same page again, it was GCed and allocated again, version must be
    // different
    std::string new_unique_id = partition_->page_store_->GetUniqueID(&page_indexes[0]);
    EXPECT_NE(old_unique_id, new_unique_id);
}

TEST_F(PartitionTest, RecycleZoneIdNormal) {
    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ false);
        auto pg = storage_manager->page_store_;
        ASSERT_EQ(pg->writing_zone_id_, 1);
        storage_manager->ReclaimOpLog();
    }
    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        auto pg = storage_manager->page_store_;
        ASSERT_EQ(pg->writing_zone_id_, 2);
        storage_manager->ReclaimOpLog();
        auto gc = storage_manager->page_gc_;
        bool ok = gc->PickNextZone();
        ASSERT_TRUE(ok);
        ok = gc->GcCurrentZone();
        ASSERT_TRUE(ok);
        gc->PurgeCompactedZones();
        /*
        auto stat = storage_manager->gc_stat_;
        auto zone = stat->ZoneOf(0, false);
        ASSERT_TRUE(zone == nullptr);
        */
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        ASSERT_EQ(pg->writing_zone_id_, 1);  // recycle zone id
    }
}
TEST_F(PartitionTest, ZoneOnReloadNormal) {
    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }

    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ false);
        storage_manager->ReclaimOpLog();
        // zone 0
    }
    for (size_t i = 0; i < 50; i++) {
        std::stringstream si;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHSet(partition_.get(), "key" + si.str(), "field1", "value1");
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        storage_manager->ReclaimOpLog();
        // zone 0, 1
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->PrepareNewZone(/* force_new_zone = */ true);
        auto gc = storage_manager->page_gc_;
        ASSERT_TRUE(gc->PickNextZone());
        // ASSERT_TRUE(!gc->GcCurrentZone());
        ASSERT_TRUE(gc->GcCurrentZone());
        gc->PurgeCompactedZones();
        // zone 1, 2
        auto pg = storage_manager->page_store_;
        ASSERT_TRUE(pg->zones_[0].get() == nullptr);
        ASSERT_TRUE(pg->zones_[1].get() == nullptr);
        ASSERT_TRUE(pg->zones_[2].get() != nullptr);
        ASSERT_TRUE(pg->zones_[3].get() != nullptr);
        ASSERT_EQ(pg->writing_zone_id_, 3);
    }

    partition_->Unload();
    partition_.reset();
    // reload
    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    options.blockcache = blockcache_.get();
    partition_.reset(new Partition(options));
    Status status = partition_->Load();
    ASSERT_TRUE(status.ok());

    {
        auto storage_manager = partition_->storage_manager_.get();
        auto pg = storage_manager->page_store_;
        ASSERT_TRUE(pg->zones_[0].get() == nullptr);
        // ASSERT_EQ(pg->zones_[0].get()->Stat().length, 0);
        ASSERT_TRUE(pg->zones_[1].get() == nullptr);
        ASSERT_TRUE(pg->zones_[2].get() != nullptr);
        ASSERT_GT(pg->zones_[2].get()->stream->Stat().length, 0);
        ASSERT_TRUE(pg->zones_[3].get() != nullptr);
        ASSERT_GT(pg->zones_[3].get()->stream->Stat().length, 0);
        ASSERT_EQ(pg->writing_zone_id_, 3);
    }
    for (size_t i = 0; i < 100; i++) {
        std::stringstream si;
        std::string value;
        si << std::setw(6) << std::setfill('0') << i;
        Status status = PartitionHGet(partition_.get(), "key" + si.str(), "field1", &value);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(value, "value1");
    }
}

TEST_F(PartitionTest, GcBulkload) {
    for (size_t i = 0; i < 1000; i++) {
        std::stringstream si;
        si << std::setw(100) << std::setfill('0') << i;
        Status status =
            PartitionSet(partition_.get(), "key" + std::to_string(i), "value" + si.str());
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->ReclaimOpLog();
        // std::cout << storage_manager->gc_stat_->DebugZones() << std::endl;
    }
    for (size_t i = 0; i < 1000; i++) {
        std::stringstream si;
        si << std::setw(100) << std::setfill('0') << i;
        Status status =
            PartitionSet(partition_.get(), "key" + std::to_string(i), "value" + si.str());
        ASSERT_TRUE(status.ok());
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        storage_manager->ReclaimOpLog();
        // std::cout << storage_manager->gc_stat_->DebugZones() << std::endl;
    }
    {
        auto storage_manager = partition_->storage_manager_.get();
        auto gc = storage_manager->page_gc_;
        ASSERT_TRUE(gc->PickNextZone());
        // ASSERT_TRUE(!gc->GcCurrentZone());
        ASSERT_TRUE(gc->GcCurrentZone());
        ASSERT_TRUE(gc->PickNextZone());
        // ASSERT_TRUE(!gc->GcCurrentZone());
        ASSERT_TRUE(gc->GcCurrentZone());
        // std::cout << storage_manager->gc_stat_->DebugZones() << std::endl;
    }
    partition_->Unload();
    partition_.reset();
    // reload
    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    partition_.reset(new Partition(options));
    options.blockcache = blockcache_.get();
    Status status = partition_->Load();
    ASSERT_TRUE(status.ok());
    for (size_t i = 0; i < 1000; i++) {
        std::stringstream si;
        std::string value;
        si << std::setw(100) << std::setfill('0') << i;
        Status status = PartitionGet(partition_.get(), "key" + std::to_string(i), &value);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(value == "value" + si.str());
    }
}

TEST_F(PartitionTest, Simple) {
    Status status = PartitionHSet(partition_.get(), "key", "field", "value");
    ASSERT_TRUE(status.ok());

    std::string value;
    status = PartitionHGet(partition_.get(), "key", "field", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value");

    partition_->Unload();
    partition_.reset();
    // reload
    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    options.blockcache = blockcache_.get();
    partition_.reset(new Partition(options));
    status = partition_->Load();
    ASSERT_TRUE(status.ok());

    // hash get from new_partition
    status = PartitionHGet(partition_.get(), "key", "field", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value");
}

TEST_F(PartitionTest, OpLogDump) {
    // stop auto loop
    StorageManager* storage = partition_->storage_manager_.get();

    Status status = PartitionHSet(partition_.get(), "key", "field", "value");
    ASSERT_TRUE(status.ok());

    Index* index = partition_->index_.get();
    // OpLogger* oplogger = partition_->op_logger_.get();
    // uint64_t log_length = oplogger->Length();
    ASSERT_TRUE(!index->slot_context_manager_->DirtySlotsEmpty());
    storage->ReclaimOpLog();
    ASSERT_TRUE(index->slot_context_manager_->DirtySlotsEmpty());

    partition_->Unload();

    // reload
    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    options.blockcache = blockcache_.get();
    partition_.reset(new Partition(options));
    status = partition_->Load();
    ASSERT_TRUE(status.ok());
    OpLogger* oplogger = partition_->op_logger_.get();
    index = partition_->index_.get();
    storage = partition_->storage_manager_.get();
    storage->Stop();
    // oplogger = partition_->op_logger_.get();
    // BYTE_ASSERT(index->MinSlotVersion() == log_length);

    FLAGS_storage_oplog_delay_dump_length = 1024;
    {
        // undump length < delay dump length, not dump
        for (int i = 0; i < 10; i++) {
            Status status =
                PartitionHSet(partition_.get(), "key", "field" + std::to_string(i), "value");
            ASSERT_TRUE(status.ok());
        }
        ASSERT_TRUE(oplogger->UnDumpLength() < FLAGS_storage_oplog_delay_dump_length);
        ASSERT_TRUE(storage->ShouldDelayDumpOplog());
    }
    {
        // undump length > delay dump length, dump directly
        for (int i = 0; i < 50; i++) {
            Status status =
                PartitionHSet(partition_.get(), "key", "field" + std::to_string(i), "value");
            ASSERT_TRUE(status.ok());
        }
        ASSERT_TRUE(oplogger->UnDumpLength() > FLAGS_storage_oplog_delay_dump_length);
        ASSERT_FALSE(storage->ShouldDelayDumpOplog());
    }
}

TEST_F(PartitionTest, Feature) {
    std::string my_uid = "9";
    uint64_t my_ts = 777;
    uint64_t my_gid = 1;
    uint32_t my_at = 2;
    uint32_t my_dr = 3;
    uint64_t my_aid = 1;
    Controller ctrl;
    feature2::AddRequest feature_add_request;
    feature_add_request.set_key(my_uid);
    feature_add_request.set_format("protobuf");
    feature::FeaturePoint feature_point;

    auto feature_data = feature_add_request.add_point_list();
    feature_data->set_ts(my_ts);

    feature_point.set_gid(my_gid);
    feature_point.set_action_type(my_at);
    feature_point.set_duration(my_dr);
    feature_point.set_author_id(my_aid);
    std::string feature_val;
    feature_point.SerializeToString(&feature_val);
    feature_data->set_value(std::move(feature_val));
    feature2::AddResponse response1;
    Status status = PartitionFeatureAdd(partition_.get(), feature_add_request, response1);
    ASSERT_TRUE(status.ok());
    feature2::QueryRequest feature_query_request;
    feature_query_request.set_key(my_uid);
    feature_query_request.set_start_ts(0);
    feature_query_request.set_end_ts(100000);
    feature_query_request.set_count(1000);
    feature_query_request.set_format("protobuf");
    feature_query_request.add_filters("gid = 1");

    ctrl.Reset();
    feature2::QueryResponse response2;
    status = PartitionQueryRequest(partition_.get(), feature_query_request, response2);
    std::cout << "query st is " << status.ToString() << std::endl;
    ASSERT_TRUE(ctrl.status().ok());
    // ASSERT_EQ(response2.key(), my_uid);
    auto seq_list = response2.point_list();
    for (auto& seq : seq_list) {
        feature::FeaturePoint feature_point2;
        feature_point2.ParseFromString(seq.value());
        std::cout << "ts " << seq.ts() << " dr " << feature_point2.duration() << std::endl;
        ASSERT_EQ(seq.ts(), my_ts);
        ASSERT_EQ(feature_point2.gid(), my_gid);
        ASSERT_EQ(feature_point2.action_type(), my_at);
        ASSERT_EQ(feature_point2.duration(), my_dr);
        ASSERT_EQ(feature_point2.author_id(), my_aid);
    }
}

std::string ConstructFeatureData(uint64_t i) {
    feature::FeaturePoint feature_point;
    feature_point.set_gid(i);
    feature_point.set_action_type((uint32_t)i);
    feature_point.set_duration((uint32_t)i);
    feature_point.set_author_id(i);

    std::string feature_val;
    feature_point.SerializeToString(&feature_val);
    return feature_val;
}

TEST_F(PartitionTest, Feature_del) {
    std::string my_uid = "uid";
    uint64_t cnt = FLAGS_feature_max_size + 10;

    feature2::AddRequest feature_request1;
    feature_request1.set_key(my_uid);
    feature_request1.set_format("protobuf");
    auto points = feature_request1.mutable_point_list();
    for (uint64_t i = 0; i < cnt; i++) {
        auto feature_data = points->Add();
        feature_data->set_ts(i);
        auto feature_val = ConstructFeatureData(i);
        feature_data->set_value(std::move(feature_val));
    }

    feature2::AddResponse response1;
    Status status = PartitionFeatureAdd(partition_.get(), feature_request1, response1);
    ASSERT_TRUE(status.ok());

    feature2::QueryRequest feature_request2;
    feature_request2.set_key(my_uid);
    feature_request2.set_start_ts(0);
    feature_request2.set_end_ts(100000);
    feature_request2.set_count(FLAGS_feature_max_size + 10UL);
    feature_request2.set_format("protobuf");
    feature_request2.add_filters("gid > 0");

    feature2::QueryResponse response2;
    status = PartitionQueryRequest(partition_.get(), feature_request2, response2);

    ASSERT_TRUE(status.ok());
    // ASSERT_EQ(response2.key(), my_uid);
    auto seq_list = response2.point_list();
    ASSERT_TRUE(seq_list.size() <= (int)FLAGS_feature_max_size);

    for (auto& seq : seq_list) {
        feature::FeaturePoint feature_point2;
        feature_point2.ParseFromString(seq.value());
        std::cout << "ts " << seq.ts() << " dr " << feature_point2.duration() << std::endl;
    }
}

TEST_F(PartitionTest, DeleteObject) {
    Status status = PartitionHSet(partition_.get(), "key1", "field1", "value1");
    ASSERT_TRUE(status.ok());

    std::string value;
    status = PartitionHGet(partition_.get(), "key1", "field1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ("value1", value);

    status = PartitionDel(partition_.get(), "key1");
    ASSERT_TRUE(status.ok());

    ReloadPartition();

    status = PartitionHGet(partition_.get(), "key1", "field1", &value);
    ASSERT_TRUE(status.IsNotFound());
}

TEST_F(PartitionTest, Evicter) {
    std::string value_base = std::string(1024, 'a');
    Index* index = partition_->index_.get();
    Evicter* evicter = partition_->evicter_.get();

    // case 1.1
    Status status = PartitionSet(partition_.get(), "key1", value_base + "value1");
    ASSERT_TRUE(status.ok());

    CoSleep(1000 * 1000 * 2);

    status = PartitionSet(partition_.get(), "key2", value_base + "value11");
    ASSERT_TRUE(status.ok());

    evicter->config_.mutable_maxmemory()->set_value(0);
    evicter->config_.mutable_policy_type()->set_value(PolicyType::LRU);
    evicter->config_.mutable_operation_type()->set_value(OperationType::DELETE);
    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok());

    // case 3.3/3.1/1.3
    std::string value;
    status = PartitionGet(partition_.get(), "key1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value1", value);

    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value11", value);

    evicter->config_.mutable_maxmemory()->set_value(
        evicter->allocator_manager_->GetTotalAllocedSize());

    CoSleep(1000 * 1000 * 2);
    status = PartitionSet(partition_.get(), "key3", value_base + "value111");
    ASSERT_TRUE(status.ok());

    CoSleep(1000 * 1000 * 2);
    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value11", value);

    evicter->config_.mutable_policy_type()->set_value(PolicyType::LRU);
    evicter->config_.mutable_operation_type()->set_value(OperationType::DELETE);
    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok()) << status;

    status = PartitionGet(partition_.get(), "key1", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = PartitionGet(partition_.get(), "key3", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(value_base + "value11", value);

    // case 3.2/3.3/1.2
    evicter->config_.mutable_maxmemory()->set_value(1);
    evicter->config_.mutable_operation_type()->set_value(OperationType::DUMP);

    uint64_t slot_id = hash_func("key2", 4);
    SlotNode* slot = &index->slot_map_[slot_id];

    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok()) << status;

    ASSERT_TRUE(!slot->InMemory());
    ASSERT_EQ(slot->GetObjectNum(), 0);

    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value11", value);

    ASSERT_TRUE(slot->InMemory());
    ASSERT_GT(slot->GetObjectNum(), 0);

    // cas 2.1
    for (int i = 0; i < 5; i++) {
        std::cout << "set key" << std::endl;
        status =
            PartitionSet(partition_.get(), "key_limit_" + std::to_string(i), value_base + "value");
        ASSERT_TRUE(status.ok());
        CoSleep(1000 * 1000 * 1);
    }
    uint64_t no_dump_slot_id = hash_func("key_limit_4", 11);
    SlotNode* no_dump_slot = &index->slot_map_[no_dump_slot_id];

    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok());

    no_dump_slot = &index->slot_map_[no_dump_slot_id];
    slot = &index->slot_map_[slot_id];
    ASSERT_TRUE(!slot->InMemory());
    ASSERT_EQ(slot->GetObjectNum(), 0);

    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value11", value);

    for (int i = 0; i < 5; i++) {
        status = PartitionGet(partition_.get(), "key_limit_" + std::to_string(i), &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value_base + "value", value);
    }

    ASSERT_TRUE(slot->InMemory());
    ASSERT_GT(slot->GetObjectNum(), 0);

    ASSERT_TRUE(no_dump_slot->InMemory());
    ASSERT_GT(no_dump_slot->GetObjectNum(), 0);
}

TEST_F(PartitionTest, BatchEvicter) {
    std::string value_base = std::string(1024, 'a');

    // stop auto loop
    RESTORE_FLAGS(FLAGS_evict_count_limit);
    RESTORE_FLAGS(FLAGS_evict_batch_size);
    FLAGS_evict_count_limit = 5;
    FLAGS_evict_batch_size = 2;

    Index* index = partition_->index_.get();
    Evicter* evicter = partition_->evicter_.get();

    // case 1.1
    Status status = PartitionSet(partition_.get(), "key1", value_base + "value1");
    ASSERT_TRUE(status.ok());

    CoSleep(1000 * 1000 * 2);

    status = PartitionSet(partition_.get(), "key2", value_base + "value11");
    ASSERT_TRUE(status.ok());

    evicter->config_.mutable_maxmemory()->set_value(0);
    evicter->config_.mutable_policy_type()->set_value(PolicyType::LRU);
    evicter->config_.mutable_operation_type()->set_value(OperationType::DELETE);
    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok());

    // case 3.3/3.1/1.3
    std::string value;
    status = PartitionGet(partition_.get(), "key1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value1", value);

    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value11", value);

    evicter->config_.mutable_maxmemory()->set_value(
        evicter->allocator_manager_->GetTotalAllocedSize());

    CoSleep(1000 * 1000 * 2);
    status = PartitionSet(partition_.get(), "key3", value_base + "value111");
    ASSERT_TRUE(status.ok());

    CoSleep(1000 * 1000 * 2);
    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value11", value);

    evicter->config_.mutable_policy_type()->set_value(PolicyType::LRU);
    evicter->config_.mutable_operation_type()->set_value(OperationType::DELETE);
    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok()) << status;

    status = PartitionGet(partition_.get(), "key1", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = PartitionGet(partition_.get(), "key3", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(value_base + "value11", value);

    // case 3.2/3.3/1.2
    evicter->config_.mutable_maxmemory()->set_value(1);
    evicter->config_.mutable_operation_type()->set_value(OperationType::DUMP);

    uint64_t slot_id = hash_func("key2", 4);
    SlotNode* slot = &index->slot_map_[slot_id];

    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok()) << status;

    ASSERT_TRUE(!slot->InMemory());
    ASSERT_EQ(slot->GetObjectNum(), 0);

    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value11", value);

    ASSERT_TRUE(slot->InMemory());
    ASSERT_GT(slot->GetObjectNum(), 0);

    // cas 2.1
    for (int i = 0; i < 5; i++) {
        std::cout << "set key" << std::endl;
        status =
            PartitionSet(partition_.get(), "key_limit_" + std::to_string(i), value_base + "value");
        ASSERT_TRUE(status.ok());
        CoSleep(1000 * 1000 * 1);
    }
    uint64_t no_dump_slot_id = hash_func("key_limit_4", 11);
    SlotNode* no_dump_slot = &index->slot_map_[no_dump_slot_id];
    ASSERT_TRUE(no_dump_slot->Dirty());

    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok());

    no_dump_slot = &index->slot_map_[no_dump_slot_id];
    slot = &index->slot_map_[slot_id];
    ASSERT_TRUE(!slot->InMemory());
    ASSERT_EQ(slot->GetObjectNum(), 0);

    ASSERT_TRUE(no_dump_slot->InMemory());
    ASSERT_GT(no_dump_slot->GetObjectNum(), 0);

    status = evicter->TryEvict();
    ASSERT_TRUE(status.ok()) << status;

    no_dump_slot = &index->slot_map_[no_dump_slot_id];
    slot = &index->slot_map_[slot_id];
    ASSERT_TRUE(!no_dump_slot->InMemory());
    ASSERT_EQ(no_dump_slot->GetObjectNum(), 0);

    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_base + "value11", value);

    for (int i = 0; i < 5; i++) {
        status = PartitionGet(partition_.get(), "key_limit_" + std::to_string(i), &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value_base + "value", value);
    }

    ASSERT_TRUE(slot->InMemory());
    ASSERT_GT(slot->GetObjectNum(), 0);

    ASSERT_TRUE(no_dump_slot->InMemory());
    ASSERT_GT(no_dump_slot->GetObjectNum(), 0);
}

TEST_F(PartitionTest, IndexGC) {
    FLAGS_index_gc_bytes_threshold = 0;
    FLAGS_index_gc_usage_trigger = 0.9;

    // stop auto loop
    StorageManager* storage = partition_->storage_manager_.get();
    Index* index = partition_->index_.get();
    auto start_record_id = index->stream_->Stat().start_record_id;

    std::map<std::string, std::string> origin_data;
    std::string field = "field";
    for (int i = 0; i < 1000; i++) {
        Controller ctrl;
        CmdRequest request;
        std::string key = "key" + std::to_string(random() % 10);

        if (random() % 10 == 0) {
            Status status = PartitionHDel(partition_.get(), key, field);
            origin_data[key] = "";
            ASSERT_TRUE(status.ok() || status.IsNotFound()) << status.ToString();
            LOG_DEBUG("HDel").put("Key", key).put("Field", field);
        } else {
            std::string value = "value" + std::to_string(random());
            Status status = PartitionHSet(partition_.get(), key, field, value);
            ASSERT_TRUE(status.ok());
            origin_data[key] = value;
            LOG_DEBUG("HSet").put("Key", key).put("Field", field).put("Value", value);
        }

        if (i % 3 == 1) {
            storage->ReclaimOpLog();
        }

        if (i % 2 == 0) {
            storage->ReclaimIndex();
        }
    }

    std::cout << "start to reclaim index after stop write" << std::endl;
    for (int i = 0; i < 10; i++) {
        storage->ReclaimIndex();
    }
    Controller ctrl;
    SYNC_CALL(index->Commit, &ctrl);

    auto new_start_record_id = index->stream_->Stat().start_record_id;
    std::cout << "start_record_id " << start_record_id << ":" << new_start_record_id << std::endl;
    ASSERT_TRUE(new_start_record_id != start_record_id);

    for (auto& kv : origin_data) {
        std::string value;
        bool exist = false;
        Status status = PartitionHGetWithExist(partition_.get(), kv.first, field, &value, &exist);
        ASSERT_TRUE(status.ok()) << status.ToString();
        if (kv.second.empty()) {
            ASSERT_FALSE(exist);
        } else {
            ASSERT_TRUE(exist);
            ASSERT_TRUE(kv.second == value);
        }
    }

    std::cout << "++++++++++ reload +++++++++++++" << std::endl;
    // reload
    partition_->Unload();

    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    options.blockcache = blockcache_.get();
    partition_.reset(new Partition(options));
    auto status = partition_->Load();
    ASSERT_TRUE(status.ok());
    index = partition_->index_.get();

    std::cout << "======== start to check get value ==============" << std::endl;

    for (auto& kv : origin_data) {
        std::string value;
        Status status = PartitionHGet(partition_.get(), kv.first, field, &value);
        if (kv.second.empty()) {
            ASSERT_TRUE(status.IsNotFound()) << status.ToString() << ", Key: " << kv.first
                                             << ", Field: " << field << ", Value: " << value;
        } else {
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_EQ(kv.second, value);
        }
    }
}

TEST_F(PartitionTest, IndexUpdateMeta) {
    {
        // Check initial meta info.
        storage::IndexLog::MetaItem& meta = partition_->index_->meta_;
        ASSERT_EQ(static_cast<uint64_t>(meta.start_oplog_id()), 0);
    }
    uint64_t cur_start_oplog_id = 0;
    {
        // Write logs and update meta.(checkpint and version)
        uint64_t old_meta_version = partition_->index_->meta_.version();
        auto status = PartitionHSet(partition_.get(), "key1", "field1", "value1");
        ASSERT_TRUE(status.ok());
        status = PartitionHSet(partition_.get(), "key2", "field1", "value1");
        ASSERT_TRUE(status.ok());

        BYTE_ASSERT(!partition_->index_->slot_context_manager_->DirtySlotsEmpty());
        partition_->storage_manager_->ReclaimOpLog();
        BYTE_ASSERT(partition_->index_->slot_context_manager_->DirtySlotsEmpty());

        storage::IndexLog::MetaItem& meta = partition_->index_->meta_;
        cur_start_oplog_id = static_cast<uint64_t>(meta.start_oplog_id());
        EXPECT_NE(cur_start_oplog_id, 0);
        ASSERT_EQ(static_cast<uint64_t>(meta.version()), old_meta_version + 1);
    }
    {
        // Dump index meta.
        Controller ctrl;
        SYNC_CALL(partition_->storage_manager_->index_->Commit, &ctrl);
    }
    {
        // Reload. Expect loading meta correctly.
        ReloadPartition();
        storage::IndexLog::MetaItem& meta = partition_->index_->meta_;
        ASSERT_EQ(static_cast<uint64_t>(meta.start_oplog_id()), cur_start_oplog_id);
    }
    {
        uint64_t old_meta_version = partition_->index_->meta_.version();
        // Write oplog, update meta again.
        auto status = PartitionHSet(partition_.get(), "key1", "field2", "value2");
        ASSERT_TRUE(status.ok());
        status = PartitionHSet(partition_.get(), "key2", "field2", "value2");
        ASSERT_TRUE(status.ok());

        partition_->storage_manager_->ReclaimOpLog();

        uint64_t prev_start_oplog_id = cur_start_oplog_id;
        storage::IndexLog::MetaItem& meta = partition_->index_->meta_;
        cur_start_oplog_id = static_cast<uint64_t>(meta.start_oplog_id());
        EXPECT_GT(cur_start_oplog_id, prev_start_oplog_id);

        ASSERT_EQ(static_cast<uint64_t>(meta.version()), old_meta_version + 1);
        Controller ctrl;
        SYNC_CALL(partition_->storage_manager_->index_->Commit, &ctrl);
    }
    {
        // Reload again.
        ReloadPartition();
        storage::IndexLog::MetaItem& meta = partition_->index_->meta_;
        ASSERT_EQ(static_cast<uint64_t>(meta.start_oplog_id()), cur_start_oplog_id);
        Controller ctrl;
        SYNC_CALL(partition_->storage_manager_->index_->Commit, &ctrl);
    }
    {
        // Expect index gc rewrites latest index meta.
        uint64_t old_meta_version = partition_->index_->meta_.version();
        FLAGS_index_gc_bytes_threshold = 0;
        FLAGS_index_gc_usage_trigger = 1.1;
        FLAGS_index_gc_max_num_per_round = 10000;

        partition_->storage_manager_->ReclaimIndex();
        storage::IndexLog::MetaItem& meta = partition_->index_->meta_;
        ASSERT_EQ(static_cast<uint64_t>(meta.version()), old_meta_version + 1);
        ASSERT_EQ(static_cast<uint64_t>(meta.start_oplog_id()), cur_start_oplog_id);
    }
}

TEST_F(PartitionTest, HSetTest) {
    StorageManager* storage = partition_->storage_manager_.get();
    storage->Stop();

    Status status = PartitionHSet(partition_.get(), "key1", "field1", "value1");  // no compress
    ASSERT_TRUE(status.ok());

    std::string value;
    status = PartitionHGet(partition_.get(), "key1", "field1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_STREQ("value1", value.c_str());

    std::string value2 = "value2";
    for (int i = 0; i < 200; i++) {
        value2 += std::to_string(random() % 10);
    }
    status = PartitionHSet(partition_.get(), "key2", "field1", value2);  // trigger compress
    ASSERT_TRUE(status.ok());

    storage->ReclaimOpLog();

    partition_->Unload();
    partition_.reset();
    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    options.blockcache = blockcache_.get();
    partition_.reset(new Partition(options));
    status = partition_->Load();
    ASSERT_TRUE(status.ok());

    status = PartitionHGet(partition_.get(), "key1", "field1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_STREQ("value1", value.c_str());

    status = PartitionHGet(partition_.get(), "key2", "field1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value2.compare(value), 0);
}

TEST_F(PartitionTest, HDelTest) {
    Status status = PartitionHSet(partition_.get(), "key1", "field1", "value1");
    ASSERT_TRUE(status.ok());

    std::string value;
    status = PartitionHGet(partition_.get(), "key1", "field1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_STREQ("value1", value.c_str());

    status = PartitionHDel(partition_.get(), "key1", "field1");
    ASSERT_TRUE(status.ok());

    partition_->Unload();
    partition_.reset();
    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    options.blockcache = blockcache_.get();
    partition_.reset(new Partition(options));
    status = partition_->Load();
    ASSERT_TRUE(status.ok());

    bool exist = false;
    status = PartitionHGetWithExist(partition_.get(), "key1", "field1", &value, &exist);
    ASSERT_FALSE(exist);
}

TEST_F(PartitionTest, StringTest) {
    Status status = PartitionSet(partition_.get(), "key1", "value1");
    ASSERT_TRUE(status.ok());

    std::string value;
    status = PartitionGet(partition_.get(), "key1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_STREQ("value1", value.c_str());

    ReloadPartition();

    status = PartitionGet(partition_.get(), "key1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_STREQ("value1", value.c_str());
}

TEST_F(PartitionTest, Object) {
    ObjectManager* obj_manager = partition_->object_manager_.get();
    std::string key = "obj_new";
    std::string value = "obj_value";
    uint64_t slot_id = hash_func(key.data(), key.size());

    Status status = PartitionSet(partition_.get(), key, value);
    ASSERT_TRUE(status.ok());
    Object object;
    ASSERT_TRUE(obj_manager
                    ->NewObject(slot_id, model::ModelManager::GetModelId<StringModel>(), key,
                                &object, false)
                    .IsAlreadyExists());

    ReloadPartition();
    std::string read_value = "";
    status = PartitionGet(partition_.get(), key, &read_value);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(read_value == value);
    obj_manager = partition_->object_manager_.get();
    ASSERT_TRUE(obj_manager
                    ->NewObject(slot_id, model::ModelManager::GetModelId<StringModel>(), key,
                                &object, false)
                    .IsAlreadyExists());

    ReloadPartition();
    status = PartitionDel(partition_.get(), key);
    ASSERT_TRUE(status.ok());
    status = PartitionGet(partition_.get(), key, &read_value);
    ASSERT_TRUE(status.IsNotFound());

    ReloadPartition();
    status = PartitionGet(partition_.get(), key, &read_value);
    ASSERT_TRUE(status.IsNotFound());
    status = PartitionDel(partition_.get(), key);
    ASSERT_TRUE(status.IsNotFound());

    // Multi Object
    auto reload_hash_func = [](const char* data, uint64_t len) -> uint64_t { return 0; };
    SetHashFunc(reload_hash_func);
    // ReloadPartition();
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string val1 = "val1";
    std::string val2 = "val2";
    status = PartitionSet(partition_.get(), key1, val1);
    ASSERT_TRUE(status.ok());
    status = PartitionSet(partition_.get(), key2, val2);
    ASSERT_TRUE(status.ok());
    ReloadPartition();

    std::string read_val1;
    std::string read_val2;
    status = PartitionGet(partition_.get(), key1, &read_val1);
    ASSERT_TRUE(status.ok());
    status = PartitionGet(partition_.get(), key2, &read_val2);
    ASSERT_TRUE(status.ok());
    BYTE_ASSERT(val1 == read_val1);
    BYTE_ASSERT(val2 == read_val2);
    status = PartitionDel(partition_.get(), key2);
    ASSERT_TRUE(status.ok());
    status = PartitionDel(partition_.get(), key1);
    ASSERT_TRUE(status.ok());
    ReloadPartition();
    val1 = "val1_1";
    status = PartitionSet(partition_.get(), key1, val1);
    ASSERT_TRUE(status.ok());
    status = PartitionGet(partition_.get(), key1, &read_val1);
    ASSERT_TRUE(status.ok());
    BYTE_ASSERT(val1 == read_val1);
    ReloadPartition();
    status = PartitionGet(partition_.get(), key1, &read_val1);
    ASSERT_TRUE(status.ok());
    BYTE_ASSERT(val1 == read_val1);
    SetHashFunc(CallHash);
    ReloadPartition();
    status = PartitionGet(partition_.get(), key1, &read_val1);
    ASSERT_TRUE(status.IsNotFound());

    std::cout << partition_->GetInfo().ShortDebugString() << "\n";
}

TEST_F(PartitionTest, IndexGcEfficiency) {
    FLAGS_index_gc_bytes_threshold = 0;
    FLAGS_index_gc_usage_trigger = 1.1;
    FLAGS_index_gc_max_num_per_round = 200;

    // stop auto loop
    StorageManager* storage = partition_->storage_manager_.get();
    Index* index = partition_->index_.get();
    auto start_record_id = index->stream_->Stat().start_record_id;

    int key_count = 10000;
    std::map<std::string, std::string> origin_data;
    for (int i = 0; i < key_count; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(random());
        Status status = PartitionSet(partition_.get(), key, value);
        ASSERT_TRUE(status.ok());
        origin_data[key] = value;
        storage->ReclaimOpLog();
    }

    for (int i = 0; i < key_count; i++) {
        std::string key = "key" + std::to_string(i);
        if (i % 2 == 0) {
            Status status = PartitionDel(partition_.get(), key);
            ASSERT_TRUE(status.ok() || status.IsNotFound());
            origin_data[key] = "";
        } else {
            std::string value = "value" + std::to_string(random());
            Status status = PartitionSet(partition_.get(), key, value);
            ASSERT_TRUE(status.ok());
            origin_data[key] = value;
        }
        storage->ReclaimOpLog();
    }

    for (auto& kv : origin_data) {
        std::string value;
        Status status = PartitionGet(partition_.get(), kv.first, &value);
        if (kv.second.empty()) {
            ASSERT_TRUE(status.IsNotFound()) << status.ToString();
        } else {
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_TRUE(kv.second == value);
        }
    }

    std::cout << "start to reclaim index after stop write" << std::endl;
    auto log_length_before_reclaim = index->stream_->Stat().usage_bytes;
    for (int i = 0; i < 500; i++) {
        storage->ReclaimIndex();
        // append noop log to push forward persistent truncated offset(need cross block)
        index->OnMetaUpdate();
        if (i % 100 == 0) {
            std::cout << "gc_utility=" << index->gc_utility_ << std::endl;
        }
    }

    Controller ctrl;
    SYNC_CALL(index->Commit, &ctrl);

    auto new_start_record_id = index->stream_->Stat().start_record_id;
    auto log_length_after_reclaim = index->stream_->Stat().usage_bytes;
    ASSERT_LT(log_length_after_reclaim, log_length_before_reclaim);
    std::cout << "index log length before gc: " << log_length_before_reclaim
              << " after gc: " << log_length_after_reclaim << std::endl;

    std::cout << "before reload, start_record_id " << start_record_id << ":" << new_start_record_id
              << " ,gc_utility : " << index->gc_utility_ << std::endl;

    SYNC_CALL(index->Commit, &ctrl);

    std::cout << "++++++++++ reload +++++++++++++" << std::endl;
    ReloadPartition();
    storage = partition_->storage_manager_.get();
    index = partition_->index_.get();
    storage->ReclaimIndex();  // refresh index gc utility
    std::cout << "after reload, start_record_id " << start_record_id << ":"
              << index->stream_->Stat().start_record_id << " ,gc_utility : " << index->gc_utility_
              << std::endl;

    for (auto& kv : origin_data) {
        std::string value;
        Status status = PartitionGet(partition_.get(), kv.first, &value);
        if (kv.second.empty()) {
            ASSERT_TRUE(status.IsNotFound()) << status.ToString();
        } else {
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_TRUE(kv.second == value);
        }
    }

    log_length_after_reclaim = index->stream_->Stat().usage_bytes;
    std::cout << "index slot count = " << index->slot_map_.size()
              << ", avg log item num = " << index->avg_log_item_.average(1.0)
              << ", avg index log size = " << index->avg_log_size_.average(1.0) << std::endl;
    std::cout << "after gc, index gc utility : " << index->gc_utility_ << std::endl;
    std::cout << "index log length before gc: " << log_length_before_reclaim
              << " after gc: " << log_length_after_reclaim << std::endl;
    ASSERT_LT(log_length_after_reclaim, log_length_before_reclaim);
}

TEST_F(PartitionTest, IndexDirtyGc) {
    FLAGS_index_gc_bytes_threshold = 1;
    FLAGS_index_gc_usage_trigger = 1.0;
    FLAGS_index_gc_max_num_per_round = 100;

    // stop auto loop
    StorageManager* storage = partition_->storage_manager_.get();
    Index* index = partition_->index_.get();
    auto start_record_id = index->stream_->Stat().start_record_id;
    Controller ctrl;

    std::map<std::string, std::string> origin_data;
    int key_count = 1000;
    for (int i = 0; i < key_count; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(random());
        Status status = PartitionSet(partition_.get(), key, value);
        ASSERT_TRUE(status.ok());
        storage->ReclaimOpLog();
        origin_data[key] = value;
    }

    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(random() % key_count);

        if (random() % 5 == 0) {
            origin_data[key] = "";
            Status status = PartitionDel(partition_.get(), key);
            ASSERT_TRUE(status.ok() || status.IsNotFound());
        } else {
            std::string value = "value" + std::to_string(random() % key_count);
            origin_data[key] = value;
            Status status = PartitionSet(partition_.get(), key, value);
            ASSERT_TRUE(status.ok());
        }

        storage->ReclaimOpLog();
    }

    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(random() % key_count);

        if (random() % 5 == 0) {
            origin_data[key] = "";
            Status status = PartitionDel(partition_.get(), key);
            ASSERT_TRUE(status.ok() || status.IsNotFound());
        } else {
            std::string value = "value" + std::to_string(random() % key_count);
            origin_data[key] = value;
            Status status = PartitionSet(partition_.get(), key, value);
            ASSERT_TRUE(status.ok());
        }
    }
    SYNC_CALL(index->Commit, &ctrl);

    std::cout << "start to reclaim index after stop write" << std::endl;
    for (int i = 0; i < 10; i++) {
        storage->ReclaimIndex();
    }

    SYNC_CALL(index->Commit, &ctrl);
    auto new_start_record_id = index->stream_->Stat().start_record_id;
    std::cout << "after gc start_record_id " << start_record_id << ":" << new_start_record_id
              << std::endl;
    ASSERT_GT(new_start_record_id, start_record_id);

    for (auto& kv : origin_data) {
        std::string value;
        Status status = PartitionGet(partition_.get(), kv.first, &value);
        if (kv.second.empty()) {
            ASSERT_TRUE(status.IsNotFound()) << status.ToString();
        } else {
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_TRUE(kv.second == value);
        }
    }

    std::cout << "++++++++++ reload +++++++++++++" << std::endl;
    ReloadPartition();
    storage = partition_->storage_manager_.get();
    index = partition_->index_.get();

    for (auto& kv : origin_data) {
        std::string value;
        Status status = PartitionGet(partition_.get(), kv.first, &value);
        if (kv.second.empty()) {
            ASSERT_TRUE(status.IsNotFound()) << status.ToString();
        } else {
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_TRUE(kv.second == value);
        }
    }
}

TEST_F(PartitionTest, SetConfig) {
    {
        Config config;
        config.mutable_evicter_config()->mutable_maxmemory()->set_value(3000);
        config.mutable_extend_config()->insert({"test_config", "test_value"});
        ASSERT_EQ(partition_->options_.config.evicter_config().maxmemory().value(), 0);
        ASSERT_EQ(partition_->evicter_->config_.maxmemory().value(), 0);
        ASSERT_EQ(partition_->options_.config.extend_config().size(), 0);
        config.set_version(partition_->GetConfig().version() + 1);
        partition_->SetConfig(config);
        ASSERT_EQ(partition_->options_.config.evicter_config().maxmemory().value(), 3000);
        ASSERT_EQ(partition_->evicter_->config_.maxmemory().value(), 3000);
        ASSERT_EQ(partition_->options_.config.extend_config().at("test_config"), "test_value");

        ReloadPartition();
    }
}

TEST_F(PartitionTest, SmallerLoadVersion) {
    partition_->Unload();
    partition_.reset();

    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = load_version_;
    options.blockcache = blockcache_.get();
    std::unique_ptr<Partition> partition(new Partition(options));
    Status status = partition->Load();
    ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
    ASSERT_EQ(partition->GetStage(), PartitionLoadStage::FAILED);

    ASSERT_TRUE(partition->Unload().ok());
    ASSERT_EQ(partition->GetStage(), PartitionLoadStage::UNLOADED);
}

TEST_F(PartitionTest, Staled) {
    RESTORE_FLAGS(FLAGS_storage_async);
    // Write
    {
        CmdRequest request;
        str::SetRequest* set_request = request.mutable_string_request()->mutable_set_request();
        set_request->set_key("key100");
        set_request->set_value("value100");

        CmdResponse response;
        Controller ctrl;
        SYNC_CALL(partition_->ExecuteCmd, &ctrl, &request, &response);
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    }

    // Load new partition
    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    options.blockcache = blockcache_.get();
    std::unique_ptr<Partition> partition(new Partition(options));
    Status status = partition->Load();
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Write in old partition
    {
        CmdRequest request;
        str::SetRequest* set_request = request.mutable_string_request()->mutable_set_request();
        set_request->set_key("key101");
        set_request->set_value("value101");

        CmdResponse response;
        Controller ctrl;
        SYNC_CALL(partition_->ExecuteCmd, &ctrl, &request, &response);
        ASSERT_TRUE(ctrl.status().IsStreamAbort()) << ctrl.status().ToString();
    }

    FLAGS_storage_async = true;

    // Read in old partition
    {
        CmdRequest request;
        str::GetRequest* get_request = request.mutable_string_request()->mutable_get_request();
        get_request->set_key("key100");

        CmdResponse response;
        Controller ctrl;
        SYNC_CALL(partition_->ExecuteCmd, &ctrl, &request, &response);
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
        ASSERT_STREQ("value100", response.string_response().get_response().value().c_str());
    }

    partition->Unload();
}

TEST_F(PartitionTest, Ttl) {
    RESTORE_FLAGS(FLAGS_expirer_scan_in_memory_slots_per_round);
    RESTORE_FLAGS(FLAGS_expirer_scan_slots_per_round);
    FLAGS_expirer_scan_in_memory_slots_per_round = 0;
    FLAGS_expirer_scan_slots_per_round = 0;

    std::string key = "ttl_key";
    std::string set_value = "ttl_vaule";
    std::string get_value;
    uint64_t set_ttl = 0;
    uint64_t get_ttl = 0;
    Status status = PartitionSet(partition_.get(), key, set_value);
    ASSERT_TRUE(status.ok());

    status = PartitionGet(partition_.get(), key, &get_value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(get_value, set_value);

    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_ttl, 0);

    set_ttl = 10 * 1000;
    status = PartitionExpire(partition_.get(), key, set_ttl);
    ASSERT_TRUE(status.ok());

    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(status.ok());

    ReloadPartition();

    CoSleep(1000 * 1000 * 2);
    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(get_ttl > 0 && get_ttl < 10 * 1000);

    set_ttl = 3000;
    set_value = "setex_value";
    status = PartitionSetEx(partition_.get(), key, set_value, set_ttl);
    ASSERT_TRUE(status.ok());

    ReloadPartition();
    partition_->storage_manager_->Start();

    status = PartitionGet(partition_.get(), key, &get_value);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(get_value, set_value);

    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(get_ttl > 0 && get_ttl < 3000);

    CoSleep(1000 * 1000 * 5);
    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(!status.ok());

    status = PartitionGet(partition_.get(), key, &get_value);
    ASSERT_TRUE(!status.ok());

    set_ttl = 5000;
    set_value = "setex_value";
    status = PartitionSetEx(partition_.get(), key, set_value, set_ttl);
    ASSERT_TRUE(status.ok());

    ReloadPartition();
    partition_->storage_manager_->Start();

    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(get_ttl > 0 && get_ttl < 5000) << get_ttl;

    CoSleep(1000 * 1000 * 6);
    uint64_t slot_id = hash_func(key.c_str(), key.length());
    Index* index = partition_->index_.get();
    Object object;
    status = index->slot_map_[slot_id].FindObject(key, &object);
    ASSERT_TRUE(status.ok());
    ASSERT_LE(get_ttl, GetCurrentTimeInMs());

    FLAGS_expirer_scan_slots_per_round = 5;
    FLAGS_expirer_scan_in_memory_slots_per_round = 100;
    CoSleep(1000 * 1000);
    ASSERT_EQ(index->slot_map_.find(slot_id), index->slot_map_.end());
    status = PartitionGet(partition_.get(), key, &get_value);
    ASSERT_TRUE(status.IsNotFound());

    partition_->storage_manager_->Stop();
    // not expire when apply log
    set_ttl = 1000;
    set_value = "setex_value";
    status = PartitionSetEx(partition_.get(), key, set_value, set_ttl);
    ASSERT_TRUE(status.ok());

    set_value = "set_value_no_expired";
    status = PartitionSet(partition_.get(), key, set_value);
    ASSERT_TRUE(status.ok());

    status = PartitionGet(partition_.get(), key, &get_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_value, set_value);

    CoSleep(1000 * 2000);
    ReloadPartition();
    partition_->storage_manager_->Start();

    CoSleep(1000 * 200);
    status = PartitionGet(partition_.get(), key, &get_value);
    ASSERT_TRUE(status.IsNotFound()) << status;

    partition_->storage_manager_->Stop();
    // not expire when apply log
    set_ttl = 1000;
    set_value = "setex_value";
    status = PartitionSetEx(partition_.get(), key, set_value, set_ttl);
    ASSERT_TRUE(status.ok());

    set_ttl = 10 * 1000;
    status = PartitionExpire(partition_.get(), key, set_ttl);
    ASSERT_TRUE(status.ok());

    ReloadPartition();
    partition_->storage_manager_->Start();
    status = PartitionGet(partition_.get(), key, &get_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_value, set_value);

    // index gc
    status = PartitionDel(partition_.get(), key);
    ASSERT_TRUE(status.ok());

    FLAGS_index_gc_bytes_threshold = 0;
    FLAGS_index_gc_usage_trigger = 1.1;
    partition_->storage_manager_->Stop();
    index = partition_->index_.get();

    for (int i = 0; i < 30; i++) {
        std::string field = "field" + std::to_string(i % 10);
        if (i % 10 == 1) {
            Status status = PartitionDel(partition_.get(), key);
            ASSERT_TRUE(status.ok()) << status.ToString();
        } else {
            std::string value = "value" + std::to_string(random());
            Status status = PartitionHSet(partition_.get(), key, field, value);
            ASSERT_TRUE(status.ok());
            set_ttl = (30 - i) * 1000;
            status = PartitionExpire(partition_.get(), key, set_ttl);
            ASSERT_TRUE(status.ok());
        }

        if (i % 3 == 1) {
            partition_->storage_manager_->ReclaimOpLog();
        }

        if (i % 2 == 0) {
            partition_->storage_manager_->ReclaimIndex();
        }
    }

    CoSleep(1000 * 2000);
    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(status.IsNotFound());

    for (int i = 0; i < 30; i++) {
        std::string field = "field" + std::to_string(i % 10);
        if (i % 10 == 1) {
            Status status = PartitionDel(partition_.get(), key);
            ASSERT_TRUE(status.ok()) << status.ToString();
        } else {
            std::string value = "value" + std::to_string(random());
            Status status = PartitionHSet(partition_.get(), key, field, value);
            ASSERT_TRUE(status.ok());

            status = PartitionTtl(partition_.get(), key, &get_ttl);

            set_ttl = (30 - i) * 1000;
            status = PartitionExpire(partition_.get(), key, set_ttl);
            ASSERT_TRUE(status.ok()) << status.ToString();
        }

        if (i % 3 == 1) {
            partition_->storage_manager_->ReclaimOpLog();
        }

        // todo: enable index gc
        /*
        if (i % 2 == 0) {
            partition_->storage_manager_->ReclaimIndex();
        }
        */
    }

    ReloadPartition();
    FLAGS_index_gc_bytes_threshold = 0;
    FLAGS_index_gc_usage_trigger = 1.1;

    CoSleep(1000 * 2000);
    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(status.IsNotFound());

    for (int i = 0; i < 100; i++) {
        std::string field = "field" + std::to_string(i % 10);
        if (i % 10 == 1) {
            status = PartitionDel(partition_.get(), key);
            ASSERT_TRUE(status.ok()) << status.ToString();
        } else {
            std::string value = "value" + std::to_string(random());
            status = PartitionHSet(partition_.get(), key, field, value);
            ASSERT_TRUE(status.ok());
            set_ttl = (i + 1) * 1000;
            status = PartitionExpire(partition_.get(), key, set_ttl);
            ASSERT_TRUE(status.ok());
        }
    }
    partition_->storage_manager_->ReclaimOpLog();
    partition_->storage_manager_->ReclaimIndex();
    status = PartitionDel(partition_.get(), key);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLog();
    partition_->storage_manager_->ReclaimIndex();

    set_value = "set_value_no_expired";
    status = PartitionSet(partition_.get(), key, set_value);
    ASSERT_TRUE(status.ok());

    status = PartitionTtl(partition_.get(), key, &get_ttl);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_ttl, 0);

    status = PartitionGet(partition_.get(), key, &get_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_value, set_value);
}

TEST_F(PartitionTest, TruncateOpLogWithoutGc) {
    RESTORE_FLAGS(FLAGS_storage_zone_size);
    RESTORE_FLAGS(FLAGS_stream_max_blob_size);
    FLAGS_storage_zone_size = 1U << 18;
    FLAGS_stream_max_blob_size = 1UL << 19;
    int key_count = 0;
    while (partition_->op_logger_->Length() < FLAGS_storage_zone_size * 4) {
        Status status = PartitionSet(partition_.get(), "key" + std::to_string(key_count),
                                     std::string(1024, 'A' + key_count % 26));
        ASSERT_TRUE(status.ok()) << status;
        key_count++;
    }
    ASSERT_GE(key_count, 10);
    while (!partition_->index_->slot_context_manager_->DirtySlotsEmpty()) {
        partition_->storage_manager_->ReclaimOpLog();
    }
    for (int i = 0; i < key_count; ++i) {
        Status status = PartitionSet(partition_.get(), "key" + std::to_string(i),
                                     std::string(1024, 'a' + i % 26));
        ASSERT_TRUE(status.ok()) << status;
    }
    partition_->storage_manager_->ReclaimPage();
    for (int i = 0; i < key_count; ++i) {
        Status status = PartitionSet(partition_.get(), "key" + std::to_string(i),
                                     std::string(1024, 'a' + i % 26));
        ASSERT_TRUE(status.ok()) << status;
    }
    ReloadPartition();

    for (int i = 0; i < key_count; ++i) {
        std::string value;
        Status status = PartitionGet(partition_.get(), "key" + std::to_string(i), &value);
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_EQ(value, std::string(1024, 'a' + i % 26));
    }
}

TEST_F(PartitionTest, Issues51KV) {
    auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 0; };
    SetHashFunc(hash_func);

    FLAGS_index_gc_bytes_threshold = 0;
    FLAGS_index_gc_usage_trigger = 1.1;
    FLAGS_index_gc_max_num_per_round = 1000;

    std::string kv_key = "kv_key";
    std::string kv_value = "kv_value";
    std::string get_kv_value;

    Status status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    kv_value = "kv_value_1";
    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    status = PartitionHSet(partition_.get(), "key_hash", "field", "value1");
    ASSERT_TRUE(status.ok());

    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    // ScanOpLog();
    // ScanIndexLog();
    partition_->storage_manager_->ReclaimIndex();

    // ScanOpLog();
    // ScanIndexLog();

    ReloadPartition();

    std::string hash_value;
    status = PartitionHGet(partition_.get(), "key_hash", "field", &hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, "value1");
}

TEST_F(PartitionTest, Issues51ObjectMeta) {
    auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 0; };
    SetHashFunc(hash_func);

    FLAGS_index_gc_bytes_threshold = 0;
    FLAGS_index_gc_usage_trigger = 1.1;
    FLAGS_index_gc_max_num_per_round = 1000;

    std::string hash_key = "hash_key";
    std::string hash_field = "hash_field";
    std::string hash_value = "hash_value";
    std::string get_hash_value;

    Status status = PartitionHSet(partition_.get(), hash_key, hash_field, hash_value);
    ASSERT_TRUE(status.ok());
    status = PartitionHGet(partition_.get(), hash_key, hash_field, &get_hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, get_hash_value);
    status = PartitionExpire(partition_.get(), hash_key, 100);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    hash_value = "hash_value_2";
    status = PartitionHSet(partition_.get(), hash_key, hash_field, hash_value);
    ASSERT_TRUE(status.ok());
    status = PartitionHGet(partition_.get(), hash_key, hash_field, &get_hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, get_hash_value);
    status = PartitionExpire(partition_.get(), hash_key, 200);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    hash_value = "hash_value_3";
    status = PartitionHSet(partition_.get(), hash_key, hash_field, hash_value);
    ASSERT_TRUE(status.ok());
    status = PartitionHGet(partition_.get(), hash_key, hash_field, &get_hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, get_hash_value);
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    hash_value = "hash_value_4";
    status = PartitionHSet(partition_.get(), hash_key, hash_field, hash_value);
    ASSERT_TRUE(status.ok());

    status = PartitionExpire(partition_.get(), hash_key, 10000);
    ASSERT_TRUE(status.ok());

    status = PartitionHGet(partition_.get(), hash_key, hash_field, &get_hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, get_hash_value);

    // ScanOpLog();
    // ScanIndexLog();

    partition_->storage_manager_->ReclaimIndex();

    // ScanOpLog();
    // ScanIndexLog();

    ReloadPartition();

    status = PartitionHGet(partition_.get(), hash_key, hash_field, &get_hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, get_hash_value);

    uint64_t get_ttl;
    status = PartitionTtl(partition_.get(), hash_key, &get_ttl);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(get_ttl > 8000 && get_ttl < 10000) << get_ttl;
}

TEST_F(PartitionTest, Issues51PageGC) {
    // FLAGS_storage_zone_size = 128;
    // FLAGS_stream_max_blob_size = 1024 * 256;

    // partition_->storage_manager_->Stop();
    // // OpLogger* op_logger = partition_->op_logger_.get();

    // std::string kv_key = "kv_key";
    // std::string kv_value = "kv_value";
    // std::string get_kv_value;

    // Status status = PartitionSet(partition_.get(), kv_key, kv_value);
    // ASSERT_TRUE(status.ok());

    // kv_value = "kv_value_1";
    // status = PartitionSet(partition_.get(), kv_key, kv_value);
    // ASSERT_TRUE(status.ok());

    // kv_value = "kv_value_2";
    // status = PartitionSet(partition_.get(), kv_key, kv_value);
    // ASSERT_TRUE(status.ok());

    // kv_value = "kv_value_3";
    // status = PartitionSet(partition_.get(), kv_key, kv_value);
    // ASSERT_TRUE(status.ok());
    // ReloadPartition();
    // partition_->storage_manager_->Stop();
    // partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    // TODO(tangyunmeng.hinata) make page stream file trunacte
    // for (int i = 0; i < 30; i++) {
    //     status = PartitionHSet(partition_.get(), "key_hash" + std::to_string(i), "field",
    //     "value1");
    //     ASSERT_TRUE(status.ok());
    //     std::string kv_value_run = "kv_value_run_" + std::to_string(i);
    //     status = PartitionSet(partition_.get(), "kv_key2", kv_value_run);
    //     ASSERT_TRUE(status.ok());
    // }

    // partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    // kv_value = "kv_value_4";
    // status = PartitionSet(partition_.get(), kv_key, kv_value);
    // ASSERT_TRUE(status.ok());
    // // ScanOpLog();
    // // ScanIndexLog();

    // partition_->storage_manager_->ReclaimPage();
    // partition_->storage_manager_->ReclaimPage();

    // // ScanOpLog();
    // // ScanIndexLog();
    // for (int i = 0; i < 30; i++) {
    //     ReloadPartition();
    //     partition_->storage_manager_->Stop();
    // }

    // ScanOpLog();
    // ScanIndexLog();

    // status = PartitionGet(partition_.get(), kv_key, &get_kv_value);
    // ASSERT_TRUE(status.ok()) << status.ToString();
    // ASSERT_EQ(get_kv_value, kv_value);
}

// TODO(tangyunmeng.hinata) fix reload objectid confusion
TEST_F(PartitionTest, Issues51ObjectIdReload) {
    RESTORE_FLAGS(FLAGS_storage_zone_size);
    auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 0; };
    SetHashFunc(hash_func);
    FLAGS_storage_zone_size = 128;

    std::string kv_key = "kv_key";
    std::string kv_value = "kv_value";
    std::string get_kv_value;

    Status status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    kv_value = "kv_value_1";
    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    kv_value = "kv_value_2";
    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    kv_value = "kv_value_3";
    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    kv_value = "kv_value_4";
    status = PartitionSet(partition_.get(), "kv_key_2", kv_value);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    kv_value = "kv_value_5";
    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    status = PartitionHSet(partition_.get(), "key_hash", "field", "value1");
    ASSERT_TRUE(status.ok());

    // ScanOpLog();
    // ScanIndexLog();

    partition_->storage_manager_->ReclaimPage();
    partition_->storage_manager_->ReclaimPage();

    // ScanOpLog();
    // ScanIndexLog();

    ReloadPartition();

    std::string get_value;
    status = PartitionGet(partition_.get(), kv_key, &get_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_value, kv_value);

    std::string hash_value;
    status = PartitionHGet(partition_.get(), "key_hash", "field", &hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, "value1");

    for (int i = 0; i < 2; i++) {
        std::string kv_reload_key = "kv_value_reload_" + std::to_string(i);
        std::string kv_reload_value = kv_reload_key;
        status = PartitionSet(partition_.get(), kv_reload_key, kv_reload_value);
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(1);
        partition_->storage_manager_->ReclaimPage();
        partition_->storage_manager_->ReclaimPage();
        // ScanOpLog();
        // ScanIndexLog();
        ReloadPartition();
        status = PartitionGet(partition_.get(), kv_reload_key, &get_value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(get_value, kv_reload_value);

        status = PartitionGet(partition_.get(), kv_key, &get_value);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(get_value, kv_value);
    }

    // ScanOpLog();
    // ScanIndexLog();

    status = PartitionGet(partition_.get(), kv_key, &get_value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(get_value, kv_value);

    status = PartitionHGet(partition_.get(), "key_hash", "field", &hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, "value1");
}

TEST_F(PartitionTest, Issues51ObjectId) {
    auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 0; };
    SetHashFunc(hash_func);

    std::string kv_key = "kv_key";
    std::string kv_value = "kv_value";
    std::string get_kv_value;

    Status status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    status = PartitionHSet(partition_.get(), "key_hash", "field", "value1");
    ASSERT_TRUE(status.ok());

    status = PartitionDel(partition_.get(), "key_hash");
    ASSERT_TRUE(status.ok());

    status = PartitionHSet(partition_.get(), "key_hash", "field", "value2");
    ASSERT_TRUE(status.ok());

    // ScanOpLog();
    // ScanIndexLog();

    ReloadPartition();

    // ScanOpLog();
    // ScanIndexLog();

    std::string hash_value;
    status = PartitionHGet(partition_.get(), "key_hash", "field", &hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, "value2");

    status = PartitionGet(partition_.get(), kv_key, &get_kv_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_kv_value, kv_value);

    status = PartitionHGet(partition_.get(), "key_hash", "field", &hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, "value2");

    ReloadPartition();

    // ScanOpLog();
    // ScanIndexLog();

    status = PartitionGet(partition_.get(), kv_key, &get_kv_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_kv_value, kv_value);

    status = PartitionHGet(partition_.get(), "key_hash", "field", &hash_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(hash_value, "value2");
}

TEST_F(PartitionTest, Issues126DeleteOplog) {
    Status status = PartitionHSet(partition_.get(), "key", "field", "value");
    ASSERT_TRUE(status.ok());

    partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    status = partition_->page_store_->PrepareNewZone(true);
    ASSERT_TRUE(status.ok());

    PartitionDel(partition_.get(), "key");
    ASSERT_TRUE(status.ok());

    auto calc_zone_num = [this]() {
        return std::accumulate(partition_->page_store_->zones_.begin(),
                               partition_->page_store_->zones_.end(), 0,
                               [](int pre, const std::unique_ptr<PageStore::ZoneOpenInfo>& zone) {
                                   if (zone != nullptr) {
                                       return pre + 1;
                                   }
                                   return pre;
                               });
    };

    int zone_num = calc_zone_num();
    for (int i = 0; i < 10; ++i) {
        partition_->page_gc_->Gc();
    }
    ASSERT_LE(calc_zone_num(), zone_num);

    ReloadPartition();

    std::string value;
    status = PartitionGet(partition_.get(), "key", &value);
    ASSERT_TRUE(status.IsNotFound());
}

TEST_F(PartitionTest, Issues51TtlRewrite) {
    auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 0; };
    SetHashFunc(hash_func);

    FLAGS_index_gc_bytes_threshold = 0;
    FLAGS_index_gc_usage_trigger = 1.1;
    FLAGS_index_gc_max_num_per_round = 1000;

    std::string kv_key = "kv_key";
    std::string kv_value = "kv_value";
    std::string get_kv_value;

    Status status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    status = PartitionExpire(partition_.get(), kv_key, 10000);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    kv_value = "kv_value_1";
    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);

    kv_value = "kv_value_2";
    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    // TODO(tangyunmeng.hinata) make index stream file truncate

    // ScanOpLog();
    // ScanIndexLog();

    // for (int i = 0; i < 10; i++) {
    //     // std::cout << "RunCase: " << i << std::endl;
    //     kv_value = "kv_value_run_" + std::to_string(i);
    //     status = PartitionSet(partition_.get(), kv_key, kv_value);
    //     ASSERT_TRUE(status.ok());
    //     partition_->storage_manager_->ReclaimIndex();
    //     partition_->storage_manager_->ReclaimOpLogWithLimit(1);
    //     ReloadPartition();
    //     partition_->storage_manager_->Stop();
    //     // ScanOpLog();
    //     // ScanIndexLog();
    // }

    partition_->storage_manager_->ReclaimIndex();
    partition_->storage_manager_->ReclaimOpLogWithLimit(1);
    ReloadPartition();
    // ScanOpLog();
    // ScanIndexLog();

    status = PartitionGet(partition_.get(), kv_key, &get_kv_value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(get_kv_value, kv_value);

    uint64_t get_ttl;
    status = PartitionTtl(partition_.get(), kv_key, &get_ttl);
    ASSERT_TRUE(get_ttl > 9000 && get_ttl < 10000) << get_ttl;
}

TEST_F(PartitionTest, SetAfterTtl) {
    std::string kv_key = "kv_key";
    std::string kv_value = "kv_value";
    std::string get_kv_value;

    Status status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    status = PartitionSetEx(partition_.get(), kv_key, kv_value, 100000);
    ASSERT_TRUE(status.ok());

    status = PartitionSet(partition_.get(), kv_key, kv_value);
    ASSERT_TRUE(status.ok());

    ReloadPartition();

    uint64_t ttl_ms = 0;
    status = PartitionTtl(partition_.get(), kv_key, &ttl_ms);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(ttl_ms, 0);
}

TEST_F(PartitionTest, Issue102) {
    partition_->storage_manager_->Stop();

    Status status = PartitionSet(partition_.get(), "test_key", "test_value");
    ASSERT_TRUE(status.ok()) << status;
    status = PartitionExpire(partition_.get(), "test_key", 5000);
    ASSERT_TRUE(status.ok()) << status;
    partition_->storage_manager_->ReclaimOpLog();

    status = PartitionDel(partition_.get(), "test_key");
    ASSERT_TRUE(status.ok()) << status;
    partition_->storage_manager_->ReclaimOpLog();

    ReloadPartition();
    sleep(5);

    status = PartitionSet(partition_.get(), "test_key", "test_value");
    ASSERT_TRUE(status.ok()) << status;
    std::string value;
    status = PartitionGet(partition_.get(), "test_key", &value);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(value, "test_value");
}

TEST_F(PartitionTest, LimiterTest) {
    RESTORE_FLAGS(FLAGS_storage_async);
    FLAGS_storage_async = true;

    partition_->Unload();
    partition_.reset();
    // reload
    Partition::Options options;
    options.env = env_.get();
    options.uri = uri_;
    options.load_version = ++load_version_;
    int write_limit_burst = 5, read_limiter_burst = 3;
    options.config.mutable_limit_config()->mutable_write_limiter()->mutable_qps()->set_value(
        write_limit_burst);
    options.config.mutable_limit_config()->mutable_write_limiter()->mutable_burst()->set_value(
        write_limit_burst);
    options.config.mutable_limit_config()->mutable_read_limiter()->mutable_qps()->set_value(
        read_limiter_burst);
    options.config.mutable_limit_config()->mutable_read_limiter()->mutable_burst()->set_value(
        read_limiter_burst);
    partition_.reset(new Partition(options));
    Status status = partition_->Load();
    ASSERT_TRUE(status.ok());

    uint64_t read_succ = 0;
    uint64_t start = GetCurrentTimeInUs();
    while (true) {
        if (GetCurrentTimeInUs() - start > 10 * 1000 * 1000) {
            break;
        }
        std::string test_key("test_key"), test_val("test_val");
        status = PartitionGet(partition_.get(), test_key, &test_val);
        if (status.IsNotFound() || status.ok()) {
            ++read_succ;
        }
    }
    // almost 30 success
    ASSERT_TRUE(read_succ > 20 && read_succ < 40) << read_succ;

    uint64_t write_succ = 0;
    start = GetCurrentTimeInUs();
    while (true) {
        if (GetCurrentTimeInUs() - start > 10 * 1000 * 1000) {
            break;
        }
        std::string test_key("test_key"), test_val("test_val");
        status = PartitionSet(partition_.get(), test_key, test_val);
        if (status.IsNotFound() || status.ok()) {
            ++write_succ;
        }
    }
    // almost 50 success
    ASSERT_TRUE(write_succ > 40 && write_succ < 60) << write_succ;
}

TEST_F(PartitionTest, ExecuteCheck) {
    ASSERT_TRUE(partition_->ExecuteCheck().ok());
    partition_->replicator_->status_ = Status::Aborted("");
    ASSERT_FALSE(partition_->ExecuteCheck().ok());
}

TEST_F(PartitionTest, Issue129) {
    Status status = PartitionHSet(partition_.get(), "test_key", "test_field", "test_value");
    ASSERT_TRUE(status.ok());

    FLAGS_store_fiu_hang_interval_ms = 1000;
    ASSERT_EQ(fiu_enable("store/file/io/hang/append", 1, nullptr, 0), 0);
    BYTE_DEFER(ASSERT_EQ(fiu_disable("store/file/io/hang/append"), 0););

    uint64_t slot_id = hash_func("test_key", sizeof("test_key") - 1);
    CoSyncClosure sync;
    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        while (partition_->index_->slot_map_[slot_id].Dirty()) {
            // not dump yet
            CoSleep(1);
        }
        Status status = PartitionDel(partition_.get(), "test_key");
        ASSERT_TRUE(status.ok());
        sync.Run();
    }));

    // reclaim oplog, dump slot pages and Invoke PartitionDel above
    partition_->storage_manager_->ReclaimOpLog();

    // reclaim oplog, dump all slots
    sync.Wait();
    partition_->storage_manager_->ReclaimOpLog();

    ASSERT_EQ(partition_->index_->slot_map_.size(), 0);
}

TEST_F(PartitionTest, Issue129DelThenAdd) {
    Status status = PartitionHSet(partition_.get(), "test_key", "f1", "v1");
    ASSERT_TRUE(status.ok());

    FLAGS_store_fiu_hang_interval_ms = 1000;
    ASSERT_EQ(fiu_enable("store/file/io/hang/append", 1, nullptr, 0), 0);
    BYTE_DEFER(ASSERT_EQ(fiu_disable("store/file/io/hang/append"), 0););

    uint64_t slot_id = hash_func("test_key", sizeof("test_key") - 1);
    CoSyncClosure sync;
    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        while (partition_->index_->slot_map_[slot_id].Dirty()) {
            // not dump yet
            CoSleep(1);
        }
        Status status = PartitionDel(partition_.get(), "test_key");
        ASSERT_TRUE(status.ok());
        status = PartitionHSet(partition_.get(), "test_key", "f2", "v200");
        ASSERT_TRUE(status.ok());
        status = PartitionDel(partition_.get(), "test_key");
        ASSERT_TRUE(status.ok());
        status = PartitionHSet(partition_.get(), "test_key", "f1", "v100");
        ASSERT_TRUE(status.ok());
        sync.Run();
    }));

    // reclaim oplog, dump slot pages
    partition_->storage_manager_->ReclaimOpLog();
    // invoke the function above
    sync.Wait();
    // dump again
    partition_->storage_manager_->ReclaimOpLog();
    std::string val;
    bool exist = false;
    status = PartitionHGetWithExist(partition_.get(), "test_key", "f2", &val, &exist);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(exist);
    ASSERT_TRUE(PartitionHGet(partition_.get(), "test_key", "f1", &val).ok());
    ASSERT_EQ("v100", val);
    ASSERT_EQ(partition_->index_->slot_map_.size(), 1);
}

TEST_F(PartitionTest, Issue129StringModel) {
    Status status = PartitionSet(partition_.get(), "key", "value-1");
    ASSERT_TRUE(status.ok());

    FLAGS_store_fiu_hang_interval_ms = 1000;
    ASSERT_EQ(fiu_enable("store/file/io/hang/append", 1, nullptr, 0), 0);
    BYTE_DEFER(ASSERT_EQ(fiu_disable("store/file/io/hang/append"), 0););

    uint64_t slot_id = hash_func("key", sizeof("key") - 1);
    CoSyncClosure sync;
    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        while (partition_->index_->slot_map_[slot_id].Dirty()) {
            // not dump yet
            CoSleep(1);
        }
        Status status = PartitionDel(partition_.get(), "key");
        ASSERT_TRUE(status.ok());
        sync.Run();
    }));

    // reclaim oplog, dump slot pages
    partition_->storage_manager_->ReclaimOpLog();
    // invoke the function above
    sync.Wait();
    // dump again
    partition_->storage_manager_->ReclaimOpLog();
    std::string val;
    ASSERT_TRUE(PartitionGet(partition_.get(), "key", &val).IsNotFound());
    ASSERT_EQ(0, partition_->index_->slot_map_.size());
}

TEST_F(PartitionTest, Issue135) {
    std::string value_base = std::string(1024, 'a');
    // stop auto loop
    Evicter* evicter = partition_->evicter_.get();
    Status status = PartitionSet(partition_.get(), "key1", value_base);

    evicter->config_.mutable_maxmemory()->set_value(
        evicter->allocator_manager_->GetTotalAllocedSize());
    evicter->config_.mutable_policy_type()->set_value(PolicyType::LRU);
    evicter->config_.mutable_operation_type()->set_value(OperationType::DUMP);
    status = PartitionSet(partition_.get(), "key2", value_base);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = evicter->TryEvict();                              // dump key1
    PartitionSet(partition_.get(), "key1", value_base);        // load key1
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);  // reclaim key1
}

TEST_F(PartitionTest, UpdateConfig) {
    {
        // update maxmemory
        Config config;
        config.mutable_evicter_config()->mutable_maxmemory()->set_value(102922);
        Status status = PartitionSetConfig(partition_.get(), config);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(partition_->evicter_->config_.maxmemory().value(), 102922);
    }

    {
        // set rep_policy and ignore maxmemory
        Config config;
        config.mutable_stream_config()->mutable_store_rep_policy()->set_value(
            StoreRepPolicy::REP_G2);

        uint64_t old_maxmemory = partition_->evicter_->config_.maxmemory().value();
        Status status = PartitionSetConfig(partition_.get(), config);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(partition_->evicter_->config_.maxmemory().value(), old_maxmemory);
        ASSERT_EQ(static_cast<stream::StreamImpl*>(partition_->index_->stream_.get())
                      ->rep_policy_.value(),
                  StoreRepPolicy::REP_G2);
        ASSERT_EQ(static_cast<stream::StreamImpl*>(partition_->op_logger_->stream_.get())
                      ->rep_policy_.value(),
                  StoreRepPolicy::REP_G2);
        ASSERT_EQ(partition_->page_store_->rep_policy_.value(), StoreRepPolicy::REP_G2);
        for (auto& zone : partition_->page_store_->zones_) {
            if (zone) {
                ASSERT_EQ(static_cast<stream::StreamImpl*>(zone->stream.get())->rep_policy_.value(),
                          StoreRepPolicy::REP_G2);
            }
        }
    }

    {
        // set maxmemory and ignore rep_policy
        Config config;
        config.mutable_evicter_config()->mutable_maxmemory()->set_value(128381233);

        Status status = PartitionSetConfig(partition_.get(), config);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(partition_->evicter_->config_.maxmemory().value(), 128381233);
        ASSERT_EQ(static_cast<stream::StreamImpl*>(partition_->index_->stream_.get())
                      ->rep_policy_.value(),
                  StoreRepPolicy::REP_G2);
        ASSERT_EQ(static_cast<stream::StreamImpl*>(partition_->op_logger_->stream_.get())
                      ->rep_policy_.value(),
                  StoreRepPolicy::REP_G2);
        ASSERT_EQ(partition_->page_store_->rep_policy_.value(), StoreRepPolicy::REP_G2);
        for (auto& zone : partition_->page_store_->zones_) {
            if (zone) {
                ASSERT_EQ(static_cast<stream::StreamImpl*>(zone->stream.get())->rep_policy_.value(),
                          StoreRepPolicy::REP_G2);
            }
        }
    }
}

TEST_F(PartitionTest, Issue136Deadcycle) {
    FLAGS_index_gc_usage_trigger = 1.1;
    FLAGS_index_gc_max_num_per_round = 10;
    FLAGS_index_gc_bytes_threshold = 0;

    for (int i = 0; i < 1000; ++i) {
        partition_->index_->OnMetaUpdate();
    }
    Controller ctrl;
    SYNC_CALL(partition_->index_->Commit, &ctrl);
    ASSERT_GT(partition_->index_->stream_->Stat().length, 1000);

    uint64_t before_start_record_id = partition_->index_->stream_->Stat().start_record_id;
    for (int i = 0; i < 100; ++i) {
        bool dirty_slot = false;
        uint64_t truncate_id = 0;
        partition_->index_->TryGc(&dirty_slot, &truncate_id);
        ASSERT_FALSE(dirty_slot);
        if (truncate_id != 0) {
            partition_->index_->Truncate(truncate_id);
        }
    }
    ASSERT_GT(partition_->index_->stream_->Stat().start_record_id, before_start_record_id);
}

TEST_F(PartitionTest, Issue136LostTTL) {
    FLAGS_storage_async = true;
    FLAGS_index_gc_usage_trigger = 10;
    FLAGS_index_gc_max_num_per_round = UINT64_MAX;
    FLAGS_index_gc_bytes_threshold = 0;

    auto hash_func = [](const char* data, uint64_t len) -> uint64_t { return 9015; };
    SetHashFunc(hash_func);
    BYTE_DEFER(SetHashFunc(CallHash));

    // 2 objects on same slot
    Status status = PartitionSetEx(partition_.get(), "test_key1", "test_value", 100000000);
    ASSERT_TRUE(status.ok());
    status = PartitionSetEx(partition_.get(), "test_key2", "test_value", 100000000);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(1000);
    Controller ctrl;
    SYNC_CALL(partition_->index_->Commit, &ctrl);

    // make some useless index log
    for (int i = 0; i < 100; ++i) {
        partition_->index_->OnMetaUpdate();
    }

    ctrl.Reset();
    SYNC_CALL(partition_->index_->Commit, &ctrl);

    // make test_key1 meta dirty
    FLAGS_store_fiu_hang_interval_ms = 60000;
    ASSERT_EQ(fiu_enable("oplog/store/file/io/hang/append", 1, nullptr, 0), 0);
    status = PartitionExpire(partition_.get(), "test_key1", 100000000);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(fiu_disable("oplog/store/file/io/hang/append"), 0);

    partition_->storage_manager_->ReclaimIndex();

    // make some useless index log to trigger stream switch block
    for (int i = 0; i < 20000; ++i) {
        partition_->index_->OnMetaUpdate();
    }
    SYNC_CALL(partition_->index_->Commit, &ctrl);

    partition_->op_logger_ = nullptr;
    ReloadPartition();

    uint64_t ttl = 0;
    status = PartitionTtl(partition_.get(), "test_key1", &ttl);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_GT(ttl, 10000000);

    status = PartitionTtl(partition_.get(), "test_key2", &ttl);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_GT(ttl, 10000000);
}

TEST_F(PartitionTest, EvicterKeepCachedEntrySize) {
    FLAGS_evict_batch_size = 100;
    FLAGS_evict_count_limit = 1000;
    FLAGS_evicter_max_memory_usage = 1;  // 1MB

    for (int i = 0; i < 1000; ++i) {
        Status status =
            PartitionSet(partition_.get(), "test_key" + std::to_string(i), std::string(10240, 'a'));
    }

    for (auto& iter : partition_->index_->slot_map_) {
        iter.second.SetLastUsed(SlotNode::WriteOptions(nullptr), 12345);
    }
    partition_->evicter_->TryEvict();

    ASSERT_LE(
        static_cast<Evicter::PolicyLru*>(partition_->evicter_->policy_.get())->cached_entry_.size(),
        partition_->evicter_->config_.pool_size().value());
}

// This UT expect to force GC to rewrite blocks to the page store
// Then evict all the pages. Get and reload all pages lead to page load from page store.
// When Blockcache enabled, all there should be blockcache hit from second load.
TEST_F(PartitionTest, BlockcacheWithPartitionTest) {
    FLAGS_storage_gc_space_utility_threshold = 1;
    FLAGS_evict_count_limit = 10000;
    FLAGS_evict_batch_size = 500;

    std::string value_base = std::string(10, 'a');

    // stop auto loop
    StorageManager* storage = partition_->storage_manager_.get();

    Evicter* evicter = partition_->evicter_.get();

    std::string value1 = "value";
    for (size_t i = 0; i < 1000; i++) {
        std::stringstream si;
        si << std::setw(100) << std::setfill('0') << i;
        // Insert keys to the partition with hash set model
        Status status =
            PartitionSet(partition_.get(), "key" + std::to_string(i), value_base + si.str());
        ASSERT_TRUE(status.ok());
    }
    storage->ReclaimOpLog();

    Status status;
    // force GC, op log entries should be written to pagestores
    storage->ReclaimOpLog();
    // storage->ReclaimMemory();

    evicter->config_.mutable_maxmemory()->set_value(1);
    evicter->config_.mutable_policy_type()->set_value(PolicyType::LRU);
    evicter->config_.mutable_operation_type()->set_value(OperationType::DUMP);
    // status = evicter->TryEvict();

    // three zones should be created, only two can be GCed
    storage->page_gc_->PickNextZone();
    storage->page_gc_->GcCurrentZone();
    storage->page_gc_->PickNextZone();
    storage->page_gc_->GcCurrentZone();
    // storage->ReclaimPage();
    storage->ReclaimIndex();

    Controller ctrl;
    SYNC_CALL(storage->index_->Commit, &ctrl);

    // Read all the pages
    CoSleep(1000 * 1000 * 2);
    for (size_t i = 0; i < 1000; i++) {
        std::stringstream si;
        std::string value;
        Status status = PartitionGet(partition_.get(), "key" + std::to_string(i), &value);
        ASSERT_TRUE(status.ok()) << status;
    }

    LOG_DEBUG("before eviction")
        .put("mem size:", evicter->allocator_manager_->GetTotalAllocedSize());

    status = evicter->TryEvict();

    ASSERT_TRUE(status.ok()) << status;
    LOG_DEBUG("after eviction")
        .put("mem size:", evicter->allocator_manager_->GetTotalAllocedSize());

    for (size_t i = 0; i < 1000; i++) {
        std::stringstream si;
        std::string value;
        Status status = PartitionGet(partition_.get(), "key" + std::to_string(i), &value);
        ASSERT_TRUE(status.ok()) << status;
    }

    LOG_DEBUG("before eviction")
        .put("mem size:", evicter->allocator_manager_->GetTotalAllocedSize());

    status = evicter->TryEvict();

    ASSERT_TRUE(status.ok()) << status;
    LOG_DEBUG("after eviction")
        .put("mem size:", evicter->allocator_manager_->GetTotalAllocedSize());

    for (size_t i = 0; i < 1000; i++) {
        std::stringstream si;
        std::string value;
        Status status = PartitionGet(partition_.get(), "key" + std::to_string(i), &value);
        ASSERT_TRUE(status.ok()) << status;
    }
}

TEST_F(PartitionTest, UpdateStalePageLog) {
    uint64_t slot_id = hash_func("key", sizeof("key") - 1);

    Status status = PartitionSet(partition_.get(), "key", "value");
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(partition_->index_->slot_map_.count(slot_id), 1);
    ASSERT_EQ(partition_->index_->slot_map_[slot_id].GetObjectNum(), 1);

    status = PartitionDel(partition_.get(), "key");
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(partition_->index_->slot_map_.count(slot_id), 1);
    ASSERT_EQ(partition_->index_->slot_map_[slot_id].GetObjectNum(), 0);

    partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    ASSERT_EQ(partition_->index_->slot_map_.count(slot_id), 0);
}

TEST_F(PartitionTest, Issue146) {
    uint64_t slot_id = hash_func("key", sizeof("key") - 1);

    Status status = PartitionHSet(partition_.get(), "key", "field", std::string(500 * 1024, 'a'));
    ASSERT_TRUE(status.ok()) << status;
    status = PartitionExpire(partition_.get(), "key", 10000000);
    ASSERT_TRUE(status.ok()) << status;

    RESTORE_FLAGS(FLAGS_stream_max_blob_size);
    RESTORE_FLAGS(FLAGS_page_store_compress_trigger_threshold);
    FLAGS_page_store_compress_trigger_threshold = 100000000000UL;  // do not compress
    FLAGS_stream_max_blob_size = 200 * 1024;
    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        while (partition_->index_->slot_map_[slot_id].Dirty()) {
            // not dump yet
            CoSleep(1);
        }
        FLAGS_stream_max_blob_size = 1024 * 1024 * 1024;
        Status status = PartitionExpire(partition_.get(), "key", 3000000);
        ASSERT_TRUE(status.ok()) << status;
    }));

    FLAGS_store_fiu_hang_interval_ms = 3000;
    ASSERT_EQ(fiu_enable("store/file/ioctl/fail/open/hang", 1, nullptr, 0), 0);
    BYTE_DEFER(ASSERT_EQ(fiu_disable("store/file/ioctl/fail/open/hang"), 0););

    ASSERT_TRUE(partition_->index_->slot_map_[slot_id].Dirty());
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    ASSERT_TRUE(partition_->index_->slot_map_[slot_id].Dirty());
}

TEST_F(PartitionTest, MultiPage) {
    std::unordered_map<std::string, std::string> origin_data;

    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    Status status = PartitionHSet(partition_.get(), key, "big_field", std::string(200 * 1024, 'a'));
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 1);
    origin_data["big_field"] = std::string(200 * 1024, 'a');

    status = PartitionHSet(partition_.get(), key, "small_field", "value");
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 2);

    // more pages
    for (int i = 0; i < 10; ++i) {
        status = PartitionHSet(partition_.get(), key, "small_field" + std::to_string(i),
                               "value" + std::to_string(i));
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        origin_data["small_field" + std::to_string(i)] = "value" + std::to_string(i);
    }
    ASSERT_GT(partition_->index_->GetSlot(slot_id)->GetPageNum(), 10);

    // compact all small pages to one
    partition_->storage_manager_->CompactPages();
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 2);

    // more pages
    for (int i = 100; i < 110; ++i) {
        origin_data["small_field" + std::to_string(i)] = "value" + std::to_string(i);
        status = PartitionHSet(partition_.get(), key, "small_field" + std::to_string(i),
                               "value" + std::to_string(i));
        ASSERT_TRUE(status.ok());
        if (i < 105) {
            partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        }
    }
    ASSERT_GT(partition_->index_->GetSlot(slot_id)->GetPageNum(), 5);

    // reload partition
    ReloadPartition();

    // check data correctness
    for (auto& origin_field_value : origin_data) {
        std::string value;
        Status status = PartitionHGet(partition_.get(), key, origin_field_value.first, &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, origin_field_value.second);
    }
}

TEST_F(PartitionTest, ObjectDeletedDuringPageRewrite) {
    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    Status status = PartitionHSet(partition_.get(), key, "test_field", "test_value");
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(10000);

    auto storage_gc_space_utility_threshold = FLAGS_storage_gc_space_utility_threshold;
    FLAGS_storage_gc_space_utility_threshold = 100;
    BYTE_DEFER(FLAGS_storage_gc_space_utility_threshold = storage_gc_space_utility_threshold);
    partition_->storage_manager_->PrepareNewZone(true);
    ASSERT_TRUE(partition_->storage_manager_->page_gc_->PickNextZone());
    ASSERT_EQ(partition_->storage_manager_->page_gc_->current_gc_zone_.zone_id, 1);

    // register task, delete object
    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        status = PartitionDel(partition_.get(), key);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 1);
        ASSERT_TRUE(
            partition_->index_->GetSlot(slot_id)->GetPages().front().dirty);  // mark page deleted
        ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPages().front().page_size,
                  0);  // mark page deleted
    }));

    // trigger dump and update pages
    FLAGS_store_fiu_hang_interval_ms = 1000;
    ASSERT_EQ(fiu_enable("store/file/io/hang/append", 1, nullptr, 0), 0);
    BYTE_DEFER(ASSERT_EQ(fiu_disable("store/file/io/hang/append"), 0););
    ASSERT_TRUE(partition_->storage_manager_->page_gc_->GcCurrentZone());

    // check update pages failed
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetObjectNum(), 0);
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 1);
    ASSERT_TRUE(partition_->index_->GetSlot(slot_id)->GetPages().front().dirty);
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPages().front().page_size, 0);
}

TEST_F(PartitionTest, DISABLED_ObjectDeletedDuringPageLogCommit) {
    RESTORE_FLAGS(FLAGS_storage_async);
    FLAGS_storage_async = true;

    FLAGS_partition_commit_oplog = false;
    BYTE_DEFER(FLAGS_partition_commit_oplog = true);

    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    // create object
    Status status = PartitionSet(partition_.get(), key, "test_value");
    ASSERT_TRUE(status.ok());

    // delete object in same oplog
    status = PartitionDel(partition_.get(), key);
    ASSERT_TRUE(status.ok());

    // dump slot and insert dirty page
    partition_->storage_manager_->ReclaimOpLogWithLimit(10000);

    // check page insert failed due to object is deleted
    if (partition_->index_->GetSlot(slot_id) == nullptr) {
        return;
    }
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetObjectNum(), 0);
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 0);
}

TEST_F(PartitionTest, ObjectRecreateDuringPageLogCommit) {
    RESTORE_FLAGS(FLAGS_storage_async);
    FLAGS_storage_async = true;

    FLAGS_partition_commit_oplog = false;
    BYTE_DEFER(FLAGS_partition_commit_oplog = true);

    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    // create object
    Status status = PartitionSet(partition_.get(), key, "test_value");
    ASSERT_TRUE(status.ok());

    // delete object in same oplog
    status = PartitionDel(partition_.get(), key);
    ASSERT_TRUE(status.ok());

    // create same object
    status = PartitionSet(partition_.get(), key, "test_value2");
    ASSERT_TRUE(status.ok());

    // dump slot and insert dirty page
    partition_->storage_manager_->ReclaimOpLogWithLimit(10000);

    // check the first page insert failed due to object is deleted
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetObjectNum(), 1);
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 1);
}

TEST_F(PartitionTest, PageDirtyDuringPageCompaction) {
    RESTORE_FLAGS(FLAGS_model_size_tiered_compaction_min_bucket_size);
    RESTORE_FLAGS(FLAGS_model_max_space_amplification);
    FLAGS_model_size_tiered_compaction_min_bucket_size = 1;
    FLAGS_model_max_space_amplification = 100;

    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    for (int i = 0; i < 10; ++i) {
        Status status =
            PartitionHSet(partition_.get(), key, "test_field" + std::to_string(i), "test_value");
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(10000);
        status =
            PartitionHSet(partition_.get(), key, "test_field" + std::to_string(i), "test_value");
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(10000);
        status =
            PartitionHSet(partition_.get(), key, "test_field" + std::to_string(i), "test_value");
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(10000);
        status = PartitionHSet(partition_.get(), key, "test_field" + std::to_string(i),
                               std::string(10240, 'a'));
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(10000);
    }

    // register task, mark first page dirty
    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        // some reason make page dirty
        auto page = partition_->index_->GetSlot(slot_id)->GetPages().front();
        page.dirty = true;
        ASSERT_TRUE(
            partition_->index_->GetSlot(slot_id)
                ->UpdatePage(SlotNode::WriteOptions(nullptr), page.object_id, page.page_id, page)
                .ok());
    }));

    std::vector<PageIndex> origin_pages = partition_->index_->GetSlot(slot_id)->GetPages();
    ASSERT_GT(origin_pages.size(), 1);
    // compact pages failed due to page is dirty
    partition_->storage_manager_->CompactPages();
    std::vector<PageIndex> current_pages = partition_->index_->GetSlot(slot_id)->GetPages();

    ASSERT_EQ(origin_pages.size(), current_pages.size());
    for (size_t i = 0; i < current_pages.size(); ++i) {
        if (current_pages[i].dirty) {
            // sential
            continue;
        }

        ASSERT_EQ(origin_pages[i].object_id, current_pages[i].object_id);
        ASSERT_EQ(origin_pages[i].model_id, current_pages[i].model_id);
        ASSERT_EQ(origin_pages[i].page_id, current_pages[i].page_id);
        ASSERT_EQ(origin_pages[i].page_in_log, current_pages[i].page_in_log);
        ASSERT_EQ(origin_pages[i].page_size, current_pages[i].page_size);
        ASSERT_EQ(origin_pages[i].address, current_pages[i].address);
    }
}

TEST_F(PartitionTest, MultiPageAndMultiObject) {
    RESTORE_FLAGS(FLAGS_model_size_tiered_compaction_min_bucket_size);
    RESTORE_FLAGS(FLAGS_model_max_space_amplification);
    FLAGS_model_size_tiered_compaction_min_bucket_size = 1;
    FLAGS_model_max_space_amplification = 100;

    auto reload_hash_func = [](const char* data, uint64_t len) -> uint64_t { return 1048; };
    SetHashFunc(reload_hash_func);

    uint64_t slot_id = 1048;

    for (int i = 0; i < 20; ++i) {
        Status status =
            PartitionHSet(partition_.get(), "key1", "key1_small_field" + std::to_string(i),
                          "key1_value" + std::to_string(i));
        ASSERT_TRUE(status.ok());

        status = PartitionHSet(partition_.get(), "key2", "key2_small_field" + std::to_string(i),
                               "key2_value" + std::to_string(i));
        ASSERT_TRUE(status.ok());

        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    }
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 40);

    // compact pages
    FLAGS_model_size_tiered_compaction_min_bucket_size = 10000000;
    FLAGS_model_size_tiered_compaction_max_ignore_bucket_size = 100000000;
    partition_->storage_manager_->CompactPages();
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 2);  // 2 pages for 2 object

    // check value
    uint64_t len = 0;
    Status status = PartitionHlen(partition_.get(), "key1", &len);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(len, 20);
    status = PartitionHlen(partition_.get(), "key2", &len);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(len, 20);
    for (int i = 0; i < 20; ++i) {
        std::string value;
        bool exist = false;
        Status status = PartitionHGetWithExist(
            partition_.get(), "key1", "key1_small_field" + std::to_string(i), &value, &exist);
        ASSERT_TRUE(exist);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "key1_value" + std::to_string(i));
        status = PartitionHGetWithExist(partition_.get(), "key2",
                                        "key2_small_field" + std::to_string(i), &value, &exist);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(exist);
        ASSERT_EQ(value, "key2_value" + std::to_string(i));
    }

    // reload and check value
    ReloadPartition();
    len = 0;
    status = PartitionHlen(partition_.get(), "key1", &len);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(len, 20);
    status = PartitionHlen(partition_.get(), "key2", &len);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(len, 20);
    for (int i = 0; i < 20; ++i) {
        std::string value;
        bool exist = false;
        Status status = PartitionHGetWithExist(
            partition_.get(), "key1", "key1_small_field" + std::to_string(i), &value, &exist);
        ASSERT_TRUE(exist);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "key1_value" + std::to_string(i));
        status = PartitionHGetWithExist(partition_.get(), "key2",
                                        "key2_small_field" + std::to_string(i), &value, &exist);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(exist);
        ASSERT_EQ(value, "key2_value" + std::to_string(i));
    }
}

TEST_F(PartitionTest, IgnoreEmptyPageAfterCompaction) {
    RESTORE_FLAGS(FLAGS_model_size_tiered_compaction_min_bucket_size);
    RESTORE_FLAGS(FLAGS_model_max_space_amplification);
    RESTORE_FLAGS(FLAGS_model_deny_full_dump);
    FLAGS_model_size_tiered_compaction_min_bucket_size = 1;
    FLAGS_model_max_space_amplification = 100;
    FLAGS_model_deny_full_dump = true;

    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    // small pages
    for (int i = 0; i < 10; ++i) {
        Status status = PartitionHSet(partition_.get(), key, "small_field" + std::to_string(i),
                                      "value" + std::to_string(i));
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    }
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 10);

    // now, delete all fields
    for (int i = 0; i < 10; ++i) {
        Status status = PartitionHDel(partition_.get(), key, "small_field" + std::to_string(i));
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    }
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 20);

    // compact all pages to empty
    FLAGS_model_size_tiered_compaction_min_bucket_size = 100000;
    partition_->storage_manager_->CompactPages();
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 0);

    // reload partition
    ReloadPartition();
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 0);
}

TEST_F(PartitionTest, SwitchModelWithMultiPage) {
    RESTORE_FLAGS(FLAGS_model_size_tiered_compaction_min_bucket_size);
    RESTORE_FLAGS(FLAGS_model_max_space_amplification);
    FLAGS_model_size_tiered_compaction_min_bucket_size = 1;
    FLAGS_model_max_space_amplification = 100;

    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    // small pages
    for (int i = 0; i < 10; ++i) {
        Status status = PartitionHSet(partition_.get(), key, "small_field" + std::to_string(i),
                                      "value" + std::to_string(i));
        ASSERT_TRUE(status.ok());
        if (i < 5) {
            partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        }
    }
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 5);

    // delete object and create new model
    Status status = PartitionDel(partition_.get(), key);
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    for (int i = 0; i < 10; ++i) {
        status = PartitionSet(partition_.get(), key, "value" + std::to_string(i));
        ASSERT_TRUE(status.ok());
    }
    std::string value;
    status = PartitionGet(partition_.get(), key, &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value9");
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 1);

    // reload partition
    ReloadPartition();
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 1);
    status = PartitionGet(partition_.get(), key, &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value9");
}

TEST_F(PartitionTest, CompactPagesWithSlotNotInMemory) {
    RESTORE_FLAGS(FLAGS_model_deny_full_dump);
    FLAGS_model_deny_full_dump = true;

    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    for (int i = 0; i < 100; ++i) {
        Status status = PartitionHSet(partition_.get(), key, "field" + std::to_string(i), "value");
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
        ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), i + 1);
    }

    // reload partition
    ReloadPartition();
    ASSERT_FALSE(partition_->index_->GetSlot(slot_id)->InMemory());

    // compact all small pages to one
    partition_->storage_manager_->CompactPages();
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 1);
}

TEST_F(PartitionTest, GcEmptyPageZone) {
    // set
    for (int i = 0; i < 10; ++i) {
        Status status =
            PartitionHSet(partition_.get(), "key" + std::to_string(i), "field", "value");
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    }

    // delete
    for (int i = 0; i < 10; ++i) {
        Status status = PartitionDel(partition_.get(), "key" + std::to_string(i));
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    }

    // new zone
    partition_->page_store_->PrepareNewZone(true);

    // pick zone
    ASSERT_TRUE(partition_->page_gc_->PickNextZone());
    ASSERT_EQ(partition_->page_gc_->current_gc_zone_.used_bytes, 0);
    ASSERT_NE(partition_->page_gc_->current_gc_zone_.total_bytes, 0);
    ASSERT_FALSE(partition_->page_gc_->current_gc_zone_.page_log);
    auto zone_id = partition_->page_gc_->current_gc_zone_.zone_id;

    // gc zone
    RESTORE_FLAGS(FLAGS_storage_gc_max_slots_per_round);
    FLAGS_storage_gc_max_slots_per_round = 1;
    partition_->page_gc_->Gc();
    storage::IndexLog::ZoneInfo zone_info;
    ASSERT_TRUE(partition_->index_->GetZoneInfo(zone_id, &zone_info));
    ASSERT_EQ(zone_info.state(), storage::IndexLog_ZoneState_RECYCLED);
}

TEST_F(PartitionTest, GcEmptyOplogger) {
    RESTORE_FLAGS(FLAGS_storage_zone_size);
    FLAGS_storage_zone_size = 10;

    // set
    for (int i = 0; i < 10; ++i) {
        Status status = PartitionSet(partition_.get(), "key" + std::to_string(i), "value");
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    }

    // delete
    for (int i = 0; i < 10; ++i) {
        Status status = PartitionDel(partition_.get(), "key" + std::to_string(i));
        ASSERT_TRUE(status.ok());
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    }

    // pick zone
    ASSERT_TRUE(partition_->page_gc_->PickNextZone());
    ASSERT_EQ(partition_->page_gc_->current_gc_zone_.used_bytes, 0);
    ASSERT_NE(partition_->page_gc_->current_gc_zone_.total_bytes, 0);
    ASSERT_TRUE(partition_->page_gc_->current_gc_zone_.page_log);

    // gc zone
    RESTORE_FLAGS(FLAGS_storage_gc_max_slots_per_round);
    FLAGS_storage_gc_max_slots_per_round = 1;
    uint64_t old = partition_->page_gc_->recycled_oplog_length_;
    partition_->page_gc_->Gc();
    uint64_t new_len = partition_->page_gc_->recycled_oplog_length_;
    ASSERT_NE(old, new_len);
}

TEST_F(PartitionTest, DoNotLoadDirtyPage) {
    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    FLAGS_store_fiu_hang_interval_ms = 10000;
    ASSERT_EQ(fiu_enable("store/file/io/hang/append", 1, nullptr, 0), 0);
    BYTE_DEFER(ASSERT_EQ(fiu_disable("store/file/io/hang/append"), 0););

    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        ASSERT_TRUE(partition_->index_->GetSlot(slot_id)->InMemory());
        partition_->evicter_->config_.mutable_maxmemory()->set_value(10 * 1024);
        partition_->evicter_->TryEvict();
        ASSERT_FALSE(partition_->index_->GetSlot(slot_id)->InMemory());

        std::string value;
        Status status = PartitionGet(partition_.get(), key, &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, std::string(20 * 1024, 'a'));
    }));

    Status status = PartitionSet(partition_.get(), key, std::string(20 * 1024, 'a'));
    ASSERT_TRUE(status.ok());
}

TEST_F(PartitionTest, DumpDeltaLogAfterPartitionReload) {
    RESTORE_FLAGS(FLAGS_model_size_tiered_compaction_min_bucket_size);
    FLAGS_model_size_tiered_compaction_min_bucket_size = 10;

    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    // 100 fields in one page
    for (int i = 0; i < 100; ++i) {
        Status status = PartitionHSet(partition_.get(), key, "field_" + std::to_string(i), "value");
        ASSERT_TRUE(status.ok());
    }
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 1);

    Status status = PartitionHSet(partition_.get(), key, "new_field1", "value1");
    ASSERT_TRUE(status.ok());

    ReloadPartition();

    status = PartitionHSet(partition_.get(), key, "new_field2", "value2");
    ASSERT_TRUE(status.ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    ASSERT_EQ(partition_->index_->GetSlot(slot_id)->GetPageNum(), 2);

    ReloadPartition();

    std::string value;
    status = PartitionHGet(partition_.get(), key, "new_field1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value1");

    status = PartitionHGet(partition_.get(), key, "new_field2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value2");
}

TEST_F(PartitionTest, MarkPageDirtyAfterSlotReload) {
    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    Status status = PartitionSet(partition_.get(), key, std::string(20 * 1024, 'a'));
    ASSERT_TRUE(status.ok());

    status = PartitionDel(partition_.get(), key);
    ASSERT_TRUE(status.ok());

    status = PartitionSet(partition_.get(), key, std::string(20 * 1024, 'b'));
    ASSERT_TRUE(status.ok());

    ASSERT_TRUE(partition_->index_->GetSlot(slot_id)->InMemory());
    partition_->evicter_->config_.mutable_maxmemory()->set_value(10 * 1024);
    partition_->evicter_->TryEvict();
    ASSERT_FALSE(partition_->index_->GetSlot(slot_id)->InMemory());

    partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    partition_->evicter_->config_.mutable_maxmemory()->set_value(10 * 1024);
    partition_->evicter_->TryEvict();
    ASSERT_FALSE(partition_->index_->GetSlot(slot_id)->InMemory());

    std::string value;
    status = PartitionGet(partition_.get(), key, &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, std::string(20 * 1024, 'b'));
}

TEST_F(PartitionTest, HashModelABA) {
    std::string key = "key";
    uint64_t slot_id = hash_func(key.data(), key.size());

    Status status = PartitionHSet(partition_.get(), key, "field1", std::string(10 * 1024, '1'));
    ASSERT_TRUE(status.ok());

    status = PartitionHSet(partition_.get(), key, "field2", std::string(10 * 1024, '2'));
    ASSERT_TRUE(status.ok());

    ASSERT_TRUE(partition_->index_->GetSlot(slot_id)->InMemory());
    partition_->evicter_->config_.mutable_maxmemory()->set_value(1);
    partition_->evicter_->TryEvict();
    ASSERT_FALSE(partition_->index_->GetSlot(slot_id)->InMemory());

    std::string value;
    bool exist = false;
    status = PartitionHGetWithExist(partition_.get(), key, "field2", &value, &exist);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(exist);
    ASSERT_EQ(value, std::string(10 * 1024, '2'));

    FLAGS_store_fiu_hang_interval_ms = 10000;
    ASSERT_EQ(fiu_enable("store/file/io/hang/append", 1, nullptr, 0), 0);
    BYTE_DEFER(ASSERT_EQ(fiu_disable("store/file/io/hang/append"), 0););

    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        while (partition_->index_->slot_map_[slot_id].Dirty()) {
            // not dump yet
            CoSleep(1);
        }
        FLAGS_store_fiu_hang_interval_ms = 1;
        Status status = PartitionDel(partition_.get(), key);
        ASSERT_TRUE(status.ok());
        status = PartitionHSet(partition_.get(), key, "field3", std::string(10 * 1024, '3'));
        ASSERT_TRUE(status.ok()) << status;
        std::cout << "del and hset\n";
    }));
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    CoSleep(1000);

    partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    std::cout << "check value\n";
    status = PartitionHGetWithExist(partition_.get(), key, "field1", &value, &exist);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(exist);
    status = PartitionHGetWithExist(partition_.get(), key, "field3", &value, &exist);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(exist);
    ASSERT_EQ(value, std::string(10 * 1024, '3'));

    ASSERT_TRUE(partition_->index_->GetSlot(slot_id)->InMemory());
    partition_->evicter_->config_.mutable_maxmemory()->set_value(1);
    partition_->evicter_->TryEvict();
    ASSERT_FALSE(partition_->index_->GetSlot(slot_id)->InMemory());

    std::cout << "reload slot and check value\n";
    status = PartitionHGetWithExist(partition_.get(), key, "field1", &value, &exist);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(exist);
    status = PartitionHGetWithExist(partition_.get(), key, "field3", &value, &exist);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(exist);
    ASSERT_EQ(value, std::string(10 * 1024, '3'));
}

TEST_F(PartitionTest, Issue149) {
    RESTORE_FLAGS(FLAGS_storage_async);
    RESTORE_FLAGS(FLAGS_partition_commit_oplog);
    FLAGS_partition_commit_oplog = false;
    FLAGS_storage_async = true;

    Status status = PartitionSet(partition_.get(), "key1", "value");
    ASSERT_TRUE(status.ok());

    status = PartitionSet(partition_.get(), "key2", "value");
    ASSERT_TRUE(status.ok());

    Controller ctrl;
    SYNC_CALL(partition_->op_logger_->Commit, &ctrl);
    ASSERT_TRUE(ctrl.status().ok());
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    status = PartitionSet(partition_.get(), "key1", "value2");
    ASSERT_TRUE(status.ok());

    status = PartitionDel(partition_.get(), "key2");
    ASSERT_TRUE(status.ok());

    ctrl.Reset();
    SYNC_CALL(partition_->op_logger_->Commit, &ctrl);
    ASSERT_TRUE(ctrl.status().ok());

    ReloadPartition();

    std::string value;
    status = PartitionGet(partition_.get(), "key1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value2");

    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.IsNotFound());

    partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    status = PartitionGet(partition_.get(), "key1", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value2");

    status = PartitionGet(partition_.get(), "key2", &value);
    ASSERT_TRUE(status.IsNotFound());
}

TEST_F(PartitionTest, DumpDuringSlotLoading) {
    std::string key = "key000000";
    uint64_t slot_id = hash_func(key.c_str(), key.size());

    Status status = PartitionHSet(partition_.get(), key, "field1", "value1");
    ASSERT_TRUE(status.ok());

    // dump and gen new page
    partition_->storage_manager_->ReclaimOpLogWithLimit(100);

    status = PartitionHSet(partition_.get(), key, "field2", "value2");
    ASSERT_TRUE(status.ok());

    // evict slot
    ASSERT_TRUE(partition_->index_->GetSlot(slot_id)->InMemory());
    partition_->evicter_->config_.mutable_maxmemory()->set_value(1);
    partition_->evicter_->TryEvict();
    ASSERT_FALSE(partition_->index_->GetSlot(slot_id)->InMemory());

    FLAGS_store_fiu_hang_interval_ms = 5000;
    ASSERT_EQ(fiu_enable("store/file/io/hang/read", 1, nullptr, 0), 0);
    BYTE_DEFER(ASSERT_EQ(fiu_disable("store/file/io/hang/read"), 0););

    byte::InvokeInCurrentThread(NewCoFuncClosure([&]() {
        // try blind dump
        partition_->storage_manager_->ReclaimOpLogWithLimit(100);
    }));

    std::string value;
    status = PartitionHGet(partition_.get(), key, "field2", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "value2");
}

TEST_F(PartitionTest, PartitionConfigTest) {
    Config c;
    CustomConfig custom_config;
    auto custom_config_map = c.mutable_module_custom_config();
    custom_config.mutable_ips_config()->set_value(default_test_table_conf2);
    (*custom_config_map)[15] = custom_config;
    ReloadPartitionWithOption(c);
    auto modules = CmdManager::GetModuleInfos();
    for (size_t i = 0; i < partition_->cmd_executor_->module_configs_.size(); i++) {
        switch (i) {
        case Module::COMMON:
        case Module::HASH:
        case Module::SET:
        case Module::STRING:
        case Module::RISK:
            ASSERT_EQ(partition_->cmd_executor_->module_configs_[i].get(), nullptr);
            break;
        case Module::IPS:
        case Module::FEATURE:
            ASSERT_FALSE(partition_->cmd_executor_->module_configs_[i].get() == nullptr);
            break;
        default:
            break;
        }
    }
}

}  // namespace partition
}  // namespace bcache2
