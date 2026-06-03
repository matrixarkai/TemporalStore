// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/log_based_stream.h"

#include <byte/algorithm/crc32.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <random>

#include "stream/log_based_env.h"
#include "test/common/temp_dir.h"

DECLARE_uint64(stream_max_blob_size);

DECLARE_bool(stream_aggregate_flush);
DECLARE_uint64(stream_aggregate_flush_loop_interval_ms);
DECLARE_uint64(stream_aggregate_flush_batch_size_byte);

namespace bcache2 {
namespace stream {

class MockedBlob : public Blob {
 public:
    MOCK_METHOD0(Close, void());
    MOCK_METHOD4(Append, void(Controller*, const void*, size_t, Closure<void>*));
    MOCK_METHOD5(Read, void(Controller*, size_t, void*, size_t, Closure<void>*));
};

using ::testing::_;
using ::testing::Return;

static thread_local std::random_device rd;

class LogBasedStreamTest : public testing::Test {
 public:
    void SetUp() override {
        byte::AsyncThreadPoolOptions tp_options;
        tp_options.name_ = "test";
        background_pool_.reset(new byte::AsyncThreadPool());
        ASSERT_TRUE(background_pool_->Init(tp_options));
        ASSERT_TRUE(background_pool_->Start());

        store_layer_.reset(new StoreLayer(background_pool_.get()));

        env_.reset(new LogBasedEnv());
        LogBasedEnv::Options env_options;
        env_options.background_pool = background_pool_.get();
        env_options.store_layer = store_layer_.get();
        env_->Init(env_options);

        metrics_manager_.reset(new MetricsManager({}, "partition"));

        uri_ = "file://" + temp_dir_.GetDir() + "/public/stream/";
        stream_.reset(new StreamImpl(env_.get(), uri_, Env::Condition(), StoreRepPolicy(), "test",
                                     metrics_manager_.get()));
        Status status = stream_->Load();
        ASSERT_TRUE(status.ok()) << status;
    }
    void TearDown() override { SYNC_CALL0(stream_->Close); }

 protected:
    void WriteRecord(char* buffer, size_t size, char data) {
        size_t header_size = RecordHeaderLength(size);
        memset(buffer + header_size, data, size);
        uint32_t crc32c = byte::CRCUtil::ComputeCRC32(0, buffer + header_size, size);
        uint32_t consumed_size = 0;
        ASSERT_TRUE(WriteRecordHeader(size, crc32c, buffer, header_size, &consumed_size));
        ASSERT_EQ(consumed_size, header_size);
    }

    TempDir temp_dir_;
    std::unique_ptr<byte::AsyncThreadPool> background_pool_;
    std::unique_ptr<StoreLayer> store_layer_;
    std::unique_ptr<LogBasedEnv> env_;
    std::string uri_;
    std::unique_ptr<StreamImpl> stream_;
    std::unique_ptr<MetricsManager> metrics_manager_;
};

// TODO(zhangyuan.42): Add more cases
TEST_F(LogBasedStreamTest, TailScanWithBlockFooter) {
    MockedBlob blob;
    char buffer[kBlockSize + 1024];
    EXPECT_CALL(blob, Read)
        .WillRepeatedly([&buffer](Controller* ctrl, size_t offset, void* data, size_t size,
                                  Closure<void>* callback) {
            memcpy(data, buffer + offset, size);
            ctrl->set_status(Status::OK());
            callback->Run();
        });

    storage::BlobHeader blob_header;
    blob_header.set_start_record_sequence(100);
    blob_header.set_start_offset(10 * kBlockSize + kBlockSize - kBlockFooterSize);
    blob_header.set_prev_block_crc32c(1111);
    blob_header.set_truncated_offset(222);

    storage::BlockFooter footer;
    footer.set_magic(kMagic);
    footer.set_version(kVersion);
    footer.set_block_end(kBlockSize - kBlockFooterSize);
    footer.set_last_record_offset(0);
    footer.set_last_record_sequence(100);
    footer.set_truncated_offset(222);
    footer.set_block_number(10);
    ASSERT_TRUE(SerializeBlockFooter(footer, buffer, kBlockSize));
    char* p = buffer + kBlockSize;
    WriteRecord(p, 10, 'A');         // 1 + 4 + 10
    WriteRecord(p + 15, 100, 'B');   // 1 + 4 + 100
    WriteRecord(p + 120, 200, 'C');  // 2 + 4 + 200
    WriteRecord(p + 326, 692, 'D');  // 2 + 4 + 698

    StreamImpl::BlobTailInfo tail_info;
    Status status = stream_->TailScan(&blob, blob_header, sizeof(buffer), &tail_info);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(104, tail_info.end_record_sequence);
    EXPECT_EQ(11 * kBlockSize + 1024, tail_info.end_offset);
    EXPECT_EQ(kBlockSize + 1024, tail_info.blob_end_offset);
    uint32_t crc = byte::CRCUtil::ComputeCRC32(0, buffer + kBlockSize, 1024);
    EXPECT_EQ(crc, tail_info.last_block_crc32c);
    EXPECT_EQ(222, tail_info.truncated_offset);
}

TEST_F(LogBasedStreamTest, TailScanWithoutBlockFooter) {
    MockedBlob blob;
    char buffer[kBlockSize - kBlockFooterSize];
    EXPECT_CALL(blob, Read)
        .WillRepeatedly([&buffer](Controller* ctrl, size_t offset, void* data, size_t size,
                                  Closure<void>* callback) {
            memcpy(data, buffer + offset, size);
            ctrl->set_status(Status::OK());
            callback->Run();
        });

    storage::BlobHeader blob_header;
    blob_header.set_start_record_sequence(100);
    blob_header.set_start_offset(10 * kBlockSize + kBlockSize - kBlockFooterSize - 1024);
    blob_header.set_prev_block_crc32c(1111);
    blob_header.set_truncated_offset(222);

    char* p = buffer + kBlockSize - kBlockFooterSize - 1024;
    WriteRecord(p, 10, 'A');         // 1 + 4 + 10
    WriteRecord(p + 15, 100, 'B');   // 1 + 4 + 100
    WriteRecord(p + 120, 200, 'C');  // 2 + 4 + 200
    WriteRecord(p + 326, 692, 'D');  // 2 + 4 + 698

    StreamImpl::BlobTailInfo tail_info;
    Status status = stream_->TailScan(&blob, blob_header, sizeof(buffer), &tail_info);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(104, tail_info.end_record_sequence);
    EXPECT_EQ(11 * kBlockSize - kBlockFooterSize, tail_info.end_offset);
    EXPECT_EQ(kBlockSize - kBlockFooterSize, tail_info.blob_end_offset);
    uint32_t crc =
        byte::CRCUtil::ComputeCRC32(1111, buffer + kBlockSize - kBlockFooterSize - 1024, 1024);
    EXPECT_EQ(crc, tail_info.last_block_crc32c);
    EXPECT_EQ(222, tail_info.truncated_offset);
}

TEST_F(LogBasedStreamTest, LastBlobInfo) {
    auto old_blob_size = FLAGS_stream_max_blob_size;
    FLAGS_stream_max_blob_size = kBlockSize + 256 * 1024;
    BYTE_DEFER({ FLAGS_stream_max_blob_size = old_blob_size; });

    // cold start
    StreamBaseImpl* stream_base = stream_->stream_base_.get();
    ASSERT_EQ(stream_base->blob_header_.data_blobs_size(), 0);
    ASSERT_EQ(stream_base->readable_blobs_.size(), 1);

    // append data and switch a new blob
    uint64_t id = 0;
    stream_->Append(std::string(FLAGS_stream_max_blob_size / 2, 'a'), &id);
    stream_->Append(std::string(FLAGS_stream_max_blob_size / 2, 'a'), &id);
    Controller ctrl;
    SYNC_CALL(stream_->Commit, &ctrl);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
    ASSERT_EQ(stream_base->blob_header_.data_blobs_size(), 1);
    ASSERT_EQ(stream_base->readable_blobs_.size(), 2);

    // continue append data and check the last blob info
    std::queue<uint64_t> ids;
    std::uniform_int_distribution<size_t> pick(1, FLAGS_stream_max_blob_size / 10);
    for (size_t i = 0; i < 10000; ++i) {
        uint64_t id = 0;
        stream_->Append(std::string(pick(rd), 'a'), &id);
        ids.push(id);

        if (pick(rd) % 10 == 0) {
            Controller ctrl;
            SYNC_CALL(stream_->Commit, &ctrl);
            ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
            const auto& last_blob_info = stream_base->LastBlobInfo();
            ASSERT_EQ(last_blob_info.blob_id(), stream_base->blob_header_.blob_id());
            ASSERT_EQ(last_blob_info.start_offset(), stream_base->blob_header_.start_offset());
            ASSERT_NE(last_blob_info.blob_start_offset(), 0);
            ASSERT_EQ(last_blob_info.blob_end_offset(), stream_->persistent_blob_offset_);
            ASSERT_EQ(last_blob_info.end_record_sequence(), stream_->persistent_sequence_);
            ASSERT_EQ(last_blob_info.end_offset(), stream_->persistent_offset_);
            ASSERT_EQ(last_blob_info.truncated_offset(), stream_->persistent_truncated_offset_);
        }

        if (pick(rd) % 10 == 0) {
            // pick an id to truncate
            while (!ids.empty() && pick(rd) % 3 == 0) {
                ids.pop();
            }
            if (!ids.empty()) {
                stream_->Truncate(ids.front());
            }
        }
    }
}

TEST_F(LogBasedStreamTest, AggregateFlush) {
    uint64_t start = GetCurrentTimeInMs();
    std::cout << "start " << start << std::endl;
    FLAGS_stream_aggregate_flush = true;
    stream_.reset(new StreamImpl(env_.get(), uri_, Env::Condition(), StoreRepPolicy(), "test",
                                 metrics_manager_.get()));
    Status status = stream_->Load();
    ASSERT_TRUE(status.ok()) << status;

    // case1: append 1/2 batch_size data, need wait loop flush
    // time cost should less than FLAGS_stream_aggregate_flush_loop_interval_ms
    std::cout << "test case1 " << std::endl;
    uint64_t id = 0;
    uint64_t start1 = GetCurrentTimeInMs();
    stream_->Append(std::string(FLAGS_stream_aggregate_flush_batch_size_byte / 2, 'a'), &id);
    Controller ctrl;
    SYNC_CALL(stream_->Commit, &ctrl);
    uint64_t cost1 = GetCurrentTimeInMs() - start1;
    std::cout << "start1 " << start1 << " cost1 " << cost1 << std::endl;
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
    ASSERT_TRUE(cost1 > FLAGS_stream_aggregate_flush_loop_interval_ms - (start1 - start));

    // case2: append batch_size data, do not wait loop flush
    std::cout << "test case2 " << std::endl;
    uint64_t start2 = GetCurrentTimeInMs();
    stream_->Append(std::string(FLAGS_stream_aggregate_flush_batch_size_byte, 'a'), &id);
    Controller ctrl1;
    SYNC_CALL(stream_->Commit, &ctrl1);
    uint64_t cost2 = GetCurrentTimeInMs() - start2;
    std::cout << "start2 " << start2 << " cost2 " << cost2 << std::endl;
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
    ASSERT_TRUE(cost2 < FLAGS_stream_aggregate_flush_loop_interval_ms - (start2 - start1));

    FLAGS_stream_aggregate_flush = false;
}
}  // namespace stream
}  // namespace bcache2
