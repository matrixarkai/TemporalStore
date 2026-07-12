// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <random>

#include "common/function_closure.h"
#include "common/sync_closure.h"
#include "stream/log_based_env.h"
#include "stream/log_based_stream.h"
#include "stream/log_based_stream_base.h"
#include "stream/log_based_stream_reader.h"
#include "test/common/temp_dir.h"

DEFINE_string(schema, "file://", "blob schema");
DECLARE_uint64(stream_max_blob_size);
DECLARE_uint64(stream_blob_deletion_min_age);
DECLARE_uint64(stream_blob_deletion_min_gap);

namespace bcache2 {
namespace stream {

static thread_local std::random_device rd;
static thread_local std::mt19937 rng(rd());

std::string random_string(size_t length) {
    static auto& chrs = "0123456789abcdefghijklmnopqrstuvwxyz";

    thread_local static std::uniform_int_distribution<size_t> pick(0, sizeof(chrs) - 2);

    std::string s;

    s.reserve(length);

    while (length--) {
        s += chrs[pick(rd)];
    }

    return s;
}

class StreamTest : public testing::Test {
 public:
    void SetUp() override {
        matrixobjectstore_set_flag("matrixobjectstore_client_log_level", "1");
        matrixobjectstore_init();

        metrics_manager_.reset(new MetricsManager({}, ""));

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

        uri_ = FLAGS_schema + temp_dir_.GetDir() + "/cluster/public/stream1/";
        Stream* stream = nullptr;
        Open(uri_, &stream);
        stream_.reset(stream);
        OpenReader(uri_, &stream);
        stream_reader_.reset(stream);
    }

    void TearDown() override {
        Close(stream_.get());
        Close(stream_reader_.get());

        matrixobjectstore_shutdown();
    }

 protected:
    void Open(const std::string& uri, Stream** stream) {
        open_count_++;
        std::string token = "token" + std::to_string(open_count_);
        std::string condition_str = "cond" + std::to_string(open_count_);
        LogBasedEnv::Condition condition;
        condition.name = FLAGS_schema + temp_dir_.GetDir() + "/cluster/public/stream1_lock";
        condition.data.fill('A' + open_count_ % 26);

        Controller ctrl;
        env_->SetCondition(&ctrl, LogBasedEnv::Condition(), condition.name, condition.data);
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();

        ctrl.Reset();
        LogBasedEnv::OpenOptions options;
        options.token = token;
        options.metrics_manager = metrics_manager_.get();
        env_->OpenStream(&ctrl, condition, uri, options, stream);
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
        ASSERT_TRUE(*stream != nullptr);
    }

    void OpenReader(const std::string& uri, Stream** stream) {
        Controller ctrl;
        LogBasedEnv::OpenOptions options;
        options.readonly = true;
        options.metrics_manager = metrics_manager_.get();
        env_->OpenStream(&ctrl, LogBasedEnv::Condition(), uri, options, stream);
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
        ASSERT_TRUE(*stream != nullptr);
    }

    void Close(Stream* stream) {
        CoSyncClosure sync;
        stream->Close(&sync);
        sync.Wait();
    }

    void Append(Stream* stream, const std::vector<std::string>& data, uint64_t* id) {
        stream->AppendV(data, id);
    }

    void Append(Stream* stream, const std::string& data, uint64_t* id) { stream->Append(data, id); }

    void Commit(Stream* stream) {
        Controller ctrl;
        CoSyncClosure sync;
        stream->Commit(&ctrl, &sync);
        sync.Wait();
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    }

    void Read(Stream* stream, uint64_t id, char* data, size_t size) {
        Controller ctrl;
        CoSyncClosure sync;
        stream->Read(&ctrl, id, data, size, &sync);
        sync.Wait();
        BYTE_ASSERT(ctrl.status().ok()) << ctrl.status().ToString();
    }

    void ExpectedRead(Stream* stream, uint64_t id, const std::string& data) {
        std::string tmp;
        tmp.resize(data.size());
        Read(stream, id, &tmp[0], tmp.size());
        EXPECT_TRUE(tmp == data);
    }

    TempDir temp_dir_;
    std::unique_ptr<byte::AsyncThreadPool> background_pool_;
    std::unique_ptr<StoreLayer> store_layer_;
    std::unique_ptr<LogBasedEnv> env_;
    std::string uri_;
    std::unique_ptr<Stream> stream_;
    std::unique_ptr<Stream> stream_reader_;
    std::unique_ptr<MetricsManager> metrics_manager_;
    LogBasedEnv::Condition condition_;
    uint64_t open_count_ = 0;
};

TEST_F(StreamTest, Simple) {
    uint64_t id = 0;
    Append(stream_.get(), "Data1", &id);
    Commit(stream_.get());
    ExpectedRead(stream_.get(), id, "Data1");
}

TEST_F(StreamTest, DataError) {
    uint64_t id = 0;
    std::string base(20, 'A');
    Append(stream_.get(), base, &id);
    Commit(stream_.get());
    Controller ctrl;
    CoSyncClosure sync;

    // simulate length mismatch
    char buf[10];
    stream_->Read(&ctrl, id, buf, 10, &sync);
    sync.Wait();
    BYTE_ASSERT(ctrl.status().IsDataLoss());

    // simulate data error
    std::string file_name;
    file_name = uri_.substr(FLAGS_schema.size()).append("DAT-0000000001");
    auto blob_info = stream_->GetInfo().blob_infos(0);
    auto offset = id - blob_info.start_offset() + blob_info.blob_start_offset() +
        RecordHeaderLength(base.size());
    int fd = open(file_name.c_str(), O_RDWR | O_SYNC, 0644);
    BYTE_ASSERT(fd >= 0);
    BYTE_ASSERT(pwrite(fd, "BB", 2, offset) == 2);
    BYTE_ASSERT(close(fd) == 0);

    char buffer[20];
    ctrl.Reset();

    CoSyncClosure another_sync;
    stream_->Read(&ctrl, id, buffer, base.size(), &another_sync);
    another_sync.Wait();
    BYTE_ASSERT(ctrl.status().IsDataLoss());
}

TEST_F(StreamTest, RandomSize) {
    std::map<uint64_t, std::string> datas;
    std::uniform_int_distribution<size_t> range(1, 1024 * 1024);
    for (int i = 0; i < 100; i++) {
        size_t l = range(rng);
        std::string s = random_string(l);
        uint64_t id = 0;
        Append(stream_.get(), s, &id);
        datas[id] = s;
    }
    Commit(stream_.get());

    for (auto& p : datas) {
        ExpectedRead(stream_.get(), p.first, p.second);
    }

    ScopedIterator iter = stream_->NewIterator(0UL, UINT64_MAX);

    std::map<uint64_t, std::string> tmp_datas = datas;
    while (true) {
        Status status = iter->Next();
        if (!status.ok()) {
            break;
        }
        EXPECT_EQ(tmp_datas[iter->Id()], std::string(iter->Data().data(), iter->Data().size()));
        tmp_datas.erase(iter->Id());
    }
    ASSERT_TRUE(tmp_datas.empty());

    Close(stream_.get());
    stream_.reset();

    Stream* stream = nullptr;
    Open(uri_, &stream);
    stream_.reset(stream);

    for (auto& p : datas) {
        ExpectedRead(stream_.get(), p.first, p.second);
    }

    iter = stream_->NewIterator(0UL, UINT64_MAX);

    while (true) {
        Status status = iter->Next();
        if (!status.ok()) {
            break;
        }
        EXPECT_EQ(datas[iter->Id()], std::string(iter->Data().data(), iter->Data().size()));
        datas.erase(iter->Id());
    }
    ASSERT_TRUE(datas.empty());
}

TEST_F(StreamTest, CrossBlock) {
    std::string data;  // 500k large value
    for (int i = 0; i < 100 * 1024; i++) {
        data.append("hello");
    }
    uint64_t id1 = 0;
    Append(stream_.get(), data, &id1);

    uint64_t id2 = 0;
    Append(stream_.get(), data, &id2);

    uint64_t id3 = 0;
    Append(stream_.get(), data, &id3);

    Commit(stream_.get());

    ExpectedRead(stream_.get(), id1, data);
    ExpectedRead(stream_.get(), id2, data);
    ExpectedRead(stream_.get(), id3, data);

    ScopedIterator iter = stream_->NewIterator(0UL, UINT64_MAX);

    Status status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id1, iter->Id());
    EXPECT_EQ(data, std::string(iter->Data().data(), iter->Data().size()));

    status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id2, iter->Id());
    EXPECT_EQ(data, std::string(iter->Data().data(), iter->Data().size()));

    status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id3, iter->Id());
    EXPECT_EQ(data, std::string(iter->Data().data(), iter->Data().size()));

    Close(stream_.get());
    stream_.reset();

    Stream* stream = nullptr;
    Open(uri_, &stream);
    stream_.reset(stream);

    ExpectedRead(stream_.get(), id1, data);
    ExpectedRead(stream_.get(), id2, data);
    ExpectedRead(stream_.get(), id3, data);

    iter = stream_->NewIterator(0UL, UINT64_MAX);

    status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id1, iter->Id());
    EXPECT_EQ(data, std::string(iter->Data().data(), iter->Data().size()));

    status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id2, iter->Id());
    EXPECT_EQ(data, std::string(iter->Data().data(), iter->Data().size()));

    status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id3, iter->Id());
    EXPECT_EQ(data, std::string(iter->Data().data(), iter->Data().size()));
}

TEST_F(StreamTest, WriteRead) {
    uint64_t id1 = 0;
    Append(stream_.get(), "data1", &id1);

    uint64_t id2 = 0;
    Append(stream_.get(), "data2", &id2);

    uint64_t id3 = 0;
    Append(stream_.get(), "data3", &id3);

    Commit(stream_.get());

    ExpectedRead(stream_.get(), id1, "data1");
    ExpectedRead(stream_.get(), id2, "data2");
    ExpectedRead(stream_.get(), id3, "data3");
}

TEST_F(StreamTest, Iterator) {
    uint64_t id1 = 0;
    Append(stream_.get(), "data1", &id1);

    uint64_t id2 = 0;
    Append(stream_.get(), "data2", &id2);

    uint64_t id3 = 0;
    Append(stream_.get(), "data3", &id3);

    Commit(stream_.get());

    ScopedIterator iter = stream_->NewIterator(0UL, UINT64_MAX);

    Status status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id1, iter->Id());
    EXPECT_STREQ("data1", std::string(iter->Data().data(), iter->Data().size()).c_str());

    status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id2, iter->Id());
    EXPECT_STREQ("data2", std::string(iter->Data().data(), iter->Data().size()).c_str());

    status = iter->Next();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(id3, iter->Id());
    EXPECT_STREQ("data3", std::string(iter->Data().data(), iter->Data().size()).c_str());

    status = iter->Next();
    ASSERT_TRUE(status.IsOutOfRange()) << status.ToString();
}

TEST_F(StreamTest, IteratorWithAppend) {
    ScopedIterator iter = stream_->NewIterator(0UL, UINT64_MAX);
    uint64_t id1 = 0;
    Append(stream_.get(), "data1", &id1);
    Commit(stream_.get());
    Status status = iter->Next();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(id1, iter->Id());
    EXPECT_STREQ("data1", std::string(iter->Data().data(), iter->Data().size()).c_str());

    uint64_t id2 = 0;
    Append(stream_.get(), "data2", &id2);
    Commit(stream_.get());
    status = iter->Next();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(id2, iter->Id());
    EXPECT_STREQ("data2", std::string(iter->Data().data(), iter->Data().size()).c_str());

    uint64_t id3 = 0;
    Append(stream_.get(), "data3", &id3);
    Commit(stream_.get());

    uint64_t id4 = 0;
    Append(stream_.get(), "data4", &id4);
    status = iter->Next();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(id3, iter->Id());
    EXPECT_STREQ("data3", std::string(iter->Data().data(), iter->Data().size()).c_str());

    Commit(stream_.get());
    status = iter->Next();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(id4, iter->Id());
    EXPECT_STREQ("data4", std::string(iter->Data().data(), iter->Data().size()).c_str());
}

TEST_F(StreamTest, RandomWriteRead) {
    std::random_device rd;
    std::vector<std::pair<uint64_t, std::string>> datas;
    for (size_t i = 0; i < 10000; ++i) {
        if (rd() % 2 == 0) {  // Append
            std::string data;
            size_t size = 8 + rd() % 8192;
            for (size_t i = 0; i < size; ++i) {
                data.push_back(rd() % 26 + 'A');
            }
            uint64_t id = 0;
            Append(stream_.get(), data, &id);
            ASSERT_TRUE(datas.empty() || id > datas.back().first);
            datas.push_back(std::make_pair(id, data));
        } else if (!datas.empty()) {  // Read
            Commit(stream_.get());

            size_t index = rd() % datas.size();
            ExpectedRead(stream_.get(), datas[index].first, datas[index].second);
        }
    }

    ScopedIterator iter = stream_->NewIterator(0UL, UINT64_MAX);
    Status status;
    auto it = datas.begin();
    while ((status = iter->Next()).ok()) {
        ASSERT_EQ(it->first, iter->Id());
        ASSERT_TRUE(iter->Data() == it->second);
        ++it;
    }
    ASSERT_TRUE(status.IsOutOfRange()) << status.ToString();
}

TEST_F(StreamTest, RandomIterator) {
    std::random_device rd;
    std::vector<std::pair<uint64_t, std::string>> datas;
    for (size_t i = 0; i < 10000; ++i) {
        std::string data;
        size_t size = 8 + rd() % 8192;
        for (size_t i = 0; i < size; ++i) {
            data.push_back(rd() % 26 + 'A');
        }
        uint64_t id = 0;
        Append(stream_.get(), data, &id);
        ASSERT_TRUE(datas.empty() || id > datas.back().first);
        datas.push_back(std::make_pair(id, data));
    }
    Controller ctrl;
    SYNC_CALL(stream_->Commit, &ctrl);

    for (size_t i = 0; i < 100; ++i) {
        size_t start = rd() % (datas.back().first + kBlockSize);
        ScopedIterator iter = stream_->NewIterator(start, UINT64_MAX);
        Status status;
        auto it = std::lower_bound(datas.begin(), datas.end(),
                                   std::pair<uint64_t, std::string>(start, ""));
        while ((status = iter->Next()).ok()) {
            ASSERT_EQ(it->first, iter->Id());
            ASSERT_TRUE(iter->Data() == it->second);
            ++it;
        }
        ASSERT_TRUE(status.IsOutOfRange()) << status.ToString();
    }
}

TEST_F(StreamTest, OpenClose) {
    std::random_device rd;
    std::vector<std::pair<uint64_t, std::string>> datas;
    for (size_t i = 0; i < 10000; ++i) {
        std::string data;
        size_t size = 8 + rd() % 8192;
        for (size_t i = 0; i < size; ++i) {
            data.push_back(rd() % 26 + 'A');
        }
        uint64_t id = 0;
        Append(stream_.get(), data, &id);
        ASSERT_TRUE(datas.empty() || id > datas.back().first);
        datas.push_back(std::make_pair(id, data));
    }

    Commit(stream_.get());
    Close(stream_.get());
    stream_.reset();

    Stream* stream = nullptr;
    Open(uri_, &stream);
    stream_.reset(stream);

    for (size_t i = 0; i < 100; ++i) {
        size_t start = rd() % (datas.back().first + kBlockSize);
        ScopedIterator iter = stream_->NewIterator(start, UINT64_MAX);
        Status status;
        auto it = std::lower_bound(datas.begin(), datas.end(),
                                   std::pair<uint64_t, std::string>(start, ""));
        while ((status = iter->Next()).ok()) {
            BYTE_ASSERT(it->first == iter->Id());
            ASSERT_EQ(it->first, iter->Id());
            ASSERT_TRUE(iter->Data() == it->second);
            ++it;
        }
        BYTE_ASSERT(status.IsOutOfRange());
        ASSERT_TRUE(status.IsOutOfRange()) << status.ToString();
    }

    for (size_t i = 0; i < 1000; ++i) {
        std::string data;
        size_t size = 8 + rd() % 8192;
        for (size_t i = 0; i < size; ++i) {
            data.push_back(rd() % 26 + 'A');
        }
        uint64_t id = 0;
        Append(stream_.get(), data, &id);
        ASSERT_TRUE(datas.empty() || id > datas.back().first);
        datas.push_back(std::make_pair(id, data));
    }

    for (size_t i = 0; i < 100; ++i) {
        size_t start = rd() % (datas.back().first + kBlockSize);
        ScopedIterator iter = stream_->NewIterator(start, UINT64_MAX);
        Status status;
        auto it = std::lower_bound(datas.begin(), datas.end(),
                                   std::pair<uint64_t, std::string>(start, ""));
        while ((status = iter->Next()).ok()) {
            BYTE_ASSERT(it->first == iter->Id());
            ASSERT_EQ(it->first, iter->Id());
            ASSERT_TRUE(iter->Data() == it->second);
            ++it;
        }
        ASSERT_TRUE(status.IsOutOfRange()) << status.ToString();
    }
}

TEST_F(StreamTest, SimpleWithReopen) {
    std::string data = "hello";

    uint64_t id1 = 0;
    Append(stream_.get(), data, &id1);
    Commit(stream_.get());

    ExpectedRead(stream_.get(), id1, data);

    Close(stream_.get());
    stream_.reset();

    Stream* stream = nullptr;
    Open(uri_, &stream);
    stream_.reset(stream);

    ExpectedRead(stream_.get(), id1, data);
}

TEST_F(StreamTest, Truncate) {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<size_t> range(1, 1024 * 1024);
    for (int i = 0; i < 30; i++) {
        size_t l = range(rng);
        std::string s = random_string(l);
        uint64_t id = 0;
        Append(stream_.get(), s, &id);
    }
    Commit(stream_.get());
    StreamBaseImpl* impl = dynamic_cast<StreamImpl*>(stream_.get())->stream_base_.get();
    std::cout << impl->readable_blobs_.size() << std::endl;
    // truncate
    stream_->Truncate(stream_->Stat().length);
    for (int i = 30; i < 60; i++) {
        size_t l = range(rng);
        std::string s = random_string(l);
        uint64_t id = 0;
        Append(stream_.get(), s, &id);
    }
    Commit(stream_.get());
    std::cout << impl->readable_blobs_.size() << std::endl;
    // truncate
    stream_->Truncate(stream_->Stat().length);
    for (int i = 60; i < 90; i++) {
        size_t l = range(rng);
        std::string s = random_string(l);
        uint64_t id = 0;
        Append(stream_.get(), s, &id);
    }
    Commit(stream_.get());
    std::cout << impl->readable_blobs_.size() << std::endl;

    std::map<uint64_t, std::string> datas;
    for (int i = 90; i < 120; i++) {
        size_t l = range(rng);
        std::string s = random_string(l);
        uint64_t id = 0;
        Append(stream_.get(), s, &id);
        datas[id] = s;
    }
    Commit(stream_.get());
    std::cout << impl->readable_blobs_.size() << std::endl;

    for (auto& p : datas) {
        ExpectedRead(stream_.get(), p.first, p.second);
    }

    // reopen
    Close(stream_.get());
    stream_.reset();

    Stream* stream = nullptr;
    Open(uri_, &stream);
    stream_.reset(stream);

    impl = dynamic_cast<StreamImpl*>(stream_.get())->stream_base_.get();
    std::cout << impl->readable_blobs_.size() << std::endl;
    for (auto& p : datas) {
        ExpectedRead(stream_.get(), p.first, p.second);
    }
}

TEST_F(StreamTest, ConditionChange) {
    uint64_t id1 = 0;
    Append(stream_.get(), "Data1", &id1);
    Controller ctrl;
    SYNC_CALL(stream_->Commit, &ctrl);

    std::unique_ptr<stream::Stream> old_stream = std::move(stream_);
    Stream* tmp_stream = nullptr;
    Open(uri_, &tmp_stream);
    stream_.reset(tmp_stream);

    ctrl.Reset();
    uint64_t id2 = 0;
    Append(old_stream.get(), "Data2", &id2);
    SYNC_CALL(old_stream->Commit, &ctrl);
    EXPECT_TRUE(ctrl.status().IsStreamAbort());

    ctrl.Reset();
    uint64_t id3 = 0;
    Append(old_stream.get(), "Data3", &id3);
    SYNC_CALL(old_stream->Commit, &ctrl);
    EXPECT_TRUE(ctrl.status().IsStreamAbort());

    ExpectedRead(old_stream.get(), id1, "Data1");
    Close(old_stream.get());
}

TEST_F(StreamTest, ReaderReplicateAIO) {
    auto stream_max_blob_size = FLAGS_stream_max_blob_size;
    auto stream_blob_deletion_min_age = FLAGS_stream_blob_deletion_min_age;
    auto stream_blob_deletion_min_gap = FLAGS_stream_blob_deletion_min_gap;
    FLAGS_stream_max_blob_size = kBlockSize + 256 * 1024;
    FLAGS_stream_blob_deletion_min_age = 0;
    FLAGS_stream_blob_deletion_min_gap = 0;
    BYTE_DEFER({
        FLAGS_stream_max_blob_size = stream_max_blob_size;
        FLAGS_stream_blob_deletion_min_age = stream_blob_deletion_min_age;
        FLAGS_stream_blob_deletion_min_gap = stream_blob_deletion_min_gap;
    });

    struct History {
        uint64_t id = 0;
        std::string data;
    };

    size_t check_success = 0;
    std::list<History> histories;
    std::uniform_int_distribution<size_t> pick(1, FLAGS_stream_max_blob_size / 100);
    bool restored = false;
    for (size_t i = 0; i < 20000; ++i) {
        History history;
        history.data = random_string(pick(rd));
        stream_->Append(history.data, &history.id);
        histories.emplace_back(std::move(history));

        // commit randomly
        if (pick(rd) % 8 == 0) {
            Controller ctrl;
            SYNC_CALL(stream_->Commit, &ctrl);
            ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
        }

        // restore reader randomly
        if (pick(rd) % 50 == 0) {
            Status status = stream_reader_->RestoreInfo(stream_->GetInfo());
            ASSERT_TRUE(status.ok()) << i << "\n"
                                     << status << "\n"
                                     << stream_->GetInfo().ShortDebugString();

            const auto& reader_stat = stream_reader_->Stat();
            const auto& stat = stream_->Stat();
            ASSERT_LE(reader_stat.start_record_id, stat.start_record_id);
            ASSERT_EQ(reader_stat.length, stat.persistent_length);
            ASSERT_EQ(reader_stat.persistent_length, stat.persistent_length);
            ASSERT_EQ(reader_stat.usage_bytes, stat.usage_bytes);
            restored = true;
        }

        // pick an id to truncate
        if (pick(rd) % 5 == 0) {
            while (!histories.empty() && pick(rd) % 3 == 0) {
                histories.pop_front();
            }
            if (!histories.empty()) {
                stream_->Truncate(histories.front().id);
            }
        }

        // check history data
        if (restored) {
            int j = 0;
            for (auto it = histories.begin(); it != histories.end() && j < 10; ++it, ++j) {
                const History& history = *it;
                std::string data(history.data, '\0');

                Controller ctrl;
                SYNC_CALL(stream_reader_->Read, &ctrl, history.id, &data[0], data.size());
                if (ctrl.status().IsOutOfRange()) {
                    // the stream reader may not have caught up
                    break;
                }
                ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
                ASSERT_EQ(data, history.data);
                ++check_success;
            }
        }
    }
    ASSERT_GT(check_success, 0);
}

TEST_F(StreamTest, ReaderReplicate) {
    auto stream_max_blob_size = FLAGS_stream_max_blob_size;
    auto stream_blob_deletion_min_age = FLAGS_stream_blob_deletion_min_age;
    auto stream_blob_deletion_min_gap = FLAGS_stream_blob_deletion_min_gap;
    FLAGS_stream_max_blob_size = kBlockSize + 256 * 1024;
    FLAGS_stream_blob_deletion_min_age = 0;
    FLAGS_stream_blob_deletion_min_gap = 0;
    BYTE_DEFER({
        FLAGS_stream_max_blob_size = stream_max_blob_size;
        FLAGS_stream_blob_deletion_min_age = stream_blob_deletion_min_age;
        FLAGS_stream_blob_deletion_min_gap = stream_blob_deletion_min_gap;
    });

    struct History {
        uint64_t id = 0;
        std::string data;
    };

    std::list<History> histories;
    std::uniform_int_distribution<size_t> pick(1, FLAGS_stream_max_blob_size / 100);
    for (size_t i = 0; i < 10000; ++i) {
        History history;
        history.data = random_string(pick(rd));
        stream_->Append(history.data, &history.id);
        histories.emplace_back(std::move(history));

        // commit randomly
        if (pick(rd) % 8 == 0) {
            Controller ctrl;
            SYNC_CALL(stream_->Commit, &ctrl);
            ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
        }

        // pick an id to truncate
        if (pick(rd) % 20 == 0) {
            while (!histories.empty() && pick(rd) % 3 == 0) {
                histories.pop_front();
            }
            if (!histories.empty()) {
                stream_->Truncate(histories.front().id);
            }
        }
    }

    // commit
    Controller ctrl;
    SYNC_CALL(stream_->Commit, &ctrl);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();

    // restore reader
    Status status = stream_reader_->RestoreInfo(stream_->GetInfo());
    ASSERT_TRUE(status.ok()) << status << "\n" << stream_->GetInfo().ShortDebugString();

    const auto& reader_stat = stream_reader_->Stat();
    const auto& stat = stream_->Stat();
    ASSERT_LE(reader_stat.start_record_id, stat.start_record_id);
    ASSERT_EQ(reader_stat.length, stat.persistent_length);
    ASSERT_EQ(reader_stat.persistent_length, stat.persistent_length);
    ASSERT_EQ(reader_stat.usage_bytes, stat.usage_bytes);

    // check history data
    std::cout << histories.size() << "\n";
    for (auto it = histories.begin(); it != histories.end(); ++it) {
        const History& history = *it;
        std::string data(history.data, '\0');

        Controller ctrl;
        SYNC_CALL(stream_reader_->Read, &ctrl, history.id, &data[0], data.size());
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
        ASSERT_EQ(data, history.data);
    }

    // append buffer (not persistent)
    History history;
    history.data = random_string(pick(rd));
    stream_->Append(history.data, &history.id);

    // restore reader
    status = stream_reader_->RestoreInfo(stream_->GetInfo());
    ASSERT_TRUE(status.ok()) << status << "\n" << stream_->GetInfo().ShortDebugString();

    // read data
    std::string data(history.data, '\0');
    ctrl.Reset();
    SYNC_CALL(stream_reader_->Read, &ctrl, history.id, &data[0], data.size());
    ASSERT_TRUE(ctrl.status().IsOutOfRange());
}

TEST_F(StreamTest, ReaderIteration) {
    auto stream_max_blob_size = FLAGS_stream_max_blob_size;
    auto stream_blob_deletion_min_age = FLAGS_stream_blob_deletion_min_age;
    auto stream_blob_deletion_min_gap = FLAGS_stream_blob_deletion_min_gap;
    FLAGS_stream_max_blob_size = kBlockSize + 256 * 1024;
    FLAGS_stream_blob_deletion_min_age = 0;
    FLAGS_stream_blob_deletion_min_gap = 0;
    BYTE_DEFER({
        FLAGS_stream_max_blob_size = stream_max_blob_size;
        FLAGS_stream_blob_deletion_min_age = stream_blob_deletion_min_age;
        FLAGS_stream_blob_deletion_min_gap = stream_blob_deletion_min_gap;
    });

    struct History {
        uint64_t id = 0;
        std::string data;
    };

    std::vector<History> histories;
    std::uniform_int_distribution<size_t> pick(1, FLAGS_stream_max_blob_size / 100);
    for (size_t i = 0; i < 10000; ++i) {
        History history;
        history.data = random_string(pick(rd));
        stream_->Append(history.data, &history.id);
        histories.emplace_back(std::move(history));

        // commit randomly
        if (pick(rd) % 8 == 0) {
            Controller ctrl;
            SYNC_CALL(stream_->Commit, &ctrl);
            ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
        }
    }

    // commit
    Controller ctrl;
    SYNC_CALL(stream_->Commit, &ctrl);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();

    // restore reader
    Status status = stream_reader_->RestoreInfo(stream_->GetInfo());
    ASSERT_TRUE(status.ok()) << status << "\n" << stream_->GetInfo().ShortDebugString();

    // iterate reader
    auto histories_idx = 0;
    ScopedIterator stream_iter =
        stream_reader_->NewIterator(stream_reader_->Stat().start_record_id, UINT64_MAX);
    size_t stream_iterate_cnt = 0;
    while (true) {
        Status status = stream_iter->Next();
        if (status.IsOutOfRange()) {
            break;
        }
        ASSERT_TRUE(status.ok());

        ASSERT_LT(histories_idx, histories.size());
        ASSERT_EQ(histories[histories_idx].id, stream_iter->Id());
        ASSERT_EQ(histories[histories_idx].data, stream_iter->Data());
        ++stream_iterate_cnt;
        ++histories_idx;
    }
    ASSERT_EQ(stream_iterate_cnt, histories.size());

    // writer append more data
    for (size_t i = 0; i < 10000; ++i) {
        History history;
        history.data = random_string(pick(rd));
        stream_->Append(history.data, &history.id);
        histories.emplace_back(std::move(history));

        // commit randomly
        if (pick(rd) % 8 == 0) {
            Controller ctrl;
            SYNC_CALL(stream_->Commit, &ctrl);
            ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
        }
    }

    // check out of range before restore info
    ASSERT_TRUE(stream_iter->Next().IsOutOfRange());

    // commit
    ctrl.Reset();
    SYNC_CALL(stream_->Commit, &ctrl);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();

    // restore reader
    status = stream_reader_->RestoreInfo(stream_->GetInfo());
    ASSERT_TRUE(status.ok()) << status << "\n" << stream_->GetInfo().ShortDebugString();

    // continue iterate reader
    while (true) {
        Status status = stream_iter->Next();
        if (status.IsOutOfRange()) {
            break;
        }
        ASSERT_TRUE(status.ok());

        ASSERT_LT(histories_idx, histories.size());
        ASSERT_EQ(histories[histories_idx].id, stream_iter->Id());
        ASSERT_EQ(histories[histories_idx].data, stream_iter->Data());
        ++stream_iterate_cnt;
        ++histories_idx;
    }
    ASSERT_EQ(stream_iterate_cnt, histories.size());
}

}  // namespace stream
}  // namespace bcache2
