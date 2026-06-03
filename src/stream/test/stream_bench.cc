// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <byte/algorithm/crc32.h>
#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <random>
#include <unordered_set>

#include "common/function_closure.h"
#include "common/metrics.h"
#include "common/scoped_invoker.h"
#include "common/sync_closure.h"
#include "stream/log_based_env.h"
#include "stream/log_based_stream_base.h"
#include "test/common/bench.h"
#include "test/common/temp_dir.h"

DEFINE_string(stream_uri, "", "stream uri");
DEFINE_string(store_type, "file", "header of stream uri, local/file/blob");
DEFINE_string(depth_open, "1/1000", "depth for open");
DEFINE_string(depth_close, "1/7000", "depth for close");
DEFINE_string(depth_append, "1", "depth for append");
DEFINE_string(depth_read, "1", "depth for read");
DEFINE_string(depth_iter, "1", "depth for iterator");
DEFINE_string(depth_truncate, "1/1000", "depth for truncate");
DEFINE_string(depth_stat, "1/1000", "depth for stat");
DEFINE_uint32(time, 60, "run time in seconds");

namespace bcache2 {
namespace stream {

class StreamVerifier : public Verifier {
 public:
    enum OpType {
        kOpen,
        kClose,
        kAppend,
        kRead,
        kIterator,
        kTrim,
        kStat,
    };

    StreamVerifier(Env* env, const std::string& uri) : env_(env), uri_(uri) {
        metrics_manager_.reset(new MetricsManager({}, "partition"));
    }
    virtual ~StreamVerifier() {}

    Operation* GeneratorOp(int op_type) override;

    void FinishOp(Operation* op) override { delete op; }

 private:
    class OpenOp;
    class CloseOp;
    class AppendOp;
    class ReadOp;
    class IteratorOp;
    class TrimOp;
    class StatOp;

    struct Record {
        uint32_t size = 0;
        uint32_t check = 0;

        bool operator==(const Record& other) const {
            return size == other.size && check == other.check;
        }
    };

    struct RecordHash {
        size_t operator()(const Record& record) const {
            return (static_cast<uint64_t>(record.size) << 32) | record.check;
        }
    };

    Env* env_ = nullptr;
    std::string uri_;
    std::unique_ptr<Stream> stream_;
    std::random_device rd_;
    std::map<uint64_t, Record> records_;
    std::vector<uint64_t> record_ids_;
    std::unordered_set<Record, RecordHash> inflight_appends_;
    uint64_t truncated_id_ = 0;
    std::unique_ptr<MetricsManager> metrics_manager_;

    DISALLOW_COPY_AND_ASSIGN(StreamVerifier);
};

class StreamVerifier::OpenOp : public Operation {
 public:
    explicit OpenOp(StreamVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!IsCoContext()) {
            byte::InvokeInCurrentThread(NewCoClosure(this, &OpenOp::Run, ctrl, callback));
            return;
        }
        if (LIKELY(verifier_->stream_.get() != nullptr)) {
            ctrl->set_status(Status::AlreadyExists(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        thread_ = byte::GetCurrentThread();
        options_.metrics_manager = verifier_->metrics_manager_.get();
        verifier_->env_->OpenStream(ctrl, condition_, verifier_->uri_, options_, &stream_);
        OnRunDone(ctrl, callback);
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        if (byte::GetCurrentThread() != thread_) {
            thread_->Invoke(NewClosure(this, &OpenOp::OnRunDone, ctrl, callback));
            return;
        }
        ScopedCallback done(callback);
        if (ctrl->status().ok()) {
            BYTE_ASSERT(!verifier_->stream_);
            verifier_->stream_.reset(stream_);
        }
    }

    StreamVerifier* verifier_ = nullptr;
    Stream* stream_ = nullptr;
    Env::Condition condition_;
    Env::OpenOptions options_;
    byte::AsyncThread* thread_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(OpenOp);
};

class StreamVerifier::CloseOp : public Operation {
 public:
    explicit CloseOp(StreamVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!IsCoContext()) {
            byte::InvokeInCurrentThread(NewCoClosure(this, &CloseOp::Run, ctrl, callback));
            return;
        }
        if (LIKELY(verifier_->stream_.get() == nullptr)) {
            ctrl->set_status(Status::NotFound(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        verifier_->stream_->Close(NewClosure(this, &CloseOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        BYTE_ASSERT(verifier_->stream_.get() != nullptr);
        verifier_->stream_.reset();
    }

    StreamVerifier* verifier_ = nullptr;
    DISALLOW_COPY_AND_ASSIGN(CloseOp);
};

class StreamVerifier::AppendOp : public Operation {
 public:
    explicit AppendOp(StreamVerifier* verifier) : verifier_(verifier) {}
    AppendOp(StreamVerifier* verifier, size_t size) : verifier_(verifier), data_(size, '\0') {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (LIKELY(verifier_->stream_.get() == nullptr)) {
            ctrl->set_status(Status::NotFound(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        if (data_.empty()) {
            data_.resize(1024 + verifier_->rd_() % kBlockSize, verifier_->rd_() % 256);
        }
        uint32_t crc32c = byte::CRCUtil::ComputeCRC32(0, data_.data(), data_.size());
        verifier_->stream_->Append(ctrl, data_.data(), data_.size(), &id_,
                                   NewClosure(this, &AppendOp::OnRunDone, ctrl, callback));
        record_.size = data_.size();
        record_.check = crc32c;
        verifier_->inflight_appends_.insert(record_);
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok()) {
            return;
        }
        auto it = verifier_->inflight_appends_.find(record_);
        BYTE_ASSERT(it != verifier_->inflight_appends_.end());
        BYTE_ASSERT(verifier_->records_.find(id_) == verifier_->records_.end());
        verifier_->records_[id_] = record_;
        verifier_->record_ids_.push_back(id_);
    }

    StreamVerifier* verifier_ = nullptr;
    std::string data_;
    uint64_t id_ = 0;
    Record record_;

    DISALLOW_COPY_AND_ASSIGN(AppendOp);
};

class StreamVerifier::ReadOp : public Operation {
 public:
    explicit ReadOp(StreamVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (LIKELY(verifier_->stream_.get() == nullptr)) {
            ctrl->set_status(Status::NotFound(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        if (LIKELY(verifier_->record_ids_.empty())) {
            ctrl->set_status(Status::NotFound(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        uint64_t record_id =
            verifier_->record_ids_[verifier_->rd_() % verifier_->record_ids_.size()];
        auto it = verifier_->records_.find(record_id);
        BYTE_ASSERT(it != verifier_->records_.end());
        record_ = it->second;
        data_.resize(record_.size);
        verifier_->stream_->Read(ctrl, record_id, &data_[0], data_.size(),
                                 NewClosure(this, &ReadOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok()) {
            return;
        }
        uint32_t crc32c = byte::CRCUtil::ComputeCRC32(0, data_.data(), data_.size());
        BYTE_ASSERT(crc32c == record_.check);
    }

    StreamVerifier* verifier_ = nullptr;
    Record record_;
    std::string data_;

    DISALLOW_COPY_AND_ASSIGN(ReadOp);
};

class StreamVerifier::IteratorOp : public Operation {
 public:
    explicit IteratorOp(StreamVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (LIKELY(verifier_->stream_.get() == nullptr)) {
            ctrl->set_status(Status::NotFound(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        BYTE_ASSERT(false);
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok()) {
            return;
        }
    }

    StreamVerifier* verifier_ = nullptr;
    DISALLOW_COPY_AND_ASSIGN(IteratorOp);
};

class StreamVerifier::TrimOp : public Operation {
 public:
    explicit TrimOp(StreamVerifier* verifier)
        : verifier_(verifier), append_op_(verifier_, kBlockSize) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (LIKELY(verifier_->stream_.get() == nullptr)) {
            ctrl->set_status(Status::NotFound(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        if (!verifier_->record_ids_.empty()) {
            truncated_id_ =
                verifier_->record_ids_[verifier_->rd_() % verifier_->record_ids_.size()];
        }
        verifier_->stream_->Truncate(truncated_id_);
        append_op_.Run(ctrl, NewClosure(this, &TrimOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok()) {
            return;
        }
        verifier_->truncated_id_ = std::max(truncated_id_, verifier_->truncated_id_);
    }

    StreamVerifier* verifier_ = nullptr;
    AppendOp append_op_;
    size_t truncated_id_ = 0;

    DISALLOW_COPY_AND_ASSIGN(TrimOp);
};

class StreamVerifier::StatOp : public Operation {
 public:
    explicit StatOp(StreamVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        ScopedInvoker done(callback);
        if (LIKELY(verifier_->stream_.get() == nullptr)) {
            ctrl->set_status(Status::NotFound(""));
            return;
        }
        Stats stats = verifier_->stream_->Stat();
        BYTE_ASSERT(stats.start_record_id >= verifier_->truncated_id_);
        verifier_->truncated_id_ = stats.start_record_id;
        ctrl->set_status(Status::OK());
    }

 private:
    StreamVerifier* verifier_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(StatOp);
};

Operation* StreamVerifier::GeneratorOp(int op_type) {
    switch (op_type) {
    case kOpen:
        return new OpenOp(this);
    case kClose:
        return new CloseOp(this);
    case kAppend:
        return new AppendOp(this);
    case kRead:
        return new ReadOp(this);
    case kIterator:
        return new IteratorOp(this);
    case kTrim:
        return new TrimOp(this);
    case kStat:
        return new StatOp(this);
    default:
        return nullptr;
    }
}

class StreamBench : public testing::Test {
 public:
    void SetUp() override {
        bytestore_set_flag("bytestore_client_log_level", "1");
        bytestore_init();
        byte::SetByteLogDir("./");
        byte::SetByteLogMaxFileNum(10);
        byte::SetByteLogMaxFileSize(1UL << 30);

        byte::AsyncThreadPoolOptions tp_options;
        tp_options.name_ = "test";
        background_pool_.reset(new byte::AsyncThreadPool());
        ASSERT_TRUE(background_pool_->Init(tp_options));
        ASSERT_TRUE(background_pool_->Start());

        byte::AsyncThreadPoolOptions work_options;
        work_pool_.reset(new byte::AsyncThreadPool());
        ASSERT_TRUE(work_pool_->Init(work_options));
        ASSERT_TRUE(work_pool_->Start());

        store_layer_.reset(new StoreLayer(background_pool_.get()));

        env_.reset(new LogBasedEnv());
        LogBasedEnv::Options env_options;
        env_options.background_pool = background_pool_.get();
        env_options.store_layer = store_layer_.get();
        env_->Init(env_options);

        if (FLAGS_stream_uri == "") {
            uri_ = FLAGS_store_type + "://" + temp_dir_.GetDir() + "/cluster/public/stream1/";
        } else {
            uri_ = FLAGS_stream_uri;
        }
        verifier_.reset(new StreamVerifier(env_.get(), uri_));
        Bench::Options options;
        options.thread_pool = work_pool_.get();
        options.jobs = 1;
        options.verifier = verifier_.get();
        bench_.Init(options);

        bench_.RegisterOp(StreamVerifier::kOpen, "Open", FLAGS_depth_open);
        bench_.RegisterOp(StreamVerifier::kClose, "Close", FLAGS_depth_close);
        bench_.RegisterOp(StreamVerifier::kAppend, "Append", FLAGS_depth_append);
        bench_.RegisterOp(StreamVerifier::kRead, "Read", FLAGS_depth_read);
        bench_.RegisterOp(StreamVerifier::kTrim, "Truncate", FLAGS_depth_truncate);
        bench_.RegisterOp(StreamVerifier::kStat, "Stat", FLAGS_depth_stat);

        bench_.Start();
    }
    void TearDown() override { bench_.Stop(); }

 protected:
    TempDir temp_dir_;
    std::string uri_;
    std::unique_ptr<byte::AsyncThreadPool> background_pool_;
    std::unique_ptr<byte::AsyncThreadPool> work_pool_;
    std::unique_ptr<StoreLayer> store_layer_;
    std::unique_ptr<LogBasedEnv> env_;
    std::unique_ptr<StreamVerifier> verifier_;
    Bench bench_;
};

TEST_F(StreamBench, SimpleBench) {
    for (size_t i = 0; i < FLAGS_time; ++i) {
        bench_.ShowStats();
        sleep(1);
    }
}

}  // namespace stream
}  // namespace bcache2
