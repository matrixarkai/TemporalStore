// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <absl/strings/numbers.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <random>

#include "blockcache/blockcache.h"
#include "common/fiu_local.h"
#include "common/macros.h"
#include "common/scoped_invoker.h"
#include "partition/index/index.h"
#include "partition/partition.h"
#include "partition/storage/storage_manager.h"
#include "protocol/storage.pb.h"
#include "stream/log_based_env.h"
#include "stream/stream.h"
#include "test/common/bench.h"
#include "test/common/kv_checker.h"
#include "test/common/temp_dir.h"

DEFINE_string(partition_uri, "", "partition uri");
DEFINE_bool(bench_mode, false, "bench mode");
DEFINE_uint32(key_size, 16, "key size");
DEFINE_uint32(key_count, 16, "key count");
DEFINE_uint32(field_size, 16, "field size");
DEFINE_uint32(field_count, 16, "field count");
DEFINE_string(depth_load, "1/1000", "depth for load");
DEFINE_string(depth_unload, "1/5000", "depth for unload");
DEFINE_string(depth_hset, "1", "depth for hset");
DEFINE_string(depth_hget, "1", "depth for hget");
DEFINE_string(depth_hdel, "1", "depth for hdel");
DEFINE_string(depth_del, "1", "depth for del");
DEFINE_string(depth_set, "1", "depth for set");
DEFINE_string(depth_get, "1", "depth for get");
DEFINE_uint32(time, 60, "run time in seconds");
DEFINE_uint32(brpc_server_port, 20000, "dummy brpc server port");
DEFINE_uint32(interval_water_level, 50, "interval for water level test bench");
DEFINE_bool(generated_key_mode, false, "whether use generated key runtime");
DEFINE_bool(only_string_model, false,
            "only string model are used in memory water level bench test");
DEFINE_uint32(generated_key_num, 500000, "not pre-allocated");

DECLARE_uint64(stream_max_blob_size);
DECLARE_uint32(storage_zone_size);
DECLARE_uint64(stream_blob_switch_retry_interval_us);
DECLARE_bool(storage_enable_index_gc);
DECLARE_double(storage_gc_space_utility_threshold);
DECLARE_bool(storage_async);
DECLARE_uint64(storage_gc_zone_destroy_delay_ms);
DECLARE_uint64(stream_blob_deletion_min_age);
DECLARE_uint64(stream_blob_deletion_min_gap);
DECLARE_uint64(storage_gc_max_bytes_per_round);
DECLARE_uint64(storage_gc_max_slots_per_round);
DECLARE_uint64(evict_batch_size);
DECLARE_uint64(evict_count_limit);

namespace bcache2 {
namespace partition {

class SimplePartitionVerifier : public Verifier {
 public:
    enum OpType {
        kLoad,
        kUnload,
        kHSet,
        kHGet,
        kHDel,
        kDelete,
        kSet,
        kGet,
    };

    SimplePartitionVerifier(stream::Env* env, const std::string& uri, size_t key_count,
                            size_t key_size, size_t field_count, size_t field_size,
                            Partition* partition = nullptr,
                            blockcache::BlockCache* blockcache = nullptr)
        : env_(env),
          uri_(uri),
          key_count_(key_count),
          key_size_(key_size),
          field_count_(field_count),
          field_size_(field_size),
          partition_(partition),
          blockcache_(blockcache) {
        if (FLAGS_generated_key_mode) return;
        keys_.resize(key_count_);
        string_keys_.resize(key_count);
        fields_.resize(field_count_);
        for (size_t i = 0; i < keys_.size(); ++i) {
            for (size_t j = 0; j < key_size_; ++j) {
                keys_[i].push_back(rd_() % 26 + 'A');
                string_keys_[i].push_back(rd_() % 26 + 'A');
            }
        }
        for (size_t i = 0; i < fields_.size(); ++i) {
            for (size_t j = 0; j < field_size_; ++j) {
                fields_[i].push_back(rd_() % 26 + 'A');
            }
        }
        for (size_t i = 0; i < keys_.size(); ++i) {
            for (size_t j = 0; j < fields_.size(); ++j) {
                checker_map_[keys_[i] + fields_[j]].reset(new KvChecker());
            }
            string_checker_map_[string_keys_[i]].reset(new KvChecker());
        }
    }

    void RecordKey(std::string key) {
        if (recorded_keys_.count(key) == 0) {
            recorded_keys_.insert(key);
            recorded_size += 2;
        }
    }
    void RecordHKeyAndField(std::string htable, std::string hfield) {
        if (recorded_hash_keys_.find(htable) == recorded_hash_keys_.end()) {
            recorded_hash_keys_[htable] = std::move(std::set<std::string>());
            recorded_size += 1;
        }
        recorded_hash_keys_[htable].insert(hfield);
        recorded_size += 2;
    }

    void RecordDeleteKey(std::string key) {
        ASSERT_EQ(recorded_keys_.count(key), 1);
        recorded_keys_.erase(key);
        recorded_size -= 2;
    }

    void RecordDeleteHKeyAndField(std::string htable, std::string hfield) {
        recorded_hash_keys_[htable].erase(hfield);
        recorded_size -= 2;
    }
    size_t GetKeyCount() { return recorded_size; }
    Operation* GeneratorOp(int op_type) override;
    void FinishOp(Operation* op) override { delete op; }

 private:
    class LoadOp;
    class UnloadOp;
    class HSetOp;
    class HSetBenchOp;
    class HGetOp;
    class HGetBenchOp;
    class HDelOp;
    class DeleteOp;
    class SetOp;
    class SetBenchOp;
    class GetOp;
    class GetBenchOp;

    std::string GeneratedKey(int32_t key_id) {
        std::string generated_key;
        for (uint32_t i = 0; i < key_size_; i++) {
            generated_key.push_back(key_id % 26 + 'A');
            key_id /= 26;
        }
        return generated_key;
    }

    std::string RandKey() {
        if (FLAGS_generated_key_mode) {
            return GeneratedKey(rd_() % FLAGS_generated_key_num);
        } else {
            return keys_[rd_() % keys_.size()];
        }
    }
    std::string RandStringKey() {
        if (FLAGS_generated_key_mode) {
            return GeneratedKey(rd_() % FLAGS_generated_key_num);
        } else {
            return string_keys_[rd_() % keys_.size()];
        }
    }

    void RandKeyAndField(std::string* key, std::string* field) {
        if (FLAGS_generated_key_mode) {
            *key = GeneratedKey(rd_() % FLAGS_generated_key_num);
            *field = GeneratedKey(rd_() % FLAGS_generated_key_num);
        } else {
            *key = keys_[rd_() % keys_.size()];
            *field = fields_[rd_() % fields_.size()];
        }
    }

    stream::Env* env_ = nullptr;
    std::string uri_;
    size_t key_count_ = 0;
    size_t key_size_ = 0;
    size_t field_count_ = 0;
    size_t field_size_ = 0;
    uint64_t load_version_ = 0;
    std::set<std::string> recorded_keys_;
    std::map<std::string, std::set<std::string>> recorded_hash_keys_;
    size_t recorded_size = 0;
    std::vector<std::string> keys_;
    std::vector<std::string> fields_;
    std::vector<std::string> string_keys_;

    Partition* partition_;
    std::map<std::string, std::unique_ptr<KvChecker>> checker_map_;
    std::map<std::string, std::unique_ptr<KvChecker>> string_checker_map_;
    std::random_device rd_;
    bcache2::blockcache::BlockCache* blockcache_{nullptr};
    DISALLOW_COPY_AND_ASSIGN(SimplePartitionVerifier);
};

class SimplePartitionVerifier::LoadOp : public Operation {
 public:
    explicit LoadOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!IsCoContext()) {
            byte::InvokeInCurrentThread(NewCoClosure(this, &LoadOp::Run, ctrl, callback));
            return;
        }

        ScopedInvoker done(callback);
        if (verifier_->partition_ != nullptr) {
            ctrl->set_status(Status::AlreadyExists(""));
            return;
        }

        if (!verifier_->blockcache_ && FLAGS_enable_blockcache) {
            verifier_->blockcache_ = new blockcache::BlockCache();
            verifier_->blockcache_->Start();
        }

        Partition::Options options;
        options.env = verifier_->env_;
        options.uri = verifier_->uri_;
        options.load_version = ++verifier_->load_version_;
        options.blockcache = verifier_->blockcache_;
        std::unique_ptr<Partition> partition(new Partition(options));
        Status status = partition->Load();
        if (!status.ok()) {
            ctrl->set_status(status);
            return;
        }
        verifier_->partition_ = partition.release();
        ctrl->set_status(Status::OK());
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {}
    SimplePartitionVerifier* verifier_ = nullptr;
    DISALLOW_COPY_AND_ASSIGN(LoadOp);
};

class SimplePartitionVerifier::UnloadOp : public Operation {
 public:
    explicit UnloadOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        ScopedInvoker done(callback);
        if (verifier_->partition_ == nullptr) {
            ctrl->set_status(Status::NotFound(""));
            return;
        }
        done.Release();
        verifier_->partition_->Unload();
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedInvoker done(callback);
        BYTE_ASSERT(verifier_->partition_ != nullptr);
        delete verifier_->partition_;
        delete verifier_->blockcache_;
        verifier_->partition_ = nullptr;
        verifier_->blockcache_ = nullptr;
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    DISALLOW_COPY_AND_ASSIGN(UnloadOp);
};

class SimplePartitionVerifier::HSetOp : public Operation {
 public:
    explicit HSetOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        hash::SetRequest* request = request_.mutable_hash_request()->mutable_set_request();
        verifier_->RandKeyAndField(request->mutable_key(), request->mutable_field());
        checker_ = verifier_->checker_map_[request->key() + request->field()].get();
        uint64_t value = 0;
        handle_ = checker_->NewWrite(&value);
        request->set_value(std::to_string(value));

        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_,
                                          NewClosure(this, &HSetOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok() || response_.response_status().code() != 0) {
            checker_->FinishWrite(handle_, false);
            return;
        }
        checker_->FinishWrite(handle_, true);
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;
    KvChecker* checker_ = nullptr;
    uint64_t handle_ = 0;

    DISALLOW_COPY_AND_ASSIGN(HSetOp);
};

class SimplePartitionVerifier::HSetBenchOp : public Operation {
 public:
    explicit HSetBenchOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        hash::SetRequest* request = request_.mutable_hash_request()->mutable_set_request();
        verifier_->RandKeyAndField(request->mutable_key(), request->mutable_field());
        uint64_t value = UINT64_MAX;
        request->set_value(std::to_string(value));
        verifier_->partition_->ExecuteCmd(
            ctrl, &request_, &response_, NewClosure(this, &HSetBenchOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        auto request = request_.hash_request().set_request();
        if (!ctrl->status().ok() || response_.response_status().code() != 0) {
            return;
        }
        verifier_->RecordHKeyAndField(request.key(), request.field());
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;

    DISALLOW_COPY_AND_ASSIGN(HSetBenchOp);
};

class SimplePartitionVerifier::HGetOp : public Operation {
 public:
    explicit HGetOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        hash::GetRequest* request = request_.mutable_hash_request()->mutable_get_request();
        verifier_->RandKeyAndField(request->mutable_key(), request->mutable_field());
        checker_ = verifier_->checker_map_[request->key() + request->field()].get();
        handle_ = checker_->NewRead();

        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_,
                                          NewClosure(this, &HGetOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok() || response_.response_status().code() != 0) {
            checker_->FinishRead(handle_, false, 0);
            return;
        }
        const std::string& data = response_.hash_response().get_response().value();
        uint64_t value = 0;
        BYTE_ASSERT(absl::SimpleAtoi(data, &value));
        checker_->FinishRead(handle_, true, value);
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;
    KvChecker* checker_ = nullptr;
    uint64_t handle_ = 0;

    DISALLOW_COPY_AND_ASSIGN(HGetOp);
};

class SimplePartitionVerifier::HGetBenchOp : public Operation {
 public:
    explicit HGetBenchOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        hash::GetRequest* request = request_.mutable_hash_request()->mutable_get_request();
        verifier_->RandKeyAndField(request->mutable_key(), request->mutable_field());

        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_, callback);
    }

 private:
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;

    DISALLOW_COPY_AND_ASSIGN(HGetBenchOp);
};

class SimplePartitionVerifier::HDelOp : public Operation {
 public:
    explicit HDelOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        hash::DelRequest* request = request_.mutable_hash_request()->mutable_del_request();
        verifier_->RandKeyAndField(request->mutable_key(), request->mutable_field());

        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_,
                                          NewClosure(this, &HDelOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!FLAGS_bench_mode || !ctrl->status().ok() || response_.response_status().code() != 0) {
            return;
        }
        auto& request = request_.hash_request().del_request();
        verifier_->RecordDeleteHKeyAndField(request.key(), request.field());
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;

    DISALLOW_COPY_AND_ASSIGN(HDelOp);
};

class SimplePartitionVerifier::DeleteOp : public Operation {
 public:
    explicit DeleteOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }

        request_.mutable_common_request()->mutable_del_object_request()->set_key(
            verifier_->RandKey());
        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_,
                                          NewClosure(this, &DeleteOp::OnRunDone, ctrl, callback));
        return;
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        auto request = request_.string_request().set_request();
        if (ctrl->status().ok() && response_.response_status().code() == 0 && FLAGS_bench_mode) {
            verifier_->RecordDeleteKey(request_.common_request().del_object_request().key());
        }
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;

    DISALLOW_COPY_AND_ASSIGN(DeleteOp);
};

class SimplePartitionVerifier::SetOp : public Operation {
 public:
    explicit SetOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        str::SetRequest* request = request_.mutable_string_request()->mutable_set_request();
        request->set_key(verifier_->RandStringKey());
        checker_ = verifier_->string_checker_map_[request->key()].get();
        uint64_t value = 0;
        handle_ = checker_->NewWrite(&value);
        request->set_value(std::to_string(value));
        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_,
                                          NewClosure(this, &SetOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok() || response_.response_status().code() != 0) {
            checker_->FinishWrite(handle_, false);
            return;
        }
        checker_->FinishWrite(handle_, true);
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;
    KvChecker* checker_ = nullptr;
    uint64_t handle_ = 0;

    DISALLOW_COPY_AND_ASSIGN(SetOp);
};

class SimplePartitionVerifier::SetBenchOp : public Operation {
 public:
    explicit SetBenchOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        str::SetRequest* request = request_.mutable_string_request()->mutable_set_request();
        request->set_key(verifier_->RandStringKey());
        request->set_value("ABNAFSFSDSD");
        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_,
                                          NewClosure(this, &SetBenchOp::OnRunDone, ctrl, callback));
        return;
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        auto request = request_.string_request().set_request();
        if (ctrl->status().ok() && response_.response_status().code() == 0) {
            verifier_->RecordKey(request.key());
        }
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;

    DISALLOW_COPY_AND_ASSIGN(SetBenchOp);
};

class SimplePartitionVerifier::GetOp : public Operation {
 public:
    explicit GetOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        str::GetRequest* request = request_.mutable_string_request()->mutable_get_request();
        request->set_key(verifier_->RandStringKey());
        checker_ = verifier_->string_checker_map_[request->key()].get();
        handle_ = checker_->NewRead();

        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_,
                                          NewClosure(this, &GetOp::OnRunDone, ctrl, callback));
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok() || response_.response_status().code() != 0) {
            checker_->FinishRead(handle_, false, 0);
            return;
        }
        const std::string& data = response_.string_response().get_response().value();
        uint64_t value = 0;
        BYTE_ASSERT(absl::SimpleAtoi(data, &value));
        checker_->FinishRead(handle_, true, value);
    }
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;
    KvChecker* checker_ = nullptr;
    uint64_t handle_ = 0;

    DISALLOW_COPY_AND_ASSIGN(GetOp);
};

class SimplePartitionVerifier::GetBenchOp : public Operation {
 public:
    explicit GetBenchOp(SimplePartitionVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (!verifier_->partition_) {
            ctrl->set_status(Status::FailedPrecondition(""));
            byte::InvokeInCurrentThread(callback);
            return;
        }
        str::GetRequest* request = request_.mutable_string_request()->mutable_get_request();
        request->set_key(verifier_->RandStringKey());

        verifier_->partition_->ExecuteCmd(ctrl, &request_, &response_, callback);
    }

 private:
    SimplePartitionVerifier* verifier_ = nullptr;
    CmdRequest request_;
    CmdResponse response_;

    DISALLOW_COPY_AND_ASSIGN(GetBenchOp);
};

Operation* SimplePartitionVerifier::GeneratorOp(int op_type) {
    switch (op_type) {
    case kLoad:
        return new LoadOp(this);
    case kUnload:
        return new UnloadOp(this);
    case kHSet:
        return FLAGS_bench_mode ? dynamic_cast<Operation*>(new HSetBenchOp(this))
                                : dynamic_cast<Operation*>(new HSetOp(this));
    case kHGet:
        return FLAGS_bench_mode ? dynamic_cast<Operation*>(new HGetBenchOp(this))
                                : dynamic_cast<Operation*>(new HGetOp(this));
    case kHDel:
        return new HDelOp(this);
    case kDelete:
        return new DeleteOp(this);
    case kSet:
        return FLAGS_bench_mode ? dynamic_cast<Operation*>(new SetBenchOp(this))
                                : dynamic_cast<Operation*>(new SetOp(this));
    case kGet:
        return FLAGS_bench_mode ? dynamic_cast<Operation*>(new GetBenchOp(this))
                                : dynamic_cast<Operation*>(new GetOp(this));
    default:
        return nullptr;
    }
}

class SimplePartitionBench : public testing::Test {
 public:
    void SetUp() {
        FLAGS_storage_async = false;
        FLAGS_storage_gc_zone_destroy_delay_ms = 0;
        FLAGS_stream_blob_deletion_min_age = 0;
        FLAGS_stream_blob_deletion_min_gap = 0;
    }

    void TearDown() {}
};

TEST(SimplePartitionBench, Smoketest) {
    bytestore_set_flag("bytestore_client_log_level", "1");
    bytestore_init();

    byte::AsyncThreadPoolOptions tp_options;
    tp_options.name_ = "test";
    byte::AsyncThreadPool background_pool;
    ASSERT_TRUE(background_pool.Init(tp_options));
    ASSERT_TRUE(background_pool.Start());

    stream::StoreLayer store_layer(&background_pool);

    stream::LogBasedEnv env;
    stream::LogBasedEnv::Options env_options;
    env_options.background_pool = &background_pool;
    env_options.store_layer = &store_layer;
    env.Init(env_options);

    byte::AsyncThreadPoolOptions work_options;
    work_options.name_ = "work";
    byte::AsyncThreadPool work_pool;
    ASSERT_TRUE(work_pool.Init(work_options));
    ASSERT_TRUE(work_pool.Start());

    TempDir temp_dir;
    std::string uri = "file://" + temp_dir.GetDir() + "/cluster/pool/partition";
    if (FLAGS_partition_uri != "") {
        uri = FLAGS_partition_uri;
    }
    SimplePartitionVerifier verifier(&env, uri, FLAGS_key_count, FLAGS_key_size, FLAGS_field_count,
                                     FLAGS_field_size);

    Bench::Options options;
    options.thread_pool = &work_pool;
    options.jobs = 1;
    options.verifier = &verifier;
    Bench bench;
    bench.Init(options);

    bench.RegisterOp(SimplePartitionVerifier::kLoad, "Load", FLAGS_depth_load);
    bench.RegisterOp(SimplePartitionVerifier::kUnload, "Unload", FLAGS_depth_unload);
    bench.RegisterOp(SimplePartitionVerifier::kHSet, "HSet", FLAGS_depth_hset);
    bench.RegisterOp(SimplePartitionVerifier::kHGet, "HGet", FLAGS_depth_hget);
    bench.RegisterOp(SimplePartitionVerifier::kHDel, "HDel", FLAGS_depth_hdel);
    bench.RegisterOp(SimplePartitionVerifier::kDelete, "Delete", FLAGS_depth_del);
    bench.RegisterOp(SimplePartitionVerifier::kSet, "Set", FLAGS_depth_set);
    bench.RegisterOp(SimplePartitionVerifier::kGet, "Get", FLAGS_depth_get);

    bench.Start();
    for (size_t i = 0; i < FLAGS_time; ++i) {
        bench.ShowStats();
        sleep(1);
    }
    bench.Stop();
    work_pool.Stop();
    background_pool.Stop();
    bytestore_shutdown();
}

TEST(SimplePartitionBench, PartitionLatency) {
    brpc::StartDummyServerAt(FLAGS_brpc_server_port);

    bytestore_set_flag("bytestore_client_log_level", "1");
    bytestore_init();

    byte::AsyncThreadPoolOptions tp_options;
    tp_options.name_ = "test";
    byte::AsyncThreadPool background_pool;
    ASSERT_TRUE(background_pool.Init(tp_options));
    ASSERT_TRUE(background_pool.Start());

    stream::StoreLayer store_layer(&background_pool);

    stream::LogBasedEnv env;
    stream::LogBasedEnv::Options env_options;
    env_options.background_pool = &background_pool;
    env_options.store_layer = &store_layer;
    env.Init(env_options);

    byte::AsyncThreadPoolOptions work_options;
    work_options.name_ = "work";
    byte::AsyncThreadPool work_pool;
    ASSERT_TRUE(work_pool.Init(work_options));
    ASSERT_TRUE(work_pool.Start());

    TempDir temp_dir;
    std::string uri = "file://" + temp_dir.GetDir() + "/cluster/public/partition";
    if (FLAGS_partition_uri != "") {
        uri = FLAGS_partition_uri;
    }

    std::unique_ptr<blockcache::BlockCache> blockcache;
    if (FLAGS_enable_blockcache) {
        blockcache.reset(new blockcache::BlockCache);
        blockcache->Start();
    }

    Partition::Options partition_options;
    partition_options.env = &env;
    partition_options.uri = uri;
    partition_options.load_version = 1;
    partition_options.blockcache = blockcache.get();
    std::unique_ptr<Partition> partition;
    partition.reset(new Partition(partition_options));

    CoSyncClosure sync;
    Status status;
    work_pool.KthThread(0)->Invoke(NewCoFuncClosure([&] {
        status = partition->Load();
        ASSERT_TRUE(status.IsOK());
        sync.Run();
    }));
    sync.Wait();
    SimplePartitionVerifier verifier(&env, uri, FLAGS_key_count, FLAGS_key_size, FLAGS_field_count,
                                     FLAGS_field_size, partition.get(), blockcache.get());

    Bench::Options options;
    options.thread_pool = &work_pool;
    options.jobs = 1;
    options.verifier = &verifier;
    Bench bench;
    bench.Init(options);

    bench.RegisterOp(SimplePartitionVerifier::kHSet, "HSet", FLAGS_depth_hset);
    bench.RegisterOp(SimplePartitionVerifier::kHGet, "HGet", FLAGS_depth_hget);
    bench.RegisterOp(SimplePartitionVerifier::kHDel, "HDel", FLAGS_depth_hdel);
    bench.RegisterOp(SimplePartitionVerifier::kDelete, "Delete", FLAGS_depth_del);
    bench.RegisterOp(SimplePartitionVerifier::kSet, "Set", FLAGS_depth_set);
    bench.RegisterOp(SimplePartitionVerifier::kGet, "Get", FLAGS_depth_get);

    bench.Start();
    for (size_t i = 0; i < FLAGS_time; ++i) {
        bench.ShowStats();
        CoSleep(1 * 1000 * 1000);
    }
    bench.Stop();

    partition->Unload();

    work_pool.Stop();
    background_pool.Stop();
    bytestore_shutdown();
}

// allocator memory used check
TEST(WaterLevelPartitionBench, MemWaterLevelTest) {
    RESTORE_FLAGS(FLAGS_bench_mode);
    RESTORE_FLAGS(FLAGS_evict_batch_size);
    RESTORE_FLAGS(FLAGS_evict_count_limit);
    RESTORE_FLAGS(FLAGS_stream_blob_deletion_min_age);
    RESTORE_FLAGS(FLAGS_stream_blob_deletion_min_gap);
    RESTORE_FLAGS(FLAGS_storage_gc_zone_destroy_delay_ms);

    FLAGS_storage_gc_zone_destroy_delay_ms = 0;
    FLAGS_stream_blob_deletion_min_age = 0;
    FLAGS_stream_blob_deletion_min_gap = 0;
    FLAGS_bench_mode = true;
    FLAGS_evict_batch_size = 500;
    FLAGS_evict_count_limit = 5000;

    std::cout << "WaterLevelPatitionBench.MemWaterLevelTest will cost about 10 mins" << std::endl;
    std::unique_ptr<byte::AsyncThreadPool> background_pool_;
    std::unique_ptr<stream::StoreLayer> store_layer_;
    std::unique_ptr<stream::LogBasedEnv> env_;
    std::unique_ptr<byte::AsyncThreadPool> work_pool_;
    TempDir temp_dir_;
    std::string uri_;
    bytestore_set_flag("bytestore_client_log_level", "1");
    bytestore_init();
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

    byte::AsyncThreadPoolOptions work_options;
    work_pool_.reset(new byte::AsyncThreadPool());
    ASSERT_TRUE(work_pool_->Init(work_options));
    ASSERT_TRUE(work_pool_->Start());
    uri_ = "file://" + temp_dir_.GetDir() + "/cluster/public/partition";
    Partition::Options options;
    uint64_t max_mem_val = (uint64_t)3000000;  // 3M
    options.config.mutable_evicter_config()->mutable_maxmemory()->set_value(max_mem_val);
    options.config.mutable_evicter_config()->mutable_operation_type()->set_value(
        OperationType::DUMP);

    options.env = env_.get();
    options.uri = uri_;
    options.load_version = 1;
    std::unique_ptr<Partition> partition_;
    partition_.reset(new Partition(options));
    CoSyncClosure sync;
    work_pool_->KthThread(0)->Invoke(NewCoFuncClosure([&] {
        Status ret = partition_->Load();
        ASSERT_TRUE(ret.IsOK());
        sync.Run();
    }));
    sync.Wait();
    int key_size = 1600, key_count = 1600;
    SimplePartitionVerifier verifier(env_.get(), uri_, key_size, key_count, FLAGS_field_count,
                                     FLAGS_field_size, partition_.get());

    Bench::Options bench_options;
    bench_options.thread_pool = work_pool_.get();
    bench_options.jobs = 1;
    bench_options.verifier = &verifier;

    Bench bench;
    bench.Init(bench_options);
    std::string interval_water_level_str = std::to_string(FLAGS_interval_water_level);
    std::string hset_freq = "1/" + interval_water_level_str,
                hget_freq = "1/" + interval_water_level_str,
                set_freq = "1/" + interval_water_level_str,
                get_freq = "1/" + interval_water_level_str;
    if (!FLAGS_only_string_model) {
        bench.RegisterOp(SimplePartitionVerifier::kHSet, "HSet", hset_freq);
        bench.RegisterOp(SimplePartitionVerifier::kHGet, "HGet", hget_freq);
    }
    // bench.RegisterOp(SimplePartitionVerifier::kHDel, "HDel", FLAGS_depth_hdel);
    // bench.RegisterOp(SimplePartitionVerifier::kDelete, "Delete", FLAGS_depth_del);
    bench.RegisterOp(SimplePartitionVerifier::kSet, "Set", set_freq);
    bench.RegisterOp(SimplePartitionVerifier::kGet, "Get", get_freq);
    bench.Start();
    for (size_t i = 0; i < FLAGS_time; ++i) {
        size_t total_size = partition_->allocator_manager_->GetTotalAllocedSize();
        std::cout << "time: " << i << " seconds  "
                  << "used view mem: " << total_size << "   max mem size:" << max_mem_val
                  << " ratio: "
                  << static_cast<double>(total_size) / static_cast<double>(max_mem_val)
                  << std::endl;
        ASSERT_GT(1.2 * static_cast<double>(max_mem_val), static_cast<double>(total_size));
        CoSleep(1 * 1000 * 1000);
    }

    bench.Stop();
    partition_->Unload();
    work_pool_->Stop();
    background_pool_->Stop();
}

TEST(SimplePartitionBench, StoreLayerFaultInject) {
    RESTORE_FLAGS(FLAGS_stream_blob_switch_retry_interval_us);
    RESTORE_FLAGS(FLAGS_storage_zone_size);
    RESTORE_FLAGS(FLAGS_stream_max_blob_size);
    RESTORE_FLAGS(FLAGS_storage_gc_space_utility_threshold);
    RESTORE_FLAGS(FLAGS_storage_gc_max_bytes_per_round);
    RESTORE_FLAGS(FLAGS_storage_gc_max_slots_per_round);

    FLAGS_stream_blob_switch_retry_interval_us = 0;
    FLAGS_storage_zone_size = 2 * 1024 * 1024;
    FLAGS_stream_max_blob_size = 1024 * 1024;
    FLAGS_storage_gc_space_utility_threshold = 0.9;
    FLAGS_storage_gc_max_bytes_per_round = 1024 * 1024;
    FLAGS_storage_gc_max_slots_per_round = 100;

    brpc::StartDummyServerAt(FLAGS_brpc_server_port);

    bytestore_set_flag("bytestore_client_log_level", "1");
    bytestore_init();

    byte::AsyncThreadPoolOptions tp_options;
    tp_options.name_ = "test";
    byte::AsyncThreadPool background_pool;
    ASSERT_TRUE(background_pool.Init(tp_options));
    ASSERT_TRUE(background_pool.Start());

    stream::StoreLayer store_layer(&background_pool);

    stream::LogBasedEnv env;
    stream::LogBasedEnv::Options env_options;
    env_options.background_pool = &background_pool;
    env_options.store_layer = &store_layer;
    env.Init(env_options);

    byte::AsyncThreadPoolOptions work_options;
    work_options.name_ = "work";
    byte::AsyncThreadPool work_pool;
    ASSERT_TRUE(work_pool.Init(work_options));
    ASSERT_TRUE(work_pool.Start());

    TempDir temp_dir;
    std::string uri = "file://" + temp_dir.GetDir() + "/cluster/public/partition";
    if (FLAGS_partition_uri != "") {
        uri = FLAGS_partition_uri;
    }

    Partition::Options partition_options;
    partition_options.env = &env;
    partition_options.uri = uri;
    partition_options.load_version = 1;
    partition_options.persistent_type = PersistentType::PERSISTENT_SYNC;
    partition_options.config.mutable_evicter_config()->mutable_maxmemory()->set_value(1);
    partition_options.config.mutable_evicter_config()->mutable_operation_type()->set_value(
        OperationType::DUMP);
    std::unique_ptr<Partition> partition;
    partition.reset(new Partition(partition_options));

    CoSyncClosure sync;
    Status status;
    work_pool.KthThread(0)->Invoke(NewCoFuncClosure([&] {
        status = partition->Load();
        ASSERT_TRUE(status.IsOK());
        sync.Run();
    }));
    sync.Wait();

    BYTE_ASSERT(!fiu_enable_random("store/file/io/hang/*", 1, nullptr, 0, 0.01));       // 1%
    BYTE_ASSERT(!fiu_enable_random("store/file/io/fail/append", 1, nullptr, 0, 0.01));  // 1%
    BYTE_ASSERT(!fiu_enable_random("store/file/io/fail/read", 1, nullptr, 0, 0.005));   // 0.5%
    BYTE_ASSERT(!fiu_enable_random("store/file/ioctl/fail/*", 1, nullptr, 0, 0.1));     // 10%

    SimplePartitionVerifier verifier(&env, uri, FLAGS_key_count, FLAGS_key_size, FLAGS_field_count,
                                     FLAGS_field_size, partition.get());
    Bench::Options options;
    options.thread_pool = &work_pool;
    options.jobs = 1;
    options.verifier = &verifier;
    Bench bench;
    bench.Init(options);

    if (!FLAGS_only_string_model) {
        bench.RegisterOp(SimplePartitionVerifier::kHSet, "HSet", FLAGS_depth_hset);
        bench.RegisterOp(SimplePartitionVerifier::kHGet, "HGet", FLAGS_depth_hget);
        bench.RegisterOp(SimplePartitionVerifier::kHDel, "HDel", FLAGS_depth_hdel);
    }

    bench.RegisterOp(SimplePartitionVerifier::kDelete, "Delete", FLAGS_depth_del);
    bench.RegisterOp(SimplePartitionVerifier::kSet, "Set", FLAGS_depth_set);
    bench.RegisterOp(SimplePartitionVerifier::kGet, "Get", FLAGS_depth_get);
    bench.Start();
    for (size_t i = 0; i < FLAGS_time; ++i) {
        bench.ShowStats();
        CoSleep(1 * 1000 * 1000);
    }
    bench.Stop();

    partition->Unload();

    work_pool.Stop();
    background_pool.Stop();
    bytestore_shutdown();
}

int64_t GetDirectorySize(const char* dir) {
    DIR* dp;
    struct dirent* entry;
    struct stat statbuf;
    int64_t totalSize = 0;
    if ((dp = opendir(dir)) == NULL) {
        fprintf(stderr, "Cannot open dir: %s\n", dir);
        return -1;
    }
    lstat(dir, &statbuf);
    totalSize += statbuf.st_size;
    while ((entry = readdir(dp)) != NULL) {
        char subdir[256];
        snprintf(subdir, sizeof(subdir), "%s/%s", dir, entry->d_name);
        lstat(subdir, &statbuf);
        if (S_ISDIR(statbuf.st_mode)) {
            if (strcmp(".", entry->d_name) == 0 || strcmp("..", entry->d_name) == 0) {
                continue;
            }
            int64_t subDirSize = GetDirectorySize(subdir);
            totalSize += subDirSize;
        } else {
            totalSize += statbuf.st_size;
        }
    }
    closedir(dp);
    return totalSize;
}

struct stream_stats_info {
    uint64_t total_size;
    uint64_t blobs_num;
};

Status CheckStreamStatsInfo(const char* blob_dir, stream_stats_info* stream_info) {
    DIR* dp;
    struct dirent* entry;
    struct stat statbuf;
    int64_t totalSize = 0;
    if ((dp = opendir(blob_dir)) == NULL) {
        // fprintf(stderr, "Cannot open dir: %s\n", blob_dir);
        return Status::OK();
    }
    lstat(blob_dir, &statbuf);
    totalSize += statbuf.st_size;
    uint64_t blob_num = 0;
    while ((entry = readdir(dp)) != NULL) {
        blob_num += 1;
        char subdir[256];
        snprintf(subdir, sizeof(subdir), "%s/%s", blob_dir, entry->d_name);
        lstat(subdir, &statbuf);
        if (S_ISDIR(statbuf.st_mode)) {
            if (strcmp(".", entry->d_name) != 0 && strcmp("..", entry->d_name) != 0) {
                return Status::NotFound("unknown dir");
            }
            continue;
        } else {
            if (FLAGS_stream_max_blob_size < static_cast<uint64_t>(statbuf.st_size)) {
                return Status::Unmatched("blob to large");
            }
            totalSize += statbuf.st_size;
        }
    }
    closedir(dp);
    stream_info->total_size = totalSize;
    stream_info->blobs_num = blob_num;
    return Status::OK();
}

// uri = "file://" + temp_dir_string + "/cluster/public/partition"
Status CheckStorage(std::string uri) {
    std::size_t dir_start_index = uri.find("//");
    std::string dir_prefix = uri.substr(dir_start_index + 2, uri.size() - 1);
    stream_stats_info index_stream, oplog_stream;

    // index
    std::string index_stream_dir = dir_prefix + "-index";
    Status ret = CheckStreamStatsInfo(index_stream_dir.c_str(), &index_stream);
    if (!ret.ok()) return ret;

    // page
    std::string page_store_dir = dir_prefix + "-page";
    uint64_t total_page_zone_size = 0;
    int total_page_store_blob_num = 0;
    for (uint32_t i = 1; i < FLAGS_storage_max_zone_count + 100; i++) {
        stream_stats_info tmp_zone;

        std::string page_store_zone_dir = page_store_dir + std::to_string(i);
        ret = CheckStreamStatsInfo(page_store_zone_dir.c_str(), &tmp_zone);
        total_page_store_blob_num += tmp_zone.blobs_num;

        if (ret.ok()) {
            total_page_zone_size += tmp_zone.total_size;
        } else {
            return ret;
        }
    }

    if (total_page_store_blob_num > 12) {
        return Status::Unmatched("page store blobs too much");
    }

    std::string oplog_stream_dir = dir_prefix + "-oplog";
    ret = CheckStreamStatsInfo(oplog_stream_dir.c_str(), &oplog_stream);
    if (!ret.ok()) return ret;
    // check parameter : generated_key_num 30000

    if (oplog_stream.blobs_num > 12) {
        return Status::Unmatched("blobs too much");
    }
    std::cout << "index size:" << index_stream.total_size
              << ":: oplog size:" << oplog_stream.total_size
              << ":: page_store size: " << total_page_zone_size << std::endl;
    return Status::OK();
    // oplog
}

// page store check
TEST(WaterLevelPartitionBench, StorageWaterLevelTest) {
    RESTORE_FLAGS(FLAGS_bench_mode);
    RESTORE_FLAGS(FLAGS_storage_gc_zone_destroy_delay_ms);
    RESTORE_FLAGS(FLAGS_storage_zone_size);
    RESTORE_FLAGS(FLAGS_stream_max_blob_size);
    RESTORE_FLAGS(FLAGS_stream_blob_deletion_min_age);
    RESTORE_FLAGS(FLAGS_stream_blob_deletion_min_gap);

    FLAGS_stream_blob_deletion_min_age = 0;
    FLAGS_stream_blob_deletion_min_gap = 0;
    FLAGS_storage_gc_zone_destroy_delay_ms = 0;

    FLAGS_bench_mode = true;

    FLAGS_storage_zone_size = 6000000;
    FLAGS_stream_max_blob_size = 2000000;

    std::cout << "WaterLevelPatitionBench.StorageWaterLevelTest will cost about 10 mins"
              << std::endl;

    bytestore_set_flag("bytestore_client_log_level", "1");
    bytestore_init();
    byte::AsyncThreadPoolOptions tp_options;
    tp_options.name_ = "test";
    byte::AsyncThreadPool background_pool;
    ASSERT_TRUE(background_pool.Init(tp_options));
    ASSERT_TRUE(background_pool.Start());

    stream::StoreLayer store_layer(&background_pool);
    stream::LogBasedEnv env;
    stream::LogBasedEnv::Options env_options;
    env_options.background_pool = &background_pool;
    env_options.store_layer = &store_layer;
    env.Init(env_options);

    byte::AsyncThreadPoolOptions work_options;
    work_options.name_ = "work";
    byte::AsyncThreadPool work_pool;
    ASSERT_TRUE(work_pool.Init(work_options));
    ASSERT_TRUE(work_pool.Start());

    TempDir temp_dir;
    std::string temp_dir_string = temp_dir.GetDir();
    std::string uri = "file://" + temp_dir_string + "/cluster/public/partition";
    if (FLAGS_partition_uri != "") {
        uri = FLAGS_partition_uri;
    }

    Partition::Options partition_options;
    partition_options.env = &env;
    partition_options.uri = uri;
    partition_options.load_version = 1;
    partition_options.persistent_type = PersistentType::PERSISTENT_SYNC;

    std::unique_ptr<Partition> partition;
    partition.reset(new Partition(partition_options));

    CoSyncClosure sync;
    Status status;
    work_pool.KthThread(0)->Invoke(NewCoFuncClosure([&] {
        status = partition->Load();
        ASSERT_TRUE(status.IsOK());
        sync.Run();
    }));
    sync.Wait();
    int key_size = 160, key_count = 1600;
    SimplePartitionVerifier verifier(&env, uri, key_count, key_size, key_count, key_size,
                                     partition.get());
    Bench::Options options;
    options.thread_pool = &work_pool;
    options.jobs = 1;
    options.verifier = &verifier;
    Bench bench;
    bench.Init(options);
    if (!FLAGS_only_string_model)
        bench.RegisterOp(SimplePartitionVerifier::kHSet, "HSet", FLAGS_depth_hset);
    bench.RegisterOp(SimplePartitionVerifier::kSet, "Set", FLAGS_depth_set);
    // bench.RegisterOp(SimplePartitionVerifier::kHGet, "HGet", FLAGS_depth_hget);
    // bench.RegisterOp(SimplePartitionVerifier::kHDel, "HDel", FLAGS_depth_hdel);
    // bench.RegisterOp(SimplePartitionVerifier::kDelete, "Delete", FLAGS_depth_del);
    // bench.RegisterOp(SimplePartitionVerifier::kGet, "Get", FLAGS_depth_get);
    bench.Start();
    for (size_t i = 0; i < FLAGS_time; ++i) {
        size_t recorded_key_count = verifier.GetKeyCount();
        size_t user_view_data_size = recorded_key_count * key_size;
        int64_t sys_view_size = GetDirectorySize(temp_dir_string.c_str());
        double storage_ratio = static_cast<double>(user_view_data_size) / sys_view_size;
        CoSleep(1 * 1000 * 1000);
        if (i > 20) {
            Status ret = CheckStorage(uri);
            if (!ret.ok()) {
                std::cout << ret.ToString() << std::endl;
            }
            ASSERT_TRUE(ret.ok());
            std::cout << "time: " << i << " seconds  "
                      << "used view storage: " << user_view_data_size
                      << "   system view storage: " << sys_view_size << " ratio: " << storage_ratio
                      << std::endl;
            ASSERT_GT(storage_ratio, 0.10);
        }
    }
    bench.Stop();
    partition->Unload();
    work_pool.Stop();
    background_pool.Stop();
    bytestore_shutdown();
}

}  // namespace partition
}  // namespace bcache2
