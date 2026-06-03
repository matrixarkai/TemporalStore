// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <absl/strings/numbers.h>
#include <byte/include/macros.h>
#include <gtest/gtest.h>

#include <cassert>
#include <cstdint>
#include <random>
#include <string>

#include "bytestore/bytestore.h"
#include "client/client.h"
#include "client/client_impl.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "concurrent/scoped_locker.h"
#include "concurrent/spinlock.h"
#include "gflags/gflags.h"
#include "protocol/master.pb.h"
#include "protocol/server.pb.h"
#include "server/server.h"
#include "test/common/bench.h"
#include "test/common/kv_checker.h"
#include "test/common/temp_dir.h"
#include "test/mini_cluster/mini_cluster.h"
#include "thread/async_thread.h"

DEFINE_string(end_point, "127.0.0.1", "end_point");
DEFINE_string(table_namespace, "consistency_bench_ns", "table namespace");
DEFINE_string(table_name, "consistency_bench", "table name");
DEFINE_string(depth_hset, "1", "depth for hset");
DEFINE_string(depth_hget, "1", "depth for hget");
DEFINE_uint32(time, 5, "run time in seconds");
DEFINE_bool(setup_server, true, "setup server");

namespace bcache2 {
namespace consistent {

// Consistent Verifier
class ConsistentVerifier : public Verifier {
 public:
    enum OpType {
        kHSet,
        kHGet,
    };
    ConsistentVerifier(const std::string& master_addr, const std::string& table_ns,
                       const std::string& table, const size_t key_count, const size_t key_size,
                       const size_t field_count, const size_t field_size)
        : key_count_(key_count),
          key_size_(key_size),
          field_count_(field_count),
          field_size_(field_size) {
        // init client
        InitClient(master_addr, table_ns, table);

        // init checker
        keys_.resize(key_count_);
        fields_.resize(field_count_);
        for (size_t i = 0; i < keys_.size(); ++i) {
            for (size_t j = 0; j < key_size_; ++j) {
                keys_[i].push_back(rd_() % 26 + 'A');
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
        }
    }

    Operation* GeneratorOp(int op_type) override;

    void FinishOp(Operation* op) override { delete op; };
    std::unique_ptr<client::Table> table_;
    std::unique_ptr<client::Client> client_;

 private:
    void InitClient(std::string master_addr, std::string table_ns, std::string table) {
        client::ClientOptions options;
        Status status;
        options.master_addr = master_addr;
        client::Client* temp_client;
        status = client::Client::Create(options, &temp_client);
        BYTE_ASSERT_TRUE(status.ok());
        client_.reset(temp_client);

        client::TableOptions table_options;
        client::Table* temp_table;
        status = client_->OpenTable(table_ns, table, table_options, &temp_table);
        BYTE_ASSERT_TRUE(status.ok());
        table_.reset(temp_table);
    }

    void RandKeyAndField(std::string* key, std::string* field) {
        *key = keys_[rd_() % keys_.size()];
        *field = fields_[rd_() % fields_.size()];
    }

    class HSetOp;
    class HGetOp;

    std::vector<std::string> keys_;
    std::vector<std::string> fields_;
    size_t key_count_ = 0;
    size_t key_size_ = 0;
    size_t field_count_ = 0;
    size_t field_size_ = 0;

    std::map<std::string, std::unique_ptr<KvChecker>> checker_map_;

    std::random_device rd_;

    DISALLOW_COPY_AND_ASSIGN(ConsistentVerifier);
};

class ConsistentVerifier::HSetOp : public Operation {
 public:
    HSetOp(ConsistentVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (verifier_->table_.get() == nullptr) {
            ScopedCallback done(callback);
            return;
        }
        uint64_t val = 0;
        std::string key;
        std::string field;
        verifier_->RandKeyAndField(&key, &field);
        checker_ = verifier_->checker_map_[key + field].get();
        handle_ = checker_->NewWrite(&val);

        Status status = verifier_->table_->HSet(key, field, std::to_string(val));
        ctrl->set_status(status);
        OnRunDone(ctrl, callback);
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        checker_->FinishWrite(handle_, ctrl->status().ok());
    }

    ConsistentVerifier* verifier_ = nullptr;
    KvChecker* checker_ = nullptr;
    uint64_t handle_;
    DISALLOW_COPY_AND_ASSIGN(HSetOp);
};

class ConsistentVerifier::HGetOp : public Operation {
 public:
    HGetOp(ConsistentVerifier* verifier) : verifier_(verifier) {}
    void Run(Controller* ctrl, Closure<void>* callback) override {
        if (verifier_->table_.get() == nullptr) {
            ScopedCallback done(callback);
            return;
        }
        std::string key;
        std::string field;

        verifier_->RandKeyAndField(&key, &field);
        checker_ = verifier_->checker_map_[key + field].get();
        handle_ = checker_->NewRead();
        Status status = verifier_->table_->HGet(key, field, &data_);
        ctrl->set_status(status);
        OnRunDone(ctrl, callback);
    }

 private:
    void OnRunDone(Controller* ctrl, Closure<void>* callback) {
        ScopedCallback done(callback);
        if (!ctrl->status().ok() || NotFound(data_)) {
            bool ok = checker_->FinishRead(handle_, false, 0);
            BYTE_ASSERT(ok);
            return;
        }
        uint64_t value = 0;

        BYTE_ASSERT(absl::SimpleAtoi(data_, &value));
        bool ok = checker_->FinishRead(handle_, true, value);
        BYTE_ASSERT(ok);
    }

    inline bool NotFound(const std::string& value) {
        // fixme(clf): checking NotFound by status
        return value.empty();
    }
    ConsistentVerifier* verifier_ = nullptr;
    KvChecker* checker_ = nullptr;
    uint64_t handle_;
    std::string data_;
    DISALLOW_COPY_AND_ASSIGN(HGetOp);
};

Operation* ConsistentVerifier::GeneratorOp(int op_type) {
    switch (op_type) {
    case kHSet:
        return dynamic_cast<Operation*>(new HSetOp(this));
    case kHGet:
        return dynamic_cast<Operation*>(new HGetOp(this));
    default:
        return nullptr;
    }
}

class BaseConsistencyTest : public ::testing::Test {
 public:
    BaseConsistencyTest() {}
    virtual ~BaseConsistencyTest() {}

    void SetUp() override {
        FLAGS_enable_blockcache = true;
        FLAGS_blockcache_dram_capacity = 134217728;  // 128 MB
        FLAGS_blockcache_ssd_capacity = 134217728;   // 128 MB

        bytestore_init();

        if (FLAGS_setup_server) {
            // init bcache2 mini cluster
            LOG_INFO("BaseConsistencyTest setup local mini cluster")
                .put("FLAGS setup server", FLAGS_setup_server);
            MiniCluster::Options options;
            options.server_count = 1;
            options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";

            cluster_.Init(options);
            Status status = cluster_.Start();
            ASSERT_TRUE(status.ok()) << status;

            CoSleep(5000000);  // Sleep 5s

            MasteWrapper* master = cluster_.GetMaster();
            status = master->CreateSimpleTable(FLAGS_table_namespace, FLAGS_table_name, 1, 1);
            ASSERT_TRUE(status.ok()) << status;
        }

        MasteWrapper* master = cluster_.GetMaster();
        ASSERT_TRUE(master != nullptr);
        // init consistency verifier
        std::string&& cluster_addr =
            FLAGS_end_point + ":" + std::to_string(master->GetMasterPort());
        ConsistentVerifier* verifier = new ConsistentVerifier(cluster_addr, FLAGS_table_namespace,
                                                              FLAGS_table_name, 10, 10, 10, 10);
        verifier_.reset(verifier);
    }

    void TearDown() override {
        cluster_.Stop();
        bytestore_shutdown();
    }

 protected:
    TempDir temp_dir_;
    MiniCluster cluster_;

    std::unique_ptr<ConsistentVerifier> verifier_;

    DISALLOW_COPY_AND_ASSIGN(BaseConsistencyTest);
};

TEST_F(BaseConsistencyTest, Base) {
    Status status;
    std::string val;
    status = verifier_->table_->HGet("foo", "bar", &val);
    BYTE_ASSERT(!status.ok());
    status = verifier_->table_->HSet("foo", "bar", "1");
    BYTE_ASSERT(status.ok());
    status = verifier_->table_->HGet("foo", "bar", &val);
    BYTE_ASSERT(status.ok());
    BYTE_ASSERT_EQ(val, "1");
}

TEST_F(BaseConsistencyTest, Basic) {
    const int job_num = 1;
    std::unique_ptr<byte::AsyncThreadPool> work_pool;
    std::unique_ptr<byte::AsyncThreadPool> background_pool;
    byte::AsyncThreadPoolOptions work_options;
    work_options.thread_num_ = job_num;
    work_pool.reset(new byte::AsyncThreadPool());
    ASSERT_TRUE(work_pool->Init(work_options));
    ASSERT_TRUE(work_pool->Start());

    Bench::Options options;
    options.thread_pool = work_pool.get();
    options.jobs = work_pool->ThreadNum();
    options.verifier = verifier_.get();
    Bench bench;
    bench.Init(options);
    bench.RegisterOp(ConsistentVerifier::kHGet, "HGet", FLAGS_depth_hget);
    bench.RegisterOp(ConsistentVerifier::kHSet, "HSet", FLAGS_depth_hset);
    bench.Start();

    // start bench
    for (size_t i = 0; i < FLAGS_time; ++i) {
        bench.ShowStats();
        sleep(1);
    }

    bench.Stop();
}

}  // namespace consistent
}  // namespace bcache2
