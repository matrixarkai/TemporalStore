// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "test/common/kv_checker.h"

#include <byte/base/closure.h>
#include <byte/thread/async_thread.h>
#include <gtest/gtest.h>

#include <random>

#include "common/function_closure.h"
#include "common/scoped_invoker.h"
#include "test/common/bench.h"

namespace bcache2 {

class KvCheckTest : public testing::Test {
 public:
    void SetUp() {
        uint64_t value = 0;
        uint64_t write1 = checker_.NewWrite(&value);
        ASSERT_EQ(1, value);
        checker_.FinishWrite(write1, true);

        uint64_t read1 = checker_.NewRead();
        ASSERT_TRUE(checker_.FinishRead(read1, true, value));
    }
    void TearDown() {}

 protected:
    KvChecker checker_;
};

TEST_F(KvCheckTest, Simple) {
    uint64_t value = 0;
    uint64_t write1 = checker_.NewWrite(&value);
    ASSERT_EQ(2, value);
    checker_.FinishWrite(write1, true);

    uint64_t read1 = checker_.NewRead();
    ASSERT_FALSE(checker_.FinishRead(read1, true, 1));

    uint64_t read2 = checker_.NewRead();
    ASSERT_FALSE(checker_.FinishRead(read2, true, 3));

    uint64_t read3 = checker_.NewRead();
    ASSERT_TRUE(checker_.FinishRead(read3, true, 2));
}

TEST_F(KvCheckTest, FixedWrite) {
    uint64_t value = 0;
    uint64_t write1 = checker_.NewWrite(&value);
    ASSERT_EQ(2, value);
    uint64_t write2 = checker_.NewWrite(&value);
    ASSERT_EQ(3, value);
    uint64_t write3 = checker_.NewWrite(&value);
    ASSERT_EQ(4, value);

    checker_.FinishWrite(write1, true);
    checker_.FinishWrite(write2, true);
    checker_.FinishWrite(write3, true);

    uint64_t write4 = checker_.NewWrite(&value);
    ASSERT_EQ(5, value);
    checker_.FinishWrite(write4, true);

    uint64_t read1 = checker_.NewRead();
    ASSERT_FALSE(checker_.FinishRead(read1, true, 2));

    uint64_t read2 = checker_.NewRead();
    ASSERT_FALSE(checker_.FinishRead(read2, true, 3));

    uint64_t read3 = checker_.NewRead();
    ASSERT_FALSE(checker_.FinishRead(read3, true, 4));

    uint64_t read4 = checker_.NewRead();
    ASSERT_TRUE(checker_.FinishRead(read4, true, 5));
}

TEST_F(KvCheckTest, FixedRead) {
    uint64_t value = 0;
    uint64_t write1 = checker_.NewWrite(&value);
    ASSERT_EQ(2, value);
    uint64_t write2 = checker_.NewWrite(&value);
    ASSERT_EQ(3, value);
    uint64_t write3 = checker_.NewWrite(&value);
    ASSERT_EQ(4, value);

    checker_.FinishWrite(write1, true);
    checker_.FinishWrite(write2, true);
    checker_.FinishWrite(write3, true);

    uint64_t read = checker_.NewRead();
    ASSERT_TRUE(checker_.FinishRead(read, true, 3));

    uint64_t read1 = checker_.NewRead();
    ASSERT_FALSE(checker_.FinishRead(read1, true, 2));

    uint64_t read2 = checker_.NewRead();
    ASSERT_FALSE(checker_.FinishRead(read2, true, 4));

    uint64_t read3 = checker_.NewRead();
    ASSERT_TRUE(checker_.FinishRead(read3, true, 3));
}

class SimpleStore {
 public:
    void Write(Controller* ctrl, uint64_t value, Closure<void>* callback) {
        auto func = [this, ctrl, value, callback] {
            value_ = value;
            byte::InvokeLaterInCurrentThread(rd_() % 1000, callback);
            ctrl->set_status(rd_() % 100 == 0 ? Status::Unknown("") : Status::OK());
        };
        byte::InvokeLaterInCurrentThread(rd_() % 1000, NewFuncClosure(func));
    }

    void Read(Controller* ctrl, uint64_t* value, Closure<void>* callback) {
        auto func = [this, ctrl, value, callback] {
            *value = value_;
            byte::InvokeLaterInCurrentThread(rd_() % 1000, callback);
            ctrl->set_status(rd_() % 100 == 0 ? Status::Unknown("") : Status::OK());
            if (*value == 0) {
                ctrl->set_status(Status::NotFound(""));
            }
        };
        byte::InvokeLaterInCurrentThread(rd_() % 1000, NewFuncClosure(func));
    }

 private:
    uint64_t value_ = 0;
    std::random_device rd_;
};

class SimpleValueVerifier : public Verifier {
 public:
    enum OpType {
        kWrite,
        kRead,
    };

    Operation* GeneratorOp(int op_type) override {
        switch (op_type) {
        case kWrite:
            return new WriteOp(this);
        case kRead:
            return new ReadOp(this);
        default:
            return nullptr;
        }
    }
    void FinishOp(Operation* op) override { delete op; }

 private:
    class WriteOp : public Operation {
     public:
        WriteOp(SimpleValueVerifier* verifier) : verifier_(verifier) {}
        void Run(Controller* ctrl, Closure<void>* callback) override {
            uint64_t value = 0;
            handle_ = verifier_->checker_.NewWrite(&value);
            verifier_->store_.Write(ctrl, value,
                                    NewClosure(this, &WriteOp::OnRunDone, ctrl, callback));
        }

     private:
        void OnRunDone(Controller* ctrl, Closure<void>* callback) {
            ScopedCallback done(callback);
            verifier_->checker_.FinishWrite(handle_, ctrl->status().ok());
        }

        SimpleValueVerifier* verifier_ = nullptr;
        uint64_t handle_ = 0;
    };

    class ReadOp : public Operation {
     public:
        ReadOp(SimpleValueVerifier* verifier) : verifier_(verifier) {}
        void Run(Controller* ctrl, Closure<void>* callback) override {
            handle_ = verifier_->checker_.NewRead();
            verifier_->store_.Read(ctrl, &value_,
                                   NewClosure(this, &ReadOp::OnRunDone, ctrl, callback));
        }

     private:
        void OnRunDone(Controller* ctrl, Closure<void>* callback) {
            ScopedCallback done(callback);
            bool ok = verifier_->checker_.FinishRead(handle_, ctrl->status().ok(), value_);
            BYTE_ASSERT(ok);
        }

        SimpleValueVerifier* verifier_ = nullptr;
        uint64_t handle_ = 0;
        uint64_t value_ = 0;
    };

    SimpleStore store_;
    KvChecker checker_;
};

TEST(ValueTest, Smoketest) {
    byte::AsyncThreadPoolOptions work_options;
    byte::AsyncThreadPool work_pool;
    ASSERT_TRUE(work_pool.Init(work_options));
    ASSERT_TRUE(work_pool.Start());

    SimpleValueVerifier verifier;

    Bench::Options options;
    options.thread_pool = &work_pool;
    options.jobs = 1;
    options.verifier = &verifier;
    Bench bench;
    bench.Init(options);

    bench.RegisterOp(SimpleValueVerifier::kWrite, "Write", "16");
    bench.RegisterOp(SimpleValueVerifier::kRead, "Read", "16");

    bench.Start();
    for (size_t i = 0; i < 60; ++i) {
        bench.ShowStats();
        sleep(1);
    }
    bench.Stop();
}

}  // namespace bcache2
