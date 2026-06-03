// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gtest/gtest.h>
#include <unistd.h>

#include <climits>
#include <cstdint>
#include <memory>
#include <random>

#include "bench/consistency_checker.h"
#include "common/status.h"
#include "extension/hash/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/string/interface.pb.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {
namespace test {

static std::random_device rd;
static std::mt19937 rng(rd());

static Operation GenOperation(uint16_t module_id, uint16_t function_id, uint64_t start_time,
                              uint64_t end_time, std::string key, std::string value,
                              uint64_t ttl_ms = 0, bcache2::Code code = bcache2::Code::kOK) {
    Operation operation;
    operation.set_module_id(module_id);
    operation.set_function_id(function_id);
    operation.set_key(key);
    operation.set_start_time_us(start_time);
    operation.set_end_time_us(end_time);
    operation.set_code(code);

    if (module_id == Module::STRING) {
        switch (function_id) {
        case str2::Function::GET: {
            str2::GetRequest req;
            str2::GetResponse resp;
            req.set_key(key);
            resp.set_value(value);
            req.SerializeToString(operation.mutable_request_bytes());
            resp.SerializeToString(operation.mutable_response_bytes());
            break;
        }
        case str2::Function::SET: {
            str2::SetRequest req;
            str2::SetResponse resp;
            req.set_key(key);
            req.set_value(value);
            req.SerializeToString(operation.mutable_request_bytes());
            resp.SerializeToString(operation.mutable_response_bytes());
            break;
        }
        case str2::Function::SETEX: {
            str2::SetexRequest req;
            str2::SetexResponse resp;
            req.set_key(key);
            req.set_value(value);
            req.set_ttl_ms(ttl_ms);
            req.SerializeToString(operation.mutable_request_bytes());
            resp.SerializeToString(operation.mutable_response_bytes());
            break;
        }
        }
    }

    if (module_id == Module::COMMON) {
        switch (function_id) {
        case common2::Function::DEL_OBJECT: {
            common2::DelObjectRequest req;
            common2::DelObjectResponse resp;
            req.set_key(key);
            req.SerializeToString(operation.mutable_request_bytes());
            resp.SerializeToString(operation.mutable_response_bytes());
            break;
        }
        case common2::Function::EXPIRE: {
            common2::ExpireRequest req;
            common2::ExpireResponse resp;
            req.set_key(key);
            req.set_ttl_ms(ttl_ms);
            req.SerializeToString(operation.mutable_request_bytes());
            resp.SerializeToString(operation.mutable_response_bytes());
            break;
        }
        case common2::Function::TTL: {
            common2::TtlRequest req;
            common2::TtlResponse resp;
            req.set_key(key);
            resp.set_ttl_ms(ttl_ms);
            req.SerializeToString(operation.mutable_request_bytes());
            resp.SerializeToString(operation.mutable_response_bytes());
            break;
        }
        }
    }

    return operation;
}

static Operation GenHashOperation(uint16_t function_id, uint64_t start_time, uint64_t end_time,
                                  std::string key, std::string field, std::string value,
                                  bool exist = true, bcache2::Code code = bcache2::Code::kOK) {
    Operation operation;
    operation.set_module_id(Module::HASH);
    operation.set_function_id(function_id);
    operation.set_key(key);
    operation.set_start_time_us(start_time);
    operation.set_end_time_us(end_time);
    operation.set_code(code);

    switch (function_id) {
    case hash2::Function::SET: {
        hash2::SetRequest req;
        hash2::SetResponse resp;
        req.set_key(key);
        req.set_field(field);
        req.set_value(value);
        req.SerializeToString(operation.mutable_request_bytes());
        resp.SerializeToString(operation.mutable_response_bytes());
        break;
    }
    case hash2::Function::GET: {
        hash2::GetRequest req;
        hash2::GetResponse resp;
        req.set_key(key);
        req.set_field(field);
        resp.set_value(value);
        resp.set_exist(exist);
        req.SerializeToString(operation.mutable_request_bytes());
        resp.SerializeToString(operation.mutable_response_bytes());
        break;
    }
    case hash2::Function::DEL: {
        hash2::DelRequest req;
        hash2::DelResponse resp;
        req.set_key(key);
        req.set_field(field);
        req.SerializeToString(operation.mutable_request_bytes());
        resp.SerializeToString(operation.mutable_response_bytes());
        break;
    }
    }

    return operation;
}

//                   Get 3
//  ------------------------------------------->
//                   Set 3
//    -------------------------->
//     Set 1         Get 1
// ------------->  ---------->
//                 Set 1
//            ------------->
//          Set 2
//       ----------->     Get 2
//                     ----------->
// Expect: Set 1 -> Get 1 -> Set 1 -> Set 2 -> Get 2 -> Set 3 -> Get 3
TEST(ConsistencyChecker, ReadWrite) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 11, 34, "key", "3"),
        GenOperation(Module::STRING, str2::Function::SET, 12, 40, "key", "3"),
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 22, 30, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 21, 28, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 15, 25, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 27, 35, "key", "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//                   Get 3
//  ------------------------------------------->
//                   Set 3
//    -------------------------->
//     Set 1         Get 1
// ------------->  ---------->
//                 Del
//            ------------->
//          Set 2
//       ----------->     Get 2
//                     ----------->
// Expect: Set 1 -> Get 1 -> Del -> Set 2 -> Get 2 -> Set 3 -> Get 3
TEST(ConsistencyChecker, ReadWriteDel) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 11, 34, "key", "3"),
        GenOperation(Module::STRING, str2::Function::SET, 12, 40, "key", "3"),
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 22, 30, "key", "1"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 21, 28, "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, 15, 25, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 27, 35, "key", "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, AmbiguousValue1) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 100;
    opts.max_expire_ambiguous_time_ms = 100;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 12, 40, "key", "1", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::SET, 23, 34, "key", "2", 0),
        GenOperation(Module::STRING, str2::Function::GET, UINT64_MAX - 1, UINT64_MAX, "key", "2",
                     0),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, AmbiguousValue2) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 100;
    opts.max_expire_ambiguous_time_ms = 100;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 12, 40, "key", "1", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::SET,
                     40 + opts.max_ambiguous_time_ms / 2 * 1000,
                     40 + opts.max_ambiguous_time_ms * 1000, "key", "2", 0),
        GenOperation(Module::STRING, str2::Function::GET, UINT64_MAX - 1, UINT64_MAX, "key", "1",
                     0),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, AmbiguousValue3) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 100;
    opts.max_expire_ambiguous_time_ms = 100;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 12, 40, "key", "1", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::SET,
                     40 + opts.max_ambiguous_time_ms * 2 * 1000,
                     40 + opts.max_ambiguous_time_ms * 3 * 1000, "key", "2", 0),
        GenOperation(Module::STRING, str2::Function::GET, UINT64_MAX - 1, UINT64_MAX, "key", "1",
                     0),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//           Get 3
//  ---------------------->
//             Get 1
//    -------------------------->
//     Set 2         Get 1
// ------------->  ---------->
//                 Del
//            ------------->
//          Set 3
//       ----------->     Get 2
//                     ----------->
TEST(ConsistencyChecker, CheckFailed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 100;
    opts.max_expire_ambiguous_time_ms = 100;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 10, 30, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 11, 40, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 9, 18, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 22, 35, "key", "1"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 16, 34, "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, 15, 24, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 23, 41, "key", "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//           Set 1
//  ------------------------------->
//       Get 1     Get 0
//    -------->  --------->
TEST(ConsistencyChecker, CheckFailed2) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 1000, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 11, 40, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 50, 100, "key", "0"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, OperationCycle) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 10, 100, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 200, 300, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 400, 500, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 600, 0, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 501, 1000, "key", ""),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, Sequence) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 200, 300, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 400, 500, "key", "1"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 600, 700, "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, 800, 900, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 1000, 1100, "key", "2"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 1200, 1300, "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, 1400, 1500, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 1600, 1700, "key", "3"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 1800, 1900, "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, 2000, 2100, "key", "4"),
        GenOperation(Module::STRING, str2::Function::GET, 2200, 2300, "key", "4"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 2400, 2500, "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, 2600, 2700, "key", "5"),
        GenOperation(Module::STRING, str2::Function::GET, 2800, 2900, "key", "5"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 3000, 3100, "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, 3200, 3300, "key", "6"),
        GenOperation(Module::STRING, str2::Function::GET, 3400, 3500, "key", "6"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 3600, 3700, "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, 3800, 3900, "key", "7"),
        GenOperation(Module::STRING, str2::Function::GET, 4000, 4100, "key", "7"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    SET 1
// ---------->
//       SET 2
// ---------------------->
//     GET 1
// -------------->
//                    GET 2
//                  ---------->
//                                   GET 2
//                                ----------->
TEST(ConsistencyChecker, Success) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 100, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 10, 150, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 10, 130, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 140, 300, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 300, 400, "key", "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    SET 1
// ---------->
//       SET 2
// ---------------------->
//     GET 1
// -------------->
//                    GET 2
//                  ---------->
//                                   GET 1
//                                ----------->
TEST(ConsistencyChecker, Failed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 100, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 10, 150, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 10, 130, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 140, 300, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 310, 400, "key", "1"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//   Set 1
// -------->
//              Set 2 (timeout)
//            ------------------>
//                                  Set 3 (timeout)     Get 10(failed)
//                                 ----------------->  ------------>
//                                                                    Get 1        Get 1
//                                                                   --------->  -------->
//
// Set 1 -> Set 2 -> Set 3-> Get 1 -> Get 1
TEST(ConsistencyChecker, AmbiguousWriteSuccess) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 10, 200, "key", "2", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::SET, 210, 300, "key", "3", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 310, 320, "key", "10", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 330, 350, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 360, 410, "key", "1"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//   Set 1
// -------->
//              Set 2 (timeout)         Set 2  max end_time |
//            ---------------------->|------------------------>
//                    Get 1       Get 1                                        Get 1          Get 2
//                 --------->  ----------->                                  ----------> ------->
//
TEST(ConsistencyChecker, AmbiguousWriteTimeout) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 1;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 30, 200, "key", "2", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 310, 320, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 330, 350, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 1000 + 160, 1000 + 410, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 1000 + 160, 1000 + 410, "key", "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//   Set 1
// -------->
//              Set 2 (timeout)         Set 2  max end_time |
//            ---------------------->|------------------------>
//                    Get 1       Get 1                                        Get 1          Get 2
//                 --------->  ----------->                                  ----------> ------->
//
TEST(ConsistencyChecker, AmbiguousWriteTimeoutFailed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 1;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 30, 200, "key", "2", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 310, 320, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 330, 350, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 1000 + 360, 1000 + 410, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 1000 + 360, 1000 + 410, "key", "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//   Set 1
// -------->
//              Set 2 (timeout)
//            ------------------>
//                               Get 2        Get 1
//                            ----------->  -------->
//
TEST(ConsistencyChecker, AmbiguousWriteFailed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 1;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::SET, 10, 200, "key", "2", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 180, 310, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 320, 410, "key", "1"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//   Set 1
// -------->
//             Get 100 (Failed)                           Set 2
//            ----------------->                        ------------------>
//                                    Get 10 (Failed)
//                                  ---------------->                    Get 1          Get 1
//                                                                    ---------------->  -------->
//
// Set 1 -> Get 100 (Failed) -> Get 10 (Failed) -> Set 2 -> Get 2 -> Get 2
TEST(ConsistencyChecker, AbnormalRead) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 10, 200, "key", "100", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 10, 200, "key", "10", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::SET, 210, 300, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 310, 350, "key", "2"),
        GenOperation(Module::STRING, str2::Function::GET, 360, 410, "key", "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    SETEX 1
// ---------->
//            GET 2 (Failed)
//     -------------------->
//               GET 1
//           -------------->
//                               GET 1
//                             ---------->
//                                                         GET 1(Failed)
//                                                      ----------->
TEST(ConsistencyChecker, SetexSuccess) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SETEX, 10, 20, "key", "1", 1000),
        GenOperation(Module::STRING, str2::Function::GET, 20, 40, "key", "2", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 30, 40, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 1 * 1e6, 1 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 3 * 1e6, 3 * 1e6 + 50, "key", "", 0,
                     kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    SET 1
// ---------->
//    EXPIRE 1,1s
// ---------->
//            GET 2 (Failed)
//     -------------------->
//               GET 1
//           -------------->
//                               GET 1
//                             ---------->
//                                                         GET 1(Failed)
//                                                      ----------->
TEST(ConsistencyChecker, ExpireSuccess) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::COMMON, common2::Function::EXPIRE, 20, 40, "key", "", 1000),
        GenOperation(Module::STRING, str2::Function::GET, 20, 40, "key", "2", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 30, 40, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 1 * 1e6, 1 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 3 * 1e6, 3 * 1e6 + 50, "key", "", 0,
                     kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    SETEX 1
// ---------->
//            GET 2 (Failed)
//     -------------------->
//               GET 1
//           -------------->
//                               GET 1
//                             ---------->
//                                                         GET 1(Failed)
//                                                      ----------->
TEST(ConsistencyChecker, Expire2Success) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SETEX, 10, 20, "key", "1", 1000),
        GenOperation(Module::STRING, str2::Function::GET, 20, 40, "key", "2", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 30, 40, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 1 * 1e6, 1 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 3 * 1e6, 3 * 1e6 + 50, "key", "", 0,
                     kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//  SETEX 1,2s
// -------->
//       GET 1
//     ---------->
//               TTL 1s
//           ---------->
//                           1s,GET 1
//                         ---------->
//                                        2s,GET 1
//                                      ---------->
//                                        2s,GET 1(NotFound)
//                                      ---------->
//                                                         3s,GET 1(Failed)
//                                                      ----------->
//                                                         3s,GET 1(NotFound)
//                                                      ----------->
TEST(ConsistencyChecker, TtlSuccess) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SETEX, 10, 20, "key", "1", 2 * 1000),
        GenOperation(Module::STRING, str2::Function::GET, 20, 40, "key", "1"),
        GenOperation(Module::COMMON, common2::Function::TTL, 1030, 1050, "key", "1", 1999),
        GenOperation(Module::COMMON, common2::Function::TTL, 2030, 2050, "key", "1", 1998),
        GenOperation(Module::STRING, str2::Function::GET, 1 * 1e6, 1 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 2 * 1e6, 2 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 2 * 1e6, 2 * 1e6 + 50, "key", "1", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 3 * 1e6, 3 * 1e6 + 50, "key", "1", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 3 * 1e6, 3 * 1e6 + 50, "key", "1", 0,
                     kInternal),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    SETEX 1,2s
// ---------->
//       GET 1
//     ---------->
//          TTL 0s
//        ------------>
//                 DEL(Failed)
//               ------------>
//                       GET 1
//                     ------------>
//                              DEL
//                            ------------>
//                                    GET 1(NotFound)
//                                  ------------>
TEST(ConsistencyChecker, DeleteSuccess) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 20, 40, "key", "1"),
        GenOperation(Module::COMMON, common2::Function::TTL, 30, 40, "key", "1", 0),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 30, 50, "key", "1", 0,
                     kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 40, 60, "key", "1"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 50, 70, "key", ""),
        GenOperation(Module::STRING, str2::Function::GET, 60, 80, "key", "1", 0, kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    SETEX 1,2s
// ---------->
//       GET 1
//     ---------->
//          TTL 0s
//        ------------>
//                 DEL(Failed)
//               ------------>
//                       GET 1(NotFound)
//                     ------------>
//                              DEL(NotFound)
//                            ------------>
//                                    GET 1(NotFound)
//                                  ------------>
TEST(ConsistencyChecker, DeleteSuccess2) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, 10, 20, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, 20, 40, "key", "1"),
        GenOperation(Module::COMMON, common2::Function::TTL, 30, 40, "key", "1", 0),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 30, 50, "key", "1", 0,
                     kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 40, 60, "key", "1", 0, kNotFound),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 50, 70, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 60, 80, "key", "1", 0, kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    TTL 1 (NotFound)
// ---------->
//    SETEX 1,1s
// ---------->
//                TTL 0s
//              ------------>
//                TTL 1s
//              ------------>
//                               TTL 0s
//                             ------------>
//                                              TTL (NotFound)
//                                            ------------>
TEST(ConsistencyChecker, TtlCriticalValue) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::COMMON, common2::Function::TTL, 10, 20, "key", "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::SETEX, 10, 20, "key", "1", 2 * 1000),
        GenOperation(Module::COMMON, common2::Function::TTL, 1e6 + 10, 1e6 + 20, "key", "1", 1000),
        GenOperation(Module::COMMON, common2::Function::TTL, 3 * 1e6 + 10, 3 * 1e6 + 20, "key", "1",
                     0, kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//    TTL 1 (NotFound)
// ---------->
//    SETEX 1,2s
// ---------->
//         TTL 2s
//       ------------>
TEST(ConsistencyChecker, TtlFailed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::COMMON, common2::Function::TTL, 10, 20, "key", "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::SETEX, 10, 20, "key", "1", 2000),
        GenOperation(Module::COMMON, common2::Function::TTL, 10, 20, "key", "1", 501),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, ProductCase) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 1000;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 7817748846, 7817992283, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7818168125, 7818398637, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7818371123, 7818578689, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7818546912, 7818692416, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::SET, 7818745473, 7818963456, "key", "0", 0,
                     kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 7818875143, 7819056647, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7818957242, 7819131126, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7818996581, 7819233842, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7819421471, 7819550168, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7820773760, 7820970977, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7820817175, 7821005447, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::SET, 7821118591, 7821333264, "key", "1", 0,
                     kInternal),
        GenOperation(Module::STRING, str2::Function::SET, 7822132499, 7822314345, "key", "2", 0,
                     kInternal),
        GenOperation(Module::STRING, str2::Function::GET, 7823788111, 7823970246, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7824252046, 7824431479, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7824333713, 7824514841, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7824450965, 7824628252, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7824922515, 7825096243, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7825629635, 7825789909, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7825823552, 7826018020, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7825987817, 7826170973, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7826477573, 7826636264, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7826781145, 7826974691, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7826997938, 7827183724, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7827049607, 7827214879, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7827302964, 7827467731, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 7827384969, 7827546551, "key", "", 0,
                     kNotFound),
        GenOperation(Module::STRING, str2::Function::SET, 7827856020, 7828028713, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 7827974660, 7828123749, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 7828307045, 7828473575, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 7828320467, 7828500311, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 7828519273, 7828687851, "key", "3"),
        GenOperation(Module::STRING, str2::Function::GET, 7828530505, 7828699155, "key", "3"),
        GenOperation(Module::STRING, str2::Function::SET, 7829662273, 7829888978, "key", "4"),
        GenOperation(Module::STRING, str2::Function::GET, 7829678488, 7829862219, "key", "4"),
        GenOperation(Module::STRING, str2::Function::GET, 7830363600, 7830554329, "key", "4"),
        GenOperation(Module::STRING, str2::Function::SET, 7830447173, 7830629104, "key", "5"),
        GenOperation(Module::STRING, str2::Function::GET, 7830497127, 7830697891, "key", "5"),
        GenOperation(Module::STRING, str2::Function::GET, 7830833955, 7831017294, "key", "5"),
        GenOperation(Module::STRING, str2::Function::GET, 7831838528, 7832041084, "key", "5"),
        GenOperation(Module::STRING, str2::Function::GET, 7831936819, 7832121435, "key", "5"),
        GenOperation(Module::STRING, str2::Function::GET, 7832115234, 7832318706, "key", "5"),
        GenOperation(Module::STRING, str2::Function::GET, 7832629058, 7832844713, "key", "5"),
        GenOperation(Module::STRING, str2::Function::SET, 7832666198, 7832857024, "key", "6"),
        GenOperation(Module::STRING, str2::Function::SET, 7832897556, 7833132159, "key", "7"),
        GenOperation(Module::STRING, str2::Function::GET, 7832988414, 7833193822, "key", "7"),
        GenOperation(Module::STRING, str2::Function::SET, 7833066055, 7833266231, "key", "8"),
        GenOperation(Module::STRING, str2::Function::SET, 7834064981, 7834332904, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7834660568, 7834846905, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7835026909, 7835231937, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7835484789, 7835650307, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7835802589, 7836000512, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7836558179, 7836748499, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7836597828, 7836778818, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7837003535, 7837215617, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7837053406, 7837254059, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7837544997, 7837724892, "key", "9"),
        GenOperation(Module::STRING, str2::Function::GET, 7838358829, 7838506369, "key", "9"),
        GenOperation(Module::STRING, str2::Function::SET, 7838593649, 7838770188, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7838814796, 7838970024, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7838858817, 7839035645, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7839004866, 7839176137, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7839200784, 7839579572, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7839814265, 7840397402, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7840075319, 7840452975, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7840650572, 7840865120, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7840854204, 7841057132, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7842159969, 7842337005, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7842346552, 7842608223, "key", "10"),
        GenOperation(Module::STRING, str2::Function::GET, 7842600243, 7842826039, "key", "10"),
        GenOperation(Module::STRING, str2::Function::SET, 7842805260, 7842975119, "key", "11"),
        GenOperation(Module::STRING, str2::Function::GET, 7843283234, 7843489840, "key", "11"),
        GenOperation(Module::STRING, str2::Function::GET, 7843722327, 7843939710, "key", "11"),
        GenOperation(Module::STRING, str2::Function::GET, 7843774751, 7843985507, "key", "11"),
        GenOperation(Module::STRING, str2::Function::SET, 7843978043, 7844182894, "key", "12"),
        GenOperation(Module::STRING, str2::Function::GET, 7844087243, 7844287890, "key", "12"),
        GenOperation(Module::STRING, str2::Function::GET, 7845048715, 7845260709, "key", "12"),
        GenOperation(Module::STRING, str2::Function::GET, 7845693109, 7845924859, "key", "12"),
        GenOperation(Module::STRING, str2::Function::GET, 7845728519, 7845986128, "key", "12"),
        GenOperation(Module::STRING, str2::Function::SET, 7845906531, 7846104213, "key", "13"),
        GenOperation(Module::STRING, str2::Function::GET, 7845930938, 7846139374, "key", "13"),
        GenOperation(Module::STRING, str2::Function::SET, 7847042547, 7847241703, "key", "14"),
        GenOperation(Module::STRING, str2::Function::GET, 7847254217, 7847450150, "key", "14"),
        GenOperation(Module::STRING, str2::Function::SET, 7847410832, 7847571047, "key", "15"),
        GenOperation(Module::STRING, str2::Function::GET, 7847656562, 7847835655, "key", "15"),
        GenOperation(Module::STRING, str2::Function::SET, 7847886822, 7848064330, "key", "16"),
        GenOperation(Module::STRING, str2::Function::GET, 7848617218, 7848760820, "key", "16"),
        GenOperation(Module::STRING, str2::Function::SET, 7849605250, 7849808916, "key", "17"),
        GenOperation(Module::STRING, str2::Function::GET, 7849878131, 7850089997, "key", "17"),
        GenOperation(Module::STRING, str2::Function::GET, 7851164158, 7851324880, "key", "17"),
        GenOperation(Module::STRING, str2::Function::SET, 7851270782, 7851447284, "key", "18"),
        GenOperation(Module::STRING, str2::Function::SET, 7851487060, 7851690798, "key", "19"),
        GenOperation(Module::STRING, str2::Function::SET, 7852575125, 7852769318, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7853048892, 7853358149, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7853100272, 7853382443, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7853148254, 7853427867, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7853521129, 7853686748, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7854002433, 7854183161, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7854259893, 7854432646, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7854638570, 7854763799, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7854770833, 7855206195, "key", "20"),
        GenOperation(Module::STRING, str2::Function::GET, 7855498860, 7855713486, "key", "20"),
    };

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

#define TIME_BASE (1663850934416)

//                   Get 3
//  ------------------------------------------->
//                   Set 3
//    -------------------------->
//     Set 1         Get 1
// ------------->  ---------->
//                 Set 1
//            ------------->
//          Set 2
//       ----------->     Get 2
//                     ----------->
// Expect: Set 1 -> Get 1 -> Set 1 -> Set 2 -> Get 2 -> Set 3 -> Get 3
TEST(ConsistencyChecker, SalveReadBasic) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 0;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 11, TIME_BASE + 34, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 12, TIME_BASE + 40, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 20, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 22, TIME_BASE + 30, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 21, TIME_BASE + 28, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 15, TIME_BASE + 25, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 27, TIME_BASE + 35, "key",
                     "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

// set 1  set 2  set 3  set 4
// --->   --->   --->   --->
//           --->   --->        --->  --->         --->
//          get 2  get 1(SLAVE) get 2 get 1(SLAVE) get 3
TEST(ConsistencyChecker, SalveReadComplexCase1) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 1000;  // anything could happen
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 30, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 35, TIME_BASE + 50, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 50, TIME_BASE + 80, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 90, TIME_BASE + 110, "key",
                     "4"),

        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 40, TIME_BASE + 60, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 70, TIME_BASE + 90, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 90, TIME_BASE + 100, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 110, TIME_BASE + 150, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 110, TIME_BASE + 200, "key",
                     "3"),
    };

    checker.CheckConsistency({ops});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());

    checker.opts_.eventual_consistency_history_time_us = 4;
    checker.CheckConsistency({ops});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

// set 1  del   set 3  set 4
// --->   --->   --->   --->
//          get(not found)  get 1(SLAVE) get 3 get not found(SLAVE) get 3
TEST(ConsistencyChecker, SalveReadDeleteCase) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 100;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 30, "key",
                     "1"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, TIME_BASE + 40, TIME_BASE + 60,
                     "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 50, TIME_BASE + 70, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 90, TIME_BASE + 110, "key",
                     "4"),

        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 40, TIME_BASE + 60, "key",
                     "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 70, TIME_BASE + 90, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 90, TIME_BASE + 100, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 70, TIME_BASE + 90, "key",
                     "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 110, TIME_BASE + 200, "key",
                     "3"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

// set 1  set2   set 3  set 4
// --->   --->   --->   --->  set5
//                       ------------>
//               --->   --->   -------->   --->   --->    --->
//               get1   get2     get3      get4   get4    get5
//        --->   --->   --->     -------->        ---->
//         get2  get3   get4     get5             get5
TEST(ConsistencyChecker, MutiSalveReadCase) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 100;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 30, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 40, TIME_BASE + 70, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 80, TIME_BASE + 110, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 80, TIME_BASE + 110, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 120, TIME_BASE + 150, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 150, TIME_BASE + 180, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 40, TIME_BASE + 70, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 80, TIME_BASE + 110, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 120, TIME_BASE + 150, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 190, TIME_BASE + 210, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 210, TIME_BASE + 230, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 120, TIME_BASE + 150, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 130, TIME_BASE + 170, "key",
                     "5"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 240, TIME_BASE + 270, "key",
                     "5"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 160, TIME_BASE + 190, "key",
                     "5"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 200, TIME_BASE + 230, "key",
                     "5"),
    };

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

// set 1  set2   set 3  set 4
// --->   --->   --->   --->  set5
//                       ------------>
//               --->   --->   --->   --->   --->   --->
//               get1   get2   get3   get4   get4   get5
//         --->   --->   --->   --->   --->
//          get2  get3   get4   get5   get5
TEST(ConsistencyChecker, MutiSalveReadCase2) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 100;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 30, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 40, TIME_BASE + 70, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 80, TIME_BASE + 110, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 120, TIME_BASE + 150, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 130, TIME_BASE + 170, "key",
                     "5"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 120, TIME_BASE + 150, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 150, TIME_BASE + 180, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 190, TIME_BASE + 210, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 210, TIME_BASE + 230, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 240, TIME_BASE + 270, "key",
                     "5"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 40, TIME_BASE + 70, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 80, TIME_BASE + 110, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 120, TIME_BASE + 150, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 160, TIME_BASE + 190, "key",
                     "5"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 200, TIME_BASE + 230, "key",
                     "5"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

// set 1  set2   set 3  set 4
// --->   --->   --->   --->  set5
//                       ------------>
//                         --->                 --->   --->
//                         get2                 get4   get5
//                          --->   --->                 --->
//                          get1  get3                 get5
TEST(ConsistencyChecker, MutiSalveReadFailedCase3) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 50;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 29, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 40, TIME_BASE + 70, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 80, TIME_BASE + 110, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 120, TIME_BASE + 150, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 130, TIME_BASE + 170, "key",
                     "5"),

        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 120, TIME_BASE + 150, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 210, TIME_BASE + 230, "key",
                     "4"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 240, TIME_BASE + 270, "key",
                     "5"),

        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 90, TIME_BASE + 120, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 90, TIME_BASE + 120, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 200, TIME_BASE + 230, "key",
                     "5"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//  SETEX 1,2s
// -------->
//       GET 1
//     ---------->
//               TTL 1
//           ---------->
//               TTL 0.5s
//           ---------->
//               TTL 1s
//           ---------->
//                           1s,GET 1
//                         ---------->
//                                        2s,GET 1
//                                      ---------->
//                                        2s,GET 1(NotFound)
//                                      ---------->
//                                                         3s,GET 1(Failed)
//                                                      ----------->
//                                                         3s,GET 1(NotFound)
//                                                      ----------->
TEST(ConsistencyChecker, TtlSlaveReadSuccess) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 5 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SETEX, TIME_BASE + 10, TIME_BASE + 20, "key",
                     "1", 2 * 1000),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 20, TIME_BASE + 40, "key",
                     "1"),
        GenOperation(Module::COMMON, common2::Function::TTL, TIME_BASE + 30, TIME_BASE + 50, "key",
                     "1", 1999),
        GenOperation(Module::COMMON, common2::Function::TTL, TIME_BASE + 30, TIME_BASE + 50, "key",
                     "1", 1999),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 1 * 1e6,
                     TIME_BASE + 1 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 2 * 1e6,
                     TIME_BASE + 2 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 2 * 1e6,
                     TIME_BASE + 2 * 1e6 + 50, "key", "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 3 * 1e6,
                     TIME_BASE + 3 * 1e6 + 50, "key", "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 3 * 1e6,
                     TIME_BASE + 3 * 1e6 + 50, "key", "1", 0, kInternal),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//  SETEX 1,2s
// -------->
//       GET 1
//     ---------->
//               TTL 1
//           ---------->
//               TTL 1s
//           ---------->
//                           1s,GET 1
//                         ---------->
//                                        2s,GET 1
//                                      ---------->
//                                        2s,GET 1(NotFound)
//                                      ---------->
//                                                         3s,GET 1(Failed)
//                                                      ----------->
//                                                         3s,GET 1(NotFound)
//                                                      ----------->
//                                                                             8s, GET 1(slave
//                                                                             failed)
//                                                                             ------->
TEST(ConsistencyChecker, TtlSlaveReadFailed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 5 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SETEX, TIME_BASE + 10, TIME_BASE + 20, "key",
                     "1", 2 * 1000),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 20, TIME_BASE + 40, "key",
                     "1"),
        GenOperation(Module::COMMON, common2::Function::TTL, TIME_BASE + 1030, TIME_BASE + 1050,
                     "key", "1", 1999),
        GenOperation(Module::COMMON, common2::Function::TTL, TIME_BASE + 2030, TIME_BASE + 2050,
                     "key", "1", 1998),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 1 * 1e6,
                     TIME_BASE + 1 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 2 * 1e6,
                     TIME_BASE + 2 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 2 * 1e6,
                     TIME_BASE + 2 * 1e6 + 50, "key", "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 3 * 1e6,
                     TIME_BASE + 3 * 1e6 + 50, "key", "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 3 * 1e6,
                     TIME_BASE + 3 * 1e6 + 50, "key", "1", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 8 * 1e6,
                     TIME_BASE + 8 * 1e6 + 50, "key", "1"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//  SET 1,2s
// -------->
//       GET 1
//     ---------->
//               TTL 1
//           ---------->
//               TTL 1s
//           ---------->
//                           1s,GET 1
//                         ---------->
//                                        2s,GET 1
//                                      ---------->
//                                        2s,GET 1(NotFound)
//                                      ---------->
//                                                         3s,GET 1(Failed)
//                                                      ----------->
//                                                         3s,GET 1(NotFound)
//                                                      ----------->
//                                                                             6s, GET(slave) 1
//                                                                             ------->
TEST(ConsistencyChecker, NotTtlSlaveReadSucceed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 5 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 1;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SETEX, TIME_BASE + 10, TIME_BASE + 20, "key",
                     "1", 2 * 1000),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 20, TIME_BASE + 40, "key",
                     "1"),
        GenOperation(Module::COMMON, common2::Function::TTL, TIME_BASE + 3000, TIME_BASE + 4000,
                     "key", "1", 1997),
        GenOperation(Module::COMMON, common2::Function::TTL, TIME_BASE + 4000, TIME_BASE + 5000,
                     "key", "1", 1996),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 1 * 1e6,
                     TIME_BASE + 1 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 2 * 1e6,
                     TIME_BASE + 2 * 1e6 + 50, "key", "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 2 * 1e6,
                     TIME_BASE + 2 * 1e6 + 50, "key", "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 3 * 1e6,
                     TIME_BASE + 3 * 1e6 + 50, "key", "1", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 3 * 1e6,
                     TIME_BASE + 3 * 1e6 + 50, "key", "1", 0, kInternal),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 6 * 1e6,
                     TIME_BASE + 6 * 1e6 + 50, "key", "1"),
    };

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//                   Get 3
//  ------------------------------------------->
//                   Set 3
//    -------------------------->
//     Set 1         Get 1
// ------------->  ---------->
//                 Del
//            ------------->
//          Set 2
//       ----------->     Get 2
//                     ----------->
// Expect: Set 1 -> Get 1 -> Del -> Set 2 -> Get 2 -> Set 3 -> Get 3
TEST(ConsistencyChecker, SlaveReadWriteDel) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 20;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 11, TIME_BASE + 34, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 12, TIME_BASE + 40, "key",
                     "3"),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 20, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 22, TIME_BASE + 30, "key",
                     "1"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, TIME_BASE + 21, TIME_BASE + 28,
                     "key", ""),
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 15, TIME_BASE + 25, "key",
                     "2"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 27, TIME_BASE + 35, "key",
                     "2"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//     Set 1                  Get (not found)
// ------------->             ---------->
//                 Del
//            ------------->
//                                              Get 1
//                                          --------------->
TEST(ConsistencyChecker, SlaveReadWriteDelSucceed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 20;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 20, "key",
                     "1"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, TIME_BASE + 21, TIME_BASE + 28,
                     "key", ""),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 30, TIME_BASE + 40, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 20, TIME_BASE + 30, "key", "",
                     0, kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

//     Set 1                  Get (not found)
// ------------->             ---------->
//                 Del
//            ------------->
//                                                     Get 1
//                                                 --------------->
TEST(ConsistencyChecker, SlaveReadWriteDelfailed) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 20;
    opts.max_ambiguous_time_ms = 0;
    opts.max_expire_ambiguous_time_ms = 0;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SET, TIME_BASE + 10, TIME_BASE + 20, "key",
                     "1"),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, TIME_BASE + 21, TIME_BASE + 28,
                     "key", ""),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 60, TIME_BASE + 0, "key",
                     "1"),
        GenOperation(Module::STRING, str2::Function::GET, TIME_BASE + 30, TIME_BASE + 40, "key", "",
                     0, kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_FALSE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, ProductCase2) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = false;
    opts.eventual_consistency_history_time_us = 30 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 1000;
    opts.max_expire_ambiguous_time_ms = 200;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 1669210681873423, 1669210681921496, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669210681873423, 1669210681957679, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669210682901055, 1669210682939905, "key",
                     "", 0, kNotFound),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 1669210683206887,
                     1669210683241300, "key", "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::SET, 1669210683710427, 1669210683733992, "key",
                     "value1", 0, kOK),
        GenOperation(Module::STRING, str2::Function::GET, 1669210685432523, 1669210685453511, "key",
                     "value1", 0, kOK),
        GenOperation(Module::STRING, str2::Function::SET, 1669210686027552, 1669210686058973, "key",
                     "value2", 0, kOK),
        GenOperation(Module::COMMON, common2::Function::DEL_OBJECT, 1669210686105055,
                     1669210686113511, "key", "", 0, kOK),
        GenOperation(Module::STRING, str2::Function::GET, 1669210686364491, 1669210686382339, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669210687090205, 1669210687103622, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669210687374973, 1669210687384982, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669210688509798, 1669210688524376, "key",
                     "", 0, kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, ProductCase3) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 30 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 1000;
    opts.max_expire_ambiguous_time_ms = 200;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 1669213190273381, 1669213190307741, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669213190691872, 1669213190716615, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669213190999167, 1669213191002563, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669213191186406, 1669213191201883, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669213191330198, 1669213191341603, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669213191545039, 1669213191552356, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::SET, 1669213197191394, 1669213197195808, "key",
                     "value", 0, kOK),
        GenOperation(Module::STRING, str2::Function::GET, 1669213197200208, 1669213197200879, "key",
                     "", 0, kNotFound),
        GenOperation(Module::COMMON, common2::Function::TTL, 1669213206774935, 1669213206795377,
                     "key", "", 0, kOK),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, ProductCase4) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 30 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 1000;
    opts.max_expire_ambiguous_time_ms = 200;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 1669648806289768, 1669648806437680, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::SET, 1669648815243185, 1669648815400956, "key",
                     "value", 0, kOK),
        GenOperation(Module::STRING, str2::Function::GET, 1669648820888289, 1669648820899314, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1669648833349569, 1669648833419529, "key",
                     "value", 0, kOK),
        GenOperation(Module::STRING, str2::Function::GET, 1669648841039879, 1669648841125569, "key",
                     "value", 0, kOK),
        GenOperation(Module::COMMON, common2::Function::TTL, 1669648843096045, 1669648843250008,
                     "key", "value", 0, kOK),
        GenOperation(Module::STRING, str2::Function::GET, 1669648844986291, 1669648845144763, "key",
                     "value", 0, kOK),
        GenOperation(Module::COMMON, common2::Function::EXPIRE, 1669648848324173, 1669648848513019,
                     "key", "value", 2000, kOK),
        GenOperation(Module::STRING, str2::Function::GET, 1669648851011263, 1669648851164484, "key",
                     "", 0, kNotFound),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, ProductCase5) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 10 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 1000;
    opts.max_expire_ambiguous_time_ms = 1000;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::SETEX, 1679405111169515, 1679405111174745,
                     "key", "f904gz40csn9xfj5dbbinf", 13148),
        GenOperation(Module::COMMON, common2::Function::TTL, 1679405111488475, 1679405111490931,
                     "key", "", 12830, kOK),
        GenOperation(Module::COMMON, common2::Function::TTL, 1679405152264932, 1679405152266429,
                     "key", "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::SET, 1679405156607516, 1679405156610536, "key",
                     "2er8h3dsxil4xw7ii445k3hak57nalksrqiou0svca9wj7u"),
        GenOperation(Module::STRING, str2::Function::GET, 1679405159470600, 1679405159472732, "key",
                     "2er8h3dsxil4xw7ii445k3hak57nalksrqiou0svca9wj7u"),
        GenOperation(Module::STRING, str2::Function::GET, 1679405166867098, 1679405166869965, "key",
                     "2er8h3dsxil4xw7ii445k3hak57nalksrqiou0svca9wj7u"),
        GenOperation(Module::STRING, str2::Function::SET, 1679405169762834, 1679405169767343, "key",
                     "hzsr9e5vfvwykkaahgfyylm5sx2umr9laop5kvz3m7ngk"),
        GenOperation(Module::STRING, str2::Function::GET, 1679405169790263, 1679405169792316, "key",
                     "2er8h3dsxil4xw7ii445k3hak57nalksrqiou0svca9wj7u"),
    };

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, ProductCase6) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 10 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 1000;
    opts.max_expire_ambiguous_time_ms = 1000;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenOperation(Module::STRING, str2::Function::GET, 1679413971448503, 1679413971450809, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::SET, 1679413997842323, 1679413997846429, "key",
                     "value"),
        GenOperation(Module::STRING, str2::Function::GET, 1679413997882245, 1679413997884356, "key",
                     "", 0, kNotFound),
        GenOperation(Module::STRING, str2::Function::GET, 1679414008444434, 1679414008447150, "key",
                     "value"),
        GenOperation(Module::COMMON, common2::Function::EXPIRE, 1679414016404919, 1679414016411805,
                     "key", "", 25222),
        GenOperation(Module::STRING, str2::Function::GET, 1679414032375446, 1679414032377652, "key",
                     "value"),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

TEST(ConsistencyChecker, HashModel1) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    ConsistencyChecker checker;
    ConsistencyChecker::Options opts;
    opts.worker_num = 1;
    opts.eventual_consistency_mode = true;
    opts.eventual_consistency_history_time_us = 10 * 1000 * 1000;
    opts.max_ambiguous_time_ms = 1000;
    opts.max_expire_ambiguous_time_ms = 1000;
    opts.timeout_ms = 60000;
    checker.Init(opts);

    std::vector<Operation> ops = {
        GenHashOperation(hash2::Function::SET, 1679469884729950, 1679469884741357, "key",
                         "field_13", "dwysy3paay5z1h2a"),
        GenOperation(Module::COMMON, common2::Function::TTL, 1679469888506901, 1679469888510788,
                     "key", "dwysy3paay5z1h2a", 0),
        GenHashOperation(hash2::Function::SET, 1679469889124792, 1679469889128513, "key", "field_8",
                         "xqo8ritscb5wuo"),
        GenHashOperation(hash2::Function::GET, 1679469889877457, 1679469889882523, "key", "field_3",
                         "", false),
        GenHashOperation(hash2::Function::DEL, 1679469894767282, 1679469894771729, "key", "field_4",
                         ""),
        GenHashOperation(hash2::Function::GET, 1679469895031328, 1679469895038198, "key",
                         "field_14", "", false),
        GenHashOperation(hash2::Function::SET, 1679469902264011, 1679469902268353, "key", "field_7",
                         "4pl79z8dmdcyg7pe", false),
        GenHashOperation(hash2::Function::SET, 1679469914558569, 1679469914565054, "key",
                         "field_13", "pitiuj8yasd5thayv39zbvv", false),
        GenHashOperation(hash2::Function::GET, 1679469914562724, 1679469914568641, "key", "field_4",
                         "", false),
    };
    std::shuffle(ops.begin(), ops.end(), rng);

    checker.CheckConsistency({std::move(ops)});
    checker.checker_countdown_->Wait();
    ASSERT_TRUE(checker.Consistency());
    ASSERT_FALSE(checker.Timeout());
}

}  // namespace test
}  // namespace bench
}  // namespace bcache2
