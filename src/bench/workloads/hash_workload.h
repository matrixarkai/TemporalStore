// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "bench/workloads/workloads.h"
#include "extension/hash/interface.pb.h"
#include "extension/modules.pb.h"

namespace bcache2 {
namespace bench {

class HashWorkload : public Workload {
 public:
    struct Options {
        uint64_t freq_hset = 0;
        uint64_t freq_hget = 0;
        uint64_t freq_hdel = 0;
        uint64_t freq_hgetall = 0;

        uint64_t field_count = 0;
    };

    HashWorkload() {}
    ~HashWorkload() {}

    void Init(Options options) {
        if (options.freq_hset > 0) {
            function_dice_.AddProperty(options.freq_hset, hash2::Function::SET);
        }
        if (options.freq_hget > 0) {
            function_dice_.AddProperty(options.freq_hget, hash2::Function::GET);
        }
        if (options.freq_hdel > 0) {
            function_dice_.AddProperty(options.freq_hdel, hash2::Function::DEL);
        }
        if (options.freq_hgetall > 0) {
            function_dice_.AddProperty(options.freq_hgetall, hash2::Function::GETALL);
        }
        opts_ = options;
    }

    std::string Name() const override { return "HashWorkload"; }

    Operation NextOperation(const std::string& key, const std::string& value) override;

 private:
    std::string RandomField() const {
        static std::random_device dev;
        static std::mt19937 rng(dev());
        return "field_" +
               std::to_string(std::uniform_int_distribution<uint64_t>(1, opts_.field_count)(rng));
    }

    Options opts_;
    RatioDice<hash2::Function> function_dice_;
};

inline Operation HashWorkload::NextOperation(const std::string& key, const std::string& value) {
    Operation operation;
    operation.set_module_id(Module::HASH);
    operation.set_function_id(function_dice_.Roll());

    switch (operation.function_id()) {
    case hash2::Function::SET: {
        hash2::SetRequest request;
        request.set_key(key);
        request.set_value(value);
        request.set_field(RandomField());
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(request.key());
        break;
    }
    case hash2::Function::GET: {
        hash2::GetRequest request;
        request.set_key(key);
        request.set_field(RandomField());
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(request.key());
        break;
    }
    case hash2::Function::DEL: {
        hash2::DelRequest request;
        request.set_key(key);
        request.set_field(RandomField());
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(request.key());
        break;
    }
    case hash2::Function::GETALL: {
        hash2::GetAllRequest request;
        request.set_key(key);
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(request.key());
        break;
    }
    }

    return operation;
}

}  // namespace bench
}  // namespace bcache2
