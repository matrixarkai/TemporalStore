// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "bench/workloads/workloads.h"
#include "extension/modules.pb.h"
#include "extension/string/interface.pb.h"

namespace bcache2 {
namespace bench {

class StringWorkload : public Workload {
 public:
    struct Options {
        uint64_t freq_set = 0;
        uint64_t freq_setex = 0;
        uint64_t freq_get = 0;

        uint64_t setex_min_ttl_ms = 0;
        uint64_t setex_max_ttl_ms = 0;
    };

    StringWorkload() {}
    ~StringWorkload() {}

    void Init(Options options) {
        if (options.freq_set > 0) {
            function_dice_.AddProperty(options.freq_set, str2::Function::SET);
        }
        if (options.freq_setex > 0) {
            function_dice_.AddProperty(options.freq_setex, str2::Function::SETEX);
        }
        if (options.freq_get > 0) {
            function_dice_.AddProperty(options.freq_get, str2::Function::GET);
        }
        opts_ = options;
    }

    std::string Name() const override { return "StringWorkload"; }

    Operation NextOperation(const std::string& key, const std::string& value) override;

 private:
    uint64_t RandomSetexTime() const {
        static std::random_device dev;
        static std::mt19937 rng(dev());
        return std::uniform_int_distribution<uint64_t>(opts_.setex_min_ttl_ms,
                                                       opts_.setex_max_ttl_ms)(rng);
    }

    Options opts_;
    RatioDice<str2::Function> function_dice_;
};

inline Operation StringWorkload::NextOperation(const std::string& key, const std::string& value) {
    Operation operation;
    operation.set_module_id(Module::STRING);
    operation.set_function_id(function_dice_.Roll());

    switch (operation.function_id()) {
    case str2::Function::SET: {
        str2::SetRequest request;
        request.set_key(key);
        request.set_value(value);
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(request.key());
        break;
    }
    case str2::Function::SETEX: {
        str2::SetexRequest request;
        request.set_key(key);
        request.set_value(value);
        request.set_ttl_ms(RandomSetexTime());
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(request.key());
        break;
    }
    case str2::Function::GET: {
        str2::GetRequest request;
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
