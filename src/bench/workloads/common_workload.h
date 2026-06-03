// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "bench/workloads/workloads.h"
#include "extension/common/interface.pb.h"
#include "extension/modules.pb.h"

namespace bcache2 {
namespace bench {

class CommonWorkload : public Workload {
 public:
    struct Options {
        uint64_t freq_del = 0;
        uint64_t freq_expire = 0;
        uint64_t freq_ttl = 0;

        uint64_t expire_min_ttl_ms = 0;
        uint64_t expire_max_ttl_ms = 0;
    };

    CommonWorkload() {}
    ~CommonWorkload() {}

    void Init(Options options) {
        if (options.freq_del > 0) {
            function_dice_.AddProperty(options.freq_del, common2::Function::DEL_OBJECT);
        }
        if (options.freq_expire > 0) {
            function_dice_.AddProperty(options.freq_expire, common2::Function::EXPIRE);
        }
        if (options.freq_ttl > 0) {
            function_dice_.AddProperty(options.freq_ttl, common2::Function::TTL);
        }
        opts_ = options;
    }

    std::string Name() const override { return "CommonWorkload"; }

    Operation NextOperation(const std::string& key, const std::string& value) override;

 private:
    uint64_t RandomExpireTime() const {
        static std::random_device dev;
        static std::mt19937 rng(dev());
        return std::uniform_int_distribution<uint64_t>(opts_.expire_min_ttl_ms,
                                                       opts_.expire_max_ttl_ms)(rng);
    }

    Options opts_;
    RatioDice<common2::Function> function_dice_;
};

inline Operation CommonWorkload::NextOperation(const std::string& key, const std::string& value) {
    Operation operation;
    operation.set_module_id(Module::COMMON);
    operation.set_function_id(function_dice_.Roll());

    switch (operation.function_id()) {
    case common2::Function::DEL_OBJECT: {
        common2::DelObjectRequest request;
        request.set_key(key);
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(request.key());
        break;
    }
    case common2::Function::EXPIRE: {
        common2::ExpireRequest request;
        request.set_key(key);
        request.set_ttl_ms(RandomExpireTime());
        request.SerializeToString(operation.mutable_request_bytes());
        operation.set_key(request.key());
        break;
    }
    case common2::Function::TTL: {
        common2::TtlRequest request;
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
