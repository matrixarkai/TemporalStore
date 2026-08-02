// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>

#include "bench/client/client.h"
#include "brpc/channel.h"
#include "brpc/controller.h"

namespace bcache2 {
namespace bench {

// TODO(wangtai.10): impl
class ThriftClient : public Client {
 public:
    struct Options {
        std::string server_addr;
        std::string namespace_name;
        std::string table_name;
        int64_t timeout_ms = 1000;
    };

    ThriftClient() {}
    ~ThriftClient() {}

    Status Init(Options opt) { return Status::OK(); }

    void Execute(Controller* ctrl, Operation* op, Closure<void>* callback) override {}

 private:
    // void ExecuteStringRequest(Controller* ctrl, str::StringModuleRequest* request,
    //                           str::StringModuleResponse* response);

    // Options opts_;

    // brpc::Channel channel_;
    // brpc::ChannelOptions channel_options_;

    DISALLOW_COPY_AND_ASSIGN(ThriftClient);
};

}  // namespace bench
}  // namespace bcache2
