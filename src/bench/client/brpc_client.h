// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>

#include "bench/client/client.h"
#include "brpc/channel.h"
#include "brpc/controller.h"

namespace bcache2 {
namespace bench {

class BrpcClient : public Client {
 public:
    struct Options {
        std::string server_addr;
        uint64_t partition_id = 0;
        int64_t timeout_ms = 1000;
    };

    BrpcClient() {}
    ~BrpcClient() {}

    Status Init(Options opt);

    void Execute(Controller* ctrl, Operation* op, Closure<void>* callback) override;

 private:
    struct ExecuteContext {
        brpc::Controller brpc_ctrl;
        Controller* ctrl = nullptr;
        Closure<void>* callback = nullptr;
        BatchExecuteCmdRequest request;
        BatchExecuteCmdResponse response;
        Operation* op;
    };
    static void OnExecuteDone(ExecuteContext* context);

    Options opts_;

    brpc::Channel channel_;
    brpc::ChannelOptions channel_options_;
    std::unique_ptr<bcache2::ServerService_Stub> bcache2_server_stub_;

    DISALLOW_COPY_AND_ASSIGN(BrpcClient);
};

}  // namespace bench
}  // namespace bcache2
