// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>

#include "bench/client/client.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "client/bcache2.h"
#include "common/status.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

class BCache2Client : public Client {
 public:
    struct Options {
        std::string idc;
        std::string table_uri;
        bool pin_primary = false;
        uint64_t timeout_ms = 5000;
    };

    BCache2Client() {}
    ~BCache2Client() {
        if (table_ != nullptr) {
            bcache2_close(table_);
        }
    }

    Status Init(Options opt);

    void Execute(Controller* ctrl, Operation* op, Closure<void>* callback) override;

 private:
    struct ExecuteContext {
        BCache2Client* client = nullptr;
        Controller* ctrl = nullptr;
        Closure<void>* callback = nullptr;
        bcache2_execution_t* executions = nullptr;
        Operation* op;
    };
    void OnExecuteDone(ExecuteContext* context);

    Options opts_;
    bcache2_table_t* table_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(BCache2Client);
};

}  // namespace bench
}  // namespace bcache2
