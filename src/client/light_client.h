// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>
#include <google/protobuf/message.h>

#include <string>
#include <utility>
#include <vector>

#include "client/swig_client.h"

namespace bcache2 {

class LightTable {
 public:
    explicit LightTable(swig::Table* table) : table_(table) {}

    void Execute(swig::Controller* ctrl, uint32_t cmd_id, const std::string& partition_key,
                 std::string request, std::string* response) {
        std::vector<swig::Execution> executions(1);
        executions[0].cmd = cmd_id;
        executions[0].partition_key.raw() = partition_key;
        executions[0].request.raw() = std::move(request);

        table_->BatchExecute(ctrl, &executions);
        if (!ctrl->status.ok()) {
            return;
        }
        ctrl->status = std::move(executions[0].status);
        *response = std::move(executions[0].response.raw());
    }

    void Execute(swig::Controller* ctrl, uint32_t cmd_id, const std::string& partition_key,
                 const google::protobuf::Message& request, google::protobuf::Message* response) {
        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            ctrl->status =
                swig::Status(static_cast<int>(swig::Code::INTERNAL), "Request serialize failed");
            return;
        }
        std::string response_bytes;
        Execute(ctrl, cmd_id, partition_key, std::move(request_bytes), &response_bytes);
        if (!ctrl->status.ok()) {
            return;
        }
        if (!response->ParseFromString(response_bytes)) {
            ctrl->status =
                swig::Status(static_cast<int>(swig::Code::INTERNAL), "Response parse failed");
            return;
        }
    }

 private:
    swig::Table* table_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(LightTable);
};

}  // namespace bcache2
