// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <vector>

#include "common/cmd_manager.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

inline std::unique_ptr<google::protobuf::Message> BuildRequest(const Operation& op) {
    auto cmd = CmdManager::GetCmd(op.module_id(), op.function_id());
    std::unique_ptr<google::protobuf::Message> request(cmd->request_builder());
    request->ParseFromString(op.request_bytes());
    return request;
}

inline std::unique_ptr<google::protobuf::Message> BuildResponse(const Operation& op) {
    auto cmd = CmdManager::GetCmd(op.module_id(), op.function_id());
    std::unique_ptr<google::protobuf::Message> response(cmd->response_builder());
    response->ParseFromString(op.response_bytes());
    return response;
}

inline std::ostream& operator<<(std::ostream& os, const Operation& op) {
    os << "Operation{Module=" << CmdManager::GetModuleInfos()[op.module_id()].name << ", Function="
       << CmdManager::GetModuleInfos()[op.module_id()].cmd_executors[op.function_id()].name
       << ", Request=" << BuildRequest(op)->ShortDebugString()
       << ", Response=" << BuildResponse(op)->ShortDebugString()
       << ", StartTimeUs=" << op.start_time_us() << ", EndTimeUs=" << op.end_time_us()
       << ", Code=" << op.code() << ", Message=" << op.message() << "}";
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const std::vector<Operation>& ops) {
    os << "[";
    bool first = true;
    for (auto& op : ops) {
        if (!first) {
            os << ", ";
        }
        os << op;
        first = false;
    }
    os << "]";
    return os;
}

}  // namespace bench
}  // namespace bcache2
