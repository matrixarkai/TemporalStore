// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>
#include <byte/include/macros.h>

#include <string>

#include "common/controller.h"
#include "common/metrics.h"
#include "common/time_tracer.h"
#include "partition/storage/object.h"
#include "partition/storage/op_logger.h"
#include "protocol/server.pb.h"

DECLARE_uint64(slow_request_threshold_us);

namespace bcache2 {
namespace partition {

struct CmdContext {
    uint64_t partition_id = 0;
    uint16_t module_id = 0;
    uint16_t function_id = 0;
    uint64_t slot_id = 0;
    uint8_t model_id = 0;
    Object object;
    absl::string_view key;
    Controller* ctrl = nullptr;
    OpLogger* op_logger = nullptr;
    RequestMetrics* metrics = nullptr;
    const google::protobuf::Message* request = nullptr;
    google::protobuf::Message* response = nullptr;
    Status* status = nullptr;
    int32_t load_slot_times = 0;
    TimeTracer time_tracer;
    bool object_in_memory = true;
    bool object_hit = true;

    CmdContext() {}
    CmdContext(uint64_t partition_id, uint16_t module_id, uint16_t function_id, Controller* ctrl,
               OpLogger* op_logger, RequestMetrics* metrics,
               const google::protobuf::Message* request, google::protobuf::Message* response,
               Status* response_status)
        : partition_id(partition_id),
          module_id(module_id),
          function_id(function_id),
          ctrl(ctrl),
          op_logger(op_logger),
          metrics(metrics),
          request(request),
          response(response),
          status(response_status) {}

    ~CmdContext() {
        time_tracer.AddEvent("end");
        uint64_t total_spent_us = time_tracer.TotalSpentUs();

        if (LIKELY(metrics != nullptr && ctrl != nullptr)) {
            metrics->Set(ctrl->status().ok(), total_spent_us, request->ByteSize(),
                         response->ByteSize(), ctrl->status().errorcode());
        }
        if (total_spent_us > FLAGS_slow_request_threshold_us && ctrl) {
            LOG_WARNING("Slow Cmd")
                .put("PartitionId", partition_id)
                .put("ModuleId", module_id)
                .put("FunctionId", function_id)
                .put("TraceId", ctrl->trace_id())
                .put("Status", ctrl->status())
                .put("ResponseStatus", status != nullptr ? status->ToString() : "")
                .put("TimeTracer", time_tracer);
        }
    }
};

}  // namespace partition
}  // namespace bcache2
