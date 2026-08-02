// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/client/bcache2_client.h"

#include <utility>
#include <vector>

#include "butil/fast_rand.h"
#include "bench/flags.h"
#include "common/cmd_manager.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"

namespace bcache2 {
namespace bench {

Status BCache2Client::Init(Options opt) {
    opts_ = std::move(opt);

    bcache2_options_t* client_opts = bcache2_options_init();
    BYTE_DEFER(bcache2_options_destory(client_opts));
    bcache2_options_set(client_opts, "idc", opts_.idc.c_str());
    bcache2_options_set(client_opts, "pin_primary", opts_.pin_primary ? "true" : "false");
    bcache2_options_set(client_opts, "meta_sync_interval_ms",
        std::to_string(FLAGS_bench_bcache2_client_meta_sync_interval_ms).c_str());
    bcache2_options_set(client_opts, "topo_error_retry_interval_ms",
        std::to_string(FLAGS_bench_bcache2_client_topo_error_retry_interval_ms).c_str());
    bcache2_init(client_opts);

    bcache2_table_options_t* table_opts = bcache2_tableoptions_init();
    bcache2_tableoptions_set(table_opts, "io_timeout_ms", std::to_string(opts_.timeout_ms).c_str());
    bcache2_tableoptions_set(table_opts, "connect_timeout_ms",
                             std::to_string(opts_.timeout_ms).c_str());
    BYTE_DEFER(bcache2_tableoptions_destory(table_opts));

    if (bcache2_open(opts_.table_uri.c_str(), table_opts, &table_) != BCACHE2_OK) {
        return Status::Internal("Failed to open table");
    }
    return Status::OK();
}

void BCache2Client::Execute(Controller* ctrl, Operation* op, Closure<void>* callback) {
    ExecuteContext* context = new ExecuteContext();
    context->client = this;
    context->ctrl = ctrl;
    context->callback = callback;
    context->op = op;
    context->executions = bcache2_execution_init(butil::fast_rand(), opts_.timeout_ms);
    bcache2_execution_add_request(context->executions,
                                  MakeCmdId(op->module_id(), op->function_id()),
                                  data_t{op->key().data(), op->key().size()},
                                  data_t{op->request_bytes().data(), op->request_bytes().size()});
    bcache2_execute(
        table_, context->executions,
        [](void* args) {
            ExecuteContext* context = static_cast<ExecuteContext*>(args);
            context->client->OnExecuteDone(context);
        },
        context);
}

void BCache2Client::OnExecuteDone(ExecuteContext* context) {
    if (bcache2_execution_get_status(context->executions, 0) == BCACHE2_NOT_FOUND) {
        context->ctrl->set_status(
            Status::NotFound(bcache2_execution_get_message(context->executions, 0)));
    } else if (bcache2_execution_get_status(context->executions, 0) != BCACHE2_OK) {
        context->ctrl->set_status(
            Status::Internal(bcache2_execution_get_message(context->executions, 0)));
    }

    data_t resp_data = bcache2_execution_get_response(context->executions, 0);
    context->op->set_response_bytes(resp_data.data, resp_data.size);
    context->callback->Run();
    bcache2_execution_destory(context->executions);
}

}  // namespace bench
}  // namespace bcache2
