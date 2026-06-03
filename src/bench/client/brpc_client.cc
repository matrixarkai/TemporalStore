// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/client/brpc_client.h"

#include <utility>

#include "common/coclosure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"

namespace bcache2 {
namespace bench {

Status BrpcClient::Init(Options opts) {
    opts_ = std::move(opts);
    channel_options_.connection_type = brpc::CONNECTION_TYPE_SINGLE;
    channel_options_.connect_timeout_ms = -1;
    channel_options_.timeout_ms = 3000;  // TODO(wangtai.10): custom timeout
    if (channel_.Init(opts_.server_addr.c_str(), &channel_options_) != 0) {
        return Status::Internal("failed to init channel");
    }
    bcache2_server_stub_.reset(new bcache2::ServerService_Stub(&channel_));
    return Status::OK();
}

void BrpcClient::OnExecuteDone(ExecuteContext* context) {
    BYTE_DEFER({
        context->callback->Run();
        delete context;
    });

    if (context->brpc_ctrl.Failed()) {
        context->ctrl->set_status(Status::Internal(context->brpc_ctrl.ErrorText()));
        return;
    }

    if (context->response.response()[0].status().code() != kOK) {
        context->ctrl->set_status(
            Status::Internal(context->response.response()[0].status().message()));
        return;
    }

    if (context->response.response()[0].response_status().code() != kOK) {
        context->ctrl->set_status(
            Status::FromRpcStatus(context->response.response()[0].response_status()));
        return;
    }

    context->op->set_response_bytes(context->response.response()[0].response_bytes());
}

void BrpcClient::Execute(Controller* ctrl, Operation* op, Closure<void>* callback) {
    ExecuteContext* context = new ExecuteContext();
    context->ctrl = ctrl;
    context->callback = callback;
    context->op = op;
    context->request.set_partition_id(opts_.partition_id);
    context->request.set_load_version(UINT64_MAX);
    auto cmd_request = context->request.add_request();
    cmd_request->set_module_id(op->module_id());
    cmd_request->set_function_id(op->function_id());
    cmd_request->set_request_bytes(op->request_bytes());

    bcache2_server_stub_->BatchExecuteCmd(&context->brpc_ctrl, &context->request,
                                          &context->response,
                                          brpc::NewCallback(BrpcClient::OnExecuteDone, context));
}

}  // namespace bench
}  // namespace bcache2
