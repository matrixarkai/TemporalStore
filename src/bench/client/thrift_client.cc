// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/client/thrift_client.h"

// #include <utility>

// #include "brpc/thrift_message.h"
// #include "common/coclosure.h"
// #include "common/logging.h"
// #include "common/scoped_invoker.h"
// #include "thrift/server_types.h"

// namespace bcache2 {
// namespace bench {

// Status ThriftClient::Init(Options opts) {
//     opts_ = std::move(opts);
//     channel_options_.protocol = brpc::PROTOCOL_THRIFT;
//     if (channel_.Init(opts_.server_addr.c_str(), &channel_options_) != 0) {
//         return Status::Internal("failed to init channel");
//     }
//     return Status::OK();
// }

// static void OnBrpcdone(CoSyncClosure* sync) { sync->Run(); }

// void ThriftClient::ExecuteRequest(Controller* ctrl, ExecuteCmdRequest* request,
//                                   ExecuteCmdResponse* response, Closure<void>* callback) {
//     BYTE_ASSERT(IsCoContext());
//     ScopedCallback done(callback);

//     switch (request->request().module_case()) {
//     case bcache2::CmdRequest::ModuleCase::kStringRequest: {
//         ExecuteStringRequest(ctrl, request->mutable_request()->mutable_string_request(),
//                              response->mutable_response()->mutable_string_response());
//         break;
//     }
//     default:
//         BYTE_ASSERT("invalid module case ") << request->request().module_case();
//     }
// }

// void ThriftClient::ExecuteStringRequest(Controller* ctrl, str::StringModuleRequest* request,
//                                         str::StringModuleResponse* response) {
//     BYTE_ASSERT(IsCoContext());

//     brpc::Controller brpc_ctrl;
//     brpc::ThriftStub stub(&channel_);
//     CoSyncClosure sync;

//     switch (request->cmd_case()) {
//     case bcache2::str::StringModuleRequest::CmdCase::kGetRequest: {
//         thrift::GetRequest trequest;
//         thrift::GetResponse tresponse;
//         trequest.__set_namespace_name(opts_.namespace_name);
//         trequest.__set_table_name(opts_.table_name);
//         trequest.__set_key(request->get_request().key());
//         stub.CallMethod("Get", &brpc_ctrl, &trequest, &tresponse,
//                         brpc::NewCallback(OnBrpcdone, &sync));
//         sync.Wait();
//         if (brpc_ctrl.Failed()) {
//             ctrl->set_status(Status::Internal(brpc_ctrl.ErrorText()));
//             return;
//         }
//         response->mutable_get_response()->set_value(tresponse.value);
//         ctrl->set_status(Status(Code(tresponse.status.code), tresponse.status.message));
//         break;
//     }
//     case bcache2::str::StringModuleRequest::CmdCase::kSetRequest: {
//         thrift::SetRequest trequest;
//         thrift::SetResponse tresponse;
//         trequest.__set_namespace_name(opts_.namespace_name);
//         trequest.__set_table_name(opts_.table_name);
//         trequest.__set_key(request->set_request().key());
//         trequest.__set_value(request->set_request().value());
//         stub.CallMethod("Set", &brpc_ctrl, &trequest, &tresponse,
//                         brpc::NewCallback(OnBrpcdone, &sync));
//         sync.Wait();
//         if (brpc_ctrl.Failed()) {
//             ctrl->set_status(Status::Internal(brpc_ctrl.ErrorText()));
//             return;
//         }
//         ctrl->set_status(Status(Code(tresponse.status.code), tresponse.status.message));
//         break;
//     }
//     default: {
//         ctrl->set_status(Status::Unknown("unknown cmd"));
//         return;
//     }
//     }
// }

// }  // namespace bench
// }  // namespace bcache2
