// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>

#include "brpc/server.h"
#include "brpc/thrift_service.h"
#include "client/client_impl.h"
#include "common/status.h"
#include "proxy/thrift_utils.h"
#include "thrift/server_types.h"

namespace bcache2 {
namespace proxy {

class Bcache2ThriftService : public brpc::ThriftService {
 public:
    explicit Bcache2ThriftService(client::ClientImpl* client)
        : brpc::ThriftService(), client_(client) {}
    ~Bcache2ThriftService() {}

    bool ParseBaseThrift(butil::IOBuf* source, const std::string& method_name,
                         uint32_t* body_len);

    void ProcessThriftFramedRequest(brpc::Controller* ctrl, brpc::ThriftFramedMessage* request,
                                    brpc::ThriftFramedMessage* response,
                                    google::protobuf::Closure* done) override;

 private:
    template <typename Request, typename Response>
    void CommonHandler(brpc::Controller* ctrl, const Request& request, Response* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        response->__set_status(ToThriftStatus(Status::OK()));

        LOG_DEBUG("RPC Received").put("Remote", ctrl->remote_side()).put("Request", request);

        client::Table* table = nullptr;
        Status status = client_->OpenTable(request.namespace_name, request.table_name,
                                           client::TableOptions(), &table);
        if (!status.ok()) {
            response->__set_status(ToThriftStatus(status));
            return;
        }

        done_guard.release();

        Controller* client_ctrl = new Controller();
        client::TableCore::Request* client_request = new client::TableCore::Request();
        client::TableCore::Response* client_response = new client::TableCore::Response();
        client::TableImpl* table_impl = static_cast<client::TableImpl*>(table);
        status = TransformRequest(request, client_request);
        BYTE_ASSERT(status.ok()) << status;

        auto func = [ctrl, client_ctrl, client_request, client_response, request, response, done] {
            brpc::ClosureGuard done_guard(done);
            std::unique_ptr<Controller> _ctrl(client_ctrl);
            std::unique_ptr<client::TableCore::Request> _request(client_request);
            std::unique_ptr<client::TableCore::Response> _response(client_response);

            LOG_DEBUG("RPC Finished")
                .put("Remote", ctrl->remote_side())
                .put("Request", request)
                .put("Response", *response);

            if (!client_ctrl->status().ok()) {
                response->__set_status(ToThriftStatus(client_ctrl->status()));
                return;
            }

            Status status = TransformResponse(*client_response, response);
            if (!status.ok()) {
                response->__set_status(ToThriftStatus(status));
                return;
            }
        };
        table_impl->Execute(client_ctrl, client_request, client_response, NewFuncClosure(func),
                            nullptr, client::RequestOptions());
    }

    client::ClientImpl* client_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(Bcache2ThriftService);
};

}  // namespace proxy
}  // namespace bcache2
