// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "brpc/server.h"
#include "brpc/thrift_service.h"
#include "client/client_impl.h"
#include "common/status.h"
#include "proxy/flags.h"
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

        const bool is_write = IsWriteMethod(ctrl->thrift_method_name());
        Status status = CheckAccountScope(request.namespace_name);
        if (!status.ok()) {
            response->__set_status(ToThriftStatus(status));
            return;
        }
        status = TryAcquireIngestRequest(is_write);
        if (!status.ok()) {
            response->__set_status(ToThriftStatus(status));
            return;
        }
        auto inflight_guard = std::shared_ptr<void>(nullptr, [this, is_write](void*) {
            ReleaseIngestRequest(is_write);
        });

        client::Table* table = nullptr;
        client::TableOptions table_options;
        table_options.io_timeout_ms = FLAGS_proxy_backend_io_timeout_ms;
        table_options.connect_timeout_ms = FLAGS_proxy_backend_connect_timeout_ms;
        status = client_->OpenTable(request.namespace_name, request.table_name, table_options,
                                    &table);
        if (!status.ok()) {
            response->__set_status(ToThriftStatus(status));
            return;
        }

        done_guard.release();

        Controller* client_ctrl = new Controller();
        client_ctrl->set_timeout_ms(FLAGS_proxy_backend_io_timeout_ms);
        client::TableCore::Request* client_request = new client::TableCore::Request();
        client::TableCore::Response* client_response = new client::TableCore::Response();
        client::TableImpl* table_impl = static_cast<client::TableImpl*>(table);
        status = TransformRequest(request, client_request);
        BYTE_ASSERT(status.ok()) << status;

        auto func = [ctrl, client_ctrl, client_request, client_response, request, response, done,
                     inflight_guard] {
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

    Status CheckAccountScope(const std::string& namespace_name) const;
    bool IsWriteMethod(const std::string& method_name) const;
    Status TryAcquireIngestRequest(bool is_write);
    void ReleaseIngestRequest(bool is_write);

    client::ClientImpl* client_ = nullptr;
    std::atomic<uint64_t> inflight_requests_{0};
    std::atomic<uint64_t> inflight_write_requests_{0};

    DISALLOW_COPY_AND_ASSIGN(Bcache2ThriftService);
};

}  // namespace proxy
}  // namespace bcache2
