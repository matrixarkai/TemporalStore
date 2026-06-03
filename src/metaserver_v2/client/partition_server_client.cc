// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/client/partition_server_client.h"

#include "butil/endpoint.h"

#include "common/proto_enhance.h"

namespace bcache2 {
namespace metaserver {

BRpcServiceImpl& BRpcServiceImpl::Init(const Endpoint& server) {
    stage_ = Stage::kError;
    // TODO(wuzhenyu) configurable
    brpc::ChannelOptions options;
    options.max_retry = 3;
    options.timeout_ms = 60'000;

    butil::EndPoint bep = ToBRpcEndpoint(server);
    if (channel_.Init(bep, &options) != 0) {
        LOG_WARNING("failed to init channel").put("remote", server);
    } else {
        stage_ = Stage::kSingle;
    }

    return *this;
}

BRpcServiceImpl& BRpcServiceImpl::Init(std::vector<Endpoint> servers) {
    stage_ = Stage::kError;
    if (servers.empty()) {
        return *this;
    }
    brpc::ParallelChannelOptions pchan_options;
    if (pchannel_.Init(&pchan_options) != 0) {
        LOG_WARNING("failed to init pchan");
        return *this;
    }
    call_mapper_ = new CallMapper();
    call_mapper_->AddRefManually();
    BYTE_DEFER({ call_mapper_->RemoveRefManually(); });

    for (const auto& server : servers) {
        brpc::ChannelOptions options;
        options.max_retry = 3;
        options.timeout_ms = 60'000;

        butil::EndPoint bep = ToBRpcEndpoint(server);
        auto chan = std::make_unique<brpc::Channel>();
        int rc = chan->Init(bep, &options);
        if (rc != 0) {
            LOG_WARNING("failed to init channel").put("remote", server);
            return *this;
        }

        rc = pchannel_.AddChannel(chan.get(), brpc::OWNS_CHANNEL, call_mapper_, nullptr);
        if (rc != 0) {
            LOG_WARNING("failed to add channel");
            return *this;
        }
        chan.release();
    }
    stage_ = Stage::kParallel;
    return *this;
}

Status BRpcServiceImpl::Call(const google::protobuf::MethodDescriptor* method,
                             const google::protobuf::Message* request,
                             google::protobuf::Message* response,
                             PartitionServerClientCallback user_cb) {
    if (UNLIKELY(stage_ == Stage::kError)) {
        return Status::Internal("bad channel");
    }
    auto cntl = new brpc::Controller();
    cntl->set_log_id(butil::fast_rand());
    auto done =
        brpc::NewCallback(this, &BRpcServiceImpl::Callback, cntl, response, std::move(user_cb));
    channel_.CallMethod(method, cntl, request, response, done);
    return Status::OK();
}

brpc::SubCall BRpcServiceImpl::CallMapper::Map(int i,
                                               const google::protobuf::MethodDescriptor* method,
                                               const google::protobuf::Message* /* request */,
                                               google::protobuf::Message* response) {
    return brpc::SubCall(method, *(requests + i), response->New(), brpc::DELETE_RESPONSE);
}

Status BRpcServiceImpl::ParallelCall(const google::protobuf::MethodDescriptor* method,
                                     const google::protobuf::Message** requests, size_t count,
                                     google::protobuf::Message* response,
                                     PartitionServerClientParallelCallback user_cb) {
    if (count != pchannel_.channel_count()) {
        return Status::Internal("endpoint count missmatch");
    }
    if (UNLIKELY(stage_ == Stage::kError)) {
        return Status::Internal("bad channel");
    }
    auto cntl = new brpc::Controller();
    cntl->set_log_id(butil::fast_rand());
    auto done = brpc::NewCallback(this, &BRpcServiceImpl::ParallelCallback, cntl, response,
                                  std::move(user_cb));
    call_mapper_->requests = requests;
    pchannel_.CallMethod(method, cntl, *requests, response, done);
    return Status::OK();
}

void BRpcServiceImpl::ParallelCallback(brpc::Controller* cntl, google::protobuf::Message* response,
                                       PartitionServerClientParallelCallback user_cb) {
    Status status = cntl->Failed() ? Status::Aborted(cntl->ErrorText()) : Status::OK();
    LOG_INFO("got parallel callback").put("status", status).put("sub_count", cntl->sub_count());
    std::vector<std::pair<Status, google::protobuf::Message*>> responses;
    CHECK_GT(cntl->sub_count(), 0) << this;
    for (int i = 0; i < cntl->sub_count(); i++) {
        const brpc::Controller* sub_cntl = cntl->sub(i);
        CHECK(!status.ok() || sub_cntl) << this;
        if (!status.ok() || sub_cntl->Failed()) {
            LOG_INFO("failed for sub request")
                .put("error", sub_cntl->ErrorText())
                .put("remote", sub_cntl->remote_side());
            responses.push_back(std::make_pair(
                status.ok() ? Status::Aborted(sub_cntl->ErrorText()) : status, nullptr));
        } else {
            responses.push_back(std::make_pair(Status::OK(), sub_cntl->response()));
        }
    }
    user_cb(responses);
    delete response;
    delete cntl;
}

void BRpcServiceImpl::Callback(brpc::Controller* cntl, google::protobuf::Message* response,
                               PartitionServerClientCallback user_cb) {
    Status status = cntl->Failed() ? Status::Aborted(cntl->ErrorText()) : Status::OK();
    user_cb(std::move(status), response);
    delete response;
    delete cntl;
}

}  // namespace metaserver
}  // namespace bcache2

