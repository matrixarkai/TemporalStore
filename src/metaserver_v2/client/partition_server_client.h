// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/parallel_channel.h"

#include "common/logging.h"
#include "common/status.h"
#include "metaserver_v2/meta/partition.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace metaserver {

using PartitionServerClientCallback = std::function<void(Status, google::protobuf::Message*)>;
using PartitionServerClientParallelCallback =
    std::function<void(const std::vector<std::pair<Status, google::protobuf::Message*>>&)>;

/// template ServiceImpl for pluggable purpose in test and chaos scenarios
template <typename ServiceImpl>
class PartitionServerClient {
 public:
    PartitionServerClient() = default;
    ~PartitionServerClient() = default;

    Status Load(const PartitionPtr& partition, const PlacementSpec& placement, bool async_load,
                PartitionServerClientCallback cb) {
        LoadRequest request;
        Status status = SerializeToLoadRequest(partition, async_load, &request);
        if (!status.ok()) {
            LOG_WARNING("failed to serialize to load request").put("result", status);
            return status;
        }

        LoadResponse* response = new LoadResponse();
        const google::protobuf::MethodDescriptor* md = descriptor_->FindMethodByName("Load");
        ServiceImpl impl;
        return impl.Init(placement.server()).Call(md, &request, response, std::move(cb));
    }

    Status Unload(const PartitionPtr& partition, google::protobuf::Closure* done) {
        return Status::Internal("not implemented");
    }

    Status UpdateMembership(const std::vector<PartitionPtr>& partitions,
                            const MembershipInfo& membership_info,
                            PartitionServerClientParallelCallback cb) {
        if (partitions.empty()) {
            return Status::Aborted("empty set");
        }
        std::vector<Endpoint> endpoints;
        std::vector<UpdateMembershipRequest> requests;
        for (auto& partition : partitions) {
            const PlacementSpec& ps = partition->GetPlacementActual();
            if (!ps.has_server() || ps.server().port() == 0) {
                continue;
            }
            requests.emplace_back();
            auto& req = requests.back();
            req.set_partition_id(partition->GetId());
            *(req.mutable_membership()) = membership_info;
            endpoints.push_back(ps.server());
        }

        std::vector<const google::protobuf::Message*> request_pointers;
        for (auto& req : requests) {
            request_pointers.push_back(&req);
        }
        AckResponse* response = new AckResponse();
        const google::protobuf::MethodDescriptor* md =
            descriptor_->FindMethodByName("UpdateMembership");
        ServiceImpl impl;
        return impl.Init(std::move(endpoints))
            .ParallelCall(md, request_pointers.data(), request_pointers.size(), response,
                          std::move(cb));
    }

 private:
    const google::protobuf::ServiceDescriptor* descriptor_{ServerService::descriptor()};
};

///////// implement

class BRpcServiceImpl {
 public:
    BRpcServiceImpl& Init(const Endpoint&);
    BRpcServiceImpl& Init(std::vector<Endpoint>);

    Status Call(const google::protobuf::MethodDescriptor* method,
                const google::protobuf::Message* request, google::protobuf::Message* response,
                PartitionServerClientCallback user_cb);

    Status ParallelCall(const google::protobuf::MethodDescriptor* method,
                        const google::protobuf::Message** request, size_t count,
                        google::protobuf::Message* response,
                        PartitionServerClientParallelCallback user_cb);

    /// invoked by brpc
    void Callback(brpc::Controller* cntl, google::protobuf::Message* response,
                  PartitionServerClientCallback user_cb);
    void ParallelCallback(brpc::Controller* cntl, google::protobuf::Message* response,
                          PartitionServerClientParallelCallback user_cb);

 private:
    enum Stage { kError, kSingle, kParallel };

    struct CallMapper : public brpc::CallMapper {
        brpc::SubCall Map(int channel_index /*starting from 0*/,
                          const google::protobuf::MethodDescriptor* method,
                          const google::protobuf::Message* request,
                          google::protobuf::Message* response) override;

        const google::protobuf::Message** requests;
    };

 private:
    Stage stage_{Stage::kError};
    brpc::Channel channel_;
    brpc::ParallelChannel pchannel_;
    CallMapper* call_mapper_{nullptr};
};

/// test only
class MockServiceImpl {
 public:
    template <typename V>
    MockServiceImpl& Init(V) {
        return *this;
    }

    Status Call(const google::protobuf::MethodDescriptor* method,
                const google::protobuf::Message* request, google::protobuf::Message* response,
                PartitionServerClientCallback user_cb) {
        user_cb(Status::OK(), response);
        delete response;
        return Status::OK();
    }

    Status ParallelCall(const google::protobuf::MethodDescriptor* method,
                        const google::protobuf::Message** requests, size_t count,
                        google::protobuf::Message* response,
                        PartitionServerClientParallelCallback user_cb) {
        std::vector<std::unique_ptr<google::protobuf::Message>> holders;
        std::vector<std::pair<Status, google::protobuf::Message*>> responses;
        for (size_t i = 0; i < count; i++) {
            holders.emplace_back(response->New());
            responses.push_back(std::make_pair(Status::OK(), holders.back().get()));
        }
        user_cb(responses);
        delete response;
        return Status::OK();
    }
};

#ifdef BCACHE2_MS_TEST_ENV
using PartitionServerClientImpl = PartitionServerClient<MockServiceImpl>;
#else
using PartitionServerClientImpl = PartitionServerClient<BRpcServiceImpl>;
#endif

}  // namespace metaserver
}  // namespace bcache2

