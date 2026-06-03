// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include "common/status.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/raft_server.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

class ManageServiceImpl : public ManageService {
 public:
    ManageServiceImpl(RaftServer* s, Metabase* mb) : raft_server_(s), metabase_(mb) {}

    void UpdateManageInfo(google::protobuf::RpcController* controller,
                          const UpdateManageInfoRequest* request, AckResponse* response,
                          google::protobuf::Closure* done) override;

    void AddServer(google::protobuf::RpcController* controller, const AddServerRequest* request,
                   AckResponse* response, google::protobuf::Closure* done) override;
    void FreezeServer(google::protobuf::RpcController* controller,
                      const FreezeServerRequest* request, AckResponse* response,
                      google::protobuf::Closure* done) override;
    void DropServer(google::protobuf::RpcController* controller, const DropServerRequest* request,
                    AckResponse* response, google::protobuf::Closure* done) override;
    void UpdateServer(google::protobuf::RpcController* controller,
                      const UpdateServerRequest* request, AckResponse* response,
                      google::protobuf::Closure* done) override;

    void AddProxy(google::protobuf::RpcController* controller, const AddProxyRequest* request,
                  AckResponse* response, google::protobuf::Closure* done) override;
    void FreezeProxy(google::protobuf::RpcController* controller, const FreezeProxyRequest* request,
                     AckResponse* response, google::protobuf::Closure* done) override;
    void DropProxy(google::protobuf::RpcController* controller, const DropProxyRequest* request,
                   AckResponse* response, google::protobuf::Closure* done) override;

    void AddNamespace(google::protobuf::RpcController* controller,
                      const AddNamespaceRequest* request, AckResponse* response,
                      google::protobuf::Closure* done) override;

    void AddTable(google::protobuf::RpcController* controller, const AddTableRequest* request,
                  AckResponse* response, google::protobuf::Closure* done) override;
    void UpdateTable(google::protobuf::RpcController* controller,
                     const UpdateTableRequestV2* request, AckResponse* response,
                     google::protobuf::Closure* done) override;
    void FreezeTable(google::protobuf::RpcController* controller, const FreezeTableRequest* request,
                     AckResponse* response, google::protobuf::Closure* done) override;
    void DropTable(google::protobuf::RpcController* controller, const DropTableRequest* request,
                   AckResponse* response, google::protobuf::Closure* done) override;

    void FreezePartition(google::protobuf::RpcController* controller,
                         const FreezePartitionRequest* request, AckResponse* response,
                         google::protobuf::Closure* done) override;
    void DropPartition(google::protobuf::RpcController* controller,
                       const DropPartitionRequest* request, AckResponse* response,
                       google::protobuf::Closure* done) override;
    void FinishLoadPartition(google::protobuf::RpcController* controller,
                             const LoadPartitionFinishRequest* request, AckResponse* response,
                             google::protobuf::Closure* done) override;

    void PutProxyGroup(google::protobuf::RpcController* controller,
                       const PutProxyGroupRequest* request, AckResponse* response,
                       google::protobuf::Closure* done) override;

    void DropProxyGroup(google::protobuf::RpcController* controller,
                        const DropProxyGroupRequest* request, AckResponse* response,
                        google::protobuf::Closure* done) override;

    void MuteMetaChange(google::protobuf::RpcController* controller, const EmptyRequest* request,
                        AckResponse* response, google::protobuf::Closure* done) override;

    void ResumeMetaChange(google::protobuf::RpcController* controller, const EmptyRequest* request,
                          AckResponse* response, google::protobuf::Closure* done) override;

 private:
    Status SanitizeRequest(brpc::Controller* cntl, RequestId* id);

 private:
    RaftServer* const raft_server_{nullptr};  // NOT OWNED
    Metabase* const metabase_{nullptr};       // NOT OWNED
};

}  // namespace metaserver
}  // namespace bcache2
