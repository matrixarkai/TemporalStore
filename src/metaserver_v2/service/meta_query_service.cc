// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/service/meta_query_service.h"

#include "butil/endpoint.h"
#include "byteraft/include/raft_node.h"
#include "spdlog/fmt/fmt.h"

#include "common/status.h"
#include "metaserver_v2/metrics.h"

namespace bcache2 {
namespace metaserver {

void MetaQueryServiceImpl::GetTableTopo(google::protobuf::RpcController* ctrl,
                                        const GetTableTopoRequest* request,
                                        GetTableTopoResponse* response,
                                        google::protobuf::Closure* done) {
    MS_METRIC(meta_query_count)->Add(1);
    LatencyMetricsRecord record(&(MS_METRIC(meta_query_latency_us)));
    brpc::ClosureGuard done_guard(done);
    const bool is_leader_ready = raft_server_->IsLeaderReady();
    if (!is_leader_ready && !FLAGS_metaserver_meta_query_allow_read_stale) {
        // TODO(wuzhenyu) force redirect if raft index is too old
        byteraft::NodeId leader = raft_server_->LeaderNode();
        butil::EndPoint ep;
        int rc = butil::str2endpoint(leader.raft_addr.c_str(), &ep);
        if (rc != 0) {
            response->mutable_status()->set_code(kUnavailable);
            response->mutable_status()->set_message("no leader found");
            MS_METRIC(meta_query_fail_count)->Add(1);

            return;
        }
        response->set_redirect_endpoint(
            fmt::format("{}:{}", butil::ip2str(ep.ip).c_str(), FLAGS_metaserver_server_port));
        return;
    }

    Status status =
        puber_->Query(request->old_topo_version(), request->namespace_(), request->table_name(),
                      request->idc(), request->compress(), response);
    if (!status.ok() && !status.IsCancelled()) {
        *response->mutable_status() = status.ToRpcStatus();
        MS_METRIC(meta_query_fail_count)->Add(1);
        return;
    }

    MS_METRIC(meta_query_bytes)->Add(response->ByteSize());
}

}  // namespace metaserver
}  // namespace bcache2
