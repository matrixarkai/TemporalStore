// Copyright (c) 2020-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "bthread/bthread.h"
#include "butil/iobuf.h"
#include "butil/time.h"
#include "byteraft/include/raft_node.h"

#include "common/macros.h"
#include "common/status.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

/// Raft Client
/// TODO(wuzhenyu) propose to leader node not local node
class RaftConnector {
 public:
    struct Context {
        uint64_t id{0};
        Status status{};

        Context() = default;
        explicit Context(uint64_t id) : id(id) {}
    };

    struct ParseResult {
        Status result;
        LogMeta meta;
        std::unique_ptr<google::protobuf::Message> message;
    };

 public:
    explicit RaftConnector(std::shared_ptr<byteraft::RaftNode> node)
        : raft_node_(std::move(node)), ctx_id_(static_cast<uint64_t>(butil::gettimeofday_us())) {}

    template <typename Request>
    Status Propose(uint64_t log_id, MetaServerLogType type, const Request* request) {
        LogMeta meta;
        uint64_t ctx_id = AcquireContextId();
        meta.set_type(type);
        meta.set_request_proto_type(request->GetDescriptor()->full_name());
        meta.set_ctx_id(ctx_id);
        meta.set_log_id(log_id);

        butil::IOBuf result_buf;
        PackLogHeader(&result_buf, meta);
        butil::IOBufAsZeroCopyOutputStream wrapper(&result_buf);
        request->SerializeToZeroCopyStream(&wrapper);
        return ProposeInternal(ctx_id, result_buf.to_string());
    }

    ParseResult ParseLogData(const std::string& data);

    void SetContextStatus(const LogMeta& meta, Status status);

 private:
    uint64_t AcquireContextId();
    void PackLogHeader(butil::IOBuf* out, const LogMeta& meta);
    Status ProposeInternal(uint64_t ctx_id, std::string log_data);

 private:
    static constexpr uint32_t kLogMagic = 1;
    static constexpr size_t kHeaderSize = 8;

 private:
    std::shared_ptr<byteraft::RaftNode> raft_node_;

    bthread::Mutex mu_;
    uint64_t ctx_id_{0};                                  // GUARDED_BY(mu_)
    std::unordered_map<uint64_t, Context> raft_ctx_map_;  // GUARDED_BY(mu_)
};

}  // namespace metaserver
}  // namespace bcache2
