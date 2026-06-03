// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/raft_connector.h"

#include <cstdlib>
#include <mutex>
#include <string>
#include <utility>

#include "bthread/countdown_event.h"
#include "butil/iobuf.h"
#include "butil/raw_pack.h"              // RawPacker RawUnpacker
#include "byte/include/macros.h"         // BYTE_DEFER
#include "google/protobuf/descriptor.h"  // MethodDescriptor
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "google/protobuf/message.h"

#include "common/logging.h"

namespace bcache2 {
namespace metaserver {

RaftConnector::ParseResult RaftConnector::ParseLogData(const std::string& data) {
    ParseResult result;
    result.result = Status::OK();
    butil::IOBuf source;
    source.append(data);
    char buf[kHeaderSize];
    do {
        uint32_t n = source.cutn(buf, kHeaderSize);
        if (n != kHeaderSize) {
            result.result = Status::Internal("parse failed, not enough");
            break;
        }
        uint32_t magic;
        uint32_t meta_size;
        butil::RawUnpacker(buf).unpack32(magic).unpack32(meta_size);
        if (magic != kLogMagic) {
            result.result = Status::Internal("parse failed, magic wrong");
            break;
        }

        butil::IOBuf meta_source;
        n = source.cutn(&meta_source, meta_size);
        if (n != meta_size) {
            result.result = Status::Internal("parse failed, not enough");
            break;
        }
        butil::IOBufAsZeroCopyInputStream wrapper(meta_source);
        if (!result.meta.ParseFromZeroCopyStream(&wrapper)) {
            result.result = Status::Internal("parse failed, meta invalid");
            break;
        }

        const google::protobuf::Descriptor* desc =
            google::protobuf::DescriptorPool::generated_pool()  //
                ->FindMessageTypeByName(result.meta.request_proto_type());
        if (desc == nullptr) {
            result.result = Status::Internal("parse failed, request type not found");
            break;
        }
        result.message.reset(
            google::protobuf::MessageFactory::generated_factory()->GetPrototype(desc)->New());
        butil::IOBufAsZeroCopyInputStream wrapper2(source);
        if (!result.message->ParseFromZeroCopyStream(&wrapper2)) {
            result.result = Status::Internal("parse failed, request invalid");
            break;
        }
    } while (false);
    return result;
}

uint64_t RaftConnector::AcquireContextId() {
    std::lock_guard<bthread::Mutex> lock_guard(mu_);
    return ctx_id_++;
}

/// header codec:
// +-----------------+------------------+---------------+
// | Version(4bytes) | MetaSize(4bytes) | MetaBody(...) |
// +-----------------+------------------+---------------+
//
void RaftConnector::PackLogHeader(butil::IOBuf* out, const LogMeta& meta) {
    const size_t meta_size = meta.ByteSizeLong();
    char* buf = new char[kHeaderSize + meta_size];
    butil::RawPacker(buf).pack32(kLogMagic).pack32(meta_size);
    if (!meta.SerializeToArray(buf + kHeaderSize, static_cast<int>(meta_size))) {
        LOG_FATAL("failed to serialize raft log meta");
        std::abort();
    }
    out->append(buf, kHeaderSize + meta_size);
    delete[] buf;
}

Status RaftConnector::ProposeInternal(uint64_t ctx_id, std::string log_data) {
    LOG_INFO("try to propose raft").put("ctx_id", ctx_id);
    {
        std::lock_guard<bthread::Mutex> lock_guard(mu_);
        raft_ctx_map_.emplace(ctx_id, Context(ctx_id));
    }

    Status status;
    bthread::CountdownEvent sync_point;
    raft_node_->Propose(std::move(log_data), [&](const byte::Status& byteraft_status) {
        if (!byteraft_status.ok()) {
            status = Status::Internal(byteraft_status.ToString());
        }
        LOG_INFO("propose finish").put("ctx_id", ctx_id).put("status", status);
        sync_point.signal();
    });
    sync_point.wait();

    std::lock_guard<bthread::Mutex> lock_guard(mu_);
    if (status.ok()) {
        status = std::move(raft_ctx_map_[ctx_id].status);
    }
    raft_ctx_map_.erase(ctx_id);
    return status;
}

void RaftConnector::SetContextStatus(const LogMeta& meta, Status status) {
    std::lock_guard<bthread::Mutex> lock_guard(mu_);
    auto it = raft_ctx_map_.find(meta.ctx_id());
    if (it == raft_ctx_map_.end()) {
        // may be called during state machine replay
        // so return directly when ctx_id is not found
        return;
    }

    it->second.status = std::move(status);
}

}  // namespace metaserver
}  // namespace bcache2
