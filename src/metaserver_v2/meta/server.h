// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "bthread/bthread.h"

#include "common/status.h"
#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/serializable.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

struct GlobalNUMANodeId {
    explicit GlobalNUMANodeId(uint64_t id) : id(id) {}
    GlobalNUMANodeId(uint32_t server_id, uint32_t inner_id)
        : id((static_cast<uint64_t>(server_id) << 32) | inner_id) {}
    uint32_t GetServerId() const { return id >> 32; }
    uint32_t GetInnerId() const { return id & 0xFFFFFFFFUL; }
    uint64_t GetId() const { return id; }

    const uint64_t id;
};

using numa_node_id_t = GlobalNUMANodeId;
static_assert(sizeof(numa_node_id_t) == sizeof(uint64_t));

class Server;

class Node {  // aka. NUMA Node
 public:
    explicit Node(const NUMANode& info, Server* server);

    uint64_t GetId();
    Server* GetServer();
    void Detach();

    void AddIntentPartition(uint64_t id);
    void RemoveIntentPartition(uint64_t id);
    void CommitIntentPartition(uint64_t id);
    void RemovePartition(uint64_t id);
    std::set<uint64_t> GetPartitionIds(bool with_intent = false);
    size_t GetPartitionCount(bool with_intent = false);
    NUMANode GetInfo();

 private:
    friend class Server;

    const NUMANode info_;
    Server* server_;

    bthread::Mutex mu_;
    std::set<uint64_t> partition_ids_{};

    // Note: below are out of fsm
    std::set<uint64_t> intent_partition_ids_{};
};

using NodePtr = std::shared_ptr<Node>;

class ServerStats {
 public:
    int64_t GetLastHeartbeatTimeUs() const;
    void SetLastHeartbeatTimeUs(int64_t);

    void SetReportedBootTimeUs(int64_t);
    int64_t GetReportedBootTimeUs() const;
    void MarkRebootDetected();
    bool IsRebootDetected() const;

    void SetBinaryVersion(std::string v);
    std::string GetBinaryVersion() const;

    std::string ToString() const;

 private:
    mutable bthread::Mutex mu_;
    int64_t last_heartbeat_time_us_{0};
    int64_t reported_boot_time_us_{0};
    bool reboot_detected_{false};
    std::string binary_version_{};
};

class Server : public Serializable, public DeepCopy {
 public:
    Server() = default;
    explicit Server(ServerInfo info);
    ~Server();

    uint32_t GetId();
    void SetId(uint32_t id);
    Location GetLocation();
    void SetLocationTag(const std::string& tag);
    Endpoint GetEndpoint();

    std::string GetName() const;
    ServerState GetState();
    void SetState(ServerState state);
    void SetFrozenState(int64_t ts, FreezeServerReason reason);
    ServerInfo GetInfo();
    std::vector<NodePtr> GetNodes();
    Status GetNode(uint64_t gid, NodePtr* node);

    ServerStats* MutableRealtimeStats();
    const ServerStats& RealtimeStats();

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;
    Status LoadSnapshot(google::protobuf::io::FileInputStream* stream);
    Status DumpSnapshot(google::protobuf::io::FileOutputStream* stream);
    std::string GetSerializeTypeName() override { return "server"; }
    Status SerializeToString(std::string* output) override;
    Status ParseFromString(const std::string& input) override;

 private:
    std::string name_;

    bthread::Mutex mu_;
    ServerInfo info_;
    std::map<uint64_t, NodePtr> nodes_;

    // Note: below are out of fsm
    ServerStats rt_stats_;
};

using ServerPtr = std::shared_ptr<Server>;

}  // namespace metaserver
}  // namespace bcache2

