// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/meta/server.h"

#include <mutex>
#include <utility>
#include <vector>

#include "spdlog/fmt/fmt.h"

#include "common/proto_enhance.h"

namespace bcache2 {
namespace metaserver {

Node::Node(const NUMANode& info, Server* server) : info_(info), server_(server) {}

uint64_t Node::GetId() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.id();
}

void Node::Detach() {
    std::lock_guard<bthread::Mutex> _(mu_);
    server_ = nullptr;
}

Server* Node::GetServer() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return server_;
}

void Node::AddIntentPartition(uint64_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    intent_partition_ids_.insert(id);
}

void Node::RemoveIntentPartition(uint64_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    intent_partition_ids_.erase(id);
}

void Node::CommitIntentPartition(uint64_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    partition_ids_.insert(id);

    // Note: this behavior may failed because intent_partition_ids_
    // is not controlled by fsm
    intent_partition_ids_.erase(id);
}

void Node::RemovePartition(uint64_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    partition_ids_.erase(id);
}

size_t Node::GetPartitionCount(bool with_intent) {
    std::lock_guard<bthread::Mutex> _(mu_);
    size_t result = partition_ids_.size();
    if (with_intent) {
        result += intent_partition_ids_.size();
    }
    return result;
}

std::set<uint64_t> Node::GetPartitionIds(bool with_intent) {
    std::lock_guard<bthread::Mutex> _(mu_);
    std::set<uint64_t> result = partition_ids_;
    if (with_intent) {
        for (uint64_t id : intent_partition_ids_) {
            result.insert(id);
        }
    }
    return result;
}

NUMANode Node::GetInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_;
}

////////////

Server::Server(ServerInfo info) : name_(to_string(info.endpoint())), info_(std::move(info)) {}

Server::~Server() {
    for (auto p : nodes_) {
        p.second->Detach();
    }
}

std::string Server::GetName() const { return name_; }

void Server::SetId(uint32_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_id(id);
    nodes_.clear();
    for (int i = 0; i < info_.numa_nodes_size(); i++) {
        auto node_info = info_.mutable_numa_nodes(i);
        numa_node_id_t global_node_id(id, i + 1);
        node_info->set_id(global_node_id.id);
        auto node = std::make_shared<Node>(*node_info, this);
        nodes_.emplace(global_node_id.id, std::move(node));
    }
}

uint32_t Server::GetId() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.id();
}

Endpoint Server::GetEndpoint() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.endpoint();
}

ServerInfo Server::GetInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_;
}

ServerState Server::GetState() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.state();
}

void Server::SetState(ServerState state) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_state(state);
}

void Server::SetFrozenState(int64_t ts, FreezeServerReason reason) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_frozen_at(ts);
    info_.set_freeze_reason(reason);
    info_.set_state(SERVER_FROZEN);
}

Location Server::GetLocation() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.location();
}

void Server::SetLocationTag(const std::string& tag) {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.mutable_location()->set_tag(tag);
}

std::vector<NodePtr> Server::GetNodes() {
    std::vector<NodePtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto iter : nodes_) {
        result.push_back(iter.second);
    }
    return result;
}

Status Server::GetNode(uint64_t gid, NodePtr* node) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = nodes_.find(gid);
    if (iter == nodes_.end()) {
        return Status::NotFound("");
    }
    *node = iter->second;
    return Status::OK();
}

ServerStats* Server::MutableRealtimeStats() { return &rt_stats_; }
const ServerStats& Server::RealtimeStats() { return rt_stats_; }

Status Server::SerializeToString(std::string* output) {
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!info_.SerializeToString(output)) {
        return Status::Internal("Serialize Error");
    }
    return Status::OK();
}

Status Server::ParseFromString(const std::string& input) {
    decltype(info_) info;
    if (!info.ParseFromString(input)) {
        return Status::Internal("Parse Error");
    }
    const uint32_t id = info.id();
    {
        std::lock_guard<bthread::Mutex> _(mu_);
        name_ = to_string(info.endpoint());
        info_ = info;
    }
    SetId(id);
    return Status::OK();
}

bool Server::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<Server*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (name_ != rhs->name_) {
        LOG_INFO("name not equal").put("mine", name_).put("rhs", rhs->name_);
        return false;
    }
    if (!ProtoEqual(info_, rhs->info_)) {
        LOG_INFO("proto info not equal");
        return false;
    }
    for (auto& pair : nodes_) {
        if (pair.second->partition_ids_.size() != rhs->nodes_[pair.first]->partition_ids_.size()) {
            LOG_INFO("partition id count not equal");
            return false;
        }
    }
    return true;
}

void Server::DeepCopyTo(DeepCopy* rhs_base) {
    auto rhs = static_cast<Server*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->name_ = name_;
    rhs->info_ = info_;
    rhs->SetId(info_.id());
    for (auto& pair : rhs->nodes_) {
        // TODO(wuzhenyu) FIXME
        pair.second->partition_ids_ = nodes_[pair.first]->partition_ids_;
    }
}

Status Server::LoadSnapshot(google::protobuf::io::FileInputStream* stream) {
    return UnPackFromStream(stream);
}
Status Server::DumpSnapshot(google::protobuf::io::FileOutputStream* stream) {
    return PackToStream(stream);
}

////////////

int64_t ServerStats::GetLastHeartbeatTimeUs() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return last_heartbeat_time_us_;
}

int64_t ServerStats::GetReportedBootTimeUs() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return reported_boot_time_us_;
}

bool ServerStats::IsRebootDetected() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return reboot_detected_;
}

void ServerStats::SetLastHeartbeatTimeUs(int64_t t) {
    std::lock_guard<bthread::Mutex> _(mu_);
    last_heartbeat_time_us_ = t;
}

void ServerStats::SetReportedBootTimeUs(int64_t t) {
    std::lock_guard<bthread::Mutex> _(mu_);
    reported_boot_time_us_ = t;
}

void ServerStats::MarkRebootDetected() {
    std::lock_guard<bthread::Mutex> _(mu_);
    reboot_detected_ = true;
}

void ServerStats::SetBinaryVersion(std::string v) {
    std::lock_guard<bthread::Mutex> _(mu_);
    binary_version_ = std::move(v);
}

std::string ServerStats::GetBinaryVersion() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return binary_version_;
}

std::string ServerStats::ToString() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return fmt::format(
        "last_heartbeat_time_us: {}\n"
        "boot_time: {}\nversion: {}\n",
        last_heartbeat_time_us_, reported_boot_time_us_ / 1'000'000, binary_version_);
}

}  // namespace metaserver
}  // namespace bcache2

