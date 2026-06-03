// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/meta/proxy.h"

#include <mutex>
#include <utility>
#include <vector>

#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "common/proto_enhance.h"
#include "metaserver_v2/meta/namespace.h"

namespace bcache2 {
namespace metaserver {

Proxy::Proxy(ProxyInfo info) : name_(to_string(info.endpoint())), info_(std::move(info)) {}

std::string Proxy::GetName() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return name_;
}

void Proxy::SetName(std::string name) {
    std::lock_guard<bthread::Mutex> _(mu_);
    name_ = name;
}

std::string Proxy::GetNamespaceName() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.namespace_name();
}

void Proxy::SetId(uint32_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_id(id);
}

uint32_t Proxy::GetId() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.id();
}

Endpoint Proxy::GetEndpoint() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.endpoint();
}

ProxyInfo Proxy::GetInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_;
}

ProxyState Proxy::GetState() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.state();
}

void Proxy::SetState(ProxyState state) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_state(state);
}

Location Proxy::GetLocation() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.location();
}

ProxyStats* Proxy::MutableRealtimeStats() { return &rt_stats_; }
const ProxyStats& Proxy::RealtimeStats() { return rt_stats_; }

ProxyGroup* Proxy::GetProxyGroup() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return group_;
}

void Proxy::Attach(ProxyGroup* proxy_group, const std::string& ns_name) {
    std::lock_guard<bthread::Mutex> _(mu_);
    CHECK(group_ == nullptr);
    group_ = proxy_group;
    info_.set_namespace_name(ns_name);
}

void Proxy::Detach() {
    std::lock_guard<bthread::Mutex> _(mu_);
    group_ = nullptr;
    info_.clear_namespace_name();
}

bool Proxy::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<Proxy*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (name_ != rhs->name_) {
        return false;
    }
    if (!ProtoEqual(info_, rhs->info_)) {
        return false;
    }

    return true;
}

void Proxy::DeepCopyTo(DeepCopy* rhs_base) {
    // Note: Proxy instance cannot be copied from here
    auto rhs = static_cast<Proxy*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->name_ = name_;
    rhs->info_ = info_;
}

Status Proxy::SerializeToString(std::string* output) {
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!info_.SerializeToString(output)) {
        return Status::Internal("Serialize Error");
    }
    return Status::OK();
}

Status Proxy::ParseFromString(const std::string& input) {
    decltype(info_) info;
    if (!info.ParseFromString(input)) {
        return Status::Internal("Parse Error");
    }
    std::lock_guard<bthread::Mutex> _(mu_);
    name_ = to_string(info.endpoint());
    info_ = info;
    return Status::OK();
}

Status Proxy::DumpSnapshot(google::protobuf::io::FileOutputStream* stream) {
    return PackToStream(stream);
}

Status Proxy::LoadSnapshot(google::protobuf::io::FileInputStream* stream) {
    return UnPackFromStream(stream);
}

////////

ProxyGroupInfo ProxyGroup::GetInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_;
}

Location ProxyGroup::GetPlacement() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.placement();
}

void ProxyGroup::UpdateInfo(const ProxyGroupInfo& info) {
    std::lock_guard<bthread::Mutex> _(mu_);
    uint64_t original_version = info_.config().version();
    info_ = info;
    info_.mutable_config()->set_version(original_version + 1);
}

size_t ProxyGroup::GetProxyCount() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return proxies_.size();
}

Status ProxyGroup::AddProxy(ProxyPtr proxy) {
    std::lock_guard<bthread::Mutex> _(mu_);
    if (proxy->GetProxyGroup() != nullptr) {
        return Status::FailedPrecondition("proxy has group");
    }
    proxy->SetState(ProxyState::PROXY_NORMAL);
    proxy->Attach(this, info_.namespace_name());
    proxies_[proxy->GetId()] = proxy;
    return Status::OK();
}

Status ProxyGroup::RemoveProxy(ProxyPtr proxy) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = proxies_.find(proxy->GetId());
    if (iter == proxies_.end()) {
        return Status::NotFound("");
    }
    proxy->SetState(ProxyState::PROXY_IDLE);
    proxy->Detach();
    proxies_.erase(iter);
    return Status::OK();
}

void ProxyGroup::RemoveAllProxies() {
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& p : proxies_) {
        p.second->Detach();
    }
    proxies_.clear();
}

std::vector<ProxyPtr> ProxyGroup::ListAllProxies() {
    std::vector<ProxyPtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& p : proxies_) {
        result.push_back(p.second);
    }
    return result;
}

bool ProxyGroup::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<ProxyGroup*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!ProtoEqual(info_, rhs->info_)) {
        LOG_INFO("proto info not equal");
        return false;
    }

    if (!MapEqual(proxies_, rhs->proxies_)) {
        LOG_INFO("proxies not equal");
        return false;
    }
    return true;
}

void ProxyGroup::DeepCopyTo(DeepCopy* rhs_base) {
    // Note: Proxy instance cannot be copied from here
    auto rhs = static_cast<ProxyGroup*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->info_ = info_;
}

Status ProxyGroup::DumpSnapshot(google::protobuf::io::FileOutputStream* stream) {
    // Note: Proxy instance is not dumped from here
    return PackToStream(stream);
}

Status ProxyGroup::LoadSnapshot(google::protobuf::io::FileInputStream* stream) {
    return UnPackFromStream(stream);
}

//////

ProxyCluster::ProxyCluster(Namespace* ns) : ns_(ns) {}
Namespace* ProxyCluster::GetNamespace() { return ns_; }

Status ProxyCluster::CreateOrUpdateProxyGroup(const ProxyGroupInfo& info) {
    const Location& loc = info.placement();
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = groups_.find(loc);
    if (iter == groups_.end()) {
        auto group = std::make_shared<ProxyGroup>(info);

        groups_.emplace(std::make_pair(loc, group));
    } else {
        ProxyGroupPtr group = iter->second;
        group->UpdateInfo(info);
    }
    return Status::OK();
}

void ProxyCluster::PutProxyGroup(ProxyGroupPtr group) {
    const Location& loc = group->GetPlacement();
    std::lock_guard<bthread::Mutex> _(mu_);
    groups_[loc] = std::move(group);
}

Status ProxyCluster::DropProxyGroup(const ProxyGroupPtr& group) {
    Location loc = group->GetInfo().placement();
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = groups_.find(loc);
    if (iter == groups_.end()) {
        return Status::NotFound("group not found");
    }
    groups_.erase(iter);
    return Status::OK();
}

Status ProxyCluster::SearchProxyGroup(const Location& loc, ProxyGroupPtr* group) {
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& p : groups_) {
        if (BelongsTo(loc, p.first)) {
            *group = p.second;
            return Status::OK();
        }
    }
    return Status::NotFound("");
}

Status ProxyCluster::GetProxyGroup(const Location& loc, ProxyGroupPtr* group) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = groups_.find(loc);
    if (iter == groups_.end()) {
        return Status::NotFound("");
    }
    *group = iter->second;
    return Status::OK();
}

std::vector<ProxyGroupPtr> ProxyCluster::ListAllProxyGroups() {
    std::vector<ProxyGroupPtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& p : groups_) {
        result.push_back(p.second);
    }
    return result;
}

bool ProxyCluster::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<ProxyCluster*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!MapEqual(groups_, rhs->groups_)) {
        LOG_INFO("proxy group not equal");
        return false;
    }

    return true;
}

void ProxyCluster::DeepCopyTo(DeepCopy* rhs_base) {
    auto rhs = static_cast<ProxyCluster*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& pair : groups_) {
        auto pg = std::make_shared<ProxyGroup>(ProxyGroupInfo());
        pair.second->DeepCopyTo(pg.get());
        rhs->groups_[pair.first] = std::move(pg);
    }
}

//////

void ConsulMap::UpdateReservedConsulNames(std::unordered_set<std::string> v) {
    std::lock_guard<bthread::Mutex> _(mu_);
    std::swap(v, reserved_consul_names_);
}

Status ConsulMap::Validate(const ProxyClusterPtr& cluster, const std::string& name) {
    std::lock_guard<bthread::Mutex> _(mu_);
    if (reserved_consul_names_.count(name) > 0) {
        return Status::FailedPrecondition(fmt::format("reserved consul name"));
    }

    auto iter = consul_to_ns_map_.find(name);
    if (iter == consul_to_ns_map_.end()) {
        return Status::OK();
    }
    const uint32_t ns_id = cluster->GetNamespace()->GetId();
    if (ns_id != iter->second) {
        return Status::FailedPrecondition("consul name already used");
    }
    return Status::OK();
}

Status ConsulMap::Calibrate(const ProxyClusterPtr& cluster) {
    std::lock_guard<bthread::Mutex> _(mu_);
    const uint32_t ns_id = cluster->GetNamespace()->GetId();
    std::unordered_set<std::string> new_names;
    for (auto& group : cluster->ListAllProxyGroups()) {
        auto names = group->GetInfo().config().consul_names();
        for (const auto& name : names) {
            new_names.insert(name);
        }
    }
    auto iter = ns_to_consul_map_.find(ns_id);
    if (iter != ns_to_consul_map_.end()) {
        const auto& original_names = iter->second;
        std::vector<std::string> added, removed;
        std::set_difference(new_names.begin(), new_names.end(), original_names.begin(),
                            original_names.end(), std::back_inserter(added));
        std::set_difference(original_names.begin(), original_names.end(), new_names.begin(),
                            new_names.end(), std::back_inserter(removed));
        for (const auto& name : removed) {
            consul_to_ns_map_.erase(name);
        }
        for (const auto& name : added) {
            auto p = consul_to_ns_map_.emplace(std::make_pair(name, ns_id));
            if (!p.second) {
                LOG_WARNING("name already exists")
                    .put("name", name)
                    .put("ns_id", ns_id)
                    .put("pre_ns_id", p.first->second);
                return Status::Internal("name already exists");
            }
        }
    } else {
        for (const auto& name : new_names) {
            auto p = consul_to_ns_map_.emplace(std::make_pair(name, ns_id));
            if (!p.second) {
                LOG_WARNING("name already exists")
                    .put("name", name)
                    .put("ns_id", ns_id)
                    .put("pre_ns_id", p.first->second);
                return Status::Internal("name already exists");
            }
        }
    }
    ns_to_consul_map_[ns_id] = new_names;
    return Status::OK();
}

bool ConsulMap::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<ConsulMap*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!SetEqual(reserved_consul_names_, rhs->reserved_consul_names_)) {
        LOG_INFO("reserved_consul_names not equal")
            .put("mine_count", reserved_consul_names_.size())
            .put("rhs", rhs->reserved_consul_names_.size());

        return false;
    }
    if (ns_to_consul_map_.size() != rhs->ns_to_consul_map_.size()) {
        LOG_INFO("ns_to_consul_map not equal")
            .put("mine_count", ns_to_consul_map_.size())
            .put("rhs", rhs->ns_to_consul_map_.size());
        return false;
    }
    for (auto& pair : ns_to_consul_map_) {
        if (!SetEqual(pair.second, rhs->ns_to_consul_map_[pair.first])) {
            return false;
        }
    }
    if (!MapEqual2(consul_to_ns_map_, rhs->consul_to_ns_map_)) {
        LOG_INFO("consul_to_ns_map not equal")
            .put("mine_count", consul_to_ns_map_.size())
            .put("rhs", rhs->consul_to_ns_map_.size());
        return false;
    }
    return true;
}

void ConsulMap::DeepCopyTo(DeepCopy* rhs_base) {
    // TODO(wuzhenyu)
    auto rhs = static_cast<ConsulMap*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->reserved_consul_names_ = reserved_consul_names_;
    rhs->ns_to_consul_map_ = ns_to_consul_map_;
    rhs->consul_to_ns_map_ = consul_to_ns_map_;
}

Status ConsulMap::Remove(const ProxyClusterPtr& cluster) {
    std::lock_guard<bthread::Mutex> _(mu_);
    const uint32_t ns_id = cluster->GetNamespace()->GetId();
    auto iter = ns_to_consul_map_.find(ns_id);
    if (iter != ns_to_consul_map_.end()) {
        const auto& names = iter->second;
        for (const auto& name : names) {
            consul_to_ns_map_.erase(name);
        }
        ns_to_consul_map_.erase(iter);
    }
    return Status::OK();
}

//////

int64_t ProxyStats::GetLastHeartbeatTimeUs() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return last_heartbeat_time_us_;
}

int64_t ProxyStats::GetReportedBootTimeUs() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return reported_boot_time_us_;
}

void ProxyStats::SetLastHeartbeatTimeUs(int64_t t) {
    std::lock_guard<bthread::Mutex> _(mu_);
    last_heartbeat_time_us_ = t;
}

void ProxyStats::SetReportedBootTimeUs(int64_t t) {
    std::lock_guard<bthread::Mutex> _(mu_);
    reported_boot_time_us_ = t;
}

void ProxyStats::SetBinaryVersion(std::string v) {
    std::lock_guard<bthread::Mutex> _(mu_);
    binary_version_ = std::move(v);
}

std::string ProxyStats::GetBinaryVersion() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return binary_version_;
}

std::string ProxyStats::ToString() const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return fmt::format(
        "last_heartbeat_time_us: {}\n"
        "boot_time: {}\nversion: {}\n",
        last_heartbeat_time_us_, reported_boot_time_us_ / 1'000'000, binary_version_);
}

}  // namespace metaserver
}  // namespace bcache2

