// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/meta/metabase.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/proto_enhance.h"

namespace bcache2 {
namespace metaserver {

Status Metabase::Init() {
    server_location_manager_ = std::make_unique<LocationManager<Server>>();
    proxy_location_manager_ = std::make_unique<LocationManager<Proxy>>();
    ns_manager_ = std::make_unique<NamespaceManager>();
    manage_info_.Clear();
    return Status::OK();
}

void Metabase::SetRaftIndex(uint64_t idx) { raft_index_ = idx; }

uint64_t Metabase::GetRaftIndex() { return raft_index_; }

void Metabase::SetMuteMetaChange(bool v) { mute_meta_change_ = v; }
bool Metabase::GetMuteMetaChange() { return mute_meta_change_; }

Status Metabase::UpdateManageInfo(const ManagementInfo& minfo) {
    std::lock_guard<bthread::Mutex> _(mu_);
    if (minfo.reserved_namespace_name_list_size() > 0) {
        ns_manager_->UpdateReservedNamespaceNames({minfo.reserved_namespace_name_list().begin(),
                                                   minfo.reserved_namespace_name_list().end()});
    }
    if (minfo.reserved_table_name_list_size() > 0) {
        ns_manager_->UpdateReservedTableNames(
            {minfo.reserved_table_name_list().begin(), minfo.reserved_table_name_list().end()});
    }
    if (minfo.reserved_consul_name_list_size() > 0) {
        ns_manager_->UpdateReservedConsulNames(
            {minfo.reserved_consul_name_list().begin(), minfo.reserved_consul_name_list().end()});
    }
    manage_info_ = minfo;
    return Status::OK();
}

const ManagementInfo& Metabase::GetManageInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return manage_info_;
}

Status Metabase::LocatePartition(uint64_t id, PartitionPtr* partition) {
    partition_id_t pid(id);
    TablePtr table;
    Status status = ns_manager_->GetTable(pid.GetTableId(), &table);
    if (!status.ok()) {
        return status;
    }
    status = table->GetPartition(id, partition);
    return status;
}

bool Metabase::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<Metabase*>(rhs_base);
    if (raft_index_ != rhs->raft_index_) {
        LOG_INFO("raft index not equal").put("mine", raft_index_).put("rhs", rhs->raft_index_);
        return false;
    }

    if (mute_meta_change_ != rhs->mute_meta_change_) {
        LOG_INFO("mute_meta_change not equal")
            .put("mine", mute_meta_change_)
            .put("rhs", rhs->mute_meta_change_);
        return false;
    }
    if (!server_location_manager_->Equal(rhs->server_location_manager_.get())) {
        LOG_INFO("server location not equal");
        return false;
    }
    if (!proxy_location_manager_->Equal(rhs->proxy_location_manager_.get())) {
        LOG_INFO("proxy location not equal");
        return false;
    }
    if (!ns_manager_->Equal(rhs->ns_manager_.get())) {
        LOG_INFO("nsmgr not equal");
        return false;
    }
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!ProtoEqual(manage_info_, rhs->manage_info_)) {
        LOG_INFO("proto info not equal");
        return false;
    }
    return true;
}

void Metabase::DeepCopyTo(DeepCopy* rhs_base) {
    auto rhs = static_cast<Metabase*>(rhs_base);

    rhs->raft_index_ = raft_index_;
    rhs->mute_meta_change_ = mute_meta_change_.load();

    LOG_INFO("start to copy server location manager");
    server_location_manager_->DeepCopyTo(rhs->server_location_manager_.get());
    LOG_INFO("start to copy proxy location manager");
    proxy_location_manager_->DeepCopyTo(rhs->proxy_location_manager_.get());
    LOG_INFO("start to copy namespace manager");
    ns_manager_->DeepCopyTo(rhs->ns_manager_.get());
    LOG_INFO("combine proxy meta");
    rhs->CombineProxyMeta();
    LOG_INFO("combine server meta");
    rhs->CombineServerMeta();

    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->manage_info_ = manage_info_;
}

Status Metabase::DumpSnapshot(const std::string& dir) {
    butil::FilePath dst(dir);
    if (!butil::PathExists(dst) && !butil::CreateDirectory(dst, true)) {
        LOG_WARNING("failed to create directory");
        return Status::Internal("create directory failed");
    }

    Status result = DumpSnapshotInternal(dst);
    if (!result.ok()) {
        butil::DeleteFile(dst, true /* -r */);
    }
    return result;
}

Status Metabase::LoadSnapshot(const std::string& dir) {
    Status status = Init();
    RETURN_IF_STATUS_ERROR(status);

    butil::FilePath dst(dir);
    if (!butil::PathExists(dst)) {
        LOG_WARNING("directory not exists").put("path", dir);
        return Status::Internal("directory not exists");
    }

    LOG_INFO("start to read manifest");
    auto manifest_file = dst.Append("manifest");
    int fd = -1;
    do {
        fd = open(manifest_file.value().c_str(), O_RDONLY, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return Status::Internal("I/O Error, failed to open file");
    }
    google::protobuf::io::FileInputStream manifest_stream(fd);
    manifest_stream.SetCloseOnDelete(true);
    SnapshotManifest manifest;
    if (!manifest.ParseFromZeroCopyStream(&manifest_stream)) {
        return Status::Internal("Parse Error, manifest parse failed");
    }

    LOG_INFO("start to init metabase");
    status = UpdateManageInfo(manifest.manage_info());
    RETURN_IF_STATUS_ERROR(status);

    LOG_INFO("start to load resource");
    auto resource_file = dst.Append("resource");
    do {
        fd = open(resource_file.value().c_str(), O_RDONLY, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return Status::Internal("I/O Error, failed to open file");
    }
    google::protobuf::io::FileInputStream resource_stream(fd);
    resource_stream.SetCloseOnDelete(true);

    LOG_INFO("start to load server")
        .put("cursor", manifest.server_id_cursor())
        .put("count", manifest.server_count());
    auto server_loc_mgr = GetServerLocationManager();
    server_loc_mgr->SetIdCursor(manifest.server_id_cursor());
    for (size_t i = 0; i < manifest.server_count(); i++) {
        auto server = std::make_shared<Server>(ServerInfo());
        status = server->LoadSnapshot(&resource_stream);
        RETURN_IF_STATUS_ERROR(status);
        server_loc_mgr->Add(std::move(server));
    }

    LOG_INFO("start to load proxy")
        .put("cursor", manifest.proxy_id_cursor())
        .put("count", manifest.proxy_count());
    auto proxy_loc_mgr = GetProxyLocationManager();
    proxy_loc_mgr->SetIdCursor(manifest.proxy_id_cursor());
    for (size_t i = 0; i < manifest.proxy_count(); i++) {
        auto proxy = std::make_shared<Proxy>(ProxyInfo());
        status = proxy->LoadSnapshot(&resource_stream);
        RETURN_IF_STATUS_ERROR(status);
        proxy_loc_mgr->Add(std::move(proxy));
    }

    LOG_INFO("start to load namespace/table/partition");
    auto nsmgr = GetNamespaceManager();
    status = nsmgr->LoadSnapshot(dst, manifest);
    RETURN_IF_STATUS_ERROR(status);

    LOG_INFO("start to combine proxy meta");
    status = CombineProxyMeta();
    RETURN_IF_STATUS_ERROR(status);

    LOG_INFO("start to combine server meta");
    status = CombineServerMeta();
    RETURN_IF_STATUS_ERROR(status);

    raft_index_ = manifest.raft_index();
    mute_meta_change_ = manifest.mute_meta_change();
    return Status::OK();
}

Status Metabase::DumpSnapshotInternal(const butil::FilePath& dst) {
    SnapshotManifest manifest;
    manifest.set_raft_index(raft_index_);
    manifest.set_mute_meta_change(mute_meta_change_);
    *manifest.mutable_manage_info() = GetManageInfo();

    LOG_INFO("start to dump namespace/table/partition");
    auto nsmgr = GetNamespaceManager();
    Status status = nsmgr->DumpSnapshot(dst, &manifest);
    RETURN_IF_STATUS_ERROR(status);

    LOG_INFO("start to dump resource");
    auto resource_file = dst.Append("resource");
    int fd = -1;
    do {
        fd = open(resource_file.value().c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return Status::Internal("I/O Error, failed to open file");
    }
    google::protobuf::io::FileOutputStream resource_stream(fd);
    resource_stream.SetCloseOnDelete(true);
    auto server_loc_mgr = GetServerLocationManager();
    manifest.set_server_id_cursor(server_loc_mgr->GetIdCursor());
    std::vector<ServerPtr> servers = server_loc_mgr->ListAll();
    manifest.set_server_count(servers.size());
    for (auto& server : servers) {
        status = server->DumpSnapshot(&resource_stream);
        RETURN_IF_STATUS_ERROR(status);
    }
    auto proxy_loc_mgr = GetProxyLocationManager();
    manifest.set_proxy_id_cursor(proxy_loc_mgr->GetIdCursor());
    std::vector<ProxyPtr> proxies = proxy_loc_mgr->ListAll();
    manifest.set_proxy_count(proxies.size());
    for (auto& proxy : proxies) {
        status = proxy->DumpSnapshot(&resource_stream);
        RETURN_IF_STATUS_ERROR(status);
    }
    if (!resource_stream.Flush()) {
        LOG_WARNING("failed to flush").put("errno", strerror(resource_stream.GetErrno()));
        return Status::Internal("I/O Error, failed to flush");
    }

    LOG_INFO("start to dump manifest");
    auto manifest_file = dst.Append("manifest");
    do {
        fd = open(manifest_file.value().c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return Status::Internal("I/O Error, failed to open file");
    }
    google::protobuf::io::FileOutputStream manifest_stream(fd);
    manifest_stream.SetCloseOnDelete(true);
    if (!manifest.SerializeToZeroCopyStream(&manifest_stream)) {
        return Status::Internal("Serialize Error");
    }
    if (!manifest_stream.Flush()) {
        LOG_WARNING("failed to flush").put("errno", strerror(manifest_stream.GetErrno()));
        return Status::Internal("I/O Error, failed to flush");
    }
    return Status::OK();
}

Status Metabase::CombineProxyMeta() {
    auto nsmgr = GetNamespaceManager();
    auto proxy_loc_mgr = GetProxyLocationManager();
    std::vector<ProxyPtr> proxies = proxy_loc_mgr->ListAll();
    for (auto& proxy : proxies) {
        const std::string ns_name = proxy->GetNamespaceName();
        if (ns_name.empty()) {
            continue;
        }
        NamespacePtr ns = nullptr;
        Status status = nsmgr->Get(ns_name, &ns);
        if (!status.ok()) {
            LOG_WARNING("failed to get namespace")
                .put("ns", ns_name)
                .put("proxy", to_string(proxy->GetInfo().location()));
            return Status::Internal("ns not found");
        }
        ProxyGroupPtr proxy_group = nullptr;
        status = ns->GetProxyCluster()->SearchProxyGroup(proxy->GetLocation(), &proxy_group);
        if (!status.ok()) {
            LOG_WARNING("failed to get proxy group")
                .put("ns", ns_name)
                .put("proxy", to_string(proxy->GetInfo().location()));
            return Status::Internal("proxy group not found");
        }
        LOG_INFO("combine proxy to proxy cluster")
            .put("proxy", to_string(proxy->GetLocation()))
            .put("ns", ns_name);
        RETURN_IF_STATUS_ERROR(proxy_group->AddProxy(proxy));
    }
    return Status::OK();
}

Status Metabase::CombineServerMeta() {
    auto nsmgr = GetNamespaceManager();
    auto server_loc_mgr = GetServerLocationManager();
    for (auto& ns : nsmgr->List()) {
        for (auto& table : ns->List(true /* with frozen */)) {
            LOG_INFO("start to combine server meta").put("table", table->GetFullName());
            for (auto& p : table->GetAllPartitions()) {
                PartitionState state = p->GetState();
                auto placement = p->GetPlacementActual();
                if (!placement.has_server()) {
                    continue;
                }
                ServerPtr server = server_loc_mgr->Get(placement.server());
                if (!server) {
                    LOG_WARNING("failed to find server")
                        .put("placement", placement.ShortDebugString());
                    return Status::Internal("server not found");
                }
                NodePtr node;
                Status status = server->GetNode(placement.node().id(), &node);
                if (!status.ok()) {
                    if (state != PartitionState::P_FROZEN) {
                        LOG_ERROR("failed to find node")
                            .put("placement", placement.ShortDebugString())
                            .put("partition", *p)
                            .put("result", status);
                        return Status::Internal("node not found");
                    } else {
                        continue;
                    }
                }
                LOG_INFO("combine server to partition")
                    .put("server", to_string(placement.server()))
                    .put("pid", p->GetId());
                node->CommitIntentPartition(p->GetId());
                p->SetPlacementActual(placement, node);
            }
        }
    }
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2
