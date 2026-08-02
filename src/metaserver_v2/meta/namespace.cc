// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/meta/namespace.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <mutex>
#include <unordered_set>
#include <utility>

#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "common/partition_id_type.h"
#include "common/proto_enhance.h"
#include "common/status.h"
#include "metaserver_v2/utils.h"

namespace bcache2 {
namespace metaserver {

Namespace::Namespace(const NamespaceInfo& info)
    : info_(info), proxy_cluster_(std::make_shared<ProxyCluster>(this)) {}

void Namespace::SetId(uint64_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.set_id(id);
}

uint64_t Namespace::GetId() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.id();
}

std::string Namespace::GetName() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.name();
}

NamespaceInfo Namespace::GetInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_;
}

size_t Namespace::GetTableCount() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return table_map_.size();
}

Status Namespace::Add(TablePtr table) {
    table->Attach(this);
    table->SetCreatingState();
    LOG_INFO("start to spawn all partitions").put("table", table->GetName());
    Status status = table->SpawnPartitionSets();
    if (!status.ok()) {
        LOG_WARNING("failed to spawn").put("table_name", table->GetName()).put("status", status);
        return status;
    }

    std::lock_guard<bthread::Mutex> _(mu_);
    auto pair = table_map_.emplace(table->GetName(), table);
    if (!pair.second) {
        return Status::AlreadyExists("");
    }
    info_.set_table_count(table_map_.size());
    return Status::OK();
}

Status Namespace::Get(const std::string& name, TablePtr* table) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = table_map_.find(name);
    if (iter == table_map_.end()) {
        return Status::NotFound("");
    }
    *table = iter->second;
    return Status::OK();
}

std::vector<TablePtr> Namespace::List(bool with_frozen) {
    std::vector<TablePtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& pair : table_map_) {
        result.push_back(pair.second);
    }
    if (with_frozen) {
        for (const auto& table : frozen_tables_) {
            result.push_back(table);
        }
    }
    return result;
}

Status Namespace::Freeze(TablePtr table, int64_t timestamp) {
    table->SetFrozenState(timestamp);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (frozen_table_map_.count(table->GetId()) > 0) {
        return Status::AlreadyExists("");
    }
    table_map_.erase(table->GetName());
    frozen_table_map_[table->GetId()] = frozen_tables_.insert(table);
    info_.set_table_count(table_map_.size());
    return Status::OK();
}

std::vector<TablePtr> Namespace::ListFrozen(int64_t timestamp_before) {
    std::vector<TablePtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (const auto& table : frozen_tables_) {
        if (timestamp_before == 0 || table->GetFrozenTimeSec() < timestamp_before) {
            result.push_back(table);
        }
    }
    return result;
}

Status Namespace::Drop(const TablePtr& table) {
    CHECK(table->GetState() == TableState::TABLE_FROZEN);

    LOG_WARNING("start to drop table").put("table", table->GetId());
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = frozen_table_map_.find(table->GetId());
    if (iter == frozen_table_map_.end()) {
        return Status::NotFound("");
    }
    Status status = table->FreezeAllPartitions();
    CHECK(status.ok()) << this;
    status = table->DropAllPartitions();
    CHECK(status.ok()) << this;
    frozen_tables_.erase(iter->second);
    frozen_table_map_.erase(iter);
    table->Detach();
    return Status::OK();
}

ProxyClusterPtr Namespace::GetProxyCluster() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return proxy_cluster_;
}

bool Namespace::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<Namespace*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!ProtoEqual(info_, rhs->info_)) {
        LOG_INFO("proto info not equal");
        return false;
    }

    if (!proxy_cluster_->Equal(rhs->proxy_cluster_.get())) {
        LOG_INFO("proxy cluster not equal");
        return false;
    }
    if (!MapEqual(table_map_, rhs->table_map_)) {
        LOG_INFO("table map not equal");
        return false;
    }
    if (!SetEqual(frozen_tables_, rhs->frozen_tables_)) {
        LOG_INFO("frozen table map not equal");
        return false;
    }

    return true;
}

void Namespace::DeepCopyTo(DeepCopy* rhs_base) {
    auto rhs = static_cast<Namespace*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->info_ = info_;
    LOG_INFO("start to copy proxy cluster");
    proxy_cluster_->DeepCopyTo(rhs->proxy_cluster_.get());
    LOG_INFO("start to copy tables");
    for (auto& pair : table_map_) {
        TablePtr cpy_table = std::make_shared<Table>(TableInfo());
        pair.second->DeepCopyTo(cpy_table.get());
        cpy_table->Attach(rhs);
        rhs->table_map_[pair.first] = std::move(cpy_table);
    }
    LOG_INFO("start to copy frozen tables");
    for (auto& my_table : frozen_tables_) {
        TablePtr cpy_table = std::make_shared<Table>(TableInfo());
        my_table->DeepCopyTo(cpy_table.get());
        cpy_table->Attach(rhs);
        rhs->frozen_table_map_[cpy_table->GetId()] = rhs->frozen_tables_.insert(cpy_table);
    }
}

Status Namespace::DumpSnapshot(google::protobuf::io::FileOutputStream* stream,
                               SnapshotManifest::NamespaceSpec* ns_spec) {
    Status status = PackToStream(stream);
    ns_spec->set_id(GetId());
    RETURN_IF_STATUS_ERROR(status);
    std::vector<TablePtr> tables = List(true /* with frozen */);
    for (auto& table : tables) {
        LOG_INFO("start to dump table").put("id", table->GetId()).put("name", table->GetFullName());
        ns_spec->add_table_ids(table->GetId());
        status = table->DumpSnapshot(stream);
        RETURN_IF_STATUS_ERROR(status);
    }

    LOG_INFO("start to dump proxy cluster").put("id", GetId()).put("name", GetName());
    ProxyClusterPtr proxy_cluster = GetProxyCluster();
    std::vector<ProxyGroupPtr> proxy_groups = proxy_cluster->ListAllProxyGroups();
    for (auto& proxy_group : proxy_groups) {
        *ns_spec->add_proxy_groups() = proxy_group->GetPlacement();
        status = proxy_group->DumpSnapshot(stream);
        RETURN_IF_STATUS_ERROR(status);
    }
    return Status::OK();
}

Status Namespace::LoadSnapshot(google::protobuf::io::FileInputStream* stream,
                               const SnapshotManifest::NamespaceSpec& ns_spec) {
    Status status = UnPackFromStream(stream);
    RETURN_IF_STATUS_ERROR(status);

    if (ns_spec.id() != GetId()) {
        return Status::Internal("ns id not match");
    }
    std::unordered_set<uint32_t> table_ids;
    for (auto id : ns_spec.table_ids()) {
        table_ids.insert(id);
    }

    LOG_INFO("start to load tables")
        .put("namespace", GetName())
        .put("table_count", table_ids.size());
    for (int i = 0; i < ns_spec.table_ids_size(); i++) {
        auto table = std::make_shared<Table>(TableInfo());
        status = table->LoadSnapshot(stream);
        RETURN_IF_STATUS_ERROR(status);

        const uint32_t table_id = table->GetId();
        table_ids.erase(table_id);
        const TableState state = table->GetState();
        table->Attach(this);

        std::lock_guard<bthread::Mutex> _(mu_);
        if (state == TableState::TABLE_FROZEN) {
            frozen_table_map_[table_id] = frozen_tables_.insert(table);
        } else {
            table_map_[table->GetName()] = table;
        }
        LOG_INFO("loaded table")
            .put("id", table_id)
            .put("name", table->GetFullName())
            .put("state", TableState_Name(state));
    }
    if (table_ids.size() > 0) {
        LOG_WARNING("table count not match").put("left", table_ids.size());
        return Status::Internal("loaded table count not match");
    }
    // NamespaceInfo only records normal state table,
    // but SnapshotManifest records normal & frozen state table.
    NamespaceInfo info = GetInfo();
    if (info.table_count() != ns_spec.table_ids_size() - frozen_table_map_.size()) {
        LOG_WARNING("table count not match")
            .put("info", info.table_count())
            .put("manifest", ns_spec.table_ids_size())
            .put("frozen table", frozen_table_map_.size());
        return Status::Internal("table count not match");
    }

    LOG_INFO("start to load proxy cluster")
        .put("namespace", GetName())
        .put("group_count", ns_spec.proxy_groups_size());
    ProxyClusterPtr proxy_cluster = GetProxyCluster();
    for (int i = 0; i < ns_spec.proxy_groups_size(); i++) {
        auto proxy_group = std::make_shared<ProxyGroup>(ProxyGroupInfo());
        status = proxy_group->LoadSnapshot(stream);
        RETURN_IF_STATUS_ERROR(status);
        LOG_INFO("loaded proxy group").put("loc", to_string(proxy_group->GetPlacement()));
        proxy_cluster->PutProxyGroup(std::move(proxy_group));
    }
    return Status::OK();
}

///////////////

void NamespaceManager::UpdateReservedNamespaceNames(std::unordered_set<std::string> v) {
    std::lock_guard<bthread::Mutex> _(mu_);
    std::swap(v, reserved_ns_names_);
}

void NamespaceManager::UpdateReservedTableNames(std::unordered_set<std::string> v) {
    std::lock_guard<bthread::Mutex> _(mu_);
    std::swap(v, reserved_table_names_);
}

void NamespaceManager::UpdateReservedConsulNames(std::unordered_set<std::string> v) {
    consul_map_.UpdateReservedConsulNames(std::move(v));
}

Status NamespaceManager::ValidateNamespaceInfo(const NamespaceInfo& info) {
    RETURN_IF_STATUS_ERROR(Validate(info));
    std::lock_guard<bthread::Mutex> _(mu_);
    return ValidateWithLock(info);
}

Status NamespaceManager::Add(NamespacePtr ns) {
    std::lock_guard<bthread::Mutex> _(mu_);
    Status status = ValidateWithLock(ns->GetInfo());
    if (!status.ok()) {
        return status;
    }

    const uint64_t id = ++id_cursor_;  // starts from 1
    ns->SetId(id);
    auto pair = ns_map_.emplace(ns->GetName(), ns);
    CHECK(pair.second);
    return Status::OK();
}

Status NamespaceManager::Get(const std::string& name, NamespacePtr* ns) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = ns_map_.find(name);
    if (iter == ns_map_.end()) {
        return Status::NotFound("");
    }
    *ns = iter->second;
    return Status::OK();
}

std::vector<NamespacePtr> NamespaceManager::List() {
    std::vector<NamespacePtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& pair : ns_map_) {
        result.push_back(pair.second);
    }
    return result;
}

Status NamespaceManager::Remove(const std::string& name) {
    std::lock_guard<bthread::Mutex> _(mu_);
    Status status = CascadeRemoveCheckWithLock(name);
    if (!status.ok()) {
        return status;
    }

    // TODO(wuzhenyu) impl

    return Status::Internal("not implemented");
}

Status NamespaceManager::ValidateWithLock(const NamespaceInfo& info) {
    if (reserved_ns_names_.count(info.name()) > 0) {
        return Status::FailedPrecondition("reserved name");
    }
    if (ns_map_.count(info.name()) > 0) {
        return Status::FailedPrecondition("name already used");
    }
    return Status::OK();
}

Status NamespaceManager::CascadeRemoveCheckWithLock(const std::string& name) {
    // TODO(wuzhenyu) impl

    return Status::Internal("not implemented");
}

Status NamespaceManager::ValidateTableInfo(const TableInfo& info) {
    RETURN_IF_STATUS_ERROR(Validate(info));
    std::lock_guard<bthread::Mutex> _(mu_);
    return ValidateTableInfoWithLock(info);
}

Status NamespaceManager::ValidateTableInfoWithLock(const TableInfo& info) {
    if (reserved_table_names_.count(info.name()) > 0) {
        return Status::FailedPrecondition("reserved name");
    }
    auto iter = ns_map_.find(info.namespace_name());
    if (iter == ns_map_.end()) {
        return Status::FailedPrecondition("namespace not found");
    }
    NamespacePtr ns = iter->second;
    TablePtr table;
    if (!(ns->Get(info.name(), &table).IsNotFound())) {
        return Status::FailedPrecondition("already exists");
    }
    return Status::OK();
}

Status NamespaceManager::AddTable(TablePtr table) {
    const TableInfo& info = table->GetInfo();

    std::lock_guard<bthread::Mutex> _(mu_);
    Status status = ValidateTableInfoWithLock(info);
    if (!status.ok()) {
        return status;
    }

    const uint32_t id = ++table_id_cursor_;  // starts from 1
    table->SetId(id);
    NamespacePtr ns = ns_map_[info.namespace_name()];
    CHECK(ns);
    status = ns->Add(table);
    if (!status.ok()) {
        return status;
    }
    flat_table_map_[id] = table;
    return Status::OK();
}

Status NamespaceManager::GetTable(uint32_t id, TablePtr* table) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = flat_table_map_.find(id);
    if (iter == flat_table_map_.end()) {
        return Status::NotFound("");
    }
    *table = iter->second;
    return Status::OK();
}

Status NamespaceManager::DropTable(uint32_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = flat_table_map_.find(id);
    if (iter == flat_table_map_.end()) {
        return Status::NotFound("");
    }
    TablePtr table = iter->second;

    Namespace* ns = table->GetNamespace();
    CHECK(ns);
    Status status = ns->Drop(table);
    if (!status.ok()) {
        return status;
    }

    flat_table_map_.erase(iter);
    return Status::OK();
}

uint32_t NamespaceManager::GetIdCursor() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return id_cursor_;
}

uint32_t NamespaceManager::GetTableIdCursor() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return table_id_cursor_;
}

bool NamespaceManager::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<NamespaceManager*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (id_cursor_ != rhs->id_cursor_ || table_id_cursor_ != rhs->table_id_cursor_) {
        LOG_INFO("cursor not euqal");
        return false;
    }

    if (!MapEqual(ns_map_, rhs->ns_map_)) {
        LOG_INFO("ns map not euqal");
        return false;
    }

    if (!MapEqual(flat_table_map_, rhs->flat_table_map_)) {
        LOG_INFO("flat table not euqal");
        return false;
    }

    if (!consul_map_.Equal(&(rhs->consul_map_))) {
        LOG_INFO("consul map not euqal");
        return false;
    }

    if (!SetEqual(reserved_ns_names_, rhs->reserved_ns_names_)) {
        LOG_INFO("reserved ns not euqal");
        return false;
    }
    if (!SetEqual(reserved_table_names_, rhs->reserved_table_names_)) {
        LOG_INFO("reserved table not euqal");
        return false;
    }

    return true;
}

void NamespaceManager::DeepCopyTo(DeepCopy* rhs_base) {
    auto rhs = static_cast<NamespaceManager*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->id_cursor_ = id_cursor_;
    rhs->table_id_cursor_ = table_id_cursor_;
    LOG_INFO("start to copy namespaces");
    for (auto& pair : ns_map_) {
        auto ns = std::make_shared<Namespace>(NamespaceInfo());
        pair.second->DeepCopyTo(ns.get());
        rhs->ns_map_[pair.first] = std::move(ns);
    }

    LOG_INFO("start to copy consul names");
    consul_map_.DeepCopyTo(&(rhs->consul_map_));
    rhs->reserved_ns_names_ = reserved_ns_names_;
    rhs->reserved_table_names_ = reserved_table_names_;
    LOG_INFO("start to construct flat table map");
    rhs->ConstructFlatTableMap();
}

void NamespaceManager::ConstructFlatTableMap() {
    // Note: no lock protection here
    for (auto& pair : ns_map_) {
        for (auto& table : pair.second->List(true)) {
            flat_table_map_[table->GetId()] = table;
        }
    }
}

Status NamespaceManager::DumpSnapshot(const butil::FilePath& dir, SnapshotManifest* manifest) {
    manifest->set_namespace_id_cursor(GetIdCursor());
    manifest->set_table_id_cursor(GetTableIdCursor());
    for (auto& ns : List()) {
        const uint32_t ns_id = ns->GetId();
        auto ns_spec = manifest->add_namespace_list();
        ns_spec->set_id(ns_id);
        auto file = dir.Append(fmt::format("namespace-{}", ns_id));
        int fd = -1;
        do {
            fd = open(file.value().c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        } while (fd < 0 && errno == EINTR);
        if (fd < 0) {
            return Status::Internal("I/O Error, failed to open file");
        }

        google::protobuf::io::FileOutputStream stream(fd);
        stream.SetCloseOnDelete(true);
        LOG_INFO("start to dump namespace")
            .put("id", ns_id)
            .put("name", ns->GetName())
            .put("file", file.value().c_str())
            .put("fd", fd);
        Status status = ns->DumpSnapshot(&stream, ns_spec);
        RETURN_IF_STATUS_ERROR(status);
        if (!stream.Flush()) {
            LOG_WARNING("failed to flush").put("errno", strerror(stream.GetErrno()));
            return Status::Internal("I/O Error, failed to flush");
        }
    }  // for namespace
    return Status::OK();
}

Status NamespaceManager::LoadSnapshot(const butil::FilePath& dir,
                                      const SnapshotManifest& manifest) {
    {
        std::lock_guard<bthread::Mutex> _(mu_);
        id_cursor_ = manifest.namespace_id_cursor();
        table_id_cursor_ = manifest.table_id_cursor();
    }
    for (const auto& ns_spec : manifest.namespace_list()) {
        const uint32_t ns_id = ns_spec.id();
        LOG_INFO("start to load namespace").put("id", ns_id);
        auto file = dir.Append(fmt::format("namespace-{}", ns_id));
        int fd = -1;
        do {
            fd = open(file.value().c_str(), O_RDONLY, 0644);
        } while (fd < 0 && errno == EINTR);
        if (fd < 0) {
            return Status::Internal("I/O Error, failed to open file");
        }
        google::protobuf::io::FileInputStream stream(fd);
        stream.SetCloseOnDelete(true);

        auto ns = std::make_shared<Namespace>(NamespaceInfo());
        Status status = ns->LoadSnapshot(&stream, ns_spec);
        RETURN_IF_STATUS_ERROR(status);

        ns_map_[ns->GetName()] = ns;
        LOG_INFO("loaded namespace").put("id", ns_id).put("name", ns->GetName());
        for (auto& table : ns->List(true /* with frozen */)) {
            flat_table_map_[table->GetId()] = table;
        }
        consul_map_.Calibrate(ns->GetProxyCluster());
    }

    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

