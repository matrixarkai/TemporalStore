// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bthread/bthread.h"
#include "butil/file_util.h"

#include "common/status.h"
#include "metaserver_v2/meta/proxy.h"
#include "metaserver_v2/meta/serializable.h"
#include "metaserver_v2/meta/table.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

class Namespace : public Serializable, public DeepCopy {
 public:
    explicit Namespace(const NamespaceInfo& info);
    ~Namespace() = default;

    void SetId(uint64_t id);
    uint64_t GetId();
    std::string GetName();

    NamespaceInfo GetInfo();

    /// table related
    size_t GetTableCount();
    Status Add(TablePtr table);
    Status Get(const std::string& name, TablePtr* table);
    std::vector<TablePtr> List(bool with_frozen = false);
    Status Freeze(TablePtr table, int64_t timestamp);
    std::vector<TablePtr> ListFrozen(int64_t timestamp_before = 0);
    Status Drop(const TablePtr& table);

    ProxyClusterPtr GetProxyCluster();

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;
    Status LoadSnapshot(google::protobuf::io::FileInputStream* stream,
                        const SnapshotManifest::NamespaceSpec& ns_spec);
    Status DumpSnapshot(google::protobuf::io::FileOutputStream* stream,
                        SnapshotManifest::NamespaceSpec* ns_spec);

    DEFINE_COMMON_SERIALIZABLE("namespace")

 private:
    bthread::Mutex mu_;
    NamespaceInfo info_;  // GUARDED_BY(mu_)
    ProxyClusterPtr proxy_cluster_{nullptr};

    std::unordered_map<std::string, TablePtr> table_map_;  // GUARDED_BY(mu_)
    using FrozenTableSet = std::multiset<TablePtr, TableFrozenTimeGreater>;
    FrozenTableSet frozen_tables_;                                             // GUARDED_BY(mu_)
    std::unordered_map<uint32_t, FrozenTableSet::iterator> frozen_table_map_;  // GUARDED_BY(mu_)
};

using NamespacePtr = std::shared_ptr<Namespace>;

class NamespaceManager : public DeepCopy {
 public:
    NamespaceManager() = default;
    ~NamespaceManager() = default;

    void UpdateReservedNamespaceNames(std::unordered_set<std::string> v);
    void UpdateReservedTableNames(std::unordered_set<std::string> v);

    void UpdateReservedConsulNames(std::unordered_set<std::string> v);
    void UpdateRestrictedConsulVdcs(std::unordered_set<std::string> v);

    // ns
    Status ValidateNamespaceInfo(const NamespaceInfo& info);
    Status CascadeRemoveCheck(const std::string& name);
    Status Add(NamespacePtr ns);
    Status Get(const std::string& name, NamespacePtr* ns);
    Status Remove(const std::string& name);
    std::vector<NamespacePtr> List();

    // table
    Status ValidateTableInfo(const TableInfo& info);
    Status AddTable(TablePtr table);
    Status GetTable(uint32_t id, TablePtr* table);
    Status DropTable(uint32_t id);

    // proxy group
    ConsulMap* GetConsulMap() { return &consul_map_; }
    Status ValidateProxyGroupInfo(const ProxyGroupInfo& info);

    uint32_t GetIdCursor();
    uint32_t GetTableIdCursor();

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;
    Status DumpSnapshot(const butil::FilePath& dir, SnapshotManifest* manifest);
    Status LoadSnapshot(const butil::FilePath& dir, const SnapshotManifest& manifest);

 private:
    Status ValidateWithLock(const NamespaceInfo& info);
    Status CascadeRemoveCheckWithLock(const std::string& name);
    Status ValidateTableInfoWithLock(const TableInfo& info);

    // for deep copy
    void ConstructFlatTableMap();

 private:
    bthread::Mutex mu_;
    uint64_t id_cursor_{0};
    uint64_t table_id_cursor_{0};

    std::unordered_map<std::string, NamespacePtr> ns_map_;

    // Note frozen tables are also in flat_table_map_
    std::unordered_map<uint32_t, TablePtr> flat_table_map_;
    ConsulMap consul_map_;

    std::unordered_set<std::string> reserved_ns_names_;
    std::unordered_set<std::string> reserved_table_names_;
};

}  // namespace metaserver
}  // namespace bcache2

