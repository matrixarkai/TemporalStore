// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>

#include "bthread/mutex.h"

#include "common/status.h"
#include "protocol/info.pb.h"
#include "protocol/master.pb.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

// TODO(wuzhenyu) refactor me to incremental syncer

using TopoPartitionInfo = GetTableTopoResponse_PartitionInfo;

struct TopoUnit {
    uint64_t version{0};
    PartitionUnit unit;
    std::map<uint64_t, TopoPartitionInfo> partition_info;

    // TODO(wuzhenyu) remove one of them after client using 'endpoint' structure
    // instead of 'host' string
    std::shared_ptr<GetTableTopoResponse> response_cache;
    std::shared_ptr<std::string> serialized_response_cache;
};

struct TableTopoCollection {
    std::string ns_slash_table_name;
    uint32_t table_id;
    std::map<std::string, uint32_t> vdc2unit_cache;
    std::map<uint32_t, TopoUnit> units;
};

class MetaPublisher {
 public:
    MetaPublisher() = default;
    ~MetaPublisher() = default;

    Status AddTable(const TableInfo& info);
    Status DropTable(uint32_t table_id);
    Status UpdatePartitionSet(uint32_t table_id, const PartitionSetInfo& info);

    Status Query(uint64_t old_version, const std::string& ns, const std::string& table,
                 const std::string& vdc, bool compress, GetTableTopoResponse* response);

    void TakeAbstract(TopoAbstract* abstract);
    Status DumpAbstract(const std::string& path, const TopoAbstract& abstract);
    Status LoadAndApplyAbstract(const std::string& path);

 private:
    void InvalidCache(TopoUnit* unit);
    void RefillCache(TopoUnit* unit);

 private:
    bthread::Mutex mu_;
    std::unordered_map<std::string, uint32_t> table_id_map_;
    std::unordered_map<uint32_t, TableTopoCollection> topo_;
};

}  // namespace metaserver
}  // namespace bcache2

