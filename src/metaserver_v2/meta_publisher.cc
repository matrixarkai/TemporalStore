// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/meta_publisher.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

#include "common/logging.h"
#include "common/proto_enhance.h"

namespace bcache2 {
namespace metaserver {

constexpr int kTopoStructureVersion = 2;

Status MetaPublisher::AddTable(const TableInfo& info) {
    const uint32_t id = info.id();
    std::string fqdn = info.namespace_name() + "/" + info.name();

    TableTopoCollection c;
    c.ns_slash_table_name = fqdn;
    c.table_id = id;
    for (const auto& unit : info.partition_units()) {
        TopoUnit topo_unit;
        topo_unit.unit = unit;
        c.units.emplace(std::make_pair(unit.id(), std::move(topo_unit)));
        for (const auto& loc : unit.placement_set()) {
            if (!loc.vdc().empty()) {
                c.vdc2unit_cache[loc.vdc()] = unit.id();
            }
        }
    }

    std::lock_guard<bthread::Mutex> _(mu_);
    if (table_id_map_.count(fqdn) > 0) {
        return Status::AlreadyExists("");
    }
    table_id_map_[fqdn] = id;
    topo_.emplace(std::make_pair(id, std::move(c)));
    LOG_INFO("add table").put("fqdn", fqdn).put("id", id);
    return Status::OK();
}

Status MetaPublisher::DropTable(uint32_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = topo_.find(id);
    if (iter == topo_.end()) {
        return Status::NotFound("");
    }
    LOG_INFO("drop table")
        .put("fqdn", iter->second.ns_slash_table_name)
        .put("id", iter->second.table_id);
    table_id_map_.erase(iter->second.ns_slash_table_name);
    topo_.erase(iter);
    return Status::OK();
}

Status MetaPublisher::UpdatePartitionSet(uint32_t table_id, const PartitionSetInfo& info) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = topo_.find(table_id);
    if (iter == topo_.end()) {
        return Status::NotFound("");
    }
    TableTopoCollection& c = iter->second;
    const MembershipInfo& ms = info.membership();
    std::map<uint64_t, PartitionPlacementInfo> place_map;
    for (auto& place : ms.placements()) {
        place_map[place.id()] = place;
    }
    for (auto& ms_unit : ms.units()) {
        uint32_t unit_id = ms_unit.partition_unit_id();
        auto tu_iter = c.units.find(unit_id);
        if (tu_iter == c.units.end()) {
            return Status::Internal("unit not found");
        }
        TopoUnit& topo_unit = tu_iter->second;
        topo_unit.version++;
        TopoPartitionInfo& topo = topo_unit.partition_info[info.id()];
        if (topo.partition_id() == 0) {
            topo.set_partition_id(info.id());
            topo.set_start_slot(info.slot_range_begin());
            topo.set_end_slot(info.slot_range_end());
        }
        topo.clear_load_infos();

        for (uint64_t partition_id : ms_unit.active_id_list()) {
            auto place_iter = place_map.find(partition_id);
            if (place_iter == place_map.end()) {
                continue;
            }
            auto m = topo.add_load_infos();

            const PlacementSpec& place = place_map[partition_id].placement();
            const Endpoint& ep = place.server();
            m->set_port(ep.port());
            *m->mutable_endpoint() = ep;
            *m->mutable_location() = place.location();
            m->set_load_version(ms.partition_set_version());
            m->set_partition_id(partition_id);
            if (partition_id == ms_unit.primary_id()) {
                m->set_is_master(true);
            }
        }
        InvalidCache(&topo_unit);
    }  // for unit
    return Status::OK();
}

Status MetaPublisher::Query(uint64_t old_version, const std::string& ns, const std::string& table,
                            const std::string& vdc, bool compress, GetTableTopoResponse* response) {
    std::string fqdn = ns + "/" + table;
    std::shared_ptr<GetTableTopoResponse> cache;
    std::shared_ptr<std::string> serialized_cache;
    {
        std::lock_guard<bthread::Mutex> _(mu_);
        auto id_iter = table_id_map_.find(fqdn);
        if (id_iter == table_id_map_.end()) {
            return Status::NotFound(fqdn);
        }
        const uint32_t id = id_iter->second;
        TableTopoCollection& c = topo_[id];
        auto unit_id_iter = c.vdc2unit_cache.find(vdc);
        if (unit_id_iter == c.vdc2unit_cache.end()) {
            if (c.units.size() > 1) {
                return Status::NotFound("dc related not found");
            } else {
                unit_id_iter = c.vdc2unit_cache.begin();
            }
        }
        TopoUnit& unit = c.units[unit_id_iter->second];
        if (!unit.response_cache) {
            RefillCache(&unit);
        }
        cache = unit.response_cache;
        serialized_cache = unit.serialized_response_cache;
    }
    if (!cache) {
        return Status::NotFound("table is creating");
    }

    if (cache->topo_version() == old_version) {
        return Status::Cancelled("no need to update");
    }
    if (cache->topo_version() < old_version) {
        LOG_WARNING("given version too large")
            .put("ns", ns)
            .put("table", table)
            .put("given", old_version)
            .put("mine", cache->topo_version());
        return Status::FailedPrecondition("given version too large");
    }
    if (compress) {
        response->set_serialized_topo(*serialized_cache);
    } else {
        response->CopyFrom(*cache);
    }
    return Status::OK();
}

void MetaPublisher::TakeAbstract(TopoAbstract* abstract) {
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& pair : topo_) {
        auto table_spec = abstract->add_table_list();
        auto& table = pair.second;
        table_spec->set_id(table.table_id);
        for (auto& pair2 : table.units) {
            auto unit_spec = table_spec->add_unit_list();
            unit_spec->set_id(pair2.first);
            unit_spec->set_version(pair2.second.version);
            LOG_INFO("take topo version")
                .put("table_id", table.table_id)
                .put("unit_id", unit_spec->id())
                .put("version", unit_spec->version());
        }
    }
}

Status MetaPublisher::DumpAbstract(const std::string& path, const TopoAbstract& abstract) {
    int fd = -1;
    do {
        fd = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return Status::Internal("I/O Error, failed to open file");
    }
    google::protobuf::io::FileOutputStream stream(fd);
    stream.SetCloseOnDelete(true);
    if (!abstract.SerializeToZeroCopyStream(&stream)) {
        return Status::Internal("Serialize Error");
    }
    if (!stream.Flush()) {
        LOG_WARNING("failed to flush").put("errno", strerror(stream.GetErrno()));
        return Status::Internal("I/O Error, failed to flush");
    }
    return Status::OK();
}

Status MetaPublisher::LoadAndApplyAbstract(const std::string& path) {
    int fd = -1;
    do {
        fd = open(path.c_str(), O_RDONLY, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return Status::Internal("I/O Error, failed to open file");
    }
    google::protobuf::io::FileInputStream stream(fd);
    stream.SetCloseOnDelete(true);
    TopoAbstract abstract;
    if (!abstract.ParseFromZeroCopyStream(&stream)) {
        return Status::Internal("Parse Error, manifest parse failed");
    }

    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& table_spec : abstract.table_list()) {
        auto iter = topo_.find(table_spec.id());
        if (iter == topo_.end()) {
            LOG_WARNING("table not found").put("id", table_spec.id());
            return Status::Internal("table not found");
        }
        auto& table = iter->second;
        for (auto& unit_spec : table_spec.unit_list()) {
            const uint32_t unit_id = unit_spec.id();
            auto iter2 = table.units.find(unit_id);
            if (iter2 == table.units.end()) {
                LOG_WARNING("table unit not found").put("id", table_spec.id()).put("unit", unit_id);
                return Status::Internal("table unit not found");
            }
            iter2->second.version = unit_spec.version();
            LOG_INFO("apply topo version")
                .put("table_id", table_spec.id())
                .put("unit_id", unit_spec.id())
                .put("version", unit_spec.version());
        }
    }
    return Status::OK();
}

void MetaPublisher::InvalidCache(TopoUnit* unit) {
    // Note: in lock
    unit->response_cache.reset();
}

void MetaPublisher::RefillCache(TopoUnit* unit) {
    auto cache = std::make_shared<GetTableTopoResponse>();
    cache->set_topo_structure_version(kTopoStructureVersion);
    cache->set_topo_version(unit->version);
    cache->clear_partitions();

    auto cache2 = std::make_shared<GetTableTopoResponse>();
    cache2->set_topo_structure_version(kTopoStructureVersion);
    cache2->set_topo_version(unit->version);
    cache2->clear_partitions();

    uint32_t location_idx = 0;
    std::unordered_map<Location, uint32_t, LocationHash> loc2id;
    for (auto& pair : unit->partition_info) {
        auto topo = cache->add_partitions();
        topo->CopyFrom(pair.second);
        for (int i = 0; i < topo->load_infos_size(); i++) {
            auto m = topo->mutable_load_infos(i);
            const Endpoint& ep = m->endpoint();
            m->set_host(ep.ip6().empty() ? ep.ip4() : ep.ip6());
        }

        // compressed data
        auto topo2 = cache2->add_partitions();
        topo2->CopyFrom(pair.second);
        topo2->clear_compressed_load_infos();
        for (int i = 0; i < topo2->load_infos_size(); i++) {
            auto load_info = topo2->load_infos(i);
            auto compressed_load_info = topo2->add_compressed_load_infos();
            compressed_load_info->set_is_master(load_info.is_master());
            compressed_load_info->set_load_version(load_info.load_version());
            compressed_load_info->set_partition_id(load_info.partition_id());
            Status status =
                ToEndpoint2(load_info.endpoint(), compressed_load_info->mutable_endpoint2());
            CHECK(status.ok()) << status.ToString();
            auto iter = loc2id.find(load_info.location());
            if (iter == loc2id.end()) {
                compressed_load_info->set_location_wrapper_id(location_idx);
                loc2id[load_info.location()] = location_idx++;
            } else {
                compressed_load_info->set_location_wrapper_id(iter->second);
            }
        }  // for loop of load infos
        // now offload
        topo2->clear_load_infos();
    }
    std::swap(cache, unit->response_cache);

    for (auto& p : loc2id) {
        auto wrapper = cache2->add_location_map();
        wrapper->set_id(p.second);
        *wrapper->mutable_location() = p.first;
    }
    auto serialized_cache = std::make_shared<std::string>();
    cache2->SerializeToString(serialized_cache.get());
    std::swap(serialized_cache, unit->serialized_response_cache);
}

}  // namespace metaserver
}  // namespace bcache2

