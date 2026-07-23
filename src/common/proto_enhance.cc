// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "common/proto_enhance.h"

#include <arpa/inet.h>
#include <netinet/in.h>

#include <unordered_set>

#include "absl/strings/match.h"
#include "butil/endpoint.h"
#include "spdlog/fmt/fmt.h"

#include "common/partition_id_type.h"

namespace bcache2 {
namespace {

bool IsIp4Literal(const std::string& ip) {
    in_addr addr;
    return !ip.empty() && inet_pton(AF_INET, ip.data(), &addr) == 1;
}

bool IsIp6Literal(const std::string& ip) {
    in6_addr addr;
    return !ip.empty() && inet_pton(AF_INET6, ip.data(), &addr) == 1;
}

}  // namespace

std::string to_string(const Endpoint& ep) {
    if (ep.addr_family() == Endpoint::ADDR_V4 || ep.addr_family() == Endpoint::ADDR_DUAL_STACK) {
        return fmt::format("{}:{}", ep.ip4(), ep.port());
    }
    return fmt::format("[{}]:{}", ep.ip6(), ep.port());
}

bool Validate(const Endpoint& ep) {
    bool ip4_valid = IsIp4Literal(ep.ip4());
    if (ep.addr_family() == Endpoint::ADDR_V4) {
        return ip4_valid;
    }

    bool ip6_valid = IsIp6Literal(ep.ip6());
    if (ep.addr_family() == Endpoint::ADDR_V6) {
        return ip6_valid;
    }
    return ip4_valid && ip6_valid;
}

bool IsSameHost(const Endpoint& lhs, const Endpoint& rhs) {
    if (!lhs.ip4().empty() || !rhs.ip4().empty()) {
        return lhs.ip4() == rhs.ip4();
    }
    return lhs.ip6() == rhs.ip6();
}

butil::EndPoint ToBRpcEndpoint(const Endpoint& ep) {
    butil::EndPoint result;
    int rc;
    if (ep.addr_family() == Endpoint::ADDR_V4 ||
        (ep.addr_family() == Endpoint::ADDR_DUAL_STACK && !ep.ip4().empty())) {
        rc = butil::str2endpoint(ep.ip4().c_str(), ep.port(), &result);
    } else {
        rc = butil::str2endpoint(ep.ip6().c_str(), ep.port(), &result);
    }
    if (rc != 0) {
        result.port = INT_MIN;
    }
    return result;
}

Status ToEndpoint2(const Endpoint& ep, Endpoint2* result) {
    result->set_addr_family(ep.addr_family());
    result->set_port(ep.port());
    if (!ep.ip4().empty()) {
        in_addr ip4;
        int rc = inet_pton(AF_INET, ep.ip4().data(), &ip4);
        if (rc <= 0) {
            return Status::InvalidArgument("ip4 wrong");
        }
        result->add_be32_addr(ip4.s_addr);
    }
    if (!ep.ip6().empty()) {
        in6_addr ip6;
        int rc = inet_pton(AF_INET6, ep.ip6().data(), &ip6);
        if (rc <= 0) {
            return Status::InvalidArgument("ip6 wrong");
        }
        for (int i = 0; i < 4; i++) {
            result->add_be32_addr(ip6.s6_addr32[i]);
        }
    }
    return Status::OK();
}

Status ToEndpoint(const Endpoint2& ep, Endpoint* result) {
    result->set_addr_family(ep.addr_family());
    result->set_port(ep.port());
    size_t unit_size = ep.be32_addr_size();
    if (unit_size == 0 || unit_size > 5) {
        return Status::InvalidArgument("unit size wrong");
    }
    int cursor = 0;
    if (unit_size == 1 || unit_size == 5) {  // v4 or dual-stack
        char buf[INET_ADDRSTRLEN];
        in_addr ip4;
        ip4.s_addr = ep.be32_addr(cursor++);
        if (inet_ntop(AF_INET, &ip4, buf, INET_ADDRSTRLEN) == nullptr) {
            return Status::InvalidArgument("invalid ip4");
        }
        result->set_ip4(buf);
    }
    if (unit_size > 1) {  // v6 or dual-stack
        char buf[INET6_ADDRSTRLEN];
        in6_addr ip6;
        for (int i = 0; i < 4; i++) {
            ip6.s6_addr32[i] = ep.be32_addr(cursor++);
        }
        if (inet_ntop(AF_INET6, &ip6, buf, INET6_ADDRSTRLEN) == nullptr) {
            return Status::InvalidArgument("invalid ip6");
        }
        result->set_ip6(buf);
    }
    return Status::OK();
}

bool Validate(const Location& loc) {
    return !loc.vregion().empty()  // NOLINT
           && !loc.vdc().empty()   // NOLINT
           && !loc.vau().empty();
}

bool ValidateFuzzily(const Location& loc) {
    // 1. no empty field
    // 2. vau is empty
    // 3. vau and vdc are empty
    return !loc.vregion().empty() && (!loc.vdc().empty() || loc.vau().empty());
}

bool BelongsTo(const Location& sub, const Location& obj) {
    if (!obj.vregion().empty() && sub.vregion() != obj.vregion()) {
        return false;
    }
    if (!obj.vdc().empty() && sub.vdc() != obj.vdc()) {
        return false;
    }
    if (!obj.vau().empty() && sub.vau() != obj.vau()) {
        return false;
    }

    if (sub.tag() != obj.tag()) {
        return false;
    }
    return true;
}

bool IsEmpty(const Location& loc) {
    return loc.vregion().empty() && loc.vdc().empty() && loc.vau().empty();
}

bool IsSame(const Location& lhs, const Location& rhs) {
    return lhs.vregion() == rhs.vregion() && lhs.vdc() == rhs.vdc() && lhs.vau() == rhs.vau() &&
           lhs.tag() == rhs.tag();
}

bool InSameVdc(const Location& lhs, const Location& rhs) {
    return lhs.vregion() == rhs.vregion() && lhs.vdc() == rhs.vdc();
}

std::string to_string(const Location& loc) {
    return fmt::format("{}/{}/{}/{}", loc.vregion(), loc.vdc(), loc.vau(), loc.tag());
}

Status Validate(const ServerInfo& info) {
    // TODO(wuzhenyu) using CMDB API to auto fill v6 address
    if (!Validate(info.endpoint())) {
        return Status::FailedPrecondition("invalid endpoint");
    }

    if (!ValidateFuzzily(info.location())) {
        return Status::FailedPrecondition("vregion/vdc/vau all required");
    }

    if (info.numa_nodes_size() == 0) {
        return Status::FailedPrecondition("numa_nodes required");
    }

    for (const auto& node : info.numa_nodes()) {
        if (node.cpu_list().empty() || node.memory_size_mb() == 0) {
            return Status::FailedPrecondition("numa_node invalid");
        }
    }
    return Status::OK();
}

Status Validate(const ProxyInfo& info) {
    if (!Validate(info.endpoint())) {
        return Status::FailedPrecondition("invalid endpoint");
    }

    if (!Validate(info.location())) {
        return Status::FailedPrecondition("vregion/vdc/vau all required");
    }

    return Status::OK();
}

Status Validate(const ProxyGroupInfo& info) {
    const Location& loc = info.placement();
    if (loc.vregion().empty() || loc.vdc().empty()) {
        return Status::FailedPrecondition("invalid placement");
    }
    return Validate(info.config());
}

Status Validate(const ProxyConfig& config) {
    if (config.consul_names_size() == 0) {
        return Status::FailedPrecondition("no conusl names");
    }
    return Status::OK();
}

static bool IsValidName(const std::string& name) {
    static const std::unordered_set<char> valid_ch{'_', '#'};
    if (name.empty() || name.size() > 128 || !isalpha(name[0])) {
        return false;
    }
    for (size_t i = 1; i < name.size(); ++i) {
        if (!isalnum(name[i]) && valid_ch.count(name[i]) == 0) {
            return false;
        }
    }
    return true;
}

static bool IsValidStoragePoolUri(const std::string& uri) {
    if (uri.empty()) {
        return false;
    }
    for (auto& prefix : {"blob://", "local://", "file://", "shared-file://", "shared://",
                         "efs://", "nfs://", "s3://", "ceph://", "ceph+s3://"}) {
        if (absl::StartsWith(uri, prefix)) {
            return true;
        }
    }
    return false;
}

Status Validate(const NamespaceInfo& info) {
    if (info.name().empty() || !IsValidName(info.name())) {
        return Status::FailedPrecondition("invalid name");
    }
    return Status::OK();
}

Status Validate(const TableInfo& info) {
    if (info.name().empty() || !IsValidName(info.name())) {
        return Status::FailedPrecondition("invalid name");
    }
    if (!info.has_quota()) {
        return Status::FailedPrecondition("empty quota");
    }
    if (!validate_partition_set_num(info.partition_set_num())) {
        return Status::FailedPrecondition("invalid partition_set_num");
    }

    if (info.partition_units_size() == 0) {
        return Status::FailedPrecondition("empty partition unit");
    }
    size_t pcnt_per_set = 0;
    for (const auto& unit : info.partition_units()) {
        if (!validate_partition_num_per_set(unit.partition_num())) {
            return Status::FailedPrecondition("invalid partition_num");
        }
        pcnt_per_set += unit.partition_num();
        if (unit.placement_set_size() != static_cast<int>(unit.partition_num())) {
            return Status::FailedPrecondition("partition num not equal to placement statements");
        }
        for (const auto& loc : unit.placement_set()) {
            if (!ValidateFuzzily(loc)) {
                return Status::FailedPrecondition("invalid placement location");
            }
        }
        if (!IsValidStoragePoolUri(unit.storage_pool_uri())) {
            return Status::FailedPrecondition("invalid storage pool uri");
        }
    }
    if (!validate_partition_num_per_set(pcnt_per_set)) {
        return Status::FailedPrecondition("invalid partition num, too large");
    }
    return Status::OK();
}

NamespaceInfo Request2Info(const metaserver::AddNamespaceRequest& req) {
    NamespaceInfo info;
    info.set_name(req.name());
    *info.mutable_consul_info() = req.consul_info();
    return info;
}

TableInfo Request2Info(const metaserver::AddTableRequest& req) {
    TableInfo info;
    info.set_namespace_name(req.namespace_name());
    info.set_name(req.name());
    info.set_partition_set_num(req.partition_set_num());
    if (req.partition_units_size() > 0) {
        *info.mutable_partition_units() = req.partition_units();
    }
    info.set_partition_unit_relation(req.partition_unit_relation());
    info.set_election_policy(req.election_policy());
    if (req.has_quota()) {
        *info.mutable_quota() = req.quota();
    }
    if (req.has_config()) {
        *info.mutable_config() = req.config();
    }
    return info;
}

ServerInfo Request2Info(const metaserver::AddServerRequest& req) {
    ServerInfo info;
    *info.mutable_endpoint() = req.endpoint();
    *info.mutable_location() = req.location();
    info.mutable_numa_nodes()->CopyFrom(req.numa_nodes());
    return info;
}

ProxyInfo Request2Info(const metaserver::AddProxyRequest& req) {
    ProxyInfo info;
    *info.mutable_endpoint() = req.endpoint();
    *info.mutable_location() = req.location();
    return info;
}

}  // namespace bcache2
