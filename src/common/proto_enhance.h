// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <ostream>
#include <string>

#include "butil/endpoint.h"
#include "google/protobuf/util/message_differencer.h"
#include "spdlog/fmt/fmt.h"

#include "common/status.h"
#include "protocol/base.pb.h"
#include "protocol/info.pb.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {

std::string to_string(const Endpoint& ep);
struct EndpointHash {
    std::size_t operator()(const Endpoint& ep) const noexcept {
        return std::hash<std::string>()(to_string(ep));
    }
};
bool Validate(const Endpoint& ep);
bool IsSameHost(const Endpoint& lhs, const Endpoint& rhs);
butil::EndPoint ToBRpcEndpoint(const Endpoint& ep);
static inline bool operator==(const Endpoint& lhs, const Endpoint& rhs) {
    return lhs.port() == rhs.port() &&  //
           lhs.ip4() == rhs.ip4() && lhs.ip6() == rhs.ip6();
}
Status ToEndpoint2(const Endpoint& ep, Endpoint2* result);
Status ToEndpoint(const Endpoint2& ep, Endpoint* result);
static inline bool operator!=(const Endpoint& lhs, const Endpoint& rhs) { return !(lhs == rhs); }
static inline std::ostream& operator<<(std::ostream& os, const Endpoint& obj) {
    return os << to_string(obj);
}

/////

std::string to_string(const Location& loc);
struct LocationHash {
    std::size_t operator()(const Location& loc) const noexcept {
        return std::hash<std::string>()(to_string(loc));
    }
};
bool Validate(const Location& loc);
bool ValidateFuzzily(const Location& loc);
bool BelongsTo(const Location& sub, const Location& obj);
bool IsEmpty(const Location& loc);
bool IsSame(const Location& lhs, const Location& rhs);
bool InSameVdc(const Location& lhs, const Location& rhs);

static inline bool operator==(const Location& lhs, const Location& rhs) { return IsSame(lhs, rhs); }
static inline bool operator!=(const Location& lhs, const Location& rhs) { return !(lhs == rhs); }
static inline std::ostream& operator<<(std::ostream& os, const Location& obj) {
    return os << to_string(obj);
}

//////

Status Validate(const ServerInfo& info);
Status Validate(const ProxyInfo& info);
Status Validate(const ProxyGroupInfo& info);
Status Validate(const ProxyConfig& info);
Status Validate(const NamespaceInfo& info);
Status Validate(const TableInfo& info);

//////

ServerInfo Request2Info(const metaserver::AddServerRequest&);
ProxyInfo Request2Info(const metaserver::AddProxyRequest&);
NamespaceInfo Request2Info(const metaserver::AddNamespaceRequest&);
TableInfo Request2Info(const metaserver::AddTableRequest&);

template <typename T>
bool ProtoEqual(const T& l, const T& r) {
    return google::protobuf::util::MessageDifferencer::Equals(l, r);
}

}  // namespace bcache2

