// Copyright (c) 2020-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>

#include <string>
#include <vector>

#include "common/status.h"

namespace bcache2 {
namespace service_discovery {

struct Endpoint {
    std::string host;
    int port = 0;

    Endpoint(const std::string& host, int port) : host(host), port(port) {}
};

class Consul {
 public:
    enum class AddrFamily {
        DualStack,
        V4,
        V6,
        Auto,
    };

    Consul();
    Consul(const std::string& agent_host, int agent_port)
        : agent_host_(agent_host), agent_port_(agent_port) {}
    ~Consul() {}

    Status Register(const std::string& name, const int port, const int ttl_s);
    Status DeRegister(const std::string& name, const int port);
    Status Lookup(const std::string& name, const AddrFamily& addr_family,
                  std::vector<Endpoint>* endpoints);
    Status LookupWithCluster(const std::string& name, const std::string& cluster,
                const AddrFamily& addr_family, std::vector<Endpoint>* endpoints);

 private:
    std::string agent_host_;
    int agent_port_ = 0;

    DISALLOW_COPY_AND_ASSIGN(Consul);
};

}  // namespace service_discovery
}  // namespace bcache2
