// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <random>
#include <string>

#include "butil/file_util.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"

#include "common/logging.h"
#include "protocol/host_spec.pb.h"

namespace bcache2 {
namespace server {
DECLARE_string(host_spec_path);
}  // namespace server

static int RandomPort() {
    static thread_local std::random_device rd;
    static thread_local std::mt19937 rng(rd());
    return std::uniform_int_distribution<int>(5000, 9000)(rng);
}

static Location MockLocation() {
    Location location;
    location.set_vregion("vregion");
    location.set_vdc("vdc");
    location.set_vau("vau");
    return location;
}

static Status InitHostSpec(std::string path, int port, const Location& location) {
    server::FLAGS_host_spec_path = path;
    server::HostSpec spec;
    auto endpoint = spec.mutable_endpoint();
    endpoint->set_ip4(getenv("BYTED_HOST_IP"));
    endpoint->set_ip6(getenv("BYTED_HOST_IPV6"));
    if (endpoint->ip4().empty()) {
        endpoint->set_ip4("127.0.0.1");
    }
    if (endpoint->ip6().empty()) {
        endpoint->set_ip6("::1");
    }
    endpoint->set_addr_family(Endpoint::ADDR_DUAL_STACK);
    endpoint->set_port(port);
    *spec.mutable_location() = location;

    struct json2pb::Pb2JsonOptions options;
    options.pretty_json = true;
    options.enum_option = json2pb::EnumOption::OUTPUT_ENUM_BY_NUMBER;
    std::string json_data;
    std::string err_msg;
    json2pb::ProtoMessageToJson(spec, &json_data, options, &err_msg);

    butil::FilePath fp(path);
    int rc = butil::WriteFile(fp, json_data.data(), json_data.size());
    if (rc < 0) {
        return Status::Internal("failed to write file " + path);
    }
    LOG_INFO("persist host spec")
        .put("path", path)
        .put("size", json_data.size())
        .put("v", spec.ShortDebugString());
    return Status::OK();
}

static Status InitHostSpec(std::string path, int port) {
    return InitHostSpec(std::move(path), port, MockLocation());
}

}  // namespace bcache2

