// Copyright (c) 2020-present, ByteDance Inc. All rights reserved.

#include "common/consul.h"

#include <sstream>

#include "brpc/channel.h"
#include "common/logging.h"
#include "json2pb/rapidjson.h"

namespace bcache2 {
namespace service_discovery {

static inline std::string AddrFamilyName(const Consul::AddrFamily& addr_family) {
    switch (addr_family) {
    case Consul::AddrFamily::DualStack:
        return "dual-stack";
    case Consul::AddrFamily::V4:
        return "v4";
    case Consul::AddrFamily::V6:
        return "v6";
    case Consul::AddrFamily::Auto:
        const char* ipv4 = getenv("BYTED_HOST_IP");
        const char* ipv6 = getenv("BYTED_HOST_IPV6");
        const bool v4 = ipv4 && ipv4[0];
        const bool v6 = ipv6 && ipv6[0];
        if (v4 && v6) {
            return "dual-stack";
        } else if (v6) {
            return "v6";
        } else if (v4) {
            return "v4";
        }
        return "";
    }
    return "";
}

static Status HttpRequest(brpc::HttpMethod method, const std::string& url,
                          const butil::rapidjson::Document& request,
                          butil::rapidjson::Document* response) {
    brpc::ChannelOptions opts;
    opts.protocol = brpc::PROTOCOL_HTTP;
    brpc::Channel channel;
    if (channel.Init(url.c_str(), &opts) != 0) {
        LOG_WARNING("Init channel failed").put("Url", url);
        return Status::FailedPrecondition("Init channel failed");
    }

    brpc::Controller ctrl;
    ctrl.http_request().uri() = url;
    ctrl.http_request().set_method(method);

    if (!request.IsNull()) {
        butil::rapidjson::StringBuffer buffer;
        butil::rapidjson::Writer<butil::rapidjson::StringBuffer> writer(buffer);
        request.Accept(writer);
        ctrl.request_attachment() = buffer.GetString();
        ctrl.http_request().set_content_type("application/json");
    }

    channel.CallMethod(nullptr, &ctrl, nullptr, nullptr, nullptr);
    if (ctrl.Failed()) {
        LOG_WARNING("Http request failed").put("Url", url).put("Error", ctrl.ErrorText());
        return Status::Internal(ctrl.ErrorText());
    }

    if (response) {
        response->Parse<0>(ctrl.response_attachment().to_string().c_str());
    }

    LOG_DEBUG("Http finish")
        .put("Url", url)
        .put("Request", ctrl.request_attachment().to_string())
        .put("Response", ctrl.response_attachment().to_string());
    return Status::OK();
}

Status Consul::Lookup(const std::string& name, const AddrFamily& addr_family,
                      std::vector<Endpoint>* endpoints) {
    char url[1024] = {};
    snprintf(url, sizeof(url), "http://%s:%d/v1/lookup/name?name=%s&addr-family=%s",
             agent_host_.c_str(), agent_port_, name.c_str(), AddrFamilyName(addr_family).c_str());

    butil::rapidjson::Document request;
    butil::rapidjson::Document response;
    Status status = HttpRequest(brpc::HTTP_METHOD_GET, url, request, &response);
    if (!status.ok()) {
        LOG_WARNING("Failed to translate name").put("Url", url).put("Status", status);
        return status;
    }

    endpoints->clear();
    for (size_t i = 0; i < response.Size(); i++) {
        endpoints->emplace_back(response[i]["Host"].GetString(), response[i]["Port"].GetInt());
    }
    return Status::OK();
}

Status Consul::LookupWithCluster(const std::string& name, const std::string& cluster,
        const AddrFamily& addr_family, std::vector<Endpoint>* endpoints) {
    char url[1024] = {};
    snprintf(url, sizeof(url), "http://%s:%d/v1/lookup/name?name=%s&addr-family=%s",
             agent_host_.c_str(), agent_port_, name.c_str(), AddrFamilyName(addr_family).c_str());

    butil::rapidjson::Document request;
    butil::rapidjson::Document response;
    Status status = HttpRequest(brpc::HTTP_METHOD_GET, url, request, &response);
    if (!status.ok()) {
        LOG_WARNING("Failed to translate name").put("Url", url).put("Status", status);
        return status;
    }

    endpoints->clear();
    for (size_t i = 0; i < response.Size(); i++) {
        if (!response[i].HasMember("Tags")) {
            continue;
        }
        if (!response[i]["Tags"].HasMember("cluster")) {
            continue;
        }
        const auto& consul_cluster = std::string(response[i]["Tags"]["cluster"].GetString());
        if (cluster.compare(consul_cluster) != 0) {
            continue;
        }
        endpoints->emplace_back(response[i]["Host"].GetString(), response[i]["Port"].GetInt());
    }
    return Status::OK();
}

Consul::Consul() {
    const char* ipv4 = getenv("BYTED_HOST_IP");
    const char* ipv6 = getenv("BYTED_HOST_IPV6");
    if (ipv4 != nullptr && ipv4[0] != '\0') {  // use ipv4 address
        agent_host_ = ipv4;
    } else if (ipv6 != nullptr && ipv6[0] != '\0') {  // use ipv6 address
        // agent_host_ = byte::StringPrint("[%s]", ipv6);
        agent_host_ = std::string("[") + ipv6 + std::string("]");
    } else {
        agent_host_ = "127.0.0.1";
    }
    agent_port_ = 2280;  // the port of consul agent
}

Status Consul::Register(const std::string& name, const int port, const int ttl_s) {
    std::string service_id = name + "-" + std::to_string(port);

    char check_url[1024] = {};
    snprintf(check_url, sizeof(check_url), "http://%s:%d/v1/agent/check/pass/service:%s",
             agent_host_.c_str(), agent_port_, service_id.c_str());

    if (!HttpRequest(brpc::HTTP_METHOD_GET, check_url, butil::rapidjson::Document(), nullptr)
             .ok()) {
        butil::rapidjson::Document request;
        request.SetObject();
        request.AddMember("id", butil::rapidjson::Value(service_id.c_str(), request.GetAllocator()),
                          request.GetAllocator());
        request.AddMember("name", butil::rapidjson::Value(name.c_str(), request.GetAllocator()),
                          request.GetAllocator());
        request.AddMember("port", port, request.GetAllocator());
        butil::rapidjson::Value checks(butil::rapidjson::kObjectType);
        std::string str_ttl = std::to_string(ttl_s) + "s";
        checks.AddMember("ttl", butil::rapidjson::Value(str_ttl.c_str(), request.GetAllocator()),
                         request.GetAllocator());
        request.AddMember("check", checks, request.GetAllocator());

        char register_url[1024] = {};
        snprintf(register_url, sizeof(register_url),
                 "http://%s:%d/v1/agent/service/register?dual-stack", agent_host_.c_str(),
                 agent_port_);
        butil::rapidjson::Document response;
        Status status = HttpRequest(brpc::HTTP_METHOD_PUT, register_url, request, &response);
        if (!status.ok()) {
            LOG_WARNING("Http request failed").put("Url", register_url).put("Status", status);
            return status;
        }

        status =
            HttpRequest(brpc::HTTP_METHOD_GET, check_url, butil::rapidjson::Document(), nullptr);
        if (!status.ok()) {
            LOG_WARNING("Http request failed").put("Url", check_url).put("Status", status);
            return status;
        }
    }

    return Status::OK();
}

Status Consul::DeRegister(const std::string& name, const int port) {
    std::string service_id = name + "-" + std::to_string(port);

    char url[1024] = {};
    snprintf(url, sizeof(url), "http://%s:%d/v1/agent/service/deregister/%s", agent_host_.c_str(),
             agent_port_, service_id.c_str());

    Status status = HttpRequest(brpc::HTTP_METHOD_PUT, url, butil::rapidjson::Document(), nullptr);
    if (!status.ok()) {
        LOG_WARNING("Http request failed").put("Url", url).put("Status", status);
        return status;
    }

    return Status::OK();
}

}  // namespace service_discovery
}  // namespace bcache2
