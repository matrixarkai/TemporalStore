// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <utility>

#include "metaserver_v2/event_harbor.h"
#include "metaserver_v2/meta/proxy.h"
#include "metaserver_v2/meta/server.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

constexpr EventHarbor::topic_t kTopicServerHeartbeat = 1;
constexpr EventHarbor::topic_t kTopicServerDrop = 2;
constexpr EventHarbor::topic_t kTopicServerStop = 3;
constexpr EventHarbor::topic_t kTopicProxyHeartbeat = 4;
constexpr EventHarbor::topic_t kTopicProxyDrop = 5;
constexpr EventHarbor::topic_t kTopicProxyStop = 6;

struct ServerHeartbeatEvent : public EventHarbor::Event {
    explicit ServerHeartbeatEvent(const ServerHeartbeatRequest& req) : request(req) {}
    EventHarbor::topic_t Topic() const override { return kTopicServerHeartbeat; }

    ServerHeartbeatRequest request;
};

struct ServerStopEvent : public EventHarbor::Event {
    explicit ServerStopEvent(ServerPtr s) : server(std::move(s)) {}
    EventHarbor::topic_t Topic() const override { return kTopicServerStop; }

    ServerPtr server;
};

struct ServerDropEvent : public EventHarbor::Event {
    explicit ServerDropEvent(Endpoint ep) : endpoint(std::move(ep)) {}
    EventHarbor::topic_t Topic() const override { return kTopicServerDrop; }

    Endpoint endpoint;
};

struct ProxyHeartbeatEvent : public EventHarbor::Event {
    explicit ProxyHeartbeatEvent(const ProxyHeartbeatRequest& req) : request(req) {}
    EventHarbor::topic_t Topic() const override { return kTopicProxyHeartbeat; }

    ProxyHeartbeatRequest request;
};

struct ProxyStopEvent : public EventHarbor::Event {
    explicit ProxyStopEvent(ProxyPtr s) : proxy(std::move(s)) {}
    EventHarbor::topic_t Topic() const override { return kTopicProxyStop; }

    ProxyPtr proxy;
};

struct ProxyDropEvent : public EventHarbor::Event {
    explicit ProxyDropEvent(Endpoint ep) : endpoint(std::move(ep)) {}
    EventHarbor::topic_t Topic() const override { return kTopicProxyDrop; }

    Endpoint endpoint;
};

}  // namespace metaserver
}  // namespace bcache2

