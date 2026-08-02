// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bthread/bthread.h"

#include "common/status.h"
#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/serializable.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

class Namespace;

class ProxyStats {
 public:
    int64_t GetLastHeartbeatTimeUs() const;
    void SetLastHeartbeatTimeUs(int64_t);

    void SetReportedBootTimeUs(int64_t);
    int64_t GetReportedBootTimeUs() const;

    void SetBinaryVersion(std::string v);
    std::string GetBinaryVersion() const;

    std::string ToString() const;

 private:
    mutable bthread::Mutex mu_;
    int64_t last_heartbeat_time_us_{0};
    int64_t reported_boot_time_us_{0};
    std::string binary_version_{};
};

class ProxyGroup;

class Proxy : public Serializable, public DeepCopy {
 public:
    Proxy() = default;
    explicit Proxy(ProxyInfo info);
    ~Proxy() = default;

    uint32_t GetId();
    void SetId(uint32_t id);
    Location GetLocation();
    Endpoint GetEndpoint();
    std::string GetNamespaceName();

    std::string GetName();
    void SetName(std::string name);

    ProxyGroup* GetProxyGroup();
    void Attach(ProxyGroup* proxy_group, const std::string& ns_name);
    void Detach();

    ProxyState GetState();
    void SetState(ProxyState state);
    ProxyInfo GetInfo();

    ProxyStats* MutableRealtimeStats();
    const ProxyStats& RealtimeStats();

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;
    Status LoadSnapshot(google::protobuf::io::FileInputStream* stream);
    Status DumpSnapshot(google::protobuf::io::FileOutputStream* stream);
    std::string GetSerializeTypeName() override { return "proxy"; }
    Status SerializeToString(std::string* output) override;
    Status ParseFromString(const std::string& input) override;

 private:
    std::string name_;

    bthread::Mutex mu_;
    ProxyInfo info_;
    ProxyGroup* group_{nullptr};

    // Note: below are out of fsm
    ProxyStats rt_stats_;
};

using ProxyPtr = std::shared_ptr<Proxy>;

class ProxyGroup : public Serializable, public DeepCopy {
 public:
    explicit ProxyGroup(ProxyGroupInfo info) : info_(std::move(info)) {
        info_.mutable_config()->set_version(1);
    }
    ~ProxyGroup() = default;

    ProxyGroupInfo GetInfo();
    Location GetPlacement();

    // behavior: overwrite
    void UpdateInfo(const ProxyGroupInfo& info);

    size_t GetProxyCount();
    Status AddProxy(ProxyPtr proxy);
    Status RemoveProxy(ProxyPtr proxy);
    void RemoveAllProxies();
    std::vector<ProxyPtr> ListAllProxies();

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;
    Status LoadSnapshot(google::protobuf::io::FileInputStream* stream);
    Status DumpSnapshot(google::protobuf::io::FileOutputStream* stream);
    DEFINE_COMMON_SERIALIZABLE("proxy_group")

 private:
    bthread::Mutex mu_;
    ProxyGroupInfo info_;
    std::unordered_map<uint32_t, ProxyPtr> proxies_;
};

using ProxyGroupPtr = std::shared_ptr<ProxyGroup>;

class ProxyCluster : public DeepCopy {
 public:
    explicit ProxyCluster(Namespace* ns);
    ~ProxyCluster() = default;

    Namespace* GetNamespace();

    Status CreateOrUpdateProxyGroup(const ProxyGroupInfo& info);
    Status DropProxyGroup(const ProxyGroupPtr& group);
    Status GetProxyGroup(const Location& loc, ProxyGroupPtr* group);
    Status SearchProxyGroup(const Location& loc, ProxyGroupPtr* group);
    std::vector<ProxyGroupPtr> ListAllProxyGroups();

    // for snapshot loading
    void PutProxyGroup(ProxyGroupPtr group);

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;

 private:
    Namespace* const ns_{nullptr};

    bthread::Mutex mu_;
    std::unordered_map<Location, ProxyGroupPtr, LocationHash> groups_;
};

using ProxyClusterPtr = std::shared_ptr<ProxyCluster>;

class ConsulMap : public DeepCopy {
 public:
    ConsulMap() = default;
    ~ConsulMap() = default;

    void UpdateReservedConsulNames(std::unordered_set<std::string> v);

    Status Validate(const ProxyClusterPtr& cluster, const std::string& name);
    Status Calibrate(const ProxyClusterPtr& cluster);
    Status Remove(const ProxyClusterPtr& cluster);

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;

 private:
    bthread::Mutex mu_;
    std::unordered_set<std::string> reserved_consul_names_;

    std::unordered_map<uint32_t, std::unordered_set<std::string>> ns_to_consul_map_;
    std::unordered_map<std::string, uint32_t> consul_to_ns_map_;
};

}  // namespace metaserver
}  // namespace bcache2
