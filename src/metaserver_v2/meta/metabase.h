// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "butil/file_util.h"
#include "common/macros.h"
#include "common/status.h"
#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/namespace.h"
#include "metaserver_v2/meta/proxy.h"
#include "metaserver_v2/meta/server.h"
#include "metaserver_v2/meta/table.h"

namespace bcache2 {
namespace metaserver {

enum class DamageSeverity { kNormal, kWarning, kCritical };

static inline std::string DamageSeverityToString(DamageSeverity ds) {
    switch (ds) {
    case DamageSeverity::kNormal:
        return "kNormal";
    case DamageSeverity::kWarning:
        return "kWarning";
    case DamageSeverity::kCritical:
        return "kCritical";
    }
    return "kUnkown";
}
class Metabase : public DeepCopy {
 public:
    Metabase() = default;
    ~Metabase() = default;

    Status Init();

    Status UpdateManageInfo(const ManagementInfo& minfo);
    const ManagementInfo& GetManageInfo();

    LocationManager<Server>* GetServerLocationManager() { return server_location_manager_.get(); }
    LocationManager<Proxy>* GetProxyLocationManager() { return proxy_location_manager_.get(); }

    NamespaceManager* GetNamespaceManager() { return ns_manager_.get(); }

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;
    Status DumpSnapshot(const std::string& dir);
    Status LoadSnapshot(const std::string& dir);

    void SetRaftIndex(uint64_t idx);
    uint64_t GetRaftIndex();

    // Return all server info which in safe mode.
    const std::unordered_map<std::string, DamageSeverity>& GetServerInSafeMode() {
        std::lock_guard<bthread::Mutex> _(safe_mode_mu_);
        return server_in_safe_mode_;
    }

    // Return all proxy info which in safe mode.
    const std::unordered_map<std::string, DamageSeverity>& GetProxyInSafeMode() {
        std::lock_guard<bthread::Mutex> _(safe_mode_mu_);
        return proxy_in_safe_mode_;
    }

    void SetServerInSafeMode(
        const std::unordered_map<std::string, DamageSeverity>& safe_mode_state) {
        std::lock_guard<bthread::Mutex> _(safe_mode_mu_);
        server_in_safe_mode_ = safe_mode_state;
    }

    void SetProxyInSafeMode(
        const std::unordered_map<std::string, DamageSeverity>& safe_mode_state) {
        std::lock_guard<bthread::Mutex> _(safe_mode_mu_);
        proxy_in_safe_mode_ = safe_mode_state;
    }
    void SetMuteMetaChange(bool v);
    bool GetMuteMetaChange();

    // helpers
    Status LocatePartition(uint64_t id, PartitionPtr* partition);

 private:
    Status DumpSnapshotInternal(const butil::FilePath& dir);
    Status CombineServerMeta();
    Status CombineProxyMeta();

 private:
    uint64_t raft_index_{0};
    std::atomic<bool> mute_meta_change_;
    std::unique_ptr<LocationManager<Server>> server_location_manager_;
    std::unique_ptr<LocationManager<Proxy>> proxy_location_manager_;

    std::unique_ptr<NamespaceManager> ns_manager_;

    bthread::Mutex mu_;
    ManagementInfo manage_info_;

    bthread::Mutex safe_mode_mu_;
    std::unordered_map<std::string, DamageSeverity> server_in_safe_mode_;
    std::unordered_map<std::string, DamageSeverity> proxy_in_safe_mode_;
};

}  // namespace metaserver
}  // namespace bcache2
