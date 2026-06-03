// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "common/proto_enhance.h"
#include "metaserver_v2/meta/serializable.h"

namespace bcache2 {
namespace metaserver {

template <typename PK, typename V, typename Compare = std::less<PK>>
class LocationContainer {
 public:
    explicit LocationContainer(std::string name) : name_(std::move(name)) {}
    ~LocationContainer() = default;

    const std::string& GetName() const { return name_; }

    Status CascadeAdd(V v, PK k) {
        auto p = elements_.emplace(k, v);
        return p.second ? Status::OK() : Status::AlreadyExists("");
    }

    template <typename T, typename CurrLevel, typename... DownLevels>
    Status CascadeAdd(T t, CurrLevel k, DownLevels... down_levels) {
        auto iter = elements_.emplace(k, k).first;
        return iter->second.CascadeAdd(t, down_levels...);
    }

    Status CascadeRemove(PK k) {
        size_t c = elements_.erase(k);
        return c > 0 ? Status::OK() : Status::NotFound("");
    }

    template <typename CurrLevel, typename... DownLevels>
    Status CascadeRemove(CurrLevel k, DownLevels... down_levels) {
        auto iter = elements_.find(k);
        if (iter == elements_.end()) {
            return Status::NotFound("");
        }
        Status status = iter->second.CascadeRemove(down_levels...);
        if (!status.ok()) {
            return status;
        }
        if (iter->second.Empty()) {
            elements_.erase(iter);
        }
        return Status::OK();
    }

    template <typename T, typename Filter>
    typename std::vector<T> List(Filter f) {
        std::vector<V> result;
        for (auto& pair : elements_) {
            if (f(pair.second)) {
                result.push_back(pair.second);
            }
        }
        return result;
    }

    template <typename T, typename Filter, typename... DownFilters>
    typename std::vector<T> List(Filter f, DownFilters... df) {
        std::vector<T> result;
        for (auto& pair : elements_) {
            if (f(pair.first)) {
                auto partial = pair.second.template List<T>(df...);
                std::copy(partial.begin(), partial.end(), std::back_inserter(result));
            }
        }
        return result;
    }

    bool Empty() { return elements_.empty(); }

 private:
    const std::string name_;

    std::map<PK, V, Compare> elements_;
};

/// Instance traits:
///   uint32_t GetId();
///   void SetId(uint32_t id);
///   Location GetLocation();
///   Endpoint GetEndpoint();
template <typename Instance>
class LocationManager : public DeepCopy {
 public:
    using InstancePtr = std::shared_ptr<Instance>;

 public:
    LocationManager() = default;
    ~LocationManager() = default;

    Status Add(InstancePtr instance);
    Status Remove(const InstancePtr& instance);

    InstancePtr Get(uint32_t id) {
        std::lock_guard<bthread::Mutex> _(mu_);
        auto iter = flat_id_map_.find(id);
        if (iter == flat_id_map_.end()) {
            return nullptr;
        }
        return iter->second;
    }

    InstancePtr Get(const Endpoint& endpoint) {
        std::lock_guard<bthread::Mutex> _(mu_);
        auto iter = flat_endpoint_map_.find(endpoint);
        if (iter == flat_endpoint_map_.end()) {
            return nullptr;
        }
        return iter->second;
    }

    // Note: property tag would not be filtered
    using Filter = std::function<bool(const InstancePtr&)>;
    std::vector<InstancePtr> List(const Location& target_loc, Filter filter) {
        auto make_filter = [](const std::string& t) -> auto {
            return [&t](const std::string& v) -> bool { return t.empty() || t == v; };
        };

        std::lock_guard<bthread::Mutex> _(mu_);
        return root_.template List<InstancePtr>(make_filter(target_loc.vregion()),
                                                make_filter(target_loc.vdc()),
                                                make_filter(target_loc.vau()), filter);
    }

    std::vector<InstancePtr> ListAll() {
        std::vector<InstancePtr> instances;
        std::lock_guard<bthread::Mutex> _(mu_);
        instances.reserve(flat_id_map_.size());
        for (const auto& pair : flat_id_map_) {
            instances.push_back(pair.second);
        }
        return instances;
    }

    uint32_t GetIdCursor() {
        std::lock_guard<bthread::Mutex> _(mu_);
        return id_cursor_;
    }

    void SetIdCursor(uint32_t v) {
        std::lock_guard<bthread::Mutex> _(mu_);
        id_cursor_ = v;
    }

    bool Equal(DeepCopy* rhs_base) override;
    void DeepCopyTo(DeepCopy* rhs_base) override;

 private:
    /// VAU(Virtual Availability Unit), Normally VAU equals to physical availability zone (AZ)
    using VAU = LocationContainer<uint32_t, InstancePtr>;
    using VDC = LocationContainer<std::string, VAU>;
    using VRegion = LocationContainer<std::string, VDC>;
    using Root = LocationContainer<std::string, VRegion>;

 private:
    bthread::Mutex mu_;

    uint32_t id_cursor_{0};  // starts from 1
    Root root_{"/"};
    std::unordered_map<uint32_t, InstancePtr> flat_id_map_;
    std::unordered_map<Endpoint, InstancePtr, EndpointHash> flat_endpoint_map_;
};

template <typename Instance>
Status LocationManager<Instance>::Add(LocationManager<Instance>::InstancePtr instance) {
    const Location& loc = instance->GetLocation();
    const Endpoint& ep = instance->GetEndpoint();

    std::lock_guard<bthread::Mutex> _(mu_);
    if (flat_endpoint_map_.count(ep) > 0) {
        return Status::FailedPrecondition("duplicated endpoint");
    }

    uint32_t id = instance->GetId();
    if (id == 0) {
        id = ++id_cursor_;
        instance->SetId(id);
    }
    Status status =
        root_.CascadeAdd(instance, loc.vregion(), loc.vdc(), loc.vau(), instance->GetId());
    if (status.ok()) {
        flat_id_map_[id] = instance;
        flat_endpoint_map_[ep] = instance;
    }
    return status;
}

template <typename Instance>
Status LocationManager<Instance>::Remove(const LocationManager<Instance>::InstancePtr& instance) {
    const Location& loc = instance->GetLocation();
    std::lock_guard<bthread::Mutex> _(mu_);
    Status status = root_.CascadeRemove(loc.vregion(), loc.vdc(), loc.vau(), instance->GetId());
    if (status.ok()) {
        flat_id_map_.erase(instance->GetId());
        flat_endpoint_map_.erase(instance->GetEndpoint());
    }
    return status;
}

template <typename Instance>
bool LocationManager<Instance>::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<LocationManager<Instance>*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (id_cursor_ != rhs->id_cursor_) {
        LOG_INFO("id cursor not equal").put("mine", id_cursor_).put("rhs", rhs->id_cursor_);
        return false;
    }
    if (!MapEqual(flat_id_map_, rhs->flat_id_map_)) {
        LOG_INFO("flat id map not equal");
        return false;
    }
    if (!MapEqual(flat_endpoint_map_, rhs->flat_endpoint_map_)) {
        LOG_INFO("flat endpoint map not equal");
        return false;
    }
    return true;
}

template <typename Instance>
void LocationManager<Instance>::DeepCopyTo(DeepCopy* rhs_base) {
    Status status;
    auto rhs = static_cast<LocationManager<Instance>*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->id_cursor_ = id_cursor_;
    for (const auto& pair : flat_id_map_) {
        auto& instance = pair.second;
        auto cpy = std::make_shared<Instance>();
        instance->DeepCopyTo(cpy.get());
        status = rhs->Add(cpy);
        CHECK(status.ok()) << status;
    }
}

}  // namespace metaserver
}  // namespace bcache2
