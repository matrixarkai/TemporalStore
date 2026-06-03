// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include "client/router.h"
#include "client/router_v2.h"
#include "common/status.h"
#include "protocol/master.pb.h"

namespace bcache2 {
namespace client {

class RouterFactory {
 public:
    const Router* Get() const {
        return version_ == Version::kV1 ? static_cast<const Router*>(&v1_)
                                        : static_cast<const Router*>(&v2_);
    }

    Status UpdatePartition(const GetTableTopoResponse& topo) {
        Status status;
        if (topo.topo_structure_version() == 2) {
            version_ = Version::kV2;
            status = v2_.UpdatePartition(topo);
        } else {
            version_ = Version::kV1;
            status = v1_.UpdatePartition(topo);
        }
        return status;
    }

    void SetAddressFamily(AddressFamily af) { v2_.SetAddressFamily(af); }

 private:
    enum class Version { kV1 = 1, kV2 = 2 };
    Version version_{Version::kV1};
    RouterV1 v1_;
    RouterV2 v2_;
};
}  // namespace client
}  // namespace bcache2

