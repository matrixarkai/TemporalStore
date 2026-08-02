// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <string>

#include "butil/time.h"

#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace server {

static void InitRequestId(const std::string& cluster_name, metaserver::RequestId* id) {
    id->set_timestamp(butil::gettimeofday_s());
    id->set_cluster_name(cluster_name);
    id->set_operator_name("server");
}

}  // namespace server
}  // namespace bcache2

