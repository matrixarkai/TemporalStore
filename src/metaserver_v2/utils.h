// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <ctype.h>
#include <stdint.h>

#include <string>

#include "spdlog/fmt/fmt.h"

namespace bcache2 {
namespace metaserver {

static std::string GetTableFullName(const std::string& ns, const std::string& table) {
    return fmt::format("{}/{}", ns, table);
}

static bool EndWithSlash(const std::string& path) {
    return !path.empty() && path[path.size() - 1] == '/';
}

static std::string GeneratePartitionSetUri(const std::string& prefix, const std::string& mid,
                                           uint64_t pset_id) {
    return EndWithSlash(prefix) ? fmt::format("{}{}-{}", prefix, mid, pset_id)
                                : fmt::format("{}/{}-{}", prefix, mid, pset_id);
}

}  // namespace metaserver
}  // namespace bcache2

