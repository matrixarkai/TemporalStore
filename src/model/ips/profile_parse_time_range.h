// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>
// #include <butil/third_party/rapidjson/document.h>
// #include <butil/third_party/rapidjson/filereadstream.h>

#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

// #include "bcache/server/ips_interface/ips_define.h"

namespace bcache2 {
namespace ips {

struct time_snap {
    int64_t start;
    int64_t end;
    int64_t precision;
};

typedef struct time_snap time_snap;

int64_t ParseTimeName(const std::string& s);

void ParseTimeSnapConfigFromJson(const rapidjson::Value& val, std::vector<time_snap>* ret);

}  // namespace ips
}  // namespace bcache2
