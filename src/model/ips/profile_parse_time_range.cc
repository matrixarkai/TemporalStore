// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "model/ips/profile_parse_time_range.h"

#include <cstdio>
#include <string>
#include <vector>

#include "model/ips/utils.h"

namespace bcache2 {
namespace ips {

int64_t ParseTimeName(const std::string& str) {
    const int64_t micros = 1;
    const int64_t s = 1000 * 1000 * micros;
    const int64_t m = 60 * s;
    const int64_t h = 60 * m;
    const int64_t d = 24 * h;

    int64_t factor = 0;
    int64_t ret = 0;
    std::vector<std::string> v;
    // time name connected by '+' means accumulation of these time name
    SplitString(str, '+', &v);
    for (const auto& part : v) {
        switch (part.back()) {
        case 's':
            factor = s;
            break;
        case 'm':
            factor = m;
            break;
        case 'h':
            factor = h;
            break;
        case 'd':
            factor = d;
            break;
            // default:
            // BC_FATAL("ParseTimeName failed: invalid symbol in time_snap");
        }
        std::string number = part.substr(0, part.size() - 1);
        ret += static_cast<int64_t>(stoi(number)) * factor;
    }
    return ret;
}

void ParseTimeSnapConfigFromJson(const rapidjson::Value& val, std::vector<time_snap>* ret) {
    time_snap t;
    for (rapidjson::Value::ConstMemberIterator it = val.MemberBegin(); it != val.MemberEnd();
         it++) {
        t.precision = ParseTimeName(it->name.GetString());
        t.start = ParseTimeName(it->value[0].GetString());
        t.end = ParseTimeName(it->value[1].GetString());
        ret->emplace_back(t);
    }

    time_snap last;
    last.start = t.end;
    last.end = ParseTimeName("36500d");
    last.precision = ParseTimeName("365d");
    ret->emplace_back(last);
}

}  // namespace ips
}  // namespace bcache2
