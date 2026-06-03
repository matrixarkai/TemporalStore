// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <fstream>
#include <string>

namespace bcache2 {

inline std::string IDC() {
    char* ptr = getenv("RUNTIME_IDC_NAME");
    if (ptr != nullptr) {
        return std::string(ptr);
    }

    std::string idc;
    std::ifstream datacenter("/opt/tmp/consul_agent/datacenter");
    if (datacenter.is_open()) {
        while (getline(datacenter, idc)) {
            break;
        }
        datacenter.close();
    }

    std::string::iterator end_pos = remove(idc.begin(), idc.end(), ' ');
    idc.erase(end_pos, idc.end());

    return idc;
}

}  // namespace bcache2
