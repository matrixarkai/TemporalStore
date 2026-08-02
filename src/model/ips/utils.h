// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <butil/strings/string_number_conversions.h>
#include <butil/strings/string_util.h>
// #include <cpputil/env/include/env.h>
// #include <idl/bcache/bcache_controller_types.h>
#include <spdlog/fmt/fmt.h>
#include <sys/time.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <ctime>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// #include "bcache/common/flags.h"
// #include "bcache/common/random.h"

// make_unique template is removed because the support of c++17 no longer need it
namespace bcache2 {
namespace ips {
// 2 small internal utility functions, for efficient hex conversions
// and no need for snprintf, toupper etc...
// Originally from wdt/util/EncryptionUtils.cpp - for ToString(true)/DecodeHex:
inline char toHex(unsigned char v) {
    if (v <= 9) {
        return '0' + v;
    }
    return 'A' + v - 10;
}

inline int fromHex(char c) {
    // toupper:
    if (c >= 'a' && c <= 'f') {
        c -= ('a' - 'A');  // aka 0x20
    }
    // validation
    if (c < '0' || (c > '9' && (c < 'A' || c > 'F'))) {
        return -1;  // invalid not 0-9A-F hex char
    }
    if (c <= '9') {
        return c - '0';
    }
    return c - 'A' + 10;
}

inline bool EndsWith(const std::string& str, const std::string& suffix) {
    return str.size() >= suffix.size() &&
           0 == str.compare(str.size() - suffix.size(), suffix.size(), suffix);
}

// inline void ReplaceTCEPSM(std::string* str_val) {
//     if (!FLAGS_tce_mode) {
//         return;
//     }
//     const std::string target_str = "$TCE_PSM";
//     auto iter = str_val->find(target_str);
//     if (iter == std::string::npos) {
//         return;
//     }
//     char* tce_psm = getenv("TCE_PSM");
//     assert(tce_psm != nullptr);
//     std::string real_psm(tce_psm);
//     str_val->replace(iter, target_str.size(), real_psm);
// }

// https://stackoverflow.com/a/2595226/3378701
template <class T>
inline void hash_combine(std::size_t* seed_ptr, const T& v) {
    size_t& seed = *seed_ptr;
    std::hash<T> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

inline void SplitString(const std::string& s, char delimiter, std::vector<std::string>* ret) {
    std::string::size_type prev_pos = 0, pos = 0;
    while ((pos = s.find(delimiter, pos)) != std::string::npos) {
        std::string substring(s.substr(prev_pos, pos - prev_pos));
        ret->emplace_back(substring);
        prev_pos = ++pos;
    }
    std::string substring(s.substr(prev_pos, pos - prev_pos));
    ret->emplace_back(substring);
}

inline bool StringToDouble(const std::string& str, double* res) {
    try {
        *res = std::stod(str);
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

inline bool StringToInt64(const std::string& str, int64_t* res) {
    try {
        *res = std::stoll(str);
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

inline void TrimString(std::string* str) {
    str->erase(
        std::remove_if(str->begin(), str->end(), [](unsigned char x) { return std::isspace(x); }),
        str->end());
}

inline int64_t GetCurTsMicros() {
    struct timeval t;
    gettimeofday(&t, NULL);
    return ((int64_t)(t.tv_sec) * (int64_t)(1000000) + (int64_t)(t.tv_usec));
}

// template <typename MapType>
// inline bool PickRandomItemFromUnorderedMap(MapType* mapp, const typename MapType::key_type** kpp,
//                                            typename MapType::mapped_type** vpp) {
//     assert(kpp != nullptr && vpp != nullptr);
//     MapType& map = *mapp;
//     const unsigned int bucket_count = map.bucket_count();
//     const unsigned int rand_num = GetCurTsMicros() % bucket_count;
//     for (unsigned int i = rand_num, n = 0; n < bucket_count; ++i, ++n) {
//         unsigned int bucket_i = i % bucket_count;
//         for (auto it = map.begin(bucket_i), end = map.end(bucket_i); it != end; ++it) {
//             *kpp = &it->first;
//             *vpp = &it->second;
//             return true;
//         }
//     }
//     return false;
// }

// inline std::string AddressToString(const idl::bcache::controller::Address& address) {
//     return address.ip + ":" + std::to_string(address.data_port) + ":" +
//            std::to_string(address.controller_port);
// }

// inline int StringToAddress(const std::string& address_str, idl::bcache::controller::Address*
// address) {
//     if (address_str.empty()) {
//         return 1;
//     }
//     auto pos1 = address_str.find_first_of(":");
//     auto pos2 = address_str.find_last_of(":");
//     if (pos1 == std::string::npos || pos2 == std::string::npos || pos1 == pos2) {
//         return 1;
//     }

//     std::string ip = address_str.substr(0, pos1);
//     int32_t data_port = atoi(address_str.substr(pos1 + 1, pos2 - pos1 - 1).c_str());
//     int32_t controller_port = atoi(address_str.substr(pos2 + 1).c_str());
//     if (data_port == 0 || controller_port == 0) {
//         return 1;
//     }

//     address->ip = ip;
//     address->data_port = data_port;
//     address->controller_port = controller_port;
//     return 0;
// }

// inline std::string DataAddressToString(const idl::bcache::controller::Address& address) {
//     return address.ip + ":" + std::to_string(address.data_port);
// }

// inline std::string ControllerAddressToString(const idl::bcache::controller::Address& address) {
//     return address.ip + ":" + std::to_string(address.controller_port);
// }

// inline std::string ShardInfoToString(const idl::bcache::controller::ShardInfo& shard_info) {
//     std::string res = "term: " + std::to_string(shard_info.term) +
//                       ", role: " + std::to_string(shard_info.role) +
//                       ", master_address: " + AddressToString(shard_info.master);
//     std::string slave_str;
//     for (auto& slave : shard_info.slaves) {
//         if (!slave_str.empty()) {
//             slave_str += ", ";
//         }
//         slave_str = slave_str + "{address: " + AddressToString(slave.address) + ", " +
//                     "status: " + std::to_string(slave.status) + "}";
//     }

//     return res + ", slave: [" + slave_str + "]";
// }

// inline std::string ClusterInfoToString(const idl::bcache::controller::ClusterInfo& cluster_info)
// {
//     return "psm: " + cluster_info.psm + ", cluster: " + cluster_info.cluster +
//            ", idc: " + cluster_info.idc + ", status: " + std::to_string(cluster_info.status);
// }

// inline std::string DBInfoToString(const idl::bcache::controller::DBInfo& db_info) {
//     return "psm: " + db_info.psm + ", cluster: " + db_info.cluster +
//            ", db_type: " + std::to_string(db_info.db_types) + ", namespace: " + db_info.ns;
// }

// inline std::string ServerInfoToString(const idl::bcache::controller::ServerInfo& server_info) {
//     std::string res = "version: " + std::to_string(server_info.version) +
//                       ", server_status: " + std::to_string(server_info.status);

//     std::string slot_str;
//     for (auto& slot : server_info.slots) {
//         if (!slot_str.empty()) {
//             slot_str += ", ";
//         }
//         slot_str += slot;
//     }
//     std::string table_str;
//     for (auto& table : server_info.tables) {
//         if (!table_str.empty()) {
//             table_str += ", ";
//         }
//         table_str += table;
//     }
//     return res + ", slot: [" + slot_str + "], table: [" + table_str + "]" + ", shard_info: {" +
//            ShardInfoToString(server_info.shard_info) + "}, cluster_info: {" +
//            ClusterInfoToString(server_info.cluster_info) + "}";
// }

// inline std::string ArchConfigToString(const idl::bcache::controller::ArchConfig& arch_config) {
//     std::string res = "version: " + std::to_string(arch_config.version) +
//                       ", arch_mode: " + std::to_string(arch_config.mode);
//     std::string region_str;
//     for (auto& region : arch_config.regions) {
//         if (!region_str.empty()) {
//             region_str += ", ";
//         }

//         std::string follower_str;
//         for (auto& follower : region.second.followers) {
//             if (!follower_str.empty()) {
//                 follower_str += ", ";
//             }
//             follower_str += ClusterInfoToString(follower);
//         }
//         region_str = region_str + "{region: " + region.first +
//                      "info: {leader: " + ClusterInfoToString(region.second.leader) +
//                      ", follower: " + follower_str + "}}";
//     }
//     return res + ", master_cluster: {" + ClusterInfoToString(arch_config.master_cluster) + "},
//     regions: {" +
//            region_str + "}, db: {" + DBInfoToString(arch_config.db) + "}, db_read: {" +
//            DBInfoToString(arch_config.db_read) + "}";
// }

inline std::string SlotsListToString(std::vector<int32_t> slots) {
    std::sort(slots.begin(), slots.end());
    std::string ret;
    int32_t begin = -1;
    int32_t end = -1;
    for (size_t i = 0; i < slots.size(); i++) {
        if (slots[i] < 0) {
            continue;
        }

        if (begin == -1) {
            begin = slots[i];
            end = slots[i];
        } else if (slots[i] <= end + 1) {
            end = slots[i] > end ? slots[i] : end;
        } else {
            if (!ret.empty()) {
                ret += ",";
            }

            if (begin == end) {
                ret += fmt::format("{}", begin);
            } else {
                ret += fmt::format("{}-{}", begin, end);
            }

            begin = slots[i];
        }
    }

    if (!ret.empty()) {
        ret += ",";
    }

    if (begin == end) {
        ret += fmt::format("{}", begin);
    } else {
        ret += fmt::format("{}-{}", begin, end);
    }

    return ret;
}

// inline const std::string& GetNamespaceOfArchConf(const idl::bcache::controller::ArchConfig&
// config) {
//     return config.__isset.db_read ? config.db_read.ns : config.db.ns;
// }

// inline int CheckSlotIdValid(int64_t slot_id) { return slot_id >= 0 && slot_id < FLAGS_num_slots;
// }

// inline int TransSlotStrToSlotInt(const std::vector<std::string>& slot_descriptions,
//                                  std::vector<int64_t>* slots) {
//     for (const auto& slot_desc : slot_descriptions) {
//         auto pos = slot_desc.find('-');
//         if (pos == std::string::npos) {
//             int64_t slot_id = 0;
//             if (!butil::StringToInt64(slot_desc, &slot_id)) {
//                 return -1;
//             }
//             if (!CheckSlotIdValid(slot_id)) {
//                 return -1;
//             }
//             slots->emplace_back(slot_id);
//         } else {
//             int64_t left_slot_id(0), right_slot_id(0);
//             if (!butil::StringToInt64(slot_desc.substr(0, pos), &left_slot_id) ||
//                 !butil::StringToInt64(slot_desc.substr(pos + 1, slot_desc.size() - pos),
//                 &right_slot_id)) { return -1;
//             }
//             if (!CheckSlotIdValid(left_slot_id) || !CheckSlotIdValid(right_slot_id) ||
//                 (left_slot_id > right_slot_id)) {
//                 return -1;
//             }
//             for (std::int64_t i = left_slot_id; i <= right_slot_id; ++i) {
//                 slots->emplace_back(i);
//             }
//         }
//     }
//     return 0;
// }

}  // namespace ips
}  // namespace bcache2
