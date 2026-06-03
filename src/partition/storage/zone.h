// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/memory/memory.h>
#include <byte/include/assert.h>
#include <byte/string/format.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

DECLARE_int32(storage_zone_size);
DECLARE_int32(storage_max_zone_count);

namespace bcache2 {
namespace partition {

const uint32_t kInvalidZoneId = UINT32_MAX;

inline uint32_t ExtractZoneId(uint64_t address) { return address >> 32; }

inline uint32_t ExtractZoneOffset(uint64_t address) { return address & 0xFFFFFFFF; }

inline uint64_t MakeZoneAddress(uint32_t zone_id, uint32_t offset) {
    return (static_cast<uint64_t>(zone_id) << 32) | offset;
}

class ZoneStats {
 public:
    ZoneStats() {
        pagestore_zones_.resize(FLAGS_storage_max_zone_count);
        oplog_zones_.resize(FLAGS_storage_max_zone_count);
    }

    void UpdatePageStoreStat(uint64_t address, int32_t size) {
        uint32_t zone_id = ExtractZoneId(address);
        Zone* zone = &pagestore_zones_[zone_id % FLAGS_storage_max_zone_count];
        Update(zone_id, zone, size);
    }

    void UpdateOpLoggerStat(uint64_t address, int32_t size) {
        uint32_t zone_id = address / FLAGS_storage_zone_size;
        Zone* zone = &oplog_zones_[zone_id % FLAGS_storage_max_zone_count];
        Update(zone_id, zone, size);
    }

    uint32_t GetPageStoreUsedBytes(uint32_t zone_id) const {
        const Zone& zone = pagestore_zones_[zone_id % FLAGS_storage_max_zone_count];
        return zone.zone_id == zone_id ? zone.used_bytes : 0;
    }

    uint32_t GetOpLoggerUsedBytes(uint32_t zone_id) const {
        const Zone& zone = oplog_zones_[zone_id % FLAGS_storage_max_zone_count];
        return zone.zone_id == zone_id ? zone.used_bytes : 0;
    }

 private:
    struct Zone {
        uint32_t zone_id = kInvalidZoneId;
        // an oplog is shared by several slot
        // so the logical used bytes may exceed 1 << 31 (2 * FLAGS_storage_max_zone_count)
        int64_t used_bytes = 0;
    };

    void Update(uint32_t zone_id, Zone* zone, int32_t size) {
        if (UNLIKELY(zone->zone_id == kInvalidZoneId)) {
            BYTE_ASSERT(zone->used_bytes == 0) << zone_id << " " << zone->used_bytes;
            zone->zone_id = zone_id;
        } else {
            BYTE_ASSERT(zone->zone_id == zone_id && zone->used_bytes > 0);
        }
        zone->used_bytes += size;
        BYTE_ASSERT(zone->used_bytes >= 0) << zone_id << " " << zone->used_bytes;
        if (UNLIKELY(zone->used_bytes == 0)) {
            zone->zone_id = kInvalidZoneId;
        }
    }

    std::vector<Zone> pagestore_zones_;
    std::vector<Zone> oplog_zones_;

    DISALLOW_COPY_AND_ASSIGN(ZoneStats);
};

}  // namespace partition
}  // namespace bcache2
