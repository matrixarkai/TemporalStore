// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <assert.h>
#include <stdint.h>

#include <ostream>

#include "butil/logging.h"

namespace bcache2 {

constexpr uint32_t kSlotNum = 1 << 30;
constexpr uint32_t kSlotMask = kSlotNum - 1;
constexpr uint32_t kMinSlotsPerPartition = 1 << 14;  // 16384
constexpr uint32_t kPartitionVersionMask = 0xFFFFUL;
constexpr uint32_t kMaxTableId = 0xFFFFUL;
constexpr uint32_t kPartitionIndexMask = 0xFFUL;

static constexpr bool validate_partition_set_num(uint32_t n) {
    return n > 0 && n <= kSlotNum / kMinSlotsPerPartition;
}

static constexpr bool validate_partition_num_per_set(uint32_t n) { return n > 0 && n <= 0xFFUL; }

/// format:
/// +-------------+--------------+---------------+------------+---------------+
/// | reserved(8) | table_id(16) | p_set_idx(16) | p_index(8) | p_version(16) |
/// +-------------+--------------+---------------+------------+---------------+
///  Note: letter p is abbreviation for partition
struct PartitionId {
    explicit PartitionId(uint64_t x) : id(x) {}

    PartitionId(uint32_t table_id, uint32_t pset_idx, uint32_t pidx, uint32_t pver) {
        CHECK_LE(table_id, 0xFFFFUL);
        CHECK_LE(pset_idx, 0xFFFFUL);
        CHECK_LE(pidx, 0xFFUL);
        CHECK_LE(pver, 0xFFFFUL);
        id = table_id;
        id = (id << 16) | pset_idx;
        id = (id << 8) | pidx;
        id = (id << 16) | pver;
    }
    uint32_t GetTableId() const { return (id >> 40) & 0xFFFFUL; }
    uint64_t GetPartitionSetId() const { return id >> 24; }
    uint32_t GetPartitionSetIndex() const { return (id >> 24) & 0xFFFFUL; }
    uint32_t GetPartitionIndex() const { return (id >> 16) & 0xFFUL; }
    uint32_t GetPartitionVersion() const { return id & 0xFFFFUL; }

    void SetPartitionSetId(uint64_t pset_id) {
        CHECK_LE(pset_id, 0xFFFFFFFFUL);
        id = (id & 0xFFFFFFUL) | (pset_id << 24);
    }

    void SetPartitionIndex(uint32_t pidx) {
        CHECK_LE(pidx, 0xFFUL);
        const uint64_t mask = ~0xFF0000UL;
        id = (id & mask) | (pidx << 16);
    }

    void SetPartitionVersion(uint32_t v) {
        CHECK_LE(v, 0xFFFFUL);
        id = ((id >> 16) << 16) | v;
    }

    uint64_t GetId() const { return id; }

    uint64_t id{0};
};
using partition_id_t = PartitionId;

static_assert(sizeof(partition_id_t) == sizeof(uint64_t));

inline std::ostream& operator<<(std::ostream& os, const partition_id_t& obj) {
    return os << obj.id << "(" << obj.GetTableId() << "|" << obj.GetPartitionSetId() << "|"
              << obj.GetPartitionIndex() << "|" << obj.GetPartitionVersion() << ")";
}

}  // namespace bcache2

