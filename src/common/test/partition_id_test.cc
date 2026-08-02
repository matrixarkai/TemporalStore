// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <vector>

#include "gtest/gtest.h"

#include "common/partition_id_type.h"

namespace bcache2::common::test {

TEST(PartitionIdTest, AllInOne) {
    std::vector<uint32_t> bit16s{0xFFFF, 0xFFF0, 0xFF00, 0xF0FF, 0xF0F0, 0xF000, 0x0000};
    std::vector<uint32_t> bit8s{0xFF, 0xF0, 0x0F, 0x00};
    for (uint32_t table_id : bit16s) {
        for (uint32_t pset_idx : bit16s) {
            for (uint32_t pidx : bit8s) {
                for (uint32_t v : bit16s) {
                    partition_id_t pid(table_id, pset_idx, pidx, v);
                    ASSERT_EQ(pid.GetTableId(), table_id);
                    ASSERT_EQ(pid.GetPartitionSetIndex(), pset_idx);
                    ASSERT_EQ(pid.GetPartitionIndex(), pidx);
                    ASSERT_EQ(pid.GetPartitionVersion(), v);
                    partition_id_t pid2(0);
                    pid2.SetPartitionSetId(pid.GetPartitionSetId());
                    pid2.SetPartitionIndex(pidx);
                    pid2.SetPartitionVersion(v);
                    ASSERT_EQ(pid2.GetTableId(), table_id);
                    ASSERT_EQ(pid2.GetPartitionSetIndex(), pset_idx);
                    ASSERT_EQ(pid2.GetPartitionIndex(), pidx);
                    ASSERT_EQ(pid2.GetPartitionVersion(), v);
                    ASSERT_EQ(pid.id, pid2.GetId());
                }
            }
        }
    }  // for table_id
}

}  // namespace bcache2::common::test

