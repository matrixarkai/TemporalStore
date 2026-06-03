// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

namespace bcache2 {
namespace metaserver {

constexpr int kTaskPriorityBalanceTable = 15;
constexpr int kTaskPriorityCreateTable = 15;
constexpr int kTaskPriorityCreatePartitionOrdinary = 10;
constexpr int kTaskPriorityCreatePartitionUrgent = 5;
constexpr int kTaskPriorityCreatePartitionCritical = 0;
constexpr int kTaskPriorityUpdateMembership = 0;

}  // namespace metaserver
}  // namespace bcache2

