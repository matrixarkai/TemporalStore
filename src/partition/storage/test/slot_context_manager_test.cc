// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/slot_context_manager.h"

#include <gtest/gtest.h>

#include "partition/allocator_manager.h"

namespace bcache2 {
namespace partition {
namespace test {

class SlotContextManagerTest : public testing::Test {
 public:
    void SetUp() override {}

    void TearDown() override {}

 protected:
    uint64_t slot_id_ = 100;
    MetricsManager metrics_manager_{{}, ""};
    AllocatorManager manager_{&metrics_manager_};
    SlotContextManager slot_ctx_manager_{&metrics_manager_, &manager_};
};

TEST_F(SlotContextManagerTest, SlotVersion) {
    ASSERT_EQ(slot_ctx_manager_.GetSlotVersion(slot_id_), 0);
    slot_ctx_manager_.SetSlotVersion(slot_id_, 200);
    ASSERT_EQ(slot_ctx_manager_.GetSlotVersion(slot_id_), 200);
    slot_ctx_manager_.ResetSlotContext(slot_id_);
    ASSERT_EQ(slot_ctx_manager_.GetSlotVersion(slot_id_), 0);
}

TEST_F(SlotContextManagerTest, SlotLastLogSequence) {
    ASSERT_EQ(slot_ctx_manager_.GetSlotLastLogSequence(slot_id_), 0);
    slot_ctx_manager_.SetSlotLastLogSequence(slot_id_, 200);
    ASSERT_EQ(slot_ctx_manager_.GetSlotLastLogSequence(slot_id_), 200);
    slot_ctx_manager_.ResetSlotContext(slot_id_);
    ASSERT_EQ(slot_ctx_manager_.GetSlotLastLogSequence(slot_id_), 0);
}

TEST_F(SlotContextManagerTest, SlotFirstDirtyLogId) {
    ASSERT_EQ(slot_ctx_manager_.GetSlotFirstDirtyLogId(slot_id_), 0);
    slot_ctx_manager_.SetSlotFirstDirtyLogId(slot_id_, 200);
    ASSERT_EQ(slot_ctx_manager_.GetSlotFirstDirtyLogId(slot_id_), 200);
    slot_ctx_manager_.ResetSlotContext(slot_id_);
    ASSERT_EQ(slot_ctx_manager_.GetSlotFirstDirtyLogId(slot_id_), 0);
}

TEST_F(SlotContextManagerTest, LogStatistic) {
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 1), 0);

    storage::OpLog::LogItem item1;
    storage::OpLog::LogItem item2;
    storage::OpLog::LogItem item3;
    item1.set_key("item1");
    item2.set_key("item2");
    item2.set_page_log(true);
    item2.set_object_id(1);
    item3.set_key("item3");
    item3.set_meta_log(true);

    slot_ctx_manager_.AddSlotLog(slot_id_, 100, 100, 100, item1);
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 1), 0);

    slot_ctx_manager_.AddSlotLog(slot_id_, 200, 200, 100, item2);
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 1), 1);

    slot_ctx_manager_.AddSlotLog(slot_id_, 300, 300, 100, item3);
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 1);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 1), 1);

    slot_ctx_manager_.TrimSlotLogs(slot_id_, 100);
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 1);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 1), 1);

    slot_ctx_manager_.TrimSlotLogs(slot_id_, 200);
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 1);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 1), 0);

    slot_ctx_manager_.TrimSlotLogs(slot_id_, 300);
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 1), 0);
}

TEST_F(SlotContextManagerTest, AddDeleteLog) {
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 1), 0);

    storage::OpLog::LogItem item;
    item.set_object_deleted(true);

    slot_ctx_manager_.AddSlotLog(slot_id_, 100, 100, 100, item);
    ASSERT_EQ(slot_ctx_manager_.GetSlotDirtyMetaLogsNum(slot_id_), 1);
    ASSERT_EQ(slot_ctx_manager_.GetSlotObjectDirtyPagesNum(slot_id_, 0), 1);
}

TEST_F(SlotContextManagerTest, Logs) {
    storage::OpLog::LogItem item1;
    storage::OpLog::LogItem item2;
    storage::OpLog::LogItem item3;
    item1.set_key("item1");
    item2.set_key("item2");
    item3.set_key("item3");

    ASSERT_EQ(slot_ctx_manager_.GetSlotLogs(slot_id_).size(), 0);

    slot_ctx_manager_.AddSlotLog(slot_id_, 100, 100, 100, item1);
    slot_ctx_manager_.AddSlotLog(slot_id_, 200, 200, 100, item2);
    slot_ctx_manager_.AddSlotLog(slot_id_, 300, 300, 100, item3);
    ASSERT_EQ(slot_ctx_manager_.GetSlotLogs(slot_id_).size(), 3);

    slot_ctx_manager_.TrimSlotLogs(slot_id_, 200);
    ASSERT_EQ(slot_ctx_manager_.GetSlotLogs(slot_id_).size(), 1);
    ASSERT_EQ(slot_ctx_manager_.GetSlotLogs(slot_id_).front().log.key(), "item3");

    slot_ctx_manager_.ClearSlotLogs(slot_id_);
    ASSERT_EQ(slot_ctx_manager_.GetSlotLogs(slot_id_).size(), 0);
}

TEST_F(SlotContextManagerTest, Events) {
    ASSERT_EQ(slot_ctx_manager_.ExtractSlotEvents(slot_id_).size(), 0);
    Controller ctrl;
    slot_ctx_manager_.AddSlotEvent(slot_id_, nullptr, true, nullptr);
    slot_ctx_manager_.AddSlotEvent(slot_id_, nullptr, true, nullptr);
    slot_ctx_manager_.AddSlotEvent(slot_id_, &ctrl, false, nullptr);

    auto events = slot_ctx_manager_.ExtractSlotEvents(slot_id_);
    ASSERT_EQ(events.size(), 3);
    ASSERT_EQ(events[0].ctrl, &ctrl);

    slot_ctx_manager_.ClearSlotEvents(slot_id_);
    ASSERT_EQ(slot_ctx_manager_.ExtractSlotEvents(slot_id_).size(), 0);
}

TEST_F(SlotContextManagerTest, Reset) {
    slot_ctx_manager_.SetSlotVersion(slot_id_, 10000);
    slot_ctx_manager_.AddSlotLog(slot_id_, 100, 100, 100, storage::OpLog::LogItem());
    slot_ctx_manager_.AddSlotLog(slot_id_, 200, 100, 100, storage::OpLog::LogItem());
    slot_ctx_manager_.AddSlotLog(slot_id_, 300, 100, 100, storage::OpLog::LogItem());
    slot_ctx_manager_.AddSlotEvent(slot_id_, nullptr, false, nullptr);
    slot_ctx_manager_.AddSlotEvent(slot_id_, nullptr, false, nullptr);
    slot_ctx_manager_.AddSlotEvent(slot_id_, nullptr, false, nullptr);
    ASSERT_EQ(slot_ctx_manager_.GetSlotVersion(slot_id_), 10000);
    ASSERT_EQ(slot_ctx_manager_.GetSlotLogs(slot_id_).size(), 3);

    slot_ctx_manager_.ResetSlotContext(slot_id_);
    ASSERT_EQ(slot_ctx_manager_.slot_contexts_.count(slot_id_), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotVersion(slot_id_), 0);
    ASSERT_EQ(slot_ctx_manager_.GetSlotLogs(slot_id_).size(), 0);
}

TEST_F(SlotContextManagerTest, PointerStabilityOfLog) {
    ASSERT_EQ(slot_ctx_manager_.MutableLastLog(slot_id_), nullptr);

    for (int i = 0; i < 10000; ++i) {
        slot_ctx_manager_.AddSlotLog(slot_id_, i, i, i, storage::OpLog::LogItem());
    }

    slot_ctx_manager_.AddSlotLog(slot_id_, 111111111, 222222222, 333333333,
                                 storage::OpLog::LogItem());
    auto log = slot_ctx_manager_.MutableLastLog(slot_id_);
    ASSERT_NE(log, nullptr);
    ASSERT_EQ(log->log_id, 111111111);
    ASSERT_EQ(log->log_sequence, 222222222);
    ASSERT_EQ(log->log_size, 333333333);

    for (int i = 0; i < 10000; ++i) {
        slot_ctx_manager_.AddSlotLog(slot_id_, 111111111 + i, 222222222 + i, 333333333 + i,
                                     storage::OpLog::LogItem());
        ASSERT_EQ(log->log_id, 111111111);
        ASSERT_EQ(log->log_sequence, 222222222);
        ASSERT_EQ(log->log_size, 333333333);
    }
    for (int i = 0; i < 10000; ++i) {
        slot_ctx_manager_.TrimSlotLogs(slot_id_, i);
        ASSERT_EQ(log->log_id, 111111111);
        ASSERT_EQ(log->log_sequence, 222222222);
        ASSERT_EQ(log->log_size, 333333333);
    }
}

}  // namespace test
}  // namespace partition
}  // namespace bcache2
