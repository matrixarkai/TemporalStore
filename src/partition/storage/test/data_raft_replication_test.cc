#include "partition/storage/data_raft_replication.h"
#include "partition/storage/data_raft_consensus.h"

#include <gtest/gtest.h>

namespace bcache2 {
namespace partition {
namespace {

TEST(DataRaftReplicationTest, SerializeParseRoundTrip) {
    DataRaftLogEntry entry;
    entry.partition_id = 17;
    entry.raft_index = 42;
    entry.log_id = 1001;
    entry.log_size = 128;
    entry.oplog.set_version(1);
    entry.oplog.set_sequence(9);
    auto* item = entry.oplog.add_item();
    item->set_slot_id(123);
    item->set_object_key("user:7");
    item->set_model_id(3);
    item->set_key("clicks");
    item->set_value("1");
    item->set_timestamp_ms(1700000000000);

    std::string encoded;
    ASSERT_TRUE(SerializeDataRaftLog(entry, &encoded).ok());
    ASSERT_FALSE(encoded.empty());

    DataRaftLogEntry parsed;
    ASSERT_TRUE(ParseDataRaftLog(encoded, &parsed).ok());
    EXPECT_EQ(parsed.partition_id, entry.partition_id);
    EXPECT_EQ(parsed.raft_index, entry.raft_index);
    EXPECT_EQ(parsed.log_id, entry.log_id);
    EXPECT_EQ(parsed.log_size, entry.log_size);
    EXPECT_EQ(parsed.oplog.sequence(), entry.oplog.sequence());
    EXPECT_EQ(parsed.oplog.item_size(), 1);
    EXPECT_EQ(parsed.oplog.item(0).slot_id(), 123);
    EXPECT_EQ(parsed.oplog.item(0).object_key(), "user:7");
    EXPECT_EQ(parsed.oplog.item(0).key(), "clicks");
    EXPECT_EQ(parsed.oplog.item(0).value(), "1");
}

TEST(DataRaftReplicationTest, RejectsCorruptPayload) {
    DataRaftLogEntry parsed;
    EXPECT_FALSE(ParseDataRaftLog("bad", &parsed).ok());

    DataRaftLogEntry entry;
    entry.partition_id = 1;
    entry.raft_index = 1;
    entry.log_id = 1;
    entry.oplog.set_sequence(1);

    std::string encoded;
    ASSERT_TRUE(SerializeDataRaftLog(entry, &encoded).ok());
    encoded[0] = '\0';
    EXPECT_FALSE(ParseDataRaftLog(encoded, &parsed).ok());
}

TEST(DataRaftReplicationTest, SerializeParseCommandRoundTrip) {
    DataRaftCommandEntry entry;
    entry.partition_id = 33;
    entry.raft_index = 44;
    entry.request_id = 55;
    entry.request.set_partition_id(entry.partition_id);
    entry.request.set_load_version(7);
    entry.request.set_pin_primary(true);
    auto* request = entry.request.add_request();
    request->set_module_id(1);
    request->set_function_id(2);
    request->set_request_bytes("payload");

    std::string encoded;
    ASSERT_TRUE(SerializeDataRaftCommand(entry, &encoded).ok());
    ASSERT_FALSE(encoded.empty());

    DataRaftCommandEntry parsed;
    ASSERT_TRUE(ParseDataRaftCommand(encoded, &parsed).ok());
    EXPECT_EQ(parsed.partition_id, entry.partition_id);
    EXPECT_EQ(parsed.raft_index, entry.raft_index);
    EXPECT_EQ(parsed.request_id, entry.request_id);
    EXPECT_EQ(parsed.request.partition_id(), entry.request.partition_id());
    EXPECT_EQ(parsed.request.load_version(), entry.request.load_version());
    EXPECT_EQ(parsed.request.pin_primary(), entry.request.pin_primary());
    ASSERT_EQ(parsed.request.request_size(), 1);
    EXPECT_EQ(parsed.request.request(0).module_id(), 1);
    EXPECT_EQ(parsed.request.request(0).function_id(), 2);
    EXPECT_EQ(parsed.request.request(0).request_bytes(), "payload");
}

TEST(DataRaftReplicationTest, RejectsInvalidCommandPayload) {
    DataRaftCommandEntry parsed;
    EXPECT_FALSE(ParseDataRaftCommand("bad", &parsed).ok());

    DataRaftCommandEntry entry;
    entry.partition_id = 1;
    entry.request.set_partition_id(2);
    entry.request.add_request();

    std::string encoded;
    EXPECT_FALSE(SerializeDataRaftCommand(entry, &encoded).ok());

    entry.request.set_partition_id(1);
    ASSERT_TRUE(SerializeDataRaftCommand(entry, &encoded).ok());
    encoded[0] = '\0';
    EXPECT_FALSE(ParseDataRaftCommand(encoded, &parsed).ok());
}

TEST(DataRaftReplicationTest, UnavailableConsensusFailsClosedForSafetyOperations) {
    DataRaftConsensusOptions options;
    options.partition_id = 11;
    options.replica_id = 11;
    options.group_id = 11;
    auto backend = NewUnavailableDataRaftConsensusBackend(options);

    uint64_t index = 0;
    EXPECT_FALSE(backend->Start().ok());
    EXPECT_FALSE(backend->Propose("x", &index).ok());
    EXPECT_FALSE(backend->WaitForAppliedIndex(1, 1).ok());
    EXPECT_FALSE(backend->TriggerSnapshot(&index).ok());
    EXPECT_FALSE(backend->ReadIndex(1).ok());

    DataRaftPeer peer;
    peer.replica_id = 12;
    peer.raft_addr = "127.0.0.1:17012";
    peer.snapshot_addr = "127.0.0.1:18012";
    EXPECT_FALSE(backend->AddPeer(peer).ok());
    EXPECT_FALSE(backend->AddLearner(peer).ok());
    EXPECT_FALSE(backend->PromotePeer(peer.replica_id).ok());
    EXPECT_FALSE(backend->RemovePeer(peer.replica_id).ok());
    EXPECT_FALSE(backend->TransferLeader(peer.replica_id).ok());
    EXPECT_FALSE(backend->CanServeBoundedStaleRead(0).ok());
}

}  // namespace
}  // namespace partition
}  // namespace bcache2

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
