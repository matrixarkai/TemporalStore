// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/light_client.h"
#include "extension/context/interface.pb.h"
#include "extension/modules.pb.h"
#include "test/smoketest/base_smoketest.h"

namespace bcache2 {
namespace swig {
namespace {

uint32_t MakeCmdId(int module_id, int function_id) {
    return (static_cast<uint32_t>(module_id) << 16) | static_cast<uint32_t>(function_id);
}

class ContextModuleTest : public ::testing::Test {
 public:
    void SetUp() override {
        MiniCluster::Options options;
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        Status status = cluster_.Start();
        ASSERT_TRUE(status.ok()) << status.message();

        cluster_.GetMaster()->CreateSimpleTable("ns", "table");

        Client* raw_client = nullptr;
        status = Client::Create(ClientOptions(), &raw_client);
        ASSERT_TRUE(status.ok()) << status.message();
        client_.reset(raw_client);

        Table* raw_table = nullptr;
        status = client_->OpenTable(
            "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) +
                "/ns/table",
            TableOptions(), &raw_table);
        ASSERT_TRUE(status.ok()) << status.message();
        table_.reset(raw_table);
        light_table_.reset(new LightTable(raw_table));
    }

    void TearDown() override { cluster_.Stop(); }

 protected:
    template <typename Request, typename Response>
    void Execute(uint32_t function_id, const std::string& route_key, const Request& request,
                 Response* response) {
        Controller ctrl;
        light_table_->Execute(&ctrl, MakeCmdId(CONTEXT, function_id), route_key, request, response);
        ASSERT_EQ(ctrl.status.code(), 0) << ctrl.status.ToString();
    }

    TempDir temp_dir_;
    MiniCluster cluster_;
    std::unique_ptr<Client> client_;
    std::unique_ptr<Table> table_;
    std::unique_ptr<LightTable> light_table_;
};

}  // namespace

TEST_F(ContextModuleTest, NodeEventIndexDirtyAndAuditRoundTrip) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8891;
    constexpr uint64_t kParent = 3001;
    constexpr uint64_t kEventTime = 1781500000000ULL;
    constexpr uint64_t kEventId = 991;
    constexpr uint64_t kActor = 701;

    {
        context::UpsertNodeRequest request;
        request.set_tenant_hash(kTenant);
        auto* node = request.mutable_node();
        node->set_node_hash(kNode);
        node->set_parent_hash(kParent);
        node->set_kind(3);
        node->set_canonical_name("gpu_purchase_request_8891");
        node->set_l0("GPU purchase request for Project 1.");
        node->set_status(1);
        node->set_last_event_time_ms(kEventTime);
        node->set_summary_dirty(false);

        context::UpsertNodeResponse response;
        Execute(context::UPSERT_NODE, "ctx-node", request, &response);
        ASSERT_EQ(response.object_key(), "ctx:node:1001:8891");
    }

    {
        context::GetNodeRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);

        context::GetNodeResponse response;
        Execute(context::GET_NODE, "ctx-node", request, &response);
        ASSERT_TRUE(response.exist());
        ASSERT_EQ(response.node().canonical_name(), "gpu_purchase_request_8891");
        ASSERT_EQ(response.node().parent_hash(), kParent);
    }

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_first_write_only(true);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_event_time_ms(kEventTime);
        event->set_kind(3);
        event->set_type(2);
        event->set_actor_hash(kActor);
        event->set_status(1);
        event->set_valid_until_ms(kEventTime + 100000);
        event->set_confidence(0.94);
        event->set_importance(0.86);
        event->set_text("GPU purchase for Project 1 was approved for $42k.");
        event->set_source_ref("cursor://thread/123/turn/45");
        event->add_related_node_hashes(7771);

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-event", request, &response);
        ASSERT_EQ(response.object_key(), "ctx:event:1001:8891");
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kEventTime - 1);
        request.set_end_time_ms(kEventTime + 1);
        request.set_limit(10);
        request.set_current_valid_only(true);
        request.set_as_of_ms(kEventTime + 1);
        request.add_kinds(3);
        request.add_statuses(1);
        request.set_min_confidence(0.9);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-event", request, &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.events(0).event_id_hash(), kEventId);
        ASSERT_EQ(response.events(0).related_node_hashes(0), 7771);
    }

    {
        context::WriteIndexRefRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name("status");
        request.set_index_value_hash(1);
        request.set_scope_hash(kParent);
        request.set_event_time_ms(kEventTime);
        request.mutable_ref()->set_primary_node_hash(kNode);
        request.mutable_ref()->set_primary_event_time_ms(kEventTime);
        request.mutable_ref()->set_event_id_hash(kEventId);

        context::WriteIndexRefResponse response;
        Execute(context::WRITE_INDEX_REF, "ctx-index", request, &response);
        ASSERT_EQ(response.object_key(), "ctxidx:1001:status:1:3001");
    }

    {
        context::QueryIndexRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name("status");
        request.set_index_value_hash(1);
        request.set_scope_hash(kParent);
        request.set_start_time_ms(kEventTime - 1);
        request.set_end_time_ms(kEventTime + 1);
        request.set_limit(10);

        context::QueryIndexResponse response;
        Execute(context::QUERY_INDEX, "ctx-index", request, &response);
        ASSERT_EQ(response.refs_size(), 1);
        ASSERT_EQ(response.refs(0).primary_node_hash(), kNode);
    }

    {
        context::MarkSummaryDirtyRequest request;
        request.set_tenant_hash(kTenant);
        request.mutable_marker()->set_node_hash(kNode);
        request.mutable_marker()->set_event_time_ms(kEventTime);
        request.mutable_marker()->set_reason(1);
        request.mutable_marker()->set_propagate_depth(1);

        context::MarkSummaryDirtyResponse response;
        Execute(context::MARK_SUMMARY_DIRTY, "ctx-dirty", request, &response);
        ASSERT_EQ(response.object_key(), "ctx:dirty:1001:8891");
    }

    {
        context::QuerySummaryDirtyRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kEventTime - 1);
        request.set_end_time_ms(kEventTime + 1);
        request.set_limit(10);

        context::QuerySummaryDirtyResponse response;
        Execute(context::QUERY_SUMMARY_DIRTY, "ctx-dirty", request, &response);
        ASSERT_EQ(response.markers_size(), 1);
        ASSERT_EQ(response.markers(0).propagate_depth(), 1);
    }

    {
        context::WritePackAuditRequest request;
        request.set_tenant_hash(kTenant);
        auto* audit = request.mutable_audit();
        audit->set_query_id("q_123");
        audit->set_session_hash(555);
        audit->set_request_time_ms(kEventTime + 50);
        audit->set_query_hash(99901);
        audit->set_max_prompt_tokens(3000);
        audit->set_selected_tokens(920);
        auto* selected = audit->add_selected_refs();
        selected->set_node_hash(kNode);
        selected->set_event_time_ms(kEventTime);
        selected->set_reason("valid_current");

        context::WritePackAuditResponse response;
        Execute(context::WRITE_PACK_AUDIT, "ctx-audit", request, &response);
        ASSERT_EQ(response.object_key(), "ctx:audit:1001:555");
    }

    {
        context::QueryPackAuditRequest request;
        request.set_tenant_hash(kTenant);
        request.set_session_hash(555);
        request.set_start_time_ms(kEventTime);
        request.set_end_time_ms(kEventTime + 100);
        request.set_limit(10);

        context::QueryPackAuditResponse response;
        Execute(context::QUERY_PACK_AUDIT, "ctx-audit", request, &response);
        ASSERT_EQ(response.audits_size(), 1);
        ASSERT_EQ(response.audits(0).query_id(), "q_123");
        ASSERT_EQ(response.audits(0).selected_refs(0).reason(), "valid_current");
    }
}

}  // namespace swig
}  // namespace bcache2
