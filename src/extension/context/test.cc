// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/light_client.h"
#include "extension/context/interface.pb.h"
#include "extension/modules.pb.h"
#include "test/smoketest/base_smoketest.h"

DECLARE_bool(storage_enable_index_gc);
DECLARE_bool(start_storage_manager_when_loading);
DECLARE_bool(stream_aggregate_flush);

namespace bcache2 {
namespace swig {
namespace {

uint32_t MakeCmdId(int module_id, int function_id) {
    return (static_cast<uint32_t>(module_id) << 16) | static_cast<uint32_t>(function_id);
}

class ContextModuleTest : public ::testing::Test {
 public:
    void SetUp() override {
        old_storage_enable_index_gc_ = FLAGS_storage_enable_index_gc;
        old_start_storage_manager_when_loading_ = FLAGS_start_storage_manager_when_loading;
        old_stream_aggregate_flush_ = FLAGS_stream_aggregate_flush;
        FLAGS_storage_enable_index_gc = false;
        FLAGS_start_storage_manager_when_loading = false;
        FLAGS_stream_aggregate_flush = false;

        MiniCluster::Options options;
        options.cluster_name = "context_module_test";
        options.server_count = 1;
        options.proxy_count = 0;
        options.work_dir = temp_dir_.GetDir();
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        ::bcache2::Status cluster_status = cluster_.Start();
        ASSERT_TRUE(cluster_status.ok()) << cluster_status.ToString();

        cluster_.GetMaster()->CreateSimpleTable("ns", "table");

        Client* raw_client = nullptr;
        Status status = Client::Create(ClientOptions(), &raw_client);
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

    void TearDown() override {
        cluster_.Stop();
        FLAGS_stream_aggregate_flush = old_stream_aggregate_flush_;
        FLAGS_start_storage_manager_when_loading = old_start_storage_manager_when_loading_;
        FLAGS_storage_enable_index_gc = old_storage_enable_index_gc_;
    }

 protected:
    template <typename Request, typename Response>
    void Execute(uint32_t function_id, const std::string& route_key, const Request& request,
                 Response* response) {
        Controller ctrl;
        light_table_->Execute(&ctrl, MakeCmdId(CONTEXT, function_id), route_key, request, response);
        ASSERT_EQ(ctrl.status.code(), 0) << ctrl.status.message();
    }

    template <typename Request, typename Response>
    void ExecuteInvalid(uint32_t function_id, const std::string& route_key, const Request& request,
                        Response* response) {
        Controller ctrl;
        light_table_->Execute(&ctrl, MakeCmdId(CONTEXT, function_id), route_key, request, response);
        ASSERT_NE(ctrl.status.code(), 0);
    }

    TempDir temp_dir_{"/tmp/context_module_test_"};
    MiniCluster cluster_;
    std::unique_ptr<Client> client_;
    std::unique_ptr<Table> table_;
    std::unique_ptr<LightTable> light_table_;
    bool old_storage_enable_index_gc_ = true;
    bool old_start_storage_manager_when_loading_ = true;
    bool old_stream_aggregate_flush_ = true;
};

}  // namespace

TEST_F(ContextModuleTest, NodeEventIndexDirtyAndAuditRoundTrip) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8891;
    constexpr uint64_t kParent = 3001;
    constexpr uint64_t kEventTime = 1781500000000ULL;
    constexpr uint64_t kEventId = 991;

    {
        context::UpsertNodeRequest request;
        request.set_tenant_hash(kTenant);
        auto* node = request.mutable_node();
        node->set_node_hash(kNode);
        node->set_parent_hash(kParent);
        node->set_kind(3);
        node->set_canonical_name("gpu_purchase_request_8891");
        node->set_l0("GPU purchase request for Project 1.");
        node->set_last_event_time_ms(kEventTime);

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
        event->set_type(2);
        event->set_confidence(0.94);
        event->set_importance(0.86);
        event->set_text("GPU purchase for Project 1 was approved for $42k.");

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
        request.add_types(2);
        request.set_min_confidence(0.9);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-event", request, &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.events(0).event_id_hash(), kEventId);
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
    }

    {
        context::WriteIndexRefRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name("status");
        request.set_index_value_hash(1);
        request.set_scope_hash(kNode);
        request.set_event_time_ms(kEventTime + 10);
        request.mutable_ref()->set_primary_node_hash(kNode);
        request.mutable_ref()->set_primary_event_time_ms(kEventTime + 10);
        request.mutable_ref()->set_event_id_hash(kEventId + 10);

        context::WriteIndexRefResponse response;
        Execute(context::WRITE_INDEX_REF, "ctx-index", request, &response);
    }

    {
        context::QueryIndexRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name("status");
        request.set_index_value_hash(1);
        request.set_scope_hash(kParent);
        request.set_start_time_ms(kEventTime - 1);
        request.set_end_time_ms(kEventTime + 20);
        request.set_limit(10);

        context::QueryIndexResponse response;
        Execute(context::QUERY_INDEX, "ctx-index", request, &response);
        ASSERT_EQ(response.refs_size(), 1);
        ASSERT_EQ(response.refs(0).event_id_hash(), kEventId);
    }

    {
        context::UpsertEntityRequest request;
        request.set_tenant_hash(kTenant);
        auto* entity = request.mutable_entity();
        entity->set_entity_hash(7001);
        entity->set_node_hash(kNode);
        entity->set_type(1);
        entity->set_name("gpu_purchase_request");
        entity->set_value("approved");
        entity->set_updated_at_ms(kEventTime);
        entity->set_valid_from_ms(kEventTime);
        entity->set_confidence(0.97);
        entity->add_source_event_hashes(kEventId);

        context::UpsertEntityResponse response;
        Execute(context::UPSERT_ENTITY, "ctx-entity", request, &response);
        ASSERT_EQ(response.object_key(), "ctx:entity:1001:8891:7001");
    }

    {
        context::GetEntityRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_entity_hash(7001);

        context::GetEntityResponse response;
        Execute(context::GET_ENTITY, "ctx-entity", request, &response);
        ASSERT_TRUE(response.exist());
        ASSERT_EQ(response.entity().name(), "gpu_purchase_request");
        ASSERT_EQ(response.entity().source_event_hashes(0), kEventId);
    }

    {
        context::QueryEntitiesRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.add_entity_hashes(7001);
        request.set_limit(10);

        context::QueryEntitiesResponse response;
        Execute(context::QUERY_ENTITIES, "ctx-entity", request, &response);
        ASSERT_EQ(response.entities_size(), 1);
        ASSERT_EQ(response.entities(0).entity_hash(), 7001);
    }

    {
        context::MarkSummaryDirtyRequest request;
        request.set_tenant_hash(kTenant);
        request.mutable_marker()->set_node_hash(kNode);
        request.mutable_marker()->set_event_time_ms(kEventTime);
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
        audit->set_max_prompt_tokens(3000);
        audit->set_selected_tokens(920);
        auto* selected = audit->add_selected_refs();
        selected->set_node_hash(kNode);
        selected->set_event_time_ms(kEventTime);

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
        ASSERT_EQ(response.audits(0).selected_refs(0).node_hash(), kNode);
    }
}

TEST_F(ContextModuleTest, RejectsInvalidProductionInputs) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8891;
    constexpr uint64_t kEventTime = 1781500000000ULL;

    {
        context::UpsertNodeRequest request;
        request.set_tenant_hash(kTenant);
        request.mutable_node()->set_node_hash(kNode);
        request.mutable_node()->set_canonical_name(std::string(600, 'x'));

        context::UpsertNodeResponse response;
        ExecuteInvalid(context::UPSERT_NODE, "ctx-node", request, &response);
    }

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(991);
        event->set_confidence(0.9);
        event->set_importance(0.8);

        context::WriteEventResponse response;
        ExecuteInvalid(context::WRITE_EVENT, "ctx-event", request, &response);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kEventTime - 1);
        request.set_end_time_ms(kEventTime + 1);
        request.set_limit(1001);

        context::QueryEventsResponse response;
        ExecuteInvalid(context::QUERY_EVENTS, "ctx-event", request, &response);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kEventTime - 1);
        request.set_end_time_ms(kEventTime + 1);
        request.set_min_confidence(1.1);

        context::QueryEventsResponse response;
        ExecuteInvalid(context::QUERY_EVENTS, "ctx-event", request, &response);
    }

    {
        context::WriteIndexRefRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name("bad:index");
        request.set_index_value_hash(1);
        request.set_event_time_ms(kEventTime);
        request.mutable_ref()->set_primary_node_hash(kNode);
        request.mutable_ref()->set_primary_event_time_ms(kEventTime);
        request.mutable_ref()->set_event_id_hash(991);

        context::WriteIndexRefResponse response;
        ExecuteInvalid(context::WRITE_INDEX_REF, "ctx-index", request, &response);
    }

    {
        context::MarkSummaryDirtyRequest request;
        request.set_tenant_hash(kTenant);
        request.mutable_marker()->set_node_hash(kNode);
        request.mutable_marker()->set_event_time_ms(kEventTime);
        request.mutable_marker()->set_propagate_depth(9);

        context::MarkSummaryDirtyResponse response;
        ExecuteInvalid(context::MARK_SUMMARY_DIRTY, "ctx-dirty", request, &response);
    }

    {
        context::WritePackAuditRequest request;
        request.set_tenant_hash(kTenant);
        auto* audit = request.mutable_audit();
        audit->set_query_id("q_bad");
        audit->set_session_hash(555);
        audit->set_request_time_ms(kEventTime);
        audit->add_selected_refs()->set_event_time_ms(kEventTime);

        context::WritePackAuditResponse response;
        ExecuteInvalid(context::WRITE_PACK_AUDIT, "ctx-audit", request, &response);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kEventTime - 1);
        request.set_end_time_ms(kEventTime + 1);
        request.set_rank_by_decayed_score(true);

        context::QueryEventsResponse response;
        ExecuteInvalid(context::QUERY_EVENTS, "ctx-event", request, &response);
    }
}

TEST_F(ContextModuleTest, QueryEventsUsesInclusiveTimeBucketsAndResultLimit) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8891;
    constexpr uint64_t kEventTime = 1781500000000ULL;

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(100);
        event->set_event_time_ms(kEventTime);
        event->set_type(4);
        event->set_confidence(0.95);
        event->set_importance(0.75);
        event->set_text("Non-matching event at the query start.");

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-event", request, &response);
    }

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(200);
        event->set_event_time_ms(kEventTime + 1);
        event->set_type(3);
        event->set_confidence(0.96);
        event->set_importance(0.9);
        event->set_text("Matching event at the inclusive query end.");

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-event", request, &response);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kEventTime);
        request.set_end_time_ms(kEventTime + 1);
        request.set_limit(1);
        request.add_types(3);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-event", request, &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.events(0).event_id_hash(), 200);
        ASSERT_EQ(response.events(0).event_time_ms(), kEventTime + 1);
    }
}

TEST_F(ContextModuleTest, EventIngestionTimeIsPrimaryTimelineKey) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8893;
    constexpr uint64_t kSourceTime = 1700000000000ULL;
    constexpr uint64_t kIngestionTime = 1781500000000ULL;
    constexpr uint64_t kEventId = 333;

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_event_time_ms(kSourceTime);
        event->set_ingestion_time_ms(kIngestionTime);
        event->set_type(4);
        event->set_confidence(0.95);
        event->set_importance(0.85);
        event->set_text("Late-arriving approval extracted from an old source document.");

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-ingestion-primary-event", request, &response);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kSourceTime - 1);
        request.set_end_time_ms(kSourceTime + 1);
        request.set_limit(10);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-source-time-window", request, &response);
        ASSERT_EQ(response.events_size(), 0);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kIngestionTime - 1);
        request.set_end_time_ms(kIngestionTime + 1);
        request.set_limit(10);
        request.set_current_valid_only(true);
        request.set_as_of_ms(kIngestionTime);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-ingestion-time-window", request, &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.events(0).event_id_hash(), kEventId);
        ASSERT_EQ(response.events(0).event_time_ms(), kSourceTime);
        ASSERT_EQ(response.events(0).ingestion_time_ms(), kIngestionTime);
    }

    {
        context::WriteIndexRefRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name("source_time");
        request.set_index_value_hash(42);
        request.set_scope_hash(kNode);
        request.set_event_time_ms(kSourceTime);
        request.mutable_ref()->set_primary_node_hash(kNode);
        request.mutable_ref()->set_primary_event_time_ms(kIngestionTime);
        request.mutable_ref()->set_event_id_hash(kEventId);

        context::WriteIndexRefResponse response;
        Execute(context::WRITE_INDEX_REF, "ctx-source-time-index", request, &response);
    }

    {
        context::QueryIndexRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name("source_time");
        request.set_index_value_hash(42);
        request.set_scope_hash(kNode);
        request.set_start_time_ms(kSourceTime - 1);
        request.set_end_time_ms(kSourceTime + 1);
        request.set_limit(10);

        context::QueryIndexResponse response;
        Execute(context::QUERY_INDEX, "ctx-source-time-index-query", request, &response);
        ASSERT_EQ(response.refs_size(), 1);
        ASSERT_EQ(response.refs(0).primary_event_time_ms(), kIngestionTime);
    }
}

TEST_F(ContextModuleTest, WriteExtractedEventCreatesDefaultInternalIndexes) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8894;
    constexpr uint64_t kScope = 3001;
    constexpr uint64_t kEventTime = 1700000000000ULL;
    constexpr uint64_t kIngestionTime = 1781500000000ULL;
    constexpr uint64_t kBucket = 1699920000000ULL;
    constexpr uint64_t kEventId = 444;
    constexpr uint64_t kGpuEntity = 501;
    constexpr uint64_t kProjectEntity = 502;
    constexpr uint64_t kApprovedStatus = 601;
    constexpr uint64_t kCursorSource = 701;

    {
        context::WriteExtractedEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_first_write_only(true);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_event_time_ms(kEventTime);
        event->set_ingestion_time_ms(kIngestionTime);
        event->set_type(7);
        event->set_confidence(0.96);
        event->set_importance(0.88);
        event->set_text("Finance confirmed the Project 1 GPU purchase approval.");
        auto* indexes = request.mutable_indexes();
        indexes->set_scope_hash(kScope);
        indexes->add_entity_hashes(kGpuEntity);
        indexes->add_entity_hashes(kProjectEntity);
        indexes->set_status_hash(kApprovedStatus);
        indexes->set_source_hash(kCursorSource);
        indexes->set_event_time_bucket_ms(kBucket);

        context::WriteExtractedEventResponse response;
        Execute(context::WRITE_EXTRACTED_EVENT, "ctx-extracted-event", request, &response);
        ASSERT_EQ(response.event_object_key(), "ctx:event:1001:8894");
        ASSERT_EQ(response.written_index_count(), 6);
        ASSERT_EQ(response.index_object_keys_size(), 6);
    }

    auto query_index = [&](const std::string& index_name, uint64_t value_hash,
                           uint64_t start_time_ms, uint64_t end_time_ms) {
        context::QueryIndexRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name(index_name);
        request.set_index_value_hash(value_hash);
        request.set_scope_hash(kScope);
        request.set_start_time_ms(start_time_ms);
        request.set_end_time_ms(end_time_ms);
        request.set_limit(10);

        context::QueryIndexResponse response;
        Execute(context::QUERY_INDEX, "ctx-extracted-index-query", request, &response);
        ASSERT_EQ(response.refs_size(), 1);
        ASSERT_EQ(response.refs(0).primary_node_hash(), kNode);
        ASSERT_EQ(response.refs(0).primary_event_time_ms(), kIngestionTime);
        ASSERT_EQ(response.refs(0).event_id_hash(), kEventId);
    };

    query_index("event_kind", 7, kIngestionTime - 1, kIngestionTime + 1);
    query_index("entity", kGpuEntity, kIngestionTime - 1, kIngestionTime + 1);
    query_index("entity", kProjectEntity, kIngestionTime - 1, kIngestionTime + 1);
    query_index("status", kApprovedStatus, kIngestionTime - 1, kIngestionTime + 1);
    query_index("source", kCursorSource, kIngestionTime - 1, kIngestionTime + 1);
    query_index("event_time_bucket", kBucket, kBucket - 1, kBucket + 1);
}

TEST_F(ContextModuleTest, WriteExtractedEventCanDisableDefaultIndexes) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8895;
    constexpr uint64_t kScope = 3001;
    constexpr uint64_t kIngestionTime = 1781500000000ULL;
    constexpr uint64_t kEventId = 445;
    constexpr uint64_t kCursorSource = 701;

    {
        context::WriteExtractedEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_ingestion_time_ms(kIngestionTime);
        event->set_type(8);
        event->set_confidence(0.9);
        event->set_importance(0.8);
        event->set_text("A low-noise event that should not be source-indexed.");
        auto* indexes = request.mutable_indexes();
        indexes->set_scope_hash(kScope);
        indexes->set_source_hash(kCursorSource);
        indexes->add_disabled_indexes(context::INTERNAL_INDEX_SOURCE);

        context::WriteExtractedEventResponse response;
        Execute(context::WRITE_EXTRACTED_EVENT, "ctx-extracted-event-disabled", request,
                &response);
        ASSERT_EQ(response.written_index_count(), 1);
    }

    {
        context::QueryIndexRequest request;
        request.set_tenant_hash(kTenant);
        request.set_index_name("source");
        request.set_index_value_hash(kCursorSource);
        request.set_scope_hash(kScope);
        request.set_start_time_ms(kIngestionTime - 1);
        request.set_end_time_ms(kIngestionTime + 1);
        request.set_limit(10);

        context::QueryIndexResponse response;
        Execute(context::QUERY_INDEX, "ctx-disabled-source-query", request, &response);
        ASSERT_EQ(response.refs_size(), 0);
    }
}

TEST_F(ContextModuleTest, RetrieveContextPackUsesCompactIndexPostings) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8896;
    constexpr uint64_t kScope = 3001;
    constexpr uint64_t kIngestionTime = 1781500000000ULL;
    constexpr uint64_t kEventId = 446;
    constexpr uint64_t kApprovedStatus = 601;

    {
        context::WriteExtractedEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_ingestion_time_ms(kIngestionTime);
        event->set_type(7);
        event->set_confidence(0.96);
        event->set_importance(0.88);
        event->set_text("Finance approved the Project Aurora GPU procurement request.");
        auto* indexes = request.mutable_indexes();
        indexes->set_scope_hash(kScope);
        indexes->set_status_hash(kApprovedStatus);

        context::WriteExtractedEventResponse response;
        Execute(context::WRITE_EXTRACTED_EVENT, "ctx-native-pack-index-write", request,
                &response);
        ASSERT_GE(response.written_index_count(), 1);
    }

    context::RetrieveContextPackRequest request;
    request.set_tenant_hash(kTenant);
    request.set_start_node_hash(kNode);
    request.set_scope_hash(kScope);
    request.set_start_time_ms(kIngestionTime - 1);
    request.set_end_time_ms(kIngestionTime + 1);
    request.set_as_of_ms(kIngestionTime + 1);
    request.set_max_context_tokens(64);
    request.set_max_selected_refs(4);
    request.set_min_score(0.1);
    auto* filter = request.add_index_filters();
    filter->set_index_name("status");
    filter->set_index_value_hash(kApprovedStatus);
    filter->set_start_time_ms(kIngestionTime - 1);
    filter->set_end_time_ms(kIngestionTime + 1);
    filter->set_limit(10);

    context::RetrieveContextPackResponse response;
    Execute(context::RETRIEVE_CONTEXT_PACK, "ctx-native-pack-index", request, &response);
    ASSERT_EQ(response.selected_refs_size(), 1);
    ASSERT_EQ(response.selected_refs(0).ref_type(), "event");
    ASSERT_EQ(response.selected_refs(0).ref_hash(), kEventId);
    ASSERT_EQ(response.selected_refs(0).node_hash(), kNode);
    ASSERT_EQ(response.selected_refs(0).matched_index_names_size(), 1);
    ASSERT_EQ(response.selected_refs(0).matched_index_names(0), "status");
    ASSERT_EQ(response.telemetry().selected_refs(), 1);
    ASSERT_EQ(response.telemetry().index_postings_read(), 1);
    ASSERT_EQ(response.telemetry().candidate_fetch_count(), 1);
    ASSERT_FALSE(response.telemetry().broad_scan_used());
    ASSERT_TRUE(response.telemetry().broad_scan_blocked());
    ASSERT_GT(response.telemetry().placement_partitions_touched(), 0);
}

TEST_F(ContextModuleTest, RetrieveContextPackReportsPlacementDrops) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8897;
    constexpr uint64_t kOtherNode = 8898;
    constexpr uint64_t kScope = 3001;
    constexpr uint64_t kIngestionTime = 1781500000000ULL;
    constexpr uint64_t kEventId = 447;
    constexpr uint64_t kApprovedStatus = 601;

    {
        context::WriteExtractedEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kOtherNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_ingestion_time_ms(kIngestionTime);
        event->set_type(7);
        event->set_confidence(0.96);
        event->set_importance(0.88);
        event->set_text("This approval belongs to another placement node.");
        auto* indexes = request.mutable_indexes();
        indexes->set_scope_hash(kScope);
        indexes->set_status_hash(kApprovedStatus);

        context::WriteExtractedEventResponse response;
        Execute(context::WRITE_EXTRACTED_EVENT, "ctx-native-pack-placement-write", request,
                &response);
    }

    context::RetrieveContextPackRequest request;
    request.set_tenant_hash(kTenant);
    request.set_start_node_hash(kNode);
    request.set_scope_hash(kScope);
    request.set_start_time_ms(kIngestionTime - 1);
    request.set_end_time_ms(kIngestionTime + 1);
    request.set_max_context_tokens(64);
    request.set_max_selected_refs(4);
    auto* filter = request.add_index_filters();
    filter->set_index_name("status");
    filter->set_index_value_hash(kApprovedStatus);
    filter->set_start_time_ms(kIngestionTime - 1);
    filter->set_end_time_ms(kIngestionTime + 1);
    filter->set_limit(10);

    context::RetrieveContextPackResponse response;
    Execute(context::RETRIEVE_CONTEXT_PACK, "ctx-native-pack-placement", request, &response);
    ASSERT_EQ(response.selected_refs_size(), 0);
    ASSERT_EQ(response.telemetry().dropped_by_placement(), 1);
    ASSERT_EQ(response.telemetry().selected_refs(), 0);
    ASSERT_FALSE(response.telemetry().broad_scan_used());
    ASSERT_EQ(response.warnings_size(), 1);
}

TEST_F(ContextModuleTest, RetrieveContextPackTraversesChildrenWithoutQueryVector) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kParentNode = 8899;
    constexpr uint64_t kChildNode = 8900;
    constexpr uint64_t kIngestionTime = 1781500000100ULL;
    constexpr uint64_t kEventId = 448;

    {
        context::UpsertChildRefRequest request;
        request.set_tenant_hash(kTenant);
        auto* ref = request.mutable_ref();
        ref->set_parent_hash(kParentNode);
        ref->set_child_hash(kChildNode);
        ref->set_updated_at_ms(kIngestionTime);
        context::UpsertChildRefResponse response;
        Execute(context::UPSERT_CHILD_REF, "ctx-native-pack-child-ref", request, &response);
    }

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kChildNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_ingestion_time_ms(kIngestionTime);
        event->set_type(7);
        event->set_confidence(0.9);
        event->set_importance(0.8);
        event->set_text("Child conversation node contains the retrieval evidence.");
        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-native-pack-child-event", request, &response);
    }

    context::RetrieveContextPackRequest request;
    request.set_tenant_hash(kTenant);
    request.set_start_node_hash(kParentNode);
    request.set_start_time_ms(kIngestionTime - 1);
    request.set_end_time_ms(kIngestionTime + 1);
    request.set_max_depth(2);
    request.set_max_candidate_nodes(8);
    request.set_max_context_tokens(64);
    request.set_max_selected_refs(4);

    context::RetrieveContextPackResponse response;
    Execute(context::RETRIEVE_CONTEXT_PACK, "ctx-native-pack-child", request, &response);
    ASSERT_EQ(response.selected_refs_size(), 1);
    ASSERT_EQ(response.selected_refs(0).ref_hash(), kEventId);
    ASSERT_EQ(response.selected_refs(0).node_hash(), kChildNode);
    ASSERT_GE(response.telemetry().placement_partitions_touched(), 2);
    ASSERT_FALSE(response.telemetry().broad_scan_used());
}

TEST_F(ContextModuleTest, FirstWriteOnlyKeepsOriginalEventPayload) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8891;
    constexpr uint64_t kEventTime = 1781500000000ULL;
    constexpr uint64_t kEventId = 991;

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_first_write_only(true);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_event_time_ms(kEventTime);
        event->set_type(3);
        event->set_confidence(0.95);
        event->set_importance(0.8);
        event->set_text("original payload");

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-event", request, &response);
    }

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_first_write_only(true);
        auto* event = request.mutable_event();
        event->set_event_id_hash(kEventId);
        event->set_event_time_ms(kEventTime);
        event->set_type(3);
        event->set_confidence(0.95);
        event->set_importance(0.8);
        event->set_text("replacement payload");

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-event", request, &response);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kEventTime - 1);
        request.set_end_time_ms(kEventTime + 1);
        request.set_limit(10);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-event", request, &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.events(0).text(), "original payload");
    }
}

TEST_F(ContextModuleTest, CurrentValidOnlyDropsFutureEvents) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8891;
    constexpr uint64_t kAsOf = 1781500000100ULL;

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(2);
        event->set_event_time_ms(kAsOf + 50);
        event->set_type(3);
        event->set_confidence(0.95);
        event->set_importance(0.8);
        event->set_text("future event");

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-event", request, &response);
    }

    {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(3);
        event->set_event_time_ms(kAsOf - 10);
        event->set_type(3);
        event->set_confidence(0.95);
        event->set_importance(0.8);
        event->set_text("current event");

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-event", request, &response);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kAsOf - 100);
        request.set_end_time_ms(kAsOf + 100);
        request.set_limit(10);
        request.set_current_valid_only(true);
        request.set_as_of_ms(kAsOf);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-event", request, &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.events(0).text(), "current event");
    }
}

TEST_F(ContextModuleTest, QueryEventsAppliesAgeDecayForRankingAndFiltering) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kNode = 8892;
    constexpr uint64_t kAsOf = 1781500000000ULL;
    constexpr uint64_t kHalfLife = 1000ULL;

    auto write_event = [&](uint64_t event_id, uint64_t event_time_ms, float importance,
                           const std::string& text) {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(event_id);
        event->set_event_time_ms(event_time_ms);
        event->set_type(3);
        event->set_confidence(1.0);
        event->set_importance(importance);
        event->set_text(text);

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-decay-event", request, &response);
    };

    write_event(1, kAsOf - 4000, 1.0, "old high importance event");
    write_event(2, kAsOf - 100, 0.6, "fresh medium importance event");

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kAsOf - 5000);
        request.set_end_time_ms(kAsOf);
        request.set_limit(10);
        request.set_as_of_ms(kAsOf);
        request.set_decay_half_life_ms(kHalfLife);
        request.set_rank_by_decayed_score(true);
        request.set_min_decayed_score(0.05);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-decay-query", request, &response);
        ASSERT_EQ(response.events_size(), 2);
        ASSERT_EQ(response.decayed_scores_size(), 2);
        ASSERT_EQ(response.events(0).event_id_hash(), 2);
        ASSERT_EQ(response.events(1).event_id_hash(), 1);
        ASSERT_GT(response.decayed_scores(0), response.decayed_scores(1));
        ASSERT_NEAR(response.decayed_scores(1), 0.0625, 0.0001);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kAsOf - 5000);
        request.set_end_time_ms(kAsOf);
        request.set_limit(10);
        request.set_as_of_ms(kAsOf);
        request.set_decay_half_life_ms(kHalfLife);
        request.set_min_decayed_score(0.1);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-decay-filter", request, &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.decayed_scores_size(), 1);
        ASSERT_EQ(response.events(0).event_id_hash(), 2);
    }
}

TEST_F(ContextModuleTest, TreeEmbeddingSummaryAndCompressionRoundTrip) {
    constexpr uint64_t kTenant = 1001;
    constexpr uint64_t kRoot = 10;
    constexpr uint64_t kGpuNode = 20;
    constexpr uint64_t kCostNode = 30;
    constexpr uint64_t kEventTime = 1781500000000ULL;

    {
        context::UpsertNodeRequest request;
        request.set_tenant_hash(kTenant);
        auto* node = request.mutable_node();
        node->set_node_hash(kRoot);
        node->set_kind(1);
        node->set_canonical_name("company_a");
        node->set_l0("Company A context root.");

        context::UpsertNodeResponse response;
        Execute(context::UPSERT_NODE, "ctx-node", request, &response);
    }

    {
        context::UpsertNodeRequest request;
        request.set_tenant_hash(kTenant);
        auto* node = request.mutable_node();
        node->set_node_hash(kGpuNode);
        node->set_parent_hash(kRoot);
        node->set_kind(2);
        node->set_canonical_name("gpu_purchase");
        node->set_l0("GPU purchase leaf node.");

        context::UpsertNodeResponse response;
        Execute(context::UPSERT_NODE, "ctx-node", request, &response);
    }

    auto write_child = [&](uint64_t child_hash, bool expect_created,
                           uint32_t expect_parent_child_count) {
        context::UpsertChildRefRequest request;
        request.set_tenant_hash(kTenant);
        auto* ref = request.mutable_ref();
        ref->set_parent_hash(kRoot);
        ref->set_child_hash(child_hash);
        ref->set_updated_at_ms(kEventTime);

        context::UpsertChildRefResponse response;
        Execute(context::UPSERT_CHILD_REF, "ctx-child", request, &response);
        ASSERT_EQ(response.object_key(), "ctx:child:1001:10");
        ASSERT_EQ(response.created(), expect_created);
        ASSERT_EQ(response.parent_child_count(), expect_parent_child_count);
    };
    write_child(kGpuNode, true, 1);
    write_child(kCostNode, true, 2);
    write_child(kGpuNode, false, 2);

    {
        context::QueryChildrenRequest request;
        request.set_tenant_hash(kTenant);
        request.set_parent_hash(kRoot);
        request.set_limit(10);

        context::QueryChildrenResponse response;
        Execute(context::QUERY_CHILDREN, "ctx-child", request, &response);
        ASSERT_EQ(response.refs_size(), 2);
        ASSERT_EQ(response.refs(0).child_hash(), kGpuNode);
    }

    {
        context::GetNodeRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kRoot);

        context::GetNodeResponse response;
        Execute(context::GET_NODE, "ctx-node", request, &response);
        ASSERT_TRUE(response.exist());
    }

    auto write_embedding = [&](uint64_t ref_hash, float first, float second) {
        context::UpsertEmbeddingRequest request;
        request.set_tenant_hash(kTenant);
        auto* embedding = request.mutable_embedding();
        embedding->set_ref_hash(ref_hash);
        embedding->set_level(1);
        embedding->add_vector(first);
        embedding->add_vector(second);
        embedding->set_updated_at_ms(kEventTime);

        context::UpsertEmbeddingResponse response;
        Execute(context::UPSERT_EMBEDDING, "ctx-embedding", request, &response);
    };
    write_embedding(kGpuNode, 1.0f, 0.0f);
    write_embedding(kCostNode, 0.0f, 1.0f);

    {
        context::TraverseContextTreeRequest request;
        request.set_tenant_hash(kTenant);
        request.set_start_node_hash(kRoot);
        request.add_query_vector(1.0f);
        request.add_query_vector(0.0f);
        request.set_max_depth(2);
        request.set_top_k_per_depth(1);
        request.set_max_children_scored_per_parent(10);
        request.set_max_candidate_nodes(4);
        request.set_leaf_only(true);

        context::TraverseContextTreeResponse response;
        Execute(context::TRAVERSE_CONTEXT_TREE, "ctx-traverse", request, &response);
        ASSERT_EQ(response.nodes_size(), 1);
        ASSERT_EQ(response.nodes(0).node_hash(), kGpuNode);
        ASSERT_GT(response.nodes(0).score(), 0.99f);
    }

    {
        context::UpsertSummaryRequest request;
        request.set_tenant_hash(kTenant);
        auto* summary = request.mutable_summary();
        summary->set_node_hash(kGpuNode);
        summary->set_level(1);
        summary->set_text("L0 GPU purchase summary.");
        summary->set_valid_from_ms(kEventTime);

        context::UpsertSummaryResponse response;
        Execute(context::UPSERT_SUMMARY, "ctx-summary", request, &response);
    }

    {
        context::UpsertSummaryRequest request;
        request.set_tenant_hash(kTenant);
        auto* summary = request.mutable_summary();
        summary->set_node_hash(kGpuNode);
        summary->set_level(1);
        summary->set_text("Latest overall GPU purchase summary.");
        summary->set_valid_from_ms(kEventTime + 5);

        context::UpsertSummaryResponse response;
        Execute(context::UPSERT_SUMMARY, "ctx-summary", request, &response);
    }

    {
        context::QuerySummariesRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kGpuNode);
        request.set_level(1);
        request.set_as_of_ms(kEventTime + 1);
        request.set_limit(10);

        context::QuerySummariesResponse response;
        Execute(context::QUERY_SUMMARIES, "ctx-summary", request, &response);
        ASSERT_EQ(response.summaries_size(), 1);
        ASSERT_EQ(response.summaries(0).text(), "L0 GPU purchase summary.");
    }

    {
        context::WriteCompressionEventRequest request;
        request.set_tenant_hash(kTenant);
        auto* event = request.mutable_event();
        event->set_compression_id_hash(5001);
        event->set_node_hash(kGpuNode);
        event->set_source_start_ms(kEventTime - 1000);
        event->set_source_end_ms(kEventTime);
        event->set_compressed_time_ms(kEventTime);
        event->set_summary("Older GPU purchase timeline compressed into one summary.");

        context::WriteCompressionEventResponse response;
        Execute(context::WRITE_COMPRESSION_EVENT, "ctx-compress", request, &response);
    }

    {
        context::QueryCompressionEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.add_node_hashes(kGpuNode);
        request.set_start_time_ms(kEventTime - 2000);
        request.set_end_time_ms(kEventTime + 1);
        request.set_limit(10);

        context::QueryCompressionEventsResponse response;
        Execute(context::QUERY_COMPRESSION_EVENTS, "ctx-compress", request, &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.events(0).node_hash(), kGpuNode);
    }

    {
        context::QueryNodeContextRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kGpuNode);
        request.set_summary_level(1);
        request.set_as_of_ms(kEventTime + 10);
        request.set_cold_start_time_ms(kEventTime - 2000);
        request.set_cold_end_time_ms(kEventTime + 1);
        request.set_compression_limit(10);

        context::QueryNodeContextResponse response;
        Execute(context::QUERY_NODE_CONTEXT, "ctx-node-context", request, &response);
        ASSERT_TRUE(response.node_exists());
        ASSERT_TRUE(response.overall_summary_exists());
        ASSERT_EQ(response.overall_summary().text(), "Latest overall GPU purchase summary.");
        ASSERT_EQ(response.cold_window_summaries_size(), 1);
        ASSERT_EQ(response.cold_window_summaries(0).summary(),
                  "Older GPU purchase timeline compressed into one summary.");
    }
}

TEST_F(ContextModuleTest, TemporalCompressionBuildsReplayableSummaryWithoutDeletingSources) {
    constexpr uint64_t kTenant = 3003;
    constexpr uint64_t kNode = 9100;
    constexpr uint64_t kStart = 1781400000000ULL;
    constexpr uint64_t kCompressedAt = 1781500000000ULL;

    auto write_event = [&](uint64_t offset_ms, uint64_t event_id, const std::string& text) {
        context::WriteEventRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        auto* event = request.mutable_event();
        event->set_event_id_hash(event_id);
        event->set_event_time_ms(kStart + offset_ms);
        event->set_type(7);
        event->set_confidence(0.96);
        event->set_importance(0.82);
        event->set_text(text);

        context::WriteEventResponse response;
        Execute(context::WRITE_EVENT, "ctx-compress-source", request, &response);
    };
    write_event(0, 7001, "Week-old approval was created.");
    write_event(10, 7002, "Week-old approval was reviewed by finance.");
    write_event(20, 7003, "Week-old approval was confirmed by infra.");

    {
        context::CompressEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_source_start_ms(kStart);
        request.set_source_end_ms(kStart + 20);
        request.set_compressed_time_ms(kCompressedAt);
        request.set_max_source_events(2);
        request.set_min_confidence(0.9);
        request.set_min_importance(0.8);

        context::CompressEventsResponse response;
        Execute(context::COMPRESS_EVENTS, "ctx-compress-operator", request, &response);
        ASSERT_EQ(response.object_key(), "ctx:compress:3003:9100");
        ASSERT_EQ(response.source_event_count(), 2);
        ASSERT_TRUE(response.truncated_source_events());
        ASSERT_EQ(response.event().node_hash(), kNode);
        ASSERT_EQ(response.event().source_start_ms(), kStart);
        ASSERT_EQ(response.event().source_end_ms(), kStart + 20);
        ASSERT_EQ(response.event().compressed_time_ms(), kCompressedAt);
        ASSERT_NE(response.event().summary().find("Temporal compression window"),
                  std::string::npos);
        ASSERT_NE(response.event().summary().find("Week-old approval was created"),
                  std::string::npos);
    }

    {
        context::QueryCompressionEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.add_node_hashes(kNode);
        request.set_start_time_ms(kStart);
        request.set_end_time_ms(kStart + 20);
        request.set_limit(10);

        context::QueryCompressionEventsResponse response;
        Execute(context::QUERY_COMPRESSION_EVENTS, "ctx-compress-query-source-window", request,
                &response);
        ASSERT_EQ(response.events_size(), 1);
        ASSERT_EQ(response.events(0).compressed_time_ms(), kCompressedAt);
        ASSERT_NE(response.events(0).summary().find("additional source events"),
                  std::string::npos);
    }

    {
        context::QueryEventsRequest request;
        request.set_tenant_hash(kTenant);
        request.set_node_hash(kNode);
        request.set_start_time_ms(kStart);
        request.set_end_time_ms(kStart + 20);
        request.set_limit(10);

        context::QueryEventsResponse response;
        Execute(context::QUERY_EVENTS, "ctx-compress-raw-events", request, &response);
        ASSERT_EQ(response.events_size(), 3);
    }
}

TEST_F(ContextModuleTest, TraverseContextTreeUsesGlobalTopKWithinEachLayer) {
    constexpr uint64_t kTenant = 2002;
    constexpr uint64_t kRoot = 100;
    constexpr uint64_t kParentA = 110;
    constexpr uint64_t kParentB = 120;
    constexpr uint64_t kLeafA1 = 111;
    constexpr uint64_t kLeafA2 = 112;
    constexpr uint64_t kLeafB1 = 121;
    constexpr uint64_t kLeafB2 = 122;
    constexpr uint64_t kEventTime = 1781500000000ULL;

    auto write_node = [&](uint64_t node_hash, uint64_t parent_hash) {
        context::UpsertNodeRequest request;
        request.set_tenant_hash(kTenant);
        auto* node = request.mutable_node();
        node->set_node_hash(node_hash);
        node->set_parent_hash(parent_hash);
        node->set_kind(3);
        node->set_canonical_name("ctx_node_" + std::to_string(node_hash));

        context::UpsertNodeResponse response;
        Execute(context::UPSERT_NODE, "ctx-node-global-topk", request, &response);
    };

    auto write_child = [&](uint64_t parent_hash, uint64_t child_hash) {
        context::UpsertChildRefRequest request;
        request.set_tenant_hash(kTenant);
        auto* ref = request.mutable_ref();
        ref->set_parent_hash(parent_hash);
        ref->set_child_hash(child_hash);
        ref->set_updated_at_ms(kEventTime + child_hash);

        context::UpsertChildRefResponse response;
        Execute(context::UPSERT_CHILD_REF, "ctx-child-global-topk", request, &response);
    };

    auto write_embedding = [&](uint64_t ref_hash, float first, float second) {
        context::UpsertEmbeddingRequest request;
        request.set_tenant_hash(kTenant);
        auto* embedding = request.mutable_embedding();
        embedding->set_ref_hash(ref_hash);
        embedding->set_level(1);
        embedding->add_vector(first);
        embedding->add_vector(second);
        embedding->set_updated_at_ms(kEventTime);

        context::UpsertEmbeddingResponse response;
        Execute(context::UPSERT_EMBEDDING, "ctx-embedding-global-topk", request, &response);
    };

    write_node(kRoot, 0);
    write_node(kParentA, kRoot);
    write_node(kParentB, kRoot);
    write_node(kLeafA1, kParentA);
    write_node(kLeafA2, kParentA);
    write_node(kLeafB1, kParentB);
    write_node(kLeafB2, kParentB);

    write_child(kRoot, kParentA);
    write_child(kRoot, kParentB);
    write_child(kParentA, kLeafA1);
    write_child(kParentA, kLeafA2);
    write_child(kParentB, kLeafB1);
    write_child(kParentB, kLeafB2);

    write_embedding(kParentA, 0.80f, 0.20f);
    write_embedding(kParentB, 0.90f, 0.10f);
    write_embedding(kLeafA1, 0.80f, 0.20f);
    write_embedding(kLeafA2, 0.70f, 0.30f);
    write_embedding(kLeafB1, 0.99f, 0.01f);
    write_embedding(kLeafB2, 0.98f, 0.02f);

    context::TraverseContextTreeRequest request;
    request.set_tenant_hash(kTenant);
    request.set_start_node_hash(kRoot);
    request.add_query_vector(1.0f);
    request.add_query_vector(0.0f);
    request.set_max_depth(2);
    request.set_top_k_per_depth(2);
    request.set_max_children_scored_per_parent(10);
    request.set_max_candidate_nodes(2);
    request.set_leaf_only(true);

    context::TraverseContextTreeResponse response;
    Execute(context::TRAVERSE_CONTEXT_TREE, "ctx-traverse-global-topk", request, &response);
    ASSERT_EQ(response.nodes_size(), 2);
    ASSERT_EQ(response.nodes(0).node_hash(), kLeafB1);
    ASSERT_EQ(response.nodes(1).node_hash(), kLeafB2);
    ASSERT_GT(response.nodes(0).score(), response.nodes(1).score());
}

TEST_F(ContextModuleTest, RejectsInvalidEmbeddingAndTraversalInputs) {
    {
        context::UpsertEmbeddingRequest request;
        request.set_tenant_hash(1001);
        auto* embedding = request.mutable_embedding();
        embedding->set_ref_hash(20);
        embedding->set_level(1);
        embedding->set_updated_at_ms(1781500000000ULL);

        context::UpsertEmbeddingResponse response;
        ExecuteInvalid(context::UPSERT_EMBEDDING, "ctx-embedding", request, &response);
    }

    {
        context::TraverseContextTreeRequest request;
        request.set_tenant_hash(1001);
        request.set_start_node_hash(10);

        context::TraverseContextTreeResponse response;
        ExecuteInvalid(context::TRAVERSE_CONTEXT_TREE, "ctx-traverse", request, &response);
    }
}

}  // namespace swig
}  // namespace bcache2
