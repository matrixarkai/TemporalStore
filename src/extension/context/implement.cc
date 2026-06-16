// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <algorithm>
#include <cstdint>
#include <functional>
#include <string>

#include "common/cmd_manager.h"
#include "common/macros.h"
#include "extension/context/interface.pb.h"
#include "model/feature_model.h"
#include "model/hash_model.h"
#include "partition/compute/execute_env.h"

namespace bcache2 {
namespace context {
namespace {

constexpr char kNodeField[] = "meta";
constexpr uint32_t kDefaultLimit = 100;
constexpr uint64_t kTimelineKeyFanout = 1024;

std::string JoinKey(const std::string& prefix, uint64_t tenant_hash, uint64_t suffix) {
    return prefix + ":" + std::to_string(tenant_hash) + ":" + std::to_string(suffix);
}

std::string EventKey(uint64_t tenant_hash, uint64_t node_hash) {
    return JoinKey("ctx:event", tenant_hash, node_hash);
}

std::string NodeKey(uint64_t tenant_hash, uint64_t node_hash) {
    return JoinKey("ctx:node", tenant_hash, node_hash);
}

std::string AuditKey(uint64_t tenant_hash, uint64_t session_hash) {
    return JoinKey("ctx:audit", tenant_hash, session_hash);
}

std::string DirtyKey(uint64_t tenant_hash, uint64_t node_hash) {
    return JoinKey("ctx:dirty", tenant_hash, node_hash);
}

std::string IndexKey(uint64_t tenant_hash, const std::string& index_name,
                     uint64_t index_value_hash, uint64_t scope_hash) {
    return "ctxidx:" + std::to_string(tenant_hash) + ":" + index_name + ":" +
           std::to_string(index_value_hash) + ":" + std::to_string(scope_hash);
}

uint32_t LimitOrDefault(uint32_t limit) {
    return limit == 0 ? kDefaultLimit : limit;
}

uint64_t TimelineKey(uint64_t timestamp_ms, uint64_t disambiguator) {
    return timestamp_ms * kTimelineKeyFanout + (disambiguator % kTimelineKeyFanout);
}

uint64_t TimelineStart(uint64_t timestamp_ms) {
    return timestamp_ms * kTimelineKeyFanout;
}

uint64_t TimelineEnd(uint64_t timestamp_ms) {
    return timestamp_ms * kTimelineKeyFanout;
}

bool Contains(const google::protobuf::RepeatedField<uint32_t>& values, uint32_t value) {
    return std::find(values.begin(), values.end(), value) != values.end();
}

bool MatchesEventFilter(const QueryEventsRequest& request, const ContextEvent& event) {
    if (request.kinds_size() > 0 && !Contains(request.kinds(), event.kind())) {
        return false;
    }
    if (request.statuses_size() > 0 && !Contains(request.statuses(), event.status())) {
        return false;
    }
    if (event.confidence() < request.min_confidence()) {
        return false;
    }
    if (event.importance() < request.min_importance()) {
        return false;
    }
    if (request.current_valid_only()) {
        const uint64_t as_of_ms = request.as_of_ms() == 0 ? request.end_time_ms() : request.as_of_ms();
        if (event.event_time_ms() > as_of_ms) {
            return false;
        }
        if (event.valid_until_ms() != 0 && event.valid_until_ms() <= as_of_ms) {
            return false;
        }
    }
    return true;
}

Status ValidateRange(uint64_t start_time_ms, uint64_t end_time_ms) {
    if (end_time_ms <= start_time_ms) {
        return Status::InvalidArgument("end_time_ms must be greater than start_time_ms");
    }
    return Status::OK();
}

}  // namespace

Status UpsertNode(ExecuteEnv* env, const UpsertNodeRequest& request, UpsertNodeResponse* response) {
    if (request.tenant_hash() == 0 || request.node().node_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and node_hash must be non-zero");
    }

    const std::string key = NodeKey(request.tenant_hash(), request.node().node_hash());
    ObjectHandle<model::HashModel> object;
    Status status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }

    std::string value;
    if (!request.node().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextNode");
    }
    status = object->OrSet().Set(nullptr, kNodeField, std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, UPSERT_NODE, UpsertNode, Write);

Status GetNode(ExecuteEnv* env, const GetNodeRequest& request, GetNodeResponse* response) {
    if (request.tenant_hash() == 0 || request.node_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and node_hash must be non-zero");
    }

    const std::string key = NodeKey(request.tenant_hash(), request.node_hash());
    response->set_object_key(key);
    ObjectHandle<model::HashModel> object;
    Status status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        response->set_exist(false);
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }

    std::string value;
    status = object->OrSet().Get(kNodeField, &value);
    if (status.IsNotFound()) {
        response->set_exist(false);
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    if (!response->mutable_node()->ParseFromString(value)) {
        return Status::InvalidArgument("stored ContextNode is corrupted");
    }
    response->set_exist(true);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, GET_NODE, GetNode, Read);

Status WriteEvent(ExecuteEnv* env, const WriteEventRequest& request, WriteEventResponse* response) {
    if (request.tenant_hash() == 0 || request.node_hash() == 0 ||
        request.event().event_time_ms() == 0 || request.event().event_id_hash() == 0) {
        return Status::InvalidArgument(
            "tenant_hash, node_hash, event_time_ms, and event_id_hash must be non-zero");
    }

    const std::string key = EventKey(request.tenant_hash(), request.node_hash());
    ObjectHandle<model::FeatureModel> object;
    Status status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    const uint64_t timeline_key =
        TimelineKey(request.event().event_time_ms(), request.event().event_id_hash());
    if (request.first_write_only() && object->OrSet().Get(timeline_key)) {
        response->set_object_key(key);
        return Status::OK();
    }

    std::string value;
    if (!request.event().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextEvent");
    }
    status = object->OrSet().Add(nullptr, timeline_key, std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, WRITE_EVENT, WriteEvent, Write);

Status QueryEvents(ExecuteEnv* env, const QueryEventsRequest& request,
                   QueryEventsResponse* response) {
    if (request.tenant_hash() == 0 || request.node_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and node_hash must be non-zero");
    }
    Status status = ValidateRange(request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }

    const std::string key = EventKey(request.tenant_hash(), request.node_hash());
    response->set_object_key(key);
    ObjectHandle<model::FeatureModel> object;
    status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }

    const uint32_t limit = LimitOrDefault(request.limit());
    object->OrSet().Query(
        TimelineStart(request.start_time_ms()), TimelineEnd(request.end_time_ms()), limit,
        [&request, response](const uint64_t, const std::string& value) {
            ContextEvent event;
            if (!event.ParseFromString(value)) {
                return;
            }
            if (MatchesEventFilter(request, event)) {
                *response->add_events() = std::move(event);
            }
        });
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_EVENTS, QueryEvents, Read);

Status WriteIndexRef(ExecuteEnv* env, const WriteIndexRefRequest& request,
                     WriteIndexRefResponse* response) {
    if (request.tenant_hash() == 0 || request.index_name().empty() ||
        request.index_value_hash() == 0 || request.event_time_ms() == 0 ||
        request.ref().primary_node_hash() == 0 || request.ref().primary_event_time_ms() == 0) {
        return Status::InvalidArgument("invalid context index ref request");
    }

    const std::string key =
        IndexKey(request.tenant_hash(), request.index_name(), request.index_value_hash(),
                 request.scope_hash());
    ObjectHandle<model::FeatureModel> object;
    Status status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }

    std::string value;
    if (!request.ref().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize IndexRef");
    }
    status = object->OrSet().Add(
        nullptr, TimelineKey(request.event_time_ms(), request.ref().event_id_hash()),
        std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, WRITE_INDEX_REF, WriteIndexRef, Write);

Status QueryIndex(ExecuteEnv* env, const QueryIndexRequest& request, QueryIndexResponse* response) {
    if (request.tenant_hash() == 0 || request.index_name().empty() ||
        request.index_value_hash() == 0) {
        return Status::InvalidArgument("tenant_hash, index_name, and index_value_hash are required");
    }
    Status status = ValidateRange(request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }

    const std::string key =
        IndexKey(request.tenant_hash(), request.index_name(), request.index_value_hash(),
                 request.scope_hash());
    response->set_object_key(key);
    ObjectHandle<model::FeatureModel> object;
    status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }

    object->OrSet().Query(TimelineStart(request.start_time_ms()),
                          TimelineEnd(request.end_time_ms()), LimitOrDefault(request.limit()),
                          [response](const uint64_t, const std::string& value) {
                              IndexRef ref;
                              if (ref.ParseFromString(value)) {
                                  *response->add_refs() = std::move(ref);
                              }
                          });
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_INDEX, QueryIndex, Read);

Status WritePackAudit(ExecuteEnv* env, const WritePackAuditRequest& request,
                      WritePackAuditResponse* response) {
    if (request.tenant_hash() == 0 || request.audit().session_hash() == 0 ||
        request.audit().request_time_ms() == 0 || request.audit().query_id().empty()) {
        return Status::InvalidArgument(
            "tenant_hash, session_hash, request_time_ms, and query_id are required");
    }

    const std::string key = AuditKey(request.tenant_hash(), request.audit().session_hash());
    ObjectHandle<model::FeatureModel> object;
    Status status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }

    std::string value;
    if (!request.audit().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextPackAudit");
    }
    const uint64_t query_hash = std::hash<std::string>{}(request.audit().query_id());
    status = object->OrSet().Add(nullptr,
                                 TimelineKey(request.audit().request_time_ms(), query_hash),
                                 std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, WRITE_PACK_AUDIT, WritePackAudit, Write);

Status QueryPackAudit(ExecuteEnv* env, const QueryPackAuditRequest& request,
                      QueryPackAuditResponse* response) {
    if (request.tenant_hash() == 0 || request.session_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and session_hash are required");
    }
    Status status = ValidateRange(request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }

    const std::string key = AuditKey(request.tenant_hash(), request.session_hash());
    response->set_object_key(key);
    ObjectHandle<model::FeatureModel> object;
    status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }

    object->OrSet().Query(TimelineStart(request.start_time_ms()),
                          TimelineEnd(request.end_time_ms()), LimitOrDefault(request.limit()),
                          [response](const uint64_t, const std::string& value) {
                              ContextPackAudit audit;
                              if (audit.ParseFromString(value)) {
                                  *response->add_audits() = std::move(audit);
                              }
                          });
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_PACK_AUDIT, QueryPackAudit, Read);

Status MarkSummaryDirty(ExecuteEnv* env, const MarkSummaryDirtyRequest& request,
                        MarkSummaryDirtyResponse* response) {
    if (request.tenant_hash() == 0 || request.marker().node_hash() == 0 ||
        request.marker().event_time_ms() == 0) {
        return Status::InvalidArgument("tenant_hash, node_hash, and event_time_ms are required");
    }

    const std::string key = DirtyKey(request.tenant_hash(), request.marker().node_hash());
    ObjectHandle<model::FeatureModel> object;
    Status status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }

    std::string value;
    if (!request.marker().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize SummaryDirtyMarker");
    }
    status = object->OrSet().Add(
        nullptr, TimelineKey(request.marker().event_time_ms(), request.marker().node_hash()),
        std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, MARK_SUMMARY_DIRTY, MarkSummaryDirty, Write);

Status QuerySummaryDirty(ExecuteEnv* env, const QuerySummaryDirtyRequest& request,
                         QuerySummaryDirtyResponse* response) {
    if (request.tenant_hash() == 0 || request.node_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and node_hash are required");
    }
    Status status = ValidateRange(request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }

    const std::string key = DirtyKey(request.tenant_hash(), request.node_hash());
    response->set_object_key(key);
    ObjectHandle<model::FeatureModel> object;
    status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }

    object->OrSet().Query(TimelineStart(request.start_time_ms()),
                          TimelineEnd(request.end_time_ms()), LimitOrDefault(request.limit()),
                          [response](const uint64_t, const std::string& value) {
                              SummaryDirtyMarker marker;
                              if (marker.ParseFromString(value)) {
                                  *response->add_markers() = std::move(marker);
                              }
                          });
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_SUMMARY_DIRTY, QuerySummaryDirty, Read);

}  // namespace context
}  // namespace bcache2
