// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/cmd_manager.h"
#include "common/macros.h"
#include "extension/context/interface.pb.h"
#include "model/context_model.h"
#include "partition/compute/execute_env.h"

namespace bcache2 {
namespace context {
namespace {

constexpr char kNodeField[] = "meta";
constexpr char kEmbeddingField[] = "embedding";
constexpr char kEntityField[] = "entity";
constexpr uint32_t kDefaultLimit = 100;
constexpr uint32_t kMaxLimit = 1000;
constexpr uint32_t kMaxTraversalDepth = 16;
constexpr uint32_t kDefaultTopKPerDepth = 5;
constexpr uint32_t kDefaultTraversalCandidates = 24;
constexpr uint32_t kMaxChildrenScoredPerParent = 256;
constexpr uint32_t kMaxFilterValues = 32;
constexpr uint32_t kMaxIndexBucketsPerFilter = 64;
constexpr size_t kMaxNativeCandidateCacheEntries = 256;
constexpr uint32_t kMaxAuditRefs = 512;
constexpr uint32_t kMaxPropagateDepth = 8;
constexpr uint32_t kMaxEmbeddingDim = 4096;
constexpr uint64_t kMaxDecayHalfLifeMs = 365ULL * 24 * 60 * 60 * 1000;
constexpr size_t kMaxIndexNameBytes = 64;
constexpr size_t kMaxCanonicalNameBytes = 512;
constexpr size_t kMaxEntityNameBytes = 512;
constexpr size_t kMaxEntityValueBytes = 16 * 1024;
constexpr size_t kMaxL0Bytes = 2048;
constexpr size_t kMaxSummaryBytes = 16 * 1024;
constexpr size_t kMaxCompressionSnippetBytes = 256;
constexpr size_t kMaxQueryIdBytes = 256;
constexpr size_t kMaxEventTextBytes = 64 * 1024;
constexpr uint64_t kTimelineKeyFanout = 1024 * 1024;
constexpr uint64_t kDefaultIndexPostingBucketMs = 60 * 1000;

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

std::string ChildKey(uint64_t tenant_hash, uint64_t parent_hash) {
    return JoinKey("ctx:child", tenant_hash, parent_hash);
}

std::string EmbeddingKey(uint64_t tenant_hash, uint64_t ref_hash) {
    return JoinKey("ctx:emb", tenant_hash, ref_hash);
}

std::string EntityKey(uint64_t tenant_hash, uint64_t node_hash, uint64_t entity_hash) {
    return "ctx:entity:" + std::to_string(tenant_hash) + ":" + std::to_string(node_hash) +
           ":" + std::to_string(entity_hash);
}

std::string SummaryKey(uint64_t tenant_hash, uint64_t node_hash, uint32_t level) {
    return "ctx:summary:" + std::to_string(tenant_hash) + ":" + std::to_string(node_hash) +
           ":" + std::to_string(level);
}

std::string CompressionKey(uint64_t tenant_hash, uint64_t node_hash) {
    return JoinKey("ctx:compress", tenant_hash, node_hash);
}

std::string IndexKey(uint64_t tenant_hash, const std::string& index_name,
                     uint64_t index_value_hash, uint64_t scope_hash) {
    return "ctxidx:" + std::to_string(tenant_hash) + ":" + index_name + ":" +
           std::to_string(index_value_hash) + ":" + std::to_string(scope_hash);
}

std::string CompactIndexKey(uint64_t tenant_hash, const std::string& index_name,
                            uint64_t scope_hash, uint64_t time_bucket_ms) {
    return "ctxidx2:" + std::to_string(tenant_hash) + ":" +
           std::to_string(scope_hash) + ":" + index_name + ":" +
           std::to_string(time_bucket_ms);
}

std::string IndexStorageKey(uint64_t tenant_hash, const std::string& index_name,
                            uint64_t index_value_hash, uint64_t scope_hash,
                            uint64_t time_bucket_ms) {
    if (time_bucket_ms != 0) {
        return CompactIndexKey(tenant_hash, index_name, scope_hash, time_bucket_ms);
    }
    return IndexKey(tenant_hash, index_name, index_value_hash, scope_hash);
}

uint64_t IndexTimeBucket(uint64_t timestamp_ms) {
    if (timestamp_ms == 0) {
        return 0;
    }
    return timestamp_ms - (timestamp_ms % kDefaultIndexPostingBucketMs);
}

std::vector<uint64_t> LatestIndexBucketsForRange(uint64_t start_time_ms,
                                                 uint64_t end_time_ms) {
    std::vector<uint64_t> buckets;
    if (start_time_ms == 0 || end_time_ms == 0 || start_time_ms > end_time_ms) {
        return buckets;
    }
    const uint64_t start_bucket = IndexTimeBucket(start_time_ms);
    uint64_t bucket = IndexTimeBucket(end_time_ms);
    for (uint32_t i = 0; i < kMaxIndexBucketsPerFilter && bucket >= start_bucket; ++i) {
        buckets.push_back(bucket);
        if (bucket < kDefaultIndexPostingBucketMs) {
            break;
        }
        bucket -= kDefaultIndexPostingBucketMs;
    }
    return buckets;
}

uint32_t LimitOrDefault(uint32_t limit) {
    return limit == 0 ? kDefaultLimit : limit;
}

Status ValidateLimit(uint32_t limit) {
    if (limit > kMaxLimit) {
        return Status::InvalidArgument("limit exceeds maximum");
    }
    return Status::OK();
}

Status ValidateByteSize(const std::string& name, const std::string& value, size_t max_size) {
    if (value.size() > max_size) {
        return Status::InvalidArgument(name + " is too large");
    }
    return Status::OK();
}

Status ValidateScore(const std::string& name, float value) {
    if (!std::isfinite(value) || value < 0.0 || value > 1.0) {
        return Status::InvalidArgument(name + " must be in [0, 1]");
    }
    return Status::OK();
}

Status ValidateIndexName(const std::string& index_name) {
    if (index_name.empty()) {
        return Status::InvalidArgument("index_name is required");
    }
    if (index_name.size() > kMaxIndexNameBytes) {
        return Status::InvalidArgument("index_name is too large");
    }
    for (unsigned char c : index_name) {
        if (!std::isalnum(c) && c != '_' && c != '-' && c != '.') {
            return Status::InvalidArgument("index_name contains invalid characters");
        }
    }
    return Status::OK();
}

Status ValidateTimelineTimestamp(uint64_t timestamp_ms) {
    if (timestamp_ms > std::numeric_limits<uint64_t>::max() / kTimelineKeyFanout) {
        return Status::InvalidArgument("timestamp_ms is too large");
    }
    return Status::OK();
}

uint64_t TimelineKey(uint64_t timestamp_ms, uint64_t disambiguator) {
    return timestamp_ms * kTimelineKeyFanout + (disambiguator % kTimelineKeyFanout);
}

uint64_t TimelineStart(uint64_t timestamp_ms) {
    return timestamp_ms * kTimelineKeyFanout;
}

uint64_t TimelineEnd(uint64_t timestamp_ms) {
    return timestamp_ms * kTimelineKeyFanout + kTimelineKeyFanout;
}

uint64_t EventPrimaryTime(const ContextEvent& event) {
    return event.ingestion_time_ms() != 0 ? event.ingestion_time_ms() : event.event_time_ms();
}

void NormalizeEventPrimaryTime(ContextEvent* event) {
    if (event->ingestion_time_ms() == 0) {
        event->set_ingestion_time_ms(event->event_time_ms());
    }
}

bool Contains(const google::protobuf::RepeatedField<uint32_t>& values, uint32_t value) {
    return std::find(values.begin(), values.end(), value) != values.end();
}

bool ContainsIndex(const google::protobuf::RepeatedField<int>& values,
                   InternalContextIndex value) {
    return std::find(values.begin(), values.end(), static_cast<int>(value)) != values.end();
}

const char* InternalIndexName(InternalContextIndex index) {
    switch (index) {
        case INTERNAL_INDEX_EVENT_KIND:
            return "event_kind";
        case INTERNAL_INDEX_ENTITY:
            return "entity";
        case INTERNAL_INDEX_STATUS:
            return "status";
        case INTERNAL_INDEX_SOURCE:
            return "source";
        case INTERNAL_INDEX_EVENT_TIME_BUCKET:
            return "event_time_bucket";
        default:
            return "";
    }
}

bool MatchesEventFilter(const QueryEventsRequest& request, const ContextEvent& event) {
    if (request.types_size() > 0 && !Contains(request.types(), event.type())) {
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
        if (EventPrimaryTime(event) > as_of_ms) {
            return false;
        }
    }
    return true;
}

bool DecayEnabled(const QueryEventsRequest& request) {
    return request.decay_half_life_ms() != 0 || request.min_decayed_score() > 0.0f ||
           request.rank_by_decayed_score();
}

float DecayedEventScore(const QueryEventsRequest& request, const ContextEvent& event) {
    const float base = event.confidence() * event.importance();
    if (request.decay_half_life_ms() == 0) {
        return base;
    }
    const uint64_t as_of_ms = request.as_of_ms() == 0 ? request.end_time_ms() : request.as_of_ms();
    const uint64_t primary_time_ms = EventPrimaryTime(event);
    const uint64_t age_ms = primary_time_ms >= as_of_ms ? 0 : as_of_ms - primary_time_ms;
    const double half_life = static_cast<double>(request.decay_half_life_ms());
    const double decay = std::pow(0.5, static_cast<double>(age_ms) / half_life);
    return static_cast<float>(static_cast<double>(base) * decay);
}

struct ScoredContextEvent {
    ContextEvent event;
    float decayed_score = 0.0f;
};

struct NativeContextCandidate {
    ContextEvent event;
    std::string ref_type;
    uint64_t ref_hash = 0;
    uint64_t node_hash = 0;
    uint64_t event_time_ms = 0;
    uint32_t token_estimate = 0;
    std::string text;
    float base_score = 0.0f;
    float score = 0.0f;
    std::set<std::string> matched_index_names;
};

std::mutex& CandidateCacheMutex() {
    static std::mutex mutex;
    return mutex;
}

std::unordered_map<std::string, std::vector<NativeContextCandidate>>& CandidateCache() {
    static std::unordered_map<std::string, std::vector<NativeContextCandidate>> cache;
    return cache;
}

std::string CandidateCacheScope(const RetrieveContextPackRequest& request) {
    if (!request.scope_key().empty()) {
        return request.scope_key();
    }
    if (!request.placement_key().empty()) {
        return request.placement_key();
    }
    if (request.scope_hash() != 0) {
        return std::to_string(request.scope_hash());
    }
    return std::to_string(request.tenant_hash());
}

std::string CandidateCacheKey(const RetrieveContextPackRequest& request,
                              uint64_t node_hash, const std::string& record_type) {
    std::ostringstream os;
    os << "scope=" << CandidateCacheScope(request)
       << "|node=" << node_hash
       << "|type=" << record_type
       << "|append=" << request.append_watermark()
       << "|resource=" << request.resource_version_watermark()
       << "|skill=" << request.skill_status_watermark()
       << "|index=" << request.index_posting_watermark();
    return os.str();
}

bool LoadCandidateCache(const RetrieveContextPackRequest& request, uint64_t node_hash,
                        const std::string& record_type,
                        std::vector<NativeContextCandidate>* cached) {
    std::lock_guard<std::mutex> lock(CandidateCacheMutex());
    const auto iter = CandidateCache().find(CandidateCacheKey(request, node_hash, record_type));
    if (iter == CandidateCache().end()) {
        return false;
    }
    *cached = iter->second;
    return true;
}

void StoreCandidateCache(const RetrieveContextPackRequest& request, uint64_t node_hash,
                         const std::string& record_type,
                         const std::vector<NativeContextCandidate>& candidates) {
    std::lock_guard<std::mutex> lock(CandidateCacheMutex());
    auto& cache = CandidateCache();
    if (cache.size() >= kMaxNativeCandidateCacheEntries) {
        cache.clear();
    }
    cache[CandidateCacheKey(request, node_hash, record_type)] = candidates;
}

uint64_t NowSteadyMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

uint64_t ElapsedSinceMs(uint64_t start_ms) {
    const uint64_t now_ms = NowSteadyMs();
    return now_ms >= start_ms ? now_ms - start_ms : 0;
}

uint32_t EstimateTokens(const std::string& text) {
    uint32_t tokens = 0;
    bool in_token = false;
    for (unsigned char c : text) {
        if (std::isspace(c)) {
            in_token = false;
        } else if (!in_token) {
            in_token = true;
            ++tokens;
        }
    }
    return std::max<uint32_t>(1, tokens);
}

std::string CandidateKey(const std::string& ref_type, uint64_t node_hash,
                         uint64_t event_time_ms, uint64_t ref_hash) {
    return ref_type + ":" + std::to_string(node_hash) + ":" +
           std::to_string(event_time_ms) + ":" + std::to_string(ref_hash);
}

void MergeCandidate(const NativeContextCandidate& candidate,
                    std::map<std::string, NativeContextCandidate>* candidates) {
    const std::string key = CandidateKey(candidate.ref_type, candidate.node_hash,
                                         candidate.event_time_ms, candidate.ref_hash);
    NativeContextCandidate& existing = (*candidates)[key];
    if (existing.ref_hash == 0) {
        existing = candidate;
        return;
    }
    existing.matched_index_names.insert(candidate.matched_index_names.begin(),
                                        candidate.matched_index_names.end());
    existing.base_score = std::max(existing.base_score, candidate.base_score);
}

Status ValidateRange(uint64_t start_time_ms, uint64_t end_time_ms) {
    if (end_time_ms <= start_time_ms) {
        return Status::InvalidArgument("end_time_ms must be greater than start_time_ms");
    }
    Status status = ValidateTimelineTimestamp(start_time_ms);
    if (!status.ok()) {
        return status;
    }
    return ValidateTimelineTimestamp(end_time_ms);
}

Status ValidateWriteTimestamp(uint64_t timestamp_ms) {
    if (timestamp_ms == 0) {
        return Status::InvalidArgument("timestamp_ms must be non-zero");
    }
    return ValidateTimelineTimestamp(timestamp_ms);
}

Status ValidateNode(const ContextNode& node) {
    if (node.node_hash() == 0) {
        return Status::InvalidArgument("node_hash must be non-zero");
    }
    if (node.canonical_name().empty()) {
        return Status::InvalidArgument("canonical_name is required");
    }
    Status status = ValidateByteSize("canonical_name", node.canonical_name(), kMaxCanonicalNameBytes);
    if (!status.ok()) {
        return status;
    }
    status = ValidateByteSize("l0", node.l0(), kMaxL0Bytes);
    if (!status.ok()) {
        return status;
    }
    if (node.last_event_time_ms() != 0) {
        status = ValidateTimelineTimestamp(node.last_event_time_ms());
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

Status ValidateEvent(const ContextEvent& event) {
    if (EventPrimaryTime(event) == 0 || event.event_id_hash() == 0) {
        return Status::InvalidArgument("ingestion_time_ms and event_id_hash must be non-zero");
    }
    Status status = ValidateTimelineTimestamp(EventPrimaryTime(event));
    if (!status.ok()) {
        return status;
    }
    if (event.event_time_ms() != 0) {
        status = ValidateTimelineTimestamp(event.event_time_ms());
    }
    if (!status.ok()) {
        return status;
    }
    status = ValidateScore("confidence", event.confidence());
    if (!status.ok()) {
        return status;
    }
    status = ValidateScore("importance", event.importance());
    if (!status.ok()) {
        return status;
    }
    status = ValidateByteSize("text", event.text(), kMaxEventTextBytes);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status ValidateQueryEventFilters(const QueryEventsRequest& request) {
    if (static_cast<uint32_t>(request.types_size()) > kMaxFilterValues) {
        return Status::InvalidArgument("too many filter values");
    }
    Status status = ValidateScore("min_confidence", request.min_confidence());
    if (!status.ok()) {
        return status;
    }
    status = ValidateScore("min_importance", request.min_importance());
    if (!status.ok()) {
        return status;
    }
    status = ValidateScore("min_decayed_score", request.min_decayed_score());
    if (!status.ok()) {
        return status;
    }
    if ((request.min_decayed_score() > 0.0f || request.rank_by_decayed_score()) &&
        request.decay_half_life_ms() == 0) {
        return Status::InvalidArgument("decay_half_life_ms is required for decayed scoring");
    }
    if (request.decay_half_life_ms() > kMaxDecayHalfLifeMs) {
        return Status::InvalidArgument("decay_half_life_ms exceeds maximum");
    }
    return Status::OK();
}

Status ValidateExtractedContextIndexes(const ExtractedContextIndexes& indexes) {
    if (static_cast<uint32_t>(indexes.entity_hashes_size()) > kMaxFilterValues) {
        return Status::InvalidArgument("too many entity indexes");
    }
    if (static_cast<uint32_t>(indexes.disabled_indexes_size()) > kMaxFilterValues) {
        return Status::InvalidArgument("too many disabled indexes");
    }
    if (indexes.event_time_bucket_ms() != 0) {
        Status status = ValidateTimelineTimestamp(indexes.event_time_bucket_ms());
        if (!status.ok()) {
            return status;
        }
    }
    for (uint64_t entity_hash : indexes.entity_hashes()) {
        if (entity_hash == 0) {
            return Status::InvalidArgument("entity index hash must be non-zero");
        }
    }
    for (int index : indexes.disabled_indexes()) {
        if (index <= INTERNAL_INDEX_UNSPECIFIED ||
            index > INTERNAL_INDEX_EVENT_TIME_BUCKET) {
            return Status::InvalidArgument("disabled index is invalid");
        }
    }
    return Status::OK();
}

Status ValidateAuditRef(const AuditRef& ref) {
    if (ref.node_hash() == 0 || ref.event_time_ms() == 0) {
        return Status::InvalidArgument("audit ref node_hash and event_time_ms are required");
    }
    Status status = ValidateTimelineTimestamp(ref.event_time_ms());
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status ValidatePackAudit(const ContextPackAudit& audit) {
    if (audit.session_hash() == 0 || audit.request_time_ms() == 0 || audit.query_id().empty()) {
        return Status::InvalidArgument("session_hash, request_time_ms, and query_id are required");
    }
    Status status = ValidateByteSize("query_id", audit.query_id(), kMaxQueryIdBytes);
    if (!status.ok()) {
        return status;
    }
    status = ValidateTimelineTimestamp(audit.request_time_ms());
    if (!status.ok()) {
        return status;
    }
    if (static_cast<uint32_t>(audit.selected_refs_size()) > kMaxAuditRefs) {
        return Status::InvalidArgument("audit refs exceed maximum");
    }
    for (const auto& ref : audit.selected_refs()) {
        status = ValidateAuditRef(ref);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

uint64_t StableHash64(const std::string& value) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char c : value) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return hash;
}

Status ValidateSummaryDirtyMarker(const SummaryDirtyMarker& marker) {
    if (marker.node_hash() == 0 || marker.event_time_ms() == 0) {
        return Status::InvalidArgument("node_hash and event_time_ms are required");
    }
    if (marker.propagate_depth() > kMaxPropagateDepth) {
        return Status::InvalidArgument("propagate_depth exceeds maximum");
    }
    return ValidateTimelineTimestamp(marker.event_time_ms());
}

Status ValidateIndexRef(const IndexRef& ref) {
    if (ref.primary_node_hash() == 0 || ref.primary_event_time_ms() == 0 ||
        ref.event_id_hash() == 0) {
        return Status::InvalidArgument("invalid context index ref");
    }
    return ValidateTimelineTimestamp(ref.primary_event_time_ms());
}

Status ValidateChildRef(const ContextChildRef& ref) {
    if (ref.parent_hash() == 0 || ref.child_hash() == 0) {
        return Status::InvalidArgument("parent_hash and child_hash are required");
    }
    Status status = ValidateWriteTimestamp(ref.updated_at_ms());
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status ValidateEmbedding(const ContextEmbedding& embedding) {
    if (embedding.ref_hash() == 0 || embedding.vector_size() == 0 ||
        embedding.updated_at_ms() == 0) {
        return Status::InvalidArgument("ref_hash, vector, and updated_at_ms are required");
    }
    if (embedding.vector_size() > static_cast<int>(kMaxEmbeddingDim)) {
        return Status::InvalidArgument("embedding dimension is invalid");
    }
    Status status = ValidateTimelineTimestamp(embedding.updated_at_ms());
    if (!status.ok()) {
        return status;
    }
    for (float value : embedding.vector()) {
        if (!std::isfinite(value)) {
            return Status::InvalidArgument("embedding vector contains non-finite value");
        }
    }
    return Status::OK();
}

Status ValidateEntity(const ContextEntity& entity) {
    if (entity.entity_hash() == 0 || entity.node_hash() == 0 || entity.updated_at_ms() == 0) {
        return Status::InvalidArgument("entity_hash, node_hash, and updated_at_ms are required");
    }
    Status status = ValidateWriteTimestamp(entity.updated_at_ms());
    if (!status.ok()) {
        return status;
    }
    if (entity.valid_from_ms() != 0) {
        status = ValidateTimelineTimestamp(entity.valid_from_ms());
        if (!status.ok()) {
            return status;
        }
    }
    status = ValidateScore("confidence", entity.confidence());
    if (!status.ok()) {
        return status;
    }
    status = ValidateByteSize("entity name", entity.name(), kMaxEntityNameBytes);
    if (!status.ok()) {
        return status;
    }
    status = ValidateByteSize("entity value", entity.value(), kMaxEntityValueBytes);
    if (!status.ok()) {
        return status;
    }
    if (static_cast<uint32_t>(entity.source_event_hashes_size()) > kMaxAuditRefs) {
        return Status::InvalidArgument("entity source events exceed maximum");
    }
    return Status::OK();
}

Status ValidateSummary(const ContextSummary& summary) {
    if (summary.node_hash() == 0 || summary.valid_from_ms() == 0) {
        return Status::InvalidArgument("node_hash and valid_from_ms are required");
    }
    Status status = ValidateByteSize("summary text", summary.text(), kMaxSummaryBytes);
    if (!status.ok()) {
        return status;
    }
    status = ValidateTimelineTimestamp(summary.valid_from_ms());
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status ValidateCompressionEvent(const ContextCompressionEvent& event) {
    if (event.compression_id_hash() == 0 || event.node_hash() == 0 ||
        event.source_start_ms() == 0 || event.source_end_ms() == 0 ||
        event.compressed_time_ms() == 0) {
        return Status::InvalidArgument("compression id, node, and time range are required");
    }
    Status status = ValidateRange(event.source_start_ms(), event.source_end_ms());
    if (!status.ok()) {
        return status;
    }
    status = ValidateWriteTimestamp(event.compressed_time_ms());
    if (!status.ok()) {
        return status;
    }
    status = ValidateByteSize("compression summary", event.summary(), kMaxSummaryBytes);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

bool MatchesCompressionSourceFilter(const CompressEventsRequest& request,
                                    const ContextEvent& event) {
    if (event.confidence() < request.min_confidence()) {
        return false;
    }
    if (event.importance() < request.min_importance()) {
        return false;
    }
    return true;
}

Status LoadSourceEvents(ExecuteEnv* env, uint64_t tenant_hash, uint64_t node_hash,
                        uint64_t start_time_ms, uint64_t end_time_ms, uint32_t limit,
                        std::vector<ContextEvent>* events) {
    const std::string key = EventKey(tenant_hash, node_hash);
    ObjectHandle<model::ContextEventModel> object;
    Status status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    const uint32_t result_limit = LimitOrDefault(limit);
    object->OrSet().Query(
        TimelineStart(start_time_ms), TimelineEnd(end_time_ms), kMaxLimit,
        [events, result_limit](const uint64_t, const std::string& value) {
            if (static_cast<uint32_t>(events->size()) >= result_limit) {
                return;
            }
            ContextEvent event;
            if (event.ParseFromString(value)) {
                events->push_back(std::move(event));
            }
        });
    return Status::OK();
}

std::string TruncateForCompression(const std::string& text) {
    if (text.size() <= kMaxCompressionSnippetBytes) {
        return text;
    }
    return text.substr(0, kMaxCompressionSnippetBytes);
}

std::string BuildCompressionSummary(uint64_t source_start_ms, uint64_t source_end_ms,
                                    const std::vector<ContextEvent>& events,
                                    bool truncated_source_events) {
    std::ostringstream summary;
    summary << "Temporal compression window [" << source_start_ms << ", " << source_end_ms
            << "] contains " << events.size() << " selected events";
    if (truncated_source_events) {
        summary << " plus additional source events";
    }
    summary << ".";
    for (size_t index = 0; index < events.size() && index < 5; ++index) {
        summary << " #" << (index + 1) << " ingest_t=" << EventPrimaryTime(events[index])
                << " type=" << events[index].type() << " confidence="
                << events[index].confidence() << " importance=" << events[index].importance()
                << ": " << TruncateForCompression(events[index].text());
    }
    std::string text = summary.str();
    if (text.size() > kMaxSummaryBytes) {
        text.resize(kMaxSummaryBytes);
    }
    return text;
}

uint64_t CompressionIdHash(uint64_t tenant_hash, uint64_t node_hash, uint64_t source_start_ms,
                           uint64_t source_end_ms,
                           const std::vector<ContextEvent>& events) {
    std::string seed = std::to_string(tenant_hash) + ":" + std::to_string(node_hash) + ":" +
                       std::to_string(source_start_ms) + ":" + std::to_string(source_end_ms);
    for (const auto& event : events) {
        seed.append(":").append(std::to_string(event.event_id_hash()));
        seed.append("@").append(std::to_string(EventPrimaryTime(event)));
    }
    return StableHash64(seed);
}

uint32_t TraversalLimit(uint32_t value, uint32_t default_value, uint32_t max_value) {
    if (value == 0) {
        return default_value;
    }
    return std::min(value, max_value);
}

float CosineSimilarity(const google::protobuf::RepeatedField<float>& left,
                       const google::protobuf::RepeatedField<float>& right) {
    const int dim = std::min(left.size(), right.size());
    float dot = 0.0f;
    float left_norm = 0.0f;
    float right_norm = 0.0f;
    for (int i = 0; i < dim; ++i) {
        dot += left.Get(i) * right.Get(i);
        left_norm += left.Get(i) * left.Get(i);
        right_norm += right.Get(i) * right.Get(i);
    }
    if (left_norm <= 0.0f || right_norm <= 0.0f) {
        return 0.0f;
    }
    return dot / (std::sqrt(left_norm) * std::sqrt(right_norm));
}

Status LoadEmbedding(ExecuteEnv* env, uint64_t tenant_hash, uint64_t ref_hash,
                     ContextEmbedding* embedding) {
    const std::string key = EmbeddingKey(tenant_hash, ref_hash);
    ObjectHandle<model::ContextEmbeddingModel> object;
    Status status = env->GetObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    std::string value;
    status = object->OrSet().Get(kEmbeddingField, &value);
    if (!status.ok()) {
        return status;
    }
    if (!embedding->ParseFromString(value)) {
        return Status::InvalidArgument("stored ContextEmbedding is corrupted");
    }
    return Status::OK();
}

Status QueryChildrenInternal(ExecuteEnv* env, uint64_t tenant_hash, uint64_t parent_hash,
                             uint32_t limit, std::vector<ContextChildRef>* refs,
                             std::string* object_key) {
    const std::string key = ChildKey(tenant_hash, parent_hash);
    if (object_key != nullptr) {
        *object_key = key;
    }
    ObjectHandle<model::ContextChildModel> object;
    Status status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    object->OrSet().Query(0, std::numeric_limits<uint64_t>::max(), kMaxLimit,
                          [refs](const uint64_t, const std::string& value) {
                              ContextChildRef ref;
                              if (ref.ParseFromString(value)) {
                                  refs->push_back(std::move(ref));
                              }
                          });
    std::sort(refs->begin(), refs->end(), [](const ContextChildRef& left,
                                             const ContextChildRef& right) {
        if (left.updated_at_ms() != right.updated_at_ms()) {
            return left.updated_at_ms() > right.updated_at_ms();
        }
        return left.child_hash() < right.child_hash();
    });
    std::set<uint64_t> seen_child_hashes;
    std::vector<ContextChildRef> latest_refs;
    latest_refs.reserve(refs->size());
    for (const auto& ref : *refs) {
        if (seen_child_hashes.insert(ref.child_hash()).second) {
            latest_refs.push_back(ref);
        }
    }
    refs->swap(latest_refs);
    std::sort(refs->begin(), refs->end(), [](const ContextChildRef& left,
                                             const ContextChildRef& right) {
        return left.updated_at_ms() > right.updated_at_ms();
    });
    const uint32_t result_limit = LimitOrDefault(limit);
    if (static_cast<uint32_t>(refs->size()) > result_limit) {
        refs->resize(result_limit);
    }
    return Status::OK();
}

Status ValidateRequestLimitAndRange(uint32_t limit, uint64_t start_time_ms, uint64_t end_time_ms) {
    Status status = ValidateLimit(limit);
    if (!status.ok()) {
        return status;
    }
    return ValidateRange(start_time_ms, end_time_ms);
}

Status ValidateTenant(uint64_t tenant_hash) {
    if (tenant_hash == 0) {
        return Status::InvalidArgument("tenant_hash must be non-zero");
    }
    return Status::OK();
}

}  // namespace

Status UpsertNode(ExecuteEnv* env, const UpsertNodeRequest& request, UpsertNodeResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status = ValidateNode(request.node());
    if (!status.ok()) {
        return status;
    }

    const std::string key = NodeKey(request.tenant_hash(), request.node().node_hash());
    ObjectHandle<model::ContextNodeModel> object;
    status = env->GetOrNewObject(key, &object);
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
    ObjectHandle<model::ContextNodeModel> object;
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
    if (request.tenant_hash() == 0 || request.node_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and node_hash must be non-zero");
    }
    ContextEvent event = request.event();
    NormalizeEventPrimaryTime(&event);
    Status status = ValidateEvent(event);
    if (!status.ok()) {
        return status;
    }

    const std::string key = EventKey(request.tenant_hash(), request.node_hash());
    ObjectHandle<model::ContextEventModel> object;
    status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    const uint64_t timeline_key =
        TimelineKey(EventPrimaryTime(event), event.event_id_hash());
    if (request.first_write_only() && object->OrSet().Get(timeline_key)) {
        response->set_object_key(key);
        return Status::OK();
    }

    std::string value;
    if (!event.SerializeToString(&value)) {
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
    Status status =
        ValidateRequestLimitAndRange(request.limit(), request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }
    status = ValidateQueryEventFilters(request);
    if (!status.ok()) {
        return status;
    }

    const std::string key = EventKey(request.tenant_hash(), request.node_hash());
    response->set_object_key(key);
    ObjectHandle<model::ContextEventModel> object;
    status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }

    std::vector<ScoredContextEvent> matched_events;
    matched_events.reserve(LimitOrDefault(request.limit()));
    object->OrSet().Query(
        TimelineStart(request.start_time_ms()), TimelineEnd(request.end_time_ms()), kMaxLimit,
        [&request, &matched_events](const uint64_t, const std::string& value) {
            ContextEvent event;
            if (!event.ParseFromString(value)) {
                return;
            }
            if (!MatchesEventFilter(request, event)) {
                return;
            }
            const float decayed_score = DecayedEventScore(request, event);
            if (decayed_score < request.min_decayed_score()) {
                return;
            }
            ScoredContextEvent scored;
            scored.event = std::move(event);
            scored.decayed_score = decayed_score;
            matched_events.push_back(std::move(scored));
        });
    if (request.rank_by_decayed_score()) {
        std::sort(matched_events.begin(), matched_events.end(),
                  [](const ScoredContextEvent& left, const ScoredContextEvent& right) {
                      if (left.decayed_score != right.decayed_score) {
                          return left.decayed_score > right.decayed_score;
                      }
                      if (EventPrimaryTime(left.event) != EventPrimaryTime(right.event)) {
                          return EventPrimaryTime(left.event) > EventPrimaryTime(right.event);
                      }
                      return left.event.event_id_hash() < right.event.event_id_hash();
                  });
    }
    const bool include_scores = DecayEnabled(request);
    const uint32_t result_limit = LimitOrDefault(request.limit());
    for (const auto& scored : matched_events) {
        if (static_cast<uint32_t>(response->events_size()) >= result_limit) {
            break;
        }
        *response->add_events() = scored.event;
        if (include_scores) {
            response->add_decayed_scores(scored.decayed_score);
        }
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_EVENTS, QueryEvents, Read);

Status WriteIndexRef(ExecuteEnv* env, const WriteIndexRefRequest& request,
                     WriteIndexRefResponse* response) {
    if (request.tenant_hash() == 0 || request.index_value_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and index_value_hash are required");
    }
    Status status = ValidateIndexName(request.index_name());
    if (!status.ok()) {
        return status;
    }
    status = ValidateWriteTimestamp(request.event_time_ms());
    if (!status.ok()) {
        return status;
    }
    status = ValidateIndexRef(request.ref());
    if (!status.ok()) {
        return status;
    }

    if (request.time_bucket_ms() != 0) {
        status = ValidateTimelineTimestamp(request.time_bucket_ms());
        if (!status.ok()) {
            return status;
        }
    }

    const std::string key =
        IndexStorageKey(request.tenant_hash(), request.index_name(),
                        request.index_value_hash(), request.scope_hash(),
                        request.time_bucket_ms());
    ObjectHandle<model::ContextIndexModel> object;
    status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }

    IndexRef stored_ref = request.ref();
    stored_ref.set_index_value_hash(request.index_value_hash());
    std::string value;
    if (!stored_ref.SerializeToString(&value)) {
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

Status WriteDefaultIndexRef(ExecuteEnv* env, uint64_t tenant_hash, uint64_t scope_hash,
                            InternalContextIndex index, uint64_t index_value_hash,
                            uint64_t index_time_ms, const IndexRef& ref,
                            WriteExtractedEventResponse* response) {
    if (index_value_hash == 0 || index_time_ms == 0) {
        return Status::OK();
    }
    WriteIndexRefRequest request;
    request.set_tenant_hash(tenant_hash);
    request.set_index_name(InternalIndexName(index));
    request.set_index_value_hash(index_value_hash);
    request.set_scope_hash(scope_hash);
    request.set_event_time_ms(index_time_ms);
    request.set_time_bucket_ms(IndexTimeBucket(index_time_ms));
    *request.mutable_ref() = ref;

    WriteIndexRefResponse index_response;
    Status status = WriteIndexRef(env, request, &index_response);
    if (!status.ok()) {
        return status;
    }
    response->add_index_object_keys(index_response.object_key());
    response->set_written_index_count(response->written_index_count() + 1);
    return Status::OK();
}

Status WriteExtractedEvent(ExecuteEnv* env, const WriteExtractedEventRequest& request,
                           WriteExtractedEventResponse* response) {
    if (request.tenant_hash() == 0 || request.node_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and node_hash must be non-zero");
    }
    ContextEvent event = request.event();
    NormalizeEventPrimaryTime(&event);
    Status status = ValidateEvent(event);
    if (!status.ok()) {
        return status;
    }
    status = ValidateExtractedContextIndexes(request.indexes());
    if (!status.ok()) {
        return status;
    }

    WriteEventRequest event_request;
    event_request.set_tenant_hash(request.tenant_hash());
    event_request.set_node_hash(request.node_hash());
    *event_request.mutable_event() = event;
    event_request.set_first_write_only(request.first_write_only());
    WriteEventResponse event_response;
    status = WriteEvent(env, event_request, &event_response);
    if (!status.ok()) {
        return status;
    }
    response->set_event_object_key(event_response.object_key());

    IndexRef ref;
    ref.set_primary_node_hash(request.node_hash());
    ref.set_primary_event_time_ms(EventPrimaryTime(event));
    ref.set_event_id_hash(event.event_id_hash());

    const auto& indexes = request.indexes();
    const uint64_t scope_hash = indexes.scope_hash();
    const uint64_t primary_time_ms = EventPrimaryTime(event);

    if (!ContainsIndex(indexes.disabled_indexes(), INTERNAL_INDEX_EVENT_KIND)) {
        status = WriteDefaultIndexRef(env, request.tenant_hash(), scope_hash,
                                      INTERNAL_INDEX_EVENT_KIND, event.type(),
                                      primary_time_ms, ref, response);
        if (!status.ok()) {
            return status;
        }
    }
    if (!ContainsIndex(indexes.disabled_indexes(), INTERNAL_INDEX_STATUS)) {
        status = WriteDefaultIndexRef(env, request.tenant_hash(), scope_hash,
                                      INTERNAL_INDEX_STATUS, indexes.status_hash(),
                                      primary_time_ms, ref, response);
        if (!status.ok()) {
            return status;
        }
    }
    if (!ContainsIndex(indexes.disabled_indexes(), INTERNAL_INDEX_SOURCE)) {
        status = WriteDefaultIndexRef(env, request.tenant_hash(), scope_hash,
                                      INTERNAL_INDEX_SOURCE, indexes.source_hash(),
                                      primary_time_ms, ref, response);
        if (!status.ok()) {
            return status;
        }
    }
    if (!ContainsIndex(indexes.disabled_indexes(), INTERNAL_INDEX_EVENT_TIME_BUCKET)) {
        status = WriteDefaultIndexRef(env, request.tenant_hash(), scope_hash,
                                      INTERNAL_INDEX_EVENT_TIME_BUCKET,
                                      indexes.event_time_bucket_ms(),
                                      indexes.event_time_bucket_ms(), ref, response);
        if (!status.ok()) {
            return status;
        }
    }
    if (!ContainsIndex(indexes.disabled_indexes(), INTERNAL_INDEX_ENTITY)) {
        for (uint64_t entity_hash : indexes.entity_hashes()) {
            status = WriteDefaultIndexRef(env, request.tenant_hash(), scope_hash,
                                          INTERNAL_INDEX_ENTITY, entity_hash,
                                          primary_time_ms, ref, response);
            if (!status.ok()) {
                return status;
            }
        }
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, WRITE_EXTRACTED_EVENT, WriteExtractedEvent, Write);

Status QueryIndex(ExecuteEnv* env, const QueryIndexRequest& request, QueryIndexResponse* response) {
    if (request.tenant_hash() == 0 || request.index_value_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and index_value_hash are required");
    }
    Status status = ValidateIndexName(request.index_name());
    if (!status.ok()) {
        return status;
    }
    status =
        ValidateRequestLimitAndRange(request.limit(), request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }

    if (request.time_bucket_ms() != 0) {
        status = ValidateTimelineTimestamp(request.time_bucket_ms());
        if (!status.ok()) {
            return status;
        }
    }

    const std::string key =
        IndexStorageKey(request.tenant_hash(), request.index_name(),
                        request.index_value_hash(), request.scope_hash(),
                        request.time_bucket_ms());
    response->set_object_key(key);
    ObjectHandle<model::ContextIndexModel> object;
    status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }

    const bool compact_bucket = request.time_bucket_ms() != 0;
    object->OrSet().Query(TimelineStart(request.start_time_ms()),
                          TimelineEnd(request.end_time_ms()), LimitOrDefault(request.limit()),
                          [response, compact_bucket, &request](
                              const uint64_t, const std::string& value) {
                              IndexRef ref;
                              if (ref.ParseFromString(value) &&
                                  (!compact_bucket ||
                                   ref.index_value_hash() == request.index_value_hash())) {
                                  *response->add_refs() = std::move(ref);
                              }
                          });
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_INDEX, QueryIndex, Read);

Status ValidateRetrieveContextPackRequest(const RetrieveContextPackRequest& request) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    if (request.start_node_hash() == 0 || request.start_time_ms() == 0 ||
        request.end_time_ms() == 0) {
        return Status::InvalidArgument("start node and time range are required");
    }
    status = ValidateRange(request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }
    if (request.as_of_ms() != 0) {
        status = ValidateTimelineTimestamp(request.as_of_ms());
        if (!status.ok()) {
            return status;
        }
    }
    status = ValidateLimit(request.max_selected_refs());
    if (!status.ok()) {
        return status;
    }
    status = ValidateLimit(request.max_candidate_nodes());
    if (!status.ok()) {
        return status;
    }
    status = ValidateScore("min_score", request.min_score());
    if (!status.ok()) {
        return status;
    }
    if (request.decay_half_life_ms() > kMaxDecayHalfLifeMs) {
        return Status::InvalidArgument("decay_half_life_ms exceeds maximum");
    }
    if (static_cast<uint32_t>(request.index_filters_size()) > kMaxFilterValues) {
        return Status::InvalidArgument("too many index filters");
    }
    if (static_cast<uint32_t>(request.query_vector_size()) > kMaxEmbeddingDim) {
        return Status::InvalidArgument("query vector dimension exceeds maximum");
    }
    for (float value : request.query_vector()) {
        if (!std::isfinite(value)) {
            return Status::InvalidArgument("query vector contains non-finite value");
        }
    }
    for (const auto& filter : request.index_filters()) {
        status = ValidateIndexName(filter.index_name());
        if (!status.ok()) {
            return status;
        }
        if (filter.index_value_hash() == 0) {
            return Status::InvalidArgument("index filter value hash is required");
        }
        status = ValidateRange(filter.start_time_ms() == 0 ? request.start_time_ms()
                                                           : filter.start_time_ms(),
                               filter.end_time_ms() == 0 ? request.end_time_ms()
                                                         : filter.end_time_ms());
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

Status LoadEventByIndexRef(ExecuteEnv* env, uint64_t tenant_hash, const IndexRef& ref,
                           ContextEvent* event, bool* found) {
    *found = false;
    if (ref.primary_node_hash() == 0 || ref.primary_event_time_ms() == 0 ||
        ref.event_id_hash() == 0) {
        return Status::OK();
    }
    const std::string key = EventKey(tenant_hash, ref.primary_node_hash());
    ObjectHandle<model::ContextEventModel> object;
    Status status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    const uint64_t timeline_key = TimelineKey(ref.primary_event_time_ms(),
                                              ref.event_id_hash());
    object->OrSet().Query(timeline_key, timeline_key + 1, 1,
                          [event, found](const uint64_t,
                                         const std::string& value) {
                              ContextEvent parsed;
                              if (parsed.ParseFromString(value)) {
                                  *event = std::move(parsed);
                                  *found = true;
                              }
                          });
    return Status::OK();
}

Status CollectCandidateNodes(ExecuteEnv* env, const RetrieveContextPackRequest& request,
                             std::set<uint64_t>* selected_node_hashes,
                             NativeRetrieveTelemetry* telemetry) {
    selected_node_hashes->insert(request.start_node_hash());
    if (request.query_vector_size() == 0) {
        const uint64_t start_ms = NowSteadyMs();
        const uint32_t max_depth = request.max_depth() == 0 ? 2 : request.max_depth();
        const uint32_t child_limit =
            TraversalLimit(request.max_children_scored_per_parent(),
                           kDefaultTraversalChildren, kMaxChildrenScoredPerParent);
        std::vector<uint64_t> frontier;
        frontier.push_back(request.start_node_hash());
        for (uint32_t depth = 0; depth < max_depth && !frontier.empty(); ++depth) {
            std::vector<uint64_t> next_frontier;
            for (uint64_t parent_hash : frontier) {
                std::vector<ContextChildRef> children;
                Status status = QueryChildrenInternal(env, request.tenant_hash(), parent_hash,
                                                      child_limit, &children, nullptr);
                if (!status.ok()) {
                    return status;
                }
                for (const auto& child : children) {
                    if (selected_node_hashes->insert(child.child_hash()).second) {
                        next_frontier.push_back(child.child_hash());
                    }
                    if (static_cast<uint32_t>(selected_node_hashes->size()) >=
                        TraversalLimit(request.max_candidate_nodes(),
                                       kDefaultTraversalCandidates, kMaxLimit)) {
                        break;
                    }
                }
                if (static_cast<uint32_t>(selected_node_hashes->size()) >=
                    TraversalLimit(request.max_candidate_nodes(),
                                   kDefaultTraversalCandidates, kMaxLimit)) {
                    break;
                }
            }
            frontier.swap(next_frontier);
        }
        telemetry->set_node_traversal_ms(ElapsedSinceMs(start_ms));
        return Status::OK();
    }
    const uint64_t start_ms = NowSteadyMs();
    TraverseContextTreeRequest traverse_request;
    traverse_request.set_tenant_hash(request.tenant_hash());
    traverse_request.set_start_node_hash(request.start_node_hash());
    for (float value : request.query_vector()) {
        traverse_request.add_query_vector(value);
    }
    traverse_request.set_max_depth(request.max_depth());
    traverse_request.set_top_k_per_depth(request.top_k_per_depth());
    traverse_request.set_max_children_scored_per_parent(
        request.max_children_scored_per_parent());
    traverse_request.set_max_candidate_nodes(request.max_candidate_nodes());
    traverse_request.set_leaf_only(request.leaf_only());
    TraverseContextTreeResponse traverse_response;
    Status status = TraverseContextTree(env, traverse_request, &traverse_response);
    if (!status.ok()) {
        return status;
    }
    for (const auto& node : traverse_response.nodes()) {
        selected_node_hashes->insert(node.node_hash());
    }
    telemetry->set_node_traversal_ms(ElapsedSinceMs(start_ms));
    return Status::OK();
}

void ApplyPlacementFilter(const RetrieveContextPackRequest& request,
                          std::set<uint64_t>* selected_node_hashes,
                          NativeRetrieveTelemetry* telemetry) {
    telemetry->set_placement_filter_applied(true);
    if (request.placement_node_hash() == 0) {
        return;
    }
    if (selected_node_hashes->find(request.placement_node_hash()) ==
        selected_node_hashes->end()) {
        telemetry->set_dropped_by_placement(
            telemetry->dropped_by_placement() +
            static_cast<uint32_t>(selected_node_hashes->size()));
        selected_node_hashes->clear();
        return;
    }
    const uint64_t placement_node_hash = request.placement_node_hash();
    const uint32_t dropped =
        static_cast<uint32_t>(selected_node_hashes->size() > 0
                                  ? selected_node_hashes->size() - 1
                                  : 0);
    selected_node_hashes->clear();
    selected_node_hashes->insert(placement_node_hash);
    telemetry->set_dropped_by_placement(telemetry->dropped_by_placement() + dropped);
}

Status AddCandidateFromEvent(const ContextEvent& event, uint64_t node_hash,
                             const std::string& matched_index_name,
                             std::map<std::string, NativeContextCandidate>* candidates) {
    const std::string key =
        CandidateKey("event", node_hash, EventPrimaryTime(event), event.event_id_hash());
    NativeContextCandidate& candidate = (*candidates)[key];
    if (candidate.ref_hash == 0) {
        candidate.event = event;
        candidate.ref_type = "event";
        candidate.ref_hash = event.event_id_hash();
        candidate.node_hash = node_hash;
        candidate.event_time_ms = EventPrimaryTime(event);
        candidate.text = event.text();
        candidate.token_estimate = EstimateTokens(candidate.text);
        candidate.base_score = event.confidence() * event.importance();
    }
    if (!matched_index_name.empty()) {
        candidate.matched_index_names.insert(matched_index_name);
    }
    return Status::OK();
}

Status AddGenericCandidate(const std::string& ref_type, uint64_t ref_hash,
                           uint64_t node_hash, uint64_t event_time_ms,
                           const std::string& text, float base_score,
                           const std::string& matched_index_name,
                           std::map<std::string, NativeContextCandidate>* candidates) {
    if (ref_hash == 0 || node_hash == 0 || text.empty()) {
        return Status::OK();
    }
    const std::string key = CandidateKey(ref_type, node_hash, event_time_ms, ref_hash);
    NativeContextCandidate& candidate = (*candidates)[key];
    if (candidate.ref_hash == 0) {
        candidate.ref_type = ref_type;
        candidate.ref_hash = ref_hash;
        candidate.node_hash = node_hash;
        candidate.event_time_ms = event_time_ms;
        candidate.text = text;
        candidate.token_estimate = EstimateTokens(text);
        candidate.base_score = base_score;
    }
    if (!matched_index_name.empty()) {
        candidate.matched_index_names.insert(matched_index_name);
    }
    return Status::OK();
}

std::string EntityCandidateText(const ContextEntity& entity) {
    std::string text = entity.name();
    if (!entity.value().empty()) {
        if (!text.empty()) {
            text.append(": ");
        }
        text.append(entity.value());
    }
    return text;
}

Status AddCandidateFromEntity(const ContextEntity& entity,
                              const std::string& matched_index_name,
                              std::map<std::string, NativeContextCandidate>* candidates) {
    const float base_score = std::max(0.05f, entity.confidence());
    const uint64_t event_time_ms =
        entity.valid_from_ms() == 0 ? entity.updated_at_ms() : entity.valid_from_ms();
    return AddGenericCandidate("entity", entity.entity_hash(), entity.node_hash(),
                               event_time_ms, EntityCandidateText(entity), base_score,
                               matched_index_name, candidates);
}

Status LoadEntityByHash(ExecuteEnv* env, uint64_t tenant_hash, uint64_t node_hash,
                        uint64_t entity_hash, ContextEntity* entity, bool* found) {
    *found = false;
    const std::string key = EntityKey(tenant_hash, node_hash, entity_hash);
    ObjectHandle<model::ContextEntityModel> object;
    Status status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    std::string value;
    status = object->OrSet().Get(kEntityField, &value);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    if (!entity->ParseFromString(value)) {
        return Status::InvalidArgument("stored ContextEntity is corrupted");
    }
    *found = true;
    return Status::OK();
}

float ApplyCandidateTemporalDecay(const RetrieveContextPackRequest& request,
                                  const NativeContextCandidate& candidate) {
    if (request.decay_half_life_ms() == 0 || candidate.event_time_ms == 0) {
        return candidate.base_score;
    }
    const uint64_t as_of_ms = request.as_of_ms() == 0 ? request.end_time_ms()
                                                      : request.as_of_ms();
    const uint64_t age_ms =
        candidate.event_time_ms >= as_of_ms ? 0 : as_of_ms - candidate.event_time_ms;
    const double decay =
        std::pow(0.5, static_cast<double>(age_ms) /
                          static_cast<double>(request.decay_half_life_ms()));
    return static_cast<float>(static_cast<double>(candidate.base_score) * decay);
}

float EmbeddingBoostForCandidate(ExecuteEnv* env,
                                 const RetrieveContextPackRequest& request,
                                 const NativeContextCandidate& candidate) {
    if (request.query_vector_size() == 0 || candidate.ref_hash == 0) {
        return 0.0f;
    }
    ContextEmbedding embedding;
    Status status = LoadEmbedding(env, request.tenant_hash(), candidate.ref_hash, &embedding);
    if (status.IsNotFound()) {
        return 0.0f;
    }
    if (!status.ok()) {
        return 0.0f;
    }
    google::protobuf::RepeatedField<float> query_vector;
    for (float value : request.query_vector()) {
        query_vector.Add(value);
    }
    return std::max(0.0f, CosineSimilarity(query_vector, embedding.vector()));
}

Status LoadLatestSummary(ExecuteEnv* env, uint64_t tenant_hash, uint64_t node_hash,
                         uint32_t level, uint64_t as_of_ms, ContextSummary* latest_summary,
                         bool* found);

Status QueryCompressionEvents(ExecuteEnv* env, const QueryCompressionEventsRequest& request,
                              QueryCompressionEventsResponse* response);

Status CollectNodeSummaryAndCompressionCandidates(
    ExecuteEnv* env, const RetrieveContextPackRequest& request,
    const std::set<uint64_t>& selected_node_hashes,
    std::map<std::string, NativeContextCandidate>* candidates,
    NativeRetrieveTelemetry* telemetry) {
    const uint64_t start_ms = NowSteadyMs();
    const uint64_t as_of_ms = request.as_of_ms() == 0 ? request.end_time_ms()
                                                      : request.as_of_ms();
    for (uint64_t node_hash : selected_node_hashes) {
        for (uint32_t level = 1; level <= 2; ++level) {
            ContextSummary summary;
            bool found_summary = false;
            Status status = LoadLatestSummary(env, request.tenant_hash(), node_hash, level,
                                              as_of_ms, &summary, &found_summary);
            if (!status.ok()) {
                return status;
            }
            if (!found_summary) {
                continue;
            }
            telemetry->set_candidate_fetch_count(telemetry->candidate_fetch_count() + 1);
            const float base_score = level == 1 ? 0.82f : 0.78f;
            status = AddGenericCandidate(level == 1 ? "summary_l0" : "summary_l1",
                                         StableHash64(std::to_string(node_hash) + ":" +
                                                      std::to_string(level)),
                                         node_hash, summary.valid_from_ms(), summary.text(),
                                         base_score, "", candidates);
            if (!status.ok()) {
                return status;
            }
        }
    }
    if (request.start_time_ms() != 0 && request.end_time_ms() != 0) {
        QueryCompressionEventsRequest compression_request;
        compression_request.set_tenant_hash(request.tenant_hash());
        for (uint64_t node_hash : selected_node_hashes) {
            compression_request.add_node_hashes(node_hash);
        }
        compression_request.set_start_time_ms(request.start_time_ms());
        compression_request.set_end_time_ms(request.end_time_ms());
        compression_request.set_limit(TraversalLimit(request.max_candidate_nodes(),
                                                     kDefaultTraversalCandidates, kMaxLimit));
        QueryCompressionEventsResponse compression_response;
        Status status = QueryCompressionEvents(env, compression_request, &compression_response);
        if (!status.ok()) {
            return status;
        }
        telemetry->set_candidate_fetch_count(telemetry->candidate_fetch_count() +
                                             compression_response.events_size());
        for (const auto& event : compression_response.events()) {
            status = AddGenericCandidate("compression", event.compression_id_hash(),
                                         event.node_hash(), event.compressed_time_ms(),
                                         event.summary(), 0.74f, "", candidates);
            if (!status.ok()) {
                return status;
            }
        }
    }
    telemetry->set_candidate_fetch_ms(telemetry->candidate_fetch_ms() +
                                      ElapsedSinceMs(start_ms));
    return Status::OK();
}

Status CollectIndexCandidates(ExecuteEnv* env, const RetrieveContextPackRequest& request,
                              const std::set<uint64_t>& selected_node_hashes,
                              std::map<std::string, NativeContextCandidate>* candidates,
                              NativeRetrieveTelemetry* telemetry) {
    const uint64_t start_ms = NowSteadyMs();
    for (const auto& filter : request.index_filters()) {
        const uint64_t filter_start_ms =
            filter.start_time_ms() == 0 ? request.start_time_ms() : filter.start_time_ms();
        const uint64_t filter_end_ms =
            filter.end_time_ms() == 0 ? request.end_time_ms() : filter.end_time_ms();
        std::vector<uint64_t> buckets;
        if (filter.time_bucket_ms() != 0) {
            buckets.push_back(filter.time_bucket_ms());
        } else if (request.index_time_bucket_ms() != 0) {
            buckets.push_back(request.index_time_bucket_ms());
        } else {
            buckets = LatestIndexBucketsForRange(filter_start_ms, filter_end_ms);
        }
        telemetry->set_compact_index_bucket_used(!buckets.empty());
        telemetry->set_compact_index_bucket_count(
            telemetry->compact_index_bucket_count() + static_cast<uint32_t>(buckets.size()));

        uint32_t refs_read_for_filter = 0;
        for (uint64_t bucket_ms : buckets) {
            QueryIndexRequest index_request;
            index_request.set_tenant_hash(request.tenant_hash());
            index_request.set_index_name(filter.index_name());
            index_request.set_index_value_hash(filter.index_value_hash());
            index_request.set_scope_hash(request.scope_hash());
            index_request.set_start_time_ms(filter_start_ms);
            index_request.set_end_time_ms(filter_end_ms);
            index_request.set_limit(filter.limit());
            index_request.set_time_bucket_ms(bucket_ms);
            QueryIndexResponse index_response;
            Status status = QueryIndex(env, index_request, &index_response);
            if (!status.ok()) {
                return status;
            }
            refs_read_for_filter += index_response.refs_size();
            telemetry->set_index_postings_read(telemetry->index_postings_read() +
                                               index_response.refs_size());
            for (const auto& ref : index_response.refs()) {
                if (!selected_node_hashes.empty() &&
                    selected_node_hashes.find(ref.primary_node_hash()) ==
                        selected_node_hashes.end()) {
                    telemetry->set_dropped_by_placement(telemetry->dropped_by_placement() + 1);
                    continue;
                }
                ContextEvent event;
                bool found = false;
                status = LoadEventByIndexRef(env, request.tenant_hash(), ref, &event, &found);
                if (!status.ok()) {
                    return status;
                }
                telemetry->set_candidate_fetch_count(telemetry->candidate_fetch_count() + 1);
                telemetry->set_placement_fetch_count(telemetry->placement_fetch_count() + 1);
                if (!found) {
                    telemetry->set_dropped_by_missing_record(
                        telemetry->dropped_by_missing_record() + 1);
                    continue;
                }
                status = AddCandidateFromEvent(event, ref.primary_node_hash(),
                                               filter.index_name(), candidates);
                if (!status.ok()) {
                    return status;
                }
                if (filter.index_name() == "entity" || filter.index_name() == "entity_hash") {
                    ContextEntity entity;
                    bool found_entity = false;
                    status = LoadEntityByHash(env, request.tenant_hash(),
                                              ref.primary_node_hash(),
                                              filter.index_value_hash(), &entity,
                                              &found_entity);
                    if (!status.ok()) {
                        return status;
                    }
                    if (found_entity) {
                        status = AddCandidateFromEntity(entity, filter.index_name(),
                                                        candidates);
                        if (!status.ok()) {
                            return status;
                        }
                    }
                }
            }
        }
        if (refs_read_for_filter == 0) {
            telemetry->set_dropped_by_index_filter(
                telemetry->dropped_by_index_filter() + 1);
        }
    }
    telemetry->set_index_prefilter_ms(ElapsedSinceMs(start_ms));
    return Status::OK();
}

Status CollectNodeEventCandidates(ExecuteEnv* env, const RetrieveContextPackRequest& request,
                                  const std::set<uint64_t>& selected_node_hashes,
                                  std::map<std::string, NativeContextCandidate>* candidates,
                                  NativeRetrieveTelemetry* telemetry) {
    const uint64_t start_ms = NowSteadyMs();
    const uint32_t per_node_limit = TraversalLimit(request.max_candidate_nodes(),
                                                   kDefaultTraversalCandidates, kMaxLimit);
    for (uint64_t node_hash : selected_node_hashes) {
        std::vector<NativeContextCandidate> cached_candidates;
        if (LoadCandidateCache(request, node_hash, "event", &cached_candidates)) {
            telemetry->set_candidate_cache_hit(true);
            for (const auto& candidate : cached_candidates) {
                MergeCandidate(candidate, candidates);
            }
            continue;
        }

        QueryEventsRequest events_request;
        events_request.set_tenant_hash(request.tenant_hash());
        events_request.set_node_hash(node_hash);
        events_request.set_start_time_ms(request.start_time_ms());
        events_request.set_end_time_ms(request.end_time_ms());
        events_request.set_limit(per_node_limit);
        events_request.set_as_of_ms(request.as_of_ms() == 0 ? request.end_time_ms()
                                                            : request.as_of_ms());
        events_request.set_decay_half_life_ms(request.decay_half_life_ms());
        events_request.set_min_decayed_score(request.min_score());
        events_request.set_rank_by_decayed_score(request.decay_half_life_ms() != 0);
        QueryEventsResponse events_response;
        Status status = QueryEvents(env, events_request, &events_response);
        if (!status.ok()) {
            return status;
        }
        telemetry->set_scanned_records(telemetry->scanned_records() +
                                       events_response.events_size());
        telemetry->set_placement_fetch_count(telemetry->placement_fetch_count() +
                                             events_response.events_size());
        std::map<std::string, NativeContextCandidate> node_candidates;
        for (const auto& event : events_response.events()) {
            status = AddCandidateFromEvent(event, node_hash, "", &node_candidates);
            if (!status.ok()) {
                return status;
            }
        }
        std::vector<NativeContextCandidate> parsed_candidates;
        parsed_candidates.reserve(node_candidates.size());
        for (const auto& pair : node_candidates) {
            parsed_candidates.push_back(pair.second);
            MergeCandidate(pair.second, candidates);
        }
        StoreCandidateCache(request, node_hash, "event", parsed_candidates);
    }
    telemetry->set_candidate_fetch_ms(ElapsedSinceMs(start_ms));
    return Status::OK();
}

float NativeCandidateScore(ExecuteEnv* env, const RetrieveContextPackRequest& request,
                           const NativeContextCandidate& candidate) {
    float decayed = 0.0f;
    if (candidate.ref_type == "event") {
        QueryEventsRequest score_request;
        score_request.set_end_time_ms(request.end_time_ms());
        score_request.set_as_of_ms(request.as_of_ms() == 0 ? request.end_time_ms()
                                                           : request.as_of_ms());
        score_request.set_decay_half_life_ms(request.decay_half_life_ms());
        decayed = DecayedEventScore(score_request, candidate.event);
    } else {
        decayed = ApplyCandidateTemporalDecay(request, candidate);
    }
    const float index_boost = candidate.matched_index_names.empty() ? 0.0f : 0.15f;
    const float embedding_boost = EmbeddingBoostForCandidate(env, request, candidate);
    return std::min(1.0f, decayed + index_boost + (embedding_boost * 0.20f));
}

Status RetrieveContextPack(ExecuteEnv* env, const RetrieveContextPackRequest& request,
                           RetrieveContextPackResponse* response) {
    Status status = ValidateRetrieveContextPackRequest(request);
    if (!status.ok()) {
        return status;
    }
    NativeRetrieveTelemetry* telemetry = response->mutable_telemetry();
    const uint64_t query_plan_start_ms = NowSteadyMs();
    telemetry->set_scope_filter_applied(request.scope_hash() != 0);
    telemetry->set_compact_index_prefilter_applied(request.index_filters_size() > 0);
    telemetry->set_stale_superseded_filter_applied(!request.include_superseded());
    telemetry->set_shared_resource_skill_quota_applied(
        request.shared_resource_max_refs() > 0 || request.skill_max_refs() > 0);
    telemetry->set_cross_session_quota_rerank_applied(
        request.cross_session_max_refs() > 0 || request.cross_session_rerank() ||
        request.same_session_priority());
    telemetry->set_broad_scan_used(false);
    telemetry->set_broad_scan_blocked(!request.allow_broad_scan_fallback());
    telemetry->set_candidate_cache_hit(false);

    std::set<uint64_t> selected_node_hashes;
    status = CollectCandidateNodes(env, request, &selected_node_hashes, telemetry);
    if (!status.ok()) {
        return status;
    }
    ApplyPlacementFilter(request, &selected_node_hashes, telemetry);
    telemetry->set_placement_partitions_touched(
        static_cast<uint32_t>(selected_node_hashes.size()));
    telemetry->set_query_plan_ms(ElapsedSinceMs(query_plan_start_ms));

    std::map<std::string, NativeContextCandidate> candidates;
    status = CollectIndexCandidates(env, request, selected_node_hashes, &candidates,
                                    telemetry);
    if (!status.ok()) {
        return status;
    }
    if (request.index_filters_size() == 0) {
        status = CollectNodeEventCandidates(env, request, selected_node_hashes, &candidates,
                                            telemetry);
        if (!status.ok()) {
            return status;
        }
    }
    status = CollectNodeSummaryAndCompressionCandidates(env, request, selected_node_hashes,
                                                        &candidates, telemetry);
    if (!status.ok()) {
        return status;
    }

    const uint64_t score_start_ms = NowSteadyMs();
    std::vector<NativeContextCandidate> scored_candidates;
    scored_candidates.reserve(candidates.size());
    for (auto& pair : candidates) {
        NativeContextCandidate candidate = std::move(pair.second);
        candidate.score = NativeCandidateScore(env, request, candidate);
        if (candidate.score < request.min_score()) {
            telemetry->set_dropped_by_score_threshold(
                telemetry->dropped_by_score_threshold() + 1);
            continue;
        }
        scored_candidates.push_back(std::move(candidate));
    }
    std::sort(scored_candidates.begin(), scored_candidates.end(),
              [](const NativeContextCandidate& left, const NativeContextCandidate& right) {
                  if (left.score != right.score) {
                      return left.score > right.score;
                  }
                  if (left.event_time_ms != right.event_time_ms) {
                      return left.event_time_ms > right.event_time_ms;
                  }
                  if (left.ref_type != right.ref_type) {
                      return left.ref_type < right.ref_type;
                  }
                  return left.ref_hash < right.ref_hash;
              });
    telemetry->set_score_ms(ElapsedSinceMs(score_start_ms));

    const uint64_t pack_start_ms = NowSteadyMs();
    const uint32_t max_refs = TraversalLimit(request.max_selected_refs(), 20, kMaxLimit);
    const uint32_t max_tokens = request.max_context_tokens() == 0 ? 4096
                                                                 : request.max_context_tokens();
    uint32_t used_tokens = 0;
    for (const auto& candidate : scored_candidates) {
        if (static_cast<uint32_t>(response->selected_refs_size()) >= max_refs) {
            telemetry->set_dropped_by_token_budget(
                telemetry->dropped_by_token_budget() + 1);
            continue;
        }
        const uint32_t tokens = candidate.token_estimate == 0
                                    ? EstimateTokens(candidate.text)
                                    : candidate.token_estimate;
        if (used_tokens + tokens > max_tokens) {
            telemetry->set_dropped_by_token_budget(
                telemetry->dropped_by_token_budget() + 1);
            continue;
        }
        ContextPackRef* ref = response->add_selected_refs();
        ref->set_ref_type(candidate.ref_type);
        ref->set_ref_hash(candidate.ref_hash);
        ref->set_node_hash(candidate.node_hash);
        ref->set_event_time_ms(candidate.event_time_ms);
        ref->set_score(candidate.score);
        ref->set_token_estimate(tokens);
        ref->set_text(candidate.text);
        for (const auto& index_name : candidate.matched_index_names) {
            ref->add_matched_index_names(index_name);
        }
        used_tokens += tokens;
    }
    telemetry->set_selected_refs(response->selected_refs_size());
    telemetry->set_dropped_refs(static_cast<uint32_t>(scored_candidates.size()) -
                                response->selected_refs_size() +
                                telemetry->dropped_by_scope() +
                                telemetry->dropped_by_missing_record() +
                                telemetry->dropped_by_placement() +
                                telemetry->dropped_by_index_filter() +
                                telemetry->dropped_by_stale_version() +
                                telemetry->dropped_by_score_threshold() +
                                telemetry->dropped_by_token_budget());
    telemetry->set_pack_ms(ElapsedSinceMs(pack_start_ms));
    telemetry->set_audit_ms(0);
    if (response->selected_refs_size() == 0) {
        response->add_warnings("native_context_pack_selected_refs_empty");
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, RETRIEVE_CONTEXT_PACK, RetrieveContextPack, Read);

Status WritePackAudit(ExecuteEnv* env, const WritePackAuditRequest& request,
                      WritePackAuditResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status = ValidatePackAudit(request.audit());
    if (!status.ok()) {
        return status;
    }

    const std::string key = AuditKey(request.tenant_hash(), request.audit().session_hash());
    ObjectHandle<model::ContextAuditModel> object;
    status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }

    std::string value;
    if (!request.audit().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextPackAudit");
    }
    const uint64_t query_hash = StableHash64(request.audit().query_id());
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
    Status status =
        ValidateRequestLimitAndRange(request.limit(), request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }

    const std::string key = AuditKey(request.tenant_hash(), request.session_hash());
    response->set_object_key(key);
    ObjectHandle<model::ContextAuditModel> object;
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
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status = ValidateSummaryDirtyMarker(request.marker());
    if (!status.ok()) {
        return status;
    }

    const std::string key = DirtyKey(request.tenant_hash(), request.marker().node_hash());
    ObjectHandle<model::ContextDirtyModel> object;
    status = env->GetOrNewObject(key, &object);
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
    Status status =
        ValidateRequestLimitAndRange(request.limit(), request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }

    const std::string key = DirtyKey(request.tenant_hash(), request.node_hash());
    response->set_object_key(key);
    ObjectHandle<model::ContextDirtyModel> object;
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

Status UpsertChildRef(ExecuteEnv* env, const UpsertChildRefRequest& request,
                      UpsertChildRefResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status = ValidateChildRef(request.ref());
    if (!status.ok()) {
        return status;
    }

    std::vector<ContextChildRef> existing_refs;
    status = QueryChildrenInternal(env, request.tenant_hash(), request.ref().parent_hash(),
                                   kMaxLimit, &existing_refs, nullptr);
    if (!status.ok()) {
        return status;
    }
    const bool created =
        std::none_of(existing_refs.begin(), existing_refs.end(),
                     [&request](const ContextChildRef& existing) {
                         return existing.child_hash() == request.ref().child_hash();
                     });

    const std::string key = ChildKey(request.tenant_hash(), request.ref().parent_hash());
    if (!created) {
        response->set_object_key(key);
        response->set_created(false);
        response->set_parent_child_count(static_cast<uint32_t>(existing_refs.size()));
        return Status::OK();
    }

    ObjectHandle<model::ContextChildModel> object;
    status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    std::string value;
    if (!request.ref().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextChildRef");
    }
    status = object->OrSet().Add(
        nullptr, TimelineKey(request.ref().updated_at_ms(), request.ref().child_hash()),
        std::move(value));
    if (!status.ok()) {
        return status;
    }

    std::vector<ContextChildRef> current_refs;
    status = QueryChildrenInternal(env, request.tenant_hash(), request.ref().parent_hash(),
                                   kMaxLimit, &current_refs, nullptr);
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    response->set_created(created);
    response->set_parent_child_count(static_cast<uint32_t>(current_refs.size()));
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, UPSERT_CHILD_REF, UpsertChildRef, Write);

Status QueryChildren(ExecuteEnv* env, const QueryChildrenRequest& request,
                     QueryChildrenResponse* response) {
    if (request.tenant_hash() == 0 || request.parent_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and parent_hash are required");
    }
    Status status = ValidateLimit(request.limit());
    if (!status.ok()) {
        return status;
    }
    std::vector<ContextChildRef> refs;
    std::string object_key;
    status = QueryChildrenInternal(env, request.tenant_hash(), request.parent_hash(),
                                   request.limit(), &refs, &object_key);
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(object_key);
    for (const auto& ref : refs) {
        *response->add_refs() = ref;
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_CHILDREN, QueryChildren, Read);

Status UpsertEmbedding(ExecuteEnv* env, const UpsertEmbeddingRequest& request,
                       UpsertEmbeddingResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status = ValidateEmbedding(request.embedding());
    if (!status.ok()) {
        return status;
    }

    const std::string key = EmbeddingKey(request.tenant_hash(), request.embedding().ref_hash());
    ObjectHandle<model::ContextEmbeddingModel> object;
    status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    std::string value;
    if (!request.embedding().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextEmbedding");
    }
    status = object->OrSet().Set(nullptr, kEmbeddingField, std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, UPSERT_EMBEDDING, UpsertEmbedding, Write);

Status QueryEmbeddings(ExecuteEnv* env, const QueryEmbeddingsRequest& request,
                       QueryEmbeddingsResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    if (static_cast<uint32_t>(request.ref_hashes_size()) > kMaxLimit) {
        return Status::InvalidArgument("too many ref_hashes");
    }
    const uint32_t result_limit = LimitOrDefault(request.limit());
    for (uint64_t ref_hash : request.ref_hashes()) {
        if (ref_hash == 0 || static_cast<uint32_t>(response->embeddings_size()) >= result_limit) {
            continue;
        }
        ContextEmbedding embedding;
        status = LoadEmbedding(env, request.tenant_hash(), ref_hash, &embedding);
        if (status.IsNotFound()) {
            continue;
        }
        if (!status.ok()) {
            return status;
        }
        *response->add_embeddings() = std::move(embedding);
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_EMBEDDINGS, QueryEmbeddings, Read);

Status TraverseContextTree(ExecuteEnv* env, const TraverseContextTreeRequest& request,
                           TraverseContextTreeResponse* response) {
    if (request.tenant_hash() == 0 || request.start_node_hash() == 0 ||
        request.query_vector_size() == 0) {
        return Status::InvalidArgument("tenant_hash, start_node_hash, and query_vector are required");
    }
    if (static_cast<uint32_t>(request.query_vector_size()) > kMaxEmbeddingDim) {
        return Status::InvalidArgument("query_vector dimension exceeds maximum");
    }
    for (float value : request.query_vector()) {
        if (!std::isfinite(value)) {
            return Status::InvalidArgument("query_vector contains non-finite value");
        }
    }

    const uint32_t max_depth =
        TraversalLimit(request.max_depth(), 6, kMaxTraversalDepth);
    const uint32_t top_k =
        TraversalLimit(request.top_k_per_depth(), kDefaultTopKPerDepth, kMaxLimit);
    const uint32_t child_limit = TraversalLimit(
        request.max_children_scored_per_parent(), kMaxChildrenScoredPerParent,
        kMaxChildrenScoredPerParent);
    const uint32_t candidate_limit = TraversalLimit(
        request.max_candidate_nodes(), kDefaultTraversalCandidates, kMaxLimit);

    std::vector<TraversedNode> frontier;
    TraversedNode root;
    root.set_node_hash(request.start_node_hash());
    root.set_depth(0);
    root.set_score(1.0f);
    frontier.push_back(root);

    for (uint32_t depth = 1; depth <= max_depth; ++depth) {
        std::vector<TraversedNode> scored_layer;
        for (const auto& parent : frontier) {
            std::vector<ContextChildRef> children;
            Status status = QueryChildrenInternal(env, request.tenant_hash(), parent.node_hash(),
                                                  child_limit, &children, nullptr);
            if (!status.ok()) {
                return status;
            }
            for (const auto& child : children) {
                ContextEmbedding embedding;
                status = LoadEmbedding(env, request.tenant_hash(), child.child_hash(), &embedding);
                if (status.IsNotFound()) {
                    continue;
                }
                if (!status.ok()) {
                    return status;
                }
                float score = CosineSimilarity(request.query_vector(), embedding.vector());
                if (score <= 0.0f) {
                    continue;
                }
                TraversedNode node;
                node.set_node_hash(child.child_hash());
                node.set_depth(depth);
                node.set_score(score);
                scored_layer.push_back(std::move(node));
            }
        }
        std::sort(scored_layer.begin(), scored_layer.end(),
                  [](const TraversedNode& left, const TraversedNode& right) {
                      if (left.score() != right.score()) {
                          return left.score() > right.score();
                      }
                      return left.node_hash() < right.node_hash();
                  });
        const uint32_t keep = std::min<uint32_t>(top_k, scored_layer.size());
        std::vector<TraversedNode> next_frontier;
        for (uint32_t index = 0; index < keep; ++index) {
            std::vector<ContextChildRef> grandchildren;
            Status status = QueryChildrenInternal(env, request.tenant_hash(),
                                                  scored_layer[index].node_hash(), 1,
                                                  &grandchildren, nullptr);
            if (!status.ok()) {
                return status;
            }
            const bool is_leaf = grandchildren.empty();
            next_frontier.push_back(scored_layer[index]);
            if (!request.leaf_only() || is_leaf) {
                *response->add_nodes() = scored_layer[index];
                if (static_cast<uint32_t>(response->nodes_size()) >= candidate_limit) {
                    return Status::OK();
                }
            }
        }
        if (next_frontier.empty()) {
            break;
        }
        frontier = std::move(next_frontier);
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, TRAVERSE_CONTEXT_TREE, TraverseContextTree, Read);

Status UpsertEntity(ExecuteEnv* env, const UpsertEntityRequest& request,
                    UpsertEntityResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status = ValidateEntity(request.entity());
    if (!status.ok()) {
        return status;
    }

    const std::string key = EntityKey(request.tenant_hash(), request.entity().node_hash(),
                                      request.entity().entity_hash());
    ObjectHandle<model::ContextEntityModel> object;
    status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    std::string value;
    if (!request.entity().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextEntity");
    }
    status = object->OrSet().Set(nullptr, kEntityField, std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, UPSERT_ENTITY, UpsertEntity, Write);

Status GetEntity(ExecuteEnv* env, const GetEntityRequest& request,
                 GetEntityResponse* response) {
    if (request.tenant_hash() == 0 || request.node_hash() == 0 || request.entity_hash() == 0) {
        return Status::InvalidArgument("tenant_hash, node_hash, and entity_hash are required");
    }
    const std::string key = EntityKey(request.tenant_hash(), request.node_hash(),
                                      request.entity_hash());
    response->set_object_key(key);
    ObjectHandle<model::ContextEntityModel> object;
    Status status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        response->set_exist(false);
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    std::string value;
    status = object->OrSet().Get(kEntityField, &value);
    if (status.IsNotFound()) {
        response->set_exist(false);
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    if (!response->mutable_entity()->ParseFromString(value)) {
        return Status::InvalidArgument("stored ContextEntity is corrupted");
    }
    response->set_exist(true);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, GET_ENTITY, GetEntity, Read);

Status QueryEntities(ExecuteEnv* env, const QueryEntitiesRequest& request,
                     QueryEntitiesResponse* response) {
    if (request.tenant_hash() == 0 || request.node_hash() == 0) {
        return Status::InvalidArgument("tenant_hash and node_hash are required");
    }
    if (static_cast<uint32_t>(request.entity_hashes_size()) > kMaxLimit) {
        return Status::InvalidArgument("too many entity_hashes");
    }
    Status status = ValidateLimit(request.limit());
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(
        EntityKey(request.tenant_hash(), request.node_hash(), request.entity_hashes_size() > 0
                                                            ? request.entity_hashes(0)
                                                            : 0));
    const uint32_t result_limit = LimitOrDefault(request.limit());
    for (uint64_t entity_hash : request.entity_hashes()) {
        if (entity_hash == 0 || static_cast<uint32_t>(response->entities_size()) >= result_limit) {
            continue;
        }
        GetEntityRequest get_request;
        get_request.set_tenant_hash(request.tenant_hash());
        get_request.set_node_hash(request.node_hash());
        get_request.set_entity_hash(entity_hash);
        GetEntityResponse get_response;
        status = GetEntity(env, get_request, &get_response);
        if (!status.ok()) {
            return status;
        }
        if (get_response.exist()) {
            *response->add_entities() = std::move(*get_response.mutable_entity());
        }
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_ENTITIES, QueryEntities, Read);

Status UpsertSummary(ExecuteEnv* env, const UpsertSummaryRequest& request,
                     UpsertSummaryResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status = ValidateSummary(request.summary());
    if (!status.ok()) {
        return status;
    }
    const std::string key =
        SummaryKey(request.tenant_hash(), request.summary().node_hash(), request.summary().level());
    ObjectHandle<model::ContextSummaryModel> object;
    status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    std::string value;
    if (!request.summary().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextSummary");
    }
    status = object->OrSet().Add(
        nullptr, TimelineKey(request.summary().valid_from_ms(),
                             request.summary().level()),
        std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, UPSERT_SUMMARY, UpsertSummary, Write);

Status QuerySummaries(ExecuteEnv* env, const QuerySummariesRequest& request,
                      QuerySummariesResponse* response) {
    if (request.tenant_hash() == 0 || request.node_hash() == 0 || request.level() == 0 ||
        request.as_of_ms() == 0) {
        return Status::InvalidArgument("tenant_hash, node_hash, level, and as_of_ms are required");
    }
    Status status = ValidateLimit(request.limit());
    if (!status.ok()) {
        return status;
    }
    status = ValidateTimelineTimestamp(request.as_of_ms());
    if (!status.ok()) {
        return status;
    }

    const std::string key = SummaryKey(request.tenant_hash(), request.node_hash(), request.level());
    response->set_object_key(key);
    ObjectHandle<model::ContextSummaryModel> object;
    status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    object->OrSet().Query(0, TimelineEnd(request.as_of_ms()), kMaxLimit,
                          [&request, response](const uint64_t, const std::string& value) {
                              if (static_cast<uint32_t>(response->summaries_size()) >=
                                  LimitOrDefault(request.limit())) {
                                  return;
                              }
                              ContextSummary summary;
                              if (!summary.ParseFromString(value)) {
                                  return;
                              }
                              if (summary.valid_from_ms() <= request.as_of_ms()) {
                                  *response->add_summaries() = std::move(summary);
                              }
                          });
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_SUMMARIES, QuerySummaries, Read);

Status LoadLatestSummary(ExecuteEnv* env, uint64_t tenant_hash, uint64_t node_hash,
                         uint32_t level, uint64_t as_of_ms, ContextSummary* latest_summary,
                         bool* found) {
    *found = false;
    const std::string key = SummaryKey(tenant_hash, node_hash, level);
    ObjectHandle<model::ContextSummaryModel> object;
    Status status = env->GetObject(key, &object);
    if (status.IsNotFound()) {
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }
    object->OrSet().Query(0, TimelineEnd(as_of_ms), kMaxLimit,
                          [as_of_ms, latest_summary, found](const uint64_t,
                                                            const std::string& value) {
                              ContextSummary summary;
                              if (!summary.ParseFromString(value)) {
                                  return;
                              }
                              if (summary.valid_from_ms() > as_of_ms) {
                                  return;
                              }
                              if (!*found ||
                                  summary.valid_from_ms() > latest_summary->valid_from_ms()) {
                                  *latest_summary = std::move(summary);
                                  *found = true;
                              }
                          });
    return Status::OK();
}

Status WriteCompressionEvent(ExecuteEnv* env, const WriteCompressionEventRequest& request,
                             WriteCompressionEventResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status = ValidateCompressionEvent(request.event());
    if (!status.ok()) {
        return status;
    }
    const std::string key = CompressionKey(request.tenant_hash(), request.event().node_hash());
    ObjectHandle<model::ContextCompressionModel> object;
    status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    std::string value;
    if (!request.event().SerializeToString(&value)) {
        return Status::InvalidArgument("failed to serialize ContextCompressionEvent");
    }
    status = object->OrSet().Add(
        nullptr, TimelineKey(request.event().compressed_time_ms(),
                             request.event().compression_id_hash()),
        std::move(value));
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(key);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, WRITE_COMPRESSION_EVENT, WriteCompressionEvent, Write);

Status CompressEvents(ExecuteEnv* env, const CompressEventsRequest& request,
                      CompressEventsResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    if (request.node_hash() == 0) {
        return Status::InvalidArgument("node_hash is required");
    }
    status = ValidateRequestLimitAndRange(request.max_source_events(),
                                          request.source_start_ms(),
                                          request.source_end_ms());
    if (!status.ok()) {
        return status;
    }
    status = ValidateScore("min_confidence", request.min_confidence());
    if (!status.ok()) {
        return status;
    }
    status = ValidateScore("min_importance", request.min_importance());
    if (!status.ok()) {
        return status;
    }
    uint64_t compressed_time_ms = request.compressed_time_ms();
    if (compressed_time_ms == 0) {
        compressed_time_ms = request.source_end_ms();
    }
    status = ValidateWriteTimestamp(compressed_time_ms);
    if (!status.ok()) {
        return status;
    }

    const uint32_t source_limit = LimitOrDefault(request.max_source_events());
    std::vector<ContextEvent> source_events;
    status = LoadSourceEvents(env, request.tenant_hash(), request.node_hash(),
                              request.source_start_ms(), request.source_end_ms(),
                              std::min<uint32_t>(source_limit + 1, kMaxLimit),
                              &source_events);
    if (!status.ok()) {
        return status;
    }

    std::vector<ContextEvent> selected_events;
    selected_events.reserve(std::min<size_t>(source_events.size(), source_limit));
    bool truncated = false;
    for (const auto& event : source_events) {
        if (!MatchesCompressionSourceFilter(request, event)) {
            continue;
        }
        if (static_cast<uint32_t>(selected_events.size()) >= source_limit) {
            truncated = true;
            break;
        }
        selected_events.push_back(event);
    }
    if (selected_events.empty()) {
        return Status::NotFound("no source events matched compression window");
    }

    ContextCompressionEvent event;
    event.set_compression_id_hash(CompressionIdHash(request.tenant_hash(), request.node_hash(),
                                                    request.source_start_ms(),
                                                    request.source_end_ms(), selected_events));
    event.set_node_hash(request.node_hash());
    event.set_source_start_ms(request.source_start_ms());
    event.set_source_end_ms(request.source_end_ms());
    event.set_compressed_time_ms(compressed_time_ms);
    event.set_summary(BuildCompressionSummary(request.source_start_ms(), request.source_end_ms(),
                                              selected_events, truncated));

    WriteCompressionEventRequest write_request;
    write_request.set_tenant_hash(request.tenant_hash());
    *write_request.mutable_event() = event;
    WriteCompressionEventResponse write_response;
    status = WriteCompressionEvent(env, write_request, &write_response);
    if (!status.ok()) {
        return status;
    }
    response->set_object_key(write_response.object_key());
    *response->mutable_event() = std::move(event);
    response->set_source_event_count(static_cast<uint32_t>(selected_events.size()));
    response->set_truncated_source_events(truncated);
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, COMPRESS_EVENTS, CompressEvents, Write);

Status QueryCompressionEvents(ExecuteEnv* env, const QueryCompressionEventsRequest& request,
                              QueryCompressionEventsResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    status =
        ValidateRequestLimitAndRange(request.limit(), request.start_time_ms(), request.end_time_ms());
    if (!status.ok()) {
        return status;
    }
    if (static_cast<uint32_t>(request.node_hashes_size()) > kMaxFilterValues) {
        return Status::InvalidArgument("too many node_hashes");
    }
    const uint32_t result_limit = LimitOrDefault(request.limit());
    for (uint64_t node_hash : request.node_hashes()) {
        if (node_hash == 0) {
            continue;
        }
        const std::string key = CompressionKey(request.tenant_hash(), node_hash);
        ObjectHandle<model::ContextCompressionModel> object;
        status = env->GetObject(key, &object);
        if (status.IsNotFound()) {
            continue;
        }
        if (!status.ok()) {
            return status;
        }
        std::vector<ContextCompressionEvent> matching_events;
        object->OrSet().Query(
            0, std::numeric_limits<uint64_t>::max(), kMaxLimit,
            [&request, &matching_events](const uint64_t, const std::string& value) {
                ContextCompressionEvent event;
                if (!event.ParseFromString(value)) {
                    return;
                }
                if (event.source_end_ms() >= request.start_time_ms() &&
                    event.source_start_ms() <= request.end_time_ms()) {
                    matching_events.push_back(std::move(event));
                }
            });
        std::sort(matching_events.begin(), matching_events.end(),
                  [](const ContextCompressionEvent& left,
                     const ContextCompressionEvent& right) {
                      if (left.source_end_ms() != right.source_end_ms()) {
                          return left.source_end_ms() > right.source_end_ms();
                      }
                      if (left.compressed_time_ms() != right.compressed_time_ms()) {
                          return left.compressed_time_ms() > right.compressed_time_ms();
                      }
                      return left.compression_id_hash() < right.compression_id_hash();
                  });
        for (const auto& event : matching_events) {
            if (static_cast<uint32_t>(response->events_size()) >= result_limit) {
                break;
            }
            *response->add_events() = event;
        }
        if (static_cast<uint32_t>(response->events_size()) >= result_limit) {
            break;
        }
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_COMPRESSION_EVENTS, QueryCompressionEvents, Read);

Status QueryNodeContext(ExecuteEnv* env, const QueryNodeContextRequest& request,
                        QueryNodeContextResponse* response) {
    Status status = ValidateTenant(request.tenant_hash());
    if (!status.ok()) {
        return status;
    }
    if (request.node_hash() == 0 || request.as_of_ms() == 0) {
        return Status::InvalidArgument("node_hash and as_of_ms are required");
    }
    status = ValidateTimelineTimestamp(request.as_of_ms());
    if (!status.ok()) {
        return status;
    }
    const uint32_t compression_limit = LimitOrDefault(request.compression_limit());
    status = ValidateLimit(request.compression_limit());
    if (!status.ok()) {
        return status;
    }

    GetNodeRequest node_request;
    node_request.set_tenant_hash(request.tenant_hash());
    node_request.set_node_hash(request.node_hash());
    GetNodeResponse node_response;
    status = GetNode(env, node_request, &node_response);
    if (!status.ok()) {
        return status;
    }
    response->set_node_exists(node_response.exist());
    if (node_response.exist()) {
        *response->mutable_node() = std::move(*node_response.mutable_node());
    }

    const uint32_t summary_level = request.summary_level() == 0 ? 1 : request.summary_level();
    ContextSummary summary;
    bool found_summary = false;
    status = LoadLatestSummary(env, request.tenant_hash(), request.node_hash(), summary_level,
                               request.as_of_ms(), &summary, &found_summary);
    if (!status.ok()) {
        return status;
    }
    response->set_overall_summary_exists(found_summary);
    if (found_summary) {
        *response->mutable_overall_summary() = std::move(summary);
    }

    if (request.cold_start_time_ms() == 0 && request.cold_end_time_ms() == 0) {
        return Status::OK();
    }
    status = ValidateRequestLimitAndRange(request.compression_limit(),
                                          request.cold_start_time_ms(),
                                          request.cold_end_time_ms());
    if (!status.ok()) {
        return status;
    }
    QueryCompressionEventsRequest compression_request;
    compression_request.set_tenant_hash(request.tenant_hash());
    compression_request.add_node_hashes(request.node_hash());
    compression_request.set_start_time_ms(request.cold_start_time_ms());
    compression_request.set_end_time_ms(request.cold_end_time_ms());
    compression_request.set_limit(compression_limit);
    QueryCompressionEventsResponse compression_response;
    status = QueryCompressionEvents(env, compression_request, &compression_response);
    if (!status.ok()) {
        return status;
    }
    for (const auto& event : compression_response.events()) {
        *response->add_cold_window_summaries() = event;
    }
    return Status::OK();
}
REGISTER_FUNCTION(CONTEXT, QUERY_NODE_CONTEXT, QueryNodeContext, Read);

}  // namespace context
}  // namespace bcache2
