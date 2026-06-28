#include "client/temporalstore_c_client.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <ctime>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "client/temporalstore_client.h"

struct temporalstore_client {
    std::unique_ptr<bcache2::client::TemporalStoreClient> impl;
};

namespace {

char* CopyCString(const std::string& value) {
    char* out = static_cast<char*>(std::malloc(value.size() + 1));
    if (out == nullptr) {
        return nullptr;
    }
    std::memcpy(out, value.data(), value.size());
    out[value.size()] = '\0';
    return out;
}

int Finish(const bcache2::Status& status, char** error_message) {
    if (error_message != nullptr) {
        *error_message = nullptr;
    }
    if (status.ok()) {
        return 0;
    }
    if (error_message != nullptr) {
        *error_message = CopyCString(status.ToString());
    }
    return status.errorcode();
}

bcache2::Status NullError(const char* name) {
    return bcache2::Status::InvalidArgument(std::string(name) + " is null");
}

bcache2::client::LogLevel ToLogLevel(int level) {
    switch (level) {
    case 0:
        return bcache2::client::LogLevel::kAll;
    case 1:
        return bcache2::client::LogLevel::kDebug;
    case 2:
        return bcache2::client::LogLevel::kInfo;
    case 3:
        return bcache2::client::LogLevel::kWarning;
    case 4:
        return bcache2::client::LogLevel::kError;
    case 5:
        return bcache2::client::LogLevel::kFatal;
    default:
        return bcache2::client::LogLevel::kWarning;
    }
}


std::string JsonStringify(const rapidjson::Value& value) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return std::string(buffer.GetString(), buffer.GetSize());
}

std::string JsonStringMember(const rapidjson::Value& value, const char* name) {
    if (!value.IsObject() || !value.HasMember(name) || !value[name].IsString()) {
        return "";
    }
    return value[name].GetString();
}

uint64_t JsonUintMember(const rapidjson::Value& value, const char* name, uint64_t fallback = 0) {
    if (!value.IsObject() || !value.HasMember(name)) {
        return fallback;
    }
    const auto& member = value[name];
    if (member.IsUint64()) {
        return member.GetUint64();
    }
    if (member.IsInt64() && member.GetInt64() >= 0) {
        return static_cast<uint64_t>(member.GetInt64());
    }
    return fallback;
}

double JsonDoubleMember(const rapidjson::Value& value, const char* name, double fallback = 0.0) {
    if (!value.IsObject() || !value.HasMember(name)) {
        return fallback;
    }
    const auto& member = value[name];
    if (member.IsNumber()) {
        return member.GetDouble();
    }
    return fallback;
}

bool JsonBoolMember(const rapidjson::Value& value, const char* name, bool fallback = false) {
    if (!value.IsObject() || !value.HasMember(name)) {
        return fallback;
    }
    const auto& member = value[name];
    if (member.IsBool()) {
        return member.GetBool();
    }
    return fallback;
}

const rapidjson::Value* JsonObjectMember(const rapidjson::Value& value, const char* name) {
    if (!value.IsObject() || !value.HasMember(name) || !value[name].IsObject()) {
        return nullptr;
    }
    return &value[name];
}


bcache2::Status ReadMatrixArkServingCount(bcache2::client::TemporalStoreClient* impl,
                                          const std::string& count_key,
                                          std::string* count_text) {
    bcache2::Status status = impl->GetString(count_key + ":serving", count_text);
    if (status.ok()) {
        return status;
    }
    return impl->GetString(count_key, count_text);
}

std::unordered_set<std::string> QueryTerms(const std::string& query) {
    std::unordered_set<std::string> terms;
    std::string token;
    for (char ch : query) {
        unsigned char uch = static_cast<unsigned char>(ch);
        if (std::isalnum(uch)) {
            token.push_back(static_cast<char>(std::tolower(uch)));
        } else {
            if (token.size() > 2) {
                terms.insert(token);
            }
            token.clear();
        }
    }
    if (token.size() > 2) {
        terms.insert(token);
    }
    return terms;
}

std::string LowerAscii(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

std::string CandidateText(const rapidjson::Value& record) {
    for (const char* field : {"text", "content", "summary_text", "state", "observation", "entity_value", "description", "value"}) {
        std::string value = JsonStringMember(record, field);
        if (!value.empty()) {
            return value;
        }
    }
    if (const rapidjson::Value* extraction = JsonObjectMember(record, "internal_extraction")) {
        std::string observation = JsonStringMember(*extraction, "observation");
        if (!observation.empty()) {
            return observation;
        }
    }
    return "";
}

uint64_t TokenEstimate(const std::string& text) {
    return std::max<uint64_t>(1, static_cast<uint64_t>((text.size() + 3) / 4));
}

double SparseScore(const std::unordered_set<std::string>& query_terms, const std::string& text) {
    if (query_terms.empty() || text.empty()) {
        return 0.0;
    }
    std::string lower = LowerAscii(text);
    uint64_t matched = 0;
    for (const auto& term : query_terms) {
        if (lower.find(term) != std::string::npos) {
            ++matched;
        }
    }
    return static_cast<double>(matched) / static_cast<double>(std::max<size_t>(1, query_terms.size()));
}

bool ScopeKeyExplicit(const rapidjson::Value& scope, const char* field) {
    if (!scope.IsObject() || !scope.HasMember("_explicit_scope_keys") || !scope["_explicit_scope_keys"].IsArray()) {
        return false;
    }
    for (const auto& item : scope["_explicit_scope_keys"].GetArray()) {
        if (item.IsString() && std::string(item.GetString()) == field) {
            return true;
        }
    }
    return false;
}

std::unordered_map<std::string, uint64_t> ParseScopeKey(const std::string& scope_key) {
    std::unordered_map<std::string, uint64_t> parsed;
    size_t start = 0;
    while (start < scope_key.size()) {
        size_t end = scope_key.find('|', start);
        std::string part = scope_key.substr(start, end == std::string::npos ? std::string::npos : end - start);
        size_t eq = part.find('=');
        if (eq != std::string::npos && eq > 0 && eq + 1 < part.size()) {
            try {
                parsed[part.substr(0, eq)] = static_cast<uint64_t>(std::stoull(part.substr(eq + 1)));
            } catch (...) {
            }
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return parsed;
}

std::string CandidateScopeKey(const rapidjson::Value& record) {
    const rapidjson::Value* record_scope = JsonObjectMember(record, "scope");
    const rapidjson::Value* access_scope = JsonObjectMember(record, "access_scope");
    const rapidjson::Value* metadata = JsonObjectMember(record, "metadata");
    const rapidjson::Value* metadata_access = metadata == nullptr ? nullptr : JsonObjectMember(*metadata, "access_scope");
    const rapidjson::Value* scopes[] = {&record, access_scope, metadata_access, record_scope};
    for (const rapidjson::Value* candidate_scope : scopes) {
        if (candidate_scope == nullptr || !candidate_scope->IsObject()) {
            continue;
        }
        std::string scope_key = JsonStringMember(*candidate_scope, "scope_key");
        if (!scope_key.empty()) {
            return scope_key;
        }
    }
    return "";
}

std::string SessionScopeMode(const rapidjson::Value& query_scope) {
    std::string mode = JsonStringMember(query_scope, "_session_scope");
    if (mode.empty()) {
        mode = JsonStringMember(query_scope, "session_scope");
    }
    if (mode == "only" || mode == "strict") {
        return "only";
    }
    return "prefer";
}

bool ScopeKeyMatchesQuery(const std::string& record_scope_key, const rapidjson::Value& query_scope) {
    if (record_scope_key.empty()) {
        return true;
    }
    auto record_parts = ParseScopeKey(record_scope_key);
    uint64_t tenant_hash = JsonUintMember(query_scope, "tenant_hash", 0);
    if (tenant_hash != 0) {
        auto it = record_parts.find("t");
        if (it == record_parts.end() || it->second != tenant_hash) {
            return false;
        }
    }
    if (ScopeKeyExplicit(query_scope, "user_id")) {
        uint64_t user_hash = JsonUintMember(query_scope, "user_hash", 0);
        if (user_hash != 0) {
            auto it = record_parts.find("u");
            if (it == record_parts.end() || it->second != user_hash) {
                return false;
            }
        }
    }
    if (ScopeKeyExplicit(query_scope, "session_id") && SessionScopeMode(query_scope) == "only") {
        uint64_t session_hash = JsonUintMember(query_scope, "session_hash", 0);
        if (session_hash != 0) {
            auto it = record_parts.find("s");
            if (it == record_parts.end() || it->second != session_hash) {
                return false;
            }
        }
    }
    return true;
}

std::string RecordScopeString(const rapidjson::Value& record, const char* field) {
    const rapidjson::Value* record_scope = JsonObjectMember(record, "scope");
    const rapidjson::Value* access_scope = JsonObjectMember(record, "access_scope");
    const rapidjson::Value* metadata = JsonObjectMember(record, "metadata");
    const rapidjson::Value* metadata_access = metadata == nullptr ? nullptr : JsonObjectMember(*metadata, "access_scope");
    const rapidjson::Value* scopes[] = {&record, access_scope, metadata_access, record_scope};
    for (const rapidjson::Value* candidate_scope : scopes) {
        if (candidate_scope == nullptr || !candidate_scope->IsObject()) {
            continue;
        }
        std::string value = JsonStringMember(*candidate_scope, field);
        if (!value.empty()) {
            return value;
        }
    }
    return "";
}

std::string SessionContinuityStatus(const rapidjson::Value& record, const rapidjson::Value* scope) {
    if (scope == nullptr || !scope->IsObject()) {
        return "unscoped";
    }
    std::string query_session = JsonStringMember(*scope, "session_id");
    if (query_session.empty()) {
        return "unscoped";
    }
    std::string record_session = RecordScopeString(record, "session_id");
    if (!record_session.empty() && record_session == query_session) {
        return "same_session";
    }
    std::string record_scope_key = CandidateScopeKey(record);
    uint64_t query_session_hash = JsonUintMember(*scope, "session_hash", 0);
    if (!record_scope_key.empty() && query_session_hash != 0) {
        auto parts = ParseScopeKey(record_scope_key);
        auto it = parts.find("s");
        if (it != parts.end() && it->second == query_session_hash) {
            return "same_session";
        }
    }
    if (!record_session.empty() || !record_scope_key.empty()) {
        return "cross_session";
    }
    return "unscoped";
}

double ContinuityBoost(const std::string& record_type, const std::string& context_class, const std::string& status) {
    if (status == "same_session") {
        if (record_type == "context_event" || record_type == "context_segment") return 0.16;
        if (record_type == "context_summary") return 0.12;
        if (record_type == "context_entity") return 0.10;
        return 0.08;
    }
    if (status == "cross_session") {
        if (record_type == "context_entity" || context_class == "resource_fact") return 0.11;
        if (record_type == "context_event" || record_type == "context_segment" || record_type == "context_compression_event") return 0.06;
    }
    return 0.0;
}

double CrossSessionRerankBoost(const rapidjson::Value& record, const std::string& record_type,
                               const std::string& context_class, const std::string& status,
                               const std::string& question_type) {
    if (status != "cross_session") {
        return 0.0;
    }
    bool has_citation = record.HasMember("source_ref") || record.HasMember("citation") || record.HasMember("source_chunk_hash");
    if (record_type == "context_entity") {
        return (question_type == "current_state" || question_type == "latest" || question_type == "multi_hop") ? 0.10 : 0.06;
    }
    if (record_type == "resource_chunk" && has_citation) {
        return 0.04;
    }
    if ((record_type == "context_event" || record_type == "context_segment") &&
        (question_type == "multi_hop" || question_type == "why_emotion" || question_type == "fact" || question_type == "evidence")) {
        return 0.01;
    }
    if (record_type == "context_compression_event") {
        return 0.05;
    }
    if (record_type == "context_summary") {
        return question_type == "broad_exploration" ? 0.05 : 0.02;
    }
    if (context_class == "resource_fact" || context_class == "resource_entity_fact") {
        return has_citation ? 0.06 : 0.04;
    }
    return 0.0;
}

double TypePriorityBoost(const std::string& record_type, const std::string& context_class,
                         const std::string& question_type) {
    if (record_type == "skill_section") {
        return question_type == "procedure" || question_type == "evidence" ? 0.42 : 0.34;
    }
    if (record_type == "resource_chunk") {
        return question_type == "evidence" || question_type == "fact" ? 0.20 : 0.12;
    }
    if (context_class == "resource_fact") {
        return 0.18;
    }
    if (record_type == "context_entity") {
        return question_type == "current_state" ? 0.24 : 0.12;
    }
    if (record_type == "context_event" || record_type == "context_segment") {
        return 0.10;
    }
    if (record_type == "context_summary") {
        return question_type == "broad" || question_type == "exploration" ? 0.12 : 0.0;
    }
    return 0.0;
}

std::string CrossSessionKey(const rapidjson::Value& record) {
    std::string session = RecordScopeString(record, "session_id");
    if (!session.empty()) {
        return session;
    }
    std::string scope_key = CandidateScopeKey(record);
    if (!scope_key.empty()) {
        return scope_key;
    }
    uint64_t node = JsonUintMember(record, "node_hash", 0);
    if (node != 0) {
        return std::string("node:") + std::to_string(node);
    }
    return "unknown_cross_session";
}

struct CrossSessionPolicy {
    bool enabled = false;
    double budget_ratio = 0.12;
    uint64_t budget_tokens = 0;
    double max_budget_ratio = 0.20;
    uint64_t max_budget_tokens = 1536;
    uint64_t max_sessions = 3;
    uint64_t max_candidates = 24;
    double min_score = 0.20;
    double raw_evidence_min_score = 0.45;
    uint64_t min_entity_bridge_refs = 2;
    uint64_t parallelism = 4;
};

CrossSessionPolicy ParseCrossSessionPolicy(const rapidjson::Value& request, const rapidjson::Value* scope,
                                           uint64_t remote_budget, const std::string& question_type) {
    CrossSessionPolicy policy;
    bool default_enabled = scope != nullptr && scope->IsObject() && SessionScopeMode(*scope) == "prefer" && remote_budget > 0;
    if (question_type == "current_state" || question_type == "latest" || question_type == "multi_hop" || question_type == "date") {
        policy.budget_ratio = 0.20;
    } else if (question_type == "broad_exploration" || question_type == "evidence") {
        policy.budget_ratio = 0.15;
    } else {
        policy.budget_ratio = 0.12;
    }
    if (const rapidjson::Value* config = JsonObjectMember(request, "cross_session")) {
        policy.enabled = JsonBoolMember(*config, "enabled", default_enabled);
        policy.max_budget_ratio = std::max(0.0, std::min(1.0, JsonDoubleMember(*config, "max_budget_ratio", policy.max_budget_ratio)));
        policy.budget_ratio = std::min(policy.budget_ratio, policy.max_budget_ratio);
        policy.budget_ratio = std::max(0.0, std::min(policy.max_budget_ratio, JsonDoubleMember(*config, "budget_ratio", policy.budget_ratio)));
        policy.max_budget_tokens = JsonUintMember(*config, "max_budget_tokens", policy.max_budget_tokens);
        policy.max_sessions = JsonUintMember(*config, "max_sessions", policy.max_sessions);
        policy.max_candidates = JsonUintMember(*config, "max_candidates", policy.max_candidates);
        policy.min_score = std::max(0.0, std::min(1.0, JsonDoubleMember(*config, "min_score", policy.min_score)));
        policy.raw_evidence_min_score = std::max(0.0, std::min(1.0, JsonDoubleMember(*config, "raw_evidence_min_score", policy.raw_evidence_min_score)));
        policy.min_entity_bridge_refs = JsonUintMember(*config, "min_entity_bridge_refs", policy.min_entity_bridge_refs);
        policy.parallelism = std::max<uint64_t>(1, JsonUintMember(*config, "parallelism", policy.parallelism));
        uint64_t computed = static_cast<uint64_t>(static_cast<double>(remote_budget) * policy.budget_ratio);
        if (remote_budget >= 1200 && computed > 0) {
            computed = std::max<uint64_t>(256, computed);
        }
        policy.budget_tokens = JsonUintMember(*config, "budget_tokens", computed);
    } else {
        policy.enabled = default_enabled;
        policy.budget_tokens = static_cast<uint64_t>(static_cast<double>(remote_budget) * policy.budget_ratio);
        if (remote_budget >= 1200 && policy.budget_tokens > 0) {
            policy.budget_tokens = std::max<uint64_t>(256, policy.budget_tokens);
        }
    }
    if (!default_enabled) {
        policy.enabled = false;
    }
    if (!policy.enabled) {
        policy.budget_tokens = 0;
        policy.max_sessions = 0;
        policy.max_candidates = 0;
        policy.min_score = 0.0;
        policy.raw_evidence_min_score = 0.0;
        policy.min_entity_bridge_refs = 0;
        policy.parallelism = 0;
    } else {
        uint64_t cap = policy.max_budget_tokens == 0 ? remote_budget : policy.max_budget_tokens;
        uint64_t ratio_cap = policy.max_budget_ratio > 0.0 ? static_cast<uint64_t>(static_cast<double>(remote_budget) * policy.max_budget_ratio) : remote_budget;
        if (ratio_cap == 0 && remote_budget > 0 && policy.max_budget_ratio > 0.0) ratio_cap = 1;
        policy.budget_tokens = std::min(remote_budget, std::min(policy.budget_tokens, std::min(cap, ratio_cap)));
    }
    return policy;
}

bool ScopeMatches(const rapidjson::Value& record, const rapidjson::Value* scope) {
    if (scope == nullptr || !scope->IsObject()) {
        return true;
    }
    std::string record_scope_key = CandidateScopeKey(record);
    if (!ScopeKeyMatchesQuery(record_scope_key, *scope)) {
        return false;
    }
    const rapidjson::Value* record_scope = JsonObjectMember(record, "scope");
    const rapidjson::Value* access_scope = JsonObjectMember(record, "access_scope");
    const rapidjson::Value* metadata = JsonObjectMember(record, "metadata");
    const rapidjson::Value* metadata_access = metadata == nullptr ? nullptr : JsonObjectMember(*metadata, "access_scope");
    const rapidjson::Value* scopes[] = {&record, access_scope, metadata_access, record_scope};
    for (const char* field : {"scope_key", "account_id", "tenant_id", "user_id", "session_id", "team", "project", "agent_name"}) {
        if (!scope->HasMember(field) || !(*scope)[field].IsString()) {
            continue;
        }
        std::string field_name(field);
        if (field_name == "scope_key") {
            continue;
        }
        if ((field_name == "account_id" || field_name == "tenant_id" || field_name == "user_id" || field_name == "session_id") && !ScopeKeyExplicit(*scope, field)) {
            continue;
        }
        if (field_name == "session_id" && SessionScopeMode(*scope) == "prefer") {
            continue;
        }
        if ((field_name == "team" || field_name == "project" || field_name == "agent_name") && !ScopeKeyExplicit(*scope, field)) {
            continue;
        }
        std::string expected = (*scope)[field].GetString();
        if (expected.empty()) {
            continue;
        }
        std::string actual;
        for (const rapidjson::Value* candidate_scope : scopes) {
            if (candidate_scope == nullptr || !candidate_scope->IsObject()) {
                continue;
            }
            actual = JsonStringMember(*candidate_scope, field);
            if (!actual.empty()) {
                break;
            }
        }
        if (!actual.empty() && actual != expected) {
            return false;
        }
    }
    return true;
}

std::string RefHash(const rapidjson::Value& record) {
    for (const char* field : {"ref_hash", "chunk_hash", "section_hash", "skill_hash", "event_id_hash", "entity_hash", "summary_hash"}) {
        if (record.IsObject() && record.HasMember(field)) {
            const auto& value = record[field];
            if (value.IsUint64()) {
                return std::to_string(value.GetUint64());
            }
            if (value.IsInt64()) {
                return std::to_string(value.GetInt64());
            }
            if (value.IsString()) {
                return value.GetString();
            }
        }
    }
    return "";
}

uint64_t NodeHash(const rapidjson::Value& record) {
    return JsonUintMember(record, "node_hash", 0);
}

std::string BatchHash(const rapidjson::Value& record) {
    uint64_t batch = JsonUintMember(record, "batch_id_hash", 0);
    return batch == 0 ? "" : std::to_string(batch);
}

std::vector<std::vector<std::string>> SecondaryGroups(const rapidjson::Value& request) {
    std::vector<std::vector<std::string>> groups;
    if (!request.IsObject() || !request.HasMember("secondary_index_groups") || !request["secondary_index_groups"].IsArray()) {
        return groups;
    }
    for (const auto& group : request["secondary_index_groups"].GetArray()) {
        std::vector<std::string> values;
        if (group.IsArray()) {
            for (const auto& item : group.GetArray()) {
                if (item.IsString() && item.GetStringLength() > 0) {
                    values.emplace_back(item.GetString());
                }
            }
        }
        if (!values.empty()) {
            groups.push_back(std::move(values));
        }
    }
    return groups;
}

bool HasGroupMatch(const std::unordered_set<std::string>& terms, const std::vector<std::vector<std::string>>& groups) {
    if (groups.empty() || terms.empty()) {
        return true;
    }
    for (const auto& group : groups) {
        for (const auto& term : group) {
            if (terms.find(term) != terms.end()) {
                return true;
            }
        }
    }
    return false;
}

std::string ContextClassName(const rapidjson::Value& record) {
    std::string record_type = JsonStringMember(record, "record_type");
    if (record_type == "context_event") {
        std::string classification = JsonStringMember(record, "classification");
        std::string event_type = JsonStringMember(record, "event_type");
        if (classification == "resource_fact" || event_type.find("resource_") == 0) {
            return "resource_fact";
        }
        return "event";
    }
    if (record_type == "context_entity") {
        return "entity";
    }
    if (record_type == "context_segment") {
        return "segment";
    }
    if (record_type == "context_summary") {
        return "summary";
    }
    if (record_type == "context_compression_event") {
        return "compression";
    }
    return record_type;
}

void DecodeMatrixArkPayload(const std::string& value, std::vector<std::string>* records) {
    rapidjson::Document doc;
    if (doc.Parse(value.c_str()).HasParseError()) {
        return;
    }
    if (doc.IsObject() && doc.HasMember("record_bundle") && doc["record_bundle"].IsArray()) {
        for (const auto& item : doc["record_bundle"].GetArray()) {
            if (item.IsObject()) {
                records->push_back(JsonStringify(item));
            }
        }
        return;
    }
    if (doc.IsObject()) {
        records->push_back(JsonStringify(doc));
    }
}

bcache2::Status MatrixArkScanCandidatesNative(
    bcache2::client::TemporalStoreClient* impl, const std::string& count_key,
    const std::string& record_hash_key, size_t shard_size, const std::string& request_json,
    std::string* output_json) {
    if (impl == nullptr) {
        return NullError("client");
    }
    if (output_json == nullptr) {
        return NullError("candidates_json");
    }
    if (count_key.empty()) {
        return bcache2::Status::InvalidArgument("count_key is empty");
    }
    if (record_hash_key.empty()) {
        return bcache2::Status::InvalidArgument("record_hash_key is empty");
    }
    if (shard_size == 0) {
        shard_size = 1024;
    }
    rapidjson::Document request;
    if (request.Parse(request_json.c_str()).HasParseError() || !request.IsObject()) {
        return bcache2::Status::InvalidArgument("request_json must be a JSON object");
    }
    std::string count_text;
    bcache2::Status status = ReadMatrixArkServingCount(impl, count_key, &count_text);
    if (!status.ok()) {
        return status;
    }
    uint64_t count = 0;
    try {
        count = static_cast<uint64_t>(std::stoull(count_text));
    } catch (...) {
        count = 0;
    }
    std::unordered_set<std::string> allowed_types;
    if (request.HasMember("record_types") && request["record_types"].IsArray()) {
        for (const auto& item : request["record_types"].GetArray()) {
            if (item.IsString()) {
                allowed_types.insert(item.GetString());
            }
        }
    }
    if (allowed_types.empty()) {
        allowed_types = {"context_compression_event", "context_entity", "context_event", "context_segment", "context_summary", "resource_chunk", "skill_section", "context_index"};
    }
    const rapidjson::Value* scope = JsonObjectMember(request, "scope");
    auto secondary_groups = SecondaryGroups(request);
    std::vector<std::string> record_jsons;
    uint64_t scanned_records = 0;
    uint64_t dropped_by_type = 0;
    uint64_t dropped_by_scope = 0;
    uint64_t max_shard = count == 0 ? 0 : (count - 1) / shard_size;
    for (uint64_t shard = 0; shard <= max_shard; ++shard) {
        char suffix[32];
        std::snprintf(suffix, sizeof(suffix), ":%06llu", static_cast<unsigned long long>(shard));
        std::vector<std::pair<std::string, std::string>> fields;
        status = impl->HGetAll(record_hash_key + suffix, &fields);
        if (!status.ok()) {
            return status;
        }
        for (const auto& pair : fields) {
            std::vector<std::string> decoded;
            DecodeMatrixArkPayload(pair.second, &decoded);
            for (const auto& record_json : decoded) {
                rapidjson::Document record;
                if (record.Parse(record_json.c_str()).HasParseError() || !record.IsObject()) {
                    continue;
                }
                ++scanned_records;
                std::string record_type = JsonStringMember(record, "record_type");
                if (!allowed_types.empty() && allowed_types.find(record_type) == allowed_types.end()) {
                    ++dropped_by_type;
                    continue;
                }
                if (!ScopeMatches(record, scope)) {
                    ++dropped_by_scope;
                    continue;
                }
                record_jsons.push_back(record_json);
            }
        }
    }
    std::unordered_map<std::string, std::unordered_set<std::string>> terms_by_ref;
    std::unordered_map<uint64_t, std::unordered_set<std::string>> terms_by_node;
    std::unordered_map<std::string, std::unordered_set<std::string>> terms_by_batch;
    for (const auto& record_json : record_jsons) {
        rapidjson::Document record;
        if (record.Parse(record_json.c_str()).HasParseError() || !record.IsObject()) {
            continue;
        }
        if (JsonStringMember(record, "record_type") != "context_index") {
            continue;
        }
        std::string term = JsonStringMember(record, "index_name");
        if (term.empty()) {
            continue;
        }
        std::string ref = RefHash(record);
        if (!ref.empty()) {
            terms_by_ref[ref].insert(term);
        } else if (NodeHash(record) != 0) {
            terms_by_node[NodeHash(record)].insert(term);
        }
        std::string batch = BatchHash(record);
        if (!batch.empty()) {
            terms_by_batch[batch].insert(term);
        }
    }
    uint64_t secondary_dropped = 0;
    uint64_t secondary_matched = 0;
    rapidjson::Document out;
    out.SetObject();
    auto& alloc = out.GetAllocator();
    rapidjson::Value records(rapidjson::kArrayType);
    for (const auto& record_json : record_jsons) {
        rapidjson::Document record;
        if (record.Parse(record_json.c_str()).HasParseError() || !record.IsObject()) {
            continue;
        }
        std::unordered_set<std::string> terms;
        std::string ref = RefHash(record);
        if (!ref.empty() && terms_by_ref.count(ref)) {
            terms.insert(terms_by_ref[ref].begin(), terms_by_ref[ref].end());
        }
        uint64_t node = NodeHash(record);
        if (node != 0 && terms_by_node.count(node)) {
            terms.insert(terms_by_node[node].begin(), terms_by_node[node].end());
        }
        std::string batch = BatchHash(record);
        if (!batch.empty() && terms_by_batch.count(batch)) {
            terms.insert(terms_by_batch[batch].begin(), terms_by_batch[batch].end());
        }
        if (!secondary_groups.empty() && !terms.empty() && !HasGroupMatch(terms, secondary_groups)) {
            ++secondary_dropped;
            continue;
        }
        if (!terms.empty()) {
            ++secondary_matched;
        }
        rapidjson::Value copied;
        copied.CopyFrom(record, alloc);
        records.PushBack(copied, alloc);
    }
    out.AddMember("ok", true, alloc);
    out.AddMember("native_candidate_prefilter", true, alloc);
    out.AddMember("count", records.Size(), alloc);
    out.AddMember("records", records, alloc);
    rapidjson::Value stats(rapidjson::kObjectType);
    stats.AddMember("execution_mode", "cpp_direct_native_candidate_prefilter", alloc);
    stats.AddMember("native_prefix_scan", true, alloc);
    stats.AddMember("native_secondary_index_prefilter", !secondary_groups.empty(), alloc);
    stats.AddMember("native_pack_assembly", false, alloc);
    stats.AddMember("pack_assembly_location", "caller_or_context_pack_api", alloc);
    stats.AddMember("scanned_records", scanned_records, alloc);
    stats.AddMember("returned_records", records.Size(), alloc);
    stats.AddMember("dropped_by_type", dropped_by_type, alloc);
    stats.AddMember("dropped_by_scope", dropped_by_scope, alloc);
    stats.AddMember("secondary_index_groups_supplied", static_cast<uint64_t>(secondary_groups.size()), alloc);
    stats.AddMember("secondary_index_matched_candidate_count", secondary_matched, alloc);
    stats.AddMember("secondary_index_dropped_candidate_count", secondary_dropped, alloc);
    out.AddMember("scan_stats", stats, alloc);
    output_json->assign(JsonStringify(out));
    return bcache2::Status::OK();
}

bcache2::Status MatrixArkRetrieveContextPackNative(
    bcache2::client::TemporalStoreClient* impl, const std::string& count_key,
    const std::string& record_hash_key, size_t shard_size, const std::string& request_json,
    std::string* output_json) {
    if (impl == nullptr) {
        return NullError("client");
    }
    if (output_json == nullptr) {
        return NullError("context_pack_json");
    }
    if (count_key.empty()) {
        return bcache2::Status::InvalidArgument("count_key is empty");
    }
    if (record_hash_key.empty()) {
        return bcache2::Status::InvalidArgument("record_hash_key is empty");
    }
    if (shard_size == 0) {
        shard_size = 1024;
    }
    rapidjson::Document request;
    if (request.Parse(request_json.c_str()).HasParseError() || !request.IsObject()) {
        return bcache2::Status::InvalidArgument("request_json must be a JSON object");
    }
    std::string count_text;
    bcache2::Status status = ReadMatrixArkServingCount(impl, count_key, &count_text);
    if (!status.ok()) {
        return status;
    }
    uint64_t count = 0;
    try {
        count = static_cast<uint64_t>(std::stoull(count_text));
    } catch (...) {
        count = 0;
    }
    std::vector<std::string> allowed_types;
    if (request.HasMember("record_types") && request["record_types"].IsArray()) {
        for (const auto& item : request["record_types"].GetArray()) {
            if (item.IsString()) {
                allowed_types.emplace_back(item.GetString());
            }
        }
    }
    if (allowed_types.empty()) {
        allowed_types = {"context_compression_event", "context_entity", "context_event", "context_segment", "context_summary", "resource_chunk", "skill_section"};
    }
    std::unordered_set<std::string> scan_allowed(allowed_types.begin(), allowed_types.end());
    scan_allowed.insert("context_index");
    std::unordered_set<std::string> allowed(allowed_types.begin(), allowed_types.end());
    const rapidjson::Value* scope = JsonObjectMember(request, "scope");
    auto secondary_groups = SecondaryGroups(request);
    std::vector<std::string> record_jsons;
    uint64_t scanned_records = 0;
    uint64_t dropped_by_type = 0;
    uint64_t dropped_by_scope = 0;
    uint64_t max_shard = count == 0 ? 0 : (count - 1) / shard_size;
    for (uint64_t shard = 0; shard <= max_shard; ++shard) {
        char suffix[32];
        std::snprintf(suffix, sizeof(suffix), ":%06llu", static_cast<unsigned long long>(shard));
        std::vector<std::pair<std::string, std::string>> fields;
        status = impl->HGetAll(record_hash_key + suffix, &fields);
        if (!status.ok()) {
            return status;
        }
        for (const auto& pair : fields) {
            std::vector<std::string> decoded;
            DecodeMatrixArkPayload(pair.second, &decoded);
            for (const auto& record_json : decoded) {
                rapidjson::Document record;
                if (record.Parse(record_json.c_str()).HasParseError() || !record.IsObject()) {
                    continue;
                }
                ++scanned_records;
                std::string record_type = JsonStringMember(record, "record_type");
                if (!scan_allowed.empty() && scan_allowed.find(record_type) == scan_allowed.end()) {
                    ++dropped_by_type;
                    continue;
                }
                if (!ScopeMatches(record, scope)) {
                    ++dropped_by_scope;
                    continue;
                }
                record_jsons.push_back(record_json);
            }
        }
    }
    std::unordered_map<std::string, std::unordered_set<std::string>> terms_by_ref;
    std::unordered_map<uint64_t, std::unordered_set<std::string>> terms_by_node;
    std::unordered_map<std::string, std::unordered_set<std::string>> terms_by_batch;
    for (const auto& record_json : record_jsons) {
        rapidjson::Document record;
        if (record.Parse(record_json.c_str()).HasParseError() || !record.IsObject()) {
            continue;
        }
        if (JsonStringMember(record, "record_type") != "context_index") {
            continue;
        }
        std::string term = JsonStringMember(record, "index_name");
        if (term.empty()) {
            continue;
        }
        std::string ref = RefHash(record);
        if (!ref.empty()) {
            terms_by_ref[ref].insert(term);
        } else if (NodeHash(record) != 0) {
            terms_by_node[NodeHash(record)].insert(term);
        }
        std::string batch = BatchHash(record);
        if (!batch.empty()) {
            terms_by_batch[batch].insert(term);
        }
    }
    std::string query = JsonStringMember(request, "query");
    auto query_terms = QueryTerms(query);
    std::string question_type = JsonStringMember(request, "question_type");
    if (question_type.empty()) {
        question_type = "fact";
    }
    uint64_t remote_budget = JsonUintMember(request, "max_context_tokens", 4000);
    if (const rapidjson::Value* local_budget = JsonObjectMember(request, "local_budget")) {
        remote_budget = JsonUintMember(*local_budget, "remote_budget_tokens", remote_budget);
    }
    CrossSessionPolicy cross_policy = ParseCrossSessionPolicy(request, scope, remote_budget, question_type);
    uint64_t max_refs = 24;
    uint64_t max_global_candidates = 512;
    double min_similarity_score = 0.20;
    std::string budget_fill_policy = "quality_first";
    if (const rapidjson::Value* ranking = JsonObjectMember(request, "ranking")) {
        max_refs = std::max<uint64_t>(1, JsonUintMember(*ranking, "max_selected_refs", max_refs));
        max_global_candidates = std::max<uint64_t>(1, JsonUintMember(*ranking, "max_global_candidates", max_global_candidates));
        min_similarity_score = std::max(0.0, std::min(1.0, JsonDoubleMember(*ranking, "min_similarity_score", min_similarity_score)));
        if (const rapidjson::Value* policy = JsonObjectMember(*ranking, "budget_fill_policy")) {
            if (policy->IsString()) {
                budget_fill_policy = policy->GetString();
            }
        }
        if (budget_fill_policy != "quality_first" && budget_fill_policy != "force_fill") {
            budget_fill_policy = "quality_first";
        }
    }
    struct ScoredRecord {
        double score;
        uint64_t tokens;
        std::string record_type;
        std::string context_class;
        std::string text;
        uint64_t node_hash;
        std::string cross_session_key;
        std::string source_ref_json;
        std::string session_continuity;
        double continuity_boost;
        double cross_session_rerank_boost;
    };
    std::vector<ScoredRecord> scored;
    uint64_t secondary_dropped = 0;
    uint64_t secondary_matched = 0;
    for (const auto& record_json : record_jsons) {
        rapidjson::Document record;
        if (record.Parse(record_json.c_str()).HasParseError() || !record.IsObject()) {
            continue;
        }
        std::string record_type = JsonStringMember(record, "record_type");
        if (record_type == "context_index" || allowed.find(record_type) == allowed.end()) {
            continue;
        }
        std::unordered_set<std::string> terms;
        std::string ref = RefHash(record);
        if (!ref.empty() && terms_by_ref.count(ref)) {
            terms.insert(terms_by_ref[ref].begin(), terms_by_ref[ref].end());
        }
        uint64_t node = NodeHash(record);
        if (node != 0 && terms_by_node.count(node)) {
            terms.insert(terms_by_node[node].begin(), terms_by_node[node].end());
        }
        std::string batch = BatchHash(record);
        if (!batch.empty() && terms_by_batch.count(batch)) {
            terms.insert(terms_by_batch[batch].begin(), terms_by_batch[batch].end());
        }
        if (!secondary_groups.empty() && !terms.empty() && !HasGroupMatch(terms, secondary_groups)) {
            ++secondary_dropped;
            continue;
        }
        if (!terms.empty()) {
            ++secondary_matched;
        }
        std::string text = CandidateText(record);
        if (text.empty()) {
            continue;
        }
        double score = SparseScore(query_terms, text);
        if (record_type == "context_entity") {
            score += 0.08;
        } else if (record_type == "context_compression_event") {
            score += 0.06;
        }
        std::string continuity = SessionContinuityStatus(record, scope);
        std::string context_class = ContextClassName(record);
        double continuity_boost = ContinuityBoost(record_type, context_class, continuity);
        score += continuity_boost;
        double cross_session_rerank_boost = CrossSessionRerankBoost(record, record_type, context_class, continuity, question_type);
        score += cross_session_rerank_boost;
        score += TypePriorityBoost(record_type, context_class, JsonStringMember(request, "question_type"));
        if (score >= min_similarity_score) {
            std::string source_ref_json;
            if (record.HasMember("source_ref")) {
                source_ref_json = JsonStringify(record["source_ref"]);
            }
            scored.push_back({score,
                              TokenEstimate(text),
                              record_type,
                              context_class,
                              text,
                              NodeHash(record),
                              CrossSessionKey(record),
                              source_ref_json,
                              continuity,
                              continuity_boost,
                              cross_session_rerank_boost});
        }
    }
    std::sort(scored.begin(), scored.end(), [](const auto& a, const auto& b) { return a.score > b.score; });
    if (scored.size() > max_global_candidates) {
        scored.resize(max_global_candidates);
    }

    rapidjson::Document out;
    out.SetObject();
    auto& alloc = out.GetAllocator();
    out.AddMember("ok", true, alloc);
    out.AddMember("native_pack_assembly", true, alloc);
    out.AddMember("raw_records_returned", false, alloc);
    out.AddMember("python_hot_path_records", 0, alloc);
    rapidjson::Value pack(rapidjson::kObjectType);
    std::string pack_id = "cpp-native-" + std::to_string(static_cast<unsigned long long>(std::time(nullptr))) + "-" + std::to_string(scored.size());
    pack.AddMember("context_pack_id", rapidjson::Value(pack_id.c_str(), alloc), alloc);
    pack.AddMember("query", rapidjson::Value(query.c_str(), alloc), alloc);
    pack.AddMember("question_type", rapidjson::Value(question_type.c_str(), alloc), alloc);
    rapidjson::Value selected(rapidjson::kArrayType);
    std::unordered_map<std::string, uint64_t> selected_counts;
    std::unordered_set<uint64_t> selected_nodes;
    uint64_t used_tokens = 0;
    uint64_t dropped_over_budget = 0;
    uint64_t dropped_cross_budget = 0;
    uint64_t dropped_cross_session_cap = 0;
    uint64_t dropped_cross_candidate_cap = 0;
    uint64_t dropped_low_score = 0;
    uint64_t cross_used_tokens = 0;
    uint64_t cross_selected_refs = 0;
    uint64_t entity_bridge_selected_refs = 0;
    std::unordered_set<std::string> selected_cross_sessions;
    for (const auto& item : scored) {
        if (selected.Size() >= max_refs) {
            break;
        }
        if (used_tokens + item.tokens > remote_budget) {
            ++dropped_over_budget;
            continue;
        }
        const std::string& record_type = item.record_type;
        const std::string& context_class = item.context_class;
        bool is_cross_session = item.session_continuity == "cross_session";
        bool is_entity_bridge = is_cross_session && context_class == "entity";
        bool is_cross_session_raw_evidence = is_cross_session && (record_type == "context_event" || record_type == "context_segment");
        std::string cross_key = is_cross_session ? item.cross_session_key : "";
        if (is_cross_session && !cross_policy.enabled) {
            ++dropped_cross_budget;
            continue;
        }
        if (is_cross_session && cross_policy.min_score > 0.0 && item.score < cross_policy.min_score) {
            ++dropped_low_score;
            continue;
        }
        if (is_cross_session_raw_evidence && cross_policy.raw_evidence_min_score > 0.0 && item.score < cross_policy.raw_evidence_min_score) {
            ++dropped_low_score;
            continue;
        }
        if (is_cross_session && cross_policy.max_candidates > 0 && cross_selected_refs >= cross_policy.max_candidates) {
            ++dropped_cross_candidate_cap;
            continue;
        }
        if (is_cross_session && cross_policy.max_sessions > 0 && selected_cross_sessions.find(cross_key) == selected_cross_sessions.end() && selected_cross_sessions.size() >= cross_policy.max_sessions) {
            ++dropped_cross_session_cap;
            continue;
        }
        if (is_cross_session && cross_policy.budget_tokens > 0 && cross_used_tokens + item.tokens > cross_policy.budget_tokens && !(is_entity_bridge && entity_bridge_selected_refs < cross_policy.min_entity_bridge_refs)) {
            ++dropped_cross_budget;
            continue;
        }
        used_tokens += item.tokens;
        if (is_cross_session) {
            cross_used_tokens += item.tokens;
            ++cross_selected_refs;
            selected_cross_sessions.insert(cross_key);
            if (is_entity_bridge) {
                ++entity_bridge_selected_refs;
            }
        }
        rapidjson::Value ref(rapidjson::kObjectType);
        selected_counts[context_class] += 1;
        if (item.node_hash != 0) {
            selected_nodes.insert(item.node_hash);
        }
        ref.AddMember("ref_type", rapidjson::Value(context_class.c_str(), alloc), alloc);
        ref.AddMember("text", rapidjson::Value(item.text.c_str(), alloc), alloc);
        ref.AddMember("token_estimate", item.tokens, alloc);
        ref.AddMember("score", item.score, alloc);
        ref.AddMember("session_continuity", rapidjson::Value(item.session_continuity.c_str(), alloc), alloc);
        ref.AddMember("continuity_boost", item.continuity_boost, alloc);
        ref.AddMember("cross_session_rerank_boost", item.cross_session_rerank_boost, alloc);
        const char* continuity_reason = item.session_continuity == "same_session" ? "same-session continuity" : (item.session_continuity == "cross_session" ? "cross-session memory bridge" : "session-neutral context");
        ref.AddMember("continuity_reason", rapidjson::Value(continuity_reason, alloc), alloc);
        ref.AddMember("selection_reason", "native_cpp_score_pack", alloc);
        if (!item.source_ref_json.empty()) {
            rapidjson::Document source_ref;
            if (!source_ref.Parse(item.source_ref_json.c_str()).HasParseError()) {
                rapidjson::Value copied;
                copied.CopyFrom(source_ref, alloc);
                ref.AddMember("source_ref", copied, alloc);
            }
        }
        selected.PushBack(ref, alloc);
    }
    pack.AddMember("selected_refs", selected, alloc);
    rapidjson::Value remote_refs;
    remote_refs.CopyFrom(pack["selected_refs"], alloc);
    pack.AddMember("remote_context_refs", remote_refs, alloc);
    rapidjson::Value count_obj(rapidjson::kObjectType);
    for (const auto& item : selected_counts) {
        rapidjson::Value key;
        key.SetString(item.first.c_str(), static_cast<rapidjson::SizeType>(item.first.size()), alloc);
        rapidjson::Value value;
        value.SetUint64(item.second);
        count_obj.AddMember(key, value, alloc);
    }
    pack.AddMember("selected_ref_counts", count_obj, alloc);
    rapidjson::Value dropped(rapidjson::kObjectType);
    dropped.AddMember("over_budget", dropped_over_budget, alloc);
    dropped.AddMember("cross_session_budget", dropped_cross_budget, alloc);
    dropped.AddMember("cross_session_session_cap", dropped_cross_session_cap, alloc);
    dropped.AddMember("cross_session_candidate_cap", dropped_cross_candidate_cap, alloc);
    dropped.AddMember("low_score", dropped_low_score, alloc);
    rapidjson::Value reasons(rapidjson::kObjectType);
    reasons.AddMember("over_budget", dropped_over_budget, alloc);
    reasons.AddMember("cross_session_budget", dropped_cross_budget, alloc);
    reasons.AddMember("cross_session_session_cap", dropped_cross_session_cap, alloc);
    reasons.AddMember("cross_session_candidate_cap", dropped_cross_candidate_cap, alloc);
    reasons.AddMember("low_score", dropped_low_score, alloc);
    dropped.AddMember("reason_counts", reasons, alloc);
    pack.AddMember("dropped_refs", dropped, alloc);
    pack.AddMember("used_context_tokens", used_tokens, alloc);
    pack.AddMember("used_remote_context_tokens", used_tokens, alloc);
    pack.AddMember("remote_context_budget_tokens", remote_budget, alloc);
    pack.AddMember("requested_max_context_tokens", JsonUintMember(request, "max_context_tokens", remote_budget), alloc);
    pack.AddMember("packing_policy", "native_cpp_question_type_aware", alloc);
    pack.AddMember("context_pack_assembly", "native_cpp_direct", alloc);
    rapidjson::Value order(rapidjson::kArrayType);
    for (const char* source : {"entities", "events", "segments", "resources", "skills", "summaries"}) {
        order.PushBack(rapidjson::Value(source, alloc), alloc);
    }
    pack.AddMember("context_sources_order", order, alloc);
    rapidjson::Value recall(rapidjson::kObjectType);
    rapidjson::Value native(rapidjson::kObjectType);
    native.AddMember("enabled", true, alloc);
    native.AddMember("backend", "cpp_direct", alloc);
    native.AddMember("scan_filter_score_pack", true, alloc);
    recall.AddMember("native_context_pack", native, alloc);
    rapidjson::Value contract(rapidjson::kObjectType);
    contract.AddMember("raw_records_returned_to_python", false, alloc);
    contract.AddMember("python_hot_path_records", 0, alloc);
    contract.AddMember("python_role", "dispatch_request_receive_context_pack", alloc);
    contract.AddMember("backend_role", "scan_filter_score_pack", alloc);
    recall.AddMember("native_response_contract", contract, alloc);
    rapidjson::Value scan_stats(rapidjson::kObjectType);
    scan_stats.AddMember("backend", "temporalstore-direct", alloc);
    scan_stats.AddMember("execution_mode", "cpp_direct_native_context_pack", alloc);
    scan_stats.AddMember("native_prefix_scan", true, alloc);
    scan_stats.AddMember("native_secondary_index_prefilter", true, alloc);
    scan_stats.AddMember("native_pack_assembly", true, alloc);
    scan_stats.AddMember("scanned_records", scanned_records, alloc);
    scan_stats.AddMember("returned_records", static_cast<uint64_t>(scored.size()), alloc);
    scan_stats.AddMember("dropped_by_type", dropped_by_type, alloc);
    scan_stats.AddMember("dropped_by_scope", dropped_by_scope, alloc);
    scan_stats.AddMember("secondary_index_dropped_candidate_count", secondary_dropped, alloc);
    scan_stats.AddMember("secondary_index_matched_candidate_count", secondary_matched, alloc);
    recall.AddMember("scan_stats", scan_stats, alloc);
    rapidjson::Value rerank(rapidjson::kObjectType);
    rerank.AddMember("enabled", true, alloc);
    rerank.AddMember("mode", "native_weighted_recall_plus_cross_session_rerank", alloc);
    rerank.AddMember("cross_session_rerank_enabled", true, alloc);
    rapidjson::Value cross_signals(rapidjson::kArrayType);
    cross_signals.PushBack("entity_state", alloc);
    cross_signals.PushBack("resource_fact_citation", alloc);
    cross_signals.PushBack("answer_event", alloc);
    cross_signals.PushBack("compression", alloc);
    cross_signals.PushBack("summary_demotion", alloc);
    rerank.AddMember("cross_session_signals", cross_signals, alloc);
    rerank.AddMember("heavy_rerank_enabled", false, alloc);
    recall.AddMember("rerank", rerank, alloc);
    rapidjson::Value ranking_policy(rapidjson::kObjectType);
    ranking_policy.AddMember("min_similarity_score", min_similarity_score, alloc);
    ranking_policy.AddMember("max_global_candidates", max_global_candidates, alloc);
    ranking_policy.AddMember("max_selected_refs", max_refs, alloc);
    ranking_policy.AddMember("budget_fill_policy", rapidjson::Value(budget_fill_policy.c_str(), alloc), alloc);
    ranking_policy.AddMember("quality_first_budget_underfill_allowed", budget_fill_policy == "quality_first", alloc);
    recall.AddMember("ranking", ranking_policy, alloc);
    rapidjson::Value session_policy(rapidjson::kObjectType);
    session_policy.AddMember("mode", scope == nullptr ? rapidjson::Value("unscoped", alloc) : rapidjson::Value(SessionScopeMode(*scope).c_str(), alloc), alloc);
    session_policy.AddMember("policy", "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope", alloc);
    session_policy.AddMember("same_session_selected_ref_count", static_cast<uint64_t>(std::count_if(selected.Begin(), selected.End(), [](const rapidjson::Value& item) { return JsonStringMember(item, "session_continuity") == "same_session"; })), alloc);
    session_policy.AddMember("cross_session_selected_ref_count", cross_selected_refs, alloc);
    session_policy.AddMember("entity_bridge_selected_ref_count", entity_bridge_selected_refs, alloc);
    recall.AddMember("session_continuity", session_policy, alloc);
    rapidjson::Value cross(rapidjson::kObjectType);
    cross.AddMember("enabled", cross_policy.enabled, alloc);
    cross.AddMember("mode", rapidjson::Value(cross_policy.enabled ? "prefer" : "disabled", alloc), alloc);
    cross.AddMember("budget_ratio", cross_policy.budget_ratio, alloc);
    cross.AddMember("max_budget_ratio", cross_policy.max_budget_ratio, alloc);
    cross.AddMember("budget_tokens", cross_policy.budget_tokens, alloc);
    cross.AddMember("remote_budget_tokens", remote_budget, alloc);
    cross.AddMember("max_budget_tokens", cross_policy.max_budget_tokens, alloc);
    cross.AddMember("max_sessions", cross_policy.max_sessions, alloc);
    cross.AddMember("max_candidates", cross_policy.max_candidates, alloc);
    cross.AddMember("min_score", cross_policy.min_score, alloc);
    cross.AddMember("raw_evidence_min_score", cross_policy.raw_evidence_min_score, alloc);
    cross.AddMember("parallelism", cross_policy.parallelism, alloc);
    cross.AddMember("selected_tokens", cross_used_tokens, alloc);
    cross.AddMember("selected_ref_count", cross_selected_refs, alloc);
    cross.AddMember("selected_session_count", static_cast<uint64_t>(selected_cross_sessions.size()), alloc);
    cross.AddMember("entity_bridge_selected_ref_count", entity_bridge_selected_refs, alloc);
    cross.AddMember("strategy", "same_session_first_entity_bridge_then_bounded_cross_session", alloc);
    cross.AddMember("budget_guidance", "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events", alloc);
    recall.AddMember("cross_session", cross, alloc);
    rapidjson::Value tree(rapidjson::kObjectType);
    tree.AddMember("enabled", true, alloc);
    tree.AddMember("native_backend", true, alloc);
    tree.AddMember("fallback_to_flat", false, alloc);
    tree.AddMember("selected_node_count", static_cast<uint64_t>(selected_nodes.size()), alloc);
    tree.AddMember("selected_leaf_count", static_cast<uint64_t>(selected_nodes.size()), alloc);
    tree.AddMember("candidate_records_after_tree", static_cast<uint64_t>(scored.size()), alloc);
    rapidjson::Value summary_embeddings(rapidjson::kArrayType);
    summary_embeddings.PushBack("node_l0", alloc);
    summary_embeddings.PushBack("node_l1", alloc);
    tree.AddMember("summary_embeddings", summary_embeddings, alloc);
    recall.AddMember("tree_traversal", tree, alloc);
    rapidjson::Value secondary(rapidjson::kObjectType);
    secondary.AddMember("enabled", true, alloc);
    secondary.AddMember("native_backend", true, alloc);
    secondary.AddMember("applied_before_embedding_scoring", true, alloc);
    secondary.AddMember("matched_candidate_count", secondary_matched, alloc);
    secondary.AddMember("dropped_candidate_count", secondary_dropped, alloc);
    recall.AddMember("secondary_index_filter", secondary, alloc);
    pack.AddMember("recall_policy", recall, alloc);
    rapidjson::Value warnings(rapidjson::kArrayType);
    pack.AddMember("quality_warnings", warnings, alloc);
    out.AddMember("context_pack", pack, alloc);
    output_json->assign(JsonStringify(out));
    return bcache2::Status::OK();
}

bcache2::client::RiskPrecision ToRiskPrecision(temporalstore_risk_precision_t precision) {
    switch (precision) {
    case TEMPORALSTORE_RISK_ONE_SECOND:
        return bcache2::client::RiskPrecision::kOneSecond;
    case TEMPORALSTORE_RISK_FIVE_SECONDS:
        return bcache2::client::RiskPrecision::kFiveSeconds;
    case TEMPORALSTORE_RISK_TEN_SECONDS:
        return bcache2::client::RiskPrecision::kTenSeconds;
    case TEMPORALSTORE_RISK_ONE_MINUTE:
        return bcache2::client::RiskPrecision::kOneMinute;
    case TEMPORALSTORE_RISK_FIVE_MINUTES:
        return bcache2::client::RiskPrecision::kFiveMinutes;
    case TEMPORALSTORE_RISK_TEN_MINUTES:
        return bcache2::client::RiskPrecision::kTenMinutes;
    case TEMPORALSTORE_RISK_ONE_HOUR:
        return bcache2::client::RiskPrecision::kOneHour;
    case TEMPORALSTORE_RISK_ONE_DAY:
        return bcache2::client::RiskPrecision::kOneDay;
    case TEMPORALSTORE_RISK_ONE_MONTH:
        return bcache2::client::RiskPrecision::kOneMonth;
    }
    return bcache2::client::RiskPrecision::kOneMinute;
}

bcache2::client::RiskWindowUnit ToWindowUnit(temporalstore_window_unit_t unit) {
    switch (unit) {
    case TEMPORALSTORE_WINDOW_SECOND:
        return bcache2::client::RiskWindowUnit::kSecond;
    case TEMPORALSTORE_WINDOW_MINUTE:
        return bcache2::client::RiskWindowUnit::kMinute;
    case TEMPORALSTORE_WINDOW_HOUR:
        return bcache2::client::RiskWindowUnit::kHour;
    case TEMPORALSTORE_WINDOW_DAY:
        return bcache2::client::RiskWindowUnit::kDay;
    }
    return bcache2::client::RiskWindowUnit::kHour;
}

bcache2::client::TemporalFeatureFilterOp ToFeatureFilterOp(
    temporalstore_feature_filter_op_t op) {
    switch (op) {
    case TEMPORALSTORE_FEATURE_FILTER_EQUAL:
        return bcache2::client::TemporalFeatureFilterOp::kEqual;
    case TEMPORALSTORE_FEATURE_FILTER_NOT_EQUAL:
        return bcache2::client::TemporalFeatureFilterOp::kNotEqual;
    case TEMPORALSTORE_FEATURE_FILTER_GREATER_THAN:
        return bcache2::client::TemporalFeatureFilterOp::kGreaterThan;
    case TEMPORALSTORE_FEATURE_FILTER_LESS_THAN:
        return bcache2::client::TemporalFeatureFilterOp::kLessThan;
    }
    return bcache2::client::TemporalFeatureFilterOp::kEqual;
}

bcache2::Status CheckClient(temporalstore_client_t* client) {
    if (client == nullptr || client->impl == nullptr) {
        return NullError("client");
    }
    return bcache2::Status::OK();
}

void ClearHashEntryArray(temporalstore_hash_entry_array_t* array) {
    if (array == nullptr) {
        return;
    }
    if (array->entries != nullptr) {
        for (size_t i = 0; i < array->count; ++i) {
            std::free(const_cast<char*>(array->entries[i].key));
            std::free(const_cast<char*>(array->entries[i].field));
            std::free(const_cast<char*>(array->entries[i].value));
            std::free(const_cast<char*>(array->entries[i].route_json));
        }
    }
    std::free(array->entries);
    array->entries = nullptr;
    array->count = 0;
}

void ClearStringArray(temporalstore_string_array_t* array) {
    if (array == nullptr) {
        return;
    }
    if (array->values != nullptr) {
        for (size_t i = 0; i < array->count; ++i) {
            std::free(array->values[i]);
        }
    }
    std::free(array->values);
    array->values = nullptr;
    array->count = 0;
}

bcache2::Status BuildFeatureQuery(uint64_t start_ts, uint64_t end_ts, uint64_t count,
                                  const temporalstore_feature_filter_t* filters,
                                  size_t filter_count,
                                  bcache2::client::TemporalFeatureQuery* query) {
    if (query == nullptr) {
        return NullError("query");
    }
    if (filters == nullptr && filter_count != 0) {
        return NullError("filters");
    }
    query->start_ts = start_ts;
    query->end_ts = end_ts;
    query->count = count;
    query->filters.clear();
    query->filters.reserve(filter_count);
    for (size_t i = 0; i < filter_count; ++i) {
        bcache2::client::TemporalFeatureFilter filter;
        filter.field = filters[i].field ? filters[i].field : "";
        filter.op = ToFeatureFilterOp(filters[i].op);
        filter.value = filters[i].value;
        query->filters.push_back(filter);
    }
    return bcache2::Status::OK();
}

}  // namespace

extern "C" {

void temporalstore_options_init(temporalstore_options_t* options) {
    if (options == nullptr) {
        return;
    }
    std::memset(options, 0, sizeof(*options));
    options->idc = "vdc1";
    options->host = "127.0.0.1";
    options->psm = "temporalstore.c.customer.client";
    options->log_dir = "./";
    options->log_level = 3;
    options->io_timeout_ms = 1000;
    options->connect_timeout_ms = 1000;
    options->request_timeout_ms = 5000;
    options->max_read_retries = 1;
    options->max_write_retries = 0;
    options->retry_backoff_ms = 2;
    options->max_feature_points_per_request = 1000;
    options->max_feature_query_count = 5000;
    options->max_key_bytes = 4096;
    options->max_value_bytes = 16ULL * 1024ULL * 1024ULL;
    options->pin_primary = 1;
}

void temporalstore_free_string(char* value) { std::free(value); }

void temporalstore_string_array_free(temporalstore_string_array_t* array) {
    ClearStringArray(array);
}

void temporalstore_hash_entry_array_free(temporalstore_hash_entry_array_t* array) {
    ClearHashEntryArray(array);
}

void temporalstore_feature_point_array_free(temporalstore_feature_point_array_t* array) {
    if (array == nullptr) {
        return;
    }
    if (array->points != nullptr) {
        for (size_t i = 0; i < array->count; ++i) {
            std::free(const_cast<char*>(array->points[i].value));
        }
    }
    std::free(array->points);
    array->points = nullptr;
    array->count = 0;
}

void temporalstore_sequence_feature_row_array_free(
    temporalstore_sequence_feature_row_array_t* array) {
    if (array == nullptr) {
        return;
    }
    std::free(array->rows);
    array->rows = nullptr;
    array->count = 0;
}

void temporalstore_ips_feature_array_free(temporalstore_ips_feature_array_t* array) {
    if (array == nullptr) {
        return;
    }
    std::free(array->features);
    array->features = nullptr;
    array->count = 0;
}

int temporalstore_connect(const temporalstore_options_t* options, temporalstore_client_t** client,
                          char** error_message) {
    if (options == nullptr) {
        return Finish(NullError("options"), error_message);
    }
    if (client == nullptr) {
        return Finish(NullError("client"), error_message);
    }
    *client = nullptr;

    bcache2::client::TemporalStoreClientOptions cpp_options;
    cpp_options.metaserver_addr = options->metaserver_addr ? options->metaserver_addr : "";
    cpp_options.metaserver_consul =
        options->metaserver_consul ? options->metaserver_consul : "";
    cpp_options.namespace_name = options->namespace_name ? options->namespace_name : "";
    cpp_options.table_name = options->table_name ? options->table_name : "";
    if (options->idc) {
        cpp_options.idc = options->idc;
    }
    if (options->host) {
        cpp_options.host = options->host;
    }
    if (options->psm) {
        cpp_options.psm = options->psm;
    }
    if (options->log_dir) {
        cpp_options.log_dir = options->log_dir;
    }
    cpp_options.log_level = ToLogLevel(options->log_level);
    cpp_options.io_timeout_ms = options->io_timeout_ms;
    cpp_options.connect_timeout_ms = options->connect_timeout_ms;
    cpp_options.request_timeout_ms = options->request_timeout_ms;
    cpp_options.max_read_retries = options->max_read_retries;
    cpp_options.max_write_retries = options->max_write_retries;
    cpp_options.retry_backoff_ms = options->retry_backoff_ms;
    cpp_options.max_feature_points_per_request = options->max_feature_points_per_request;
    cpp_options.max_feature_query_count = options->max_feature_query_count;
    cpp_options.max_key_bytes = options->max_key_bytes;
    cpp_options.max_value_bytes = options->max_value_bytes;
    cpp_options.pin_primary = options->pin_primary != 0;

    std::unique_ptr<bcache2::client::TemporalStoreClient> cpp_client;
    bcache2::Status status =
        bcache2::client::TemporalStoreClient::Connect(cpp_options, &cpp_client);
    if (!status.ok()) {
        return Finish(status, error_message);
    }
    temporalstore_client_t* out = new temporalstore_client;
    out->impl = std::move(cpp_client);
    *client = out;
    return Finish(bcache2::Status::OK(), error_message);
}

int temporalstore_close(temporalstore_client_t* client, char** error_message) {
    if (client == nullptr) {
        return Finish(bcache2::Status::OK(), error_message);
    }
    bcache2::Status status = bcache2::Status::OK();
    if (client->impl) {
        status = client->impl->Close();
    }
    delete client;
    return Finish(status, error_message);
}

int temporalstore_put_string(temporalstore_client_t* client, const char* key, const char* value,
                             char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->PutString(key ? key : "", value ? value : "");
    }
    return Finish(status, error_message);
}

int temporalstore_put_string_with_ttl(temporalstore_client_t* client, const char* key,
                                      const char* value, uint64_t ttl_ms,
                                      char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->PutStringWithTtl(key ? key : "", value ? value : "", ttl_ms);
    }
    return Finish(status, error_message);
}

int temporalstore_get_string(temporalstore_client_t* client, const char* key, char** value,
                             char** error_message) {
    if (value == nullptr) {
        return Finish(NullError("value"), error_message);
    }
    *value = nullptr;
    bcache2::Status status = CheckClient(client);
    std::string out;
    if (status.ok()) {
        status = client->impl->GetString(key ? key : "", &out);
    }
    if (status.ok()) {
        *value = CopyCString(out);
        if (*value == nullptr) {
            status = bcache2::Status::ResourceExhausted("failed to allocate output string");
        }
    }
    return Finish(status, error_message);
}

int temporalstore_delete_object(temporalstore_client_t* client, const char* key,
                                char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->DeleteObject(key ? key : "");
    }
    return Finish(status, error_message);
}

int temporalstore_expire(temporalstore_client_t* client, const char* key, uint64_t ttl_ms,
                         char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->Expire(key ? key : "", ttl_ms);
    }
    return Finish(status, error_message);
}

int temporalstore_ttl(temporalstore_client_t* client, const char* key, uint64_t* ttl_ms,
                      char** error_message) {
    if (ttl_ms == nullptr) {
        return Finish(NullError("ttl_ms"), error_message);
    }
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->Ttl(key ? key : "", ttl_ms);
    }
    return Finish(status, error_message);
}

int temporalstore_hset(temporalstore_client_t* client, const char* key, const char* field,
                       const char* value, char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->HSet(key ? key : "", field ? field : "", value ? value : "");
    }
    return Finish(status, error_message);
}

int temporalstore_hget(temporalstore_client_t* client, const char* key, const char* field,
                       char** value, char** error_message) {
    if (value == nullptr) {
        return Finish(NullError("value"), error_message);
    }
    *value = nullptr;
    bcache2::Status status = CheckClient(client);
    std::string out;
    if (status.ok()) {
        status = client->impl->HGet(key ? key : "", field ? field : "", &out);
    }
    if (status.ok()) {
        *value = CopyCString(out);
        if (*value == nullptr) {
            status = bcache2::Status::ResourceExhausted("failed to allocate output string");
        }
    }
    return Finish(status, error_message);
}

int temporalstore_hgetall(temporalstore_client_t* client, const char* key,
                          temporalstore_hash_entry_array_t* entries,
                          char** error_message) {
    if (entries == nullptr) {
        return Finish(NullError("entries"), error_message);
    }
    ClearHashEntryArray(entries);
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::HashEntry> values;
    if (status.ok()) {
        status = client->impl->HGetAll(key ? key : "", &values);
    }
    if (status.ok()) {
        entries->entries = static_cast<temporalstore_hash_entry_t*>(
            std::calloc(values.size(), sizeof(temporalstore_hash_entry_t)));
        if (entries->entries == nullptr && !values.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate hash entry array");
        } else {
            entries->count = values.size();
            for (size_t i = 0; i < values.size(); ++i) {
                entries->entries[i].key = CopyCString(values[i].key);
                entries->entries[i].field = CopyCString(values[i].field);
                entries->entries[i].value = CopyCString(values[i].value);
                entries->entries[i].route_json = CopyCString(values[i].route_json);
                if (entries->entries[i].key == nullptr || entries->entries[i].field == nullptr ||
                    entries->entries[i].value == nullptr || entries->entries[i].route_json == nullptr) {
                    status = bcache2::Status::ResourceExhausted("failed to allocate hash entry value");
                    ClearHashEntryArray(entries);
                    break;
                }
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_hdel(temporalstore_client_t* client, const char* key, const char* field,
                       char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->HDel(key ? key : "", field ? field : "");
    }
    return Finish(status, error_message);
}

int temporalstore_hgetall(temporalstore_client_t* client, const char* key,
                          temporalstore_string_array_t* field_values,
                          char** error_message) {
    if (field_values == nullptr) {
        return Finish(NullError("field_values"), error_message);
    }
    ClearStringArray(field_values);
    bcache2::Status status = CheckClient(client);
    std::vector<std::pair<std::string, std::string>> values;
    if (status.ok()) {
        status = client->impl->HGetAll(key ? key : "", &values);
    }
    if (status.ok()) {
        const size_t output_count = values.size() * 2;
        field_values->values = static_cast<char**>(std::calloc(output_count, sizeof(char*)));
        if (field_values->values == nullptr && output_count != 0) {
            status = bcache2::Status::ResourceExhausted("failed to allocate string array");
        } else {
            field_values->count = output_count;
            for (size_t i = 0; i < values.size(); ++i) {
                field_values->values[i * 2] = CopyCString(values[i].first);
                field_values->values[i * 2 + 1] = CopyCString(values[i].second);
                if (field_values->values[i * 2] == nullptr ||
                    field_values->values[i * 2 + 1] == nullptr) {
                    status = bcache2::Status::ResourceExhausted("failed to allocate array value");
                    ClearStringArray(field_values);
                    break;
                }
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_sadd(temporalstore_client_t* client, const char* key, const char* member,
                       char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->SAdd(key ? key : "", member ? member : "");
    }
    return Finish(status, error_message);
}

int temporalstore_matrixark_batch_append_records(temporalstore_client_t* client,
                                                 const temporalstore_hash_entry_t* entries,
                                                 size_t entry_count, const char* count_key,
                                                 const char* count_value,
                                                 char** error_message) {
    return temporalstore_matrixark_batch_append_records_v2(
        client, entries, entry_count, count_key, count_value, nullptr, error_message);
}

int temporalstore_matrixark_batch_append_records_v2(temporalstore_client_t* client,
                                                    const temporalstore_hash_entry_t* entries,
                                                    size_t entry_count, const char* count_key,
                                                    const char* count_value,
                                                    const char* append_options_json,
                                                    char** error_message) {
    if (entries == nullptr && entry_count != 0) {
        return Finish(NullError("entries"), error_message);
    }
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::MatrixArkHashRecord> records;
    if (status.ok()) {
        std::vector<bcache2::client::HashEntry> batch;
        batch.reserve(entry_count);
        for (size_t i = 0; i < entry_count; ++i) {
            const temporalstore_hash_entry_t& entry = entries[i];
            batch.push_back(bcache2::client::HashEntry{
                entry.key ? entry.key : "",
                entry.field ? entry.field : "",
                entry.value ? entry.value : "",
                entry.route_json ? entry.route_json : "",
            });
        }
        bcache2::client::MatrixArkBatchAppendOptions options;
        options.append_options_json = append_options_json ? append_options_json : "";
        status = client->impl->MatrixArkBatchAppendRecords(
            batch,
            (count_key != nullptr && count_key[0] != '\0') ? count_key : "",
            count_value != nullptr ? count_value : "",
            options);
    }
    return Finish(status, error_message);
}

int temporalstore_matrixark_retrieve_context_pack(temporalstore_client_t* client,
                                                  const char* request_json,
                                                  char** response_json,
                                                  char** error_message) {
    if (response_json == nullptr) {
        return Finish(NullError("response_json"), error_message);
    }
    *response_json = nullptr;
    bcache2::Status status = CheckClient(client);
    std::string out;
    if (status.ok()) {
        status = client->impl->MatrixArkRetrieveContextPack(
            request_json ? request_json : "", &out);
    }
    if (status.ok()) {
        *response_json = CopyCString(out);
        if (*response_json == nullptr) {
            status = bcache2::Status::ResourceExhausted("failed to allocate response_json");
        }
    }
    return Finish(status, error_message);
}


int temporalstore_matrixark_scan_candidates(temporalstore_client_t* client,
                                             const char* count_key,
                                             const char* record_hash_key,
                                             size_t shard_size,
                                             const char* request_json,
                                             char** candidates_json,
                                             char** error_message) {
    if (candidates_json == nullptr) {
        return Finish(NullError("candidates_json"), error_message);
    }
    *candidates_json = nullptr;
    bcache2::Status status = CheckClient(client);
    std::string out;
    if (status.ok()) {
        status = MatrixArkScanCandidatesNative(
            client->impl.get(), count_key ? count_key : "", record_hash_key ? record_hash_key : "",
            shard_size, request_json ? request_json : "{}", &out);
    }
    if (status.ok()) {
        *candidates_json = CopyCString(out);
        if (*candidates_json == nullptr) {
            status = bcache2::Status::ResourceExhausted("failed to allocate candidate JSON");
        }
    }
    return Finish(status, error_message);
}

int temporalstore_matrixark_retrieve_context_pack(temporalstore_client_t* client,
                                                  const char* count_key,
                                                  const char* record_hash_key,
                                                  size_t shard_size,
                                                  const char* request_json,
                                                  char** context_pack_json,
                                                  char** error_message) {
    if (context_pack_json == nullptr) {
        return Finish(NullError("context_pack_json"), error_message);
    }
    *context_pack_json = nullptr;
    bcache2::Status status = CheckClient(client);
    std::string out;
    if (status.ok()) {
        status = MatrixArkRetrieveContextPackNative(
            client->impl.get(), count_key ? count_key : "", record_hash_key ? record_hash_key : "",
            shard_size, request_json ? request_json : "{}", &out);
    }
    if (status.ok()) {
        *context_pack_json = CopyCString(out);
        if (*context_pack_json == nullptr) {
            status = bcache2::Status::ResourceExhausted("failed to allocate context pack JSON");
        }
    }
    return Finish(status, error_message);
}

int temporalstore_smembers(temporalstore_client_t* client, const char* key,
                           temporalstore_string_array_t* members, char** error_message) {
    if (members == nullptr) {
        return Finish(NullError("members"), error_message);
    }
    ClearStringArray(members);
    bcache2::Status status = CheckClient(client);
    std::vector<std::string> values;
    if (status.ok()) {
        status = client->impl->SMembers(key ? key : "", &values);
    }
    if (status.ok()) {
        members->values = static_cast<char**>(std::calloc(values.size(), sizeof(char*)));
        if (members->values == nullptr && !values.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate string array");
        } else {
            members->count = values.size();
            for (size_t i = 0; i < values.size(); ++i) {
                members->values[i] = CopyCString(values[i]);
                if (members->values[i] == nullptr) {
                    status = bcache2::Status::ResourceExhausted("failed to allocate array value");
                    ClearStringArray(members);
                    break;
                }
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_add_feature_points(temporalstore_client_t* client, const char* key,
                                     const temporalstore_feature_point_t* points, size_t count,
                                     char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok() && points == nullptr && count != 0) {
        status = NullError("points");
    }
    if (status.ok()) {
        std::vector<bcache2::client::TemporalFeaturePoint> cpp_points;
        cpp_points.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            cpp_points.push_back(
                bcache2::client::TemporalFeaturePoint{points[i].timestamp,
                                                       points[i].value ? points[i].value : ""});
        }
        status = client->impl->AddFeaturePoints(key ? key : "", cpp_points);
    }
    return Finish(status, error_message);
}

int temporalstore_query_feature_points(temporalstore_client_t* client, const char* key,
                                       uint64_t start_ts, uint64_t end_ts, uint64_t count,
                                       temporalstore_feature_point_array_t* points,
                                       char** error_message) {
    return temporalstore_query_feature_points_with_filters(client, key, start_ts, end_ts, count,
                                                           nullptr, 0, points, error_message);
}

int temporalstore_query_feature_points_with_filters(
    temporalstore_client_t* client, const char* key, uint64_t start_ts, uint64_t end_ts,
    uint64_t count, const temporalstore_feature_filter_t* filters, size_t filter_count,
    temporalstore_feature_point_array_t* points, char** error_message) {
    if (points == nullptr) {
        return Finish(NullError("points"), error_message);
    }
    temporalstore_feature_point_array_free(points);
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::TemporalFeaturePoint> cpp_points;
    if (status.ok()) {
        bcache2::client::TemporalFeatureQuery query;
        status = BuildFeatureQuery(start_ts, end_ts, count, filters, filter_count, &query);
        if (status.ok()) {
            status = client->impl->QueryFeaturePoints(key ? key : "", query, &cpp_points);
        }
    }
    if (status.ok()) {
        points->points = static_cast<temporalstore_feature_point_t*>(
            std::calloc(cpp_points.size(), sizeof(temporalstore_feature_point_t)));
        if (points->points == nullptr && !cpp_points.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate feature points");
        } else {
            points->count = cpp_points.size();
            for (size_t i = 0; i < cpp_points.size(); ++i) {
                points->points[i].timestamp = cpp_points[i].timestamp;
                points->points[i].value = CopyCString(cpp_points[i].value);
                if (points->points[i].value == nullptr) {
                    status = bcache2::Status::ResourceExhausted("failed to allocate point value");
                    temporalstore_feature_point_array_free(points);
                    break;
                }
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_add_sequence_feature_rows(temporalstore_client_t* client, const char* key,
                                            const temporalstore_sequence_feature_row_t* rows,
                                            size_t count, char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok() && rows == nullptr && count != 0) {
        status = NullError("rows");
    }
    if (status.ok()) {
        std::vector<bcache2::client::SequenceFeatureRow> cpp_rows;
        cpp_rows.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            bcache2::client::SequenceFeatureRow row;
            row.timestamp = rows[i].timestamp;
            row.gid = rows[i].gid;
            row.action_type = rows[i].action_type;
            row.duration = rows[i].duration;
            row.author_id = rows[i].author_id;
            cpp_rows.push_back(row);
        }
        status = client->impl->AddSequenceFeatureRows(key ? key : "", cpp_rows);
    }
    return Finish(status, error_message);
}

int temporalstore_query_sequence_feature_rows(
    temporalstore_client_t* client, const char* key, uint64_t start_ts, uint64_t end_ts,
    uint64_t count, const temporalstore_feature_filter_t* filters, size_t filter_count,
    temporalstore_sequence_feature_row_array_t* rows, char** error_message) {
    if (rows == nullptr) {
        return Finish(NullError("rows"), error_message);
    }
    temporalstore_sequence_feature_row_array_free(rows);
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::SequenceFeatureRow> cpp_rows;
    if (status.ok()) {
        bcache2::client::TemporalFeatureQuery query;
        status = BuildFeatureQuery(start_ts, end_ts, count, filters, filter_count, &query);
        if (status.ok()) {
            status = client->impl->QuerySequenceFeatureRows(key ? key : "", query, &cpp_rows);
        }
    }
    if (status.ok()) {
        rows->rows = static_cast<temporalstore_sequence_feature_row_t*>(
            std::calloc(cpp_rows.size(), sizeof(temporalstore_sequence_feature_row_t)));
        if (rows->rows == nullptr && !cpp_rows.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate sequence rows");
        } else {
            rows->count = cpp_rows.size();
            for (size_t i = 0; i < cpp_rows.size(); ++i) {
                rows->rows[i].timestamp = cpp_rows[i].timestamp;
                rows->rows[i].gid = cpp_rows[i].gid;
                rows->rows[i].action_type = cpp_rows[i].action_type;
                rows->rows[i].duration = cpp_rows[i].duration;
                rows->rows[i].author_id = cpp_rows[i].author_id;
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_add_ips_instance(temporalstore_client_t* client, const char* table, int64_t uid,
                                   int64_t timestamp_us, int32_t action_type,
                                   int32_t logical_table,
                                   const temporalstore_ips_feature_stat_t* features,
                                   size_t feature_count, char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok() && features == nullptr && feature_count != 0) {
        status = NullError("features");
    }
    if (status.ok()) {
        bcache2::client::IpsInstance instance;
        if (table != nullptr && table[0] != '\0') {
            instance.table = table;
        }
        instance.uid = uid;
        instance.timestamp_us = timestamp_us;
        instance.action_type = action_type;
        instance.logical_table = logical_table;
        instance.features.reserve(feature_count);
        for (size_t i = 0; i < feature_count; ++i) {
            bcache2::client::IpsFeatureStat feature;
            feature.id = features[i].id;
            feature.slot = features[i].slot;
            feature.has_slot = features[i].has_slot != 0;
            feature.type = features[i].type;
            feature.v1 = features[i].v1;
            feature.v2 = features[i].v2;
            instance.features.push_back(feature);
        }
        status = client->impl->AddIpsInstance(instance);
    }
    return Finish(status, error_message);
}

int temporalstore_query_ips_last_instances(temporalstore_client_t* client, const char* table,
                                           int64_t uid, int32_t action_type,
                                           int32_t logical_table, int32_t slot, int32_t top_k,
                                           int64_t last_instances,
                                           temporalstore_ips_feature_array_t* features,
                                           char** error_message) {
    if (features == nullptr) {
        return Finish(NullError("features"), error_message);
    }
    temporalstore_ips_feature_array_free(features);
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::IpsFeatureStat> cpp_features;
    if (status.ok()) {
        bcache2::client::IpsLastQuery query;
        if (table != nullptr && table[0] != '\0') {
            query.table = table;
        }
        query.uid = uid;
        query.action_type = action_type;
        query.logical_table = logical_table;
        query.slot = slot;
        query.top_k = top_k;
        query.last_instances = last_instances;
        status = client->impl->QueryIpsLastInstances(query, &cpp_features);
    }
    if (status.ok()) {
        features->features = static_cast<temporalstore_ips_feature_stat_t*>(
            std::calloc(cpp_features.size(), sizeof(temporalstore_ips_feature_stat_t)));
        if (features->features == nullptr && !cpp_features.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate IPS features");
        } else {
            features->count = cpp_features.size();
            for (size_t i = 0; i < cpp_features.size(); ++i) {
                features->features[i].id = cpp_features[i].id;
                features->features[i].slot = cpp_features[i].slot;
                features->features[i].has_slot = cpp_features[i].has_slot ? 1 : 0;
                features->features[i].type = cpp_features[i].type;
                features->features[i].v1 = cpp_features[i].v1;
                features->features[i].v2 = cpp_features[i].v2;
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_risk_increment(temporalstore_client_t* client, const char* key, int64_t amount,
                                 uint64_t ttl_seconds,
                                 temporalstore_risk_precision_t precision, const char* uuid,
                                 uint64_t occur_time_seconds, char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->RiskIncrement(key ? key : "", amount, ttl_seconds,
                                             ToRiskPrecision(precision), uuid ? uuid : "",
                                             occur_time_seconds);
    }
    return Finish(status, error_message);
}

int temporalstore_risk_count(temporalstore_client_t* client, const char* key,
                             temporalstore_risk_precision_t precision, int64_t window_start,
                             int64_t window_end, temporalstore_window_unit_t window_unit,
                             int64_t* count, char** error_message) {
    if (count == nullptr) {
        return Finish(NullError("count"), error_message);
    }
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        bcache2::client::RiskWindow window;
        window.start = window_start;
        window.end = window_end;
        window.unit = ToWindowUnit(window_unit);
        status = client->impl->RiskCount(key ? key : "", ToRiskPrecision(precision), window,
                                         count);
    }
    return Finish(status, error_message);
}

}  // extern "C"
