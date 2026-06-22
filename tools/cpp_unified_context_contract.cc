#include <algorithm>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>

namespace {

using Value = rapidjson::Value;

struct Event {
    uint64_t tenant_hash = 0;
    uint64_t node_hash = 0;
    uint64_t event_id_hash = 0;
    uint64_t event_time_ms = 0;
    std::string event_type;
    std::string actor;
    std::string team;
    std::string project;
    std::string status;
    uint64_t confidence = 0;
    uint64_t importance = 0;
    uint64_t token_estimate = 0;
    std::string payload;
};

struct ResourceChunk {
    uint64_t tenant_hash = 0;
    uint64_t resource_hash = 0;
    uint64_t chunk_hash = 0;
    std::string raw_uri;
    std::string resource_type;
    std::string source_ref;
    std::string text;
    uint64_t token_estimate = 0;
};

struct Entity {
    uint64_t tenant_hash = 0;
    uint64_t node_hash = 0;
    uint64_t entity_hash = 0;
    uint64_t type = 0;
    std::string name;
    std::string value;
    std::string event_type;
    std::string team;
    std::string project;
    std::string status;
    uint64_t updated_at_ms = 0;
    uint64_t valid_from_ms = 0;
    uint64_t confidence = 0;
    uint64_t token_estimate = 1;
    std::vector<uint64_t> source_event_hashes;
};

struct ContextPack {
    std::vector<uint64_t> entity_hashes;
    std::vector<uint64_t> summary_refs;
    std::vector<uint64_t> event_ids;
    std::vector<uint64_t> chunk_hashes;
    uint64_t selected_tokens = 0;
    std::string query_understanding_source;
    std::string staleness_policy = "algorithmic_freshness_v1";
    std::vector<std::string> sections = {"current_state"};
    uint64_t blocked_ref_count = 0;
    uint64_t dropped_ref_count = 0;
};

struct QueryPlan {
    std::string event_type = "business_event";
    std::string status;
    std::string team;
    std::string project;
    std::string source = "rules_fallback";
    std::string staleness_policy = "algorithmic_freshness_v1";
};

struct Compression {
    uint64_t compression_id_hash = 0;
    uint64_t node_hash = 0;
    uint64_t source_start_ms = 0;
    uint64_t source_end_ms = 0;
    uint64_t compressed_time_ms = 0;
    std::string summary;
    std::vector<uint64_t> source_event_ids;
};

struct IndexRefLite {
    uint64_t event_id_hash = 0;
    uint64_t node_hash = 0;
    uint64_t event_time_ms = 0;
};

struct NodeMeta {
    uint64_t parent_hash = 0;
    uint64_t child_count = 0;
    uint64_t updated_at_ms = 0;
};

struct State {
    std::map<std::pair<uint64_t, uint64_t>, NodeMeta> nodes;
    std::map<std::pair<uint64_t, uint64_t>, std::vector<uint64_t>> children;
    std::map<std::pair<uint64_t, uint64_t>, std::vector<double>> embeddings;
    std::map<std::tuple<uint64_t, uint64_t, uint64_t>, Entity> entities;
    std::map<std::pair<uint64_t, uint64_t>, std::vector<Event>> events;
    std::map<std::tuple<uint64_t, std::string, std::string, uint64_t>, std::vector<IndexRefLite>> indexes;
    std::map<std::pair<uint64_t, uint64_t>, std::vector<ResourceChunk>> resources_by_chunk;
    std::map<std::pair<uint64_t, uint64_t>, int> dirty_counts;
    std::map<std::pair<uint64_t, uint64_t>, int> summary_counts;
    std::map<std::pair<uint64_t, uint64_t>, std::vector<uint64_t>> summary_refs;
    std::map<std::pair<uint64_t, uint64_t>, std::vector<Compression>> compressions;
    std::map<std::pair<uint64_t, uint64_t>, int> pack_audit_counts;
    std::set<std::tuple<uint64_t, std::string, std::string>> api_idempotency_keys;
    std::map<std::tuple<uint64_t, std::string, uint64_t>, uint64_t> stream_committed_offsets;
};

[[noreturn]] void Fail(const std::string& message) {
    throw std::runtime_error(message);
}

std::string Kind(const Value& command) {
    if (!command.HasMember("kind") || !command["kind"].IsString()) {
        Fail("command missing kind");
    }
    return command["kind"].GetString();
}

std::string S(const Value& value, const char* field, const std::string& fallback = "") {
    if (!value.IsObject() || !value.HasMember(field) || !value[field].IsString()) {
        return fallback;
    }
    return value[field].GetString();
}

uint64_t U(const Value& value, const char* field, uint64_t fallback = 0) {
    if (!value.IsObject() || !value.HasMember(field) || !value[field].IsUint64()) {
        return fallback;
    }
    return value[field].GetUint64();
}

std::string Lower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return text;
}

bool Contains(const std::string& text, const std::string& needle) {
    return text.find(needle) != std::string::npos;
}

std::string NestedS(const Value& value, const char* object_field, const char* field) {
    if (!value.IsObject() || !value.HasMember(object_field) || !value[object_field].IsObject()) {
        return "";
    }
    return S(value[object_field], field);
}

std::vector<double> VectorFromJson(const Value& value) {
    std::vector<double> out;
    if (!value.IsArray()) {
        return out;
    }
    for (const auto& item : value.GetArray()) {
        if (!item.IsNumber()) {
            Fail("vector contains non-number");
        }
        out.push_back(item.GetDouble());
    }
    return out;
}

std::vector<uint64_t> U64Array(const Value& value) {
    std::vector<uint64_t> out;
    if (!value.IsArray()) {
        Fail("expected integer array");
    }
    for (const auto& item : value.GetArray()) {
        if (!item.IsUint64()) {
            Fail("integer array contains non-uint64");
        }
        out.push_back(item.GetUint64());
    }
    return out;
}

void ExpectEqual(const std::vector<uint64_t>& actual,
                 const std::vector<uint64_t>& expected,
                 const std::string& label) {
    if (actual == expected) {
        return;
    }
    std::ostringstream oss;
    oss << label << " mismatch actual=[";
    for (size_t i = 0; i < actual.size(); ++i) {
        oss << (i ? "," : "") << actual[i];
    }
    oss << "] expected=[";
    for (size_t i = 0; i < expected.size(); ++i) {
        oss << (i ? "," : "") << expected[i];
    }
    oss << "]";
    Fail(oss.str());
}

std::vector<std::string> StringArray(const Value& value) {
    std::vector<std::string> out;
    if (!value.IsArray()) {
        Fail("expected string array");
    }
    for (const auto& item : value.GetArray()) {
        if (!item.IsString()) {
            Fail("string array contains non-string");
        }
        out.emplace_back(item.GetString());
    }
    return out;
}

void ExpectEqual(const std::vector<std::string>& actual,
                 const std::vector<std::string>& expected,
                 const std::string& label) {
    if (actual == expected) {
        return;
    }
    Fail(label + " string array mismatch");
}

void AssertPackContract(const ContextPack& pack, const Value& command, const std::string& label) {
    if (command.HasMember("expect_query_understanding_source") &&
        pack.query_understanding_source != S(command, "expect_query_understanding_source")) {
        Fail(label + " query understanding source mismatch");
    }
    if (command.HasMember("expect_staleness_policy") &&
        pack.staleness_policy != S(command, "expect_staleness_policy")) {
        Fail(label + " staleness policy mismatch");
    }
    if (command.HasMember("expect_context_pack_sections")) {
        ExpectEqual(pack.sections, StringArray(command["expect_context_pack_sections"]), label);
    }
    if (command.HasMember("expect_entity_hashes")) {
        ExpectEqual(pack.entity_hashes, U64Array(command["expect_entity_hashes"]), label);
    }
    if (command.HasMember("expect_summary_refs")) {
        ExpectEqual(pack.summary_refs, U64Array(command["expect_summary_refs"]), label);
    }
    if (command.HasMember("expect_blocked_ref_count") &&
        pack.blocked_ref_count != U(command, "expect_blocked_ref_count")) {
        Fail(label + " blocked ref count mismatch");
    }
    if (command.HasMember("expect_dropped_ref_count") &&
        pack.dropped_ref_count != U(command, "expect_dropped_ref_count")) {
        Fail(label + " dropped ref count mismatch");
    }
}

bool ContainsId(const std::vector<uint64_t>& values, uint64_t expected) {
    return std::find(values.begin(), values.end(), expected) != values.end();
}

std::string IndexValueKey(const Value& value) {
    if (value.IsObject() && value.HasMember("index_value_hash") && value["index_value_hash"].IsUint64()) {
        return std::to_string(value["index_value_hash"].GetUint64());
    }
    return S(value, "index_value");
}

std::tuple<uint64_t, std::string, std::string, uint64_t> IndexKeyLite(const Value& value) {
    return {U(value, "tenant_hash"), S(value, "index_name"), IndexValueKey(value),
            U(value, "scope_hash", 0)};
}

void AddIndexRef(State& state, const Value& record) {
    IndexRefLite ref;
    ref.event_id_hash = U(record, "event_id_hash");
    ref.node_hash = U(record, "node_hash", U(record, "primary_node_hash"));
    ref.event_time_ms = U(record, "event_time_ms", U(record, "primary_event_time_ms"));
    if (record.HasMember("ref") && record["ref"].IsObject()) {
        ref.event_id_hash = U(record["ref"], "event_id_hash", ref.event_id_hash);
        ref.node_hash = U(record["ref"], "primary_node_hash", ref.node_hash);
        ref.event_time_ms = U(record["ref"], "primary_event_time_ms", ref.event_time_ms);
    } else if (record.HasMember("index_ref") && record["index_ref"].IsObject()) {
        ref.event_id_hash = U(record["index_ref"], "event_id_hash", ref.event_id_hash);
        ref.node_hash = U(record["index_ref"], "primary_node_hash", ref.node_hash);
        ref.event_time_ms = U(record["index_ref"], "primary_event_time_ms", ref.event_time_ms);
    }
    if (ref.event_time_ms == 0) {
        ref.event_time_ms = U(record, "event_time_ms");
    }
    auto& refs = state.indexes[IndexKeyLite(record)];
    const auto duplicate = std::find_if(refs.begin(), refs.end(), [&ref](const IndexRefLite& existing) {
        return existing.event_id_hash == ref.event_id_hash && existing.node_hash == ref.node_hash &&
               existing.event_time_ms == ref.event_time_ms;
    });
    if (duplicate == refs.end()) {
        refs.push_back(ref);
    }
}

std::vector<uint64_t> QueryIndexRefs(const State& state, const Value& command) {
    auto it = state.indexes.find(IndexKeyLite(command));
    if (it == state.indexes.end()) {
        return {};
    }
    const uint64_t start_ms = U(command, "start_time_ms", 0);
    const uint64_t end_ms = U(command, "end_time_ms", UINT64_MAX);
    const uint64_t limit = U(command, "limit", UINT64_MAX);
    std::vector<IndexRefLite> refs;
    for (const auto& ref : it->second) {
        if (ref.event_time_ms >= start_ms && ref.event_time_ms <= end_ms) {
            refs.push_back(ref);
        }
    }
    std::sort(refs.begin(), refs.end(), [](const IndexRefLite& a, const IndexRefLite& b) {
        return std::tie(a.event_time_ms, a.event_id_hash, a.node_hash) <
               std::tie(b.event_time_ms, b.event_id_hash, b.node_hash);
    });
    std::vector<uint64_t> ids;
    for (const auto& ref : refs) {
        if (ids.size() >= limit) {
            break;
        }
        ids.push_back(ref.event_id_hash);
    }
    return ids;
}

std::vector<uint64_t> QueryIndexIntersection(const State& state, const Value& command) {
    if (!command.HasMember("filters") || !command["filters"].IsArray()) {
        return {};
    }
    std::vector<uint64_t> result;
    bool first = true;
    for (const auto& filter : command["filters"].GetArray()) {
        rapidjson::Document scoped;
        scoped.SetObject();
        auto& alloc = scoped.GetAllocator();
        scoped.AddMember("tenant_hash", U(command, "tenant_hash"), alloc);
        scoped.AddMember("index_name", rapidjson::Value(S(filter, "index_name").c_str(), alloc), alloc);
        if (filter.HasMember("index_value_hash") && filter["index_value_hash"].IsUint64()) {
            scoped.AddMember("index_value_hash", filter["index_value_hash"].GetUint64(), alloc);
        } else {
            scoped.AddMember("index_value", rapidjson::Value(S(filter, "index_value").c_str(), alloc), alloc);
        }
        scoped.AddMember("scope_hash", U(filter, "scope_hash", U(command, "scope_hash", 0)), alloc);
        scoped.AddMember("start_time_ms", U(command, "start_time_ms", 0), alloc);
        scoped.AddMember("end_time_ms", U(command, "end_time_ms", UINT64_MAX), alloc);
        scoped.AddMember("limit", U(command, "limit", UINT64_MAX), alloc);
        auto ids = QueryIndexRefs(state, scoped);
        if (first) {
            result = std::move(ids);
            first = false;
            continue;
        }
        std::vector<uint64_t> next;
        for (auto id : result) {
            if (ContainsId(ids, id)) {
                next.push_back(id);
            }
        }
        result = std::move(next);
    }
    return result;
}

bool EventExists(const State& state, uint64_t tenant_hash, uint64_t event_id_hash) {
    for (const auto& [key, events] : state.events) {
        if (key.first != tenant_hash) {
            continue;
        }
        for (const auto& event : events) {
            if (event.event_id_hash == event_id_hash) {
                return true;
            }
        }
    }
    return false;
}

uint64_t CountEventId(const State& state, uint64_t tenant_hash, uint64_t event_id_hash) {
    uint64_t count = 0;
    for (const auto& [key, events] : state.events) {
        if (key.first != tenant_hash) {
            continue;
        }
        for (const auto& event : events) {
            if (event.event_id_hash == event_id_hash) {
                ++count;
            }
        }
    }
    return count;
}

void ExpectEventsExist(const State& state, uint64_t tenant_hash,
                       const std::vector<uint64_t>& event_ids,
                       const std::string& label) {
    for (const auto event_id : event_ids) {
        if (!EventExists(state, tenant_hash, event_id)) {
            Fail(label + " missing event " + std::to_string(event_id));
        }
    }
}

void ExpectEventsAbsent(const State& state, uint64_t tenant_hash,
                        const std::vector<uint64_t>& event_ids,
                        const std::string& label) {
    for (const auto event_id : event_ids) {
        if (EventExists(state, tenant_hash, event_id)) {
            Fail(label + " unexpected event " + std::to_string(event_id));
        }
    }
}

double Dot(const std::vector<double>& left, const std::vector<double>& right) {
    double score = 0.0;
    const size_t n = std::min(left.size(), right.size());
    for (size_t i = 0; i < n; ++i) {
        score += left[i] * right[i];
    }
    return score;
}

void UpsertEntity(State& state, const Entity& entity) {
    state.entities[{entity.tenant_hash, entity.node_hash, entity.entity_hash}] = entity;
}

bool EntityMatches(const Entity& entity, const QueryPlan& intent) {
    if (!intent.event_type.empty() && entity.event_type != intent.event_type) {
        return false;
    }
    if (!intent.status.empty() && entity.status != intent.status) {
        return false;
    }
    if (!intent.team.empty() && entity.team != intent.team) {
        return false;
    }
    if (!intent.project.empty() && entity.project != intent.project) {
        return false;
    }
    return true;
}

std::vector<Entity> QueryEntities(State& state, uint64_t tenant_hash, uint64_t node_hash,
                                  const std::vector<uint64_t>& entity_hashes) {
    std::vector<Entity> out;
    for (uint64_t entity_hash : entity_hashes) {
        const auto it = state.entities.find({tenant_hash, node_hash, entity_hash});
        if (it != state.entities.end()) {
            out.push_back(it->second);
        }
    }
    return out;
}

Event ExtractEvent(const std::string& raw_text, const Value& hints) {
    const auto lower = Lower(raw_text);
    Event event;
    if (Contains(lower, "approved")) {
        event.status = "approved";
    } else if (Contains(lower, "confirmed")) {
        event.status = "confirmed";
    } else if (Contains(lower, "rejected")) {
        event.status = "rejected";
    } else {
        event.status = "observed";
    }
    if (Contains(lower, "approval") || Contains(lower, "approved")) {
        event.event_type = "approval_confirmation";
    } else if (Contains(lower, "incident")) {
        event.event_type = "incident_update";
    } else if (Contains(lower, "cost") || Contains(lower, "budget")) {
        event.event_type = "cost_update";
    } else {
        event.event_type = S(hints, "event_type", "business_event");
    }
    event.team = S(hints, "team");
    event.project = S(hints, "project");
    event.confidence = U(hints, "confidence", 90);
    event.importance = U(hints, "importance", 80);
    event.token_estimate = U(hints, "token_estimate", 24);
    event.payload = raw_text;
    return event;
}

void UpsertEntityFromHints(State& state, uint64_t tenant_hash, uint64_t node_hash,
                           const Event& event, const Value& hints) {
    if (!hints.HasMember("entity_hash") || U(hints, "entity_hash") == 0) {
        return;
    }
    Entity entity;
    entity.tenant_hash = tenant_hash;
    entity.node_hash = node_hash;
    entity.entity_hash = U(hints, "entity_hash");
    entity.type = U(hints, "entity_type", 1);
    entity.name = S(hints, "entity_name", "entity");
    entity.value = S(hints, "entity_value", event.payload);
    entity.event_type = event.event_type;
    entity.team = event.team;
    entity.project = event.project;
    entity.status = event.status;
    entity.updated_at_ms = event.event_time_ms;
    entity.valid_from_ms = U(hints, "valid_from_ms", event.event_time_ms);
    entity.confidence = event.confidence;
    entity.token_estimate = U(hints, "entity_token_estimate", 1);
    entity.source_event_hashes.push_back(event.event_id_hash);
    UpsertEntity(state, entity);
}

Event WriteEvent(State& state, uint64_t tenant_hash, uint64_t node_hash, Event event) {
    event.tenant_hash = tenant_hash;
    event.node_hash = node_hash;
    state.events[{tenant_hash, node_hash}].push_back(event);
    return event;
}

Event IngestRawEvent(State& state, uint64_t tenant_hash, const std::string& raw_text,
                     const Value& hints) {
    const uint64_t leaf_hash = U(hints, "leaf_node_hash");
    auto& children = state.children[{tenant_hash, U(hints, "parent_hash")}];
    if (std::find(children.begin(), children.end(), leaf_hash) == children.end()) {
        children.push_back(leaf_hash);
    }
    if (hints.HasMember("embedding")) {
        state.embeddings[{tenant_hash, leaf_hash}] = VectorFromJson(hints["embedding"]);
    }
    auto event = ExtractEvent(raw_text, hints);
    event.event_id_hash = U(hints, "event_id_hash");
    event.event_time_ms = U(hints, "event_time_ms");
    WriteEvent(state, tenant_hash, leaf_hash, event);
    UpsertEntityFromHints(state, tenant_hash, leaf_hash, event, hints);
    const auto write_index = [&](const std::string& name, const std::string& value) {
        rapidjson::Document record;
        record.SetObject();
        auto& alloc = record.GetAllocator();
        record.AddMember("tenant_hash", tenant_hash, alloc);
        record.AddMember("index_name", rapidjson::Value(name.c_str(), alloc), alloc);
        record.AddMember("index_value", rapidjson::Value(value.c_str(), alloc), alloc);
        record.AddMember("scope_hash", U(hints, "scope_hash", 0), alloc);
        record.AddMember("node_hash", leaf_hash, alloc);
        record.AddMember("event_id_hash", event.event_id_hash, alloc);
        record.AddMember("event_time_ms", event.event_time_ms, alloc);
        AddIndexRef(state, record);
    };
    write_index("status", event.status);
    write_index("event_type", event.event_type);
    write_index("project", event.project);
    return event;
}

bool Matches(const Event& event, uint64_t start_ms, uint64_t end_ms, const Value* filters) {
    if (event.event_time_ms < start_ms || event.event_time_ms > end_ms) {
        return false;
    }
    if (filters != nullptr && filters->IsObject()) {
        const std::pair<const char*, std::string Event::*> strings[] = {
            {"event_type", &Event::event_type},
            {"team", &Event::team},
            {"project", &Event::project},
            {"status", &Event::status},
        };
        for (const auto& item : strings) {
            if (filters->HasMember(item.first) && (*filters)[item.first].IsString()) {
                const std::string expected = (*filters)[item.first].GetString();
                if (!expected.empty() && event.*(item.second) != expected) {
                    return false;
                }
            }
        }
        if (filters->HasMember("min_confidence") &&
            event.confidence < (*filters)["min_confidence"].GetUint64()) {
            return false;
        }
        if (filters->HasMember("min_importance") &&
            event.importance < (*filters)["min_importance"].GetUint64()) {
            return false;
        }
    }
    return true;
}

std::vector<uint64_t> QueryEvents(State& state, const Value& command) {
    const uint64_t tenant_hash = U(command, "tenant_hash");
    const uint64_t node_hash = U(command, "node_hash");
    const uint64_t start_ms = U(command, "start_time_ms");
    const uint64_t end_ms = U(command, "end_time_ms", UINT64_MAX);
    const uint64_t limit = U(command, "limit", UINT64_MAX);
    const Value* filters = command.HasMember("filters") ? &command["filters"] : nullptr;
    std::vector<uint64_t> ids;
    for (const auto& event : state.events[{tenant_hash, node_hash}]) {
        if (Matches(event, start_ms, end_ms, filters)) {
            ids.push_back(event.event_id_hash);
            if (ids.size() >= limit) {
                break;
            }
        }
    }
    return ids;
}

std::vector<uint64_t> Traverse(State& state, const Value& command) {
    const uint64_t tenant_hash = U(command, "tenant_hash");
    const auto query = VectorFromJson(command["query_vector"]);
    const size_t top_k = static_cast<size_t>(U(command, "top_k_per_depth", 3));
    const size_t max_candidates = static_cast<size_t>(U(command, "max_candidate_nodes", 12));
    const uint64_t max_depth = U(command, "max_depth", 6);
    std::vector<uint64_t> frontier{U(command, "root_node_hash")};
    std::vector<uint64_t> candidates;
    for (uint64_t depth = 0; depth < max_depth; ++depth) {
        std::vector<std::pair<double, uint64_t>> scored;
        for (auto parent : frontier) {
            for (auto child : state.children[{tenant_hash, parent}]) {
                scored.push_back({Dot(query, state.embeddings[{tenant_hash, child}]), child});
            }
        }
        if (scored.empty()) {
            break;
        }
        std::sort(scored.begin(), scored.end(), [](const auto& left, const auto& right) {
            if (left.first != right.first) {
                return left.first > right.first;
            }
            return left.second < right.second;
        });
        frontier.clear();
        for (size_t i = 0; i < std::min(top_k, scored.size()); ++i) {
            frontier.push_back(scored[i].second);
            if (scored[i].first > 0.0 &&
                std::find(candidates.begin(), candidates.end(), scored[i].second) == candidates.end()) {
                candidates.push_back(scored[i].second);
                if (candidates.size() >= max_candidates) {
                    return candidates;
                }
            }
        }
    }
    return candidates;
}

QueryPlan QueryIntent(const Value& command) {
    if (command.HasMember("query_plan") && command["query_plan"].IsObject()) {
        const auto& plan = command["query_plan"];
        QueryPlan out;
        out.event_type = NestedS(plan, "filters", "event_type");
        if (out.event_type.empty()) {
            out.event_type = S(plan, "event_type", "business_event");
        }
        out.status = NestedS(plan, "filters", "status");
        if (out.status.empty()) {
            out.status = S(plan, "status");
        }
        out.team = NestedS(plan, "scope", "team");
        if (out.team.empty()) {
            out.team = NestedS(plan, "filters", "team");
        }
        out.project = NestedS(plan, "scope", "project");
        if (out.project.empty()) {
            out.project = NestedS(plan, "filters", "project");
        }
        out.source = S(plan, "source", "model");
        out.staleness_policy = S(plan, "staleness_policy", "algorithmic_freshness_v1");
        return out;
    }
    const auto raw = Lower(S(command, "raw_query"));
    QueryPlan out;
    if (Contains(raw, "approved") || Contains(raw, "approval")) {
        out.event_type = "approval_confirmation";
    } else if (Contains(raw, "incident")) {
        out.event_type = "incident_update";
    } else if (Contains(raw, "cost") || Contains(raw, "budget")) {
        out.event_type = "cost_update";
    }
    if (Contains(raw, "approved")) {
        out.status = "approved";
    } else if (Contains(raw, "confirmed")) {
        out.status = "confirmed";
    } else if (Contains(raw, "rejected")) {
        out.status = "rejected";
    }
    const Value& hints = command.HasMember("hints") ? command["hints"] : command;
    out.team = S(hints, "team");
    out.project = S(hints, "project");
    return out;
}

uint64_t StalenessScore(const Event& event, uint64_t as_of_ms) {
    const uint64_t age_ms = as_of_ms > event.event_time_ms ? as_of_ms - event.event_time_ms : 0;
    const uint64_t age_penalty = std::min<uint64_t>(age_ms / 1000, 100);
    return event.confidence + event.importance > age_penalty
               ? event.confidence + event.importance - age_penalty
               : 0;
}

ContextPack Retrieve(State& state, const Value& command) {
    const auto intent = QueryIntent(command);
    rapidjson::Document filters;
    filters.SetObject();
    auto& alloc = filters.GetAllocator();
    filters.AddMember("event_type", rapidjson::Value(intent.event_type.c_str(), alloc), alloc);
    if (!intent.status.empty()) {
        filters.AddMember("status", rapidjson::Value(intent.status.c_str(), alloc), alloc);
    }
    const Value& hints = command.HasMember("hints") ? command["hints"] : filters;
    if (!intent.team.empty()) {
        filters.AddMember("team", rapidjson::Value(intent.team.c_str(), alloc), alloc);
    } else if (hints.IsObject() && hints.HasMember("team")) {
        filters.AddMember("team", rapidjson::Value(S(hints, "team").c_str(), alloc), alloc);
    }
    if (!intent.project.empty()) {
        filters.AddMember("project", rapidjson::Value(intent.project.c_str(), alloc), alloc);
    } else if (hints.IsObject() && hints.HasMember("project")) {
        filters.AddMember("project", rapidjson::Value(S(hints, "project").c_str(), alloc), alloc);
    }
    if (hints.IsObject() && intent.team.empty() && intent.project.empty()) {
        if (hints.HasMember("team")) {
            filters.AddMember("team", rapidjson::Value(S(hints, "team").c_str(), alloc), alloc);
        }
        if (hints.HasMember("project")) {
            filters.AddMember("project", rapidjson::Value(S(hints, "project").c_str(), alloc), alloc);
        }
    }
    filters.AddMember("min_confidence", U(command, "min_confidence", 0), alloc);
    filters.AddMember("min_importance", U(command, "min_importance", 0), alloc);

    const uint64_t tenant_hash = U(command, "tenant_hash");
    const uint64_t start_ms = U(command, "start_time_ms", 0);
    const uint64_t end_ms = U(command, "end_time_ms", UINT64_MAX);
    const uint64_t max_tokens = U(command, "max_prompt_tokens");
    uint64_t tokens = 0;
    ContextPack pack;
    pack.query_understanding_source = intent.source;
    pack.staleness_policy = intent.staleness_policy;
    const auto candidate_nodes = Traverse(state, command);
    if (command.HasMember("include_entities") && command["include_entities"].IsBool() &&
        command["include_entities"].GetBool()) {
        for (auto node_hash : candidate_nodes) {
            for (const auto& [key, entity] : state.entities) {
                if (std::get<0>(key) != tenant_hash || std::get<1>(key) != node_hash) {
                    continue;
                }
                if (!EntityMatches(entity, intent)) {
                    continue;
                }
                if (tokens + entity.token_estimate > max_tokens) {
                    continue;
                }
                tokens += entity.token_estimate;
                pack.entity_hashes.push_back(entity.entity_hash);
            }
        }
    }
    if (command.HasMember("include_summaries") && command["include_summaries"].IsBool() &&
        command["include_summaries"].GetBool()) {
        const uint64_t summary_tokens = U(command, "summary_token_estimate", 1);
        for (auto node_hash : candidate_nodes) {
            const auto& refs = state.summary_refs[{tenant_hash, node_hash}];
            for (auto summary_ref : refs) {
                if (ContainsId(pack.summary_refs, summary_ref)) {
                    continue;
                }
                if (tokens + summary_tokens > max_tokens) {
                    continue;
                }
                tokens += summary_tokens;
                pack.summary_refs.push_back(summary_ref);
            }
        }
    }
    for (auto node_hash : candidate_nodes) {
        for (const auto& event : state.events[{tenant_hash, node_hash}]) {
            if (!Matches(event, start_ms, end_ms, &filters)) {
                continue;
            }
            if (tokens + event.token_estimate > max_tokens) {
                continue;
            }
            tokens += event.token_estimate;
            (void)StalenessScore(event, end_ms);
            pack.event_ids.push_back(event.event_id_hash);
        }
    }
    if (!pack.entity_hashes.empty() && !pack.summary_refs.empty()) {
        pack.sections = {"entity_state", "summary_context", "current_state"};
    } else if (!pack.entity_hashes.empty()) {
        pack.sections = {"entity_state", "current_state"};
    } else if (!pack.summary_refs.empty()) {
        pack.sections = {"summary_context", "current_state"};
    }
    pack.selected_tokens = tokens;
    return pack;
}

std::vector<uint64_t> QueryResourceChunks(State& state, const Value& command) {
    const uint64_t tenant_hash = U(command, "tenant_hash");
    const auto query = VectorFromJson(command["query_vector"]);
    const uint64_t top_k = U(command, "top_k", 1);
    const Value* filters = command.HasMember("filters") ? &command["filters"] : nullptr;
    std::vector<std::pair<double, uint64_t>> scored;
    for (const auto& [key, chunks] : state.resources_by_chunk) {
        if (key.first != tenant_hash) {
            continue;
        }
        for (const auto& chunk : chunks) {
            if (filters != nullptr && filters->IsObject() && filters->HasMember("raw_uri") &&
                chunk.raw_uri != S(*filters, "raw_uri")) {
                continue;
            }
            if (filters != nullptr && filters->IsObject() && filters->HasMember("resource_type") &&
                chunk.resource_type != S(*filters, "resource_type")) {
                continue;
            }
            if (filters != nullptr && filters->IsObject() && filters->HasMember("source_ref") &&
                chunk.source_ref != S(*filters, "source_ref")) {
                continue;
            }
            scored.push_back({Dot(query, state.embeddings[{tenant_hash, chunk.chunk_hash}]),
                              chunk.chunk_hash});
        }
    }
    std::sort(scored.begin(), scored.end(), [](const auto& left, const auto& right) {
        if (left.first != right.first) {
            return left.first > right.first;
        }
        return left.second < right.second;
    });
    std::vector<uint64_t> ids;
    for (const auto& [score, id] : scored) {
        if (score <= 0.0 || ids.size() >= top_k) {
            break;
        }
        ids.push_back(id);
    }
    return ids;
}

void AssertParityGates(State& state, const Value& command, const std::string& label) {
    const uint64_t tenant_hash = U(command, "tenant_hash");
    uint64_t passed = 0;

    ExpectEventsExist(state, tenant_hash, U64Array(command["expect_api_event_ids"]), label);
    ++passed;

    ExpectEventsExist(state, tenant_hash, U64Array(command["expect_stream_event_ids"]), label);
    ++passed;

    ExpectEventsAbsent(state, tenant_hash, U64Array(command["expect_absent_event_ids"]), label);
    ++passed;

    ExpectEventsExist(state, tenant_hash, U64Array(command["expect_batch_event_ids"]), label);
    ++passed;

    const auto pack = Retrieve(state, command);
    ExpectEqual(pack.event_ids, U64Array(command["expect_retrieve_event_ids"]), label);
    ++passed;

    if (pack.selected_tokens != U(command, "expect_selected_tokens_eq")) {
        Fail(label + " selected token count mismatch");
    }
    ++passed;

    const auto& compressions =
        state.compressions[{tenant_hash, U(command, "approval_node_hash")}];
    std::vector<uint64_t> compression_ids;
    for (const auto& compression : compressions) {
        compression_ids.push_back(compression.compression_id_hash);
    }
    for (const auto id : U64Array(command["expect_compression_ids"])) {
        if (!ContainsId(compression_ids, id)) {
            Fail(label + " missing compression " + std::to_string(id));
        }
    }
    ++passed;

    if (command.HasMember("expect_compression_source_event_ids")) {
        std::vector<uint64_t> actual_sources;
        for (const auto& compression : compressions) {
            actual_sources.insert(actual_sources.end(),
                                  compression.source_event_ids.begin(),
                                  compression.source_event_ids.end());
        }
        for (const auto id : U64Array(command["expect_compression_source_event_ids"])) {
            if (!ContainsId(actual_sources, id)) {
                Fail(label + " missing compression source event " + std::to_string(id));
            }
        }
        ++passed;
    }

    if (command.HasMember("hints") && command["hints"].IsObject() &&
        command["hints"].HasMember("query_embedding_model")) {
        const auto& hints = command["hints"];
        if (S(hints, "query_embedding_model") !=
            "sentence-transformers/all-MiniLM-L6-v2") {
            Fail(label + " query embedding model mismatch");
        }
        if (S(hints, "reranker_model") != "BAAI/bge-reranker-base") {
            Fail(label + " reranker model mismatch");
        }
        if (S(hints, "provider") != "local OSS") {
            Fail(label + " provider mismatch");
        }
        ++passed;
    }

    bool found_resource = false;
    for (const auto chunk_hash : U64Array(command["expect_resource_chunk_any"])) {
        const auto it = state.resources_by_chunk.find({tenant_hash, chunk_hash});
        if (it != state.resources_by_chunk.end() && !it->second.empty()) {
            found_resource = true;
            break;
        }
    }
    if (!found_resource) {
        Fail(label + " missing resource chunk evidence");
    }
    const auto child_it = state.children.find({tenant_hash, U(command, "root_node_hash")});
    if (child_it == state.children.end() ||
        child_it->second.size() < U(command, "expect_child_count_gte")) {
        Fail(label + " child count below parity threshold");
    }
    ++passed;

    if (passed != U(command, "expect_passed_gates")) {
        Fail(label + " parity gate count mismatch");
    }
}

void RunCommand(State& state, const Value& command, const std::string& label) {
    const auto kind = Kind(command);
    if (kind == "context_upsert_node") {
        if (!command.HasMember("record")) {
            return;
        }
        const auto& r = command["record"];
        NodeMeta meta;
        meta.parent_hash = U(r, "parent_hash");
        meta.child_count = U(r, "child_count");
        meta.updated_at_ms = U(r, "updated_at_ms");
        state.nodes[{U(r, "tenant_hash"), U(r, "node_hash")}] = meta;
    } else if (kind == "context_get_node") {
        if (!command.HasMember("expect_node")) {
            return;
        }
        const auto meta = state.nodes[{U(command, "tenant_hash"), U(command, "node_hash")}];
        const auto& expected = command["expect_node"];
        if (expected.HasMember("child_count") && meta.child_count != U(expected, "child_count")) {
            Fail(label + " child_count mismatch");
        }
        if (expected.HasMember("updated_at_ms") && meta.updated_at_ms != U(expected, "updated_at_ms")) {
            Fail(label + " updated_at_ms mismatch");
        }
        if (expected.HasMember("parent_hash") && meta.parent_hash != U(expected, "parent_hash")) {
            Fail(label + " parent_hash mismatch");
        }
    } else if (kind == "context_upsert_child_ref") {
        const auto& r = command["record"];
        const auto key = std::make_pair(U(r, "tenant_hash"), U(r, "parent_hash"));
        auto& children = state.children[key];
        const auto child_hash = U(r, "child_hash");
        if (std::find(children.begin(), children.end(), child_hash) == children.end()) {
            children.push_back(child_hash);
        }
        auto node_it = state.nodes.find(key);
        if (node_it != state.nodes.end()) {
            node_it->second.child_count = children.size();
            node_it->second.updated_at_ms =
                std::max(node_it->second.updated_at_ms, U(r, "updated_at_ms"));
        }
    } else if (kind == "context_query_children") {
        auto actual = state.children[{U(command, "tenant_hash"), U(command, "parent_hash")}];
        ExpectEqual(actual, U64Array(command["expect_child_hashes"]), label);
    } else if (kind == "context_upsert_embedding") {
        const auto& r = command["record"];
        state.embeddings[{U(r, "tenant_hash"), U(r, "node_hash")}] = VectorFromJson(r["vector"]);
    } else if (kind == "context_query_embeddings") {
        std::vector<uint64_t> refs;
        const uint64_t tenant_hash = U(command, "tenant_hash");
        for (auto ref_hash : U64Array(command["ref_hashes"])) {
            if (state.embeddings.count({tenant_hash, ref_hash}) != 0) {
                refs.push_back(ref_hash);
            }
        }
        ExpectEqual(refs, U64Array(command["expect_ref_hashes"]), label);
    } else if (kind == "context_assert_summary_embeddings") {
        std::vector<uint64_t> refs;
        const uint64_t tenant_hash = U(command, "tenant_hash");
        for (auto node_hash : U64Array(command["node_hashes"])) {
            if (state.summary_counts[{tenant_hash, node_hash}] > 0 &&
                state.embeddings.count({tenant_hash, node_hash}) != 0) {
                refs.push_back(node_hash);
            }
        }
        ExpectEqual(refs, U64Array(command["expect_ref_hashes"]), label);
    } else if (kind == "context_upsert_entity") {
        const auto& r = command["record"];
        Entity entity;
        entity.tenant_hash = U(r, "tenant_hash");
        entity.node_hash = U(r, "node_hash");
        entity.entity_hash = U(r, "entity_hash");
        entity.type = U(r, "entity_type", U(r, "type", 1));
        entity.name = S(r, "name");
        entity.value = S(r, "value");
        entity.event_type = S(r, "event_type");
        entity.team = S(r, "team");
        entity.project = S(r, "project");
        entity.status = S(r, "status");
        entity.updated_at_ms = U(r, "updated_at_ms");
        entity.valid_from_ms = U(r, "valid_from_ms", entity.updated_at_ms);
        entity.confidence = U(r, "confidence", 90);
        entity.token_estimate = U(r, "token_estimate", 1);
        if (r.HasMember("source_event_hashes")) {
            entity.source_event_hashes = U64Array(r["source_event_hashes"]);
        }
        UpsertEntity(state, entity);
    } else if (kind == "context_query_entities") {
        const auto entities = QueryEntities(state, U(command, "tenant_hash"),
                                            U(command, "node_hash"),
                                            U64Array(command["entity_hashes"]));
        std::vector<uint64_t> hashes;
        for (const auto& entity : entities) {
            hashes.push_back(entity.entity_hash);
        }
        ExpectEqual(hashes, U64Array(command["expect_entity_hashes"]), label);
    } else if (kind == "context_write_event") {
        if (!command.HasMember("record")) {
            return;
        }
        const auto& r = command["record"];
        Event e;
        e.tenant_hash = U(r, "tenant_hash");
        e.node_hash = U(r, "node_hash");
        e.event_id_hash = U(r, "event_id_hash");
        e.event_time_ms = U(r, "event_time_ms");
        e.event_type = S(r, "event_type");
        e.team = S(r, "team");
        e.project = S(r, "project");
        e.status = S(r, "status");
        e.confidence = U(r, "confidence");
        e.importance = U(r, "importance");
        e.token_estimate = U(r, "token_estimate", 16);
        e.payload = S(r, "payload");
        WriteEvent(state, e.tenant_hash, e.node_hash, e);
    } else if (kind == "context_query_events") {
        if (!command.HasMember("expect_event_ids")) {
            return;
        }
        ExpectEqual(QueryEvents(state, command), U64Array(command["expect_event_ids"]), label);
    } else if (kind == "context_write_index_ref") {
        if (!command.HasMember("record")) {
            return;
        }
        AddIndexRef(state, command["record"]);
    } else if (kind == "context_query_index") {
        if ((!command.HasMember("index_value") && !command.HasMember("index_value_hash")) ||
            !command.HasMember("expect_event_ids")) {
            return;
        }
        ExpectEqual(QueryIndexRefs(state, command), U64Array(command["expect_event_ids"]), label);
    } else if (kind == "context_query_index_and") {
        ExpectEqual(QueryIndexIntersection(state, command), U64Array(command["expect_event_ids"]), label);
    } else if (kind == "context_mark_summary_dirty") {
        if (!command.HasMember("record")) {
            return;
        }
        const auto& r = command["record"];
        state.dirty_counts[{U(r, "tenant_hash"), U(r, "node_hash")}]++;
    } else if (kind == "context_query_dirty") {
        const int actual = state.dirty_counts[{U(command, "tenant_hash"), U(command, "node_hash")}];
        if (actual != static_cast<int>(U(command, "expect_count"))) Fail(label + " dirty count mismatch");
    } else if (kind == "context_upsert_summary") {
        const auto& r = command["record"];
        const uint64_t tenant_hash = U(r, "tenant_hash");
        const uint64_t node_hash = U(r, "node_hash");
        state.summary_counts[{tenant_hash, node_hash}]++;
        if (!ContainsId(state.summary_refs[{tenant_hash, node_hash}], node_hash)) {
            state.summary_refs[{tenant_hash, node_hash}].push_back(node_hash);
        }
    } else if (kind == "context_query_summaries") {
        const int actual = state.summary_counts[{U(command, "tenant_hash"), U(command, "node_hash")}];
        if (actual != static_cast<int>(U(command, "expect_count"))) Fail(label + " summary count mismatch");
    } else if (kind == "context_write_compression") {
        const auto& r = command["record"];
        Compression compression;
        compression.compression_id_hash = U(r, "compression_id_hash");
        compression.node_hash = U(r, "node_hash");
        compression.source_start_ms = U(r, "source_start_ms");
        compression.source_end_ms = U(r, "source_end_ms");
        compression.compressed_time_ms = U(r, "compressed_time_ms");
        compression.summary = S(r, "compressed_summary");
        if (r.HasMember("source_event_ids")) {
            compression.source_event_ids = U64Array(r["source_event_ids"]);
        }
        state.compressions[{U(r, "tenant_hash"), compression.node_hash}].push_back(std::move(compression));
    } else if (kind == "context_query_compression") {
        std::vector<uint64_t> ids;
        std::vector<uint64_t> source_ids;
        for (const auto& compression : state.compressions[{U(command, "tenant_hash"), U(command, "node_hash")}]) {
            if (compression.source_end_ms >= U(command, "start_time_ms", 0) &&
                compression.source_start_ms <= U(command, "end_time_ms", UINT64_MAX)) {
                ids.push_back(compression.compression_id_hash);
                source_ids.insert(source_ids.end(), compression.source_event_ids.begin(),
                                  compression.source_event_ids.end());
            }
        }
        if (command.HasMember("expect_compression_ids")) {
            ExpectEqual(ids, U64Array(command["expect_compression_ids"]), label);
        } else if (ids.size() != U(command, "expect_count")) {
            Fail(label + " compression count mismatch");
        }
        if (command.HasMember("expect_compression_source_event_ids")) {
            ExpectEqual(source_ids, U64Array(command["expect_compression_source_event_ids"]), label);
        }
    } else if (kind == "context_write_pack_audit") {
        if (!command.HasMember("record")) {
            return;
        }
        const auto& r = command["record"];
        state.pack_audit_counts[{U(r, "tenant_hash"), U(r, "query_id_hash")}]++;
    } else if (kind == "context_query_pack_audit") {
        if (!command.HasMember("query_id_hash") || !command.HasMember("expect_count")) {
            return;
        }
        const int actual = state.pack_audit_counts[{U(command, "tenant_hash"), U(command, "query_id_hash")}];
        if (actual != static_cast<int>(U(command, "expect_count"))) Fail(label + " audit count mismatch");
    } else if (kind == "context_traverse_tree") {
        ExpectEqual(Traverse(state, command), U64Array(command["expect_node_hashes"]), label);
    } else if (kind == "context_build_pack") {
        std::vector<uint64_t> ids;
        uint64_t tokens = 0;
        for (auto node_hash : U64Array(command["candidate_node_hashes"])) {
            for (const auto& event : state.events[{U(command, "tenant_hash"), node_hash}]) {
                if (tokens + event.token_estimate <= U(command, "max_prompt_tokens")) {
                    tokens += event.token_estimate;
                    ids.push_back(event.event_id_hash);
                }
            }
        }
        ids.resize(std::min(ids.size(), U64Array(command["expect_event_ids"]).size()));
        ExpectEqual(ids, U64Array(command["expect_event_ids"]), label);
    } else if (kind == "context_ingest_raw_event") {
        IngestRawEvent(state, U(command, "tenant_hash"), S(command, "raw_text"), command["hints"]);
    } else if (kind == "context_api_ingest_raw_event") {
        const uint64_t tenant_hash = U(command, "tenant_hash");
        const auto key = std::make_tuple(tenant_hash, S(command, "endpoint"), S(command, "idempotency_key"));
        const bool created = state.api_idempotency_keys.insert(key).second;
        if (command.HasMember("expect_created") && command["expect_created"].IsBool() &&
            created != command["expect_created"].GetBool()) {
            Fail(label + " API idempotency created flag mismatch");
        }
        if (created) {
            const auto event = IngestRawEvent(state, tenant_hash, S(command, "raw_text"), command["hints"]);
            if (event.event_id_hash != U(command, "expect_event_id_hash")) {
                Fail(label + " API ingest event id mismatch");
            }
        } else if (CountEventId(state, tenant_hash, U(command, "expect_event_id_hash")) != 0) {
            Fail(label + " duplicate API idempotency key wrote a new event");
        }
    } else if (kind == "context_batch_ingest_raw_events") {
        std::vector<uint64_t> event_ids;
        std::vector<uint64_t> leaf_hashes;
        const uint64_t tenant_hash = U(command, "tenant_hash");
        for (const auto& item : command["events"].GetArray()) {
            const auto& h = item["hints"];
            auto event = IngestRawEvent(state, tenant_hash, S(item, "raw_text"), h);
            event_ids.push_back(event.event_id_hash);
            leaf_hashes.push_back(U(h, "leaf_node_hash"));
        }
        ExpectEqual(event_ids, U64Array(command["expect_event_ids"]), label);
        ExpectEqual(leaf_hashes, U64Array(command["expect_leaf_node_hashes"]), label);
    } else if (kind == "context_stream_ingest_raw_events") {
        std::vector<uint64_t> event_ids;
        std::vector<uint64_t> committed_offsets;
        const uint64_t tenant_hash = U(command, "tenant_hash");
        const std::string stream_name = S(command, "stream_name");
        for (const auto& item : command["events"].GetArray()) {
            const uint64_t partition = U(item, "partition");
            const uint64_t offset = U(item, "offset");
            const auto checkpoint_key = std::make_tuple(tenant_hash, stream_name, partition);
            const auto checkpoint_it = state.stream_committed_offsets.find(checkpoint_key);
            if (checkpoint_it != state.stream_committed_offsets.end() &&
                offset <= checkpoint_it->second) {
                continue;
            }
            const auto event = IngestRawEvent(state, tenant_hash, S(item, "raw_text"), item["hints"]);
            event_ids.push_back(event.event_id_hash);
            committed_offsets.push_back(offset);
            state.stream_committed_offsets[checkpoint_key] = offset;
        }
        ExpectEqual(event_ids, U64Array(command["expect_event_ids"]), label);
        ExpectEqual(committed_offsets, U64Array(command["expect_committed_offsets"]), label);
    } else if (kind == "context_extract_query") {
        auto intent = QueryIntent(command);
        const auto& expected = command["expect_intent"];
        if (intent.event_type != S(expected, "event_type") || intent.status != S(expected, "status")) {
            Fail(label + " intent mismatch");
        }
        if (command.HasMember("expect_query_plan")) {
            const auto& expected_plan = command["expect_query_plan"];
            if (expected_plan.HasMember("source") && intent.source != S(expected_plan, "source")) {
                Fail(label + " query plan source mismatch");
            }
            const auto expected_event_type = NestedS(expected_plan, "filters", "event_type");
            if (!expected_event_type.empty() && intent.event_type != expected_event_type) {
                Fail(label + " query plan event_type mismatch");
            }
            const auto expected_status = NestedS(expected_plan, "filters", "status");
            if (!expected_status.empty() && intent.status != expected_status) {
                Fail(label + " query plan status mismatch");
            }
        }
    } else if (kind == "context_retrieve") {
        const auto pack = Retrieve(state, command);
        ExpectEqual(pack.event_ids, U64Array(command["expect_event_ids"]), label);
        AssertPackContract(pack, command, label);
        if (command.HasMember("expect_selected_tokens_eq")) {
            if (pack.selected_tokens != U(command, "expect_selected_tokens_eq")) {
                Fail(label + " selected token count mismatch");
            }
        }
    } else if (kind == "context_ingest_resource") {
        const auto& h = command["hints"];
        const uint64_t tenant = U(command, "tenant_hash");
        const uint64_t resource_hash = U(h, "resource_hash");
        state.children[{tenant, U(h, "parent_hash")}].push_back(resource_hash);
        std::vector<uint64_t> chunks;
        for (const auto& chunk : command["chunks"].GetArray()) {
            ResourceChunk c;
            c.tenant_hash = tenant;
            c.resource_hash = resource_hash;
            c.chunk_hash = U(chunk, "chunk_hash");
            c.raw_uri = S(command, "raw_uri");
            c.resource_type = S(command, "resource_type");
            c.source_ref = S(chunk, "source_ref");
            c.text = S(chunk, "text");
            c.token_estimate = U(chunk, "token_estimate", 32);
            state.resources_by_chunk[{tenant, c.chunk_hash}].push_back(c);
            state.embeddings[{tenant, c.chunk_hash}] = VectorFromJson(chunk["vector"]);
            chunks.push_back(c.chunk_hash);
        }
        ExpectEqual(chunks, U64Array(command["expect_chunk_hashes"]), label);
    } else if (kind == "context_query_resource_chunks") {
        ExpectEqual(QueryResourceChunks(state, command), U64Array(command["expect_chunk_hashes"]), label);
    } else if (kind == "context_extract_resource_events") {
        std::vector<uint64_t> ids;
        uint64_t offset = 0;
        for (auto chunk_hash : U64Array(command["source_chunk_hashes"])) {
            const auto chunk = state.resources_by_chunk[{U(command, "tenant_hash"), chunk_hash}].front();
            auto event = ExtractEvent(chunk.text, command["hints"]);
            event.event_id_hash = U(command["hints"], "event_id_base_hash") + offset;
            event.event_time_ms = U(command["hints"], "event_time_ms") + offset;
            const uint64_t tenant_hash = U(command, "tenant_hash");
            const uint64_t node_hash = U(command["hints"], "node_hash");
            WriteEvent(state, tenant_hash, node_hash, event);
            UpsertEntityFromHints(state, tenant_hash, node_hash, event, command["hints"]);
            ids.push_back(event.event_id_hash);
            offset++;
        }
        ExpectEqual(ids, U64Array(command["expect_event_ids"]), label);
    } else if (kind == "context_ingest_feedback") {
        auto event = ExtractEvent(S(command, "feedback_text"), command["hints"]);
        event.event_id_hash = U(command["hints"], "event_id_hash");
        event.event_time_ms = U(command["hints"], "event_time_ms");
        const uint64_t tenant_hash = U(command, "tenant_hash");
        const uint64_t node_hash = U(command, "node_hash");
        WriteEvent(state, tenant_hash, node_hash, event);
        UpsertEntityFromHints(state, tenant_hash, node_hash, event, command["hints"]);
    } else if (kind == "context_retrieve_with_resources") {
        auto pack = Retrieve(state, command);
        ExpectEqual(pack.event_ids, U64Array(command["expect_event_ids"]), label);
        rapidjson::Document chunk_query;
        chunk_query.SetObject();
        auto& alloc = chunk_query.GetAllocator();
        chunk_query.AddMember("tenant_hash", U(command, "tenant_hash"), alloc);
        rapidjson::Value query_vector;
        query_vector.CopyFrom(command["query_vector"], alloc);
        chunk_query.AddMember("query_vector", query_vector, alloc);
        chunk_query.AddMember("top_k", U(command, "resource_top_k", 1), alloc);
        rapidjson::Value filters;
        filters.CopyFrom(command["resource_filters"], alloc);
        chunk_query.AddMember("filters", filters, alloc);
        const uint64_t tenant = U(command, "tenant_hash");
        const uint64_t max_tokens = U(command, "max_prompt_tokens");
        for (auto chunk_hash : QueryResourceChunks(state, chunk_query)) {
            const auto& chunks = state.resources_by_chunk[{tenant, chunk_hash}];
            if (chunks.empty()) {
                continue;
            }
            const uint64_t chunk_tokens = chunks.front().token_estimate;
            if (pack.selected_tokens + chunk_tokens <= max_tokens) {
                pack.selected_tokens += chunk_tokens;
                pack.chunk_hashes.push_back(chunk_hash);
            }
        }
        if (!pack.chunk_hashes.empty()) {
            if (!pack.entity_hashes.empty() && !pack.summary_refs.empty()) {
                pack.sections = {"entity_state", "summary_context", "current_state", "selected_evidence"};
            } else if (!pack.entity_hashes.empty()) {
                pack.sections = {"entity_state", "current_state", "selected_evidence"};
            } else if (!pack.summary_refs.empty()) {
                pack.sections = {"summary_context", "current_state", "selected_evidence"};
            } else {
                pack.sections = {"current_state", "selected_evidence"};
            }
        }
        ExpectEqual(pack.chunk_hashes, U64Array(command["expect_chunk_hashes"]), label);
        AssertPackContract(pack, command, label);
        if (command.HasMember("expect_selected_tokens_eq")) {
            if (pack.selected_tokens != U(command, "expect_selected_tokens_eq")) {
                Fail(label + " selected token count mismatch");
            }
        }
    } else if (kind == "context_assert_parity_gates") {
        AssertParityGates(state, command, label);
    } else if (kind.rfind("context_", 0) == 0 || kind == "existing_test") {
        // Other context surfaces are schema/path validated by the Python hook.
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 2) {
            Fail("usage: cpp_unified_context_contract <corpus.json>");
        }
        std::ifstream input(argv[1]);
        if (!input) {
            Fail("failed to open corpus");
        }
        rapidjson::IStreamWrapper stream(input);
        rapidjson::Document doc;
        doc.ParseStream(stream);
        if (doc.HasParseError() || !doc.IsObject() || !doc.HasMember("cases")) {
            Fail("failed to parse corpus");
        }
        State state;
        uint64_t context_steps = 0;
        std::set<std::string> context_cases;
        for (const auto& test_case : doc["cases"].GetArray()) {
            const std::string case_name = S(test_case, "name");
            bool has_context = false;
            for (const auto& step : test_case["steps"].GetArray()) {
                const auto& command = step["command"];
                const auto kind = Kind(command);
                if (kind.rfind("context_", 0) == 0) {
                    has_context = true;
                    context_steps++;
                }
                RunCommand(state, command, case_name + "." + S(step, "name"));
            }
            if (has_context) {
                context_cases.insert(case_name);
            }
        }
        if (doc.HasMember("coverage")) {
            if (context_cases.empty()) {
                Fail("current shared corpus must include context cases");
            }
        } else {
            const std::vector<std::string> required = {
                "context_tree_event_pack_replay",
                "context_raw_extraction_query_pipeline",
                "context_incident_time_aware_pipeline",
                "context_resource_feedback_second_query_pipeline",
                "context_pack_token_budget_parity",
                "context_layered_resource_parsing_pipeline",
                "context_batch_extraction_query_ingestion_x8",
                "context_stream_batch_api_ingestion_compression",
                "context_eight_parity_gates",
                "context_nine_ingestion_compression_parity_gates",
                "context_ten_model_config_parity_gates",
            };
            for (const auto& name : required) {
                if (!context_cases.count(name)) {
                    Fail("missing required C++ unified context case: " + name);
                }
            }
        }
        std::cout << "C++ unified context contract passed: cases=" << context_cases.size()
                  << " context_steps=" << context_steps << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "C++ unified context contract failed: " << ex.what() << std::endl;
        return 1;
    }
}
