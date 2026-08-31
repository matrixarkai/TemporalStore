// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// native serving / memory-inventory / layer-budget helpers, split from matrixark_rust_proxy_impl.rs (textually include!d;
// shares parent use-imports + flat scope; no use-statements or mod wrapper).

fn context_pack_debug_lineage_enabled() -> bool {
    env::var("MATRIXARK_CONTEXT_PACK_DEBUG_LINEAGE")
        .ok()
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn context_pack_include_scores_enabled() -> bool {
    env::var("MATRIXARK_CONTEXT_PACK_INCLUDE_SCORES")
        .ok()
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn native_serving_ref(mut item: Value) -> Value {
    let include_lineage = context_pack_debug_lineage_enabled();
    let include_scores = context_pack_include_scores_enabled();
    if let Some(object) = item.as_object_mut() {
        if !include_lineage {
            for field in [
                "ref_hash",
                "node_hash",
                "node_path",
                "continuity_reason",
                "selection_reason",
                "source_session_ids",
                "source_entity_hashes",
                "source_entity_types",
                "source_roles",
                "source_role_counts",
                "budget_source_roles",
                "budget_source_role_counts",
                "source_hook_types",
                "source_hook_type_counts",
                "source_codex_events",
                "source_codex_event_counts",
                "source_memory_scopes",
                "source_session_continuities",
                "source_extraction_phases",
                "source_profile_promotion_policies",
                "source_profile_promotion_blockers",
                "source_ref",
            ] {
                object.remove(field);
            }
        }
        if !include_scores {
            for field in [
                "token_estimate",
                "score",
                "continuity_boost",
                "cross_session_rerank_boost",
            ] {
                object.remove(field);
            }
        }
    }
    item
}

fn native_serving_refs(refs: &[Value]) -> Vec<Value> {
    refs.iter().cloned().map(native_serving_ref).collect()
}

fn increment_inventory_count(inventory: &mut Value, layer: &str, field: &str) {
    let Some(bucket) = inventory.get_mut(layer).and_then(Value::as_object_mut) else {
        return;
    };
    let current = bucket.get(field).and_then(Value::as_u64).unwrap_or(0);
    bucket.insert(field.to_string(), json!(current + 1));
}

fn inventory_layer_available(inventory: &Value, layer: &str) -> bool {
    inventory
        .get(layer)
        .and_then(Value::as_object)
        .map(|bucket| bucket.values().any(|value| value.as_u64().unwrap_or(0) > 0))
        .unwrap_or(false)
}

fn native_retrieval_memory_inventory(records: &[Value], query_scope: Option<&Value>) -> Value {
    let mut inventory = json!({
        "session": {
            "context_events": 0,
            "context_segments": 0,
            "context_entities": 0,
            "context_indexes": 0,
            "context_summaries": 0,
            "summary_dirty_markers": 0
        },
        "profile": {
            "context_entities": 0,
            "context_indexes": 0,
            "context_summaries": 0,
            "summary_dirty_markers": 0
        },
        "shared": {
            "resource_chunks": 0,
            "resource_manifests": 0,
            "skill_sections": 0,
            "skill_manifests": 0,
            "context_entities": 0,
            "context_indexes": 0
        },
        "available_layers": [],
        "query_scope": {
            "session_scope": query_scope.map(session_scope_mode).unwrap_or("prefer"),
            "has_session_id": query_scope
                .and_then(|scope| scope.get("session_id"))
                .and_then(Value::as_str)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false),
            "has_user_id": query_scope
                .and_then(|scope| scope.get("user_id"))
                .and_then(Value::as_str)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false),
            "has_tenant_id": query_scope
                .and_then(|scope| scope.get("tenant_id"))
                .and_then(Value::as_str)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
        },
        "profile_records_available_but_not_selected": false
    });

    for record in records {
        let record_type = string_field(record, "record_type");
        let memory_scope = string_field(record, "memory_scope")
            .trim()
            .to_ascii_lowercase();
        let session_continuity = string_field(record, "session_continuity")
            .trim()
            .to_ascii_lowercase();
        let data_model = string_field(record, "data_model")
            .trim()
            .to_ascii_lowercase();
        let sharing_scope = record_scope_value(record)
            .and_then(|scope| scope.get("sharing_scope"))
            .and_then(Value::as_str)
            .or_else(|| record.get("sharing_scope").and_then(Value::as_str))
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let is_shared = matches!(sharing_scope.as_str(), "tenant_shared" | "global_shared")
            || matches!(
                record_type,
                "resource_chunk" | "resource_manifest" | "skill_section" | "skill_manifest" | "skill_registry_update"
            )
            || matches!(data_model.as_str(), "resource_chunk" | "skill_section");
        let is_profile = matches!(
            memory_scope.as_str(),
            "user_profile" | "profile" | "cross_session_profile"
        ) || data_model == "context_profile_entity"
            || (matches!(
                record_type,
                "context_entity" | "context_embedding" | "context_summary" | "context_summary_dirty"
            ) && session_continuity == "cross_session");
        let is_session = matches!(memory_scope.as_str(), "session" | "session_memory")
            || session_continuity == "same_session";

        if is_shared {
            match record_type {
                "resource_chunk" => increment_inventory_count(&mut inventory, "shared", "resource_chunks"),
                "resource_manifest" => increment_inventory_count(&mut inventory, "shared", "resource_manifests"),
                "skill_section" => increment_inventory_count(&mut inventory, "shared", "skill_sections"),
                "skill_manifest" | "skill_registry_update" => {
                    increment_inventory_count(&mut inventory, "shared", "skill_manifests")
                }
                "context_entity" => increment_inventory_count(&mut inventory, "shared", "context_entities"),
                "context_index" => increment_inventory_count(&mut inventory, "shared", "context_indexes"),
                _ => {}
            }
            continue;
        }

        if is_profile {
            match record_type {
                "context_entity" => increment_inventory_count(&mut inventory, "profile", "context_entities"),
                "context_index" => increment_inventory_count(&mut inventory, "profile", "context_indexes"),
                "context_summary" => increment_inventory_count(&mut inventory, "profile", "context_summaries"),
                "context_summary_dirty" => {
                    increment_inventory_count(&mut inventory, "profile", "summary_dirty_markers")
                }
                _ => {}
            }
            continue;
        }

        if is_session || matches!(record_type, "context_event" | "context_segment") {
            match record_type {
                "context_event" => increment_inventory_count(&mut inventory, "session", "context_events"),
                "context_segment" => increment_inventory_count(&mut inventory, "session", "context_segments"),
                "context_entity" => increment_inventory_count(&mut inventory, "session", "context_entities"),
                "context_index" => increment_inventory_count(&mut inventory, "session", "context_indexes"),
                "context_summary" => increment_inventory_count(&mut inventory, "session", "context_summaries"),
                "context_summary_dirty" => {
                    increment_inventory_count(&mut inventory, "session", "summary_dirty_markers")
                }
                _ => {}
            }
        }
    }

    let mut available_layers = Vec::new();
    for layer in ["session", "profile", "shared"] {
        if inventory_layer_available(&inventory, layer) {
            available_layers.push(json!(layer));
        }
    }
    let has_session_memory = inventory_layer_available(&inventory, "session");
    let has_profile_memory = inventory_layer_available(&inventory, "profile");
    let has_shared_memory = inventory_layer_available(&inventory, "shared");
    if let Some(object) = inventory.as_object_mut() {
        object.insert("available_layers".to_string(), Value::Array(available_layers));
        object.insert("has_session_memory".to_string(), json!(has_session_memory));
        object.insert("has_profile_memory".to_string(), json!(has_profile_memory));
        object.insert("has_shared_memory".to_string(), json!(has_shared_memory));
    }
    inventory
}

fn native_serving_memory_layer_budget(value: &Value) -> Value {
    if context_pack_debug_lineage_enabled() {
        return value.clone();
    }
    let mut compact = value.clone();
    if let Some(object) = compact.as_object_mut() {
        for field in [
            "by_source_role",
            "by_hook_type",
            "by_codex_event",
            "source_message_counts_by_role",
            "source_hook_counts_by_type",
            "source_codex_event_counts_by_event",
            "by_profile_promotion_policy",
            "by_profile_promotion_blocker",
        ] {
            object.remove(field);
        }
    }
    compact
}

fn native_serving_memory_layer_pressure(value: &Value) -> Value {
    if context_pack_debug_lineage_enabled() {
        return value.clone();
    }
    let mut compact = value.clone();
    let lineage_dimensions: BTreeSet<&str> = [
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
        "by_profile_promotion_policy",
        "by_profile_promotion_blocker",
    ]
    .into_iter()
    .collect();
    if let Some(object) = compact.as_object_mut() {
        for list_field in ["pressure_dimensions", "dropped_dimensions"] {
            if let Some(values) = object.get_mut(list_field).and_then(Value::as_array_mut) {
                values.retain(|value| {
                    value
                        .as_str()
                        .map(|dimension| !lineage_dimensions.contains(dimension))
                        .unwrap_or(true)
                });
            }
        }
        if let Some(dimensions) = object.get_mut("by_dimension").and_then(Value::as_object_mut) {
            dimensions.retain(|dimension, _| !lineage_dimensions.contains(dimension.as_str()));
        }
        for field in [
            "assistant_memory_pressure",
            "user_memory_pressure",
            "tool_memory_pressure",
            "assistant_source_message_pressure",
            "user_source_message_pressure",
            "tool_source_message_pressure",
            "hook_boundary_source_pressure",
            "after_llm_source_pressure",
            "tool_result_source_pressure",
            "stop_event_source_pressure",
            "post_tool_use_source_pressure",
        ] {
            object.remove(field);
        }
    }
    compact
}

fn native_serving_dropped_refs(mut dropped_refs: Value) -> Value {
    if context_pack_debug_lineage_enabled() {
        return dropped_refs;
    }
    if let Some(object) = dropped_refs.as_object_mut() {
        let dropped_ref_count = object
            .get("refs")
            .and_then(Value::as_array)
            .map(|refs| refs.len())
            .unwrap_or(0);
        object.remove("refs");
        if dropped_ref_count > 0 {
            object.insert(
                "dropped_ref_detail_available_in_audit".to_string(),
                json!(true),
            );
            object.insert("dropped_ref_count".to_string(), json!(dropped_ref_count));
        }
    }
    dropped_refs
}

fn increment_layer_bucket(breakdown: &mut Value, bucket_name: &str, key: &str, tokens: u64) {
    let Some(bucket_map) = breakdown
        .get_mut(bucket_name)
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    let bucket = bucket_map
        .entry(key.to_string())
        .or_insert_with(|| json!({"refs": 0, "tokens": 0}));
    if let Some(bucket_object) = bucket.as_object_mut() {
        let refs = bucket_object
            .get("refs")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        let total_tokens = bucket_object
            .get("tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + tokens;
        bucket_object.insert("refs".to_string(), json!(refs));
        bucket_object.insert("tokens".to_string(), json!(total_tokens));
    }
}

fn source_layer_values(
    record: &Value,
    source_field: &str,
    fallback_field: &str,
    default_value: &str,
) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(source_values) = record.get(source_field).and_then(Value::as_array) {
        for value in source_values
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if !values.iter().any(|existing| existing == value) {
                values.push(value.to_string());
            }
        }
    }
    if values.is_empty() {
        if let Some(value) = record
            .get(fallback_field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            values.push(value.to_string());
        }
    }
    if values.is_empty() {
        values.push(default_value.to_string());
    }
    values
}

fn selected_ref_layer_budget(refs: &[Value]) -> Value {
    let mut breakdown = json!({
        "by_memory_layer": {},
        "by_memory_scope": {},
        "by_session_continuity": {},
        "by_extraction_phase": {},
        "by_ref_type": {},
        "by_entity_type": {},
        "by_source_role": {},
        "by_hook_type": {},
        "by_codex_event": {},
        "source_message_counts_by_role": {},
        "source_hook_counts_by_type": {},
        "source_codex_event_counts_by_event": {},
        "by_profile_promotion_policy": {},
        "by_profile_promotion_blocker": {},
        "final_session_boundary_ref_count": 0,
        "provisional_ref_count": 0,
        "final_ref_count": 0,
        "total_selected_refs": refs.len(),
        "total_selected_tokens": 0
    });
    for item in refs {
        let tokens = item
            .get("token_estimate")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let total_tokens = breakdown
            .get("total_selected_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + tokens;
        if let Some(object) = breakdown.as_object_mut() {
            object.insert("total_selected_tokens".to_string(), json!(total_tokens));
        }
        let memory_layer = item
            .get("memory_layer")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                let ref_type = item
                    .get("ref_type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                broad_memory_layer(item, ref_type)
            });
        if !memory_layer.is_empty() {
            increment_layer_bucket(&mut breakdown, "by_memory_layer", &memory_layer, tokens);
        }
        for memory_scope in source_layer_values(item, "source_memory_scopes", "memory_scope", "unscoped") {
            increment_layer_bucket(&mut breakdown, "by_memory_scope", &memory_scope, tokens);
        }
        for session_continuity in source_layer_values(
            item,
            "source_session_continuities",
            "session_continuity",
            "neutral",
        ) {
            increment_layer_bucket(
                &mut breakdown,
                "by_session_continuity",
                &session_continuity,
                tokens,
            );
        }
        let extraction_phase = item
            .get("extraction_phase")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown");
        for source_extraction_phase in source_layer_values(
            item,
            "source_extraction_phases",
            "extraction_phase",
            "unknown",
        ) {
            increment_layer_bucket(
                &mut breakdown,
                "by_extraction_phase",
                &source_extraction_phase,
                tokens,
            );
        }
        let ref_type = item
            .get("ref_type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown");
        increment_layer_bucket(&mut breakdown, "by_ref_type", ref_type, tokens);
        for entity_type in source_layer_values(item, "source_entity_types", "entity_type", "") {
            if !entity_type.is_empty() {
                increment_layer_bucket(&mut breakdown, "by_entity_type", &entity_type, tokens);
            }
        }
        for policy in source_layer_values(item, "source_profile_promotion_policies", "profile_promotion_policy", "") {
            if !policy.is_empty() {
                increment_layer_bucket(&mut breakdown, "by_profile_promotion_policy", &policy, tokens);
            }
        }
        for blocker in source_layer_values(item, "source_profile_promotion_blockers", "profile_promotion_blocker", "") {
            if !blocker.is_empty() {
                increment_layer_bucket(&mut breakdown, "by_profile_promotion_blocker", &blocker, tokens);
            }
        }
        if let Some(roles) = item.get("source_roles").and_then(Value::as_array) {
            for role in roles
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut breakdown, "by_source_role", role, tokens);
            }
        }
        if let Some(hook_types) = item.get("source_hook_types").and_then(Value::as_array) {
            for hook_type in hook_types
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut breakdown, "by_hook_type", hook_type, tokens);
            }
        }
        if let Some(codex_events) = item.get("source_codex_events").and_then(Value::as_array) {
            for codex_event in codex_events
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut breakdown, "by_codex_event", codex_event, tokens);
            }
        }
        increment_source_count_bucket(
            &mut breakdown,
            "source_message_counts_by_role",
            item.get("source_role_counts"),
        );
        increment_source_count_bucket(
            &mut breakdown,
            "source_hook_counts_by_type",
            item.get("source_hook_type_counts"),
        );
        increment_source_count_bucket(
            &mut breakdown,
            "source_codex_event_counts_by_event",
            item.get("source_codex_event_counts"),
        );
        if item
            .get("final_session_boundary")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let count = breakdown
                .get("final_session_boundary_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            if let Some(object) = breakdown.as_object_mut() {
                object.insert("final_session_boundary_ref_count".to_string(), json!(count));
            }
        }
        if extraction_phase == "provisional" {
            let count = breakdown
                .get("provisional_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            if let Some(object) = breakdown.as_object_mut() {
                object.insert("provisional_ref_count".to_string(), json!(count));
            }
        }
        if extraction_phase == "final" {
            let count = breakdown
                .get("final_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            if let Some(object) = breakdown.as_object_mut() {
                object.insert("final_ref_count".to_string(), json!(count));
            }
        }
    }
    breakdown
}

fn ref_type_budget_from_counts(
    counts: &HashMap<String, u64>,
    token_counts: &HashMap<String, u64>,
) -> Value {
    let mut buckets = serde_json::Map::new();
    for (class_name, refs) in counts {
        if *refs == 0 {
            continue;
        }
        buckets.insert(
            class_name.clone(),
            json!({
                "refs": refs,
                "tokens": token_counts.get(class_name).copied().unwrap_or(0),
            }),
        );
    }
    Value::Object(buckets)
}

fn string_field<'a>(value: &'a Value, field: &str) -> &'a str {
    value.get(field).and_then(Value::as_str).unwrap_or("")
}

fn entity_current_state_key(record: &Value) -> Option<String> {
    if string_field(record, "record_type") != "context_entity"
        && string_field(record, "ref_type") != "entity"
    {
        return None;
    }
    let entity_type = string_field(record, "entity_type").trim().to_ascii_lowercase();
    let entity_name = string_field(record, "entity_name").trim().to_ascii_lowercase();
    if entity_type.is_empty() || entity_name.is_empty() {
        return None;
    }
    Some(format!("{entity_type}::{entity_name}"))
}

fn profile_shadow_maps(scored: &[NativeScoredCandidate]) -> (HashMap<String, (u64, String)>, HashMap<String, (u64, String)>) {
    let mut by_entity: HashMap<String, (u64, String)> = HashMap::new();
    let mut by_source_entity_hash: HashMap<String, (u64, String)> = HashMap::new();
    for candidate in scored {
        let record = &candidate.record;
        if string_field(record, "memory_scope") != "user_profile"
            || string_field(record, "session_continuity") != "cross_session"
        {
            continue;
        }
        let Some(profile_hash) = record_ref_hash(record) else {
            continue;
        };
        let updated_at_ms = record.get("updated_at_ms").and_then(Value::as_u64).unwrap_or(0);
        if let Some(key) = entity_current_state_key(record) {
            let replace = by_entity
                .get(&key)
                .map(|(existing_updated_at, _)| updated_at_ms >= *existing_updated_at)
                .unwrap_or(true);
            if replace {
                by_entity.insert(key, (updated_at_ms, profile_hash.clone()));
            }
        }
        if let Some(source_hashes) = record.get("source_entity_hashes").and_then(Value::as_array) {
            for source_hash in source_hashes {
                let source_key = source_hash
                    .as_u64()
                    .map(|value| value.to_string())
                    .or_else(|| source_hash.as_str().map(str::to_string));
                let Some(source_key) = source_key else {
                    continue;
                };
                let replace = by_source_entity_hash
                    .get(&source_key)
                    .map(|(existing_updated_at, _)| updated_at_ms >= *existing_updated_at)
                    .unwrap_or(true);
                if replace {
                    by_source_entity_hash.insert(source_key, (updated_at_ms, profile_hash.clone()));
                }
            }
        }
    }
    (by_entity, by_source_entity_hash)
}

fn profile_shadow_for_candidate(
    candidate: &NativeScoredCandidate,
    by_entity: &HashMap<String, (u64, String)>,
    by_source_entity_hash: &HashMap<String, (u64, String)>,
) -> Option<(String, &'static str)> {
    if candidate.context_class != "entity" || string_field(&candidate.record, "memory_scope") != "session" {
        return None;
    }
    if let Some(ref_hash) = record_ref_hash(&candidate.record) {
        if let Some((_, profile_hash)) = by_source_entity_hash.get(&ref_hash) {
            return Some((profile_hash.clone(), "source_entity_lineage"));
        }
    }
    let key = entity_current_state_key(&candidate.record)?;
    by_entity
        .get(&key)
        .map(|(_, profile_hash)| (profile_hash.clone(), "same_entity_identity"))
}

fn profile_shadow_maps_from_selected_refs(
    selected_refs: &[Value],
) -> (HashMap<String, (u64, String)>, HashMap<String, (u64, String)>) {
    let mut by_entity: HashMap<String, (u64, String)> = HashMap::new();
    let mut by_source_entity_hash: HashMap<String, (u64, String)> = HashMap::new();
    for selected_ref in selected_refs {
        if string_field(selected_ref, "memory_scope") != "user_profile"
            || string_field(selected_ref, "session_continuity") != "cross_session"
        {
            continue;
        }
        let Some(profile_hash) = record_ref_hash(selected_ref) else {
            continue;
        };
        let updated_at_ms = selected_ref
            .get("updated_at_ms")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if let Some(key) = entity_current_state_key(selected_ref) {
            let replace = by_entity
                .get(&key)
                .map(|(existing_updated_at, _)| updated_at_ms >= *existing_updated_at)
                .unwrap_or(true);
            if replace {
                by_entity.insert(key, (updated_at_ms, profile_hash.clone()));
            }
        }
        if let Some(source_hashes) = selected_ref.get("source_entity_hashes").and_then(Value::as_array) {
            for source_hash in source_hashes {
                let source_key = source_hash
                    .as_u64()
                    .map(|value| value.to_string())
                    .or_else(|| source_hash.as_str().map(str::to_string));
                let Some(source_key) = source_key else {
                    continue;
                };
                let replace = by_source_entity_hash
                    .get(&source_key)
                    .map(|(existing_updated_at, _)| updated_at_ms >= *existing_updated_at)
                    .unwrap_or(true);
                if replace {
                    by_source_entity_hash.insert(source_key, (updated_at_ms, profile_hash.clone()));
                }
            }
        }
    }
    (by_entity, by_source_entity_hash)
}

fn profile_shadow_for_selected_ref(
    selected_ref: &Value,
    by_entity: &HashMap<String, (u64, String)>,
    by_source_entity_hash: &HashMap<String, (u64, String)>,
) -> Option<(String, &'static str)> {
    if string_field(selected_ref, "ref_type") != "entity"
        || string_field(selected_ref, "memory_scope") != "session"
    {
        return None;
    }
    if let Some(ref_hash) = record_ref_hash(selected_ref) {
        if let Some((_, profile_hash)) = by_source_entity_hash.get(&ref_hash) {
            return Some((profile_hash.clone(), "source_entity_lineage"));
        }
    }
    let key = entity_current_state_key(selected_ref)?;
    by_entity
        .get(&key)
        .map(|(_, profile_hash)| (profile_hash.clone(), "same_entity_identity"))
}

fn native_dropped_ref_detail(
    record: &Value,
    text: &str,
    context_class: &str,
    reason: &str,
    tokens: u64,
    profile_shadow: Option<(String, &str)>,
) -> Value {
    let mut detail = json!({
        "ref_type": context_class,
        "ref_hash": record_ref_hash(record).unwrap_or_default(),
        "context_class": context_class,
        "drop_reason": reason,
        "reason": reason,
        "token_estimate": tokens,
        "token_cost": tokens,
        "node_hash": record_node_hash(record),
        "node_path": record.get("node_path").cloned().unwrap_or_else(|| json!([])),
        "memory_scope": string_field(record, "memory_scope"),
        "session_continuity": string_field(record, "session_continuity"),
        "entity_type": string_field(record, "entity_type"),
        "entity_name": string_field(record, "entity_name"),
        "source_roles": record.get("source_roles").cloned().unwrap_or_else(|| json!([])),
        "source_role_counts": record.get("source_role_counts").cloned().unwrap_or_else(|| json!({})),
        "source_hook_types": record.get("source_hook_types").cloned().unwrap_or_else(|| json!([])),
        "source_hook_type_counts": record.get("source_hook_type_counts").cloned().unwrap_or_else(|| json!({})),
        "source_codex_events": record.get("source_codex_events").cloned().unwrap_or_else(|| json!([])),
        "source_codex_event_counts": record.get("source_codex_event_counts").cloned().unwrap_or_else(|| json!({})),
        "source_entity_types": record.get("source_entity_types").cloned().unwrap_or_else(|| json!([])),
        "source_memory_scopes": record.get("source_memory_scopes").cloned().unwrap_or_else(|| json!([])),
        "source_session_continuities": record.get("source_session_continuities").cloned().unwrap_or_else(|| json!([])),
        "source_extraction_phases": record.get("source_extraction_phases").cloned().unwrap_or_else(|| json!([])),
        "source_profile_promotion_policies": record.get("source_profile_promotion_policies").cloned().unwrap_or_else(|| json!([])),
        "source_profile_promotion_blockers": record.get("source_profile_promotion_blockers").cloned().unwrap_or_else(|| json!([])),
        "stale_or_superseded": reason == "stale",
        "text_preview": text.chars().take(160).collect::<String>(),
    });
    if let Some((profile_hash, shadow_reason)) = profile_shadow {
        if let Some(object) = detail.as_object_mut() {
            object.insert("profile_shadowed_by_ref_hash".to_string(), json!(profile_hash));
            object.insert("profile_shadowed_reason".to_string(), json!(shadow_reason));
        }
    }
    detail
}

fn dropped_ref_layer_budget_from_native_counts(
    reason_counts: &[(&str, u64, u64)],
    ref_type_counts: &HashMap<String, u64>,
    ref_type_token_counts: &HashMap<String, u64>,
    dropped_ref_details: &[Value],
) -> Value {
    let mut by_drop_reason = serde_json::Map::new();
    let mut total_refs = 0_u64;
    let mut total_tokens = 0_u64;
    for (reason, refs, tokens) in reason_counts {
        if *refs == 0 && *tokens == 0 {
            continue;
        }
        total_refs += *refs;
        total_tokens += *tokens;
        by_drop_reason.insert(
            (*reason).to_string(),
            json!({
                "refs": refs,
                "tokens": tokens,
            }),
        );
    }
    let mut detail_budget = json!({
        "by_memory_layer": {},
        "by_memory_scope": {},
        "by_session_continuity": {},
        "by_extraction_phase": {},
        "by_entity_type": {},
        "by_source_role": {},
        "by_hook_type": {},
        "by_codex_event": {},
        "source_message_counts_by_role": {},
        "source_hook_counts_by_type": {},
        "source_codex_event_counts_by_event": {},
        "by_profile_promotion_policy": {},
        "by_profile_promotion_blocker": {},
        "by_profile_shadowed_reason": {},
        "total_dropped_tokens_with_detail": 0,
        "stale_ref_count": 0,
        "stale_token_estimate": 0,
        "profile_shadowed_ref_count": 0,
        "profile_shadowed_token_estimate": 0,
    });
    for detail in dropped_ref_details {
        let tokens = detail
            .get("token_estimate")
            .or_else(|| detail.get("token_cost"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let total_detail_tokens = detail_budget
            .get("total_dropped_tokens_with_detail")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + tokens;
        if let Some(object) = detail_budget.as_object_mut() {
            object.insert("total_dropped_tokens_with_detail".to_string(), json!(total_detail_tokens));
        }
        let memory_layer = detail
            .get("memory_layer")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                let ref_type = detail
                    .get("ref_type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                broad_memory_layer(detail, ref_type)
            });
        if !memory_layer.is_empty() {
            increment_layer_bucket(&mut detail_budget, "by_memory_layer", &memory_layer, tokens);
        }
        for memory_scope in source_layer_values(detail, "source_memory_scopes", "memory_scope", "unscoped") {
            increment_layer_bucket(&mut detail_budget, "by_memory_scope", &memory_scope, tokens);
        }
        for session_continuity in source_layer_values(
            detail,
            "source_session_continuities",
            "session_continuity",
            "neutral",
        ) {
            increment_layer_bucket(
                &mut detail_budget,
                "by_session_continuity",
                &session_continuity,
                tokens,
            );
        }
        for extraction_phase in source_layer_values(
            detail,
            "source_extraction_phases",
            "extraction_phase",
            "unknown",
        ) {
            increment_layer_bucket(
                &mut detail_budget,
                "by_extraction_phase",
                &extraction_phase,
                tokens,
            );
        }
        for entity_type in source_layer_values(detail, "source_entity_types", "entity_type", "") {
            if !entity_type.is_empty() {
                increment_layer_bucket(&mut detail_budget, "by_entity_type", &entity_type, tokens);
            }
        }
        for policy in source_layer_values(detail, "source_profile_promotion_policies", "profile_promotion_policy", "") {
            if !policy.is_empty() {
                increment_layer_bucket(&mut detail_budget, "by_profile_promotion_policy", &policy, tokens);
            }
        }
        for blocker in source_layer_values(detail, "source_profile_promotion_blockers", "profile_promotion_blocker", "") {
            if !blocker.is_empty() {
                increment_layer_bucket(&mut detail_budget, "by_profile_promotion_blocker", &blocker, tokens);
            }
        }
        if let Some(roles) = detail.get("source_roles").and_then(Value::as_array) {
            for role in roles
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut detail_budget, "by_source_role", role, tokens);
            }
        }
        if let Some(hook_types) = detail.get("source_hook_types").and_then(Value::as_array) {
            for hook_type in hook_types
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut detail_budget, "by_hook_type", hook_type, tokens);
            }
        }
        if let Some(codex_events) = detail.get("source_codex_events").and_then(Value::as_array) {
            for codex_event in codex_events
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
            {
                increment_layer_bucket(&mut detail_budget, "by_codex_event", codex_event, tokens);
            }
        }
        increment_source_count_bucket(
            &mut detail_budget,
            "source_message_counts_by_role",
            detail.get("source_role_counts"),
        );
        increment_source_count_bucket(
            &mut detail_budget,
            "source_hook_counts_by_type",
            detail.get("source_hook_type_counts"),
        );
        increment_source_count_bucket(
            &mut detail_budget,
            "source_codex_event_counts_by_event",
            detail.get("source_codex_event_counts"),
        );
        if detail
            .get("stale_or_superseded")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let stale_count = detail_budget
                .get("stale_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            let stale_tokens = detail_budget
                .get("stale_token_estimate")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + tokens;
            if let Some(object) = detail_budget.as_object_mut() {
                object.insert("stale_ref_count".to_string(), json!(stale_count));
                object.insert("stale_token_estimate".to_string(), json!(stale_tokens));
            }
        }
        if detail.get("profile_shadowed_by_ref_hash").is_some() {
            let shadow_count = detail_budget
                .get("profile_shadowed_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            let shadow_tokens = detail_budget
                .get("profile_shadowed_token_estimate")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + tokens;
            if let Some(object) = detail_budget.as_object_mut() {
                object.insert("profile_shadowed_ref_count".to_string(), json!(shadow_count));
                object.insert("profile_shadowed_token_estimate".to_string(), json!(shadow_tokens));
            }
            let shadow_reason = detail
                .get("profile_shadowed_reason")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown");
            increment_layer_bucket(&mut detail_budget, "by_profile_shadowed_reason", shadow_reason, tokens);
        }
    }
    json!({
        "by_drop_reason": Value::Object(by_drop_reason),
        "by_memory_layer": detail_budget["by_memory_layer"].clone(),
        "by_memory_scope": detail_budget["by_memory_scope"].clone(),
        "by_session_continuity": detail_budget["by_session_continuity"].clone(),
        "by_extraction_phase": detail_budget["by_extraction_phase"].clone(),
        "by_ref_type": ref_type_budget_from_counts(ref_type_counts, ref_type_token_counts),
        "by_entity_type": detail_budget["by_entity_type"].clone(),
        "by_source_role": detail_budget["by_source_role"].clone(),
        "by_hook_type": detail_budget["by_hook_type"].clone(),
        "by_codex_event": detail_budget["by_codex_event"].clone(),
        "source_message_counts_by_role": detail_budget["source_message_counts_by_role"].clone(),
        "source_hook_counts_by_type": detail_budget["source_hook_counts_by_type"].clone(),
        "source_codex_event_counts_by_event": detail_budget["source_codex_event_counts_by_event"].clone(),
        "by_profile_promotion_policy": detail_budget["by_profile_promotion_policy"].clone(),
        "by_profile_promotion_blocker": detail_budget["by_profile_promotion_blocker"].clone(),
        "by_profile_shadowed_reason": detail_budget["by_profile_shadowed_reason"].clone(),
        "total_dropped_refs_with_detail": dropped_ref_details.len() as u64,
        "total_dropped_tokens_with_detail": detail_budget["total_dropped_tokens_with_detail"].clone(),
        "total_dropped_refs_from_native_counts": total_refs,
        "total_dropped_tokens_from_native_counts": total_tokens,
        "stale_ref_count": detail_budget["stale_ref_count"].clone(),
        "stale_token_estimate": detail_budget["stale_token_estimate"].clone(),
        "profile_shadowed_ref_count": detail_budget["profile_shadowed_ref_count"].clone(),
        "profile_shadowed_token_estimate": detail_budget["profile_shadowed_token_estimate"].clone(),
    })
}

fn budget_total(budget: &Value, names: &[&str]) -> u64 {
    for name in names {
        if let Some(value) = budget.get(*name).and_then(Value::as_u64) {
            return value;
        }
    }
    0
}

fn budget_bucket_total(bucket: Option<&Value>, name: &str) -> u64 {
    bucket.and_then(|value| value.get(name))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn memory_layer_pressure_summary(selected_budget: &Value, dropped_budget: &Value) -> Value {
    let mut by_dimension = serde_json::Map::new();
    let mut pressure_dimensions: Vec<String> = Vec::new();
    let mut dropped_dimensions: Vec<String> = Vec::new();
    for dimension in [
        "by_memory_layer",
        "by_drop_reason",
        "by_memory_scope",
        "by_session_continuity",
        "by_extraction_phase",
        "by_ref_type",
        "by_entity_type",
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "by_profile_promotion_policy",
        "by_profile_promotion_blocker",
        "by_profile_shadowed_reason",
    ] {
        let selected_buckets = selected_budget.get(dimension).and_then(Value::as_object);
        let dropped_buckets = dropped_budget.get(dimension).and_then(Value::as_object);
        let mut bucket_names = BTreeSet::new();
        if let Some(buckets) = selected_buckets {
            bucket_names.extend(buckets.keys().cloned());
        }
        if let Some(buckets) = dropped_buckets {
            bucket_names.extend(buckets.keys().cloned());
        }
        let mut dimension_summary = serde_json::Map::new();
        for bucket_name in bucket_names {
            let selected_bucket = selected_buckets.and_then(|buckets| buckets.get(&bucket_name));
            let dropped_bucket = dropped_buckets.and_then(|buckets| buckets.get(&bucket_name));
            let selected_refs = budget_bucket_total(selected_bucket, "refs");
            let dropped_refs = budget_bucket_total(dropped_bucket, "refs");
            if selected_refs == 0 && dropped_refs == 0 {
                continue;
            }
            let selected_tokens = budget_bucket_total(selected_bucket, "tokens");
            let dropped_tokens = budget_bucket_total(dropped_bucket, "tokens");
            dimension_summary.insert(
                bucket_name,
                json!({
                    "selected_refs": selected_refs,
                    "selected_tokens": selected_tokens,
                    "dropped_refs": dropped_refs,
                    "dropped_tokens": dropped_tokens,
                    "selected_and_dropped": selected_refs > 0 && dropped_refs > 0,
                }),
            );
        }
        if !dimension_summary.is_empty() {
            if dimension_summary
                .values()
                .any(|bucket| bucket.get("dropped_refs").and_then(Value::as_u64).unwrap_or(0) > 0)
            {
                dropped_dimensions.push(dimension.to_string());
            }
            if dimension_summary
                .values()
                .any(|bucket| bucket.get("selected_and_dropped").and_then(Value::as_bool).unwrap_or(false))
            {
                pressure_dimensions.push(dimension.to_string());
            }
            by_dimension.insert(dimension.to_string(), Value::Object(dimension_summary));
        }
    }
    for dimension in [
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
    ] {
        let selected_counts = selected_budget.get(dimension).and_then(Value::as_object);
        let dropped_counts = dropped_budget.get(dimension).and_then(Value::as_object);
        let mut bucket_names = BTreeSet::new();
        if let Some(counts) = selected_counts {
            bucket_names.extend(counts.keys().cloned());
        }
        if let Some(counts) = dropped_counts {
            bucket_names.extend(counts.keys().cloned());
        }
        let mut count_summary = serde_json::Map::new();
        for bucket_name in bucket_names {
            let selected_count = selected_counts
                .and_then(|counts| counts.get(&bucket_name))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let dropped_count = dropped_counts
                .and_then(|counts| counts.get(&bucket_name))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if selected_count == 0 && dropped_count == 0 {
                continue;
            }
            count_summary.insert(
                bucket_name,
                json!({
                    "selected_count": selected_count,
                    "dropped_count": dropped_count,
                    "selected_and_dropped": selected_count > 0 && dropped_count > 0,
                }),
            );
        }
        if !count_summary.is_empty() {
            if count_summary
                .values()
                .any(|bucket| bucket.get("dropped_count").and_then(Value::as_u64).unwrap_or(0) > 0)
            {
                dropped_dimensions.push(dimension.to_string());
            }
            if count_summary
                .values()
                .any(|bucket| bucket.get("selected_and_dropped").and_then(Value::as_bool).unwrap_or(false))
            {
                pressure_dimensions.push(dimension.to_string());
            }
            by_dimension.insert(dimension.to_string(), Value::Object(count_summary));
        }
    }
    let dropped_in = |dimension: &str, bucket: &str| -> u64 {
        by_dimension
            .get(dimension)
            .and_then(Value::as_object)
            .and_then(|buckets| buckets.get(bucket))
            .and_then(|entry| entry.get("dropped_refs"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    };
    let dropped_count_in = |dimension: &str, bucket: &str| -> u64 {
        by_dimension
            .get(dimension)
            .and_then(Value::as_object)
            .and_then(|buckets| buckets.get(bucket))
            .and_then(|entry| entry.get("dropped_count"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    };
    let pressure_bucket_count = by_dimension
        .values()
        .filter_map(Value::as_object)
        .flat_map(|buckets| buckets.values())
        .filter(|bucket| {
            bucket
                .get("selected_and_dropped")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .count() as u64;
    let dropped_bucket_count = by_dimension
        .values()
        .filter_map(Value::as_object)
        .flat_map(|buckets| buckets.values())
        .filter(|bucket| {
            bucket.get("dropped_refs").and_then(Value::as_u64).unwrap_or(0) > 0
                || bucket.get("dropped_count").and_then(Value::as_u64).unwrap_or(0) > 0
        })
        .count() as u64;
    json!({
        "selected_refs": budget_total(selected_budget, &["total_selected_refs"]),
        "selected_tokens": budget_total(selected_budget, &["total_selected_tokens"]),
        "dropped_refs": budget_total(dropped_budget, &["total_dropped_refs", "total_dropped_refs_with_detail"]),
        "dropped_tokens": budget_total(dropped_budget, &["total_dropped_tokens", "total_dropped_tokens_with_detail"]),
        "pressure_dimensions": pressure_dimensions,
        "dropped_dimensions": dropped_dimensions,
        "by_dimension": by_dimension,
        "profile_entity_pressure": dropped_in("by_memory_layer", "profile_entity") > 0,
        "same_session_event_pressure": dropped_in("by_memory_layer", "same_session_event") > 0,
        "cross_session_event_pressure": dropped_in("by_memory_layer", "cross_session_event") > 0,
        "summary_layer_pressure": dropped_in("by_memory_layer", "summary") > 0,
        "compression_layer_pressure": dropped_in("by_memory_layer", "compression") > 0,
        "resource_layer_pressure": dropped_in("by_memory_layer", "resource_fact") > 0
            || dropped_in("by_memory_layer", "resource_entity_fact") > 0
            || dropped_in("by_memory_layer", "resource_chunk") > 0,
        "skill_layer_pressure": dropped_in("by_memory_layer", "skill_section") > 0,
        "profile_memory_pressure": dropped_in("by_memory_scope", "user_profile") > 0,
        "session_memory_pressure": dropped_in("by_memory_scope", "session") > 0,
        "cross_session_pressure": dropped_in("by_session_continuity", "cross_session") > 0,
        "same_session_pressure": dropped_in("by_session_continuity", "same_session") > 0,
        "summary_memory_pressure": dropped_in("by_ref_type", "summary") > 0,
        "entity_memory_pressure": dropped_in("by_ref_type", "entity") > 0,
        "event_memory_pressure": dropped_in("by_ref_type", "event") > 0,
        "final_memory_pressure": dropped_in("by_extraction_phase", "final") > 0,
        "provisional_memory_pressure": dropped_in("by_extraction_phase", "provisional") > 0,
        "stale_current_state_pressure": budget_total(dropped_budget, &["stale_ref_count"]) > 0,
        "profile_shadowed_current_state_pressure": budget_total(dropped_budget, &["profile_shadowed_ref_count"]) > 0,
        "assistant_memory_pressure": dropped_in("by_source_role", "assistant") > 0,
        "user_memory_pressure": dropped_in("by_source_role", "user") > 0,
        "tool_memory_pressure": dropped_in("by_source_role", "tool") > 0,
        "assistant_source_message_pressure": dropped_count_in("source_message_counts_by_role", "assistant") > 0,
        "user_source_message_pressure": dropped_count_in("source_message_counts_by_role", "user") > 0,
        "tool_source_message_pressure": dropped_count_in("source_message_counts_by_role", "tool") > 0,
        "hook_boundary_source_pressure": dropped_count_in("source_hook_counts_by_type", "hook_boundary") > 0,
        "after_llm_source_pressure": dropped_count_in("source_hook_counts_by_type", "after_llm") > 0,
        "tool_result_source_pressure": dropped_count_in("source_hook_counts_by_type", "tool_result") > 0,
        "stop_event_source_pressure": dropped_count_in("source_codex_event_counts_by_event", "Stop") > 0,
        "post_tool_use_source_pressure": dropped_count_in("source_codex_event_counts_by_event", "PostToolUse") > 0,
        "pressure_bucket_count": pressure_bucket_count,
        "dropped_bucket_count": dropped_bucket_count,
    })
}
