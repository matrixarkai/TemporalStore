// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// retrieve_context_pack_native, split from matrixark_rust_proxy_impl.rs (textually include!d;
// shares parent use-imports + flat scope; no use-statements or mod wrapper).

fn retrieve_context_pack_native(
    engine: &TemporalEngine,
    command: &RecordLogRequest,
) -> Result<Value, String> {
    let started = Instant::now();
    let request = command.record.clone().unwrap_or_else(|| json!({}));
    let query = request
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let query_terms: HashSet<String> = query
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| part.len() > 2)
        .map(str::to_string)
        .collect();
    let remote_budget = json_field(&request, &["local_budget", "remote_budget_tokens"])
        .and_then(Value::as_u64)
        .or_else(|| request.get("max_context_tokens").and_then(Value::as_u64))
        .unwrap_or(4000);
    let question_type = request
        .get("question_type")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| infer_native_question_type(&query).to_string());
    let max_refs = json_field(&request, &["ranking", "max_selected_refs"])
        .and_then(Value::as_u64)
        .unwrap_or(24)
        .max(1);
    let max_global_candidates = json_field(&request, &["ranking", "max_global_candidates"])
        .and_then(Value::as_u64)
        .unwrap_or(512)
        .max(1);
    let min_similarity_score = json_field(&request, &["ranking", "min_similarity_score"])
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    let budget_fill_policy = json_field(&request, &["ranking", "budget_fill_policy"])
        .and_then(Value::as_str)
        .filter(|policy| *policy == "quality_first" || *policy == "force_fill")
        .unwrap_or("quality_first")
        .to_string();
    let mut scan_command = command.clone();
    scan_command.scope = request
        .get("scope")
        .cloned()
        .or_else(|| command.scope.clone());
    scan_command.secondary_index_groups = request
        .get("secondary_index_groups")
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .map(|group| {
                    group
                        .as_array()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .or_else(|| command.secondary_index_groups.clone());
    if scan_command
        .record_types
        .as_ref()
        .map(Vec::is_empty)
        .unwrap_or(true)
    {
        scan_command.record_types = Some(vec![
            "context_compression_event".to_string(),
            "context_entity".to_string(),
            "context_event".to_string(),
            "context_segment".to_string(),
            "context_summary".to_string(),
            "resource_chunk".to_string(),
            "skill_section".to_string(),
            "context_index".to_string(),
        ]);
    }
    let scan_started = Instant::now();
    let scan = scan_matrixark_candidates(engine, &scan_command)?;
    let candidate_fetch_ms = scan_started.elapsed().as_secs_f64() * 1000.0;
    let records = scan
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let scope_for_continuity = scan_command.scope.clone();
    let mut memory_inventory =
        native_retrieval_memory_inventory(&records, scope_for_continuity.as_ref());
    let cross_policy = parse_cross_session_policy(
        &request,
        scope_for_continuity.as_ref(),
        remote_budget,
        &question_type,
    );
    let source_role_budget_tokens = parse_source_role_budget_tokens(&request);
    let memory_selection_policy_budget_tokens =
        parse_memory_selection_policy_budget_tokens(&request);
    let extraction_phase_budget_tokens = parse_extraction_phase_budget_tokens(&request);
    let mut source_role_used_tokens: HashMap<String, u64> = HashMap::new();
    let mut source_role_selected_ref_counts: HashMap<String, u64> = HashMap::new();
    for role in source_role_budget_tokens.keys() {
        source_role_used_tokens.insert(role.clone(), 0);
        source_role_selected_ref_counts.insert(role.clone(), 0);
    }
    let mut memory_selection_policy_used_tokens: HashMap<String, u64> = HashMap::new();
    let mut memory_selection_policy_selected_ref_counts: HashMap<String, u64> = HashMap::new();
    for policy in memory_selection_policy_budget_tokens.keys() {
        memory_selection_policy_used_tokens.insert(policy.clone(), 0);
        memory_selection_policy_selected_ref_counts.insert(policy.clone(), 0);
    }
    let mut extraction_phase_used_tokens: HashMap<String, u64> = HashMap::new();
    let mut extraction_phase_selected_ref_counts: HashMap<String, u64> = HashMap::new();
    for phase in extraction_phase_budget_tokens.keys() {
        extraction_phase_used_tokens.insert(phase.clone(), 0);
        extraction_phase_selected_ref_counts.insert(phase.clone(), 0);
    }
    let mut raw_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    let mut text_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    let mut prepared_records = Vec::with_capacity(records.len());
    for record in records.iter() {
        let context_class = context_class_name(record);
        increment_class_count(&mut raw_candidate_class_counts, &context_class);
        let text = candidate_text(record);
        let tokens = token_estimate(&text);
        if !text.is_empty() {
            increment_class_count(&mut text_candidate_class_counts, &context_class);
        }
        prepared_records.push((record.clone(), context_class, text, tokens));
    }
    let score_started = Instant::now();
    let mut scored_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    let mut score_threshold_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut scored: Vec<NativeScoredCandidate> = prepared_records
        .into_iter()
        .filter(|(record, _context_class, text, _tokens)| {
            matches!(
                record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                "context_compression_event"
                    | "context_entity"
                    | "context_event"
                    | "context_segment"
                    | "context_summary"
                    | "resource_chunk"
                    | "skill_section"
            ) && !text.is_empty()
        })
        .filter_map(|(record, context_class, text, tokens)| {
            let mut score = sparse_query_score(&query_terms, &text);
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_entity")
            ) {
                score += 0.08;
            }
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_compression_event")
            ) {
                score += 0.06;
            }
            let session_continuity =
                session_continuity_status(&record, scope_for_continuity.as_ref());
            let continuity_boost_value =
                continuity_boost(&record, &context_class, &session_continuity);
            score += continuity_boost_value;
            let cross_session_rerank_boost_value = cross_session_rerank_boost(
                &record,
                &context_class,
                &session_continuity,
                &question_type,
            );
            score += cross_session_rerank_boost_value;
            score += type_priority_boost(&record, &context_class, &question_type);
            if score >= min_similarity_score {
                increment_class_count(&mut scored_candidate_class_counts, &context_class);
                Some(NativeScoredCandidate {
                    score,
                    record,
                    text,
                    tokens,
                    context_class,
                    session_continuity,
                    continuity_boost_value,
                    cross_session_rerank_boost_value,
                })
            } else {
                increment_class_count(&mut score_threshold_dropped_class_counts, &context_class);
                None
            }
        })
        .collect();
    let score_ms = score_started.elapsed().as_secs_f64() * 1000.0;
    scored.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if scored.len() > max_global_candidates as usize {
        scored.truncate(max_global_candidates as usize);
    }
    let mut selected = Vec::new();
    let mut selected_signatures: HashSet<String> = HashSet::new();
    let mut selected_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_nodes: HashSet<u64> = HashSet::new();
    let mut dropped_over_budget = 0_u64;
    let mut dropped_cross_budget = 0_u64;
    let mut dropped_cross_session_cap = 0_u64;
    let mut dropped_cross_candidate_cap = 0_u64;
    let mut dropped_source_role_budget = 0_u64;
    let mut dropped_memory_selection_policy_budget = 0_u64;
    let mut dropped_extraction_phase_budget = 0_u64;
    let mut dropped_low_score = 0_u64;
    let mut dropped_duplicate_ref = 0_u64;
    let mut dropped_policy_ref = 0_u64;
    let mut dropped_stale_ref = 0_u64;
    let mut budget_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut policy_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut duplicate_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_policy_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_low_score_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_cap_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut source_role_budget_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut memory_selection_policy_budget_dropped_class_counts: HashMap<String, u64> =
        HashMap::new();
    let mut extraction_phase_budget_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_class_counts: HashMap<String, u64> = HashMap::new();
    let mut dropped_ref_type_counts: HashMap<String, u64> = HashMap::new();
    let mut dropped_ref_type_token_counts: HashMap<String, u64> = HashMap::new();
    let mut dropped_over_budget_tokens = 0_u64;
    let mut dropped_cross_budget_tokens = 0_u64;
    let mut dropped_cross_session_cap_tokens = 0_u64;
    let mut dropped_cross_candidate_cap_tokens = 0_u64;
    let mut dropped_source_role_budget_tokens = 0_u64;
    let mut dropped_memory_selection_policy_budget_tokens = 0_u64;
    let mut dropped_extraction_phase_budget_tokens = 0_u64;
    let mut dropped_low_score_tokens = 0_u64;
    let mut dropped_duplicate_ref_tokens = 0_u64;
    let mut dropped_policy_ref_tokens = 0_u64;
    let mut dropped_stale_ref_tokens = 0_u64;
    let mut dropped_ref_details: Vec<Value> = Vec::new();
    let mut cross_used_tokens = 0_u64;
    let mut cross_selected_refs = 0_u64;
    let mut entity_bridge_selected_refs = 0_u64;
    let mut selected_cross_sessions: HashSet<String> = HashSet::new();
    let mut used_tokens = 0_u64;
    let has_scored_event_candidate = scored.iter().any(|candidate| candidate.context_class == "event");
    let summary_allowed_for_question = matches!(
        question_type.as_str(),
        "broad" | "broad_exploration" | "exploration" | "profile_memory"
    );
    let current_state_query = matches!(
        question_type.as_str(),
        "current_state" | "latest" | "profile_memory"
    );
    let (profile_by_entity, profile_by_source_entity_hash) = if current_state_query {
        profile_shadow_maps(&scored)
    } else {
        (HashMap::new(), HashMap::new())
    };
    for candidate in scored {
        if selected.len() as u64 >= max_refs {
            break;
        }
        let NativeScoredCandidate {
            score,
            record,
            text,
            tokens,
            context_class,
            session_continuity,
            continuity_boost_value,
            cross_session_rerank_boost_value,
        } = candidate;
        let candidate_for_shadow = NativeScoredCandidate {
            score,
            record: record.clone(),
            text: text.clone(),
            tokens,
            context_class: context_class.clone(),
            session_continuity: session_continuity.clone(),
            continuity_boost_value,
            cross_session_rerank_boost_value,
        };
        if current_state_query {
            if let Some(profile_shadow) = profile_shadow_for_candidate(
                &candidate_for_shadow,
                &profile_by_entity,
                &profile_by_source_entity_hash,
            ) {
                dropped_stale_ref += 1;
                dropped_stale_ref_tokens += tokens;
                increment_class_count(&mut dropped_ref_type_counts, &context_class);
                increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
                dropped_ref_details.push(native_dropped_ref_detail(
                    &record,
                    &text,
                    &context_class,
                    "stale",
                    tokens,
                    Some(profile_shadow),
                ));
                continue;
            }
        }
        if used_tokens + tokens > remote_budget {
            dropped_over_budget += 1;
            dropped_over_budget_tokens += tokens;
            increment_class_count(&mut budget_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if !is_serving_selected_ref_class(&context_class) {
            dropped_policy_ref += 1;
            dropped_policy_ref_tokens += tokens;
            increment_class_count(&mut policy_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if context_class == "summary" && has_scored_event_candidate && !summary_allowed_for_question {
            dropped_policy_ref += 1;
            dropped_policy_ref_tokens += tokens;
            increment_class_count(&mut policy_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        let is_cross_session = session_continuity == "cross_session";
        let record_type = record
            .get("record_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        let is_entity_bridge = is_cross_session && context_class == "entity";
        let is_cross_session_raw_evidence =
            is_cross_session && matches!(record_type, "context_event" | "context_segment");
        let cross_key = if is_cross_session {
            cross_session_key(&record)
        } else {
            String::new()
        };
        if is_cross_session && !cross_policy.enabled {
            dropped_cross_budget += 1;
            dropped_cross_budget_tokens += tokens;
            increment_class_count(&mut cross_policy_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session && cross_policy.min_score > 0.0 && score < cross_policy.min_score {
            dropped_low_score += 1;
            dropped_low_score_tokens += tokens;
            increment_class_count(&mut cross_low_score_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session_raw_evidence
            && cross_policy.raw_evidence_min_score > 0.0
            && score < cross_policy.raw_evidence_min_score
        {
            dropped_low_score += 1;
            dropped_low_score_tokens += tokens;
            increment_class_count(&mut cross_low_score_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session
            && cross_policy.max_candidates > 0
            && cross_selected_refs >= cross_policy.max_candidates
        {
            dropped_cross_candidate_cap += 1;
            dropped_cross_candidate_cap_tokens += tokens;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session
            && cross_policy.max_sessions > 0
            && !selected_cross_sessions.contains(&cross_key)
            && selected_cross_sessions.len() as u64 >= cross_policy.max_sessions
        {
            dropped_cross_session_cap += 1;
            dropped_cross_session_cap_tokens += tokens;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        if is_cross_session
            && cross_policy.budget_tokens > 0
            && cross_used_tokens + tokens > cross_policy.budget_tokens
            && !(is_entity_bridge
                && entity_bridge_selected_refs < cross_policy.min_entity_bridge_refs)
        {
            dropped_cross_budget += 1;
            dropped_cross_budget_tokens += tokens;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        let candidate_source_roles = source_role_names(&record);
        let capped_roles: Vec<String> = candidate_source_roles
            .iter()
            .filter(|role| {
                source_role_budget_tokens
                    .get(*role)
                    .map(|budget| {
                        source_role_used_tokens.get(*role).copied().unwrap_or(0) + tokens > *budget
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if !capped_roles.is_empty() {
            dropped_source_role_budget += 1;
            dropped_source_role_budget_tokens += tokens;
            increment_class_count(&mut source_role_budget_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            let mut detail = native_dropped_ref_detail(
                &record,
                &text,
                &context_class,
                "source_role_budget",
                tokens,
                None,
            );
            if let Some(object) = detail.as_object_mut() {
                object.insert(
                    "source_role_budget_capped_roles".to_string(),
                    json!(capped_roles),
                );
            }
            dropped_ref_details.push(detail);
            continue;
        }
        let candidate_memory_selection_policies = memory_selection_policy_names(&record);
        let capped_memory_selection_policies: Vec<String> = candidate_memory_selection_policies
            .iter()
            .filter(|policy| {
                memory_selection_policy_budget_tokens
                    .get(*policy)
                    .map(|budget| {
                        memory_selection_policy_used_tokens
                            .get(*policy)
                            .copied()
                            .unwrap_or(0)
                            + tokens
                            > *budget
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if !capped_memory_selection_policies.is_empty() {
            dropped_memory_selection_policy_budget += 1;
            dropped_memory_selection_policy_budget_tokens += tokens;
            increment_class_count(
                &mut memory_selection_policy_budget_dropped_class_counts,
                &context_class,
            );
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            let mut detail = native_dropped_ref_detail(
                &record,
                &text,
                &context_class,
                "memory_selection_policy_budget",
                tokens,
                None,
            );
            if let Some(object) = detail.as_object_mut() {
                object.insert(
                    "memory_selection_policy_budget_capped_policies".to_string(),
                    json!(capped_memory_selection_policies),
                );
            }
            dropped_ref_details.push(detail);
            continue;
        }
        let candidate_extraction_phase = extraction_phase_name(&record);
        if extraction_phase_budget_tokens
            .get(&candidate_extraction_phase)
            .map(|budget| {
                extraction_phase_used_tokens
                    .get(&candidate_extraction_phase)
                    .copied()
                    .unwrap_or(0)
                    + tokens
                    > *budget
            })
            .unwrap_or(false)
        {
            dropped_extraction_phase_budget += 1;
            dropped_extraction_phase_budget_tokens += tokens;
            increment_class_count(
                &mut extraction_phase_budget_dropped_class_counts,
                &context_class,
            );
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            let mut detail = native_dropped_ref_detail(
                &record,
                &text,
                &context_class,
                "extraction_phase_budget",
                tokens,
                None,
            );
            if let Some(object) = detail.as_object_mut() {
                object.insert(
                    "extraction_phase_budget_capped_phase".to_string(),
                    json!(candidate_extraction_phase),
                );
            }
            dropped_ref_details.push(detail);
            continue;
        }
        let ref_signature = format!(
            "{}:{}",
            context_class,
            record_ref_hash(&record).unwrap_or_else(|| {
                record
                    .get("record_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            })
        );
        if !selected_signatures.insert(ref_signature) {
            dropped_duplicate_ref += 1;
            dropped_duplicate_ref_tokens += tokens;
            increment_class_count(&mut duplicate_dropped_class_counts, &context_class);
            increment_class_count(&mut dropped_ref_type_counts, &context_class);
            increment_class_tokens(&mut dropped_ref_type_token_counts, &context_class, tokens);
            continue;
        }
        used_tokens += tokens;
        if is_cross_session {
            cross_used_tokens += tokens;
            cross_selected_refs += 1;
            selected_cross_sessions.insert(cross_key);
            if is_entity_bridge {
                entity_bridge_selected_refs += 1;
            }
        }
        for role in candidate_source_roles {
            if source_role_budget_tokens.contains_key(&role) {
                *source_role_used_tokens.entry(role.clone()).or_default() += tokens;
                *source_role_selected_ref_counts.entry(role).or_default() += 1;
            }
        }
        for policy in candidate_memory_selection_policies {
            if memory_selection_policy_budget_tokens.contains_key(&policy) {
                *memory_selection_policy_used_tokens
                    .entry(policy.clone())
                    .or_default() += tokens;
                *memory_selection_policy_selected_ref_counts
                    .entry(policy)
                    .or_default() += 1;
            }
        }
        if extraction_phase_budget_tokens.contains_key(&candidate_extraction_phase) {
            *extraction_phase_used_tokens
                .entry(candidate_extraction_phase.clone())
                .or_default() += tokens;
            *extraction_phase_selected_ref_counts
                .entry(candidate_extraction_phase)
                .or_default() += 1;
        }
        *selected_counts.entry(context_class.clone()).or_default() += 1;
        increment_class_count(&mut selected_class_counts, &context_class);
        if let Some(node_hash) = record_node_hash(&record) {
            selected_nodes.insert(node_hash);
        }
        selected.push(pack_ref_from_record(
            &record,
            &text,
            &context_class,
            score,
            "native_rust_proxy_score_pack",
            &session_continuity,
            continuity_boost_value,
            cross_session_rerank_boost_value,
        ));
    }
    let context_pack_id = format!("rust-native-{}-{}", unix_ms(), selected.len());
    let mut scan_stats = scan.get("scan_stats").cloned().unwrap_or_else(|| json!({}));
    if let Some(stats) = scan_stats.as_object_mut() {
        stats.insert("native_pack_assembly".to_string(), json!(true));
        // Same as the compact-snapshot path: candidates are ordered by term overlap plus boosts,
        // and no vector is read. Said here so a caller does not have to infer it from a config.
        stats.insert(
            "ranking".to_string(),
            json!("lexical_term_overlap_and_boosts"),
        );
        stats.insert("ranking_uses_vectors".to_string(), json!(false));
        stats.insert(
            "pack_assembly_location".to_string(),
            json!("rust_proxy_native"),
        );
        stats.insert("next_native_gap".to_string(), json!(""));
    }
    let candidate_class_counts = json!({
        "raw": raw_candidate_class_counts,
        "with_text": text_candidate_class_counts,
        "scored": scored_candidate_class_counts,
        "selected": selected_class_counts,
        "score_threshold_dropped": score_threshold_dropped_class_counts,
        "budget_dropped": budget_dropped_class_counts,
        "policy_dropped": policy_dropped_class_counts,
        "duplicate_dropped": duplicate_dropped_class_counts,
        "cross_policy_dropped": cross_policy_dropped_class_counts,
        "cross_low_score_dropped": cross_low_score_dropped_class_counts,
        "cross_cap_dropped": cross_cap_dropped_class_counts,
        "source_role_budget_dropped": source_role_budget_dropped_class_counts,
        "memory_selection_policy_budget_dropped": memory_selection_policy_budget_dropped_class_counts,
        "extraction_phase_budget_dropped": extraction_phase_budget_dropped_class_counts
    });
    let memory_layer_budget = selected_ref_layer_budget(&selected);
    let serving_selected_refs = native_serving_refs(&selected);
    let selected_profile_ref_count = selected
        .iter()
        .filter(|item| {
            matches!(
                item.get("memory_scope").and_then(Value::as_str),
                Some("user_profile" | "profile" | "cross_session_profile")
            ) || (item.get("session_continuity").and_then(Value::as_str) == Some("cross_session")
                && item.get("ref_type").and_then(Value::as_str) == Some("entity"))
        })
        .count();
    let profile_available = memory_inventory
        .get("has_profile_memory")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if let Some(object) = memory_inventory.as_object_mut() {
        object.insert(
            "profile_records_available_but_not_selected".to_string(),
            json!(profile_available && selected_profile_ref_count == 0),
        );
    }
    let source_role_budget_policy = json!({
        "enabled": !source_role_budget_tokens.is_empty(),
        "budget_tokens": json_u64_map(&source_role_budget_tokens),
        "selected_tokens_by_role": hash_u64_map_to_json(&source_role_used_tokens),
        "selected_ref_count_by_role": hash_u64_map_to_json(&source_role_selected_ref_counts)
    });
    let memory_selection_policy_budget_policy = json!({
        "enabled": !memory_selection_policy_budget_tokens.is_empty(),
        "mode": if memory_selection_policy_budget_tokens.is_empty() { "disabled" } else { "bounded_memory_selection_policy_tokens" },
        "budget_tokens": json_u64_map(&memory_selection_policy_budget_tokens),
        "selected_tokens_by_policy": hash_u64_map_to_json(&memory_selection_policy_used_tokens),
        "selected_ref_count_by_policy": hash_u64_map_to_json(&memory_selection_policy_selected_ref_counts),
        "dropped_ref_count": dropped_memory_selection_policy_budget,
        "strategy": "bound_lossy_summary_decision_tool_evidence_selection_policies_before_context_pack_injection"
    });
    let extraction_phase_budget_policy = json!({
        "enabled": !extraction_phase_budget_tokens.is_empty(),
        "mode": if extraction_phase_budget_tokens.is_empty() { "disabled" } else { "bounded_extraction_phase_tokens" },
        "budget_tokens": json_u64_map(&extraction_phase_budget_tokens),
        "selected_tokens_by_phase": hash_u64_map_to_json(&extraction_phase_used_tokens),
        "selected_ref_count_by_phase": hash_u64_map_to_json(&extraction_phase_selected_ref_counts),
        "dropped_ref_count": dropped_extraction_phase_budget,
        "strategy": "bound_pending_provisional_and_final_memory_before_context_pack_injection"
    });
    let dropped_memory_layer_budget = dropped_ref_layer_budget_from_native_counts(
        &[
            ("over_budget", dropped_over_budget, dropped_over_budget_tokens),
            ("cross_session_budget", dropped_cross_budget, dropped_cross_budget_tokens),
            (
                "cross_session_session_cap",
                dropped_cross_session_cap,
                dropped_cross_session_cap_tokens,
            ),
            (
                "cross_session_candidate_cap",
                dropped_cross_candidate_cap,
                dropped_cross_candidate_cap_tokens,
            ),
            (
                "source_role_budget",
                dropped_source_role_budget,
                dropped_source_role_budget_tokens,
            ),
            (
                "memory_selection_policy_budget",
                dropped_memory_selection_policy_budget,
                dropped_memory_selection_policy_budget_tokens,
            ),
            (
                "extraction_phase_budget",
                dropped_extraction_phase_budget,
                dropped_extraction_phase_budget_tokens,
            ),
            ("low_score", dropped_low_score, dropped_low_score_tokens),
            ("duplicate_ref", dropped_duplicate_ref, dropped_duplicate_ref_tokens),
            ("policy_ref", dropped_policy_ref, dropped_policy_ref_tokens),
            ("stale", dropped_stale_ref, dropped_stale_ref_tokens),
        ],
        &dropped_ref_type_counts,
        &dropped_ref_type_token_counts,
        &dropped_ref_details,
    );
    let memory_layer_pressure =
        memory_layer_pressure_summary(&memory_layer_budget, &dropped_memory_layer_budget);
    let serving_memory_layer_budget = native_serving_memory_layer_budget(&memory_layer_budget);
    let serving_dropped_memory_layer_budget =
        native_serving_memory_layer_budget(&dropped_memory_layer_budget);
    let serving_memory_layer_pressure =
        native_serving_memory_layer_pressure(&memory_layer_pressure);
    let serving_dropped_refs = native_serving_dropped_refs(json!({
        "over_budget": dropped_over_budget,
        "cross_session_budget": dropped_cross_budget,
        "cross_session_session_cap": dropped_cross_session_cap,
        "cross_session_candidate_cap": dropped_cross_candidate_cap,
        "source_role_budget": dropped_source_role_budget,
        "memory_selection_policy_budget": dropped_memory_selection_policy_budget,
        "extraction_phase_budget": dropped_extraction_phase_budget,
        "low_score": dropped_low_score,
        "duplicate_ref": dropped_duplicate_ref,
        "policy_ref": dropped_policy_ref,
        "reason_counts": {
            "over_budget": dropped_over_budget,
            "cross_session_budget": dropped_cross_budget,
            "cross_session_session_cap": dropped_cross_session_cap,
            "cross_session_candidate_cap": dropped_cross_candidate_cap,
            "source_role_budget": dropped_source_role_budget,
            "memory_selection_policy_budget": dropped_memory_selection_policy_budget,
            "extraction_phase_budget": dropped_extraction_phase_budget,
            "low_score": dropped_low_score,
            "duplicate_ref": dropped_duplicate_ref,
            "policy_ref": dropped_policy_ref,
            "stale": dropped_stale_ref
        },
        "refs": dropped_ref_details
    }));
    let pack = json!({
        "context_pack_id": context_pack_id,
        "query": query,
        "question_type": question_type,
        "selected_ref_counts": selected_counts,
        "remote_context_refs": serving_selected_refs,
        "selected_refs": serving_selected_refs,
        "dropped_refs": serving_dropped_refs,
        "memory_inventory": memory_inventory.clone(),
        "used_context_tokens": used_tokens,
        "used_remote_context_tokens": used_tokens,
        "remote_context_budget_tokens": remote_budget,
        "requested_max_context_tokens": request.get("max_context_tokens").cloned().unwrap_or_else(|| json!(remote_budget)),
        "packing_policy": "native_rust_proxy_question_type_aware",
        "context_pack_assembly": "native_rust_proxy",
        "context_sources_order": ["entities", "events", "segments", "resources", "skills", "summaries"],
        "recall_policy": {
            "native_context_pack": {
                "enabled": true,
                "backend": "rust_proxy",
                "scan_filter_score_pack": true
            },
            "native_response_contract": {
                "raw_records_returned_to_python": false,
                "python_hot_path_records": 0,
                "python_role": "dispatch_request_receive_context_pack",
                "backend_role": "scan_filter_score_pack"
            },
            "scan_stats": scan_stats,
            "rerank": {
                "enabled": true,
                "mode": "native_weighted_recall_plus_cross_session_rerank",
                "cross_session_rerank_enabled": true,
                "cross_session_signals": ["entity_state", "resource_fact_citation", "answer_event", "compression", "summary_demotion"],
                "heavy_rerank_enabled": false
            },
            "ranking": {
                "min_similarity_score": min_similarity_score,
                "max_global_candidates": max_global_candidates,
                "max_selected_refs": max_refs,
                "budget_fill_policy": budget_fill_policy,
                "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first"
            },
            "session_continuity": {
                "mode": scan_command.scope.as_ref().map(session_scope_mode).unwrap_or("prefer"),
                "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                "same_session_selected_ref_count": selected.iter().filter(|item| item.get("session_continuity").and_then(Value::as_str) == Some("same_session")).count(),
                "cross_session_selected_ref_count": cross_selected_refs,
                "entity_bridge_selected_ref_count": entity_bridge_selected_refs
            },
            "memory_layer_budget": serving_memory_layer_budget,
            "dropped_memory_layer_budget": serving_dropped_memory_layer_budget,
            "memory_layer_pressure": serving_memory_layer_pressure,
            "memory_inventory": memory_inventory.clone(),
            "source_role_budget": source_role_budget_policy,
            "memory_selection_policy_budget_policy": memory_selection_policy_budget_policy,
            "extraction_phase_budget_policy": extraction_phase_budget_policy,
            "cross_session": {
                "enabled": cross_policy.enabled,
                "mode": if cross_policy.enabled { "prefer" } else { "disabled" },
                "budget_ratio": cross_policy.budget_ratio,
                "max_budget_ratio": cross_policy.max_budget_ratio,
                "budget_tokens": cross_policy.budget_tokens,
                "remote_budget_tokens": remote_budget,
                "max_budget_tokens": cross_policy.max_budget_tokens,
                "max_sessions": cross_policy.max_sessions,
                "max_candidates": cross_policy.max_candidates,
                "min_score": cross_policy.min_score,
                "raw_evidence_min_score": cross_policy.raw_evidence_min_score,
                "parallelism": cross_policy.parallelism,
                "selected_tokens": cross_used_tokens,
                "selected_ref_count": cross_selected_refs,
                "selected_session_count": selected_cross_sessions.len() as u64,
                "entity_bridge_selected_ref_count": entity_bridge_selected_refs,
                "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
                "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events"
            },
            "tree_traversal": {
                "enabled": true,
                "native_backend": true,
                "fallback_to_flat": false,
                "selected_node_count": selected_nodes.len() as u64,
                "selected_leaf_count": selected_nodes.len() as u64,
                "summary_embeddings": ["node_l0", "node_l1"]
            },
            "secondary_index_filter": {
                "enabled": true,
                "native_backend": true,
                "applied_before_embedding_scoring": true,
                "matched_candidate_count": scan.get("scan_stats").and_then(|v| v.get("secondary_index_matched_candidate_count")).cloned().unwrap_or_else(|| json!(0)),
                "dropped_candidate_count": scan.get("scan_stats").and_then(|v| v.get("secondary_index_dropped_candidate_count")).cloned().unwrap_or_else(|| json!(0))
            },
            "candidate_class_counts": candidate_class_counts.clone()
        },
        "quality_warnings": []
    });
    let scan_dropped_count = scan_stats
        .get("dropped_by_type")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        + scan_stats
            .get("dropped_by_scope")
            .and_then(Value::as_u64)
            .unwrap_or(0)
        + scan_stats
            .get("selected_node_dropped_candidate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0)
        + scan_stats
            .get("secondary_index_dropped_candidate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
    let dropped_ref_count = dropped_over_budget
        + dropped_cross_budget
        + dropped_cross_session_cap
        + dropped_cross_candidate_cap
        + dropped_source_role_budget
        + dropped_memory_selection_policy_budget
        + dropped_extraction_phase_budget
        + dropped_policy_ref
        + dropped_duplicate_ref
        + scan_dropped_count;
    let candidate_cache_hit = scan_stats
        .get("candidate_cache_hit")
        .or_else(|| scan_stats.get("cache_hit"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let candidate_cache_scope = scan_stats.get("candidate_cache_scope").and_then(Value::as_str).unwrap_or("process_global");
    let native_placement_candidate_cache_hit = scan_stats
        .get("native_placement_candidate_cache_hit")
        .and_then(Value::as_bool)
        .unwrap_or(candidate_cache_hit);
    let native_placement_candidate_cache_entries = scan_stats
        .get("native_placement_candidate_cache_entries")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let native_candidate_cache_key_shape = scan_stats
        .get("native_candidate_cache_key_shape")
        .and_then(Value::as_str)
        .unwrap_or("storage_prefix+count+scope+record_types+selected_node_hashes+secondary_index_groups+return_index_records");
    let native_candidate_cache_payload = scan_stats
        .get("native_candidate_cache_payload")
        .and_then(Value::as_str)
        .unwrap_or("compact_struct");
    let serving_memory_cache_layer = scan_stats
        .get("serving_memory_cache_layer")
        .and_then(Value::as_str)
        .unwrap_or("rust_proxy_scan_cache");
    let serving_memory_promoted = scan_stats
        .get("serving_memory_promoted")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let serving_memory_promoted_record_count = scan_stats
        .get("serving_memory_promoted_record_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let scanned_records = scan_stats
        .get("scanned_records")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let placement_partitions_touched = scan_stats
        .get("placement_partitions_touched")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let index_postings_read = scan_stats
        .get("index_postings_read")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut output = json!({
        "ok": true,
        "count": selected.len(),
        "native_pack_assembly": true,
        "raw_records_returned": false,
        "python_hot_path_records": 0,
        "scan_count": scanned_records,
        "cache_hit": candidate_cache_hit,
        "selected_ref_count": selected.len(),
        "dropped_ref_count": dropped_ref_count,
        "dropped_duplicate_ref_count": dropped_duplicate_ref,
        "retrieval_metrics": {
            "query_plan_ms": 0.0,
            "node_traversal_ms": 0.0,
            "index_prefilter_ms": 0.0,
            "candidate_fetch_ms": candidate_fetch_ms,
            "score_ms": score_ms,
            "pack_ms": total_ms,
            "audit_ms": 0.0,
            "append_queue_wait_ms": 0.0,
            "append_engine_ms": 0.0,
            "selected_refs": selected.len(),
            "dropped_refs": dropped_ref_count,
            "scanned_records": scanned_records,
            "index_postings_read": index_postings_read,
            "index_postings_touched": index_postings_read,
            "placement_partitions_touched": placement_partitions_touched,
            "candidate_cache_hit": candidate_cache_hit,
            "cache_hit": candidate_cache_hit,
            "candidate_cache_scope": candidate_cache_scope,
            "native_placement_candidate_cache_hit": native_placement_candidate_cache_hit,
            "native_placement_candidate_cache_entries": native_placement_candidate_cache_entries,
            "native_candidate_cache_key_shape": native_candidate_cache_key_shape,
            "native_candidate_cache_payload": native_candidate_cache_payload,
            "serving_memory_cache_layer": serving_memory_cache_layer,
            "serving_memory_promoted": serving_memory_promoted,
            "serving_memory_promoted_record_count": serving_memory_promoted_record_count,
            "compact_index_bucket_used": index_postings_read > 0,
            "compact_index_bucket_count": index_postings_read,
            "native_pack_assembly": true,
            "python_pack_fallback": false,
            "raw_candidate_tables_returned": false,
            "memory_layer_budget": serving_memory_layer_budget,
            "dropped_memory_layer_budget": serving_dropped_memory_layer_budget,
            "memory_layer_pressure": serving_memory_layer_pressure,
            "memory_inventory": memory_inventory,
            "broad_scan_used": false,
            "broad_scan_blocked": false,
            "fallback_flags": [],
            "normal_path_stages": [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack"
            ]
        },
        "context_pack": pack,
        "scan_stats": scan_stats
    });
    if let Some(metrics) = output
        .get_mut("retrieval_metrics")
        .and_then(Value::as_object_mut)
    {
        metrics.insert(
            "candidate_class_counts".to_string(),
            candidate_class_counts.clone(),
        );
    }
    let retrieval_metrics_for_pack = output.get("retrieval_metrics").cloned();
    if let (Some(metrics), Some(pack)) = (
        retrieval_metrics_for_pack,
        output.get_mut("context_pack").and_then(Value::as_object_mut),
    ) {
        pack.insert("retrieval_metrics".to_string(), metrics);
    }
    Ok(output)
}

