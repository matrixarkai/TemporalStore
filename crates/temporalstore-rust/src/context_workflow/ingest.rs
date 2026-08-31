// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Context ingest + resource/skill secondary-index verification, split from context_workflow.rs.
use super::*;

pub fn ingest_extract_context(
    engine: &TemporalEngine,
    request: ContextIngestExtractRequest,
) -> ContextIngestExtractReport {
    // Hold the per-record index reconstruct for this whole ingest and do it once below. Each
    // Context* write would otherwise rebuild the first-index over every live page in the shard --
    // eight walks of the store for one add.
    let reconstruct_window = crate::engine::IngestReconstructWindow::open();
    let policy = ContextWorkflowPolicy::default();
    let mut extracts = Vec::new();
    let mut failed_sources = Vec::new();
    let mut node_hashes = Vec::new();
    let mut source_kind_counts = BTreeMap::new();
    let mut provider_counts = BTreeMap::new();
    let provider = normalize_provider(request.provider.clone());
    let source_count = request.sources.len();
    let start_time_ms = request.start_time_ms;
    let end_time_ms = request.end_time_ms;
    let gate_l1 = context_ingest_gate_l1();

    for mut source in request.sources {
        source.shard_id = request.shard_id;
        source.tenant_hash = request.tenant_hash;
        if source.provider.provider_name.is_empty() {
            source.provider = provider.clone();
        }
        *source_kind_counts
            .entry(context_source_kind_name(source.source_kind).to_string())
            .or_insert(0) += 1;
        *provider_counts
            .entry(source.provider.provider_name.clone())
            .or_insert(0) += 1;
        let policy_report = validate_context_extract_policy(&policy, &source);
        if !policy_report.status.ok {
            failed_sources.push(ContextIngestSourceFailure {
                source_id: source.source_id,
                status: policy_report.status,
            });
            continue;
        }
        let source_id = source.source_id.clone();
        // Keep the public API's historical full-fanout default, while allowing
        // local-context backfill to opt into the same frugal L1 gate as live
        // hook ingestion.
        let extract = extract_context_gated(engine, source, gate_l1);
        if extract.status.ok {
            node_hashes.push(extract.node.node_hash);
            extracts.push(extract);
        } else {
            failed_sources.push(ContextIngestSourceFailure {
                source_id,
                status: extract.status.clone(),
            });
        }
    }

    // No reconstruct: the writes above maintain the index themselves now, so the window has
    // nothing to catch up on. It stays open across the ingest only to keep the per-record
    // fallback rebuild from firing for a write maintenance does not cover -- and if one is not
    // covered, the fallback runs at that write, not here.
    drop(reconstruct_window);

    node_hashes.sort_unstable();
    node_hashes.dedup();
    let failed = failed_sources.len();
    let accepted = extracts.len();
    let status = if failed == 0 {
        Status::ok()
    } else if accepted > 0 {
        Status::error(
            "partial_context_ingest_extract_failure",
            format!("{failed} context sources failed"),
        )
    } else {
        Status::error(
            "context_ingest_extract_failed",
            "all context sources failed ingestion/extraction",
        )
    };
    let retrieve_request = ContextRetrieveRequest {
        shard_id: request.shard_id,
        tenant_hash: request.tenant_hash,
        node_hashes: node_hashes.clone(),
        query: request.query,
        start_time_ms: request.start_time_ms,
        end_time_ms: request.end_time_ms,
        max_events: request.max_events,
        min_confidence: 0.0,
        min_importance: 0.0,
        tiers: default_tiers(),
        max_summary_nodes: default_summary_fanout_node_limit(),
        max_event_nodes: default_event_fanout_node_limit(),
        prefer_current_agent: false,
        current_agent_scope_key: default_current_agent_scope_key(),
        provider: request.provider,
    };
    let summary = ContextIngestExtractSummary {
        source_count,
        accepted,
        failed,
        unique_node_count: node_hashes.len(),
        extracted_node_count: extracts.len(),
        extracted_event_count: extracts.len(),
        extracted_index_ref_count: extracts.len(),
        extracted_dirty_marker_count: extracts.len(),
        extracted_summary_ref_count: extracts
            .iter()
            .map(|extract| extract.summary_refs.len())
            .sum(),
        retrieval_node_count: retrieve_request.node_hashes.len(),
        source_kind_counts,
        provider_counts,
        start_time_ms,
        end_time_ms,
    };

    ContextIngestExtractReport {
        status,
        accepted,
        failed,
        summary,
        extracts,
        failed_sources,
        node_hashes,
        retrieve_request,
        parity: context_pipeline_parity_evidence(),
    }
}

fn context_ingest_gate_l1() -> bool {
    std::env::var("MATRIXARK_CONTEXT_INGEST_GATE_L1")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

pub fn ingest_resource_skill_context(
    engine: &TemporalEngine,
    request: ContextResourceSkillIngestRequest,
) -> ContextResourceSkillIngestReport {
    let provider = normalize_provider(request.provider.clone());
    let mut resources = Vec::new();
    let mut skills = Vec::new();
    let mut sources = Vec::new();
    let mut resource_ref_by_source = BTreeMap::new();
    let mut skill_ref_by_source = BTreeMap::new();
    let mut timestamp_ms = request.start_time_ms.max(1);

    for resource_request in request.resources {
        // An oversized payload gets stored WHOLE in the engine-owned blob store before
        // parsing, so the synthesized external URI on the report is real and fetchable --
        // chunks stay searchable in records while the original attachment lives beside the
        // engine, one TemporalStore holding everything.
        let oversized_payload = if resource_request.text.as_bytes().len()
            > default_context_resource_max_inline_bytes()
        {
            Some(resource_request.text.clone())
        } else {
            None
        };
        let mut resource = parse_context_resource(resource_request);
        if let Some(payload) = oversized_payload {
            match engine.resource_blob_put(request.tenant_hash, payload.as_bytes()) {
                Ok((uri, _size, _hash)) => resource.external_object_uri = uri,
                Err(err) => resource
                    .parser_warnings
                    .push(format!("resource blob store put failed: {err}")),
            }
        }
        for chunk in &resource.chunks {
            resource_ref_by_source.insert(chunk.source_ref.clone(), resource.raw_uri.clone());
            sources.push(ContextExtractRequest {
                shard_id: request.shard_id,
                tenant_hash: request.tenant_hash,
                source_kind: ContextSourceKind::Document,
                source_id: chunk.source_ref.clone(),
                title: chunk
                    .metadata
                    .get("heading")
                    .cloned()
                    .unwrap_or_else(|| resource.resource_title.clone()),
                body: chunk.text.clone(),
                timestamp_ms,
                provider: provider.clone(),
            });
            timestamp_ms = timestamp_ms.saturating_add(1);
        }
        resources.push(resource);
    }

    for skill_input in request.skills {
        let skill = parse_context_skill_markdown(skill_input.raw_uri, skill_input.text);
        if skill.enabled {
            for chunk in &skill.resource.chunks {
                skill_ref_by_source.insert(chunk.source_ref.clone(), skill.skill_name.clone());
                sources.push(ContextExtractRequest {
                    shard_id: request.shard_id,
                    tenant_hash: request.tenant_hash,
                    source_kind: ContextSourceKind::Document,
                    source_id: chunk.source_ref.clone(),
                    title: format!("skill:{}", skill.skill_name),
                    body: chunk.text.clone(),
                    timestamp_ms,
                    provider: provider.clone(),
                });
                timestamp_ms = timestamp_ms.saturating_add(1);
            }
        }
        skills.push(skill);
    }
    let skill_registry = context_skill_registry_from_parsed(&skills, request.start_time_ms);
    let skill_selection = if skill_registry.entries.is_empty() {
        ContextSkillSelectionReport::default()
    } else {
        select_context_skills_for_retrieval(ContextSkillSelectionRequest {
            query: request.query.clone(),
            owner_scope: String::new(),
            tool_name: "context_workflow_harness".to_string(),
            include_disabled: false,
            allowed_scope_layers: Vec::new(),
            limit: default_skill_selection_limit(),
            registry: skill_registry.entries.clone(),
        })
    };

    let ingest = ingest_extract_context(
        engine,
        ContextIngestExtractRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            sources,
            query: request.query.clone(),
            start_time_ms: request.start_time_ms,
            end_time_ms: request.end_time_ms,
            max_events: request.max_events,
            provider,
        },
    );
    let mut fanout = ContextResourceSkillModelFanoutReport {
        node_count: ingest.extracts.len(),
        event_count: ingest.extracts.len(),
        slab_count: ingest.extracts.len(),
        embedding_count: ingest.extracts.len().saturating_mul(3),
        summary_count: ingest.extracts.len().saturating_mul(2),
        dirty_marker_count: ingest.extracts.len(),
        ..ContextResourceSkillModelFanoutReport::default()
    };
    let mut secondary_indexes = ContextResourceSkillSecondaryIndexReport::default();

    for extract in &ingest.extracts {
        let entity_ref = format!("entity:{}", extract.node.canonical_name);
        let entity_hash = stable_hash64(&entity_ref);
        let child_ref = ContextChildRef {
            parent_hash: stable_hash64(&format!("resource-skill-root:{}", request.tenant_hash)),
            child_hash: extract.node.node_hash,
            updated_at_ms: extract.event.event_time_ms,
        };
        let entity = ContextEntity {
            entity_hash,
            node_hash: extract.node.node_hash,
            entity_type: source_kind_code(ContextSourceKind::Document),
            name: extract.node.canonical_name.clone(),
            value: extract.source_ref.clone(),
            updated_at_ms: extract.event.event_time_ms,
            valid_from_ms: extract.event.event_time_ms,
            confidence: 1.0,
            source_event_hashes: vec![extract.event.event_id_hash],
            vector: Vec::new(),
        };
        let compression = ContextCompressionEvent {
            compression_id_hash: stable_hash64(&format!(
                "ctx-resource-skill-compress:{}:{}",
                extract.source_ref, extract.event.event_time_ms
            )),
            node_hash: extract.node.node_hash,
            source_start_ms: extract.event.event_time_ms,
            source_end_ms: extract.event.event_time_ms.saturating_add(1),
            compressed_time_ms: extract.event.event_time_ms.saturating_add(1),
            summary: extract.compact_summary_ref.clone(),
        };
        let mut index_writes = vec![
            ("source_ref".to_string(), extract.source_ref.clone()),
            ("entity_ref".to_string(), entity_ref.clone()),
        ];
        for summary_ref in &extract.summary_refs {
            index_writes.push(("summary_ref".to_string(), summary_ref.clone()));
        }
        if let Some(resource_ref) = resource_ref_by_source.get(&extract.source_ref) {
            index_writes.push(("resource_ref".to_string(), resource_ref.clone()));
        }
        if let Some(skill_ref) = skill_ref_by_source.get(&extract.source_ref) {
            index_writes.push(("skill_ref".to_string(), skill_ref.clone()));
        }

        for command in [
            Command::ContextUpsertEntity {
                tenant_hash: request.tenant_hash,
                entity: entity.clone(),
            },
            Command::ContextUpsertChildRef {
                tenant_hash: request.tenant_hash,
                child_ref: child_ref.clone(),
            },
            Command::ContextWriteCompressionEvent {
                tenant_hash: request.tenant_hash,
                event: compression.clone(),
            },
        ] {
            let response = engine.execute_durable(ExecuteRequest {
                shard_id: request.shard_id,
                command,
            });
            if !response.status.ok {
                fanout.missing_models.push(response.status.code);
            }
        }
        fanout.entity_count += 1;
        fanout.child_ref_count += 1;
        fanout.compression_count += 1;

        for (index_name, index_ref_value) in index_writes {
            if !crate::context_workflow::context_secondary_index_enabled() {
                // The refs are still REPORTED (callers read the report to know what would be
                // indexed); only the durable writes and their query-back are skipped.
                match index_name.as_str() {
                    "resource_ref" => secondary_indexes.resource_refs.push(index_ref_value),
                    "skill_ref" => secondary_indexes.skill_refs.push(index_ref_value),
                    "entity_ref" => secondary_indexes.entity_refs.push(index_ref_value),
                    "source_ref" => secondary_indexes.source_refs.push(index_ref_value),
                    "summary_ref" => secondary_indexes.summary_refs.push(index_ref_value),
                    _ => {}
                }
                continue;
            }
            let response = engine.execute_durable(ExecuteRequest {
                shard_id: request.shard_id,
                command: Command::ContextWriteIndexRef {
                    tenant_hash: request.tenant_hash,
                    index_name: index_name.clone(),
                    index_value_hash: stable_hash64(&index_ref_value),
                    scope_hash: 0,
                    event_time_ms: extract.event.event_time_ms,
                    index_ref: extract.index_ref.clone(),
                },
            });
            if response.status.ok {
                fanout.secondary_index_count += 1;
                match index_name.as_str() {
                    "resource_ref" => secondary_indexes.resource_refs.push(index_ref_value),
                    "skill_ref" => secondary_indexes.skill_refs.push(index_ref_value),
                    "entity_ref" => secondary_indexes.entity_refs.push(index_ref_value),
                    "source_ref" => secondary_indexes.source_refs.push(index_ref_value),
                    "summary_ref" => secondary_indexes.summary_refs.push(index_ref_value),
                    _ => {}
                }
            } else {
                secondary_indexes
                    .missing_refs
                    .push(format!("{index_name}:{index_ref_value}"));
            }
        }

    }
    secondary_indexes.resource_refs.sort();
    secondary_indexes.resource_refs.dedup();
    secondary_indexes.skill_refs.sort();
    secondary_indexes.skill_refs.dedup();
    secondary_indexes.entity_refs.sort();
    secondary_indexes.entity_refs.dedup();
    secondary_indexes.source_refs.sort();
    secondary_indexes.source_refs.dedup();
    secondary_indexes.summary_refs.sort();
    secondary_indexes.summary_refs.dedup();
    let embedding_evidence = context_resource_skill_embedding_evidence(&ingest.extracts);

    verify_resource_skill_fanout(
        engine,
        request.shard_id,
        request.tenant_hash,
        &ingest.extracts,
        request.start_time_ms,
        request.end_time_ms,
        &mut secondary_indexes,
        &mut fanout,
    );
    let retrieval = retrieve_context(engine, ingest.retrieve_request.clone());
    let mut status = if ingest.status.ok
        && fanout.query_back_ok
        && secondary_indexes.query_back_ok
        && retrieval.status.ok
    {
        Status::ok()
    } else {
        Status::error(
            "context_resource_skill_ingest_incomplete",
            "resource/skill ingest did not satisfy all fanout checks",
        )
    };
    if ingest.accepted == 0 {
        status = Status::error(
            "context_resource_skill_ingest_empty",
            "resource/skill ingest produced no accepted context sources",
        );
    }
    let resource_lifecycle = context_resource_lifecycle_report(
        resources
            .iter()
            .map(|resource| resource.lifecycle.clone())
            .collect(),
        request.start_time_ms,
    );

    ContextResourceSkillIngestReport {
        status,
        resources,
        resource_lifecycle,
        skills,
        skill_registry,
        skill_selection,
        ingest,
        embedding_evidence,
        fanout,
        secondary_indexes,
        retrieval,
        parity: context_pipeline_parity_evidence(),
    }
}

pub fn validate_resource_skill_secondary_indexes(
    engine: &TemporalEngine,
    request: ContextResourceSkillSecondaryIndexValidationRequest,
) -> ContextResourceSkillSecondaryIndexValidationReport {
    let family_inputs: [(&str, &[String]); 5] = [
        ("resource_ref", &request.secondary_indexes.resource_refs),
        ("skill_ref", &request.secondary_indexes.skill_refs),
        ("entity_ref", &request.secondary_indexes.entity_refs),
        ("source_ref", &request.secondary_indexes.source_refs),
        ("summary_ref", &request.secondary_indexes.summary_refs),
    ];
    let mut checked_ref_count = 0;
    let mut found_ref_count = 0;
    let mut missing_refs = Vec::new();
    let mut families = Vec::new();

    for (index_name, refs) in family_inputs {
        let family_missing = query_missing_secondary_index_refs(
            engine,
            request.shard_id,
            request.tenant_hash,
            index_name,
            refs,
            request.start_time_ms,
            request.end_time_ms,
        );
        checked_ref_count += refs.len();
        found_ref_count += refs.len().saturating_sub(family_missing.len());
        missing_refs.extend(family_missing.iter().cloned());
        families.push(ContextSecondaryIndexFamilyValidationReport {
            index_name: index_name.to_string(),
            checked_ref_count: refs.len(),
            found_ref_count: refs.len().saturating_sub(family_missing.len()),
            missing_refs: family_missing,
        });
    }

    missing_refs.sort();
    missing_refs.dedup();
    let query_back_ok = checked_ref_count > 0 && missing_refs.is_empty();
    let status = if query_back_ok {
        Status::ok()
    } else if checked_ref_count == 0 {
        Status::error(
            "context_resource_skill_secondary_index_empty",
            "resource/skill ingest produced no secondary index refs to validate",
        )
    } else {
        Status::error(
            "context_resource_skill_secondary_index_missing",
            "resource/skill secondary indexes were not fully queryable",
        )
    };

    ContextResourceSkillSecondaryIndexValidationReport {
        status,
        query_back_ok,
        checked_ref_count,
        found_ref_count,
        missing_refs,
        families,
    }
}

pub(crate) fn verify_resource_skill_fanout(
    engine: &TemporalEngine,
    shard_id: ShardId,
    tenant_hash: u64,
    extracts: &[ContextExtractReport],
    start_time_ms: u64,
    end_time_ms: u64,
    secondary_indexes: &mut ContextResourceSkillSecondaryIndexReport,
    fanout: &mut ContextResourceSkillModelFanoutReport,
) {
    let root_hash = stable_hash64(&format!("resource-skill-root:{tenant_hash}"));
    let mut missing = Vec::new();

    for extract in extracts {
        let node = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextGetNode {
                tenant_hash,
                node_hash: extract.node.node_hash,
            },
        });
        if !matches!(
            node.response,
            CommandResponse::ContextNode { node: Some(_), .. }
        ) {
            missing.push("ContextNodeModel".to_string());
        }

        let events = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQueryEvents {
                tenant_hash,
                node_hash: extract.node.node_hash,
                start_time_ms: extract.event.event_time_ms.saturating_sub(1),
                end_time_ms: extract.event.event_time_ms.saturating_add(1),
                limit: Some(8),
                max_scan: None,
                current_valid_only: false,
                as_of_ms: 0,
                kinds: Vec::new(),
                statuses: Vec::new(),
                min_confidence: 0.0,
                min_importance: 0.0,
            },
        });
        if !matches!(
            events.response,
            CommandResponse::ContextEvents { ref events, .. }
                if events.iter().any(|event| event.event_id_hash == extract.event.event_id_hash)
        ) {
            missing.push("ContextEventModel".to_string());
            missing.push("ContextSegment".to_string());
        }

        let entity_hash = stable_hash64(&format!("entity:{}", extract.node.canonical_name));
        let entity = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextGetEntity {
                tenant_hash,
                node_hash: extract.node.node_hash,
                entity_hash,
            },
        });
        if !matches!(
            entity.response,
            CommandResponse::ContextEntity {
                entity: Some(_),
                ..
            }
        ) {
            missing.push("ContextEntityModel".to_string());
        }

        let summaries_l0 = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQuerySummaries {
                tenant_hash,
                node_hash: extract.node.node_hash,
                level: 1,
                as_of_ms: extract.event.event_time_ms,
                limit: Some(2),
            },
        });
        let summaries_l1 = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQuerySummaries {
                tenant_hash,
                node_hash: extract.node.node_hash,
                level: 2,
                as_of_ms: extract.event.event_time_ms,
                limit: Some(2),
            },
        });
        if !matches!(
            summaries_l0.response,
            CommandResponse::ContextSummaries { ref summaries, .. } if !summaries.is_empty()
        ) || !matches!(
            summaries_l1.response,
            CommandResponse::ContextSummaries { ref summaries, .. } if !summaries.is_empty()
        ) {
            missing.push("ContextSummaryModel".to_string());
        }

        let compression = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQueryCompressionEvents {
                tenant_hash,
                node_hashes: vec![extract.node.node_hash],
                start_time_ms: extract.event.event_time_ms.saturating_sub(1),
                end_time_ms: extract.event.event_time_ms.saturating_add(2),
                limit: Some(2),
            },
        });
        if !matches!(
            compression.response,
            CommandResponse::ContextCompressionEvents { ref events, .. } if !events.is_empty()
        ) {
            missing.push("ContextCompressionModel".to_string());
        }
    }

    let children = engine.execute(ExecuteRequest {
        shard_id,
        command: Command::ContextQueryChildren {
            tenant_hash,
            parent_hash: root_hash,
            limit: Some(extracts.len().max(1)),
        },
    });
    if !matches!(
        children.response,
        CommandResponse::ContextChildRefs { ref refs, .. } if refs.len() >= extracts.len()
    ) {
        missing.push("ContextChildModel".to_string());
    }

    // Embedding evidence is owner-side now: the vector lives on the node (and the summaries),
    // so the query-back asks the owners rather than a separate keyspace that no longer exists.
    let node_hashes: Vec<u64> = extracts
        .iter()
        .map(|extract| extract.node.node_hash)
        .collect();
    if !node_hashes.is_empty() {
        let nodes = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextGetNodes {
                tenant_hash,
                node_hashes: node_hashes.clone(),
            },
        });
        if !matches!(
            nodes.response,
            CommandResponse::ContextNodes { ref nodes }
                if nodes.len() == node_hashes.len()
                    && nodes.iter().all(|node| !node.vector.is_empty())
        ) {
            missing.push("ContextEmbeddingModel".to_string());
        }
    }

    let resource_refs = secondary_indexes.resource_refs.clone();
    let skill_refs = secondary_indexes.skill_refs.clone();
    let entity_refs = secondary_indexes.entity_refs.clone();
    let source_refs = secondary_indexes.source_refs.clone();
    let summary_refs = secondary_indexes.summary_refs.clone();
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "resource_ref",
        &resource_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "skill_ref",
        &skill_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "entity_ref",
        &entity_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "source_ref",
        &source_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "summary_ref",
        &summary_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );

    missing.sort();
    missing.dedup();
    fanout.missing_models.extend(missing);
    fanout.missing_models.sort();
    fanout.missing_models.dedup();
    fanout.query_back_ok = fanout.missing_models.is_empty();
    secondary_indexes.missing_refs.sort();
    secondary_indexes.missing_refs.dedup();
    secondary_indexes.query_back_ok = secondary_indexes.missing_refs.is_empty();
}

pub(crate) fn context_resource_skill_embedding_evidence(
    extracts: &[ContextExtractReport],
) -> ContextResourceSkillEmbeddingEvidenceReport {
    let mut report = ContextResourceSkillEmbeddingEvidenceReport {
        extract_count: extracts.len(),
        production_evidence_ready: !extracts.is_empty(),
        ..ContextResourceSkillEmbeddingEvidenceReport::default()
    };
    for extract in extracts {
        let generation = &extract.embedding_generation;
        report.requested_vector_count = report
            .requested_vector_count
            .saturating_add(generation.requested_vector_count);
        report.generated_vector_count = report
            .generated_vector_count
            .saturating_add(generation.generated_vector_count);
        report.live_call_count = report
            .live_call_count
            .saturating_add(generation.live_call_count);
        if generation.mock_mode {
            report.mock_generation_count = report.mock_generation_count.saturating_add(1);
        }
        if generation.fallback_used {
            report.fallback_generation_count = report.fallback_generation_count.saturating_add(1);
        }
        report.provider_names.push(generation.provider_name.clone());
        report
            .embedding_models
            .push(generation.embedding_model.clone());
        if generation.vector_dimension > 0 {
            report.vector_dimensions.push(generation.vector_dimension);
        }
        report.production_evidence_ready &= generation.production_evidence_ready
            && !generation.mock_mode
            && generation.live_call_count > 0
            && generation.generated_vector_count == generation.requested_vector_count
            && generation.vector_dimension > 0;
    }
    report.provider_names.sort();
    report.provider_names.dedup();
    report.embedding_models.sort();
    report.embedding_models.dedup();
    report.vector_dimensions.sort_unstable();
    report.vector_dimensions.dedup();
    report
}

pub(crate) fn verify_secondary_index_refs(
    engine: &TemporalEngine,
    shard_id: ShardId,
    tenant_hash: u64,
    index_name: &str,
    refs: &[String],
    start_time_ms: u64,
    end_time_ms: u64,
    report: &mut ContextResourceSkillSecondaryIndexReport,
) {
    // Nothing was written when index building is off, so there is nothing to query back --
    // reporting every ref as missing would fail the ingest for skipping work on purpose.
    if !crate::context_workflow::context_secondary_index_enabled() {
        return;
    }
    report
        .missing_refs
        .extend(query_missing_secondary_index_refs(
            engine,
            shard_id,
            tenant_hash,
            index_name,
            refs,
            start_time_ms,
            end_time_ms,
        ));
}

pub(crate) fn query_missing_secondary_index_refs(
    engine: &TemporalEngine,
    shard_id: ShardId,
    tenant_hash: u64,
    index_name: &str,
    refs: &[String],
    start_time_ms: u64,
    end_time_ms: u64,
) -> Vec<String> {
    refs.iter()
        .filter_map(|value| {
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: Command::ContextQueryIndex {
                    tenant_hash,
                    index_name: index_name.to_string(),
                    index_value_hash: stable_hash64(value),
                    scope_hash: 0,
                    start_time_ms,
                    end_time_ms,
                    limit: Some(8),
                },
            });
            if matches!(
                response.response,
                CommandResponse::ContextIndexRefs { ref refs, .. } if !refs.is_empty()
            ) {
                None
            } else {
                Some(format!("{index_name}:{value}"))
            }
        })
        .collect()
}
