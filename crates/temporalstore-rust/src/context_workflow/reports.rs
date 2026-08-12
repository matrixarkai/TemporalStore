// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Context workflow provider/state/manage report + policy-validation fns, split from context_workflow.rs.
use super::*;

pub fn default_context_model_providers() -> Vec<ContextModelProviderConfig> {
    vec![
        ContextModelProviderConfig::default(),
        ContextModelProviderConfig {
            provider_name: "openai-compatible".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            api_key_env: "OPENAI_API_KEY".to_string(),
            model: "local-or-commercial-chat-model".to_string(),
            embedding_model: "local-or-commercial-embedding-model".to_string(),
            vlm_model: "local-or-commercial-vlm-model".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "reference-open-source-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            api_key_env: "MATRIXARK_MODEL_API_KEY".to_string(),
            model: "qwen2.5:7b-instruct".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            vlm_model: "qwen2.5vl:7b".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "reference-gpt-4o-mini-reader".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "https://api.openai.com/v1".to_string(),
            api_key_env: "OPENAI_API_KEY".to_string(),
            model: "gpt-4o-mini".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            vlm_model: "none".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "matrixark-cpp-oss-context".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            api_key_env: "MATRIXARK_MODEL_API_KEY".to_string(),
            model: "google/flan-t5-small".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            vlm_model: "none".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "reference-open-source-gpt-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            api_key_env: "MATRIXARK_MODEL_API_KEY".to_string(),
            model: "lmsys/vicuna-7b-v1.5".to_string(),
            embedding_model: "BAAI/bge-m3".to_string(),
            vlm_model: "Vision-CAIR/MiniGPT-4".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
    ]
}

pub fn reference_open_source_model_profiles() -> Vec<ContextReferenceModelProfile> {
    vec![
        ContextReferenceModelProfile {
            profile_name: "reference-qwen2_5_vl-local".to_string(),
            provider_name: "reference-open-source-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            chat_model: "qwen2.5:7b-instruct".to_string(),
            vlm_model: "qwen2.5vl:7b".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "Recommended local reference-style profile for Ollama or another OpenAI-compatible local gateway."
                .to_string(),
        },
        ContextReferenceModelProfile {
            profile_name: "reference-llava-local".to_string(),
            provider_name: "reference-llava-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            chat_model: "llama3.1:8b-instruct".to_string(),
            vlm_model: "llava:7b".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "Fallback local profile for LLaVA-compatible OpenAI gateway deployments."
                .to_string(),
        },
        ContextReferenceModelProfile {
            profile_name: "reference-internvl-vllm".to_string(),
            provider_name: "reference-internvl-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            chat_model: "Qwen/Qwen2.5-7B-Instruct".to_string(),
            vlm_model: "OpenGVLab/InternVL2_5-8B".to_string(),
            embedding_model: "BAAI/bge-m3".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "reference-style vLLM or OpenAI-compatible gateway profile for GPU deployments."
                .to_string(),
        },
        ContextReferenceModelProfile {
            profile_name: "reference-gpt-4o-mini-reader".to_string(),
            provider_name: "reference-gpt-4o-mini-reader".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "https://api.openai.com/v1".to_string(),
            chat_model: "gpt-4o-mini".to_string(),
            vlm_model: "none".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            capabilities: vec![
                "reference_reader_parity".to_string(),
                "chat_context_extraction".to_string(),
                "semantic_retrieval".to_string(),
                "locomo_context_benchmark".to_string(),
                "longmemeval_s_context_benchmark".to_string(),
            ],
            notes: "Reference benchmark reader profile using GPT-4o-mini through an OpenAI-compatible /v1/chat/completions endpoint."
                .to_string(),
        },
        ContextReferenceModelProfile {
            profile_name: "matrixark-cpp-oss-context".to_string(),
            provider_name: "matrixark-cpp-oss-context".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            chat_model: "google/flan-t5-small".to_string(),
            vlm_model: "none".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            capabilities: vec![
                "cpp_path_oss_model_parity".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
                "locomo_context_benchmark".to_string(),
            ],
            notes: "Matches the MatrixArk/C++ path OSS benchmark setup from the LLM Specific TemporalStore Use Cases thread: transformers extraction with google/flan-t5-small and sentence-transformers/all-MiniLM-L6-v2 embeddings."
                .to_string(),
        },
        ContextReferenceModelProfile {
            profile_name: "reference-minigpt4-gpt-style-vlm".to_string(),
            provider_name: "reference-open-source-gpt-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            chat_model: "lmsys/vicuna-7b-v1.5".to_string(),
            vlm_model: "Vision-CAIR/MiniGPT-4".to_string(),
            embedding_model: "BAAI/bge-m3".to_string(),
            capabilities: vec![
                "gpt_style_vlm_reasoning".to_string(),
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "Open-source GPT-4-style VLM profile inspired by MiniGPT-4; serve through an OpenAI-compatible gateway for reference-style image/content understanding."
                .to_string(),
        },
    ]
}

pub fn reference_context_parity_cases() -> Vec<ContextReferenceParityCase> {
    vec![
        ContextReferenceParityCase {
            case_name: "locomo_multi_hop_project_selection".to_string(),
            category: "multi_hop_reasoning".to_string(),
            query: "Which project did Lee pick because Dana suggested it during planning?"
                .to_string(),
            positive_memory: "Later planning note: Dana suggested the observability dashboard because the team needed better benchmark traces, so Lee picked that project.".to_string(),
            stale_memory: "Initial planning thread: Lee considered a search cleanup project and had not chosen the final work item.".to_string(),
            expected_terms: vec!["Dana".to_string(), "observability dashboard".to_string()],
            expected_model_profile: "reference-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextReferenceParityCase {
            case_name: "locomo_temporal_reschedule".to_string(),
            category: "temporal".to_string(),
            query: "When is Maya's dentist appointment after it was rescheduled?".to_string(),
            positive_memory: "Latest calendar update: Maya rescheduled the dentist appointment to Thursday at 3pm after the clinic called.".to_string(),
            stale_memory: "Earlier memory: Maya had a dentist appointment scheduled for Tuesday morning.".to_string(),
            expected_terms: vec!["Thursday".to_string(), "3pm".to_string()],
            expected_model_profile: "reference-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextReferenceParityCase {
            case_name: "longmem_memory_update_risk_score".to_string(),
            category: "memory_update".to_string(),
            query: "What risk score was recorded after the latest fraud review?".to_string(),
            positive_memory: "Latest fraud review: the checkout risk score was updated to 87 after the payment incident escalated.".to_string(),
            stale_memory: "Earlier fraud review: the checkout risk score was 42 before the payment incident escalated.".to_string(),
            expected_terms: vec!["87".to_string()],
            expected_model_profile: "reference-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextReferenceParityCase {
            case_name: "locomo_stale_memory_current_pet".to_string(),
            category: "stale_memory".to_string(),
            query: "What is the dog's name in the latest pet update?".to_string(),
            positive_memory: "Latest pet update: the newly adopted dog is named Miso and needs evening walks.".to_string(),
            stale_memory: "Old profile note: the family dog was called Pepper in a previous home.".to_string(),
            expected_terms: vec!["Miso".to_string()],
            expected_model_profile: "reference-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextReferenceParityCase {
            case_name: "locomo_open_domain_cafe_recommendation".to_string(),
            category: "open_domain_retrieval".to_string(),
            query: "Who recommended the cafe that Nina booked after the conference?".to_string(),
            positive_memory: "Later chat: Omar recommended the quiet riverside cafe, and Nina booked it after the conference.".to_string(),
            stale_memory: "Earlier conversation: Nina wanted to book a cafe after the conference but had not chosen one yet.".to_string(),
            expected_terms: vec!["Omar".to_string(), "riverside cafe".to_string()],
            expected_model_profile: "reference-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextReferenceParityCase {
            case_name: "reference_vlm_receipt_context".to_string(),
            category: "vlm_image_content_understanding".to_string(),
            query: "What merchant and total should be remembered from the receipt image?"
                .to_string(),
            positive_memory: "VLM extraction note: the receipt image shows merchant Northstar Cafe and total $18.40 for the lunch order.".to_string(),
            stale_memory: "Older image note: a different receipt showed merchant Harbor Books and total $42.00.".to_string(),
            expected_terms: vec!["Northstar Cafe".to_string(), "$18.40".to_string()],
            expected_model_profile: "reference-minigpt4-gpt-style-vlm".to_string(),
            uses_vlm: true,
            benchmark_proven: false,
        },
    ]
}

pub fn context_workflow_state_report() -> ContextWorkflowStateReport {
    let reference_parity_cases = reference_context_parity_cases();
    let mut reference_parity_categories = reference_parity_cases
        .iter()
        .map(|case| case.category.clone())
        .collect::<Vec<_>>();
    reference_parity_categories.sort();
    reference_parity_categories.dedup();
    let reference_model_profiles = reference_open_source_model_profiles();
    let vlm_provider_configured = reference_model_profiles
        .iter()
        .any(|profile| profile.vlm_model != "none");
    ContextWorkflowStateReport {
        status: Status::ok(),
        providers: default_context_model_providers(),
        context_model_descriptors: context_model_descriptors(),
        reference_model_profiles,
        reference_parity_cases,
        reference_parity_categories,
        open_model_provider_packaged: true,
        open_model_local_run_proven: false,
        vlm_provider_configured,
        vlm_benchmark_proven: false,
        policy: ContextWorkflowPolicy::default(),
        parity: context_pipeline_parity_evidence(),
        reference_comparison:
            "TemporalStore keeps hierarchical L0/L1/L2 context, but stores it in ContextNode/Event/Index/Audit models instead of a separate archival filesystem."
                .to_string(),
        supported_routes: vec![
            "/context/extract".to_string(),
            "/context/ingest_extract".to_string(),
            "/context/retrieve".to_string(),
            "/context/inject".to_string(),
            "/context/manage".to_string(),
            "/context/workflow/state".to_string(),
            "/context/model/providers".to_string(),
            "/context/model/provider".to_string(),
        ],
    }
}

pub fn context_pipeline_manage_report() -> ContextPipelineManageReport {
    let state = context_workflow_state_report();
    let parity = context_pipeline_parity_evidence();
    let stages = vec![
        "manage".to_string(),
        "ingest".to_string(),
        "extract".to_string(),
        "index".to_string(),
        "retrieve".to_string(),
        "inject".to_string(),
        "audit".to_string(),
    ];
    let stage_reports = vec![
        ContextPipelineStageReport {
            stage: "manage".to_string(),
            ready: parity.pipeline_ready,
            route: Some("/context/manage".to_string()),
            evidence: vec![
                "reports supported routes, providers, policy controls, and stage readiness"
                    .to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "ingest".to_string(),
            ready: parity.extraction_stage_ready,
            route: Some("/context/ingest_extract".to_string()),
            evidence: vec![
                "accepts multiple Context sources under one shard/tenant batch".to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "extract".to_string(),
            ready: parity.extraction_stage_ready,
            route: Some("/context/extract".to_string()),
            evidence: vec![
                "normalizes provider policy and writes ContextNode/Event/IndexRef/DirtyMarker"
                    .to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "index".to_string(),
            ready: parity.index_refs_ready,
            route: None,
            evidence: vec!["source index refs and dirty summary markers are persisted".to_string()],
        },
        ContextPipelineStageReport {
            stage: "retrieve".to_string(),
            ready: parity.retrieval_stage_ready,
            route: Some("/context/retrieve".to_string()),
            evidence: vec![
                "retrieval builds L0/L1/L2 blocks from node hashes and time windows".to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "inject".to_string(),
            ready: parity.injection_stage_ready,
            route: Some("/context/inject".to_string()),
            evidence: vec![
                "prompt injection enforces token budget and records selected refs".to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "audit".to_string(),
            ready: parity.pack_audit_ready && parity.summary_dirty_ready,
            route: None,
            evidence: vec!["ContextPackAudit and summary dirty markers are persisted".to_string()],
        },
    ];
    let provider_names = state
        .providers
        .iter()
        .map(|provider| provider.provider_name.clone())
        .collect();
    ContextPipelineManageReport {
        status: Status::ok(),
        pipeline_ready: parity.pipeline_ready,
        management_ready: true,
        ingestion_extraction_ready: true,
        retrieval_ready: true,
        injection_ready: true,
        provider_count: state.providers.len(),
        supported_routes: state.supported_routes,
        stages,
        stage_reports,
        provider_names,
        policy_controls: vec![
            "provider allow-list".to_string(),
            "model allow-list".to_string(),
            "PII filtering".to_string(),
            "tenant isolation".to_string(),
            "prompt token budget".to_string(),
            "rate limit".to_string(),
            "provider failure budget".to_string(),
        ],
        parity,
    }
}

pub fn validate_context_extract_policy(
    policy: &ContextWorkflowPolicy,
    request: &ContextExtractRequest,
) -> ContextWorkflowPolicyReport {
    context_policy_report_for_text(
        policy,
        &request.provider,
        request.tenant_hash,
        request.body.as_str(),
        estimate_tokens(&request.body),
        request.body.len(),
    )
}

pub fn validate_context_inject_policy(
    policy: &ContextWorkflowPolicy,
    request: &ContextInjectRequest,
) -> ContextWorkflowPolicyReport {
    context_policy_report_for_text(
        policy,
        &request.provider,
        request.retrieve.tenant_hash,
        request.prompt.as_str(),
        request
            .max_prompt_tokens
            .max(estimate_tokens(&request.prompt)),
        request.prompt.len(),
    )
}
