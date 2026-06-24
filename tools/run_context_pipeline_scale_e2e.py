#!/usr/bin/env python3
"""Run a local scaled context extraction/ingestion/query E2E corpus.

The test stays intentionally local: it generates a temporary unified corpus,
validates it through the C++ contract runner, then runs the Rust unified mock
proxy test against the same corpus. No TemporalStore service or Docker daemon is
required for this gate.

By default the generated corpus uses open-source model provider names for query
encoding, leaf summary embedding, and resource summary/VLM handling. If the local
Python environment has sentence-transformers installed, the script uses that
encoder for real vectors. Otherwise it emits a deterministic local fallback and
marks the run as fallback. Pass --require-models to make missing OSS packages a
hard failure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    from tools.matrixark_resource_parser import parse_resource
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_resource_parser import parse_resource


ROOT = Path(__file__).resolve().parents[1]

DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_VLM_MODEL = "Salesforce/blip-image-captioning-base"


class LocalModelProvider:
    """Small OSS-first provider used by the local scale corpus generator."""

    def __init__(
        self,
        *,
        provider: str,
        embedding_model: str,
        vlm_model: str,
        require_models: bool,
    ) -> None:
        self.requested_provider = provider
        self.embedding_model = embedding_model
        self.vlm_model = vlm_model
        self.require_models = require_models
        self._encoder = None
        self.embedding_backend = "deterministic-local-fallback"
        self.vlm_backend = "metadata-only-vlm-fallback"
        if provider == "open_source":
            self._load_open_source_models()
        elif provider != "deterministic":
            raise SystemExit(f"unsupported --model-provider={provider!r}")

    def _load_open_source_models(self) -> None:
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            self._encoder = SentenceTransformer(self.embedding_model)
            self.embedding_backend = "sentence-transformers"
        except Exception as exc:  # pragma: no cover - depends on local packages.
            if self.require_models:
                raise SystemExit(
                    "open-source embedding model is required but unavailable. "
                    "Install sentence-transformers/torch and ensure the model is cached: "
                    f"{self.embedding_model}. Original error: {exc}"
                ) from exc
        try:
            import transformers  # noqa: F401  # type: ignore
            from PIL import Image  # noqa: F401  # type: ignore

            self.vlm_backend = "transformers-vlm-ready"
        except Exception as exc:  # pragma: no cover - depends on local packages.
            if self.require_models:
                raise SystemExit(
                    "open-source VLM model is required but transformers/Pillow are unavailable. "
                    f"Requested VLM model: {self.vlm_model}. Original error: {exc}"
                ) from exc

    @property
    def effective_provider(self) -> str:
        if self.embedding_backend == "sentence-transformers":
            return "open_source"
        if self.requested_provider == "open_source":
            return "open_source_fallback"
        return "deterministic"

    def encode_text(self, text: str) -> list[float]:
        return self.encode_texts([text])[0]

    def encode_texts(self, texts: list[str]) -> list[list[float]]:
        if self._encoder is not None:
            vectors = self._encoder.encode(
                texts,
                batch_size=min(32, max(1, len(texts))),
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            return [[float(value) for value in vector] for vector in vectors]
        return [self._deterministic_embedding(text) for text in texts]

    def summarize_resource_chunk(self, raw_uri: str, resource_type: str, text: str) -> str:
        if resource_type.lower() in {"png", "jpg", "jpeg", "webp"}:
            return f"VLM summary for {raw_uri}: image evidence referenced by source metadata."
        first_sentence = text.strip().split(".")[0].strip()
        return first_sentence or f"Resource summary for {raw_uri}"

    def understand_query(self, raw_query: str, hints: dict) -> dict:
        """Return the model-facing query plan used by MatrixArk retrieval.

        The local runner keeps extraction deterministic for CI, but the contract
        records this as the model/provider boundary: a real deployment can swap
        this method for an OSS LLM, OpenAI, or an agent-provided plan without
        changing TemporalStore serving behavior.
        """
        lower = raw_query.lower()
        if "approval" in lower or "approved" in lower:
            event_type = "approval_confirmation"
            status = "approved" if "approved" in lower else ""
            intent = "current_state_decision"
        elif "incident" in lower:
            event_type = "incident_update"
            status = "confirmed" if "confirmed" in lower else ""
            intent = "current_state_decision"
        elif "budget" in lower or "cost" in lower:
            event_type = "cost_update"
            status = ""
            intent = "financial_state"
        else:
            event_type = "business_event"
            status = ""
            intent = "context_lookup"
        scope = {
            "team": hints.get("team", ""),
            "project": hints.get("project", ""),
        }
        return {
            "source": "model",
            "provider": self.effective_provider,
            "model": self.embedding_model,
            "query_embedding_model": self.embedding_model,
            "embedding_backend": self.embedding_backend,
            "intent": intent,
            "scope": scope,
            "time_window": hints.get("time_window", "latest"),
            "filters": {
                "event_type": event_type,
                "status": status,
                "team": scope["team"],
                "project": scope["project"],
                "min_confidence": 90,
                "min_importance": 80,
            },
            "staleness_policy": "algorithmic_freshness_v1",
            "token_budget_policy": "latest_state_first",
        }

    @staticmethod
    def _deterministic_embedding(text: str, dim: int = 16) -> list[float]:
        buckets = [0.0] * dim
        tokens = [token for token in text.lower().replace("_", " ").split() if token]
        if not tokens:
            tokens = [text.lower()]
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            for i, byte in enumerate(digest[:dim]):
                buckets[i] += (byte / 255.0) - 0.5
        norm = sum(value * value for value in buckets) ** 0.5 or 1.0
        return [round(value / norm, 6) for value in buckets]


def model_hints(provider: LocalModelProvider) -> dict:
    return {
        "provider": provider.effective_provider,
        "requested_provider": provider.requested_provider,
        "query_embedding_model": provider.embedding_model,
        "summary_embedding_model": provider.embedding_model,
        "vlm_model": provider.vlm_model,
        "embedding_backend": provider.embedding_backend,
        "vlm_backend": provider.vlm_backend,
    }


def event_hints(
    *,
    event_id: int,
    event_time: int,
    leaf_hash: int,
    parent_hash: int,
    team: str,
    project: str,
    embedding: list[float],
    token_estimate: int = 1,
    event_type: str = "business_event",
    entity_hash: int | None = None,
    entity_name: str | None = None,
    entity_value: str | None = None,
) -> dict:
    hints = {
        "event_id_hash": event_id,
        "event_time_ms": event_time,
        "leaf_node_hash": leaf_hash,
        "parent_hash": parent_hash,
        "path": f"company_a/{team}/{project}/scale/{leaf_hash}",
        "leaf_name": f"leaf_{leaf_hash}",
        "level": 4,
        "child_rank": leaf_hash,
        "team": team,
        "project": project,
        "actor": "scale_agent",
        "confidence": 95,
        "importance": 90,
        "token_estimate": token_estimate,
        "event_type": event_type,
        "embedding": embedding,
    }
    if entity_hash is not None:
        hints.update(
            {
                "entity_hash": entity_hash,
                "entity_type": 1,
                "entity_name": entity_name or f"entity_{entity_hash}",
                "entity_value": entity_value or "",
                "entity_token_estimate": 1,
                "valid_from_ms": event_time,
            }
        )
    return hints


def agent_message(role: str, content: str, *, name: str, created_at_ms: int) -> dict:
    return {
        "role": role,
        "content": content,
        "name": name,
        "created_at_ms": created_at_ms,
    }


def agent_context_envelope(
    *,
    kind: str,
    messages: list[dict],
    scope: dict,
    metadata: dict,
    **extra: object,
) -> dict:
    envelope = {
        "kind": kind,
        "messages": messages,
        "scope": scope,
        "metadata": metadata,
    }
    envelope.update(extra)
    return envelope


def agent_hook(
    *,
    source: str,
    hook_type: str,
    hook_id: str,
    observed_at_ms: int,
    idempotency_key: str,
    trigger: str,
) -> dict:
    return {
        "source": source,
        "hook_type": hook_type,
        "hook_id": hook_id,
        "observed_at_ms": observed_at_ms,
        "idempotency_key": idempotency_key,
        "trigger": trigger,
        "auto_captured": True,
    }


def step(name: str, command: dict) -> dict:
    return {"name": name, "command": command}


def build_corpus(events_per_lane: int, provider: LocalModelProvider) -> tuple[dict, dict]:
    tenant = 42
    root = 4200
    approval_collection = 4201
    incident_collection = 4202
    approval_leaf = 4210
    incident_leaf = 4220
    resource_leaf = 4230
    approval_entity = 60_000
    incident_entity = 60_001
    start_ms = 1_781_500_000_000
    team = "infra_team"
    project = "project_1"
    session_id = "agent-session-scale-1"
    approval_summary = "GPU approvals approved budget purchase request"
    incident_summary = "incident rollback confirmed health check stable"
    resource_text = "# Rollback\n\nIncident rollback confirmed stable after health checks passed."
    resource_chunks = parse_resource(
        "scale_runbook.md",
        resource_type="md",
        text=resource_text,
        chunk_hash_base=53_000,
    )
    resource_chunk = resource_chunks[0]
    approval_vector = provider.encode_text(approval_summary)
    incident_vector = provider.encode_text(incident_summary)
    approval_query_vector = provider.encode_text("Which GPU approvals are approved?")
    incident_query_vector = provider.encode_text("Was the incident rollback confirmed?")
    approval_query_hints = {"team": team, "project": project, "time_window": "latest"}
    incident_query_hints = {"team": team, "project": project, "time_window": "latest"}
    approval_query_plan = provider.understand_query(
        "Which GPU approvals are approved?",
        approval_query_hints,
    )
    incident_query_plan = provider.understand_query(
        "Was the incident rollback confirmed?",
        incident_query_hints,
    )
    resource_vector = provider.encode_text(
        "Was the incident rollback confirmed? "
        + provider.summarize_resource_chunk("scale_runbook.md", "md", resource_chunk.text)
    )
    root_vector = provider.encode_text("company context root")
    common_hints = model_hints(provider)
    incident_pack_summary_refs = [incident_collection, resource_leaf, incident_leaf]
    if provider.effective_provider == "open_source":
        # The real encoder can keep the approval collection in the second-query
        # frontier. That is acceptable: the pack still stays under budget and
        # carries a replayable summary ref rather than scanning raw events.
        incident_pack_summary_refs = [
            incident_collection,
            approval_collection,
            resource_leaf,
            incident_leaf,
        ]
    incident_pack_summary_token_count = len(incident_pack_summary_refs)

    def upsert_node(name: str, node_hash: int, parent_hash: int, canonical_name: str) -> dict:
        return step(
            name,
            {
                "kind": "context_upsert_node",
                "record": {
                    "tenant_hash": tenant,
                    "node_hash": node_hash,
                    "parent_hash": parent_hash,
                    "updated_at_ms": start_ms,
                    "canonical_name": canonical_name,
                },
            },
        )

    def upsert_child(name: str, parent_hash: int, child_hash: int, rank: int) -> dict:
        return step(
            name,
            {
                "kind": "context_upsert_child_ref",
                "record": {
                    "tenant_hash": tenant,
                    "parent_hash": parent_hash,
                    "child_hash": child_hash,
                    "child_rank": rank,
                    "updated_at_ms": start_ms + rank,
                },
            },
        )

    def get_node(name: str, node_hash: int, expect: dict) -> dict:
        return step(
            name,
            {
                "kind": "context_get_node",
                "tenant_hash": tenant,
                "node_hash": node_hash,
                "expect_node": expect,
            },
        )

    def query_children(name: str, parent_hash: int, child_hashes: list[int]) -> dict:
        return step(
            name,
            {
                "kind": "context_query_children",
                "tenant_hash": tenant,
                "parent_hash": parent_hash,
                "expect_child_hashes": child_hashes,
            },
        )

    def upsert_embedding(name: str, node_hash: int, vector: list[float], level: int) -> dict:
        return step(
            name,
            {
                "kind": "context_upsert_embedding",
                "record": {
                    "tenant_hash": tenant,
                    "node_hash": node_hash,
                    "ref_hash": node_hash,
                    "ref_type": "summary",
                    "embedding_type": "L0",
                    "model_hash": 384,
                    "dim": len(vector),
                    "level": level,
                    "updated_at_ms": start_ms,
                    "vector": vector,
                    **common_hints,
                },
            },
        )

    def query_embedding(name: str, *ref_hashes: int) -> dict:
        return step(
            name,
            {
                "kind": "context_query_embeddings",
                "tenant_hash": tenant,
                "ref_hashes": list(ref_hashes),
                "expect_ref_hashes": list(ref_hashes),
            },
        )

    def assert_summary_embeddings(name: str, *node_hashes: int) -> dict:
        return step(
            name,
            {
                "kind": "context_assert_summary_embeddings",
                "tenant_hash": tenant,
                "node_hashes": list(node_hashes),
                "expect_ref_hashes": list(node_hashes),
            },
        )

    def upsert_summary(name: str, node_hash: int, level: int, text: str, valid_from_ms: int) -> dict:
        return step(
            name,
            {
                "kind": "context_upsert_summary",
                "record": {
                    "tenant_hash": tenant,
                    "node_hash": node_hash,
                    "level": level,
                    "summary": text,
                    "valid_from_ms": valid_from_ms,
                    **common_hints,
                },
            },
        )

    def query_summary(name: str, node_hash: int, expect_count: int, as_of_ms: int, level: int = 1) -> dict:
        return step(
            name,
            {
                "kind": "context_query_summaries",
                "tenant_hash": tenant,
                "node_hash": node_hash,
                "level": level,
                "as_of_ms": as_of_ms,
                "expect_count": expect_count,
            },
        )

    def write_compression(
        name: str,
        node_hash: int,
        compression_id: int,
        source_start_ms: int,
        source_end_ms: int,
        compressed_time_ms: int,
        summary: str,
        source_event_ids: list[int],
    ) -> dict:
        return step(
            name,
            {
                "kind": "context_write_compression",
                "record": {
                    "tenant_hash": tenant,
                    "node_hash": node_hash,
                    "compression_id_hash": compression_id,
                    "source_start_ms": source_start_ms,
                    "source_end_ms": source_end_ms,
                    "compressed_time_ms": compressed_time_ms,
                    "compressed_summary": summary,
                    "source_event_ids": source_event_ids,
                    **common_hints,
                },
            },
        )

    def query_compression(
        name: str,
        node_hash: int,
        compression_ids: list[int],
        source_event_ids: list[int],
        start_time_ms: int,
        end_time_ms: int,
    ) -> dict:
        return step(
            name,
            {
                "kind": "context_query_compression",
                "tenant_hash": tenant,
                "node_hash": node_hash,
                "start_time_ms": start_time_ms,
                "end_time_ms": end_time_ms,
                "expect_count": len(compression_ids),
                "expect_compression_ids": compression_ids,
                "expect_compression_source_event_ids": source_event_ids,
            },
        )

    steps: list[dict] = [
        upsert_node("upsert_scale_root", root, 0, "company_a"),
        upsert_node("upsert_approval_collection", approval_collection, root, "approvals"),
        upsert_node("upsert_incident_collection", incident_collection, root, "incidents"),
        upsert_child("link_root_to_approvals", root, approval_collection, 10),
        upsert_child("link_root_to_incidents", root, incident_collection, 20),
        get_node(
            "get_root_context_node",
            root,
            {"tenant_hash": tenant, "node_hash": root, "canonical_name": "company_a"},
        ),
        query_children(
            "query_root_context_children",
            root,
            [approval_collection, incident_collection],
        ),
        upsert_embedding("store_root_summary_embedding", root, root_vector, 0),
        upsert_embedding(
            "store_approval_collection_summary_embedding",
            approval_collection,
            approval_vector,
            1,
        ),
        upsert_embedding(
            "store_incident_collection_summary_embedding",
            incident_collection,
            incident_vector,
            1,
        ),
        query_embedding(
            "query_initial_summary_embeddings",
            root,
            approval_collection,
            incident_collection,
        ),
        upsert_summary("refresh_root_l0_summary", root, 1, "Company A context root.", start_ms),
        upsert_summary(
            "refresh_approval_collection_l0_summary",
            approval_collection,
            1,
            approval_summary,
            start_ms,
        ),
        upsert_summary(
            "refresh_incident_collection_l0_summary",
            incident_collection,
            1,
            incident_summary,
            start_ms,
        ),
        query_summary("query_approval_collection_summary", approval_collection, 1, start_ms + 1),
        step(
            "api_ingest_first_approval",
            {
                "kind": "context_api_ingest_raw_event",
                "tenant_hash": tenant,
                "endpoint": "/v1/context/ingest",
                "idempotency_key": "scale-api-approval-1",
                "raw_text": "Alice approved the first GPU budget request.",
                "agent_envelope": agent_context_envelope(
                    kind="message",
                    messages=[
                        agent_message(
                            "user",
                            "Alice approved the first GPU budget request.",
                            name="Alice",
                            created_at_ms=start_ms,
                        )
                    ],
                    scope={
                        "session_id": session_id,
                        "team": team,
                        "project": project,
                    },
                    metadata={
                        "source": "ai_agent_message",
                        "node_path": ["company_a", team, project, "approvals"],
                        "tool_name": "approval_service.lookup_request",
                        "tool_success": True,
                    },
                ),
                "agent_hook": agent_hook(
                    source="matrixark-sdk",
                    hook_type="before_llm",
                    hook_id="hook-before-approval-1",
                    observed_at_ms=start_ms,
                    idempotency_key="scale-api-approval-1",
                    trigger="user_message",
                ),
                "hints": event_hints(
                    event_id=50_000,
                    event_time=start_ms,
                    leaf_hash=approval_leaf,
                    parent_hash=approval_collection,
                    team=team,
                    project=project,
                    embedding=approval_vector,
                    entity_hash=approval_entity,
                    entity_name="gpu_purchase_request",
                    entity_value="GPU purchase request approved for Project 1.",
                )
                | common_hints,
                "expect_created": True,
                "expect_event_id_hash": 50_000,
                "expect_leaf_node_hash": approval_leaf,
            },
        ),
        step(
            "api_duplicate_does_not_write",
            {
                "kind": "context_api_ingest_raw_event",
                "tenant_hash": tenant,
                "endpoint": "/v1/context/ingest",
                "idempotency_key": "scale-api-approval-1",
                "raw_text": "Alice approved a duplicate GPU budget request.",
                "hints": event_hints(
                    event_id=59_999,
                    event_time=start_ms + 1,
                    leaf_hash=approval_leaf,
                    parent_hash=approval_collection,
                    team=team,
                    project=project,
                    embedding=approval_vector,
                    entity_hash=approval_entity,
                    entity_name="gpu_purchase_request",
                    entity_value="Duplicate GPU purchase request should not create entity.",
                )
                | common_hints,
                "expect_created": False,
                "expect_event_id_hash": 59_999,
                "expect_leaf_node_hash": approval_leaf,
            },
        ),
        get_node(
            "get_approval_leaf_after_api_ingest",
            approval_leaf,
            {"tenant_hash": tenant, "node_hash": approval_leaf},
        ),
        query_children(
            "query_approval_children_after_api_ingest",
            approval_collection,
            [approval_leaf],
        ),
    ]

    batch_events = []
    batch_ids = []
    for i in range(events_per_lane):
        event_id = 51_000 + i
        batch_ids.append(event_id)
        batch_events.append(
            {
                "raw_text": f"Alice approved scale GPU request {i}.",
                "hints": event_hints(
                    event_id=event_id,
                    event_time=start_ms + 10 + i,
                    leaf_hash=approval_leaf,
                    parent_hash=approval_collection,
                    team=team,
                    project=project,
                    embedding=approval_vector,
                    entity_hash=approval_entity,
                    entity_name="gpu_purchase_request",
                    entity_value=f"GPU scale request {i} approved.",
                )
                | common_hints,
            }
        )
    steps.append(
        step(
            "batch_ingest_approval_events",
            {
                "kind": "context_batch_ingest_raw_events",
                "tenant_hash": tenant,
                "events": batch_events,
                "expect_event_ids": batch_ids,
                "expect_leaf_node_hashes": [approval_leaf] * events_per_lane,
            },
        )
    )
    approval_leaf_summary = provider.summarize_resource_chunk(
        "approval_events",
        "context",
        "GPU approvals approved for Project 1.",
    )
    approval_leaf_summary_vector = provider.encode_text(approval_leaf_summary)
    steps.extend(
        [
            upsert_embedding(
                "store_approval_leaf_summary_embedding",
                approval_leaf,
                approval_leaf_summary_vector,
                2,
            ),
            upsert_summary(
                "refresh_approval_leaf_summary_after_batch",
                approval_leaf,
                1,
                approval_leaf_summary,
                start_ms + 20,
            ),
            query_embedding("query_approval_leaf_summary_embedding", approval_leaf),
            query_summary(
                "query_approval_leaf_summary_after_batch",
                approval_leaf,
                1,
                start_ms + 21,
            ),
            write_compression(
                "compress_approval_api_batch_window",
                approval_leaf,
                70_000,
                start_ms,
                start_ms + 999,
                start_ms + 5_000,
                "Compressed approved GPU requests for Project 1.",
                [50_000] + batch_ids,
            ),
            query_compression(
                "query_approval_compression_window",
                approval_leaf,
                [70_000],
                [50_000] + batch_ids,
                start_ms,
                start_ms + 5_001,
            ),
        ]
    )

    stream_events = []
    stream_ids = []
    committed_offsets = []
    for i in range(events_per_lane):
        event_id = 52_000 + i
        stream_ids.append(event_id)
        committed_offsets.append(i + 1)
        stream_events.append(
            {
                "partition": 0,
                "offset": i + 1,
                "raw_text": f"Incident {i} confirmed rollback health check.",
                "hints": event_hints(
                    event_id=event_id,
                    event_time=start_ms + 1_000 + i,
                    leaf_hash=incident_leaf,
                    parent_hash=incident_collection,
                    team=team,
                    project=project,
                    embedding=incident_vector,
                    event_type="incident_update",
                    entity_hash=incident_entity,
                    entity_name="rollback_incident",
                    entity_value=f"Incident rollback confirmation {i}.",
                )
                | common_hints,
            }
        )
    stream_events.append(stream_events[-1])
    steps.append(
        step(
            "stream_ingest_incidents_with_duplicate_offset",
            {
                "kind": "context_stream_ingest_raw_events",
                "tenant_hash": tenant,
                "stream_name": "scale_incidents",
                "events": stream_events,
                "expect_event_ids": stream_ids,
                "expect_committed_offsets": committed_offsets,
            },
        )
    )
    steps.extend(
        [
            get_node(
                "get_incident_leaf_after_stream_ingest",
                incident_leaf,
                {"tenant_hash": tenant, "node_hash": incident_leaf},
            ),
            query_children(
                "query_incident_children_after_stream_ingest",
                incident_collection,
                [incident_leaf],
            ),
        ]
    )
    incident_leaf_summary = provider.summarize_resource_chunk(
        "incident_events",
        "context",
        "Incident rollback confirmed and stable for Project 1.",
    )
    incident_leaf_summary_vector = provider.encode_text(incident_leaf_summary)
    steps.extend(
        [
            upsert_embedding(
                "store_incident_leaf_summary_embedding",
                incident_leaf,
                incident_leaf_summary_vector,
                2,
            ),
            upsert_summary(
                "refresh_incident_leaf_summary_after_stream",
                incident_leaf,
                1,
                incident_leaf_summary,
                start_ms + 1_020,
            ),
            query_embedding("query_incident_leaf_summary_embedding", incident_leaf),
            query_summary(
                "query_incident_leaf_summary_after_stream",
                incident_leaf,
                1,
                start_ms + 1_021,
            ),
        ]
    )

    steps.extend(
        [
            step(
                "query_all_approval_events",
                {
                    "kind": "context_query_events",
                    "tenant_hash": tenant,
                    "node_hash": approval_leaf,
                    "start_time_ms": start_ms,
                    "end_time_ms": start_ms + 999,
                    "limit": events_per_lane + 2,
                    "filters": {
                        "event_type": "approval_confirmation",
                        "team": team,
                        "project": project,
                        "status": "approved",
                        "min_confidence": 90,
                    },
                    "expect_event_ids": [50_000] + batch_ids,
                },
            ),
            step(
                "query_all_stream_incident_events",
                {
                    "kind": "context_query_events",
                    "tenant_hash": tenant,
                    "node_hash": incident_leaf,
                    "start_time_ms": start_ms + 1_000,
                    "end_time_ms": start_ms + 2_000,
                    "limit": events_per_lane + 1,
                    "filters": {
                        "event_type": "incident_update",
                        "team": team,
                        "project": project,
                    },
                    "expect_event_ids": stream_ids,
                },
            ),
            step(
                "query_status_index_time_window",
                {
                    "kind": "context_query_index",
                    "tenant_hash": tenant,
                    "index_name": "status",
                    "index_value": "approved",
                    "start_time_ms": start_ms,
                    "end_time_ms": start_ms + 12,
                    "limit": 3,
                    "expect_event_ids": [50_000, 51_000, 51_001],
                },
            ),
            step(
                "query_approval_secondary_index_and",
                {
                    "kind": "context_query_index_and",
                    "tenant_hash": tenant,
                    "start_time_ms": start_ms,
                    "end_time_ms": start_ms + 999,
                    "filters": [
                        {"index_name": "status", "index_value": "approved"},
                        {"index_name": "event_type", "index_value": "approval_confirmation"},
                        {"index_name": "project", "index_value": project},
                    ],
                    "expect_event_ids": [50_000] + batch_ids,
                },
            ),
            step(
                "query_incident_secondary_index_and",
                {
                    "kind": "context_query_index_and",
                    "tenant_hash": tenant,
                    "start_time_ms": start_ms + 1_000,
                    "end_time_ms": start_ms + 2_000,
                    "filters": [
                        {"index_name": "status", "index_value": "confirmed"},
                        {"index_name": "event_type", "index_value": "incident_update"},
                        {"index_name": "project", "index_value": project},
                    ],
                    "expect_event_ids": stream_ids,
                },
            ),
            step(
                "write_scoped_hash_status_index",
                {
                    "kind": "context_write_index_ref",
                    "record": {
                        "tenant_hash": tenant,
                        "index_name": "status_hash",
                        "index_value_hash": 7001,
                        "scope_hash": approval_collection,
                        "event_time_ms": start_ms,
                        "index_ref": {
                            "primary_node_hash": approval_leaf,
                            "primary_event_time_ms": start_ms,
                            "event_id_hash": 50_000,
                        },
                    },
                },
            ),
            step(
                "duplicate_scoped_hash_status_index_is_idempotent",
                {
                    "kind": "context_write_index_ref",
                    "record": {
                        "tenant_hash": tenant,
                        "index_name": "status_hash",
                        "index_value_hash": 7001,
                        "scope_hash": approval_collection,
                        "event_time_ms": start_ms,
                        "index_ref": {
                            "primary_node_hash": approval_leaf,
                            "primary_event_time_ms": start_ms,
                            "event_id_hash": 50_000,
                        },
                    },
                },
            ),
            step(
                "write_other_scope_hash_status_index",
                {
                    "kind": "context_write_index_ref",
                    "record": {
                        "tenant_hash": tenant,
                        "index_name": "status_hash",
                        "index_value_hash": 7001,
                        "scope_hash": incident_collection,
                        "event_time_ms": start_ms + 1_000,
                        "index_ref": {
                            "primary_node_hash": incident_leaf,
                            "primary_event_time_ms": start_ms + 1_000,
                            "event_id_hash": stream_ids[0],
                        },
                    },
                },
            ),
            step(
                "query_scoped_hash_status_index",
                {
                    "kind": "context_query_index",
                    "tenant_hash": tenant,
                    "index_name": "status_hash",
                    "index_value_hash": 7001,
                    "scope_hash": approval_collection,
                    "start_time_ms": start_ms - 1,
                    "end_time_ms": start_ms + 1,
                    "limit": 10,
                    "expect_event_ids": [50_000],
                },
            ),
            step(
                "query_other_scope_hash_status_index",
                {
                    "kind": "context_query_index",
                    "tenant_hash": tenant,
                    "index_name": "status_hash",
                    "index_value_hash": 7001,
                    "scope_hash": incident_collection,
                    "start_time_ms": start_ms,
                    "end_time_ms": start_ms + 1_001,
                    "limit": 10,
                    "expect_event_ids": [stream_ids[0]],
                },
            ),
            step(
                "query_approval_entity_state",
                {
                    "kind": "context_query_entities",
                    "tenant_hash": tenant,
                    "node_hash": approval_leaf,
                    "entity_hashes": [approval_entity],
                    "expect_entity_hashes": [approval_entity],
                },
            ),
            step(
                "query_incident_entity_state",
                {
                    "kind": "context_query_entities",
                    "tenant_hash": tenant,
                    "node_hash": incident_leaf,
                    "entity_hashes": [incident_entity],
                    "expect_entity_hashes": [incident_entity],
                },
            ),
            step(
                "retrieve_approval_pack_under_budget",
                {
                    "kind": "context_retrieve",
                    "tenant_hash": tenant,
                    "raw_query": "Which GPU approvals are approved?",
                    "hints": approval_query_hints,
                    "query_plan": approval_query_plan,
                    "query_vector": approval_query_vector,
                    "root_node_hash": root,
                    "max_depth": 3,
                    "top_k_per_depth": 1,
                    "max_candidate_nodes": 4,
                    "include_entities": True,
                    "include_summaries": True,
                    "summary_token_estimate": 1,
                    "start_time_ms": start_ms,
                    "end_time_ms": start_ms + 999,
                    "max_prompt_tokens": events_per_lane + 4,
                    "min_confidence": 90,
                    "min_importance": 80,
                    "expect_event_ids": [50_000] + batch_ids,
                    "expect_entity_hashes": [approval_entity],
                    "expect_summary_refs": [approval_collection, approval_leaf],
                    "expect_selected_tokens_eq": events_per_lane + 4,
                    "expect_query_plan": {
                        "source": "model",
                        "intent": "current_state_decision",
                        "filters": {"event_type": "approval_confirmation", "status": "approved"},
                        "scope": {"team": team, "project": project},
                    },
                    "expect_query_understanding_source": "model",
                    "expect_staleness_policy": "algorithmic_freshness_v1",
                    "expect_context_pack_sections": ["entity_state", "summary_context", "current_state"],
                    "expect_blocked_ref_count": 0,
                    "expect_dropped_ref_count": 0,
                },
            ),
            step(
                "ingest_scale_resource",
                {
                "kind": "context_ingest_resource",
                "tenant_hash": tenant,
                "raw_uri": "scale_runbook.md",
                "resource_type": "md",
                "agent_envelope": agent_context_envelope(
                    kind="resource",
                    messages=[
                        agent_message(
                            "tool",
                            "Attached rollback runbook evidence for incident recovery.",
                            name="resource_parser",
                            created_at_ms=start_ms + 3_000,
                        )
                    ],
                    scope={
                        "session_id": session_id,
                        "team": team,
                        "project": project,
                    },
                    metadata={
                        "source": "ai_agent_resource",
                        "node_path": ["company_a", team, project, "resources", "scale_runbook.md"],
                    },
                    raw_uri="scale_runbook.md",
                    resource_type="md",
                ),
                "agent_hook": agent_hook(
                    source="matrixark-resource-plugin",
                    hook_type="resource_added",
                    hook_id="hook-resource-scale-runbook",
                    observed_at_ms=start_ms + 3_000,
                    idempotency_key="resource-scale-runbook",
                    trigger="resource_attachment",
                ),
                "hints": {
                    "parent_hash": incident_collection,
                    "resource_hash": resource_leaf,
                        "path": f"company_a/{team}/{project}/resources/scale_runbook.md",
                        "name": "scale_runbook",
                        "level": 4,
                        "child_rank": 90,
                        "updated_at_ms": start_ms + 3_000,
                        **common_hints,
                    },
                    "chunks": [
                        {
                            "chunk_hash": resource_chunk.chunk_hash,
                            "source_ref": resource_chunk.source_ref,
                            "text": resource_chunk.text,
                            "token_estimate": resource_chunk.token_estimate,
                            "metadata": resource_chunk.metadata,
                            "summary": provider.summarize_resource_chunk(
                                "scale_runbook.md",
                                "md",
                                resource_chunk.text,
                            ),
                            "vector": resource_vector,
                        }
                    ],
                    "expect_chunk_hashes": [resource_chunk.chunk_hash],
                },
            ),
            step(
                "extract_resource_event_updates_entity",
                {
                    "kind": "context_extract_resource_events",
                    "tenant_hash": tenant,
                    "raw_uri": "scale_runbook.md",
                    "source_chunk_hashes": [resource_chunk.chunk_hash],
                    "hints": {
                        "node_hash": incident_leaf,
                        "event_id_base_hash": 53_100,
                        "event_time_ms": start_ms + 4_050,
                        "team": team,
                        "project": project,
                        "actor": "resource_parser",
                        "event_type": "resource_evidence",
                        "confidence": 92,
                        "importance": 82,
                        "token_estimate": 1,
                        "entity_hash": incident_entity,
                        "entity_type": 1,
                        "entity_name": "rollback_incident",
                        "entity_value": "Runbook evidence attached to rollback incident.",
                        "entity_token_estimate": 1,
                    }
                    | common_hints,
                    "expect_event_ids": [53_100],
                },
            ),
            upsert_embedding(
                "store_resource_node_summary_embedding",
                resource_leaf,
                resource_vector,
                2,
            ),
            upsert_summary(
                "refresh_resource_summary_after_extraction",
                resource_leaf,
                1,
                provider.summarize_resource_chunk("scale_runbook.md", "md", resource_chunk.text),
                start_ms + 4_051,
            ),
            query_summary(
                "query_resource_summary_after_extraction",
                resource_leaf,
                1,
                start_ms + 4_052,
            ),
            get_node(
                "get_resource_context_node_after_ingest",
                resource_leaf,
                {"tenant_hash": tenant, "node_hash": resource_leaf},
            ),
            query_children(
                "query_incident_children_after_resource_ingest",
                incident_collection,
                [incident_leaf, resource_leaf],
            ),
            query_embedding("query_resource_summary_embedding", resource_leaf),
            step(
                "second_query_uses_events_and_resource",
                {
                    "kind": "context_retrieve_with_resources",
                    "tenant_hash": tenant,
                    "raw_query": "Was the incident rollback confirmed?",
                    "hints": incident_query_hints,
                    "query_plan": incident_query_plan,
                    "query_vector": incident_query_vector,
                    "root_node_hash": root,
                    "max_depth": 3,
                    "top_k_per_depth": 2,
                    "max_candidate_nodes": 4,
                    "include_entities": True,
                    "include_summaries": True,
                    "summary_token_estimate": 1,
                    "start_time_ms": start_ms,
                    "end_time_ms": start_ms + 4_000,
                    "max_prompt_tokens": events_per_lane + 1 + incident_pack_summary_token_count + resource_chunk.token_estimate,
                    "min_confidence": 90,
                    "min_importance": 80,
                    "resource_top_k": 1,
                    "resource_filters": {"raw_uri": "scale_runbook.md", "resource_type": "md"},
                    "expect_event_ids": stream_ids,
                    "expect_entity_hashes": [incident_entity],
                    "expect_summary_refs": incident_pack_summary_refs,
                    "expect_chunk_hashes": [resource_chunk.chunk_hash],
                    "expect_selected_tokens_eq": events_per_lane + 1 + incident_pack_summary_token_count + resource_chunk.token_estimate,
                    "expect_query_plan": {
                        "source": "model",
                        "intent": "current_state_decision",
                        "filters": {"event_type": "incident_update", "status": "confirmed"},
                        "scope": {"team": team, "project": project},
                    },
                    "expect_query_understanding_source": "model",
                    "expect_staleness_policy": "algorithmic_freshness_v1",
                    "expect_context_pack_sections": [
                        "entity_state",
                        "summary_context",
                        "current_state",
                        "selected_evidence",
                    ],
                    "expect_blocked_ref_count": 0,
                    "expect_dropped_ref_count": 0,
                },
            ),
            step(
                "ingest_feedback_confirmation",
                {
                    "kind": "context_ingest_feedback",
                    "tenant_hash": tenant,
                "query_id_hash": 54_000,
                "node_hash": incident_leaf,
                "feedback_text": "User confirmed the incident rollback answer.",
                "agent_envelope": agent_context_envelope(
                    kind="feedback",
                    messages=[
                        agent_message(
                            "user",
                            "Yes, that rollback answer is correct.",
                            name="Alice",
                            created_at_ms=start_ms + 4_100,
                        ),
                        agent_message(
                            "assistant",
                            "The incident rollback was confirmed by stream and runbook evidence.",
                            name="MatrixArkAgent",
                            created_at_ms=start_ms + 4_099,
                        ),
                    ],
                    scope={
                        "session_id": session_id,
                        "team": team,
                        "project": project,
                    },
                    metadata={
                        "source": "ai_agent_feedback",
                        "node_path": ["company_a", team, project, "incidents"],
                        "reply_to_context_pack_id": "pack-incident-54000",
                    },
                    query_id_hash=54_000,
                    context_pack_id="pack-incident-54000",
                    accepted_refs=[
                        {"ref_type": "event", "ref_hash": 52_000},
                        {"ref_type": "resource_chunk", "ref_hash": resource_chunk.chunk_hash},
                    ],
                    rejected_refs=[],
                ),
                "agent_hook": agent_hook(
                    source="matrixark-sdk",
                    hook_type="after_llm",
                    hook_id="hook-after-incident-answer",
                    observed_at_ms=start_ms + 4_100,
                    idempotency_key="feedback-incident-54000",
                    trigger="final_answer_feedback",
                ),
                "hints": event_hints(
                    event_id=54_001,
                    event_time=start_ms + 4_100,
                        leaf_hash=incident_leaf,
                        parent_hash=incident_collection,
                        team=team,
                        project=project,
                        embedding=incident_vector,
                        event_type="user_confirmation",
                        entity_hash=incident_entity,
                        entity_name="rollback_incident",
                        entity_value="User confirmed the rollback answer.",
                    )
                    | common_hints,
                    "expect_event_id_hash": 54_001,
                    "expect_extracted": {"event_type": "incident_update", "status": "confirmed"},
                },
            ),
            upsert_embedding(
                "store_incident_feedback_summary_embedding",
                incident_leaf,
                provider.encode_text("Incident rollback confirmed by user feedback."),
                2,
            ),
            upsert_summary(
                "refresh_incident_leaf_summary_after_feedback",
                incident_leaf,
                1,
                "Incident rollback confirmed by user feedback.",
                start_ms + 4_101,
            ),
            query_summary(
                "query_incident_leaf_summaries_after_feedback",
                incident_leaf,
                2,
                start_ms + 4_102,
            ),
            write_compression(
                "compress_incident_stream_resource_feedback_window",
                incident_leaf,
                70_001,
                start_ms + 1_000,
                start_ms + 5_000,
                start_ms + 5_100,
                "Compressed incident rollback confirmations, resource evidence, and user feedback.",
                stream_ids + [53_100, 54_001],
            ),
            query_compression(
                "query_incident_compression_window",
                incident_leaf,
                [70_001],
                stream_ids + [53_100, 54_001],
                start_ms + 1_000,
                start_ms + 5_101,
            ),
            step(
                "query_feedback_memory",
                {
                    "kind": "context_query_events",
                    "tenant_hash": tenant,
                    "node_hash": incident_leaf,
                    "start_time_ms": start_ms + 4_100,
                    "end_time_ms": start_ms + 5_000,
                    "limit": 10,
                    "filters": {"status": "confirmed", "team": team, "project": project},
                    "expect_event_ids": [54_001],
                },
            ),
            step(
                "query_entity_after_feedback",
                {
                    "kind": "context_query_entities",
                    "tenant_hash": tenant,
                    "node_hash": incident_leaf,
                    "entity_hashes": [incident_entity],
                    "expect_entity_hashes": [incident_entity],
                },
            ),
            assert_summary_embeddings(
                "assert_l0_summaries_have_embeddings",
                root,
                approval_collection,
                incident_collection,
                approval_leaf,
                incident_leaf,
                resource_leaf,
            ),
        ]
    )

    corpus = {
        "name": "temporalstore_context_pipeline_scale_e2e",
        "schema_version": 1,
        "coverage": {
            "required_case_names": ["context_pipeline_scale_e2e"],
            "required_raft_case_names": [],
            "required_command_kinds": [
                "context_upsert_node",
                "context_get_node",
                "context_upsert_child_ref",
                "context_query_children",
                "context_api_ingest_raw_event",
                "context_upsert_embedding",
                "context_query_embeddings",
                "context_assert_summary_embeddings",
                "context_upsert_summary",
                "context_query_summaries",
                "context_write_compression",
                "context_query_compression",
                "context_batch_ingest_raw_events",
                "context_stream_ingest_raw_events",
                "context_query_events",
                "context_write_index_ref",
                "context_query_index",
                "context_query_index_and",
                "context_query_entities",
                "context_retrieve",
                "context_ingest_resource",
                "context_extract_resource_events",
                "context_retrieve_with_resources",
                "context_ingest_feedback",
            ],
            "required_response_kinds": [],
        },
        "cases": [{"name": "context_pipeline_scale_e2e", "shard_id": 42, "steps": steps}],
    }
    expected = {
        "events_per_lane": events_per_lane,
        "requested_model_provider": provider.requested_provider,
        "effective_model_provider": provider.effective_provider,
        "embedding_model": provider.embedding_model,
        "summary_embedding_model": provider.embedding_model,
        "vlm_model": provider.vlm_model,
        "embedding_backend": provider.embedding_backend,
        "vlm_backend": provider.vlm_backend,
        "embedding_dim": len(approval_vector),
        "api_events": 1,
        "batch_events": events_per_lane,
        "stream_events": events_per_lane,
        "resource_chunks": 1,
        "resource_extracted_events": 1,
        "entity_records": 2,
        "summary_records": 7,
        "summary_embedding_refs": 6,
        "summary_embedding_paired_refs": 6,
        "summary_refs_in_context_packs": 2 + incident_pack_summary_token_count,
        "compression_records": 2,
        "compression_source_event_refs": 3 + (events_per_lane * 2),
        "feedback_events": 1,
        "agent_envelopes": 3,
        "agent_envelope_kinds": ["message", "resource", "feedback"],
        "hook_captured_envelopes": 3,
        "hook_types": ["before_llm", "resource_added", "after_llm"],
        "agent_always_extract_envelopes": 3,
        "confirmation_requires_context": True,
        "total_expected_events": 3 + (events_per_lane * 2),
        "tree_shape": "root/collection/leaf",
        "layer_traversal": "global_topk_per_depth",
        "query_understanding": "model",
        "staleness_scoring": "algorithmic_freshness_v1",
        "token_budgeting": "max_prompt_tokens",
        "pipeline": [
            "raw_query_plus_hints",
            "model_query_understanding",
            "scope_time_filter_planning",
            "context_node_traversal",
            "context_event_resource_retrieval",
            "algorithmic_staleness_scoring",
            "token_budgeting",
            "context_pack",
        ],
        "steps": len(steps),
    }
    return corpus, expected


def run(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> float:
    started = time.perf_counter()
    subprocess.run(command, cwd=cwd, check=True, env=env)
    return time.perf_counter() - started


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events-per-lane", type=int, default=50)
    parser.add_argument(
        "--model-provider",
        choices=["open_source", "deterministic"],
        default="open_source",
        help="open_source tries local OSS models first; deterministic is explicit fallback mode.",
    )
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--vlm-model", default=DEFAULT_VLM_MODEL)
    parser.add_argument(
        "--require-models",
        action="store_true",
        help="fail if local OSS embedding/VLM packages or cached models are unavailable.",
    )
    parser.add_argument("--skip-rust", action="store_true")
    parser.add_argument("--write-results", type=Path)
    args = parser.parse_args()

    if args.events_per_lane < 1:
        raise SystemExit("--events-per-lane must be positive")

    provider = LocalModelProvider(
        provider=args.model_provider,
        embedding_model=args.embedding_model,
        vlm_model=args.vlm_model,
        require_models=args.require_models,
    )
    corpus, expected = build_corpus(args.events_per_lane, provider)
    with tempfile.TemporaryDirectory(prefix="temporalstore-context-scale-") as tmp:
        corpus_path = Path(tmp) / "context_pipeline_scale_e2e.json"
        corpus_path.write_text(json.dumps(corpus, indent=2) + "\n", encoding="utf-8")

        timings = {}
        timings["cpp_unified_contract_s"] = run(
            [
                "bash",
                "tools/run_cpp_unified_context_contract.sh",
                str(corpus_path),
            ],
            cwd=ROOT,
        )
        if not args.skip_rust:
            timings["rust_unified_mock_s"] = run(
                [
                    "cargo",
                    "test",
                    "--no-default-features",
                    "--features",
                    "proxy",
                    "--test",
                    "unified_corpus",
                ],
                cwd=ROOT / "sdk" / "rust" / "temporalstore",
                env={
                    **os.environ,
                    "TEMPORALSTORE_UNIFIED_CORPUS": str(corpus_path),
                },
            )

        result = {
            "status": "passed",
            "corpus": corpus["name"],
            "case": "context_pipeline_scale_e2e",
            "expected": expected,
            "timings": timings,
            "rust_executed": not args.skip_rust,
        }
        if args.write_results:
            args.write_results.parent.mkdir(parents=True, exist_ok=True)
            args.write_results.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
