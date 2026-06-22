#!/usr/bin/env python3
"""Validate the shared TemporalStore C++/Rust corpus against the C++ checkout.

This is the C++ side hook called by the Rust repo's unified runner. The corpus
JSON remains owned by the Rust repo unless --corpus points at a local copy.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = ROOT / "sdk" / "unified" / "temporalstore_unified_corpus.json"


def require_string(command: dict, name: str, case_name: str) -> None:
    if not isinstance(command.get(name), str) or not command[name]:
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a non-empty string")


def require_string_value(command: dict, name: str, case_name: str) -> None:
    if not isinstance(command.get(name), str):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a string")


def require_int(command: dict, name: str, case_name: str) -> None:
    if not isinstance(command.get(name), int):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be an integer")


def require_optional_int(command: dict, name: str, case_name: str) -> None:
    if name in command and command[name] is not None and not isinstance(command[name], int):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be an integer or null")


def require_bool(command: dict, name: str, case_name: str) -> None:
    if not isinstance(command.get(name), bool):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a boolean")


def require_optional_bool(command: dict, name: str, case_name: str) -> None:
    if name in command and not isinstance(command[name], bool):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a boolean")


def require_int_list(command: dict, name: str, case_name: str) -> None:
    values = command.get(name)
    if not isinstance(values, list) or not all(isinstance(value, int) for value in values):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be an integer list")


def require_bytes(command: dict, name: str, case_name: str) -> None:
    values = command.get(name)
    if not isinstance(values, list) or not all(isinstance(value, int) and 0 <= value <= 255 for value in values):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a byte list")


def require_number_list(command: dict, name: str, case_name: str) -> None:
    values = command.get(name)
    if not isinstance(values, list) or not values or not all(isinstance(value, (int, float)) for value in values):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a non-empty number list")


def require_string_list(command: dict, name: str, case_name: str) -> None:
    values = command.get(name)
    if not isinstance(values, list) or not values or not all(isinstance(value, str) and value for value in values):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a non-empty string list")


def require_string_list_value(command: dict, name: str, case_name: str) -> None:
    values = command.get(name)
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a string list")


def require_feature_points(command: dict, name: str, case_name: str) -> None:
    points = command.get(name)
    if not isinstance(points, list) or not points:
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a non-empty point list")
    for point in points:
        if not isinstance(point, dict):
            raise SystemExit(f"{case_name}: {command.get('kind')}: point must be an object")
        require_int(point, "timestamp_ms", case_name)
        require_bytes(point, "value", case_name)


def require_hash_entries(command: dict, name: str, case_name: str) -> None:
    entries = command.get(name)
    if not isinstance(entries, list) or not entries:
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be a non-empty entry list")
    for entry in entries:
        if (
            not isinstance(entry, list)
            or len(entry) != 2
            or not isinstance(entry[0], str)
            or not isinstance(entry[1], list)
            or not all(isinstance(value, int) and 0 <= value <= 255 for value in entry[1])
        ):
            raise SystemExit(f"{case_name}: {command.get('kind')}: {name} entries must be [string, bytes]")


def validate_existing_test(command: dict, case_name: str) -> None:
    require_string(command, "suite", case_name)
    require_string(command, "mode", case_name)
    require_string_list(command, "required_paths", case_name)
    mode = command["mode"]
    if mode not in {"static", "runtime", "stress"}:
        raise SystemExit(f"{case_name}: existing_test mode must be static, runtime, or stress")
    if "runner" in command:
        require_string(command, "runner", case_name)
    if "timeout_s" in command:
        require_int(command, "timeout_s", case_name)
    env = command.get("env", {})
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
        raise SystemExit(f"{case_name}: existing_test env must be a string map")


def validate_filters(command: dict, case_name: str) -> None:
    filters = command.get("filters", [])
    if not isinstance(filters, list):
        raise SystemExit(f"{case_name}: filters must be a list")
    allowed_ops = {"equal", "not_equal", "greater_than", "greater_or_equal", "less_than", "less_or_equal"}
    for item in filters:
        if not isinstance(item, dict):
            raise SystemExit(f"{case_name}: filter must be an object")
        require_string(item, "field", case_name)
        if item.get("op") not in allowed_ops:
            raise SystemExit(f"{case_name}: filter op must be one of {sorted(allowed_ops)}")
        if not isinstance(item.get("value"), int):
            raise SystemExit(f"{case_name}: filter value must be an integer")


def validate_sequence_query_spec(query: dict, case_name: str) -> None:
    if not isinstance(query, dict):
        raise SystemExit(f"{case_name}: sequence query spec must be an object")
    require_string(query, "key", case_name)
    require_int(query, "start_ms", case_name)
    require_int(query, "end_ms", case_name)
    require_int(query, "count", case_name)
    validate_filters(query, case_name)


def validate_rows(command: dict, case_name: str) -> None:
    rows = command.get("rows")
    if not isinstance(rows, list) or not rows:
        raise SystemExit(f"{case_name}: add_sequence rows must be a non-empty list")
    required = ["timestamp", "gid", "action_type", "duration", "author_id"]
    for row in rows:
        if not isinstance(row, dict):
            raise SystemExit(f"{case_name}: add_sequence row must be an object")
        for field in required:
            if not isinstance(row.get(field), int):
                raise SystemExit(f"{case_name}: add_sequence row {field} must be an integer")


def validate_context_object(command: dict, name: str, case_name: str) -> None:
    if not isinstance(command.get(name), dict):
        raise SystemExit(f"{case_name}: {command.get('kind')}: {name} must be an object")


def validate_agent_part(part: dict, case_name: str, command_kind: str) -> None:
    if not isinstance(part, dict):
        raise SystemExit(f"{case_name}: {command_kind}: agent_envelope parts must be objects")
    part_type = part.get("type")
    if part_type == "text":
        if not isinstance(part.get("text"), str) or not part["text"]:
            raise SystemExit(f"{case_name}: {command_kind}: text part needs non-empty text")
    elif part_type == "context_ref":
        require_string(part, "uri", case_name)
        if "ref_hash" in part:
            require_int(part, "ref_hash", case_name)
        if "abstract" in part and not isinstance(part["abstract"], str):
            raise SystemExit(f"{case_name}: {command_kind}: context_ref abstract must be a string")
    elif part_type == "tool":
        require_string(part, "name", case_name)
        require_bool(part, "success", case_name)
        if "input" in part and not isinstance(part["input"], dict):
            raise SystemExit(f"{case_name}: {command_kind}: tool input must be an object")
        if "output" in part and not isinstance(part["output"], dict):
            raise SystemExit(f"{case_name}: {command_kind}: tool output must be an object")
    elif part_type == "image":
        require_string(part, "uri", case_name)
    else:
        raise SystemExit(
            f"{case_name}: {command_kind}: agent_envelope part type must be "
            "text, context_ref, tool, or image"
        )


def validate_agent_message(message: dict, case_name: str, command_kind: str) -> None:
    if not isinstance(message, dict):
        raise SystemExit(f"{case_name}: {command_kind}: agent_envelope messages must be objects")
    require_string(message, "role", case_name)
    if message["role"] not in {"user", "assistant", "tool", "system"}:
        raise SystemExit(f"{case_name}: {command_kind}: agent_envelope message role is invalid")
    require_string(message, "content", case_name)
    if "name" in message and not isinstance(message["name"], str):
        raise SystemExit(f"{case_name}: {command_kind}: agent_envelope message name must be a string")
    if "created_at_ms" in message:
        require_int(message, "created_at_ms", case_name)
    if "metadata" in message and not isinstance(message["metadata"], dict):
        raise SystemExit(f"{case_name}: {command_kind}: agent_envelope message metadata must be an object")


def has_confirmation_context(envelope: dict) -> bool:
    if envelope.get("context_pack_id"):
        return True
    metadata = envelope.get("metadata")
    if isinstance(metadata, dict) and metadata.get("reply_to_context_pack_id"):
        return True
    if envelope.get("accepted_refs") or envelope.get("rejected_refs"):
        return True
    for part in envelope.get("parts", []):
        if isinstance(part, dict) and part.get("type") == "context_ref":
            return True
    return False


def validate_agent_hook(command: dict, case_name: str) -> None:
    hook = command.get("agent_hook")
    if hook is None:
        return
    command_kind = command.get("kind")
    if not isinstance(hook, dict):
        raise SystemExit(f"{case_name}: {command_kind}: agent_hook must be an object")
    require_string(hook, "source", case_name)
    require_string(hook, "hook_type", case_name)
    if hook["hook_type"] not in {
        "before_llm",
        "after_llm",
        "tool_result",
        "resource_added",
        "feedback",
        "session_commit",
    }:
        raise SystemExit(f"{case_name}: {command_kind}: agent_hook hook_type is invalid")
    require_string(hook, "hook_id", case_name)
    require_int(hook, "observed_at_ms", case_name)
    require_bool(hook, "auto_captured", case_name)
    if "idempotency_key" in hook:
        require_string(hook, "idempotency_key", case_name)
    if "trigger" in hook and not isinstance(hook["trigger"], str):
        raise SystemExit(f"{case_name}: {command_kind}: agent_hook trigger must be a string")
    if command.get("agent_envelope") is None:
        raise SystemExit(f"{case_name}: {command_kind}: agent_hook requires agent_envelope")


def validate_agent_envelope(command: dict, case_name: str) -> None:
    envelope = command.get("agent_envelope")
    if envelope is None:
        return
    command_kind = command.get("kind")
    if not isinstance(envelope, dict):
        raise SystemExit(f"{case_name}: {command_kind}: agent_envelope must be an object")
    if envelope.get("kind") not in {"message", "feedback", "resource"}:
        raise SystemExit(f"{case_name}: {command_kind}: agent_envelope kind is invalid")
    messages = envelope.get("messages")
    if messages is not None:
        if not isinstance(messages, list) or not messages:
            raise SystemExit(f"{case_name}: {command_kind}: agent_envelope messages must be non-empty")
        for message in messages:
            validate_agent_message(message, case_name, command_kind)
        if "scope" in envelope and not isinstance(envelope["scope"], dict):
            raise SystemExit(f"{case_name}: {command_kind}: agent_envelope scope must be an object")
        if "metadata" in envelope and not isinstance(envelope["metadata"], dict):
            raise SystemExit(f"{case_name}: {command_kind}: agent_envelope metadata must be an object")
    else:
        require_int(envelope, "tenant_hash", case_name)
        require_string(envelope, "session_id", case_name)
        require_string(envelope, "message_id", case_name)
        require_string(envelope, "role", case_name)
        if envelope["role"] not in {"user", "assistant", "tool", "system"}:
            raise SystemExit(f"{case_name}: {command_kind}: agent_envelope role is invalid")
        require_int(envelope, "created_at_ms", case_name)
        parts = envelope.get("parts")
        if not isinstance(parts, list) or not parts:
            raise SystemExit(f"{case_name}: {command_kind}: agent_envelope parts must be non-empty")
        for part in parts:
            validate_agent_part(part, case_name, command_kind)
        if "hints" in envelope and not isinstance(envelope["hints"], dict):
            raise SystemExit(f"{case_name}: {command_kind}: agent_envelope hints must be an object")
    if envelope["kind"] == "feedback":
        if "query_id_hash" in envelope:
            require_int(envelope, "query_id_hash", case_name)
        if "context_pack_id" in envelope:
            require_string(envelope, "context_pack_id", case_name)
        if "accepted_refs" in envelope and not isinstance(envelope["accepted_refs"], list):
            raise SystemExit(f"{case_name}: {command_kind}: accepted_refs must be a list")
        if "rejected_refs" in envelope and not isinstance(envelope["rejected_refs"], list):
            raise SystemExit(f"{case_name}: {command_kind}: rejected_refs must be a list")
        if not has_confirmation_context(envelope):
            raise SystemExit(
                f"{case_name}: {command_kind}: feedback needs context_pack_id, "
                "reply_to_context_pack_id, or accepted/rejected refs"
            )
    if envelope["kind"] == "resource":
        require_string(envelope, "raw_uri", case_name)
        require_string(envelope, "resource_type", case_name)


def validate_context_command(command: dict, case_name: str) -> bool:
    kind = command.get("kind")
    if kind in {
        "context_upsert_node",
        "context_upsert_child_ref",
        "context_write_event",
        "context_write_index_ref",
        "context_mark_summary_dirty",
        "context_upsert_summary",
        "context_write_compression",
        "context_write_pack_audit",
        "context_upsert_entity",
    }:
        validate_context_object(command, "record", case_name)
        return True
    if kind == "context_upsert_embedding":
        validate_context_object(command, "record", case_name)
        record = command["record"]
        require_number_list(record, "vector", case_name)
        return True
    if kind == "context_query_embeddings":
        require_int(command, "tenant_hash", case_name)
        require_int_list(command, "ref_hashes", case_name)
        require_int_list(command, "expect_ref_hashes", case_name)
        return True
    if kind == "context_assert_summary_embeddings":
        require_int(command, "tenant_hash", case_name)
        require_int_list(command, "node_hashes", case_name)
        require_int_list(command, "expect_ref_hashes", case_name)
        return True
    if kind == "context_get_node":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "node_hash", case_name)
        validate_context_object(command, "expect_node", case_name)
        return True
    if kind == "context_query_children":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "parent_hash", case_name)
        require_int_list(command, "expect_child_hashes", case_name)
        return True
    if kind == "context_query_events":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "node_hash", case_name)
        require_int(command, "start_time_ms", case_name)
        require_int(command, "end_time_ms", case_name)
        require_int(command, "limit", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        if "filters" in command and not isinstance(command["filters"], dict):
            raise SystemExit(f"{case_name}: context_query_events filters must be an object")
        return True
    if kind == "context_query_index":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "index_name", case_name)
        if "index_value_hash" in command:
            require_int(command, "index_value_hash", case_name)
        else:
            require_string(command, "index_value", case_name)
        if "scope_hash" in command:
            require_int(command, "scope_hash", case_name)
        if "start_time_ms" in command:
            require_int(command, "start_time_ms", case_name)
        if "end_time_ms" in command:
            require_int(command, "end_time_ms", case_name)
        if "limit" in command:
            require_int(command, "limit", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        return True
    if kind == "context_query_index_and":
        require_int(command, "tenant_hash", case_name)
        filters = command.get("filters")
        if not isinstance(filters, list) or not filters:
            raise SystemExit(f"{case_name}: context_query_index_and filters must be a non-empty list")
        for item in filters:
            if not isinstance(item, dict):
                raise SystemExit(f"{case_name}: context_query_index_and filter must be an object")
            require_string(item, "index_name", case_name)
            if "index_value_hash" in item:
                require_int(item, "index_value_hash", case_name)
            else:
                require_string(item, "index_value", case_name)
            if "scope_hash" in item:
                require_int(item, "scope_hash", case_name)
        if "scope_hash" in command:
            require_int(command, "scope_hash", case_name)
        if "start_time_ms" in command:
            require_int(command, "start_time_ms", case_name)
        if "end_time_ms" in command:
            require_int(command, "end_time_ms", case_name)
        if "limit" in command:
            require_int(command, "limit", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        return True
    if kind in {"context_query_dirty", "context_query_summaries", "context_query_compression", "context_query_pack_audit"}:
        require_int(command, "tenant_hash", case_name)
        if "node_hash" in command:
            require_int(command, "node_hash", case_name)
        if "query_id_hash" in command:
            require_int(command, "query_id_hash", case_name)
        require_int(command, "expect_count", case_name)
        if "expect_compression_ids" in command:
            require_int_list(command, "expect_compression_ids", case_name)
        return True
    if kind == "context_traverse_tree":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "root_node_hash", case_name)
        require_number_list(command, "query_vector", case_name)
        require_int(command, "max_depth", case_name)
        require_int(command, "top_k_per_depth", case_name)
        require_int(command, "max_candidate_nodes", case_name)
        require_int_list(command, "expect_node_hashes", case_name)
        return True
    if kind == "context_query_entities":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "node_hash", case_name)
        require_int_list(command, "entity_hashes", case_name)
        require_int_list(command, "expect_entity_hashes", case_name)
        return True
    if kind == "context_build_pack":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "query_id_hash", case_name)
        require_string(command, "query_text", case_name)
        require_int(command, "max_prompt_tokens", case_name)
        require_int_list(command, "candidate_node_hashes", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        if "expect_selected_tokens_lte" in command:
            require_int(command, "expect_selected_tokens_lte", case_name)
        return True
    if kind == "context_ingest_raw_event":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "raw_text", case_name)
        validate_context_object(command, "hints", case_name)
        require_int(command, "expect_event_id_hash", case_name)
        require_int(command, "expect_leaf_node_hash", case_name)
        validate_context_object(command, "expect_extracted", case_name)
        return True
    if kind == "context_api_ingest_raw_event":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "endpoint", case_name)
        require_string(command, "idempotency_key", case_name)
        require_string(command, "raw_text", case_name)
        validate_context_object(command, "hints", case_name)
        require_int(command, "expect_event_id_hash", case_name)
        require_int(command, "expect_leaf_node_hash", case_name)
        require_optional_bool(command, "expect_created", case_name)
        return True
    if kind == "context_batch_ingest_raw_events":
        require_int(command, "tenant_hash", case_name)
        events = command.get("events")
        if not isinstance(events, list) or not events:
            raise SystemExit(f"{case_name}: context_batch_ingest_raw_events events must be a non-empty list")
        for event in events:
            if not isinstance(event, dict):
                raise SystemExit(f"{case_name}: context_batch_ingest_raw_events event must be an object")
            require_string(event, "raw_text", case_name)
            validate_context_object(event, "hints", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        require_int_list(command, "expect_leaf_node_hashes", case_name)
        return True
    if kind == "context_stream_ingest_raw_events":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "stream_name", case_name)
        events = command.get("events")
        if not isinstance(events, list) or not events:
            raise SystemExit(f"{case_name}: context_stream_ingest_raw_events events must be a non-empty list")
        for event in events:
            if not isinstance(event, dict):
                raise SystemExit(f"{case_name}: context_stream_ingest_raw_events event must be an object")
            require_int(event, "partition", case_name)
            require_int(event, "offset", case_name)
            require_string(event, "raw_text", case_name)
            validate_context_object(event, "hints", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        require_int_list(command, "expect_committed_offsets", case_name)
        return True
    if kind == "context_extract_query":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "raw_query", case_name)
        if "hints" in command:
            validate_context_object(command, "hints", case_name)
        if "query_plan" in command:
            validate_context_object(command, "query_plan", case_name)
        validate_context_object(command, "expect_intent", case_name)
        if "expect_query_plan" in command:
            validate_context_object(command, "expect_query_plan", case_name)
        return True
    if kind == "context_retrieve":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "raw_query", case_name)
        if "hints" in command:
            validate_context_object(command, "hints", case_name)
        require_number_list(command, "query_vector", case_name)
        require_int(command, "root_node_hash", case_name)
        require_int(command, "max_prompt_tokens", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        if "expect_entity_hashes" in command:
            require_int_list(command, "expect_entity_hashes", case_name)
        if "expect_summary_refs" in command:
            require_int_list(command, "expect_summary_refs", case_name)
        if "include_summaries" in command:
            require_optional_bool(command, "include_summaries", case_name)
        if "summary_token_estimate" in command:
            require_int(command, "summary_token_estimate", case_name)
        if "expect_selected_tokens_eq" in command:
            require_int(command, "expect_selected_tokens_eq", case_name)
        if "expect_intent" in command:
            validate_context_object(command, "expect_intent", case_name)
        if "query_plan" in command:
            validate_context_object(command, "query_plan", case_name)
        if "expect_query_plan" in command:
            validate_context_object(command, "expect_query_plan", case_name)
        if "expect_query_understanding_source" in command:
            require_string(command, "expect_query_understanding_source", case_name)
        if "expect_staleness_policy" in command:
            require_string(command, "expect_staleness_policy", case_name)
        if "expect_context_pack_sections" in command:
            values = command["expect_context_pack_sections"]
            if not isinstance(values, list) or not values or not all(isinstance(item, str) for item in values):
                raise SystemExit(f"{case_name}: expect_context_pack_sections must be a non-empty string list")
        if "expect_blocked_ref_count" in command:
            require_int(command, "expect_blocked_ref_count", case_name)
        if "expect_dropped_ref_count" in command:
            require_int(command, "expect_dropped_ref_count", case_name)
        return True
    if kind == "context_ingest_resource":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "raw_uri", case_name)
        require_string(command, "resource_type", case_name)
        validate_context_object(command, "hints", case_name)
        values = command.get("chunks")
        if not isinstance(values, list) or not values:
            raise SystemExit(f"{case_name}: context_ingest_resource chunks must be a non-empty list")
        for chunk in values:
            if not isinstance(chunk, dict):
                raise SystemExit(f"{case_name}: context_ingest_resource chunk must be an object")
            require_int(chunk, "chunk_hash", case_name)
            require_string(chunk, "text", case_name)
            require_number_list(chunk, "vector", case_name)
        require_int_list(command, "expect_chunk_hashes", case_name)
        return True
    if kind == "context_query_resource_chunks":
        require_int(command, "tenant_hash", case_name)
        require_number_list(command, "query_vector", case_name)
        require_int(command, "top_k", case_name)
        require_int_list(command, "expect_chunk_hashes", case_name)
        if "filters" in command and not isinstance(command["filters"], dict):
            raise SystemExit(f"{case_name}: context_query_resource_chunks filters must be an object")
        return True
    if kind == "context_extract_resource_events":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "raw_uri", case_name)
        validate_context_object(command, "hints", case_name)
        require_int_list(command, "source_chunk_hashes", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        return True
    if kind == "context_ingest_feedback":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "query_id_hash", case_name)
        require_int(command, "node_hash", case_name)
        require_string(command, "feedback_text", case_name)
        validate_context_object(command, "hints", case_name)
        require_int(command, "expect_event_id_hash", case_name)
        validate_context_object(command, "expect_extracted", case_name)
        return True
    if kind == "context_retrieve_with_resources":
        require_int(command, "tenant_hash", case_name)
        require_string(command, "raw_query", case_name)
        if "hints" in command:
            validate_context_object(command, "hints", case_name)
        require_number_list(command, "query_vector", case_name)
        require_int(command, "root_node_hash", case_name)
        require_int(command, "max_prompt_tokens", case_name)
        require_int_list(command, "expect_event_ids", case_name)
        require_int_list(command, "expect_chunk_hashes", case_name)
        if "expect_entity_hashes" in command:
            require_int_list(command, "expect_entity_hashes", case_name)
        if "expect_summary_refs" in command:
            require_int_list(command, "expect_summary_refs", case_name)
        if "include_summaries" in command:
            require_optional_bool(command, "include_summaries", case_name)
        if "summary_token_estimate" in command:
            require_int(command, "summary_token_estimate", case_name)
        if "expect_selected_tokens_eq" in command:
            require_int(command, "expect_selected_tokens_eq", case_name)
        if "expect_intent" in command:
            validate_context_object(command, "expect_intent", case_name)
        if "query_plan" in command:
            validate_context_object(command, "query_plan", case_name)
        if "expect_query_plan" in command:
            validate_context_object(command, "expect_query_plan", case_name)
        if "expect_query_understanding_source" in command:
            require_string(command, "expect_query_understanding_source", case_name)
        if "expect_staleness_policy" in command:
            require_string(command, "expect_staleness_policy", case_name)
        if "expect_context_pack_sections" in command:
            values = command["expect_context_pack_sections"]
            if not isinstance(values, list) or not values or not all(isinstance(item, str) for item in values):
                raise SystemExit(f"{case_name}: expect_context_pack_sections must be a non-empty string list")
        if "expect_blocked_ref_count" in command:
            require_int(command, "expect_blocked_ref_count", case_name)
        if "expect_dropped_ref_count" in command:
            require_int(command, "expect_dropped_ref_count", case_name)
        return True
    if kind == "context_assert_parity_gates":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "expect_passed_gates", case_name)
        require_int(command, "root_node_hash", case_name)
        require_int(command, "approval_node_hash", case_name)
        require_number_list(command, "query_vector", case_name)
        require_int(command, "max_prompt_tokens", case_name)
        require_int(command, "start_time_ms", case_name)
        require_int(command, "end_time_ms", case_name)
        require_int_list(command, "expect_api_event_ids", case_name)
        require_int_list(command, "expect_stream_event_ids", case_name)
        require_int_list(command, "expect_batch_event_ids", case_name)
        require_int_list(command, "expect_absent_event_ids", case_name)
        require_int_list(command, "expect_retrieve_event_ids", case_name)
        require_int_list(command, "expect_compression_ids", case_name)
        if "expect_compression_source_event_ids" in command:
            require_int_list(command, "expect_compression_source_event_ids", case_name)
        require_int_list(command, "expect_resource_chunk_any", case_name)
        require_int(command, "expect_selected_tokens_eq", case_name)
        require_int(command, "expect_child_count_gte", case_name)
        return True
    return False


def validate_current_unified_command(command: dict, case_name: str) -> bool:
    kind = command.get("kind")
    if kind in {
        "string_get",
        "common_delete",
        "common_ttl",
        "common_exists",
        "hash_get_all",
        "hash_len",
        "set_members",
    }:
        require_string(command, "key", case_name)
        return True
    if kind == "string_set":
        require_string(command, "key", case_name)
        require_bytes(command, "value", case_name)
        return True
    if kind == "common_expire":
        require_string(command, "key", case_name)
        require_int(command, "ttl_ms", case_name)
        return True
    if kind in {"hash_set", "hash_get", "hash_delete"}:
        require_string(command, "key", case_name)
        require_string(command, "field", case_name)
        if kind == "hash_set":
            require_bytes(command, "value", case_name)
        return True
    if kind == "hash_incr_by":
        require_string(command, "key", case_name)
        require_string(command, "field", case_name)
        require_int(command, "increment", case_name)
        return True
    if kind == "hash_multi_set":
        require_string(command, "key", case_name)
        require_hash_entries(command, "entries", case_name)
        return True
    if kind == "hash_multi_get":
        require_string(command, "key", case_name)
        require_string_list(command, "fields", case_name)
        return True
    if kind == "set_add":
        require_string(command, "key", case_name)
        require_bytes(command, "member", case_name)
        return True
    if kind == "feature_append":
        require_string(command, "key", case_name)
        require_feature_points(command, "points", case_name)
        return True
    if kind == "feature_append_with_policy":
        require_string(command, "key", case_name)
        require_feature_points(command, "points", case_name)
        if command.get("policy") not in {"upsert", "insert_if_absent", "replace_existing"}:
            raise SystemExit(f"{case_name}: feature_append_with_policy policy is invalid")
        return True
    if kind in {"feature_query", "feature_query_filtered"}:
        require_string(command, "key", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        require_optional_int(command, "count", case_name)
        if kind == "feature_query_filtered":
            validate_filters(command, case_name)
        return True
    if kind == "feature_replace":
        require_string(command, "key", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        require_feature_points(command, "points", case_name)
        return True
    if kind == "feature_delete":
        require_string(command, "key", case_name)
        return True
    if kind == "feature_agg_query":
        require_string(command, "key", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        require_string(command, "aggregator", case_name)
        require_optional_int(command, "count", case_name)
        return True
    if kind == "sequence_add":
        require_string(command, "key", case_name)
        rows = command.get("rows")
        if not isinstance(rows, list) or not rows:
            raise SystemExit(f"{case_name}: sequence_add rows must be a non-empty list")
        for row in rows:
            for field in ["timestamp_ms", "gid", "action_type", "duration", "author_id"]:
                require_int(row, field, case_name)
        return True
    if kind == "sequence_query":
        require_string(command, "key", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        require_int(command, "count", case_name)
        validate_filters(command, case_name)
        return True
    if kind == "sequence_batch_query":
        queries = command.get("queries")
        if not isinstance(queries, list) or not queries:
            raise SystemExit(f"{case_name}: sequence_batch_query queries must be a non-empty list")
        for query in queries:
            validate_sequence_query_spec(query, case_name)
        return True
    if kind == "ips_load":
        require_string(command, "key", case_name)
        require_feature_points(command, "points", case_name)
        return True
    if kind in {"ips_add", "ips_add_with_options"}:
        require_string(command, "key", case_name)
        require_int(command, "timestamp_ms", case_name)
        require_bytes(command, "instance", case_name)
        if kind == "ips_add_with_options":
            require_optional_int(command, "action_type", case_name)
            require_optional_int(command, "table_id", case_name)
            if command.get("request_id") is not None:
                require_string(command, "request_id", case_name)
        return True
    if kind in {"ips_query_range", "ips_snapshot", "ips_stat", "ips_snapshot_report"}:
        require_string(command, "key", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        if kind in {"ips_query_range", "ips_snapshot", "ips_snapshot_report"}:
            require_optional_int(command, "count", case_name)
        return True
    if kind == "ips_filter":
        require_string(command, "key", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        require_optional_int(command, "count", case_name)
        require_optional_int(command, "action_type", case_name)
        require_optional_int(command, "table_id", case_name)
        return True
    if kind == "ips_batch_query_last":
        require_string_list(command, "keys", case_name)
        require_int(command, "count", case_name)
        return True
    if kind in {"risk_increment", "risk_count"}:
        require_string(command, "key", case_name)
        if kind == "risk_increment":
            require_int(command, "timestamp_ms", case_name)
            require_int(command, "amount", case_name)
        else:
            require_int(command, "start_ms", case_name)
            require_int(command, "end_ms", case_name)
        return True
    if kind == "risk_set":
        if command.get("family") not in {"h", "cpc", "fol"}:
            raise SystemExit(f"{case_name}: risk_set family must be h, cpc, or fol")
        require_string(command, "key", case_name)
        require_int(command, "timestamp_ms", case_name)
        require_int(command, "amount", case_name)
        return True
    if kind == "risk_family_query":
        if command.get("family") not in {"h", "cpc", "fol"}:
            raise SystemExit(f"{case_name}: risk_family_query family must be h, cpc, or fol")
        require_string(command, "key", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        require_string(command, "aggregator", case_name)
        return True
    if kind == "risk_set_and_get":
        if command.get("family") not in {"h", "cpc", "fol"}:
            raise SystemExit(f"{case_name}: risk_set_and_get family must be h, cpc, or fol")
        require_string(command, "key", case_name)
        require_int(command, "timestamp_ms", case_name)
        require_int(command, "amount", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        require_string(command, "aggregator", case_name)
        return True
    if kind == "risk_fol_set":
        require_string(command, "key", case_name)
        require_bytes(command, "value", case_name)
        require_int(command, "occur_time_ms", case_name)
        require_int(command, "ttl_ms", case_name)
        if command.get("fol_type") not in {"first", "last"}:
            raise SystemExit(f"{case_name}: risk_fol_set fol_type must be first or last")
        return True
    if kind in {"risk_fol_query", "risk_manager"}:
        require_string(command, "key", case_name)
        return True
    if kind == "risk_debug":
        require_string(command, "key", case_name)
        require_int(command, "start_ms", case_name)
        require_int(command, "end_ms", case_name)
        return True
    if kind in {
        "storage_dump_load_recovery",
        "storage_fault_matrix",
        "storage_follower_safe_gc",
        "storage_cache_refill",
    }:
        require_string(command, "migration_case", case_name)
        return True
    if kind == "storage_shared_store_replay":
        require_string(command, "migration_case", case_name)
        mode = command.get("mode")
        if mode not in {"Sync", "Async"}:
            raise SystemExit(f"{case_name}: storage_shared_store_replay mode must be Sync or Async")
        return True
    if kind == "context_upsert_node":
        if "record" in command:
            return False
        require_int(command, "tenant_hash", case_name)
        validate_context_object(command, "node", case_name)
        return True
    if kind == "context_get_node":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "node_hash", case_name)
        return True
    if kind == "context_write_event":
        if "record" in command:
            return False
        require_int(command, "tenant_hash", case_name)
        require_int(command, "node_hash", case_name)
        validate_context_object(command, "event", case_name)
        require_optional_bool(command, "first_write_only", case_name)
        return True
    if kind == "context_query_events":
        if "expect_event_ids" in command:
            return False
        require_int(command, "tenant_hash", case_name)
        require_int(command, "node_hash", case_name)
        require_int(command, "start_time_ms", case_name)
        require_int(command, "end_time_ms", case_name)
        require_optional_int(command, "limit", case_name)
        require_optional_bool(command, "current_valid_only", case_name)
        require_optional_int(command, "as_of_ms", case_name)
        if "types" in command:
            require_int_list(command, "types", case_name)
        return True
    if kind == "context_write_index_ref":
        if "record" in command:
            return False
        require_int(command, "tenant_hash", case_name)
        require_string(command, "index_name", case_name)
        require_int(command, "index_value_hash", case_name)
        require_optional_int(command, "scope_hash", case_name)
        require_int(command, "event_time_ms", case_name)
        validate_context_object(command, "index_ref", case_name)
        return True
    if kind == "context_query_index":
        if "expect_event_ids" in command:
            return False
        require_int(command, "tenant_hash", case_name)
        require_string(command, "index_name", case_name)
        require_int(command, "index_value_hash", case_name)
        require_optional_int(command, "scope_hash", case_name)
        require_int(command, "start_time_ms", case_name)
        require_int(command, "end_time_ms", case_name)
        require_optional_int(command, "limit", case_name)
        return True
    if kind == "context_write_pack_audit":
        if "record" in command:
            return False
        require_int(command, "tenant_hash", case_name)
        validate_context_object(command, "audit", case_name)
        return True
    if kind == "context_query_pack_audit":
        if "query_id_hash" in command:
            return False
        require_int(command, "tenant_hash", case_name)
        require_int(command, "session_hash", case_name)
        require_int(command, "start_time_ms", case_name)
        require_int(command, "end_time_ms", case_name)
        require_optional_int(command, "limit", case_name)
        return True
    if kind == "context_mark_summary_dirty":
        if "record" in command:
            return False
        require_int(command, "tenant_hash", case_name)
        validate_context_object(command, "marker", case_name)
        return True
    if kind == "context_query_summary_dirty":
        require_int(command, "tenant_hash", case_name)
        require_int(command, "node_hash", case_name)
        require_int(command, "start_time_ms", case_name)
        require_int(command, "end_time_ms", case_name)
        require_optional_int(command, "limit", case_name)
        return True
    return False


def validate_command(command: dict, case_name: str) -> None:
    validate_agent_hook(command, case_name)
    validate_agent_envelope(command, case_name)
    kind = command.get("kind")
    if validate_current_unified_command(command, case_name):
        return
    if validate_context_command(command, case_name):
        return
    if kind == "put_string":
        require_string(command, "key", case_name)
        require_string_value(command, "value", case_name)
    elif kind == "expect_string":
        require_string(command, "key", case_name)
        require_string_value(command, "value", case_name)
    elif kind == "delete_object":
        require_string(command, "key", case_name)
    elif kind == "expire":
        require_string(command, "key", case_name)
        require_int(command, "ttl_ms", case_name)
    elif kind == "expect_ttl_positive":
        require_string(command, "key", case_name)
    elif kind == "hset":
        require_string(command, "key", case_name)
        require_string(command, "field", case_name)
        require_string_value(command, "value", case_name)
    elif kind == "expect_hget":
        require_string(command, "key", case_name)
        require_string(command, "field", case_name)
        require_string_value(command, "value", case_name)
    elif kind == "hdel":
        require_string(command, "key", case_name)
        require_string(command, "field", case_name)
    elif kind == "sadd":
        require_string(command, "key", case_name)
        require_string(command, "member", case_name)
    elif kind == "expect_smembers":
        require_string(command, "key", case_name)
        require_string_list_value(command, "members", case_name)
    elif kind == "add_sequence":
        require_string(command, "key", case_name)
        validate_rows(command, case_name)
    elif kind == "query_sequence":
        require_string(command, "key", case_name)
        require_int(command, "start_ts", case_name)
        require_int(command, "end_ts", case_name)
        require_int(command, "count", case_name)
        validate_filters(command, case_name)
        if not isinstance(command.get("expect_gids"), list) or not all(
            isinstance(value, int) for value in command["expect_gids"]
        ):
            raise SystemExit(f"{case_name}: query_sequence expect_gids must be a list of integers")
    elif kind == "existing_test":
        validate_existing_test(command, case_name)
    else:
        raise SystemExit(f"{case_name}: unsupported command kind={kind!r}")


def validate_corpus(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        corpus = json.load(handle)
    if corpus.get("schema_version") != 1:
        raise SystemExit(f"{path}: unsupported schema_version={corpus.get('schema_version')!r}")
    cases = corpus.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SystemExit(f"{path}: cases must be a non-empty list")
    coverage = corpus.get("coverage")
    if coverage is not None:
        for name in [
            "required_case_names",
            "required_raft_case_names",
            "required_command_kinds",
            "required_response_kinds",
        ]:
            values = coverage.get(name)
            if not isinstance(values, list) or not all(isinstance(value, str) and value for value in values):
                raise SystemExit(f"{path}: coverage.{name} must be a string list")
    seen_case_names = set()
    seen_command_kinds = set()
    seen_response_kinds = set()
    for case in cases:
        steps = case.get("steps")
        if not case.get("name") or not isinstance(case.get("shard_id"), int):
            raise SystemExit(f"{path}: every case needs name and integer shard_id")
        if case["name"] in seen_case_names:
            raise SystemExit(f"{path}: duplicate case name {case['name']}")
        seen_case_names.add(case["name"])
        if not isinstance(steps, list) or not steps:
            raise SystemExit(f"{path}: case {case['name']} must have non-empty steps")
        seen_step_names = set()
        for step in steps:
            command = step.get("command")
            if not step.get("name") or not isinstance(command, dict) or "kind" not in command:
                raise SystemExit(f"{path}: invalid step in case {case['name']}")
            if step["name"] in seen_step_names:
                raise SystemExit(f"{path}: duplicate step name {case['name']}/{step['name']}")
            seen_step_names.add(step["name"])
            validate_command(command, case["name"])
            seen_command_kinds.add(command["kind"])
            expect = step.get("expect")
            if expect is not None:
                if not isinstance(expect, dict) or "kind" not in expect:
                    raise SystemExit(f"{path}: invalid expect in case {case['name']}/{step['name']}")
                seen_response_kinds.add(expect["kind"])
    if coverage is not None:
        missing_cases = sorted(set(coverage["required_case_names"]) - seen_case_names)
        missing_raft_cases = sorted(set(coverage.get("required_raft_case_names", [])) - seen_case_names)
        missing_commands = sorted(set(coverage["required_command_kinds"]) - seen_command_kinds)
        missing_responses = sorted(set(coverage["required_response_kinds"]) - seen_response_kinds)
        if missing_cases:
            raise SystemExit(f"{path}: missing required cases: {', '.join(missing_cases)}")
        if missing_raft_cases:
            raise SystemExit(f"{path}: missing required Raft cases: {', '.join(missing_raft_cases)}")
        if missing_commands:
            raise SystemExit(f"{path}: missing required command kinds: {', '.join(missing_commands)}")
        if missing_responses:
            raise SystemExit(f"{path}: missing required response kinds: {', '.join(missing_responses)}")
    return corpus


def iter_existing_tests(corpus_data: dict):
    for case in corpus_data["cases"]:
        for step in case["steps"]:
            command = step["command"]
            if command.get("kind") == "existing_test":
                yield case, step, command


def validate_existing_test_paths(corpus_data: dict) -> None:
    missing = []
    for case, _step, command in iter_existing_tests(corpus_data):
        for relative in command["required_paths"]:
            if not (ROOT / relative).exists():
                missing.append(f"{case['name']}: {relative}")
    if missing:
        raise SystemExit("missing unified existing-test surfaces:\n- " + "\n- ".join(missing))


def run_existing_tests(corpus_data: dict) -> None:
    for case, step, command in iter_existing_tests(corpus_data):
        runner = command.get("runner")
        if not runner:
            continue
        env = os.environ.copy()
        env.update(command.get("env", {}))
        timeout_s = command.get("timeout_s")
        label = f"{case['name']}.{step['name']}"
        print(f"+ [{label}] {runner}", flush=True)
        subprocess.run(runner, cwd=ROOT, shell=True, check=True, env=env, timeout=timeout_s)


def run_native_gate(corpus: Path, corpus_data: dict, validate_only: bool, run_existing: bool) -> None:
    if validate_only:
        return
    validate_existing_test_paths(corpus_data)
    if run_existing:
        run_existing_tests(corpus_data)
        return
    command = os.environ.get("TS_CPP_UNIFIED_NATIVE_CMD")
    if command:
        rendered = command.format(corpus=str(corpus), cpp_repo=str(ROOT))
        print(f"+ {rendered}", flush=True)
        subprocess.run(rendered, cwd=ROOT, shell=True, check=True)
        return

    context_contract = ROOT / "tools" / "run_cpp_unified_context_contract.sh"
    if context_contract.exists():
        subprocess.run(["bash", str(context_contract), str(corpus)], cwd=ROOT, check=True)
        return

    required_surfaces = [
        "src/client/temporalstore_client.cc",
        "src/server/redis_command_handler.cc",
        "src/model/ips_model.cc",
        "src/model/risk_hash_model.cc",
        "src/model/model_context.cc",
        "test/smoketest/basic_smoketest.cc",
        "test/smoketest/consistency_bench.cc",
        "src/partition/storage/test/data_raft_replication_test.cc",
        "src/blockcache/test/blockcache_smoke.cc",
        "src/blockcache/test/blockcache_test.cc",
        "tools/run_production_readiness_local_ubuntu22.sh",
    ]
    missing = [relative for relative in required_surfaces if not (ROOT / relative).exists()]
    if missing:
        raise SystemExit("missing C++ parity surfaces:\n- " + "\n- ".join(missing))
    print(
        "C++ corpus contract surfaces present; set TS_CPP_UNIFIED_NATIVE_CMD "
        "to run a full C++ corpus executor or production gate.",
        file=sys.stderr,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", default=DEFAULT_CORPUS, type=Path)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--run-existing-tests", action="store_true")
    args = parser.parse_args()

    corpus = args.corpus.resolve()
    data = validate_corpus(corpus)
    print(
        f"validated {data['name']} schema={data['schema_version']} "
        f"cases={len(data['cases'])} path={corpus}"
    )
    run_existing = args.run_existing_tests or os.environ.get("TS_CPP_UNIFIED_RUN_EXISTING") == "1"
    run_native_gate(corpus, data, args.validate_only, run_existing)
    print("TemporalStore C++ unified corpus hook passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
