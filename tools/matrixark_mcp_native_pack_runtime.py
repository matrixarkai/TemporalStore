#!/usr/bin/env python3
"""Native ContextPack runtime for TemporalStore direct adapters."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        canonical_scope_key,
        compact_context_pack_for_serving,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_model_name,
        json,
        now_ms,
        os,
        stable_hash,
        _mcp_debug_log,
    )
    from tools.matrixark_mcp_native_helpers import (
        compact_native_selected_refs,
        float_metric_or_default,
    )
    from tools.matrixark_mcp_native_pack import build_native_context_pack_request
    from tools.matrixark_mcp_retrieval import native_retrieve_fallback_allowed
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        canonical_scope_key,
        compact_context_pack_for_serving,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_model_name,
        json,
        now_ms,
        os,
        stable_hash,
        _mcp_debug_log,
    )
    from matrixark_mcp_native_helpers import (
        compact_native_selected_refs,
        float_metric_or_default,
    )
    from matrixark_mcp_native_pack import build_native_context_pack_request
    from matrixark_mcp_retrieval import native_retrieve_fallback_allowed


def try_native_context_pack(target: Any, args: Json) -> Json | None:
    self = target
    if os.environ.get("MATRIXARK_DISABLE_NATIVE_CONTEXT_PACK", "").strip().lower() in {"1", "true", "yes"}:
        return None
    if not self.supports_native_context_pack():
        return None
    native_pack_request = build_native_context_pack_request(self, args)
    request = native_pack_request["request"]
    cache_key = str(native_pack_request["cache_key"])
    scope = native_pack_request["scope"]
    query = str(native_pack_request["query"])
    debug_context_pack = bool(native_pack_request["debug_context_pack"])
    cached = self._direct_context_pack_response_cache_get(cache_key)
    if cached is not None:
        return cached
    started_perf = time.perf_counter()
    try:
        response = self.native_context_pack(request)
        if response is None:
            if not native_retrieve_fallback_allowed(args):
                result = self._native_context_pack_fallback_blocker(args, reason="native_context_pack_unavailable")
                self._direct_context_pack_response_cache_put(cache_key, result)
                return result
            return None
    except Exception as exc:
        _mcp_debug_log(f"matrixark native context pack failed: {exc}")
        if not native_retrieve_fallback_allowed(args):
            result = self._native_context_pack_fallback_blocker(args, reason=f"native_context_pack_error:{exc}")
            self._direct_context_pack_response_cache_put(cache_key, result)
            return result
        return None
    try:
        pack = json.loads(response) if isinstance(response, str) else response
    except Exception as exc:
        _mcp_debug_log(f"matrixark native context pack returned invalid JSON: {exc}")
        if not native_retrieve_fallback_allowed(args):
            result = self._native_context_pack_fallback_blocker(args, reason=f"native_context_pack_invalid_json:{exc}")
            self._direct_context_pack_response_cache_put(cache_key, result)
            return result
        return None
    if not isinstance(pack, dict):
        if not native_retrieve_fallback_allowed(args):
            result = self._native_context_pack_fallback_blocker(args, reason="native_context_pack_not_object")
            self._direct_context_pack_response_cache_put(cache_key, result)
            return result
        return None
    native_envelope = dict(pack)
    if isinstance(pack.get("context_pack"), dict):
        inner_pack = dict(pack["context_pack"])
        if isinstance(native_envelope.get("scan_stats"), dict):
            recall_policy = inner_pack.get("recall_policy") if isinstance(inner_pack.get("recall_policy"), dict) else {}
            recall_policy.setdefault("scan_stats", native_envelope["scan_stats"])
            inner_pack["recall_policy"] = recall_policy
        if isinstance(native_envelope.get("retrieval_metrics"), dict) and not isinstance(inner_pack.get("retrieval_metrics"), dict):
            inner_pack["retrieval_metrics"] = native_envelope["retrieval_metrics"]
        if native_envelope.get("selected_ref_count") is not None:
            inner_pack.setdefault("selected_ref_count", native_envelope.get("selected_ref_count"))
        if native_envelope.get("dropped_ref_count") is not None:
            inner_pack.setdefault("dropped_ref_count", native_envelope.get("dropped_ref_count"))
        pack = inner_pack
    selected_refs = pack.get("selected_refs", [])
    groups = pack.get("groups", [])
    if not isinstance(selected_refs, list) and not isinstance(groups, (list, dict)):
        if not native_retrieve_fallback_allowed(args):
            return self._native_context_pack_fallback_blocker(args, reason="native_context_pack_missing_refs_or_groups")
        return None
    compact_dropped_refs = 0
    if isinstance(selected_refs, list) and selected_refs:
        compact_refs, compact_dropped_refs = compact_native_selected_refs(selected_refs)
        if compact_refs and (compact_dropped_refs or len(compact_refs) != len(selected_refs)):
            pack["selected_refs"] = compact_refs
            pack["remote_context_refs"] = compact_refs
            selected_refs = compact_refs
        compact_token_total = 0
        for ref in selected_refs:
            if not isinstance(ref, dict):
                continue
            try:
                compact_token_total += int(ref.get("token_estimate") or 0)
            except (TypeError, ValueError):
                compact_token_total += max(1, (len(str(ref.get("text") or "")) + 3) // 4)
        if compact_token_total > 0:
            pack["used_context_tokens"] = compact_token_total
            pack["used_remote_context_tokens"] = compact_token_total
    raw_candidate_tables = (
        pack.get("candidate_records")
        or pack.get("raw_candidate_records")
        or pack.get("candidate_tables")
        or pack.get("raw_candidate_tables")
    )
    if raw_candidate_tables:
        _mcp_debug_log("matrixark native context pack returned raw candidate tables")
        if not native_retrieve_fallback_allowed(args):
            blocker = self._native_context_pack_fallback_blocker(args, reason="native_context_pack_returned_raw_candidate_tables")
            blocker["retrieval_metrics"]["raw_candidate_tables_returned"] = True
            return blocker
        return None
    pack.setdefault("context_pack_id", str(stable_hash(f"native:{query}:{canonical_scope_key(scope)}:{now_ms()}")))
    pack["context_pack_assembly"] = "native_cpp_direct"
    pack.setdefault("native_context_pack", True)
    pack.setdefault("query_embedding_model", embedding_model_name())
    pack.setdefault("embedding_execution_mode", embedding_execution_mode_name())
    pack.setdefault("embedding_fallback_used", embedding_fallback_used())
    if bool(args.get("include_retrieval_metrics")):
        pack["include_retrieval_metrics"] = True
    if selected_refs and "remote_context_refs" not in pack:
        pack["remote_context_refs"] = selected_refs
    if "recall_policy" not in pack:
        pack["recall_policy"] = {}
    if isinstance(pack["recall_policy"], dict):
        native_telemetry = pack.get("retrieval_metrics") if isinstance(pack.get("retrieval_metrics"), dict) else {}
        scan_stats = pack["recall_policy"].get("scan_stats") if isinstance(pack["recall_policy"].get("scan_stats"), dict) else {}
        if scan_stats:
            merged_native_telemetry = dict(scan_stats)
            merged_native_telemetry.update(native_telemetry)
            native_telemetry = merged_native_telemetry
        native_stage_metrics = native_telemetry.get("stages") if isinstance(native_telemetry.get("stages"), dict) else {}
        total_native_ms = round((time.perf_counter() - started_perf) * 1000.0, 3)
        selected_count = len(selected_refs) if isinstance(selected_refs, list) else 0
        pack_ms = float(native_telemetry.get("pack_ms") or native_stage_metrics.get("pack_ms") or 0.0)
        index_postings_read = int(
            native_telemetry.get("index_postings_read")
            or native_telemetry.get("index_postings_touched")
            or native_telemetry.get("native_index_postings_found")
            or 0
        )
        candidate_cache_hit = bool(
            native_telemetry.get("candidate_cache_hit", native_telemetry.get("cache_hit", False))
        )
        native_fallback_flags = native_telemetry.get("fallback_flags")
        if isinstance(native_fallback_flags, str):
            fallback_flags = [native_fallback_flags]
        elif isinstance(native_fallback_flags, list):
            fallback_flags = [str(flag) for flag in native_fallback_flags if str(flag)]
        else:
            fallback_flags = []
        retrieval_metrics = {
            "query_plan_ms": round(float(native_telemetry.get("query_plan_ms") or native_stage_metrics.get("query_plan_ms") or 0.0), 3),
            "node_traversal_ms": round(float(native_telemetry.get("node_traversal_ms") or native_stage_metrics.get("node_traversal_ms") or 0.0), 3),
            "index_prefilter_ms": round(float(native_telemetry.get("index_prefilter_ms") or native_stage_metrics.get("index_prefilter_ms") or 0.0), 3),
            "candidate_fetch_ms": round(float(native_telemetry.get("candidate_fetch_ms") or native_stage_metrics.get("candidate_fetch_ms") or 0.0), 3),
            "score_ms": round(float(native_telemetry.get("score_ms") or native_stage_metrics.get("score_ms") or 0.0), 3),
            "pack_ms": round(pack_ms, 3),
            "audit_ms": round(float(native_telemetry.get("audit_ms") or native_stage_metrics.get("audit_ms") or 0.0), 3),
            "append_queue_wait_ms": round(float_metric_or_default(native_telemetry, "append_queue_wait_ms", self._append_queue_wait_ms_avg()), 3),
            "append_engine_ms": round(float_metric_or_default(native_telemetry, "append_engine_ms", self._append_engine_ms_avg()), 3),
            "selected_refs": selected_count,
            "dropped_refs": int(native_telemetry.get("dropped_refs") or native_telemetry.get("dropped_ref_count") or 0) + compact_dropped_refs,
            "scanned_records": int(native_telemetry.get("scanned_records") or 0),
            "candidate_cache_hit": candidate_cache_hit,
            "cache_hit": candidate_cache_hit,
            "placement_partitions_touched": int(native_telemetry.get("placement_partitions_touched") or 0),
            "placement_fetch_count": int(native_telemetry.get("placement_fetch_count") or 0),
            "index_postings_read": index_postings_read,
            "index_postings_touched": index_postings_read,
            "compact_index_bucket_used": bool(native_telemetry.get("compact_index_bucket_used", False)),
            "compact_index_bucket_count": int(native_telemetry.get("compact_index_bucket_count") or 0),
            "candidate_cache_key_shape": str(native_telemetry.get("candidate_cache_key_shape") or "scope_key+node_hash+record_type+append_watermark+resource_version_watermark"),
            "native_pack_assembly": True,
            "python_pack_fallback": False,
            "raw_candidate_tables_returned": False,
            "broad_scan_used": bool(native_telemetry.get("broad_scan_used", False)),
            "broad_scan_blocked": bool(native_telemetry.get("broad_scan_blocked", False)),
            "broad_scan_fallback_allowed": bool(native_telemetry.get("broad_scan_fallback_allowed", False)),
            "timeout_count": int(native_telemetry.get("timeout_count") or 0),
            "fallback_flags": fallback_flags,
            "broad_scan_policy": "explicit_fallback_or_debug_only",
            "fallback_reason": str(native_telemetry.get("fallback_reason") or ""),
            "normal_path_stages": list(request["normal_path_stages"]),
            "health_readiness_metrics": {
                "health": True,
                "readiness": True,
                "metrics": True,
            },
            "native_context_pack_ms": total_native_ms,
            "source": "native_context_pack",
        }
        native_candidate_class_counts = native_telemetry.get("candidate_class_counts")
        if isinstance(native_candidate_class_counts, dict):
            retrieval_metrics["candidate_class_counts"] = native_candidate_class_counts
        native_correctness = (
            native_telemetry.get("correctness_evidence")
            if isinstance(native_telemetry.get("correctness_evidence"), dict)
            else {}
        )
        if native_correctness:
            retrieval_metrics["correctness_evidence"] = {
                "scope_filtering": bool(native_correctness.get("scope_filtering")),
                "placement_filtering": bool(native_correctness.get("placement_filtering")),
                "compact_secondary_index_prefilter": bool(
                    native_correctness.get("compact_secondary_index_prefilter")
                ),
                "stale_superseded_exclusion": bool(
                    native_correctness.get("stale_superseded_exclusion")
                ),
                "shared_resource_skill_quota": bool(
                    native_correctness.get("shared_resource_skill_quota")
                ),
                "cross_session_quota_rerank": bool(
                    native_correctness.get("cross_session_quota_rerank")
                ),
            }
        native_drop_counters = native_telemetry.get("drop_counters") if isinstance(native_telemetry.get("drop_counters"), dict) else {}
        if not native_drop_counters:
            native_drop_counters = pack.get("drop_counters") if isinstance(pack.get("drop_counters"), dict) else {}
        if not native_drop_counters and isinstance(pack.get("dropped_refs"), dict):
            dropped = pack.get("dropped_refs", {})
            native_drop_counters = {
                "scope": int(dropped.get("scope", 0) or dropped.get("access_denied", 0) or 0),
                "placement": int(dropped.get("placement", 0) or dropped.get("placement_filter", 0) or 0),
                "index_filter": int(dropped.get("index_filter", 0) or dropped.get("secondary_index_filter", 0) or 0),
                "stale": int(dropped.get("stale", 0) or dropped.get("superseded", 0) or 0),
                "token_budget": int(dropped.get("over_budget", 0) or dropped.get("max_selected_refs", 0) or 0),
                "score_threshold": int(dropped.get("low_score", 0) or dropped.get("score_threshold", 0) or 0),
            }
        if compact_dropped_refs:
            native_drop_counters = dict(native_drop_counters or {})
            native_drop_counters["token_budget"] = int(native_drop_counters.get("token_budget") or 0) + compact_dropped_refs
        if native_drop_counters:
            retrieval_metrics["drop_counters"] = native_drop_counters
            if not int(retrieval_metrics.get("dropped_refs") or 0):
                dropped_total = 0
                for value in native_drop_counters.values():
                    try:
                        dropped_total += int(value or 0)
                    except (TypeError, ValueError):
                        continue
                retrieval_metrics["dropped_refs"] = dropped_total
        pack["retrieval_metrics"] = retrieval_metrics
        pack["recall_policy"].setdefault(
            "backend_retrieval_pushdown",
            {
                "backend": self._backend_label(),
                "execution_mode": "native_context_pack",
                "native_pack_assembly": True,
                "watermark_count": request["watermark_count"],
                "python_materialized_records": 0,
            },
        )
        pack["recall_policy"].setdefault(
            "stage_latency_budgets",
            {
                "native_context_pack_ms": total_native_ms,
                "metrics": retrieval_metrics,
            },
        )
    dropped_refs = pack.get("dropped_refs")
    if isinstance(dropped_refs, list):
        pack["dropped_refs"] = {"refs": dropped_refs, "native_summary": True}
    elif not isinstance(dropped_refs, dict):
        pack["dropped_refs"] = {"refs": [], "native_summary": True}
    if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
        self._direct_context_pack_response_cache_put(cache_key, pack)
        return pack
    if isinstance(selected_refs, list) and selected_refs:
        result = compact_context_pack_for_serving(pack)
        self._direct_context_pack_response_cache_put(cache_key, result)
        return result
    self._direct_context_pack_response_cache_put(cache_key, pack)
    return pack

def native_context_pack(target: Any, request: Json) -> Json | None:
    retriever = getattr(getattr(target, "_client", None), "matrixark_retrieve_context_pack", None)
    if not callable(retriever):
        return None
    try:
        response = retriever(
            count_key=target._count_key,
            record_hash_key=target._record_hash_key,
            shard_size=target._shard_size,
            request=request,
        )
    except Exception as exc:
        if target.native_context_pack_required():
            raise MatrixArkError(
                f"backend-native ContextPack assembly failed for {target._backend_label()}: {exc}. "
                "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
            ) from exc
        return None
    if not isinstance(response, dict) or not response.get("native_pack_assembly"):
        if target.native_context_pack_required():
            raise MatrixArkError(
                f"backend-native ContextPack assembly returned an invalid response for {target._backend_label()}. "
                "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
            )
        return None
    if isinstance(response.get("records"), list):
        raise MatrixArkError(
            "native matrixark_retrieve_context_pack must return a finished ContextPack, not raw records"
        )
    pack = response.get("context_pack")
    if not isinstance(pack, dict):
        return None
    pack.setdefault("context_pack_assembly", "native_backend")
    pack.setdefault("backend", target._backend_label())
    recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
    contract = recall_policy.get("native_response_contract") if isinstance(recall_policy.get("native_response_contract"), dict) else {}
    contract.setdefault("raw_records_returned_to_python", False)
    contract.setdefault("python_hot_path_records", 0)
    contract.setdefault("python_role", "dispatch_request_receive_context_pack")
    contract.setdefault("backend_role", "scan_filter_score_pack")
    recall_policy["native_response_contract"] = contract
    pack["recall_policy"] = recall_policy
    return pack


def native_context_pack_fallback_blocker(target: Any, args: Json, *, reason: str) -> Json:
    scope = args.get("scope") if isinstance(args.get("scope"), dict) else {}
    query = str(args.get("query") or "")
    context_pack_id = str(stable_hash(f"native-blocked:{query}:{canonical_scope_key(scope)}:{now_ms()}"))
    pack: Json = {
        "context_pack_id": context_pack_id,
        "status": "timeout_partial",
        "native_context_pack": False,
        "context_pack_assembly": "native_context_pack_blocked",
        "query_embedding_model": embedding_model_name(),
        "embedding_execution_mode": embedding_execution_mode_name(),
        "embedding_fallback_used": embedding_fallback_used(),
        "remote_context_refs": [],
        "groups": [],
        "quality_warnings": [
            {
                "code": "native_backend_contract_blocked",
                "message": "Native matrixark_retrieve_context_pack was available but did not return a valid compact ContextPack; Python broad scan and hot-path pack fallback are disabled for production retrieval.",
                "reason": reason,
            }
        ],
        "retrieval_metrics": {
            "backend": target._backend_label(),
            "native_api": "matrixark_retrieve_context_pack",
            "native_pack_assembly": False,
            "python_pack_fallback": False,
            "raw_candidate_tables_returned": False,
            "broad_scan_used": False,
            "broad_scan_blocked": True,
            "broad_scan_policy": "explicit_fallback_or_debug_only",
            "fallback_reason": reason,
            "selected_refs": 0,
            "dropped_refs": 0,
            "scanned_records": 0,
            "index_postings_read": 0,
            "placement_partitions_touched": 0,
            "candidate_cache_hit": False,
            "normal_path_stages": [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack",
            ],
            "health_readiness_metrics": {
                "health": True,
                "readiness": True,
                "metrics": True,
            },
        },
        "recall_policy": {
            "backend_retrieval_pushdown": {
                "backend": target._backend_label(),
                "execution_mode": "native_context_pack_blocked",
                "python_materialized_records": 0,
                "broad_scan_blocked": True,
                "fallback_reason": reason,
            }
        },
    }
    if bool(args.get("include_retrieval_metrics")):
        pack["include_retrieval_metrics"] = True
    return pack
