"""_LocalAdapterRetrieveMixin methods split from matrixark_mcp_local_adapter.MatrixArkLocalAdapter (mixin)."""
from __future__ import annotations

import os as _os
import re as _re

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403


# --- Lexical exact-recall lane (gated by MATRIXARK_LEXICAL_EXACT_RECALL, default ON) -----------
# Dense (MiniLM) similarity under-weights rare/exact tokens -- numbers, units, counts, version
# strings, hex hashes, and capitalized proper names / acronyms. When a query names such a token,
# the record carrying it can be pruned by tree/node placement before it is ever scored, so the
# fact is never a candidate (funnel-loss class "never a candidate"). This lane admits a record as
# a candidate when it shares a rare/exact token with the query, complementing dense recall. It is
# purely additive to candidate GATHERING -- scoring, packing, and budget semantics are unchanged,
# and the admitted record still has to earn its place on score/budget like any other candidate.
_LEXICAL_EXACT_STOPWORDS = frozenset({
    "the", "and", "for", "with", "that", "this", "from", "have", "has", "had", "what", "when",
    "where", "which", "who", "whom", "whose", "how", "why", "are", "was", "were", "you", "your",
    "yours", "its", "their", "there", "then", "than", "into", "onto", "over", "under", "about",
    "does", "did", "done", "not", "but", "our", "his", "her", "him", "she", "they", "them",
})



# record_type -> (ref_type, owner hash field): how a folded owner names the embedding it
# carries, mirroring INLINE_VECTOR_OWNER_BY_REF_TYPE on the write side.
_EMBEDDING_OWNER_REFS = {
    "context_event": ("event", "event_id_hash"),
    "context_entity": ("entity", "entity_hash"),
    "context_summary": ("summary", "summary_hash"),
    "context_node": ("node", "node_hash"),
    "context_segment": ("segment", "segment_hash"),
    "context_compression_event": ("compression", "compression_id_hash"),
    "resource_chunk": ("resource_chunk", "chunk_hash"),
    "skill_section": ("skill_section", "section_hash"),
}

def lexical_exact_recall_enabled() -> bool:
    """Feature gate. Default ON; set MATRIXARK_LEXICAL_EXACT_RECALL=0 to restore exact prior behavior."""
    return str(_os.environ.get("MATRIXARK_LEXICAL_EXACT_RECALL", "1")).strip().lower() not in {
        "0", "false", "no", "off", "",
    }


def lexical_exact_recall_query_tokens(query: object) -> set:
    """Rare/exact query tokens worth a lexical recall lane: numeric/version/hash tokens always, and
    capitalized proper-name/acronym tokens when not the leading word. Common words are excluded so
    ordinary phrasings ("how many shards") yield an empty set and behavior is unchanged."""
    out: set = set()
    for index, raw_token in enumerate(_re.findall(r"[A-Za-z0-9_]+", str(query or ""))):
        low = raw_token.lower()
        if len(low) < 3 or low in _LEXICAL_EXACT_STOPWORDS:
            continue
        if any(ch.isdigit() for ch in raw_token):
            out.add(low)  # numbers, counts, versions, hex hashes (e.g. 226, 3924, 512, 9a803784)
        elif index > 0 and any(ch.isupper() for ch in raw_token):
            out.add(low)  # proper names / acronyms mid-query (e.g. Priya, Raman, LRU, MatrixCache)
    return out


def record_lexical_exact_match(record: Any, exact_tokens: set) -> bool:
    """True when the record's text carries any of the query's rare/exact tokens."""
    if not exact_tokens:
        return False
    blob = " ".join(
        str(record.get(field, "") or "")
        for field in ("text", "summary_text", "state", "entity_name", "topic")
    )
    if not blob:
        return False
    return not exact_tokens.isdisjoint(tokens(blob))

# Context-source policy resolver + remote-only local fallback floor
# (runtime_config is a leaf module -> no import cycle).
try:  # package path
    from tools.matrixark_mcp_runtime_config import (
        apply_remote_only_local_fallback,
        resolve_context_source_mode,
        QUERY_REWRITE_ENABLED,
        QUERY_REWRITE_WINDOW,
        PACK_PRECISION_EXPAND_ENABLED,
        PACK_PRECISION_EXPAND_MAX_EVENTS,
        PACK_PRECISION_EXPAND_QUESTION_TYPES,
    )  # noqa: F401
    from tools import matrixark_query_rewrite as _query_rewrite
except ImportError:
    from matrixark_mcp_runtime_config import (  # noqa: F401
        apply_remote_only_local_fallback,
        resolve_context_source_mode,
        QUERY_REWRITE_ENABLED,
        QUERY_REWRITE_WINDOW,
        PACK_PRECISION_EXPAND_ENABLED,
        PACK_PRECISION_EXPAND_MAX_EVENTS,
        PACK_PRECISION_EXPAND_QUESTION_TYPES,
    )
    import matrixark_query_rewrite as _query_rewrite


def precision_expand_pack(pack, records, question_type, *, max_events, budget_tokens) -> int:
    """Expand matched segments/summaries in the pack to their source raw events.

    For exact-fact queries a summary can drop the exact hash/number/command it summarized; the
    raw events are the exact record. This looks up each packed segment/summary's source_event_ids
    and appends those raw context_events (deduped, within budget) so precision is recovered. Adds
    tokens (raw > summary), so it is gated + scoped to exact-fact question types. Mutates pack;
    returns the number of raw events added. Never raises.
    """
    try:
        refs = pack.get("selected_refs")
        if not isinstance(refs, list) or not refs:
            return 0
        event_text = {}
        seg_sources = {}
        for r in records:
            rt = r.get("record_type")
            if rt == "context_event" and r.get("event_id_hash") is not None:
                event_text[int(r["event_id_hash"])] = str(r.get("text", ""))
            elif rt in {"context_segment", "context_summary"}:
                h = r.get("segment_hash") or r.get("summary_hash")
                if h is not None:
                    seg_sources[int(h)] = [int(x) for x in (r.get("source_event_ids") or []) if str(x).lstrip("-").isdigit()]
        already = set()
        for r in refs:
            rh = r.get("ref_hash")
            if rh is not None:
                already.add(int(rh))
        added = 0
        used = 0
        additions = []
        for r in refs:
            if r.get("ref_type") not in {"segment", "summary", "compression"}:
                continue
            rh = r.get("ref_hash")
            for eid in (seg_sources.get(int(rh), []) if rh is not None else [])[:max_events]:
                if eid in already or eid not in event_text:
                    continue
                txt = event_text[eid]
                t = max(1, len(txt) // 4)
                if used + t > budget_tokens:
                    break
                additions.append({"ref_type": "event", "ref_hash": eid, "text": txt,
                                  "tokens": t, "memory_layer": "session", "source": "precision_expand"})
                already.add(eid); used += t; added += 1
        if additions:
            pack["selected_refs"] = refs + additions
            pack["used_context_tokens"] = int(pack.get("used_context_tokens", 0) or 0) + used
            pack["precision_expanded"] = {"raw_events_added": added, "tokens_added": used}
        return added
    except Exception:
        return 0


def _recent_user_texts_for_rewrite(args, adapter, scope) -> list:
    """Recent user-turn texts for follow-up rewriting: prefer explicit args, else session buffer.

    Only called for follow-up queries (cheap early-out otherwise), so the buffer read is rare.
    """
    for key in ("prior_user_messages", "prior_messages", "recent_user_texts", "recent_turns"):
        val = args.get(key)
        if isinstance(val, list) and val:
            out = []
            for m in val:
                if isinstance(m, str):
                    out.append(m)
                elif isinstance(m, dict) and str(m.get("role", "user")) in ("user", "human"):
                    c = m.get("content") or m.get("text")
                    if isinstance(c, str):
                        out.append(c)
            if out:
                return out
    try:  # best-effort: pull recent user turns from the session buffer
        events = adapter.pending_session_events(scope) or []
        texts = []
        for rec in events[-16:]:
            for msg in (messages_from_event_record(rec) or []):
                if str(msg.get("role", "")) in ("user", "human") and isinstance(msg.get("content"), str):
                    texts.append(msg["content"])
        return texts
    except Exception:
        return []


def _maybe_rewrite_retrieval_query(query, args, adapter, scope):
    """Conditional follow-up rewrite of the RANKING query only (never the pack). Returns (rq, info)."""
    if not QUERY_REWRITE_ENABLED or not _query_rewrite.is_followup_query(query):
        return query, {"query_rewritten": False, "reason": "disabled_or_standalone"}
    priors = _recent_user_texts_for_rewrite(args, adapter, scope)
    rq, rewritten, reason = _query_rewrite.conditional_retrieval_query(
        query, priors, enabled=True, window=QUERY_REWRITE_WINDOW)
    return rq, {"query_rewritten": rewritten, "reason": reason}

try:  # names owned by the parent module
    from tools.matrixark_mcp_local_adapter import (
    Any,
    async_pipeline_retrieval_readiness,
    auto_extraction_phase_budget_tokens,
    auto_memory_layer_budget_tokens,
    auto_memory_selection_policy_budget_tokens,
    auto_source_role_budget_tokens,
    codex_outcome_budget_query,
    codex_session_identity_policy,
    compact_context_pack_for_serving,
    dropped_ref_layer_budget,
    effective_retrieval_question_type,
    memory_layer_budget_question_reason,
    memory_layer_pressure_summary,
    pre_retrieval_idle_commit_flush,
    pre_retrieval_summary_refresh_enabled,
    pre_retrieval_summary_refresh_explicitly_configured,
    pre_retrieval_summary_refresh_limit,
    pre_retrieval_summary_refresh_memory_layer_budget_tokens,
    quality_first_underfill_summary,
    refresh_final_selected_budget_policies,
    retrieval_memory_inventory,
    selected_ref_layer_budget,
    suppress_extracted_represented_pending_events,
    suppress_overlapping_profile_current_entities,
    suppress_profile_shadowed_session_entities,
)
except ImportError:
    from matrixark_mcp_local_adapter import (
    Any,
    async_pipeline_retrieval_readiness,
    auto_extraction_phase_budget_tokens,
    auto_memory_layer_budget_tokens,
    auto_memory_selection_policy_budget_tokens,
    auto_source_role_budget_tokens,
    codex_outcome_budget_query,
    codex_session_identity_policy,
    compact_context_pack_for_serving,
    dropped_ref_layer_budget,
    effective_retrieval_question_type,
    memory_layer_budget_question_reason,
    memory_layer_pressure_summary,
    pre_retrieval_idle_commit_flush,
    pre_retrieval_summary_refresh_enabled,
    pre_retrieval_summary_refresh_explicitly_configured,
    pre_retrieval_summary_refresh_limit,
    pre_retrieval_summary_refresh_memory_layer_budget_tokens,
    quality_first_underfill_summary,
    refresh_final_selected_budget_policies,
    retrieval_memory_inventory,
    selected_ref_layer_budget,
    suppress_extracted_represented_pending_events,
    suppress_overlapping_profile_current_entities,
    suppress_profile_shadowed_session_entities,
)


def _sibling_sessions_enabled(scope) -> bool:
    """Whether this tenant's retrieval may descend into other sessions (default ON).

    Lazily imported and defaulting to the knob's own value, so a deployment without the policy
    module keeps today's behaviour rather than silently narrowing every search.
    """
    try:
        from matrixark_index_growth_bound import traverse_sibling_sessions_enabled
    except Exception:  # pragma: no cover - policy module absent
        return True
    return bool(traverse_sibling_sessions_enabled(scope))


class _LocalAdapterRetrieveMixin:
    def retrieve(self, args: Json) -> Json:
        started_perf = time.perf_counter()
        query = require_string(args, "query")
        scope = optional_object(args, "scope")
        # Conditional follow-up rewrite for RANKING ONLY (does not change the pack -> zero added
        # model tokens). `query` stays the user's prompt everywhere else; `retrieval_query` ranks.
        retrieval_query, _rewrite_info = _maybe_rewrite_retrieval_query(query, args, self, scope)
        storage_options = normalize_storage_options(args)
        ranking = optional_object(args, "ranking")
        audit_mode = str(args.get("audit_mode") or os.environ.get("MATRIXARK_CONTEXT_AUDIT_MODE", "off")).strip().lower()
        if audit_mode not in {"full", "telemetry_only", "off"}:
            raise MatrixArkError("audit_mode must be full, telemetry_only, or off")
        if "audit_sample_rate" in args:
            raw_audit_sample_rate = args.get("audit_sample_rate")
        elif audit_mode == "full":
            raw_audit_sample_rate = 1.0
        else:
            raw_audit_sample_rate = os.environ.get("MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE", 0.01)
        try:
            audit_sample_rate = clamp01(float(raw_audit_sample_rate))
        except (TypeError, ValueError):
            raise MatrixArkError("audit_sample_rate must be a number between 0 and 1")
        raw_deadline_ms = args.get("deadline_ms", ranking.get("deadline_ms", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", 0)))
        try:
            deadline_ms = int(raw_deadline_ms or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("deadline_ms must be an integer")

        def deadline_exceeded() -> bool:
            return deadline_ms > 0 and (time.perf_counter() - started_perf) * 1000.0 >= deadline_ms

        stage_names = ["query_understanding", "candidate_fetch", "node_traversal", "rerank_score", "pack", "audit"]
        explicit_stage_budgets = optional_object(args, "stage_budgets_ms") or optional_object(ranking, "stage_budgets_ms")
        if deadline_ms > 0:
            default_stage_budgets = {
                "query_understanding": max(25, int(deadline_ms * 0.15)),
                "candidate_fetch": max(25, int(deadline_ms * 0.20)),
                "node_traversal": max(25, int(deadline_ms * 0.15)),
                "rerank_score": max(25, int(deadline_ms * 0.30)),
                "pack": max(25, int(deadline_ms * 0.15)),
                "audit": max(10, int(deadline_ms * 0.05)),
            }
        else:
            default_stage_budgets = {
                "query_understanding": 500,
                "candidate_fetch": 750,
                "node_traversal": 500,
                "rerank_score": 1000,
                "pack": 500,
                "audit": 250,
            }
        stage_budgets_ms: dict[str, int] = {}
        for stage in stage_names:
            value = explicit_stage_budgets.get(stage, ranking.get(f"{stage}_budget_ms", default_stage_budgets[stage]))
            if not isinstance(value, int) or value < 0:
                raise MatrixArkError(f"stage budget for {stage} must be a non-negative integer")
            stage_budgets_ms[stage] = value
        stage_latencies_ms: dict[str, float] = {}
        stage_started_perf = time.perf_counter()

        def finish_retrieval_stage(stage: str, started: float) -> float:
            elapsed = round((time.perf_counter() - started) * 1000.0, 3)
            stage_latencies_ms[stage] = elapsed
            self._observe_model_latency(f"retrieval_{stage}", elapsed)
            return elapsed

        def stage_budget_snapshot() -> Json:
            stages = {
                stage: {
                    "budget_ms": stage_budgets_ms[stage],
                    "elapsed_ms": round(float(stage_latencies_ms.get(stage, 0.0)), 3),
                    "over_budget": bool(stage_budgets_ms[stage] > 0 and float(stage_latencies_ms.get(stage, 0.0)) > stage_budgets_ms[stage]),
                }
                for stage in stage_names
            }
            return {
                "enabled": True,
                "source": "explicit" if explicit_stage_budgets else ("deadline_derived" if deadline_ms > 0 else "defaults"),
                "stages": stages,
                "over_budget_stages": [stage for stage, row in stages.items() if row["over_budget"]],
            }

        question_type = effective_retrieval_question_type(query, args.get("question_type"))
        retrieval_session_scope = str(args.get("session_scope") or ranking.get("session_scope") or "prefer").strip().lower()
        if retrieval_session_scope not in {"prefer", "only"}:
            raise MatrixArkError("session_scope must be prefer or only")
        # A tenant that declined sibling sessions gets "only" whatever the request asked for. The
        # knob is a ceiling rather than another default: it says what this deployment does, so a
        # per-request argument must not widen it. Narrowing to "only" is always still allowed.
        if not _sibling_sessions_enabled(scope):
            retrieval_session_scope = "only"
        retrieval_scope = {**scope, "_session_scope": retrieval_session_scope}
        secondary_index_filter_groups = infer_secondary_index_filter_groups(query, question_type)
        secondary_index_filter_mode = "any_group" if len(secondary_index_filter_groups) > 1 else "all_groups"
        secondary_index_dropped_count = 0
        secondary_index_matched_count = 0
        pre_retrieval_idle_commit = pre_retrieval_idle_commit_flush(
            self,
            args,
            ranking,
            scope=scope,
        )
        idle_summary_refresh = (
            pre_retrieval_idle_commit.get("summary_refresh")
            if isinstance(pre_retrieval_idle_commit.get("summary_refresh"), dict)
            else {}
        )
        idle_memory_layers = (
            pre_retrieval_idle_commit.get("memory_layers_written")
            if isinstance(pre_retrieval_idle_commit.get("memory_layers_written"), dict)
            else {}
        )
        idle_committed_event_count = int(pre_retrieval_idle_commit.get("committed_event_count") or 0)
        fresh_idle_summary_required = bool(
            idle_committed_event_count > 0
            and (
                idle_summary_refresh.get("dirty_hashes")
                or idle_summary_refresh.get("session_dirty_hashes")
                or idle_summary_refresh.get("profile_dirty_hashes")
                or idle_summary_refresh.get("profile_summary_refresh_required")
                or idle_memory_layers.get("summary_dirty_nodes")
            )
        )
        explicit_summary_refresh = pre_retrieval_summary_refresh_explicitly_configured(args, ranking)
        configured_summary_refresh_enabled = pre_retrieval_summary_refresh_enabled(args, ranking)
        auto_summary_refresh_after_idle = bool(
            fresh_idle_summary_required
            and not explicit_summary_refresh
            and question_type in {
                "current_state",
                "latest",
                "profile_memory",
                "multi_hop",
                "date",
                "broad_exploration",
                "evidence",
                "benchmark_quality",
            }
        )
        requested_summary_refresh_limit = pre_retrieval_summary_refresh_limit(args, ranking)
        if auto_summary_refresh_after_idle:
            requested_summary_refresh_limit = max(
                requested_summary_refresh_limit,
                int(idle_memory_layers.get("summary_dirty_nodes") or 0),
                8 if bool(idle_summary_refresh.get("profile_summary_refresh_required", False)) else 1,
            )
        pre_retrieval_summary_refresh: Json = {
            "enabled": bool(configured_summary_refresh_enabled or auto_summary_refresh_after_idle),
            "requested_limit": requested_summary_refresh_limit,
            "refreshed_count": 0,
            "status": "disabled",
            "source": "explicit" if explicit_summary_refresh else "fresh_idle_commit" if auto_summary_refresh_after_idle else "default",
            "fresh_idle_commit_dirty": fresh_idle_summary_required,
            "fresh_idle_commit_summary_required": fresh_idle_summary_required,
            "fresh_idle_commit_committed_event_count": idle_committed_event_count,
            "fresh_idle_commit_summary_dirty_nodes": int(idle_memory_layers.get("summary_dirty_nodes") or 0),
            "fresh_idle_commit_profile_summary_required": bool(idle_summary_refresh.get("profile_summary_refresh_required", False)),
        }
        if not pre_retrieval_summary_refresh["enabled"] and fresh_idle_summary_required:
            pre_retrieval_summary_refresh["status_reason"] = "fresh_idle_commit_dirty_summary_pending"
        pre_retrieval_refreshed_records: list[Json] = []
        if pre_retrieval_summary_refresh["enabled"]:
            refresh_started_perf = time.perf_counter()
            try:
                refresh_result = self.refresh_summaries(
                    {
                        "scope": scope,
                        "limit": int(pre_retrieval_summary_refresh["requested_limit"]),
                        "refreshed_at_ms": now_ms(),
                        **(
                            {"skip_dirty_reasons": args.get("pre_retrieval_summary_refresh_skip_dirty_reasons")}
                            if isinstance(args.get("pre_retrieval_summary_refresh_skip_dirty_reasons"), list)
                            else {}
                        ),
                    }
                )
                refreshed_count = int(refresh_result.get("refreshed_count") or 0)
                pre_retrieval_refreshed_records = [
                    record
                    for record in refresh_result.get("refreshed", [])
                    if isinstance(record, dict)
                ]
                pre_retrieval_summary_refresh.update(
                    {
                        "status": "refreshed" if refreshed_count else "no_dirty_nodes",
                        "refreshed_count": refreshed_count,
                        "compression_created_count": int(refresh_result.get("compression_created_count") or 0),
                        "skipped_dirty_count": int(refresh_result.get("skipped_dirty_count") or 0),
                        "skipped_dirty_reasons": (
                            refresh_result.get("skipped_dirty_reasons")
                            if isinstance(refresh_result.get("skipped_dirty_reasons"), dict)
                            else {}
                        ),
                        "elapsed_ms": round((time.perf_counter() - refresh_started_perf) * 1000.0, 3),
                    }
                )
            except Exception as exc:
                pre_retrieval_summary_refresh.update(
                    {
                        "status": "error",
                        "error": str(exc)[:240],
                        "elapsed_ms": round((time.perf_counter() - refresh_started_perf) * 1000.0, 3),
                    }
                )
        budget_source = "agent_provided_max_context_tokens" if "max_context_tokens" in args else "matrixark_default_max_context_tokens"
        max_context_tokens = args.get("max_context_tokens", DEFAULT_MAX_CONTEXT_TOKENS)
        if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
            raise MatrixArkError("max_context_tokens must be a positive integer")
        local_budget = local_context_budget(args)
        # Context-source policy: synthetic/debug requests (or a global flip) use the remote
        # TemporalStore pack ONLY -- reserve zero local budget so remote gets the whole window
        # and inject no local refs. Real sessions keep local + remote (unchanged). Mutating the
        # shared local_budget here means the deadline fallback packer inherits the same policy.
        context_source_mode = resolve_context_source_mode(args)
        if context_source_mode == "remote_only":
            # Set local aside (not discard): the remote-only safety floor can re-admit it
            # for this turn if the remote pack comes back too sparse (see below).
            local_budget["_remote_only_fallback_items"] = list(local_budget.get("items") or [])
            local_budget["observed_local_token_estimate"] = int(local_budget.get("token_estimate", 0))
            local_budget["token_estimate"] = 0
            local_budget["items"] = []
            local_budget["text_hashes"] = set()
        local_budget["context_source_mode"] = context_source_mode
        local_tokens = int(local_budget.get("token_estimate", 0))
        safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
        remote_context_budget_tokens = max(0, max_context_tokens - local_tokens - safety_margin_tokens)
        local_budget["remote_budget_tokens"] = remote_context_budget_tokens
        cross_session_policy = build_cross_session_policy(
            args,
            ranking,
            question_type=question_type,
            session_scope=retrieval_session_scope,
            remote_budget_tokens=remote_context_budget_tokens,
            context_source_mode=context_source_mode,
        )
        retrieval_scope["_allow_profile_bridge"] = bool(
            cross_session_policy.get("enabled")
            and int(cross_session_policy.get("min_entity_bridge_refs") or 0) > 0
        )
        shared_context_policy = build_shared_context_policy(
            args,
            ranking,
            remote_budget_tokens=remote_context_budget_tokens,
        )
        source_role_budget_tokens = optional_object(args, "source_role_budget_tokens") or optional_object(ranking, "source_role_budget_tokens")
        source_role_budget_mode = "explicit" if source_role_budget_tokens else ""
        if not source_role_budget_tokens:
            source_role_budget_tokens, source_role_budget_mode = auto_source_role_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        explicit_memory_layer_budget_tokens = optional_object(args, "memory_layer_budget_tokens") or optional_object(ranking, "memory_layer_budget_tokens")
        custom_memory_layer_budget_fractions = optional_object(args, "memory_layer_budget_fractions") or optional_object(ranking, "memory_layer_budget_fractions")
        memory_layer_budget_tokens = explicit_memory_layer_budget_tokens
        memory_layer_budget_mode = "explicit" if memory_layer_budget_tokens else ""
        if pre_retrieval_summary_refresh["enabled"] and not explicit_memory_layer_budget_tokens and not custom_memory_layer_budget_fractions:
            memory_layer_budget_tokens, memory_layer_budget_mode = pre_retrieval_summary_refresh_memory_layer_budget_tokens(
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
                outcome_query=codex_outcome_budget_query(args, ranking, question_type=question_type),
                args=args,
                ranking=ranking,
            )
        elif not memory_layer_budget_tokens:
            memory_layer_budget_tokens, memory_layer_budget_mode = auto_memory_layer_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        memory_layer_budget_reason = memory_layer_budget_question_reason(question_type)
        memory_selection_policy_budget_tokens = (
            optional_object(args, "memory_selection_policy_budget_tokens")
            or optional_object(ranking, "memory_selection_policy_budget_tokens")
        )
        memory_selection_policy_budget_mode = "explicit" if memory_selection_policy_budget_tokens else ""
        if not memory_selection_policy_budget_tokens:
            memory_selection_policy_budget_tokens, memory_selection_policy_budget_mode = auto_memory_selection_policy_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        extraction_phase_budget_tokens = (
            optional_object(args, "extraction_phase_budget_tokens")
            or optional_object(ranking, "extraction_phase_budget_tokens")
        )
        extraction_phase_budget_mode = "explicit" if extraction_phase_budget_tokens else ""
        if not extraction_phase_budget_tokens:
            extraction_phase_budget_tokens, extraction_phase_budget_mode = auto_extraction_phase_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        query_terms = {term for term in tokens(retrieval_query) if len(term) > 2}
        # Lexical exact-recall lane: rare/exact tokens the dense encoder under-weights. Empty for
        # ordinary phrasings, so this is a no-op unless the query actually names an exact token.
        lexical_exact_tokens = (
            lexical_exact_recall_query_tokens(retrieval_query)
            if lexical_exact_recall_enabled()
            else set()
        )
        raw_reference_time_ms = args.get("reference_time_ms", now_ms())
        if not isinstance(raw_reference_time_ms, int):
            raise MatrixArkError("reference_time_ms must be an integer")
        reference_time_ms = raw_reference_time_ms
        query_plan = build_structured_query_plan(
            query,
            question_type=question_type,
            secondary_index_filter_groups=secondary_index_filter_groups,
            secondary_index_filter_mode=secondary_index_filter_mode,
            reference_time_ms=reference_time_ms,
        )
        debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
        pack_cache_enabled = (
            self._context_pack_cache_max_entries > 0
            and self._context_pack_cache_ttl_s > 0
            and python_hot_cache_allowed(backend_label=str(getattr(self, "_backend_label", lambda: "local")()))
        )
        pack_cache_key = (
            self._retrieval_records_cache_generation,
            canonical_scope_key(scope),
            query,
            question_type,
            retrieval_session_scope,
            max_context_tokens,
            int(local_budget.get("token_estimate", 0)),
            tuple(sorted(local_budget.get("text_hashes", set()))),
            json.dumps(ranking, sort_keys=True, separators=(",", ":")),
            json.dumps(cross_session_policy, sort_keys=True, separators=(",", ":")),
            json.dumps(shared_context_policy, sort_keys=True, separators=(",", ":")),
            json.dumps(source_role_budget_tokens, sort_keys=True, separators=(",", ":")),
            json.dumps(memory_layer_budget_tokens, sort_keys=True, separators=(",", ":")),
            json.dumps(memory_selection_policy_budget_tokens, sort_keys=True, separators=(",", ":")),
            json.dumps(extraction_phase_budget_tokens, sort_keys=True, separators=(",", ":")),
            json.dumps(
                {
                    "enabled": bool(pre_retrieval_summary_refresh.get("enabled")),
                    "requested_limit": int(pre_retrieval_summary_refresh.get("requested_limit") or 0),
                    "status": pre_retrieval_summary_refresh.get("status"),
                    "refreshed_count": int(pre_retrieval_summary_refresh.get("refreshed_count") or 0),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            json.dumps(
                {
                    "enabled": bool(pre_retrieval_idle_commit.get("enabled")),
                    "status": pre_retrieval_idle_commit.get("status"),
                    "committed_event_count": int(pre_retrieval_idle_commit.get("committed_event_count") or 0),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
            debug_refs,
            bool(args.get("debug_context_pack") or args.get("include_retrieval_debug")),
            bool(args.get("include_retrieval_metrics")),
        )
        if pack_cache_enabled:
            with self._context_pack_cache_lock:
                cached = self._context_pack_cache.get(pack_cache_key)
                if cached is not None:
                    cached_at, cached_pack = cached
                    if time.monotonic() - cached_at <= self._context_pack_cache_ttl_s:
                        pack = json.loads(json.dumps(cached_pack))
                        pack["context_pack_cache_hit"] = True
                        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
                        recall_policy["context_pack_cache"] = {"hit": True, "ttl_s": self._context_pack_cache_ttl_s}
                        pack["recall_policy"] = recall_policy
                        if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
                            return pack
                        return compact_context_pack_for_serving(pack, include_debug=debug_refs)
                    self._context_pack_cache.pop(pack_cache_key, None)
        auxiliary_quota = integer_arg(ranking, "auxiliary_quota", 2, minimum=0)
        embedding_metadata_by_ref: dict[tuple[Any, Any], Json] = {}

        def remember_embedding_metadata(record: Json) -> None:
            # A context_embedding row copies its owner's metadata. Measured on a real ingest:
            # 29 of the 33 populated fields are IDENTICAL on the owner record, and only four are
            # unique to the row -- dim, model, model_ref (encoder provenance) and storage_options.
            #
            # This is NOT a fold waiting to happen. record_with_embedding_defaults() below fills
            # only fields the owner is MISSING, so retrieval is already owner-first and this copy
            # is never consulted in normal operation. It is a self-repair net for an owner that
            # has lost fields, which
            # test_retrieve_recovers_hot_event_type_from_embedding_metadata exercises by
            # stripping event_type/classification/status/source_kind from a context_event.
            #
            # Removing the copy was measured end to end: it saves 2.6% of total record bytes
            # (129,938 -> 126,510) because the vector dominates a row, not its metadata, and it
            # breaks the repair path. Recorded so the trade is not re-derived: losing self-repair
            # to save 2.6% is not worth it, and dropping these rows entirely costs more still --
            # eight tests covering hot_event_type recovery, cross-session profile lineage and
            # memory-layer classification all read them.
            ref_type = record.get("ref_type")
            ref_hash = record.get("ref_hash")
            if ref_type in (None, "") or ref_hash in (None, ""):
                return
            metadata_fields = [
                "memory_scope",
                "session_continuity",
                "entity_type",
                "entity_name",
                "event_type",
                "classification",
                "status",
                "source_kind",
                "source_type",
                "promoted_from_memory_scope",
                "profile_promotion_policy",
                "profile_promotion_blocker",
                "extraction_phase",
                "final_session_boundary",
                "source_roles",
                "source_role_counts",
                "source_hook_types",
                "source_hook_type_counts",
                "source_codex_events",
                "source_codex_event_counts",
                "source_memory_selection_policies",
                "source_memory_selection_policy_counts",
                "source_memory_scopes",
                "source_session_continuities",
                "source_extraction_phases",
                "source_profile_promotion_policies",
                "source_profile_promotion_blockers",
                "source_session_ids",
                "source_entity_hashes",
                "source_event_ids",
                "extraction_context_event_ids",
                "profile_source_session_count",
                "profile_source_entity_count",
                "profile_entity_current",
                "profile_revision",
                "previous_profile_revision",
                "previous_profile_updated_at_ms",
                "source_session_count",
                "source_entity_count",
                "current_state_source_session_count",
                "current_state_source_entity_count",
            ]
            metadata = {
                field: record[field]
                for field in metadata_fields
                if record.get(field) not in (None, "", [], {})
            }
            if metadata:
                embedding_metadata_by_ref[(ref_type, ref_hash)] = metadata

        def record_with_embedding_defaults(record: Json, ref_type: object, ref_hash: object) -> Json:
            embedding_metadata = embedding_metadata_by_ref.get((ref_type, ref_hash), {})
            if not embedding_metadata:
                return record
            recovered = dict(record)
            for key, value in embedding_metadata.items():
                if recovered.get(key) in (None, "", [], {}):
                    recovered[key] = value
            return recovered

        def first_explicit_bool(key: str, *sources: Json) -> bool | None:
            for source in sources:
                if not isinstance(source, dict) or key not in source:
                    continue
                value = source.get(key)
                if value in (None, ""):
                    continue
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return bool(value)
                if isinstance(value, str):
                    normalized = value.strip().lower()
                    if normalized in {"1", "true", "yes", "on"}:
                        return True
                    if normalized in {"0", "false", "no", "off"}:
                        return False
            return None

        def annotate_session_continuity(candidate: Json, record: Json) -> Json:
            embedding_metadata = embedding_metadata_by_ref.get(
                (candidate.get("ref_type"), candidate.get("ref_hash")),
                {},
            )

            def first_value(key: str, default: object = "") -> object:
                for source in (candidate, record, embedding_metadata):
                    value = source.get(key) if isinstance(source, dict) else None
                    if value not in (None, "", [], {}):
                        return value
                return default

            def first_list(key: str) -> list[Any]:
                value = first_value(key, [])
                return value if isinstance(value, list) else []

            def first_dict(key: str) -> Json:
                value = first_value(key, {})
                return value if isinstance(value, dict) else {}

            record_scope = candidate_access_scope(record)
            status = session_continuity_status(record_scope, retrieval_scope)
            explicit_status = str(first_value("session_continuity") or "")
            explicit_memory_scope = str(first_value("memory_scope") or "")
            explicit_profile_memory_class = str(first_value("profile_memory_class") or "")
            explicit_profile_memory_kind = str(first_value("profile_memory_kind") or "")
            explicit_profile_current = first_explicit_bool("profile_entity_current", candidate, record, embedding_metadata)
            if explicit_status in {"same_session", "cross_session"} and explicit_memory_scope == "user_profile":
                status = explicit_status
            elif status in {"", "unscoped"} and explicit_status in {"same_session", "cross_session"}:
                status = explicit_status
            boost = session_continuity_boost({**candidate, "session_continuity": status}, question_type)
            reason = (
                "same-session continuity"
                if status == "same_session"
                else "cross-session memory bridge"
                if status == "cross_session"
                else "session-neutral context"
            )
            source_session_ids = first_list("source_session_ids")
            source_entity_hashes = first_list("source_entity_hashes")
            source_event_ids = first_list("source_event_ids")
            extraction_context_event_ids = first_list("extraction_context_event_ids")
            current_state_source_session_count = candidate.get("current_state_source_session_count")
            current_state_source_entity_count = candidate.get("current_state_source_entity_count")
            try:
                current_state_source_session_count = int(
                    current_state_source_session_count
                    or first_value("profile_source_session_count", 0)
                    or first_value("source_session_count", 0)
                    or 0
                )
            except (TypeError, ValueError):
                current_state_source_session_count = 0
            try:
                current_state_source_entity_count = int(
                    current_state_source_entity_count
                    or first_value("profile_source_entity_count", 0)
                    or first_value("source_entity_count", 0)
                    or 0
                )
            except (TypeError, ValueError):
                current_state_source_entity_count = 0
            return {
                **candidate,
                "session_continuity": status,
                "continuity_boost": round(boost, 6),
                "continuity_reason": reason,
                "memory_scope": first_value("memory_scope"),
                "profile_memory_class": explicit_profile_memory_class,
                "profile_memory_kind": explicit_profile_memory_kind,
                "event_type": first_value("event_type"),
                "classification": first_value("classification"),
                "extraction_status": first_value("extraction_status", first_value("status")),
                "source_kind": first_value("source_kind", first_value("source_type")),
                "extraction_phase": first_value("extraction_phase"),
                "final_session_boundary": bool(first_value("final_session_boundary", False)),
                "promoted_from_memory_scope": first_value("promoted_from_memory_scope"),
                "profile_promotion_policy": first_value("profile_promotion_policy"),
                "profile_promotion_blocker": first_value("profile_promotion_blocker"),
                "source_role": normalize_message_role(first_value("source_role")),
                "source_roles": first_list("source_roles"),
                "source_role_counts": first_dict("source_role_counts"),
                "source_hook_types": first_list("source_hook_types"),
                "source_hook_type_counts": first_dict("source_hook_type_counts"),
                "source_codex_events": first_list("source_codex_events"),
                "source_codex_event_counts": first_dict("source_codex_event_counts"),
                "source_memory_selection_policies": first_list("source_memory_selection_policies"),
                "source_memory_selection_policy_counts": first_dict("source_memory_selection_policy_counts"),
                "source_memory_layers": first_list("source_memory_layers"),
                "source_memory_layer_counts": first_dict("source_memory_layer_counts"),
                "source_memory_selection_lossy_count": first_value("source_memory_selection_lossy_count", 0),
                "source_memory_selection_complete_count": first_value("source_memory_selection_complete_count", 0),
                "source_memory_selection_dropped_text_chars": first_value("source_memory_selection_dropped_text_chars", 0),
                "source_memory_selection_dropped_line_count": first_value("source_memory_selection_dropped_line_count", 0),
                "source_memory_selection_retained_text_ratio_avg": first_value("source_memory_selection_retained_text_ratio_avg", 1.0),
                "source_memory_selection_retained_line_ratio_avg": first_value("source_memory_selection_retained_line_ratio_avg", 1.0),
                "source_memory_scopes": first_list("source_memory_scopes"),
                "source_session_continuities": first_list("source_session_continuities"),
                "source_extraction_phases": first_list("source_extraction_phases"),
                "source_profile_promotion_policies": first_list("source_profile_promotion_policies"),
                "source_profile_promotion_blockers": first_list("source_profile_promotion_blockers"),
                "source_session_ids": source_session_ids,
                "source_event_ids": source_event_ids,
                "source_entity_hashes": source_entity_hashes,
                "source_session_count": len(source_session_ids),
                "source_event_count": len(source_event_ids),
                "source_entity_count": len(source_entity_hashes),
                "extraction_context_event_ids": extraction_context_event_ids,
                "current_state_source_session_count": current_state_source_session_count or len(source_session_ids),
                "current_state_source_entity_count": current_state_source_entity_count or len(source_entity_hashes),
                "profile_entity_current": (
                    explicit_profile_current
                    if explicit_profile_current is not None
                    else explicit_memory_scope == "user_profile" and status == "cross_session"
                ),
                "profile_revision": first_value("profile_revision", 0),
                "previous_profile_revision": first_value("previous_profile_revision", 0),
                "previous_profile_updated_at_ms": first_value("previous_profile_updated_at_ms", 0),
                "source_entity_types": first_list("source_entity_types"),
                "question_type": question_type,
            }

        finish_retrieval_stage("query_understanding", stage_started_perf)
        native_pack = self.native_context_pack({
            "query": query,
            "scope": retrieval_scope,
            "question_type": question_type,
            "query_plan": query_plan,
            "secondary_index_groups": [sorted(group) for group in secondary_index_filter_groups],
            "secondary_index_filter_mode": secondary_index_filter_mode,
            "max_context_tokens": max_context_tokens,
            "local_budget": {
                "token_estimate": int(local_budget.get("token_estimate", 0)),
                "safety_margin_tokens": int(local_budget.get("safety_margin_tokens", 0)),
                "remote_budget_tokens": int(local_budget.get("remote_budget_tokens", max_context_tokens)),
            },
            "cross_session": cross_session_policy,
            "shared_context": shared_context_policy,
            "source_role_budget_tokens": source_role_budget_tokens,
            "source_role_budget_mode": source_role_budget_mode or ("explicit" if source_role_budget_tokens else "disabled"),
            "memory_layer_budget_tokens": memory_layer_budget_tokens,
            "memory_layer_budget_mode": memory_layer_budget_mode or ("explicit" if memory_layer_budget_tokens else "disabled"),
            "memory_selection_policy_budget_tokens": memory_selection_policy_budget_tokens,
            "memory_selection_policy_budget_mode": memory_selection_policy_budget_mode or (
                "explicit" if memory_selection_policy_budget_tokens else "disabled"
            ),
            "extraction_phase_budget_tokens": extraction_phase_budget_tokens,
            "extraction_phase_budget_mode": extraction_phase_budget_mode or (
                "explicit" if extraction_phase_budget_tokens else "disabled"
            ),
            "memory_layer_budget_question_type": question_type,
            "memory_layer_budget_question_reason": memory_layer_budget_reason,
            "pre_retrieval_idle_commit": pre_retrieval_idle_commit,
            "pre_retrieval_summary_refresh": pre_retrieval_summary_refresh,
            "ranking": ranking,
            "deadline_ms": deadline_ms,
            "reference_time_ms": reference_time_ms,
            "include_superseded_resources": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
            "audit_mode": audit_mode,
        })
        if native_pack is not None:
            recall_policy = native_pack.get("recall_policy") if isinstance(native_pack.get("recall_policy"), dict) else {}
            recall_policy.setdefault("native_context_pack", {
                "enabled": True,
                "python_role": "mcp_auth_model_request_shaping_only",
                "backend_role": "scan_filter_score_pack",
            })
            recall_policy.setdefault("stage_latency_budgets", stage_budget_snapshot())
            recall_policy.setdefault("pre_retrieval_summary_refresh", pre_retrieval_summary_refresh)
            native_pack["recall_policy"] = recall_policy
            native_pack.setdefault("pre_retrieval_summary_refresh", pre_retrieval_summary_refresh)
            native_pack.setdefault("context_pack_cache_hit", False)
            native_pack.setdefault("context_pack_assembly", "native_backend")
            native_pack.setdefault("remote_context_refs", native_pack.get("selected_refs", []))
            native_pack.setdefault("selected_ref_counts", selected_context_class_counts(native_pack.get("selected_refs", [])))
            selected_refs = native_pack.get("selected_refs", []) if isinstance(native_pack.get("selected_refs"), list) else []
            context_pack_id_text = str(native_pack.get("context_pack_id") or stable_hash(f"native:{query}:{selected_refs}:{now_ms()}"))
            native_pack["context_pack_id"] = context_pack_id_text
            if audit_mode == "full" and audit_sample_rate > 0 and (audit_sample_rate >= 1.0 or stable_hash(context_pack_id_text) % 10000 < int(audit_sample_rate * 10000)):
                self.append_audit(
                    compact_context_pack_audit_record({
                        "record_type": "context_pack_audit",
                        "context_pack_id": context_pack_id_text,
                        "query": query,
                        "scope": scope,
                        "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected_refs), limit=512),
                        "selected_refs": compact_refs_for_audit(selected_refs),
                        "local_context_refs": compact_local_context_refs(local_budget),
                        "context_sources_order": native_pack.get("context_sources_order", []),
                        "selected_ref_counts": native_pack.get("selected_ref_counts", {}),
                        "dropped_refs": native_pack.get("dropped_refs", {}),
                        "quality_warnings": native_pack.get("quality_warnings", []),
                        "question_type": question_type,
                        "packing_policy": native_pack.get("packing_policy", "native_backend"),
                        "recall_policy": recall_policy,
                        "stage_latency_budgets": recall_policy.get("stage_latency_budgets", {}),
                        "storage_options": storage_options,
                        "used_remote_context_tokens": native_pack.get("used_remote_context_tokens", native_pack.get("used_context_tokens", 0)),
                        "remote_context_budget_tokens": native_pack.get("remote_context_budget_tokens", max_context_tokens),
                        "requested_max_context_tokens": native_pack.get("requested_max_context_tokens", max_context_tokens),
                        "created_at_ms": now_ms(),
                    })
                )
            # Remote-only safety floor (native/production path): if the native remote pack came
            # back too sparse, re-admit the request's local context so a retrieval miss never
            # leaves the agent blind. Only fires for remote_only requests below the token floor.
            native_used_remote = int(
                native_pack.get("used_remote_context_tokens", native_pack.get("used_context_tokens", 0)) or 0
            )
            if apply_remote_only_local_fallback(local_budget, native_used_remote):
                native_pack["local_context_refs"] = compact_local_context_refs(local_budget)
                native_pack["context_source_mode"] = "remote_only_local_fallback"
            serving_selected_refs = compact_context_pack_refs(selected_refs, include_debug=debug_refs)
            native_pack["selected_refs"] = serving_selected_refs
            native_pack["remote_context_refs"] = serving_selected_refs
            native_pack["dropped_refs"] = compact_dropped_refs_for_context_pack(native_pack.get("dropped_refs", {}), include_debug=debug_refs)
            native_pack["context_pack_payload_policy"] = {
                "serving_refs": "compact" if not debug_refs else "debug_full",
                "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
                "dropped_ref_details": "audit_only" if not debug_refs else "included",
                "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
            }
            return compact_context_pack_for_serving(native_pack, include_debug=debug_refs)
        if self.native_context_pack_required():
            raise MatrixArkError(
                "backend-native ContextPack assembly is required for TemporalStore serving, "
                "but this backend did not return matrixark_retrieve_context_pack. "
                "Python reference packing is disabled unless explicitly overridden for local debug."
            )
        embedding_started_perf = time.perf_counter()
        # The query side takes the query prefix; every other call here embeds document text.
        query_embedding = embedding_for_text(retrieval_query, role="query")
        self._observe_model_latency("query_embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
        stage_started_perf = time.perf_counter()
        retrieval_record_result = self.retrieval_records(
            scope=retrieval_scope,
            secondary_index_groups=secondary_index_filter_groups,
        )
        records = retrieval_record_result["records"]
        if pre_retrieval_refreshed_records or int(pre_retrieval_summary_refresh.get("refreshed_count") or 0) > 0:
            same_user_summary_records = list(pre_retrieval_refreshed_records)
            try:
                same_user_summary_records.extend(
                    record
                    for record in self.read_all()
                    if isinstance(record, dict)
                    and record.get("record_type") == "context_summary"
                    and access_scope_matches_before_scoring(record, retrieval_scope)
                )
            except Exception:
                pass
            seen_refreshed_summary_ids = {
                (
                    record.get("record_type"),
                    record.get("summary_hash") or record.get("node_hash"),
                    tuple(record.get("node_path", [])),
                )
                for record in records
                if isinstance(record, dict)
            }
            for record in same_user_summary_records:
                if record.get("record_type") != "context_summary":
                    continue
                identity = (
                    record.get("record_type"),
                    record.get("summary_hash") or record.get("node_hash"),
                    tuple(record.get("node_path", [])),
                )
                if identity in seen_refreshed_summary_ids:
                    continue
                records.append(record)
                seen_refreshed_summary_ids.add(identity)
        retrieval_scan_stats = retrieval_record_result.get("scan_stats", {})
        async_pipeline_readiness = async_pipeline_retrieval_readiness(records, retrieval_scope)
        inventory_record_result = self.retrieval_records(
            scope=retrieval_scope,
            secondary_index_groups=[],
        )
        memory_inventory = retrieval_memory_inventory(inventory_record_result["records"], retrieval_scope)
        node_scope_by_hash: dict[int, Json] = {}
        ref_scope_by_key: dict[tuple[str, Any], Json] = {}

        def remember_ref_scope(ref_type: str, ref_hash: Any, source_record: Json) -> None:
            if ref_hash in (None, ""):
                return
            source_scope = candidate_access_scope(source_record)
            if source_scope:
                ref_scope_by_key.setdefault((ref_type, ref_hash), source_scope)

        for source_record in records:
            source_record_type = str(source_record.get("record_type") or "")
            if source_record_type == "context_event":
                remember_ref_scope("event", source_record.get("event_id_hash"), source_record)
            elif source_record_type == "context_entity":
                remember_ref_scope("entity", source_record.get("entity_hash"), source_record)
            elif source_record_type == "context_segment":
                remember_ref_scope("segment", source_record.get("segment_hash"), source_record)
            elif source_record_type == "context_summary":
                remember_ref_scope("summary", source_record.get("summary_hash") or source_record.get("node_hash"), source_record)
            elif source_record_type == "context_compression_event":
                remember_ref_scope("compression", source_record.get("compression_id_hash"), source_record)
            try:
                source_node_hash = int(source_record.get("node_hash") or 0)
            except (TypeError, ValueError):
                source_node_hash = 0
            if not source_node_hash or source_node_hash in node_scope_by_hash:
                continue
            source_scope = candidate_access_scope(source_record)
            if source_scope:
                node_scope_by_hash[source_node_hash] = source_scope

        def scope_from_node_path(node_path: Any) -> Json:
            if not isinstance(node_path, list):
                return {}
            recovered_scope: Json = {}
            for part in node_path:
                value = str(part or "")
                if value.startswith("tenant:"):
                    recovered_scope["tenant_id"] = value.split(":", 1)[1]
                elif value.startswith("user:"):
                    recovered_scope["user_id"] = value.split(":", 1)[1]
                elif value.startswith("session:"):
                    recovered_scope["session_id"] = value.split(":", 1)[1]
            return {key: value for key, value in recovered_scope.items() if value}

        def recovered_record_scope(record: Json) -> Json:
            record_scope = candidate_access_scope(record)
            if record_scope:
                return record_scope
            # A folded owner carries the retired embedding record's fields under embedding_meta;
            # the access scope that used to be recovered from the separate record is there.
            meta = record.get("embedding_meta")
            if isinstance(meta, dict):
                record_scope = candidate_access_scope(meta)
                if record_scope:
                    return record_scope
            if record.get("record_type") == "context_embedding":
                ref_scope = ref_scope_by_key.get((str(record.get("ref_type") or ""), record.get("ref_hash")))
                if ref_scope:
                    return ref_scope
            try:
                node_hash = int(record.get("node_hash") or 0)
            except (TypeError, ValueError):
                node_hash = 0
            if node_hash and node_hash in node_scope_by_hash:
                return node_scope_by_hash[node_hash]
            return scope_from_node_path(record.get("node_path", []))

        def recovered_scope_for_query(record: Json, query_scope: Json) -> Json:
            record_scope = recovered_record_scope(record)
            if (
                record_scope
                and query_scope.get("account_id")
                and not record_scope.get("account_id")
                and (record_scope.get("tenant_id") or record_scope.get("user_id") or record_scope.get("session_id"))
            ):
                record_scope = {**record_scope, "account_id": query_scope.get("account_id")}
            return record_scope

        def recovered_scope_matches(record: Json, query_scope: Json) -> bool:
            return scope_matches(recovered_scope_for_query(record, query_scope), query_scope)

        def session_scope_allows_retrieval_record(record: Json, query_scope: Json) -> bool:
            if session_scope_mode(query_scope) != "only":
                return True
            query_session = str(query_scope.get("session_id") or "").strip()
            if not query_session:
                return True
            record_type = str(record.get("record_type") or "")
            if not record_type.startswith("context_") and record_type != "matrixark_async_pipeline_task":
                return True
            memory_scope = str(record.get("memory_scope") or "").strip().lower()
            session_continuity = str(record.get("session_continuity") or "").strip().lower()
            if memory_scope in {"user_profile", "profile", "cross_session_profile"} or session_continuity == "cross_session":
                return False
            record_scope = recovered_scope_for_query(record, query_scope)
            record_session = str(record_scope.get("session_id") or "").strip()
            if record_session and record_session != query_session:
                return False
            return True

        def profile_summary_scope_matches(record: Json, query_scope: Json) -> bool:
            if record.get("record_type") != "context_summary":
                return False
            node_path = [str(part or "") for part in record.get("node_path", []) if str(part or "")]
            if "profile:long_term_memory" not in node_path:
                return False
            path_scope = scope_from_node_path(node_path)
            if query_scope.get("account_id") and not path_scope.get("account_id"):
                path_scope = {**path_scope, "account_id": query_scope.get("account_id")}
            return scope_matches(path_scope, query_scope)

        if session_scope_mode(retrieval_scope) == "only":
            records = [
                record
                for record in records
                if session_scope_allows_retrieval_record(record, retrieval_scope)
            ]

        def deadline_fallback(reason: str, fallback_records: list[Json] | None = None) -> Json:
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records if fallback_records is None else fallback_records,
                reason=reason,
                budget_source=budget_source,
                retrieval_scope=retrieval_scope,
                source_role_budget_tokens=source_role_budget_tokens,
                source_role_budget_mode=source_role_budget_mode,
                memory_layer_budget_tokens=memory_layer_budget_tokens,
                memory_layer_budget_mode=memory_layer_budget_mode,
                memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
                memory_selection_policy_budget_mode=memory_selection_policy_budget_mode,
                extraction_phase_budget_tokens=extraction_phase_budget_tokens,
                extraction_phase_budget_mode=extraction_phase_budget_mode,
            )
        skill_controls = self.latest_skill_controls(records)
        include_superseded_resources = bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False))
        latest_resource_version_by_hash: dict[int, str] = {}
        resource_uri_by_hash: dict[int, str] = {}
        for manifest in reversed(records):
            if manifest.get("record_type") != "resource_manifest":
                continue
            if not scope_matches(candidate_access_scope(manifest), scope):
                continue
            try:
                resource_hash_key = int(manifest.get("resource_hash") or 0)
            except (TypeError, ValueError):
                resource_hash_key = 0
            raw_uri_key = str(manifest.get("raw_uri") or "")
            resource_version_key = str(manifest.get("resource_version") or "")
            if resource_hash_key:
                if raw_uri_key and resource_hash_key not in resource_uri_by_hash:
                    resource_uri_by_hash[resource_hash_key] = raw_uri_key
                if resource_version_key and resource_hash_key not in latest_resource_version_by_hash:
                    latest_resource_version_by_hash[resource_hash_key] = resource_version_key
        finish_retrieval_stage("candidate_fetch", stage_started_perf)
        stage_started_perf = time.perf_counter()
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_record_load",
                budget_source=budget_source,
                retrieval_scope=retrieval_scope,
                source_role_budget_tokens=source_role_budget_tokens,
                source_role_budget_mode=source_role_budget_mode,
                memory_layer_budget_tokens=memory_layer_budget_tokens,
                memory_layer_budget_mode=memory_layer_budget_mode,
                memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
                memory_selection_policy_budget_mode=memory_selection_policy_budget_mode,
                extraction_phase_budget_tokens=extraction_phase_budget_tokens,
                extraction_phase_budget_mode=extraction_phase_budget_mode,
            )
        node_scores: dict[int, Json] = {}
        event_embedding_vectors: dict[int, list[float]] = {}
        entity_embedding_vectors: dict[int, list[float]] = {}
        segment_embedding_vectors: dict[int, list[float]] = {}
        compression_embedding_vectors: dict[int, list[float]] = {}
        resource_embedding_vectors: dict[int, list[float]] = {}
        skill_embedding_vectors: dict[int, list[float]] = {}
        index_terms_by_batch: dict[Any, list[str]] = {}
        index_terms_by_node: dict[Any, list[str]] = {}
        index_terms_by_ref: dict[Any, list[str]] = {}
        index_terms_by_node_for_prefilter: dict[int, list[str]] = {}
        node_summary_text_by_hash: dict[int, str] = {}
        for scan_index, record in enumerate(records, 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_embedding_index_scan")
            record_type = record.get("record_type")
            if record_type == "context_index" and recovered_scope_matches(record, retrieval_scope):
                index_name = str(record.get("index_name", ""))
                if index_name:
                    ref_hashes = context_index_ref_hashes(record)
                    if record.get("batch_id_hash") is not None:
                        index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                    node_hash_for_index = record.get("node_hash")
                    try:
                        index_terms_by_node_for_prefilter.setdefault(int(node_hash_for_index), []).append(index_name)
                    except (TypeError, ValueError):
                        pass
                    if ref_hashes:
                        for ref_hash in ref_hashes:
                            index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                    else:
                        ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
                        if ref_hash is not None:
                            index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                        else:
                            index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
            if record_type == "context_summary" and recovered_scope_matches(record, retrieval_scope):
                summary_type = str(record.get("summary_type", ""))
                if summary_type in {"node_l0", "node_l1", "batch_l0", "session_l0"}:
                    try:
                        node_hash_for_summary = int(record.get("node_hash"))
                    except (TypeError, ValueError):
                        continue
                    existing = node_summary_text_by_hash.get(node_hash_for_summary, "")
                    summary_text = str(record.get("summary_text", ""))
                    if len(summary_text) > len(existing):
                        node_summary_text_by_hash[node_hash_for_summary] = summary_text
        secondary_index_prefilter_node_hashes = {
            node_hash
            for node_hash, terms in index_terms_by_node_for_prefilter.items()
            if passes_secondary_index_filters(set(terms), secondary_index_filter_groups, mode=secondary_index_filter_mode)
        } if secondary_index_filter_groups else set()
        query_plan["secondary_index_prefilter"] = {
            "applied_before_l0_l1_traversal": True,
            "matched_node_count": len(secondary_index_prefilter_node_hashes),
            "fallback_when_no_index_matches": True,
            "strategy": "ContextIndex node hints boost L0/L1 traversal; leaf candidates still verify filters before embedding scoring",
        }
        # The encoder this request is asking with. Read once: it is process configuration, and a
        # value that changed mid-scan would decline some of a store and score the rest.
        active_embedding_model = embedding_model_name()
        embedding_model_conflict_records = 0
        embedding_width_conflict_records = 0

        def stored_encoder_name(record: Json) -> str:
            """Which encoder wrote this record's vector.

            Owner records carry it under `embedding_meta`, not at the top level: the separate
            embedding row was folded into its owner and its fields ride along there. Reading only
            the top level found nothing on every record a current ingest writes, so the check
            silently never applied -- the shape of bug it exists to catch.
            """
            meta = record.get("embedding_meta")
            if isinstance(meta, dict):
                name = meta.get("model") or meta.get("model_ref") or ""
                if name:
                    return str(name)
            return str(record.get("model") or record.get("model_ref") or "")

        def usable_vector(record: Json) -> list:
            """The record's vector, or nothing at all when it cannot be compared with the query's.

            Nothing rather than a zero score. The blend maps a dense score of 0.0 to 0.5 before
            weighting, so a meaningless dense value is not neutral -- it hands the record 0.36 of
            the final score for having a vector nobody can read. Returning an empty vector puts the
            record on the same footing as one that has not been embedded yet, which is what it is.
            """
            nonlocal embedding_model_conflict_records, embedding_width_conflict_records
            # Decoded FIRST. Vectors are stored base64-encoded now, so reading the raw field and
            # testing `isinstance(list)` sees a string, concludes "no vector", and silently stops
            # scoring every record in the store -- no error, no log, just worse answers. The guard
            # has to look at the decoded vector, which is also the only form its width check means
            # anything against.
            vector = record_vector(record)
            if not vector:
                return []
            stored_model = stored_encoder_name(record)
            if embedding_model_conflicts(stored_model, active_embedding_model):
                embedding_model_conflict_records += 1
                return []
            # Width is the coarser check and catches what the name cannot: a store written before
            # the model was recorded carries no name, so nothing conflicts by name, and can still
            # hold vectors of another width.
            if query_embedding and len(vector) != len(query_embedding):
                embedding_width_conflict_records += 1
                return []
            return vector

        for scan_index, record in enumerate(records, 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_embedding_vector_scan")
            record_type = record.get("record_type")
            if record_type == "context_embedding":
                remember_embedding_metadata(record)
                if not recovered_scope_matches(record, retrieval_scope):
                    continue
            else:
                # A folded owner carries the retired record's copy under embedding_meta; the
                # self-repair net reads it exactly as it read the separate row, so an owner
                # that lost top-level fields is still repaired from its own ride-along copy.
                meta = record.get("embedding_meta")
                owner_ref = _EMBEDDING_OWNER_REFS.get(str(record_type or ""))
                if isinstance(meta, dict) and meta and owner_ref is not None:
                    ref_type, field = owner_ref
                    ref_hash = record.get(field)
                    if ref_hash not in (None, ""):
                        remember_embedding_metadata(
                            {**meta, "ref_type": ref_type, "ref_hash": ref_hash}
                        )
            if record_type == "context_embedding" and record.get("embedding_type") in {"node_l0", "node_l1", "context_node"}:
                dense_score = cosine(query_embedding, usable_vector(record))
                node_hash = record["node_hash"]
                node_text = " ".join(record.get("node_path", [])) + " " + node_summary_text_by_hash.get(node_hash, "")
                sparse_score = sparse_lexical_score(query_terms, node_text)
                index_hint_boost = 0.08 if node_hash in secondary_index_prefilter_node_hashes else 0.0
                score = round(clamp01(0.72 * normalized_dense_score(dense_score) + 0.28 * sparse_score + index_hint_boost), 6)
                current = node_scores.get(node_hash)
                if current is None or score > current["score"]:
                    node_scores[node_hash] = {
                        "node_hash": node_hash,
                        "node_path": record.get("node_path", []),
                        "depth": record.get("depth", len(record.get("node_path", []))),
                        "score": score,
                        "dense_score": dense_score,
                        "sparse_score": sparse_score,
                        "embedding_type": record.get("embedding_type"),
                    }
            elif record_type == "context_event" and record.get("vector"):
                # DUAL-READ: the owner record carries its vector -- since the fold-and-drop this
                # is the ONLY place a new log stores it. setdefault so a separate
                # context_embedding record from an old log still wins when both exist; the two
                # are identical by construction where both were written.
                event_embedding_vectors.setdefault(record["event_id_hash"], usable_vector(record))
            elif record_type == "context_entity" and record.get("vector"):
                entity_embedding_vectors.setdefault(record["entity_hash"], usable_vector(record))
            elif record_type == "context_segment" and record.get("vector"):
                segment_embedding_vectors.setdefault(record["segment_hash"], usable_vector(record))
            elif record_type == "context_compression_event" and record.get("vector"):
                compression_embedding_vectors.setdefault(
                    record["compression_id_hash"], usable_vector(record)
                )
            elif record_type == "resource_chunk" and record.get("vector"):
                resource_embedding_vectors.setdefault(record["chunk_hash"], usable_vector(record))
            elif record_type == "skill_section" and record.get("vector"):
                resource_embedding_vectors.setdefault(record["section_hash"], usable_vector(record))
            elif record_type == "context_node" and record.get("vector"):
                # Node scoring from the node's own vector, exactly the formula the separate
                # records used; an old log's separate record wins via the earlier branch, so
                # this only fills nodes nothing has scored yet.
                dense_score = cosine(query_embedding, usable_vector(record))
                node_hash = record["node_hash"]
                node_text = " ".join(record.get("node_path", [])) + " " + node_summary_text_by_hash.get(node_hash, "")
                sparse_score = sparse_lexical_score(query_terms, node_text)
                index_hint_boost = 0.08 if node_hash in secondary_index_prefilter_node_hashes else 0.0
                score = round(clamp01(0.72 * normalized_dense_score(dense_score) + 0.28 * sparse_score + index_hint_boost), 6)
                current = node_scores.get(node_hash)
                if current is None or score > current["score"]:
                    node_scores[node_hash] = {
                        "node_hash": node_hash,
                        "node_path": record.get("node_path", []),
                        "depth": record.get("depth", len(record.get("node_path", []))),
                        "score": score,
                        "dense_score": dense_score,
                        "sparse_score": sparse_score,
                        "embedding_type": "context_node",
                    }
            elif record_type == "context_embedding" and record.get("embedding_type") == "event_text":
                event_embedding_vectors[record["ref_hash"]] = record_vector(record)
            elif record_type == "context_embedding" and record.get("embedding_type") in {"entity_state", "profile_entity_state"}:
                entity_embedding_vectors[record["ref_hash"]] = record_vector(record)
            elif record_type == "context_embedding" and record.get("embedding_type") == "segment_text":
                segment_embedding_vectors[record["ref_hash"]] = record_vector(record)
            elif record_type == "context_embedding" and record.get("embedding_type") == "compression_summary":
                compression_embedding_vectors[record["ref_hash"]] = record_vector(record)
            elif record_type == "context_embedding" and record.get("embedding_type") == "resource_chunk":
                resource_embedding_vectors[record["ref_hash"]] = record_vector(record)
            elif record_type == "context_embedding" and record.get("embedding_type") == "skill_section":
                resource_embedding_vectors[record["ref_hash"]] = record_vector(record)
            elif record_type == "context_embedding" and record.get("embedding_type") == "skill_summary":
                skill_embedding_vectors[record["ref_hash"]] = record_vector(record)
        for record in records:
            if record.get("record_type") != "context_node":
                continue
            try:
                node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                continue
            if node_hash not in secondary_index_prefilter_node_hashes or node_hash in node_scores:
                continue
            node_scores[node_hash] = {
                "node_hash": node_hash,
                "node_path": record.get("node_path", []),
                "depth": record.get("depth", len(record.get("node_path", []))),
                "score": 0.58,
                "dense_score": 0.0,
                "sparse_score": 0.0,
                "embedding_type": "secondary_index_hint",
            }
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_embedding_index_scan",
                budget_source=budget_source,
                retrieval_scope=retrieval_scope,
                source_role_budget_tokens=source_role_budget_tokens,
                source_role_budget_mode=source_role_budget_mode,
                memory_layer_budget_tokens=memory_layer_budget_tokens,
                memory_layer_budget_mode=memory_layer_budget_mode,
                memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
                memory_selection_policy_budget_mode=memory_selection_policy_budget_mode,
                extraction_phase_budget_tokens=extraction_phase_budget_tokens,
                extraction_phase_budget_mode=extraction_phase_budget_mode,
            )

        top_k_per_layer = integer_arg(ranking, "top_k_per_layer", DEFAULT_TOP_K_PER_LAYER, minimum=1)
        max_children_scored_per_parent = bounded_max_children_scored_per_parent(
            integer_arg(
                ranking,
                "max_children_scored_per_parent",
                DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT,
                minimum=1,
            )
        )
        hard_max_children_scored_per_parent = max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT)
        max_candidates_per_node = integer_arg(ranking, "max_candidates_per_node", DEFAULT_MAX_CANDIDATES_PER_NODE, minimum=1)
        max_selected_refs = integer_arg(ranking, "max_selected_refs", DEFAULT_MAX_SELECTED_REFS, minimum=1)
        max_global_candidates = integer_arg(ranking, "max_global_candidates", DEFAULT_MAX_GLOBAL_CANDIDATES, minimum=1)
        min_similarity_score = float_arg(ranking, "min_similarity_score", DEFAULT_RETRIEVAL_MIN_SCORE, minimum=0.0, maximum=1.0)
        budget_fill_policy = str(ranking.get("budget_fill_policy", DEFAULT_BUDGET_FILL_POLICY) or DEFAULT_BUDGET_FILL_POLICY).strip().lower()
        if budget_fill_policy not in {"quality_first", "force_fill"}:
            raise MatrixArkError("budget_fill_policy must be quality_first or force_fill")
        max_raw_events_per_node = integer_arg(ranking, "max_raw_events_per_node", TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE, minimum=1)
        traversal = tree_first_traversal(
            node_scores,
            top_k_per_layer=top_k_per_layer,
            max_children_scored_per_parent=max_children_scored_per_parent,
        )
        finish_retrieval_stage("node_traversal", stage_started_perf)
        stage_started_perf = time.perf_counter()
        selected_paths = traversal["selected_paths"]
        selected_leaf_paths = traversal["leaf_paths"]
        selected_node_hashes = traversal["selected_node_hashes"]

        placement_record_result: Json = {}
        placement_candidate_records: list[Json] = []
        if selected_node_hashes and not traversal.get("fallback_to_flat"):
            placement_record_result = self.retrieval_records(
                scope=scope,
                secondary_index_groups=secondary_index_filter_groups,
                selected_node_hashes=selected_node_hashes,
                allow_broad_scan_fallback=False,
            )
            placement_candidate_records = placement_record_result.get("records", [])

            def record_identity(record: Json) -> tuple[str, Any]:
                record_type = str(record.get("record_type") or "")
                for field in (
                    "event_id_hash",
                    "entity_hash",
                    "segment_hash",
                    "compression_id_hash",
                    "summary_hash",
                    "chunk_hash",
                    "section_hash",
                    "skill_hash",
                    "resource_hash",
                    "batch_id_hash",
                ):
                    if record.get(field) is not None:
                        return (record_type, record.get(field))
                if record_type == "context_index":
                    return (
                        record_type,
                        (
                            record.get("index_name"),
                            record.get("node_hash"),
                            tuple(context_index_ref_hashes(record)),
                            record.get("timestamp_key_ms"),
                        ),
                    )
                return (record_type, stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":"))))

            seen_record_identities = {record_identity(record) for record in records}
            for record in placement_candidate_records:
                identity = record_identity(record)
                if identity in seen_record_identities:
                    continue
                records.append(record)
                seen_record_identities.add(identity)

            for record in placement_candidate_records:
                record_type = record.get("record_type")
                if record_type == "context_index" and recovered_scope_matches(record, retrieval_scope):
                    index_name = str(record.get("index_name", ""))
                    if index_name:
                        ref_hashes = context_index_ref_hashes(record)
                        if record.get("batch_id_hash") is not None:
                            index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                        node_hash_for_index = record.get("node_hash")
                        try:
                            index_terms_by_node_for_prefilter.setdefault(int(node_hash_for_index), []).append(index_name)
                        except (TypeError, ValueError):
                            pass
                        if ref_hashes:
                            for ref_hash in ref_hashes:
                                index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                        else:
                            ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
                            if ref_hash is not None:
                                index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                            else:
                                index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
                elif record_type == "context_embedding" and recovered_scope_matches(record, retrieval_scope):
                    remember_embedding_metadata(record)
                    embedding_type = record.get("embedding_type")
                    if embedding_type == "event_text":
                        event_embedding_vectors[record["ref_hash"]] = record_vector(record)
                    elif embedding_type in {"entity_state", "profile_entity_state"}:
                        entity_embedding_vectors[record["ref_hash"]] = record_vector(record)
                    elif embedding_type == "segment_text":
                        segment_embedding_vectors[record["ref_hash"]] = record_vector(record)
                    elif embedding_type == "compression_summary":
                        compression_embedding_vectors[record["ref_hash"]] = record_vector(record)
                    elif embedding_type == "resource_chunk":
                        resource_embedding_vectors[record["ref_hash"]] = record_vector(record)
                    elif embedding_type == "skill_section":
                        resource_embedding_vectors[record["ref_hash"]] = record_vector(record)
                    elif embedding_type == "skill_summary":
                        skill_embedding_vectors[record["ref_hash"]] = record_vector(record)

        def selected_by_tree(record: Json) -> bool:
            if traversal.get("fallback_to_flat"):
                return True
            path = node_path_tuple(record.get("node_path", []))
            if path and path in selected_paths:
                return True
            if path and any(
                starts_with_path(path, leaf_path) or starts_with_path(leaf_path, path)
                for leaf_path in selected_leaf_paths
            ):
                return True
            try:
                if int(record.get("node_hash")) in selected_node_hashes:
                    return True
            except (TypeError, ValueError):
                pass
            # Lexical exact-recall lane: admit a record whose node was pruned (or that carries no
            # scored node placement, e.g. a scope-less segment) when it shares a rare/exact token
            # with the query. Gated + additive; when lexical_exact_tokens is empty this is a no-op.
            if lexical_exact_tokens and record_lexical_exact_match(record, lexical_exact_tokens):
                return True
            return False

        if placement_candidate_records and not traversal.get("fallback_to_flat"):
            tree_candidate_records = [record for record in placement_candidate_records if selected_by_tree(record)]
            tree_prefilter_dropped_count = max(0, len(placement_candidate_records) - len(tree_candidate_records))
            retrieval_scan_stats = {
                **retrieval_scan_stats,
                "leaf_fetch": placement_record_result.get("scan_stats", {}),
                "leaf_fetch_record_count": len(placement_candidate_records),
                "leaf_fetch_strategy": "selected_node_placement",
            }
        else:
            tree_candidate_records = records if traversal.get("fallback_to_flat") else [record for record in records if selected_by_tree(record)]
            tree_prefilter_dropped_count = 0 if traversal.get("fallback_to_flat") else max(0, len(records) - len(tree_candidate_records))
        # --- Exact-value fact admission (general, gated) --------------------------------
        # Candidate FETCH is scoped to selected node placements, so a fact whose node was not
        # query-selected (a cross-session entity, or a current-session turn on an unselected
        # batch node) never becomes a candidate even when captured, scored, and scope-eligible.
        # Admit scope-matching exact_value_fact entities that are embedding- or lexically
        # relevant to the query (capped), so the value token can be packed. Additive + gated
        # (MATRIXARK_VALUE_FACT_ADMISSION, default on); reversible to prior fetch behavior.
        if (
            str(_os.environ.get("MATRIXARK_VALUE_FACT_ADMISSION", "1")).strip().lower()
            not in {"0", "false", "no", "off"}
            and not traversal.get("fallback_to_flat")
        ):
            _admitted_value_hashes = {
                record.get("entity_hash")
                for record in tree_candidate_records
                if record.get("record_type") == "context_entity"
            }
            _value_fact_scored: list = []
            for _vrecord in records:
                if _vrecord.get("record_type") != "context_entity":
                    continue
                if _vrecord.get("entity_type") != "exact_value_fact":
                    continue
                if _vrecord.get("entity_hash") in _admitted_value_hashes:
                    continue
                if not recovered_scope_matches(_vrecord, retrieval_scope):
                    continue
                _vsim = cosine(query_embedding, entity_embedding_vectors.get(_vrecord.get("entity_hash"), []))
                _vlex = bool(lexical_exact_tokens) and record_lexical_exact_match(_vrecord, lexical_exact_tokens)
                if _vsim >= 0.40 or _vlex:
                    _value_fact_scored.append((1.0 if _vlex else float(_vsim), _vrecord))
            _value_fact_scored.sort(key=lambda item: item[0], reverse=True)
            for _score, _vrecord in _value_fact_scored[:8]:
                tree_candidate_records.append(_vrecord)
        seen_profile_summary_hashes = {
            record.get("summary_hash")
            for record in tree_candidate_records
            if record.get("record_type") == "context_summary"
        }
        profile_summary_bridge_sources = list(records)
        try:
            profile_summary_bridge_sources.extend(
                record
                for record in self.read_all()
                if isinstance(record, dict) and record.get("record_type") == "context_summary"
            )
        except Exception:
            pass
        profile_summary_bridges = [
            record
            for record in profile_summary_bridge_sources
            if record.get("record_type") == "context_summary"
            and str(
                record.get("memory_scope")
                or embedding_metadata_by_ref.get(("summary", record.get("summary_hash") or record.get("node_hash")), {}).get("memory_scope")
                or ""
            )
            == "user_profile"
            and str(
                record.get("session_continuity")
                or embedding_metadata_by_ref.get(("summary", record.get("summary_hash") or record.get("node_hash")), {}).get("session_continuity")
                or ""
            )
            == "cross_session"
            and record.get("summary_hash") not in seen_profile_summary_hashes
            and (recovered_scope_matches(record, retrieval_scope) or profile_summary_scope_matches(record, retrieval_scope))
        ]
        tree_candidate_records.extend(profile_summary_bridges)
        # --- One partition pass, replacing seven full walks of this list -------------------
        # Built HERE, after the value-fact appends and the profile-summary extend above:
        # bucketing at construction time would drop every record added afterwards from every
        # scan below, and no test would notice because each scan would simply find less.
        _tree_records_by_type: dict[str, list] = {}
        _tree_prefilter_source: list = []
        _tree_resource_skill_source: list = []
        for _bucket_record in tree_candidate_records:
            _bucket_type = str(_bucket_record.get("record_type") or "")
            _tree_records_by_type.setdefault(_bucket_type, []).append(_bucket_record)
            # The two multi-type scans get their source built in this same pass rather than by
            # concatenating buckets afterwards, which would reorder interleaved records.
            if _bucket_type in ("context_event", "context_compression_event"):
                _tree_prefilter_source.append(_bucket_record)
            if _bucket_type in ("resource_chunk", "skill_section"):
                _tree_resource_skill_source.append(_bucket_record)
        # Default ON. Measured on the local backend, both arms scoring the same 212 candidates from
        # identical corpora in isolated event logs: 1272 record visits per retrieve against 122, a
        # 10.4x reduction, and the returned packs were IDENTICAL across five queries. Set the
        # variable to a falsey value to walk the whole candidate list per scan again.
        _type_buckets_enabled = str(
            _os.environ.get("MATRIXARK_RETRIEVE_TYPE_BUCKETS", "1")
        ).strip().lower() not in {"0", "false", "no", "off"}
        _scan_visits = [0]

        def _scan_source(bucketed):
            """The list a scan should walk, counting what walking it costs.

            Counted rather than timed: this host runs other work, and a visit count cannot be
            flattered by a quiet minute.
            """
            source = bucketed if _type_buckets_enabled else tree_candidate_records
            _scan_visits[0] += len(source)
            return source

        extraction_committed_event_ids = {
            int(record.get("event_id_hash") or 0)
            for record in records
            if record.get("record_type") == "matrixark_async_pipeline_task"
            and record.get("status") == "extraction_committed"
            and int(record.get("event_id_hash") or 0)
        }
        raw_event_ids_by_node: dict[Any, set[int]] = {}
        raw_event_time_window_dropped_count = 0
        events_by_node: dict[Any, list[Json]] = {}
        nodes_with_compression: set[Any] = set()
        for scan_index, record in enumerate(_scan_source(_tree_prefilter_source), 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_tree_candidate_prefilter", records)
            if record.get("record_type") == "context_compression_event":
                node_key_for_compression: Any = record.get("node_hash")
                if node_key_for_compression is None:
                    node_key_for_compression = tuple(record.get("node_path", []))
                nodes_with_compression.add(node_key_for_compression)
                continue
            if record.get("record_type") != "context_event":
                continue
            if record.get("source_chunk_hash"):
                continue
            node_key: Any = record.get("node_hash")
            if node_key is None:
                node_key = tuple(record.get("node_path", []))
            events_by_node.setdefault(node_key, []).append(record)
        for node_key, node_events in events_by_node.items():
            if node_key not in nodes_with_compression:
                continue
            node_events.sort(
                key=lambda item: (
                    self.context_event_ingestion_time_ms(item),
                    int(item.get("event_id_hash") or 0),
                ),
                reverse=True,
            )
            admitted = {
                int(record.get("event_id_hash"))
                for record in node_events[:max_raw_events_per_node]
                if record.get("event_id_hash") is not None
            }
            raw_event_ids_by_node[node_key] = admitted
            raw_event_time_window_dropped_count += max(0, len(node_events) - len(admitted))
        candidate_count_by_node: dict[Any, int] = {}
        fanout_dropped_count = 0

        def admit_candidate_for_node(record: Json) -> bool:
            nonlocal fanout_dropped_count
            node_key: Any = record.get("node_hash")
            if node_key is None:
                node_key = tuple(record.get("node_path", []))
            count = candidate_count_by_node.get(node_key, 0)
            if count >= max_candidates_per_node:
                fanout_dropped_count += 1
                return False
            candidate_count_by_node[node_key] = count + 1
            return True

        layer_scores = sorted(
            traversal["trace"] or node_scores.values(),
            key=lambda item: (item.get("depth", 0), -float(item.get("score", 0.0)), item.get("node_hash", 0)),
        )
        primary_matches = []
        auxiliary_matches = []
        summary_scan_question_types = {"broad_exploration", "profile_memory", "current_state", "latest"}
        # Lexical exact-recall lane: fact/evidence/etc. normally skip summaries (they prefer dense
        # raw refs), but when the exact fact lives ONLY in a properly-scoped L0/L1 summary, skipping
        # summaries loses it. Enter the summary scan for those types too when the query names a
        # rare/exact token, admitting ONLY lexically-matching summaries so normal fact packs are
        # unchanged. The original four question types keep their exact prior behavior.
        summary_lexical_lane = bool(lexical_exact_tokens) and question_type not in summary_scan_question_types
        if question_type in summary_scan_question_types or summary_lexical_lane:
            for scan_index, record in enumerate(reversed(_scan_source(_tree_records_by_type.get("context_summary", ()))), 1):
                if scan_index % 64 == 0 and deadline_exceeded():
                    return deadline_fallback("deadline_during_summary_scan", records)
                if record.get("record_type") != "context_summary":
                    continue
                if summary_lexical_lane and not record_lexical_exact_match(record, lexical_exact_tokens):
                    continue
                if not recovered_scope_matches(record, retrieval_scope) and not profile_summary_scope_matches(record, retrieval_scope):
                    continue
                summary_ref_hash = record.get("summary_hash") or record.get("node_hash")
                embedding_metadata = embedding_metadata_by_ref.get(("summary", summary_ref_hash), {})
                recovered_memory_scope = str(record.get("memory_scope") or embedding_metadata.get("memory_scope") or "")
                recovered_session_continuity = str(
                    record.get("session_continuity") or embedding_metadata.get("session_continuity") or ""
                )
                recovered_profile_summary_current = bool(
                    record.get("profile_summary_current")
                    or embedding_metadata.get("profile_summary_current")
                    or (
                        recovered_memory_scope == "user_profile"
                        and recovered_session_continuity == "cross_session"
                    )
                    or any(str(part).startswith("profile:") for part in record.get("node_path", []))
                )
                is_profile_summary_bridge = (
                    recovered_memory_scope == "user_profile"
                    and recovered_session_continuity == "cross_session"
                )
                if question_type in {"current_state", "latest"} and not is_profile_summary_bridge:
                    continue
                if not selected_by_tree(record) and not is_profile_summary_bridge:
                    continue
                summary_type = str(record.get("summary_type") or "")
                if summary_type not in {"node_l0", "node_l1", "resource_l0", "batch_l0", "session_l0", "session_final"}:
                    continue
                index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
                text = str(record.get("summary_text", ""))
                if not text:
                    continue
                lineage_text = " ".join(
                    [
                        recovered_memory_scope,
                        recovered_session_continuity,
                        *[str(value) for value in (record.get("source_roles") or embedding_metadata.get("source_roles") or []) if str(value or "")],
                        *[str(value) for value in (record.get("source_hook_types") or embedding_metadata.get("source_hook_types") or []) if str(value or "")],
                        *[str(value) for value in (record.get("source_codex_events") or embedding_metadata.get("source_codex_events") or []) if str(value or "")],
                        *[
                            str(value)
                            for value in (
                                record.get("source_memory_selection_policies")
                                or embedding_metadata.get("source_memory_selection_policies")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[
                            str(value).replace("_", " ")
                            for value in (
                                record.get("source_memory_selection_policies")
                                or embedding_metadata.get("source_memory_selection_policies")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (
                                record.get("source_memory_layers")
                                or embedding_metadata.get("source_memory_layers")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[
                            str(value).replace("_", " ")
                            for value in (
                                record.get("source_memory_layers")
                                or embedding_metadata.get("source_memory_layers")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (record.get("source_memory_scopes") or embedding_metadata.get("source_memory_scopes") or [])
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (
                                record.get("source_session_continuities")
                                or embedding_metadata.get("source_session_continuities")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (
                                record.get("source_profile_promotion_policies")
                                or embedding_metadata.get("source_profile_promotion_policies")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (
                                record.get("source_profile_promotion_blockers")
                                or embedding_metadata.get("source_profile_promotion_blockers")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[str(value) for value in record.get("source_entity_types", []) if str(value or "")],
                        *[str(value).replace("_", " ") for value in record.get("source_entity_types", []) if str(value or "")],
                        *sorted(index_terms),
                    ]
                )
                summary_filter_text = " ".join([text, text.replace("_", " "), lineage_text])
                filter_terms = set(index_terms)
                if is_profile_summary_bridge:
                    filter_terms.update(tokens(summary_filter_text))
                if not passes_applicable_secondary_index_filters(filter_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                    secondary_index_dropped_count += 1
                    continue
                secondary_index_matched_count += 1
                if not is_profile_summary_bridge and not admit_candidate_for_node(record):
                    continue
                lineage_score = sparse_lexical_score(query_terms, lineage_text)
                sparse_score = sparse_lexical_score(query_terms, text)
                keyword_score = len(query_terms.intersection(tokens(text)))
                embedding_score = cosine(query_embedding, embedding_for_text(" ".join(record.get("node_path", []) + [summary_type, text])))
                node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
                origin_score = min(1.0, 0.18 + hybrid_origin_score(query_terms, text, embedding_score, node_score) + 0.10 * lineage_score)
                if is_profile_summary_bridge and question_type in {"broad_exploration", "profile_memory", "current_state", "latest"}:
                    origin_score = min(1.0, origin_score + 0.20)
                if origin_score <= 0:
                    continue
                primary_matches.append(
                    score_recall_candidate(
                        annotate_session_continuity({
                            "ref_type": "summary",
                            "ref_hash": summary_ref_hash,
                            "node_hash": record.get("node_hash"),
                            "node_path": record.get("node_path", []),
                            "origin_score": origin_score,
                            "keyword_score": keyword_score,
                            "sparse_score": sparse_score,
                            "source_lineage_score": lineage_score,
                            "embedding_score": embedding_score,
                            "node_score": node_score,
                            "matched_index_terms": sorted(index_terms),
                            "selection_reason": "selected by tree path and L0/L1 summary relevance",
                            "event_type": summary_type,
                            "context_class": "summary",
                            "summary_type": summary_type,
                            "source_roles": record.get("source_roles") or embedding_metadata.get("source_roles", []),
                            "source_role_counts": record.get("source_role_counts") or embedding_metadata.get("source_role_counts", {}),
                            "source_hook_types": record.get("source_hook_types") or embedding_metadata.get("source_hook_types", []),
                            "source_hook_type_counts": record.get("source_hook_type_counts") or embedding_metadata.get("source_hook_type_counts", {}),
                            "source_codex_events": record.get("source_codex_events") or embedding_metadata.get("source_codex_events", []),
                            "source_codex_event_counts": record.get("source_codex_event_counts") or embedding_metadata.get("source_codex_event_counts", {}),
                            "source_memory_selection_policies": record.get("source_memory_selection_policies") or embedding_metadata.get("source_memory_selection_policies", []),
                            "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts") or embedding_metadata.get("source_memory_selection_policy_counts", {}),
                            "source_memory_selection_lossy_count": record.get("source_memory_selection_lossy_count", embedding_metadata.get("source_memory_selection_lossy_count", 0)),
                            "source_memory_selection_complete_count": record.get("source_memory_selection_complete_count", embedding_metadata.get("source_memory_selection_complete_count", 0)),
                            "source_memory_selection_dropped_text_chars": record.get("source_memory_selection_dropped_text_chars", embedding_metadata.get("source_memory_selection_dropped_text_chars", 0)),
                            "source_memory_selection_dropped_line_count": record.get("source_memory_selection_dropped_line_count", embedding_metadata.get("source_memory_selection_dropped_line_count", 0)),
                            "source_memory_selection_retained_text_ratio_avg": record.get("source_memory_selection_retained_text_ratio_avg", embedding_metadata.get("source_memory_selection_retained_text_ratio_avg", 1.0)),
                            "source_memory_selection_retained_line_ratio_avg": record.get("source_memory_selection_retained_line_ratio_avg", embedding_metadata.get("source_memory_selection_retained_line_ratio_avg", 1.0)),
                            "source_memory_scopes": record.get("source_memory_scopes") or embedding_metadata.get("source_memory_scopes", []),
                            "source_session_continuities": record.get("source_session_continuities") or embedding_metadata.get("source_session_continuities", []),
                            "source_extraction_phases": record.get("source_extraction_phases") or embedding_metadata.get("source_extraction_phases", []),
                            "source_profile_promotion_policies": record.get("source_profile_promotion_policies") or embedding_metadata.get("source_profile_promotion_policies", []),
                            "source_profile_promotion_blockers": record.get("source_profile_promotion_blockers") or embedding_metadata.get("source_profile_promotion_blockers", []),
                            "source_profile_memory_classes": record.get("source_profile_memory_classes") or embedding_metadata.get("source_profile_memory_classes", []),
                            "source_profile_memory_kinds": record.get("source_profile_memory_kinds") or embedding_metadata.get("source_profile_memory_kinds", []),
                            "profile_memory_class": record.get("profile_memory_class") or embedding_metadata.get("profile_memory_class", ""),
                            "profile_memory_kind": record.get("profile_memory_kind") or embedding_metadata.get("profile_memory_kind", ""),
                            "source_entity_types": record.get("source_entity_types", []),
                            "source_final_session_boundary_count": record.get("source_final_session_boundary_count", embedding_metadata.get("source_final_session_boundary_count", 0)),
                            "memory_scope": recovered_memory_scope,
                            "session_continuity": recovered_session_continuity,
                            "extraction_phase": record.get("extraction_phase") or embedding_metadata.get("extraction_phase", ""),
                            "final_session_boundary": bool(record.get("final_session_boundary", embedding_metadata.get("final_session_boundary", False))),
                            "profile_summary_current": recovered_profile_summary_current,
                            "profile_promotion_policy": record.get("profile_promotion_policy") or embedding_metadata.get("profile_promotion_policy", ""),
                            "profile_promotion_blocker": record.get("profile_promotion_blocker") or embedding_metadata.get("profile_promotion_blocker", ""),
                            "access_decision": "allowed_by_registry_scope_before_scoring",
                            "access_scope": candidate_access_scope(record),
                            "scope": candidate_access_scope(record),
                            "updated_at_ms": record.get("updated_at_ms", now_ms()),
                            "text": clip_context_text(text),
                            "recall_path": "primary_summary",
                        }, record),
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        for scan_index, record in enumerate(reversed(_scan_source(_tree_records_by_type.get("context_event", ()))), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_event_scan", records)
            if record.get("record_type") != "context_event":
                continue
            if (
                str(record.get("event_type") or record.get("classification") or "").lower() == "pending_async"
                and int(record.get("event_id_hash") or 0) in extraction_committed_event_ids
            ):
                continue
            event_node_key: Any = record.get("node_hash")
            if event_node_key is None:
                event_node_key = tuple(record.get("node_path", []))
            if (
                not record.get("source_chunk_hash")
                and event_node_key in raw_event_ids_by_node
                and int(record.get("event_id_hash") or 0) not in raw_event_ids_by_node[event_node_key]
            ):
                continue
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
            raw_event_type = str(record.get("event_type") or record.get("classification") or "")
            is_pending_async_event = (
                raw_event_type.strip().lower() == "pending_async"
                or str(record.get("classification") or "").strip().upper() == "PENDING_ASYNC_EXTRACTION"
                or str(record.get("extraction_phase") or "").strip().lower() == "pending_async"
                or str(record.get("extraction_status") or "").strip().lower() in {"pending", "async_pending"}
                or str(record.get("extraction_mode") or "").strip().lower() == "async_pending"
            )
            record_scope = (
                recovered_scope_for_query(record, retrieval_scope)
                if is_pending_async_event
                else candidate_access_scope(record)
            )
            if not (
                scope_matches(record_scope, retrieval_scope)
                if is_pending_async_event
                else access_scope_matches_before_scoring(record, retrieval_scope)
            ):
                continue
            if not selected_by_tree(record):
                continue
            text = str(record.get("text", ""))
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            index_record = record_with_embedding_defaults(record, "event", record.get("event_id_hash"))
            index_terms = candidate_index_terms(index_record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            secondary_filters_pass = passes_secondary_index_filters(
                index_terms,
                secondary_index_filter_groups,
                mode=secondary_index_filter_mode,
            )
            live_pending_text_match = is_pending_async_event and (keyword_score > 0 or sparse_score > 0)
            if not secondary_filters_pass and not live_pending_text_match:
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            embedding_score = cosine(query_embedding, event_embedding_vectors.get(record["event_id_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = hybrid_origin_score(query_terms, text, embedding_score, node_score)
            event_type = raw_event_type or ("pending_async" if is_pending_async_event else "")
            event_memory_layer = (
                candidate_memory_layer_name({**record, "ref_type": "event"})
                if is_pending_async_event
                else ""
            )
            candidate_metadata: Json = {}
            record_metadata = record.get("metadata")
            envelope_metadata = envelope.get("metadata")
            if isinstance(record_metadata, dict):
                candidate_metadata.update(record_metadata)
            if isinstance(envelope_metadata, dict):
                candidate_metadata.update(envelope_metadata)
            internal_extraction = record.get("internal_extraction") if isinstance(record.get("internal_extraction"), dict) else {}
            candidate = {
                "ref_type": "event",
                "ref_hash": record["event_id_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": (
                    "selected by tree path, secondary indexes, and resource fact/event hybrid score"
                    if record.get("source_chunk_hash")
                    else "selected by tree path, secondary indexes, and event hybrid score"
                ),
                "event_type": event_type,
                "batch_event_type": record.get("batch_event_type", "") if is_pending_async_event else "",
                "memory_layer": event_memory_layer,
                "classification": record.get("classification", ""),
                "extraction_status": record.get("status", ""),
                "extraction_mode": internal_extraction.get("mode", ""),
                "context_class": "resource_fact" if record.get("source_chunk_hash") else "event",
                "source_roles": record.get("source_roles", []),
                "source_role_counts": record.get("source_role_counts", {}),
                "source_hook_types": record.get("source_hook_types", []),
                "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                "source_codex_events": record.get("source_codex_events", []),
                "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
                "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
                "profile_memory_class": record.get("profile_memory_class", ""),
                "profile_memory_kind": record.get("profile_memory_kind", ""),
                "source_profile_memory_classes": record.get("source_profile_memory_classes", []),
                "source_profile_memory_kinds": record.get("source_profile_memory_kinds", []),
                "source_memory_scopes": record.get("source_memory_scopes", []),
                "source_session_continuities": record.get("source_session_continuities", []),
                "source_extraction_phases": record.get("source_extraction_phases", []),
                "source_chunk_hash": record.get("source_chunk_hash"),
                "source_ref": record.get("source_ref", ""),
                "metadata": candidate_metadata,
                "scope": record_scope,
                "updated_at_ms": record.get("updated_at_ms") or envelope.get("ingestion_time_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_text = " ".join(record.get("node_path", []) + sorted(index_terms) + [event_type, text])
            graph_score = sparse_lexical_score(query_terms, graph_text)
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_event_scan",
                budget_source=budget_source,
                retrieval_scope=retrieval_scope,
                source_role_budget_tokens=source_role_budget_tokens,
                source_role_budget_mode=source_role_budget_mode,
                memory_layer_budget_tokens=memory_layer_budget_tokens,
                memory_layer_budget_mode=memory_layer_budget_mode,
                memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
                memory_selection_policy_budget_mode=memory_selection_policy_budget_mode,
                extraction_phase_budget_tokens=extraction_phase_budget_tokens,
                extraction_phase_budget_mode=extraction_phase_budget_mode,
            )
        for scan_index, record in enumerate(reversed(_scan_source(_tree_records_by_type.get("context_entity", ()))), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_entity_scan", records)
            if record.get("record_type") != "context_entity":
                continue
            record = record_with_embedding_defaults(record, "entity", record.get("entity_hash"))
            is_profile_entity_bridge = (
                str(record.get("memory_scope") or "") == "user_profile"
                and str(record.get("session_continuity") or "") == "cross_session"
            )
            if not (
                access_scope_matches_before_scoring(record, retrieval_scope)
                or (is_profile_entity_bridge and bool(retrieval_scope.get("_allow_profile_bridge")))
            ):
                continue
            entity_metadata = embedding_metadata_by_ref.get(("entity", record.get("entity_hash")), {})
            profile_current_value = first_explicit_bool("profile_entity_current", record, entity_metadata)
            if is_profile_entity_bridge and profile_current_value is False:
                secondary_index_dropped_count += 1
                continue
            if not selected_by_tree(record) and not is_profile_entity_bridge:
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            profile_bridge_allowed = is_profile_entity_bridge and bool(retrieval_scope.get("_allow_profile_bridge"))
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                if not profile_bridge_allowed:
                    secondary_index_dropped_count += 1
                    continue
            else:
                secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
            entity_metadata = embedding_metadata_by_ref.get(("entity", record.get("entity_hash")), {})
            source_entity_hashes = record.get("source_entity_hashes", [])
            source_session_ids = record.get("source_session_ids", [])
            profile_memory_class = str(
                record.get("profile_memory_class")
                or entity_metadata.get("profile_memory_class")
                or (
                    profile_memory_class_for_entity_type(record.get("entity_type"))
                    if is_profile_entity_bridge
                    else ""
                )
            ).strip()
            profile_memory_kind = str(
                record.get("profile_memory_kind")
                or entity_metadata.get("profile_memory_kind")
                or (
                    profile_memory_kind_for_entity_type(record.get("entity_type"))
                    if is_profile_entity_bridge
                    else ""
                )
            ).strip()
            profile_class_term = context_index_name("profile_memory_class", profile_memory_class)
            profile_class_matched_query = bool(
                profile_class_term
                and any(profile_class_term in group for group in secondary_index_filter_groups)
            )
            profile_kind_term = context_index_name("profile_memory_kind", profile_memory_kind)
            profile_kind_matched_query = bool(
                profile_kind_term
                and any(profile_kind_term in group for group in secondary_index_filter_groups)
            )
            profile_feature_matched_query = bool(
                is_profile_entity_bridge
                and (
                    str(record.get("entity_type") or "").strip() == "memory_feature_profile"
                    or profile_memory_kind == "memory_feature"
                    or profile_memory_class == "memory_feature"
                    or any("memory_feature" in str(value or "") for value in record.get("source_memory_layers", []))
                )
                and (
                    any(context_index_name("entity_type", "memory_feature_profile") in group for group in secondary_index_filter_groups)
                    or profile_class_matched_query
                    or profile_kind_matched_query
                )
            )
            profile_text_parts = [
                profile_memory_class.replace("_", " "),
                profile_memory_kind.replace("_", " "),
                " ".join(str(value) for value in record.get("source_memory_layers", []) if str(value or "")),
                " ".join(str(value).replace("_", " ") for value in record.get("source_memory_layers", []) if str(value or "")),
                str(record.get("entity_type", "")),
                str(record.get("entity_name", "")),
                str(record.get("state", "")),
            ]
            profile_scoring_text = " ".join(part for part in profile_text_parts if part).strip()
            entity_hybrid_text = profile_scoring_text if is_profile_entity_bridge and profile_scoring_text else text
            sparse_score = sparse_lexical_score(query_terms, entity_hybrid_text)
            keyword_score = len(query_terms.intersection(tokens(entity_hybrid_text)))
            embedding_score = cosine(query_embedding, entity_embedding_vectors.get(record["entity_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(
                1.0,
                0.12
                + hybrid_origin_score(query_terms, entity_hybrid_text, embedding_score, node_score)
                + (0.14 if profile_class_matched_query else 0.0)
                + (0.12 if profile_kind_matched_query else 0.0)
                + (0.18 if profile_feature_matched_query else 0.0),
            )
            candidate = {
                "ref_type": "entity",
                "ref_hash": record["entity_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": (
                    "selected as class-matched cross-session user-profile entity bridge"
                        if is_profile_entity_bridge and profile_class_matched_query
                        else "selected as kind-matched cross-session user-profile entity bridge"
                        if is_profile_entity_bridge and profile_kind_matched_query
                        else "selected as cross-session user-profile entity bridge"
                    if is_profile_entity_bridge
                    else
                    "selected by tree path, secondary indexes, and resource entity state score"
                    if record.get("source_chunk_hash")
                    else "selected by tree path, secondary indexes, and entity state score"
                ),
                "entity_type": record.get("entity_type", ""),
                "entity_name": record.get("entity_name", ""),
                "profile_memory_class": profile_memory_class,
                "profile_memory_kind": profile_memory_kind,
                "context_class": "resource_entity_fact" if record.get("source_chunk_hash") else "entity",
                "source_chunk_hash": record.get("source_chunk_hash"),
                "source_ref": record.get("source_ref", ""),
                "source_roles": record.get("source_roles", []),
                "source_role_counts": record.get("source_role_counts", {}),
                "source_hook_types": record.get("source_hook_types", []),
                "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                "source_codex_events": record.get("source_codex_events", []),
                "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                "source_session_ids": source_session_ids,
                "source_event_ids": record.get("source_event_ids", []),
                "source_entity_hashes": source_entity_hashes,
                "source_memory_scopes": record.get("source_memory_scopes", []),
                "source_session_continuities": record.get("source_session_continuities", []),
                "source_extraction_phases": record.get("source_extraction_phases", []),
                "memory_scope": record.get("memory_scope", ""),
                "session_continuity": record.get("session_continuity", ""),
                "extraction_phase": record.get("extraction_phase", ""),
                "final_session_boundary": bool(record.get("final_session_boundary", False)),
                "profile_promotion_policy": record.get("profile_promotion_policy", ""),
                "profile_promotion_blocker": record.get("profile_promotion_blocker", ""),
                "profile_revision": record.get("profile_revision", entity_metadata.get("profile_revision", 0)),
                "previous_profile_revision": record.get("previous_profile_revision", entity_metadata.get("previous_profile_revision", 0)),
                "previous_profile_updated_at_ms": record.get("previous_profile_updated_at_ms", entity_metadata.get("previous_profile_updated_at_ms", 0)),
                "supersedes_session_entity_hash": record.get("supersedes_session_entity_hash", 0),
                "supersedes_session_entity_hashes": record.get("supersedes_session_entity_hashes", []),
                "profile_entity_current": (
                    profile_current_value
                    if profile_current_value is not None
                    else is_profile_entity_bridge
                ),
                "profile_current_state_representative": is_profile_entity_bridge,
                "current_state_source_session_count": len(source_session_ids) if isinstance(source_session_ids, list) else 0,
                "current_state_source_entity_count": len(source_entity_hashes) if isinstance(source_entity_hashes, list) else 0,
                "current_state_policy": (
                    "profile_entity_bridge_preferred_over_session_local_history"
                    if is_profile_entity_bridge
                    else ""
                ),
                "metadata": record.get("metadata", {}),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_entity_scan",
                budget_source=budget_source,
                retrieval_scope=retrieval_scope,
                source_role_budget_tokens=source_role_budget_tokens,
                source_role_budget_mode=source_role_budget_mode,
                memory_layer_budget_tokens=memory_layer_budget_tokens,
                memory_layer_budget_mode=memory_layer_budget_mode,
                memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
                memory_selection_policy_budget_mode=memory_selection_policy_budget_mode,
                extraction_phase_budget_tokens=extraction_phase_budget_tokens,
                extraction_phase_budget_mode=extraction_phase_budget_mode,
            )
        for scan_index, record in enumerate(reversed(_scan_source(_tree_records_by_type.get("context_segment", ()))), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_segment_scan", records)
            if record.get("record_type") != "context_segment":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            segment_layer_text = " ".join(
                [str(value) for value in record.get("source_memory_layers", []) if str(value or "")]
                + [str(value).replace("_", " ") for value in record.get("source_memory_layers", []) if str(value or "")]
            )
            text = " ".join(
                part
                for part in [f"{record.get('topic', '')}: {record.get('summary_text', '')}", segment_layer_text]
                if part.strip()
            )
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, segment_embedding_vectors.get(record["segment_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            saliency_score = float(record.get("saliency_score", 0.0))
            origin_score = min(
                1.0,
                0.1 + 0.75 * hybrid_origin_score(query_terms, text, embedding_score, node_score) + 0.15 * saliency_score,
            )
            candidate = {
                "ref_type": "segment",
                "ref_hash": record["segment_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": "selected by tree path, secondary indexes, segment saliency, and segment hybrid score",
                "saliency_score": saliency_score,
                "topic": record.get("topic", ""),
                "coordinate_tuples": record.get("coordinate_tuples", []),
                "non_contiguous": record.get("non_contiguous", False),
                "source_event_ids": record.get("source_event_ids", []),
                "source_event_count": int(record.get("source_event_count") or 0)
                or len(record.get("source_event_ids", []) or []),
                "source_record_type": record.get("source_record_type", ""),
                "segment_origin": record.get("segment_origin", ""),
                "derived_from_context_events": bool(record.get("derived_from_context_events", False)),
                "source_roles": record.get("source_roles", []),
                "source_role_counts": record.get("source_role_counts", {}),
                "source_hook_types": record.get("source_hook_types", []),
                "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                "source_codex_events": record.get("source_codex_events", []),
                "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
                "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
                "source_memory_selection_lossy_count": record.get("source_memory_selection_lossy_count", 0),
                "source_memory_selection_complete_count": record.get("source_memory_selection_complete_count", 0),
                "source_memory_selection_dropped_text_chars": record.get("source_memory_selection_dropped_text_chars", 0),
                "source_memory_selection_dropped_line_count": record.get("source_memory_selection_dropped_line_count", 0),
                "source_memory_selection_retained_text_ratio_avg": record.get("source_memory_selection_retained_text_ratio_avg", 1.0),
                "source_memory_selection_retained_line_ratio_avg": record.get("source_memory_selection_retained_line_ratio_avg", 1.0),
                "source_memory_scopes": record.get("source_memory_scopes", []),
                "source_session_continuities": record.get("source_session_continuities", []),
                "source_extraction_phases": record.get("source_extraction_phases", []),
                "memory_scope": record.get("memory_scope", ""),
                "session_continuity": record.get("session_continuity", ""),
                "extraction_phase": record.get("extraction_phase", ""),
                "final_session_boundary": bool(record.get("final_session_boundary", False)),
                "profile_promotion_policy": record.get("profile_promotion_policy", ""),
                "profile_promotion_blocker": record.get("profile_promotion_blocker", ""),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(str(record.get("summary_text", ""))),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [record.get("topic", ""), text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_segment_scan",
                budget_source=budget_source,
                retrieval_scope=retrieval_scope,
                source_role_budget_tokens=source_role_budget_tokens,
                source_role_budget_mode=source_role_budget_mode,
                memory_layer_budget_tokens=memory_layer_budget_tokens,
                memory_layer_budget_mode=memory_layer_budget_mode,
                memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
                memory_selection_policy_budget_mode=memory_selection_policy_budget_mode,
                extraction_phase_budget_tokens=extraction_phase_budget_tokens,
                extraction_phase_budget_mode=extraction_phase_budget_mode,
            )
        for scan_index, record in enumerate(reversed(_scan_source(_tree_resource_skill_source)), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_resource_skill_scan", records)
            if record.get("record_type") not in {"resource_chunk", "skill_section"}:
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            if record.get("record_type") == "resource_chunk" and record.get("resource_type") == "skill":
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            if record.get("record_type") == "skill_section":
                ref_type = "skill_section"
                ref_hash = int(record.get("section_hash") or 0)
                parent_skill_hash = int(record.get("skill_hash") or 0)
                control = skill_controls.get(parent_skill_hash, {})
                if str(control.get("status") or "active") != "active":
                    continue
                resource_hash = parent_skill_hash
                raw_uri_value = str(record.get("raw_uri") or "")
                source_locator = str(record.get("source_locator") or "")
                citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
                resource_version_value = str(record.get("metadata", {}).get("resource_version") or record.get("resource_version") or "")
                version_state = "current"
                is_superseded_version = False
                text = f"skill section {record.get('heading', '')}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = "skill"
                metadata = {**record.get("metadata", {}), "skill_registry": control}
            else:
                ref_type = "resource_chunk"
                ref_hash = int(record.get("chunk_hash") or 0)
                metadata = record.get("metadata", {})
                resource_hash = int(record.get("resource_hash") or 0)
                raw_uri_value = str(record.get("raw_uri") or resource_uri_by_hash.get(resource_hash, ""))
                source_locator = str(record.get("source_locator") or metadata.get("source_locator") or "")
                citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
                resource_version_value = str(metadata.get("resource_version") or record.get("resource_version") or "")
                latest_version = latest_resource_version_by_hash.get(resource_hash, resource_version_value)
                is_superseded_version = bool(
                    resource_version_value
                    and latest_version
                    and resource_version_value != latest_version
                )
                if is_superseded_version and not include_superseded_resources:
                    secondary_index_dropped_count += 1
                    continue
                version_state = "historical" if is_superseded_version else "current"
                text = f"resource {source_locator}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = str(record.get("resource_type") or "resource")
            sharing_scope = sharing_scope_from_candidate(record)
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            if origin_score <= 0:
                continue
            primary_matches.append(
                score_recall_candidate(
                    annotate_session_continuity({
                        "ref_type": ref_type,
                        "ref_hash": ref_hash,
                        "node_hash": record.get("node_hash"),
                        "node_path": record.get("node_path", []),
                        "origin_score": origin_score,
                        "keyword_score": keyword_score,
                        "sparse_score": sparse_score,
                        "embedding_score": embedding_score,
                        "node_score": node_score,
                        "matched_index_terms": sorted(index_terms),
                        "selection_reason": (
                            "selected by tree path, secondary indexes, and resource/skill hybrid score"
                            if index_terms
                            else "selected by tree path and resource/skill hybrid score"
                        ),
                        "event_type": business_type,
                        "context_class": ref_type,
                        "resource_hash": resource_hash,
                        "source_locator": source_locator,
                        "resource_type": record.get("resource_type", ""),
                        "resource_version": resource_version_value,
                        "supersedes_chunk_hash": metadata.get("supersedes_chunk_hash"),
                        "version_state": version_state,
                        "stale_or_superseded": is_superseded_version,
                        "access_decision": "allowed_by_registry_scope_before_scoring",
                        "access_scope": candidate_access_scope(record),
                        "sharing_scope": sharing_scope,
                        "deployment_scope": record.get("deployment_scope", "local"),
                        "citation": citation,
                        "metadata": metadata,
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", now_ms()),
                        "text": clip_context_text(text),
                        "recall_path": "primary_resource_skill",
                    }, record),
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )

        for scan_index, record in enumerate(reversed(_scan_source(_tree_records_by_type.get("context_compression_event", ()))), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_compression_scan", records)
            if record.get("record_type") != "context_compression_event":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = f"TIME_COMPRESS: {summarize_text(str(record.get('summary_text', '')), limit=96)}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            compression_hash = int(record.get("compression_id_hash") or 0)
            embedding_score = cosine(query_embedding, compression_embedding_vectors.get(compression_hash, embedding_for_text(text)))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "compression",
                "ref_hash": compression_hash,
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "event_type": "time_compress",
                "operator": "TIME_COMPRESS",
                "context_class": "compression",
                "source_event_ids": record.get("source_event_ids", []),
                "source_event_count": record.get("source_event_count", 0),
                "source_start_ms": record.get("source_start_ms"),
                "source_end_ms": record.get("source_end_ms"),
                "memory_scope": record.get("memory_scope", ""),
                "session_continuity": record.get("session_continuity", ""),
                "extraction_phase": record.get("extraction_phase", ""),
                "final_session_boundary": bool(record.get("final_session_boundary")),
                "source_roles": record.get("source_roles", []),
                "source_role_counts": record.get("source_role_counts", {}),
                "source_hook_types": record.get("source_hook_types", []),
                "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                "source_codex_events": record.get("source_codex_events", []),
                "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
                "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
                "source_memory_selection_lossy_count": record.get("source_memory_selection_lossy_count", 0),
                "source_memory_selection_complete_count": record.get("source_memory_selection_complete_count", 0),
                "source_memory_selection_dropped_text_chars": record.get("source_memory_selection_dropped_text_chars", 0),
                "source_memory_selection_dropped_line_count": record.get("source_memory_selection_dropped_line_count", 0),
                "source_memory_selection_retained_text_ratio_avg": record.get("source_memory_selection_retained_text_ratio_avg", 1.0),
                "source_memory_selection_retained_line_ratio_avg": record.get("source_memory_selection_retained_line_ratio_avg", 1.0),
                "source_memory_scopes": record.get("source_memory_scopes", []),
                "source_session_continuities": record.get("source_session_continuities", []),
                "source_extraction_phases": record.get("source_extraction_phases", []),
                "source_profile_promotion_policies": record.get("source_profile_promotion_policies", []),
                "source_profile_promotion_blockers": record.get("source_profile_promotion_blockers", []),
                "source_profile_memory_classes": record.get("source_profile_memory_classes", []),
                "source_profile_memory_kinds": record.get("source_profile_memory_kinds", []),
                "profile_memory_class": record.get("profile_memory_class", ""),
                "profile_memory_kind": record.get("profile_memory_kind", ""),
                "source_final_session_boundary_count": record.get("source_final_session_boundary_count", 0),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_time_compression"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text, "time_compress"]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if any(
            record.get("record_type") == "context_entity"
            and str(record.get("memory_scope") or "") == "user_profile"
            and str(record.get("session_continuity") or "") == "cross_session"
            for record in records
        ):
            existing_profile_entity_hashes = {
                item.get("ref_hash")
                for item in primary_matches + auxiliary_matches
                if item.get("ref_type") == "entity"
                and item.get("memory_scope") == "user_profile"
                and item.get("session_continuity") == "cross_session"
            }
            for record in records:
                if record.get("record_type") != "context_entity":
                    continue
                if record.get("entity_hash") in existing_profile_entity_hashes:
                    continue
                record = record_with_embedding_defaults(record, "entity", record.get("entity_hash"))
                if str(record.get("memory_scope") or "") != "user_profile":
                    continue
                if str(record.get("session_continuity") or "") != "cross_session":
                    continue
                entity_metadata = embedding_metadata_by_ref.get(("entity", record.get("entity_hash")), {})
                profile_current_value = first_explicit_bool("profile_entity_current", record, entity_metadata)
                if profile_current_value is False:
                    continue
                text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
                if not text.strip(" :=	"):
                    continue
                source_entity_hashes = record.get("source_entity_hashes", [])
                source_session_ids = record.get("source_session_ids", [])
                profile_memory_class = str(
                    record.get("profile_memory_class")
                    or entity_metadata.get("profile_memory_class")
                    or profile_memory_class_for_entity_type(record.get("entity_type"))
                ).strip()
                profile_memory_kind = str(
                    record.get("profile_memory_kind")
                    or entity_metadata.get("profile_memory_kind")
                    or profile_memory_kind_for_entity_type(record.get("entity_type"))
                ).strip()
                profile_scoring_text = " ".join(
                    part
                    for part in [
                        profile_memory_class.replace("_", " "),
                        profile_memory_kind.replace("_", " "),
                        str(record.get("entity_type", "")),
                        str(record.get("entity_name", "")),
                        str(record.get("state", "")),
                    ]
                    if part
                )
                sparse_score = sparse_lexical_score(query_terms, text)
                keyword_score = len(query_terms.intersection(tokens(text)))
                embedding_score = cosine(query_embedding, entity_embedding_vectors.get(record.get("entity_hash"), []))
                node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
                origin_score = min(1.0, 0.28 + hybrid_origin_score(query_terms, profile_scoring_text or text, embedding_score, node_score))
                candidate = annotate_session_continuity(
                    {
                        "ref_type": "entity",
                        "ref_hash": record.get("entity_hash"),
                        "node_hash": record.get("node_hash"),
                        "node_path": record.get("node_path", []),
                        "origin_score": origin_score,
                        "keyword_score": keyword_score,
                        "sparse_score": sparse_score,
                        "embedding_score": embedding_score,
                        "node_score": node_score,
                        "matched_index_terms": sorted(candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)),
                        "selection_reason": "selected by direct cross-session user-profile entity bridge",
                        "entity_type": record.get("entity_type", ""),
                        "entity_name": record.get("entity_name", ""),
                        "profile_memory_class": profile_memory_class,
                        "profile_memory_kind": profile_memory_kind,
                        "context_class": "entity",
                        "source_roles": record.get("source_roles", []),
                        "source_role_counts": record.get("source_role_counts", {}),
                        "source_hook_types": record.get("source_hook_types", []),
                        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                        "source_codex_events": record.get("source_codex_events", []),
                        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                        "source_session_ids": source_session_ids,
                        "source_event_ids": record.get("source_event_ids", []),
                        "source_entity_hashes": source_entity_hashes,
                        "source_memory_scopes": record.get("source_memory_scopes", []),
                        "source_session_continuities": record.get("source_session_continuities", []),
                        "source_extraction_phases": record.get("source_extraction_phases", []),
                        "memory_scope": record.get("memory_scope", ""),
                        "session_continuity": record.get("session_continuity", ""),
                        "extraction_phase": record.get("extraction_phase", ""),
                        "final_session_boundary": bool(record.get("final_session_boundary", False)),
                        "profile_promotion_policy": record.get("profile_promotion_policy", ""),
                        "profile_promotion_blocker": record.get("profile_promotion_blocker", ""),
                        "profile_revision": record.get("profile_revision", entity_metadata.get("profile_revision", 0)),
                        "previous_profile_revision": record.get("previous_profile_revision", entity_metadata.get("previous_profile_revision", 0)),
                        "previous_profile_updated_at_ms": record.get("previous_profile_updated_at_ms", entity_metadata.get("previous_profile_updated_at_ms", 0)),
                        "supersedes_session_entity_hash": record.get("supersedes_session_entity_hash", 0),
                        "supersedes_session_entity_hashes": record.get("supersedes_session_entity_hashes", []),
                        "profile_entity_current": profile_current_value if profile_current_value is not None else True,
                        "profile_current_state_representative": True,
                        "current_state_source_session_count": len(source_session_ids) if isinstance(source_session_ids, list) else 0,
                        "current_state_source_entity_count": len(source_entity_hashes) if isinstance(source_entity_hashes, list) else 0,
                        "current_state_policy": "profile_entity_bridge_preferred_over_session_local_history",
                        "metadata": record.get("metadata", {}),
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", now_ms()),
                        "text": clip_context_text(text),
                        "recall_path": "direct_profile_entity_bridge",
                    },
                    record,
                )
                primary_matches.append(score_recall_candidate(candidate, ranking, reference_time_ms=reference_time_ms))
                existing_profile_entity_hashes.add(record.get("entity_hash"))
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_compression_scan",
                budget_source=budget_source,
                retrieval_scope=retrieval_scope,
                source_role_budget_tokens=source_role_budget_tokens,
                source_role_budget_mode=source_role_budget_mode,
                memory_layer_budget_tokens=memory_layer_budget_tokens,
                memory_layer_budget_mode=memory_layer_budget_mode,
                memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
                memory_selection_policy_budget_mode=memory_selection_policy_budget_mode,
                extraction_phase_budget_tokens=extraction_phase_budget_tokens,
                extraction_phase_budget_mode=extraction_phase_budget_mode,
            )
        finish_retrieval_stage("rerank_score", stage_started_perf)
        stage_started_perf = time.perf_counter()
        primary_matches.sort(key=lambda item: item["score"], reverse=True)
        auxiliary_matches.sort(key=lambda item: item["score"], reverse=True)
        selected_ref_cap = max(1, int(max_selected_refs or DEFAULT_MAX_SELECTED_REFS))
        rerank_candidate_limit = max(selected_ref_cap, max_global_candidates)
        first_stage_candidate_count = len(primary_matches) + len(auxiliary_matches)
        rerank_policy = {
            "enabled": True,
            "stage": "packing_rerank",
            "mode": "question_type_token_efficiency",
            "input_candidate_count": first_stage_candidate_count,
            "max_candidates": rerank_candidate_limit,
            "reranked_candidate_count": min(first_stage_candidate_count, rerank_candidate_limit),
            "question_type": question_type,
            "signals": [
                "weighted_recall_score",
                "question_type_ref_boost",
                "cross_session_rerank_boost",
                "token_efficiency",
                "multi_hop_node_diversity",
            ],
            "cross_session_rerank_enabled": True,
            "cross_session_signals": ["entity_state", "resource_fact_citation", "answer_event", "compression", "summary_demotion"],
            "fallback": "weighted_recall",
            "heavy_rerank_enabled": False,
            "min_similarity_score": min_similarity_score,
            "budget_fill_policy": budget_fill_policy,
        }
        selected, used_context_tokens, dropped_over_budget = select_token_budgeted_refs(
            primary_matches,
            auxiliary_matches,
            max_context_tokens=remote_context_budget_tokens,
            auxiliary_quota=auxiliary_quota,
            question_type=question_type,
            reserved_tokens=0,
            max_selected_refs=max_selected_refs,
            min_score=min_similarity_score,
            max_global_candidates=max_global_candidates,
            budget_fill_policy=budget_fill_policy,
            duplicate_text_hashes=local_budget["text_hashes"],
            deadline_exceeded=deadline_exceeded,
            deadline_reason="deadline_during_context_pack",
            cross_session_policy=cross_session_policy,
            shared_context_policy=shared_context_policy,
            source_role_budget_tokens=source_role_budget_tokens,
            memory_layer_budget_tokens=memory_layer_budget_tokens,
            memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
            extraction_phase_budget_tokens=extraction_phase_budget_tokens,
        )
        if retrieval_session_scope == "only":
            selected = [
                item
                for item in selected
                if str(item.get("memory_scope") or "").strip().lower()
                not in {"user_profile", "profile", "cross_session_profile"}
                and str(item.get("session_continuity") or "").strip().lower() != "cross_session"
            ]
            used_context_tokens = sum(max(1, token_count(str(item.get("text", "")))) for item in selected)
        if (
            (
                bool(cross_session_policy.get("enabled"))
                or (
                    retrieval_session_scope == "prefer"
                    and question_type in {"current_state", "latest", "profile_memory"}
                )
            )
            and (
                int(cross_session_policy.get("min_entity_bridge_refs") or 0) > 0
                or question_type in {"current_state", "latest", "profile_memory"}
            )
            and not any(
                item.get("ref_type") == "entity"
                and item.get("memory_scope") == "user_profile"
                and item.get("session_continuity") == "cross_session"
                for item in selected
            )
        ):
            profile_candidates = [
                item
                for item in merge_ranked_paths(
                    primary_matches,
                    auxiliary_matches,
                    total_limit=max_global_candidates,
                    auxiliary_quota=auxiliary_quota,
                )
                if item.get("ref_type") == "entity"
                and item.get("memory_scope") == "user_profile"
                and item.get("session_continuity") == "cross_session"
                and float(item.get("score", 0.0)) >= min_similarity_score
            ]
            if not profile_candidates:
                profile_bridge_records = []
                seen_profile_bridge_records = set()
                for candidate_record in list(records) + list(tree_candidate_records):
                    candidate_key = (
                        candidate_record.get("record_type"),
                        candidate_record.get("entity_hash"),
                        candidate_record.get("node_hash"),
                    )
                    if candidate_key in seen_profile_bridge_records:
                        continue
                    seen_profile_bridge_records.add(candidate_key)
                    profile_bridge_records.append(candidate_record)
                for record in profile_bridge_records:
                    if record.get("record_type") != "context_entity":
                        continue
                    record = record_with_embedding_defaults(record, "entity", record.get("entity_hash"))
                    if str(record.get("memory_scope") or "") != "user_profile":
                        continue
                    if str(record.get("session_continuity") or "") != "cross_session":
                        continue
                    if not (
                        access_scope_matches_before_scoring(record, retrieval_scope)
                        or bool(retrieval_scope.get("_allow_profile_bridge"))
                    ):
                        continue
                    entity_metadata = embedding_metadata_by_ref.get(("entity", record.get("entity_hash")), {})
                    profile_current_value = first_explicit_bool("profile_entity_current", record, entity_metadata)
                    if profile_current_value is False:
                        continue
                    text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
                    if not text.strip(" :=\t"):
                        continue
                    source_entity_hashes = record.get("source_entity_hashes", [])
                    source_session_ids = record.get("source_session_ids", [])
                    profile_memory_class = str(
                        record.get("profile_memory_class")
                        or entity_metadata.get("profile_memory_class")
                        or profile_memory_class_for_entity_type(record.get("entity_type"))
                    ).strip()
                    profile_memory_kind = str(
                        record.get("profile_memory_kind")
                        or entity_metadata.get("profile_memory_kind")
                        or profile_memory_kind_for_entity_type(record.get("entity_type"))
                    ).strip()
                    profile_class_term = context_index_name("profile_memory_class", profile_memory_class)
                    profile_class_matched_query = bool(
                        profile_class_term
                        and any(profile_class_term in group for group in secondary_index_filter_groups)
                    )
                    profile_kind_term = context_index_name("profile_memory_kind", profile_memory_kind)
                    profile_kind_matched_query = bool(
                        profile_kind_term
                        and any(profile_kind_term in group for group in secondary_index_filter_groups)
                    )
                    sparse_score = sparse_lexical_score(query_terms, text)
                    keyword_score = len(query_terms.intersection(tokens(text)))
                    embedding_score = cosine(query_embedding, entity_embedding_vectors.get(record.get("entity_hash"), []))
                    node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
                    profile_scoring_text = " ".join(
                        part
                        for part in [
                            profile_memory_class.replace("_", " "),
                            profile_memory_kind.replace("_", " "),
                            str(record.get("entity_type", "")),
                            str(record.get("entity_name", "")),
                            str(record.get("state", "")),
                        ]
                        if part
                    )
                    origin_score = min(
                        1.0,
                        0.28
                        + hybrid_origin_score(query_terms, profile_scoring_text or text, embedding_score, node_score)
                        + (0.14 if profile_class_matched_query else 0.0)
                        + (0.12 if profile_kind_matched_query else 0.0),
                    )
                    if origin_score <= 0:
                        continue
                    candidate = annotate_session_continuity(
                        {
                            "ref_type": "entity",
                            "ref_hash": record.get("entity_hash"),
                            "node_hash": record.get("node_hash"),
                            "node_path": record.get("node_path", []),
                            "origin_score": origin_score,
                            "keyword_score": keyword_score,
                            "sparse_score": sparse_score,
                            "embedding_score": embedding_score,
                            "node_score": node_score,
                            "matched_index_terms": [],
                            "selection_reason": "selected by direct cross-session user-profile entity bridge",
                            "entity_type": record.get("entity_type", ""),
                            "entity_name": record.get("entity_name", ""),
                            "profile_memory_class": profile_memory_class,
                            "profile_memory_kind": profile_memory_kind,
                            "context_class": "entity",
                            "source_roles": record.get("source_roles", []),
                            "source_role_counts": record.get("source_role_counts", {}),
                            "source_hook_types": record.get("source_hook_types", []),
                            "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                            "source_codex_events": record.get("source_codex_events", []),
                            "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                            "source_session_ids": source_session_ids,
                            "source_event_ids": record.get("source_event_ids", []),
                            "source_entity_hashes": source_entity_hashes,
                            "source_memory_scopes": record.get("source_memory_scopes", []),
                            "source_session_continuities": record.get("source_session_continuities", []),
                            "source_extraction_phases": record.get("source_extraction_phases", []),
                            "memory_scope": record.get("memory_scope", ""),
                            "session_continuity": record.get("session_continuity", ""),
                            "extraction_phase": record.get("extraction_phase", ""),
                            "final_session_boundary": bool(record.get("final_session_boundary", False)),
                            "profile_promotion_policy": record.get("profile_promotion_policy", ""),
                            "profile_promotion_blocker": record.get("profile_promotion_blocker", ""),
                            "profile_revision": record.get("profile_revision", entity_metadata.get("profile_revision", 0)),
                            "previous_profile_revision": record.get("previous_profile_revision", entity_metadata.get("previous_profile_revision", 0)),
                            "previous_profile_updated_at_ms": record.get("previous_profile_updated_at_ms", entity_metadata.get("previous_profile_updated_at_ms", 0)),
                            "supersedes_session_entity_hash": record.get("supersedes_session_entity_hash", 0),
                            "supersedes_session_entity_hashes": record.get("supersedes_session_entity_hashes", []),
                            "profile_entity_current": (
                                profile_current_value
                                if profile_current_value is not None
                                else True
                            ),
                            "profile_current_state_representative": True,
                            "current_state_source_session_count": len(source_session_ids) if isinstance(source_session_ids, list) else 0,
                            "current_state_source_entity_count": len(source_entity_hashes) if isinstance(source_entity_hashes, list) else 0,
                            "current_state_policy": "profile_entity_bridge_preferred_over_session_local_history",
                            "metadata": record.get("metadata", {}),
                            "scope": candidate_access_scope(record),
                            "updated_at_ms": record.get("updated_at_ms", now_ms()),
                            "text": clip_context_text(text),
                            "recall_path": "direct_profile_entity_bridge",
                        },
                        record,
                    )
                    scored = score_recall_candidate(candidate, ranking, reference_time_ms=reference_time_ms)
                    if float(scored.get("score", 0.0)) >= min_similarity_score:
                        profile_candidates.append(scored)
            profile_candidates.sort(key=lambda item: packing_sort_key(item, question_type), reverse=True)
            for candidate in profile_candidates:
                if context_text_hashes(str(candidate.get("text", ""))).intersection(local_budget["text_hashes"]):
                    continue
                ref_tokens = max(1, token_count(str(candidate.get("text", ""))))
                while selected and (
                    len(selected) >= max_selected_refs
                    or used_context_tokens + ref_tokens > remote_context_budget_tokens
                    or (
                        question_type in {"current_state", "latest"}
                        and any(item.get("ref_type") == "summary" for item in selected)
                    )
                ):
                    removable_index = next(
                        (
                            index
                            for index in range(len(selected) - 1, -1, -1)
                            if selected[index].get("ref_type") in {"summary", "event", "segment"}
                            and not bool(selected[index].get("profile_current_state_representative"))
                        ),
                        None,
                    )
                    if removable_index is None:
                        break
                    removed = selected.pop(removable_index)
                    removed_tokens = max(1, token_count(str(removed.get("text", ""))))
                    used_context_tokens = max(0, used_context_tokens - removed_tokens)
                    dropped_over_budget.setdefault("profile_entity_bridge_replaced_refs", 0)
                    dropped_over_budget["profile_entity_bridge_replaced_refs"] += 1
                    record_dropped_candidate(
                        dropped_over_budget,
                        removed,
                        reason="memory_layer_floor",
                        token_estimate=removed_tokens,
                    )
                if len(selected) >= max_selected_refs or used_context_tokens + ref_tokens > remote_context_budget_tokens:
                    continue
                candidate_entity_type = str(candidate.get("entity_type") or "").strip().lower()
                candidate_role_names = {
                    normalize_message_role(role_name)
                    for role_name in candidate.get("source_roles", []) or []
                    if normalize_message_role(role_name)
                }
                for role_name in (candidate.get("source_role_counts", {}) or {}).keys():
                    normalized_role_name = normalize_message_role(role_name)
                    if normalized_role_name:
                        candidate_role_names.add(normalized_role_name)
                candidate_budget_role = semantic_source_role_for_entity_type(candidate_entity_type, candidate_role_names)
                selected.append(
                    {
                        **candidate,
                        "token_estimate": ref_tokens,
                        "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                        "packing_policy": question_type,
                        "budget_memory_layer": candidate_memory_layer_name(candidate),
                        "budget_source_roles": [candidate_budget_role] if candidate_budget_role else [],
                        "budget_source_role_counts": {candidate_budget_role: 1} if candidate_budget_role else {},
                    }
                )
                used_context_tokens += ref_tokens
                dropped_over_budget.setdefault("profile_entity_bridge_injected", True)
                break
        profile_entity_texts_by_role: dict[str, list[str]] = {}
        for item in selected:
            if (
                item.get("ref_type") == "entity"
                and item.get("memory_scope") == "user_profile"
                and item.get("session_continuity") == "cross_session"
                and bool(item.get("profile_current_state_representative"))
            ):
                role_names = set()
                for role_name in item.get("source_roles", []) or []:
                    normalized_role = normalize_message_role(role_name)
                    if normalized_role:
                        role_names.add(normalized_role)
                for role_name in (item.get("source_role_counts", {}) or {}).keys():
                    normalized_role = normalize_message_role(role_name)
                    if normalized_role:
                        role_names.add(normalized_role)
                item_text = str(item.get("text") or "").lower()
                for role_name in role_names:
                    profile_entity_texts_by_role.setdefault(role_name, []).append(item_text)
        if profile_entity_texts_by_role:
            deduped_selected: list[Json] = []
            removed_tokens = 0
            for item in selected:
                if item.get("ref_type") != "event":
                    deduped_selected.append(item)
                    continue
                role_names = set()
                for role_name in item.get("source_roles", []) or []:
                    normalized_role = normalize_message_role(role_name)
                    if normalized_role:
                        role_names.add(normalized_role)
                for role_name in (item.get("source_role_counts", {}) or {}).keys():
                    normalized_role = normalize_message_role(role_name)
                    if normalized_role:
                        role_names.add(normalized_role)
                event_text = str(item.get("text") or "").lower()
                represented_by_profile = any(
                    event_text
                    and role_name in profile_entity_texts_by_role
                    and any(event_text in profile_text or profile_text in event_text for profile_text in profile_entity_texts_by_role[role_name])
                    for role_name in role_names
                )
                if represented_by_profile:
                    removed_tokens += max(1, token_count(str(item.get("text") or "")))
                    dropped_over_budget.setdefault("profile_entity_represented_events", 0)
                    dropped_over_budget["profile_entity_represented_events"] += 1
                    continue
                deduped_selected.append(item)
            if len(deduped_selected) != len(selected):
                selected = deduped_selected
                used_context_tokens = max(0, used_context_tokens - removed_tokens)

        selected, removed_shadowed_tokens = suppress_profile_shadowed_session_entities(selected, dropped_over_budget)
        if removed_shadowed_tokens:
            used_context_tokens = max(0, used_context_tokens - removed_shadowed_tokens)

        selected, removed_overlapping_profile_tokens = suppress_overlapping_profile_current_entities(selected, dropped_over_budget)
        if removed_overlapping_profile_tokens:
            used_context_tokens = max(0, used_context_tokens - removed_overlapping_profile_tokens)

        selected, removed_pending_tokens = suppress_extracted_represented_pending_events(selected, dropped_over_budget)
        if removed_pending_tokens:
            used_context_tokens = max(0, used_context_tokens - removed_pending_tokens)

        if (
            question_type in {"broad_exploration", "evidence", "current_state", "latest", "multi_hop", "date", "profile_memory"}
            and bool(pre_retrieval_summary_refresh.get("enabled"))
            and not any(
                item.get("ref_type") == "summary"
                and item.get("memory_scope") == "user_profile"
                and item.get("session_continuity") == "cross_session"
                for item in selected
            )
        ):
            refreshed_summary_candidates: list[Json] = []
            for record in records:
                if record.get("record_type") != "context_summary":
                    continue
                summary_ref_hash = record.get("summary_hash") or record.get("node_hash")
                embedding_metadata = embedding_metadata_by_ref.get(("summary", summary_ref_hash), {})
                recovered_memory_scope = str(record.get("memory_scope") or embedding_metadata.get("memory_scope") or "")
                recovered_session_continuity = str(
                    record.get("session_continuity") or embedding_metadata.get("session_continuity") or ""
                )
                if recovered_memory_scope != "user_profile":
                    continue
                if recovered_session_continuity != "cross_session":
                    continue
                if str(record.get("summary_type") or "") not in {"node_l0", "node_l1", "batch_l0", "session_l0", "session_final"}:
                    continue
                if not recovered_scope_matches(record, retrieval_scope):
                    continue
                summary_text = str(record.get("summary_text") or "")
                if not summary_text:
                    continue
                lineage_text = " ".join(
                    [
                        recovered_memory_scope,
                        recovered_session_continuity,
                        *[
                            str(value)
                            for value in (record.get("source_roles") or embedding_metadata.get("source_roles") or [])
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (record.get("source_hook_types") or embedding_metadata.get("source_hook_types") or [])
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (record.get("source_codex_events") or embedding_metadata.get("source_codex_events") or [])
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (
                                record.get("source_memory_selection_policies")
                                or embedding_metadata.get("source_memory_selection_policies")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[
                            str(value).replace("_", " ")
                            for value in (
                                record.get("source_memory_selection_policies")
                                or embedding_metadata.get("source_memory_selection_policies")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (record.get("source_memory_scopes") or embedding_metadata.get("source_memory_scopes") or [])
                            if str(value or "")
                        ],
                        *[
                            str(value)
                            for value in (
                                record.get("source_session_continuities")
                                or embedding_metadata.get("source_session_continuities")
                                or []
                            )
                            if str(value or "")
                        ],
                        *[str(value) for value in record.get("source_entity_types", []) if str(value or "")],
                        *[str(value).replace("_", " ") for value in record.get("source_entity_types", []) if str(value or "")],
                    ]
                )
                summary_filter_text = " ".join([summary_text, summary_text.replace("_", " "), lineage_text])
                text_score = sparse_lexical_score(query_terms, summary_filter_text)
                lineage_score = sparse_lexical_score(query_terms, lineage_text)
                if text_score <= 0 and lineage_score <= 0:
                    continue
                candidate = annotate_session_continuity(
                    {
                        "ref_type": "summary",
                        "ref_hash": summary_ref_hash,
                        "node_hash": record.get("node_hash"),
                        "node_path": record.get("node_path", []),
                        "origin_score": min(1.0, 0.72 + 0.18 * text_score + 0.10 * lineage_score),
                        "keyword_score": len(query_terms.intersection(tokens(summary_text))),
                        "sparse_score": text_score,
                        "source_lineage_score": lineage_score,
                        "embedding_score": 0.0,
                        "node_score": node_scores.get(record.get("node_hash"), {}).get("score", 0.0),
                        "matched_index_terms": [],
                        "selection_reason": "pre-retrieval refreshed profile summary bridge",
                        "event_type": record.get("summary_type"),
                        "context_class": "summary",
                        "summary_type": record.get("summary_type"),
                        "source_roles": record.get("source_roles", []),
                        "source_role_counts": record.get("source_role_counts", {}),
                        "source_hook_types": record.get("source_hook_types", []),
                        "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                        "source_codex_events": record.get("source_codex_events", []),
                        "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                        "source_memory_selection_policies": record.get("source_memory_selection_policies", []),
                        "source_memory_selection_policy_counts": record.get("source_memory_selection_policy_counts", {}),
                        "source_memory_selection_lossy_count": record.get("source_memory_selection_lossy_count", 0),
                        "source_memory_selection_complete_count": record.get("source_memory_selection_complete_count", 0),
                        "source_memory_selection_dropped_text_chars": record.get("source_memory_selection_dropped_text_chars", 0),
                        "source_memory_selection_dropped_line_count": record.get("source_memory_selection_dropped_line_count", 0),
                        "source_memory_selection_retained_text_ratio_avg": record.get("source_memory_selection_retained_text_ratio_avg", 1.0),
                        "source_memory_selection_retained_line_ratio_avg": record.get("source_memory_selection_retained_line_ratio_avg", 1.0),
                        "source_memory_scopes": record.get("source_memory_scopes", []),
                        "source_session_continuities": record.get("source_session_continuities", []),
                        "source_extraction_phases": record.get("source_extraction_phases", []),
                        "source_final_session_boundary_count": record.get("source_final_session_boundary_count", 0),
                        "memory_scope": record.get("memory_scope") or embedding_metadata.get("memory_scope", ""),
                        "session_continuity": record.get("session_continuity") or embedding_metadata.get("session_continuity", ""),
                        "extraction_phase": record.get("extraction_phase") or embedding_metadata.get("extraction_phase", ""),
                        "final_session_boundary": bool(
                            record.get("final_session_boundary", embedding_metadata.get("final_session_boundary", False))
                        ),
                        "access_decision": "allowed_by_registry_scope_before_scoring",
                        "access_scope": candidate_access_scope(record),
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", now_ms()),
                        "text": clip_context_text(summary_text),
                        "recall_path": "pre_retrieval_refreshed_profile_summary",
                    },
                    record,
                )
                refreshed_summary_candidates.append(score_recall_candidate(candidate, ranking, reference_time_ms=reference_time_ms))
            refreshed_summary_candidates.sort(key=lambda item: packing_sort_key(item, question_type), reverse=True)
            for candidate in refreshed_summary_candidates:
                ref_tokens = max(1, token_count(str(candidate.get("text", ""))))
                if len(selected) >= selected_ref_cap or used_context_tokens + ref_tokens > remote_context_budget_tokens:
                    removable_index = next(
                        (
                            index
                            for index in range(len(selected) - 1, -1, -1)
                            if selected[index].get("ref_type") in {"event", "segment"}
                            and not bool(selected[index].get("profile_current_state_representative"))
                        ),
                        None,
                    )
                    if removable_index is None:
                        continue
                    removed = selected.pop(removable_index)
                    used_context_tokens = max(0, used_context_tokens - max(1, token_count(str(removed.get("text", "")))))
                if used_context_tokens + ref_tokens > remote_context_budget_tokens:
                    continue
                selected.append(
                    {
                        **candidate,
                        "token_estimate": ref_tokens,
                        "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                        "packing_policy": question_type,
                        "budget_memory_layer": (
                            "profile_summary"
                            if candidate.get("memory_scope") == "user_profile"
                            and candidate.get("session_continuity") == "cross_session"
                            else "summary"
                        ),
                    }
                )
                used_context_tokens += ref_tokens
                dropped_over_budget.setdefault("pre_retrieval_summary_refresh", {})["injected_profile_summary_ref"] = True
                break

        partial_context_pack = bool(dropped_over_budget.get("deadline_exceeded"))
        request_metadata = optional_object(args, "metadata")
        quality_warnings = []
        if partial_context_pack:
            quality_warnings.append(f"retrieval_deadline_exceeded:{dropped_over_budget.get('deadline_reason', 'deadline_during_context_pack')}")
        quality_warnings.extend(async_pipeline_readiness.get("freshness_warnings", []))
        session_id_source = str(
            request_metadata.get("session_id_source")
            or request_metadata.get("codex_session_id_source")
            or ""
        )
        session_identity_policy = codex_session_identity_policy(session_id_source)
        if session_identity_policy["fallback_session_identity"]:
            quality_warnings.append(f"session_identity_fallback:{session_id_source}")
        context_pack_id = stable_hash(f"{query}:{selected}:{now_ms()}")
        context_pack_id_text = str(context_pack_id)
        recall_reinforcement_enabled = bool(ranking.get("recall_reinforcement", True))
        if recall_reinforcement_enabled:
            reinforcement = self.append_recall_reinforcement_markers(
                context_pack_id=context_pack_id_text,
                selected_refs=selected,
                reinforced_at_ms=now_ms(),
            )
        else:
            reinforcement = {
                "reinforced_event_count": 0,
                "protect_ms": 0,
                "protected_until_ms": 0,
                "skipped": True,
                "reason": "disabled_for_read_only_scale_or_benchmark_run",
            }
        debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
        if (
            not any(
                item.get("ref_type") == "entity"
                and item.get("memory_scope") == "user_profile"
                and item.get("session_continuity") == "cross_session"
                for item in selected
            )
            and bool(cross_session_policy.get("enabled"))
            and (
                int(cross_session_policy.get("min_entity_bridge_refs") or 0) > 0
                or question_type in {"current_state", "latest", "profile_memory"}
            )
        ):
            for record in inventory_record_result.get("records", []):
                if record.get("record_type") != "context_entity":
                    continue
                if str(record.get("memory_scope") or "") != "user_profile":
                    continue
                if str(record.get("session_continuity") or "") != "cross_session":
                    continue
                entity_metadata = embedding_metadata_by_ref.get(("entity", record.get("entity_hash")), {})
                profile_current_value = first_explicit_bool("profile_entity_current", record, entity_metadata)
                if profile_current_value is False:
                    continue
                text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
                if not text.strip(" :=	"):
                    continue
                ref_tokens = max(1, token_count(text))
                if used_context_tokens + ref_tokens > remote_context_budget_tokens:
                    continue
                source_session_ids = record.get("source_session_ids", [])
                source_entity_hashes = record.get("source_entity_hashes", [])
                profile_memory_class = str(
                    record.get("profile_memory_class")
                    or entity_metadata.get("profile_memory_class")
                    or profile_memory_class_for_entity_type(record.get("entity_type"))
                ).strip()
                profile_memory_kind = str(
                    record.get("profile_memory_kind")
                    or entity_metadata.get("profile_memory_kind")
                    or profile_memory_kind_for_entity_type(record.get("entity_type"))
                ).strip()
                selected.append(
                    annotate_session_continuity(
                        {
                            "ref_type": "entity",
                            "ref_hash": record.get("entity_hash"),
                            "node_hash": record.get("node_hash"),
                            "node_path": record.get("node_path", []),
                            "origin_score": 1.0,
                            "score": 1.0,
                            "keyword_score": len(query_terms.intersection(tokens(text))),
                            "sparse_score": sparse_lexical_score(query_terms, text),
                            "embedding_score": 0.0,
                            "node_score": 0.0,
                            "matched_index_terms": sorted(candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)),
                            "selection_reason": "selected by final cross-session user-profile entity bridge",
                            "entity_type": record.get("entity_type", ""),
                            "entity_name": record.get("entity_name", ""),
                            "profile_memory_class": profile_memory_class,
                            "profile_memory_kind": profile_memory_kind,
                            "context_class": "entity",
                            "source_roles": record.get("source_roles", []),
                            "source_role_counts": record.get("source_role_counts", {}),
                            "source_hook_types": record.get("source_hook_types", []),
                            "source_hook_type_counts": record.get("source_hook_type_counts", {}),
                            "source_codex_events": record.get("source_codex_events", []),
                            "source_codex_event_counts": record.get("source_codex_event_counts", {}),
                            "source_session_ids": source_session_ids,
                            "source_event_ids": record.get("source_event_ids", []),
                            "source_entity_hashes": source_entity_hashes,
                            "source_memory_scopes": record.get("source_memory_scopes", []),
                            "source_session_continuities": record.get("source_session_continuities", []),
                            "source_extraction_phases": record.get("source_extraction_phases", []),
                            "memory_scope": record.get("memory_scope", ""),
                            "session_continuity": record.get("session_continuity", ""),
                            "extraction_phase": record.get("extraction_phase", ""),
                            "final_session_boundary": bool(record.get("final_session_boundary", False)),
                            "profile_promotion_policy": record.get("profile_promotion_policy", ""),
                            "profile_promotion_blocker": record.get("profile_promotion_blocker", ""),
                            "profile_revision": record.get("profile_revision", entity_metadata.get("profile_revision", 0)),
                            "previous_profile_revision": record.get("previous_profile_revision", entity_metadata.get("previous_profile_revision", 0)),
                            "previous_profile_updated_at_ms": record.get("previous_profile_updated_at_ms", entity_metadata.get("previous_profile_updated_at_ms", 0)),
                            "supersedes_session_entity_hash": record.get("supersedes_session_entity_hash", 0),
                            "supersedes_session_entity_hashes": record.get("supersedes_session_entity_hashes", []),
                            "profile_entity_current": profile_current_value if profile_current_value is not None else True,
                            "profile_current_state_representative": True,
                            "current_state_source_session_count": len(source_session_ids) if isinstance(source_session_ids, list) else 0,
                            "current_state_source_entity_count": len(source_entity_hashes) if isinstance(source_entity_hashes, list) else 0,
                            "current_state_policy": "profile_entity_bridge_preferred_over_session_local_history",
                            "metadata": record.get("metadata", {}),
                            "scope": candidate_access_scope(record),
                            "updated_at_ms": record.get("updated_at_ms", now_ms()),
                            "text": clip_context_text(text),
                            "token_estimate": ref_tokens,
                            "packing_score": 1.0,
                            "packing_policy": question_type,
                            "budget_memory_layer": "profile_entity",
                            "recall_path": "direct_profile_entity_bridge",
                        },
                        record,
                    )
                )
                used_context_tokens += ref_tokens
                dropped_over_budget.setdefault("profile_entity_bridge_injected", True)
                break
        serving_selected = compact_context_pack_refs(selected, include_debug=debug_refs)
        serving_dropped = compact_dropped_refs_for_context_pack(dropped_over_budget, include_debug=debug_refs)
        pack_summary = summarize_text(
            " ".join(str(item.get("text", "")) for item in selected),
            limit=512,
        )
        selected_context_counts = selected_context_class_counts(selected)
        freshness_tolerance_ms = int(ranking.get("freshness_tolerance_ms", DEFAULT_TIME_DECAY_TOLERANCE_MS))
        half_life_ms = int(ranking.get("half_life_ms", DEFAULT_TIME_DECAY_HALFLIFE_MS))
        selected_time_scores = [float(item.get("time_score", 0.0)) for item in selected if "time_score" in item]
        selected_age_ms: list[int] = []
        for item in selected:
            try:
                selected_age_ms.append(max(0, int(reference_time_ms) - int(item.get("updated_at_ms") or reference_time_ms)))
            except (TypeError, ValueError):
                continue
        time_weighted_recall = {
            "enabled": True,
            "role": "ranking_prior_not_temporal_compression",
            "score_field": "time_score",
            "formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
            "freshness_tolerance_ms": freshness_tolerance_ms,
            "half_life_ms": half_life_ms,
            "selected_ref_count": len(selected),
            "avg_selected_time_score": round(sum(selected_time_scores) / len(selected_time_scores), 6) if selected_time_scores else 0.0,
            "min_selected_time_score": round(min(selected_time_scores), 6) if selected_time_scores else 0.0,
            "max_selected_age_ms": max(selected_age_ms) if selected_age_ms else 0,
            "recent_selected_ref_count": sum(1 for age_ms in selected_age_ms if age_ms <= freshness_tolerance_ms),
            "older_selected_ref_count": sum(1 for age_ms in selected_age_ms if age_ms > freshness_tolerance_ms),
        }
        memory_layer_budget = selected_ref_layer_budget(selected)
        dropped_memory_layer_budget = dropped_ref_layer_budget(dropped_over_budget)
        memory_layer_pressure = memory_layer_pressure_summary(memory_layer_budget, dropped_memory_layer_budget)
        profile_selected_ref_count = sum(
            1
            for item in selected
            if str(item.get("memory_scope") or "").strip().lower() in {"user_profile", "profile", "cross_session_profile"}
            or (
                str(item.get("session_continuity") or "").strip().lower() == "cross_session"
                and str(item.get("ref_type") or "") == "entity"
            )
        )
        memory_inventory["profile_records_available_but_not_selected"] = bool(
            memory_inventory.get("has_profile_memory") and profile_selected_ref_count == 0
        )
        if memory_inventory["profile_records_available_but_not_selected"]:
            quality_warnings.append("profile_memory_available_but_not_selected")
        refresh_final_selected_budget_policies(selected, dropped_over_budget)

        selected_pending_async_refs = [
            item
            for item in selected
            if candidate_memory_layer_name(item)
            in {
                "pending_async_event",
                "pending_async_codex_outcome_event",
                "pending_async_memory_feature_event",
            }
            or str(item.get("extraction_phase") or "").strip().lower() == "pending_async"
        ]
        selected_pending_async_summary = {
            "selected_ref_count": len(selected_pending_async_refs),
            "selected_event_hashes": [
                ref.get("event_id_hash") or ref.get("ref_hash")
                for ref in selected_pending_async_refs[:16]
                if ref.get("event_id_hash") is not None or ref.get("ref_hash") is not None
            ],
            "selected_source_roles": ordered_normalized_role_list(
                [
                    role
                    for ref in selected_pending_async_refs
                    for role in (ref.get("source_roles", []) if isinstance(ref.get("source_roles"), list) else [])
                ]
            ),
            "selected_hook_types": ordered_unique(
                [
                    str(hook_type)
                    for ref in selected_pending_async_refs
                    for hook_type in (ref.get("source_hook_types", []) if isinstance(ref.get("source_hook_types"), list) else [])
                    if str(hook_type or "")
                ]
            ),
        }
        if selected_pending_async_refs:
            quality_warnings.append(
                f"selected_pending_async_event_refs:{len(selected_pending_async_refs)}"
            )
        quality_first_underfill = quality_first_underfill_summary(
            budget_fill_policy=budget_fill_policy,
            selected_ref_count=len(selected),
            used_context_tokens=used_context_tokens,
            remote_context_budget_tokens=remote_context_budget_tokens,
            dropped_over_budget=dropped_over_budget,
        )
        if quality_first_underfill.get("enabled"):
            quality_warnings.append(quality_first_underfill["warning"])
        retrieval_model_coverage = {
            "event_embedding_vectors": len(event_embedding_vectors),
            "entity_embedding_vectors": len(entity_embedding_vectors),
            "segment_embedding_vectors": len(segment_embedding_vectors),
            "compression_embedding_vectors": len(compression_embedding_vectors),
            "resource_embedding_vectors": len(resource_embedding_vectors),
            "skill_embedding_vectors": len(skill_embedding_vectors),
            "index_terms_by_ref": sum(len(values) for values in index_terms_by_ref.values()),
            "index_terms_by_node": sum(len(values) for values in index_terms_by_node.values()),
            "index_terms_by_batch": sum(len(values) for values in index_terms_by_batch.values()),
            "node_scope_recovered_count": len(node_scope_by_hash),
            "compact_scope_recovery_enabled": True,
        }
        # Remote-only safety floor: if this was a remote_only request and the remote pack came
        # back too sparse, re-admit the request's local context so the agent is never blind.
        if apply_remote_only_local_fallback(local_budget, used_context_tokens):
            local_tokens = int(local_budget.get("token_estimate", 0))
        # Said where a person reads it, not only in an audit record. A pack that is thinner than
        # it should be, with nothing explaining why, reads as bad retrieval -- and the fix for bad
        # retrieval is not the fix for this.
        if embedding_model_conflict_records:
            quality_warnings.append(
                "%d stored %s not searched: they were embedded by a different model than the one "
                "in use now (%s). Re-embed the store, or switch back, or those memories stay "
                "unsearchable."
                % (embedding_model_conflict_records,
                   "memory was" if embedding_model_conflict_records == 1 else "memories were",
                   active_embedding_model or "unnamed")
            )
        if embedding_width_conflict_records:
            quality_warnings.append(
                "%d stored %s not searched: their vectors are a different width from this query's, "
                "which happens when an encoder was unavailable and fallback vectors were written."
                % (embedding_width_conflict_records,
                   "memory was" if embedding_width_conflict_records == 1 else "memories were")
            )

        pack = {
            "context_pack_id": str(context_pack_id),
            "context_sources_order": ["local_context", "matrixark_remote_context"],
            "pre_retrieval_idle_commit": pre_retrieval_idle_commit,
            "pre_retrieval_summary_refresh": pre_retrieval_summary_refresh,
            "local_context_refs": local_context_refs_for_pack(local_budget),
            "selected_refs": serving_selected,
            "remote_context_refs": serving_selected,
            "selected_ref_counts": selected_context_counts,
            "context_assembly_policy": {
                "access_scope_before_scoring": True,
                "skill_selection": "skill_section_only",
                "resource_selection": "resource_facts_entities_and_chunks_are_ranked_separately",
                "recall_reinforcement": "selected event refs and compression source ids receive protection markers before raw-event pruning",
            },
            "memory_inventory": memory_inventory,
            "layer_scores": layer_scores[:24],
            "question_type": question_type,
            "packing_policy": f"question_type_aware:{question_type}",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "recall_policy": {
                "query_plan": query_plan,
                "session_continuity": {
                    "mode": retrieval_session_scope,
                    "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                    "same_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "same_session"),
                    "cross_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session"),
                    "entity_bridge_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session" and item.get("ref_type") == "entity"),
                },
                "session_identity": session_identity_policy,
                "memory_layer_budget": memory_layer_budget,
                "dropped_memory_layer_budget": dropped_memory_layer_budget,
                "memory_layer_pressure": memory_layer_pressure,
                "selected_pending_async": selected_pending_async_summary,
                "quality_first_underfill": quality_first_underfill,
                "pre_retrieval_idle_commit": pre_retrieval_idle_commit,
                "pre_retrieval_summary_refresh": pre_retrieval_summary_refresh,
                "async_pipeline_readiness": async_pipeline_readiness,
                "cross_session": dropped_over_budget.get("cross_session_policy", cross_session_policy),
                "shared_context": dropped_over_budget.get("shared_context_policy", shared_context_policy),
                "source_role_budget": {
                    **(dropped_over_budget.get("source_role_budget_policy", {"enabled": False}) if isinstance(dropped_over_budget.get("source_role_budget_policy"), dict) else {"enabled": False}),
                    "mode": source_role_budget_mode or ("explicit" if source_role_budget_tokens else "disabled"),
                    "remote_budget_tokens": remote_context_budget_tokens,
                    "derived": source_role_budget_mode in {"auto", "balanced", "codex_auto"},
                    "budget_semantics": "independent_per_role_caps_under_global_remote_budget",
                    "independent_caps": True,
                    "global_remote_budget_enforced": True,
                },
                "memory_layer_budget_policy": {
                    **(dropped_over_budget.get("memory_layer_budget_policy", {"enabled": False}) if isinstance(dropped_over_budget.get("memory_layer_budget_policy"), dict) else {"enabled": False}),
                    "mode": memory_layer_budget_mode or ("explicit" if memory_layer_budget_tokens else "disabled"),
                    "remote_budget_tokens": remote_context_budget_tokens,
                    "question_type": question_type,
                    "question_budget_reason": memory_layer_budget_reason,
                    "derived": memory_layer_budget_mode in {
                        "auto",
                        "balanced",
                        "codex_auto",
                        "pre_retrieval_summary_refresh_balanced",
                    },
                    "budget_semantics": "independent_per_layer_caps_under_global_remote_budget",
                    "independent_caps": True,
                    "global_remote_budget_enforced": True,
                },
                "memory_selection_policy_budget_policy": {
                    **(
                        dropped_over_budget.get("memory_selection_policy_budget_policy", {"enabled": False})
                        if isinstance(dropped_over_budget.get("memory_selection_policy_budget_policy"), dict)
                        else {"enabled": False}
                    ),
                    "mode": memory_selection_policy_budget_mode or (
                        "explicit" if memory_selection_policy_budget_tokens else "disabled"
                    ),
                    "remote_budget_tokens": remote_context_budget_tokens,
                    "budget_semantics": "independent_per_memory_selection_policy_caps_under_global_remote_budget",
                    "independent_caps": True,
                    "global_remote_budget_enforced": True,
                },
                "extraction_phase_budget_policy": {
                    **(
                        dropped_over_budget.get("extraction_phase_budget_policy", {"enabled": False})
                        if isinstance(dropped_over_budget.get("extraction_phase_budget_policy"), dict)
                        else {"enabled": False}
                    ),
                    "mode": extraction_phase_budget_mode or (
                        "explicit" if extraction_phase_budget_tokens else "disabled"
                    ),
                    "remote_budget_tokens": remote_context_budget_tokens,
                    "budget_semantics": "independent_per_extraction_phase_caps_under_global_remote_budget",
                    "independent_caps": True,
                    "global_remote_budget_enforced": True,
                },
                "backend_retrieval_pushdown": retrieval_scan_stats,
                "retrieval_model_coverage": retrieval_model_coverage,
                "memory_inventory": memory_inventory,
                "ranking": {
                    "min_similarity_score": min_similarity_score,
                    "max_global_candidates": max_global_candidates,
                    "max_selected_refs": max_selected_refs,
                    "budget_fill_policy": budget_fill_policy,
                    "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first",
                },
                "tree_traversal": {
                    "enabled": True,
                    "summary_embeddings": ["node_l0", "node_l1"],
                    "top_k_per_layer": top_k_per_layer,
                    "max_children_scored_per_parent": max_children_scored_per_parent,
                    "hard_max_children_scored_per_parent": hard_max_children_scored_per_parent,
                    "children_scoring_policy": "score_all_children_up_to_hard_cap_then_split_node_layers",
                    "max_candidates_per_node": max_candidates_per_node,
                    "max_raw_events_per_node": max_raw_events_per_node,
                    "max_selected_refs": max_selected_refs,
                    "selected_node_count": len(selected_node_hashes),
                    "selected_path_count": len(selected_paths),
                    "selected_leaf_count": len(traversal.get("leaf_paths", [])),
                    "candidate_records_after_tree": len(tree_candidate_records),
                    "records_dropped_by_tree": tree_prefilter_dropped_count,
                    "records_dropped_by_node_fanout": fanout_dropped_count,
                    # Kept apart on purpose. Two widths means a provider outage seeded fallback
                    # vectors; two encoders at one width means the embedding model was changed.
                    # Same symptom -- memories that stop matching -- and different fixes.
                    "records_declined_by_encoder_change": embedding_model_conflict_records,
                    "records_declined_by_vector_width": embedding_width_conflict_records,
                    "raw_events_dropped_by_time_window": raw_event_time_window_dropped_count,
                    "cold_events_represented_by_compression": raw_event_time_window_dropped_count > 0,
                    "leaf_record_fetch_policy": "events/entities/resources/skills/compressions scanned only inside selected L0/L1 folders",
                    "fallback_to_flat": bool(traversal.get("fallback_to_flat")),
                    "fallback_reason": "missing_or_stale_summary_embeddings" if traversal.get("fallback_to_flat") else "",
                },
                "secondary_index_filter": {
                    "enabled": bool(secondary_index_filter_groups),
                    "required_groups": [sorted(group) for group in secondary_index_filter_groups],
                    "matched_candidate_count": secondary_index_matched_count,
                    "dropped_candidate_count": secondary_index_dropped_count,
                    "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group",
                    "effective_mode": secondary_index_filter_mode,
                    "applied_before_embedding_scoring": True,
                    "fanout_cap_applied_before_embedding_scoring": True,
                },
                "rerank": rerank_policy,
                "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
                "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
                "time_decay": {
                    "freshness_tolerance_ms": freshness_tolerance_ms,
                    "half_life_ms": half_life_ms,
                },
                "time_weighted_recall": time_weighted_recall,
                "recall_reinforcement": reinforcement,
                "weights": {
                    "time": optional_object(ranking, "weights").get("time", DEFAULT_TIME_WEIGHT),
                    "business": optional_object(ranking, "weights").get("business", DEFAULT_BUSINESS_WEIGHT),
                },
                "auxiliary_quota": auxiliary_quota,
                "storage_options": storage_options,
                "hard_deadline": {
                    "deadline_ms": deadline_ms,
                    "elapsed_ms": round((time.perf_counter() - started_perf) * 1000.0, 3),
                    "partial_context_pack": partial_context_pack,
                    "fallback_reason": dropped_over_budget.get("deadline_reason", "") if partial_context_pack else "",
                },
            },
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "used_context_tokens": used_context_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "used_local_context_tokens": local_tokens,
            "total_prompt_context_tokens": used_context_tokens + local_tokens,
            "remote_context_budget_tokens": remote_context_budget_tokens,
            "requested_max_context_tokens": max_context_tokens,
            "local_context_safety_margin_tokens": safety_margin_tokens,
            "budget_source": budget_source,
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_tokens,
                "local_context_token_source": local_budget.get("token_source", "estimated_from_local_context"),
                "safety_margin_tokens": safety_margin_tokens,
                "safety_margin_source": local_budget.get("safety_margin_source", "matrixark_default_5_percent_capped"),
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": serving_dropped,
            "quality_warnings": quality_warnings,
            # Vectors this query could not compare against. Reported even when zero, because the
            # difference between "none were declined" and "nobody counted" is the whole point.
            "embedding_conflicts": {
                "encoder_change": embedding_model_conflict_records,
                "vector_width": embedding_width_conflict_records,
                "active_embedding_model": active_embedding_model,
            },
            # Stated by the path that did it. The other implementation over this same store ranks
            # by term overlap and reads no vector, and a caller cannot tell the two apart from the
            # results alone.
            "served_by": {
                "assembly": "python_local_adapter",
                "ranking": "dense_cosine_blended_with_lexical",
                "ranking_uses_vectors": True,
            },
            "insufficient_context": not selected,
            "partial_context_pack": partial_context_pack,
            "context_pack_payload_policy": {
                "serving_refs": "compact" if not debug_refs else "debug_full",
                "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
                "dropped_ref_details": "audit_only" if not debug_refs else "included",
                "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
            },
            "operational_visibility_policy": {
                "audit_mode": audit_mode,
                "audit_sample_rate": audit_sample_rate,
                "telemetry_record": audit_mode != "off",
                "rich_replay_audit": audit_mode == "full" and audit_sample_rate > 0,
                "rich_replay_audit_force_on_partial_or_warning": True,
            },
        }
        finish_retrieval_stage("pack", stage_started_perf)
        pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
        over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
        if over_budget_stages:
            quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
            pack["quality_warnings"] = quality_warnings
        audit_started_perf = time.perf_counter()
        # Measurement channel. The visit count goes into the audit record below, which is STORED
        # rather than returned, and this process's stderr is discarded by whatever spawned it -- so
        # neither is readable from an A/B. A file named by the environment is.
        _visits_path = str(_os.environ.get("MATRIXARK_SCAN_VISITS_PATH", "")).strip()
        if _visits_path:
            try:
                with open(_visits_path, "a", encoding="utf-8") as _visits_file:
                    _visits_file.write(
                        "%d %d %s\n"
                        % (_scan_visits[0], len(tree_candidate_records), _type_buckets_enabled)
                    )
            except OSError:
                # A measurement channel must never break the request it is measuring.
                pass
        audit_record = {
            "record_type": "context_pack_audit",
            "context_pack_id": context_pack_id_text,
            "query": query,
            "scope": scope,
            "summary_text": pack_summary,
            "selected_refs": compact_refs_for_audit(selected),
            "local_context_refs": compact_local_context_refs(local_budget),
            "context_sources_order": pack["context_sources_order"],
            "selected_ref_counts": selected_context_counts,
            "context_assembly_policy": pack["context_assembly_policy"],
            "dropped_refs": dropped_over_budget,
            "quality_warnings": quality_warnings,
            "partial_context_pack": partial_context_pack,
            "layer_scores": layer_scores[:24],
            "tree_traversal": pack["recall_policy"]["tree_traversal"],
            "secondary_index_filter": pack["recall_policy"]["secondary_index_filter"],
            "question_type": question_type,
            "packing_policy": pack["packing_policy"],
            "rerank_policy": rerank_policy,
            "recall_policy": pack["recall_policy"],
            "backend_retrieval_pushdown": pack["recall_policy"].get("backend_retrieval_pushdown", {}),
            "stage_latency_budgets": pack["recall_policy"]["stage_latency_budgets"],
            "storage_options": storage_options,
            "local_context_policy": pack["local_context_policy"],
            "memory_layer_budget": memory_layer_budget,
            "dropped_memory_layer_budget": dropped_memory_layer_budget,
            "memory_layer_pressure": memory_layer_pressure,
            "selected_pending_async": selected_pending_async_summary,
            "quality_first_underfill": quality_first_underfill,
            "memory_inventory": memory_inventory,
            "async_pipeline_readiness": async_pipeline_readiness,
            "used_local_context_tokens": pack["used_local_context_tokens"],
            "used_remote_context_tokens": pack["used_remote_context_tokens"],
            "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
            "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
            "requested_max_context_tokens": pack["requested_max_context_tokens"],
            "local_context_safety_margin_tokens": pack["local_context_safety_margin_tokens"],
            "budget_source": pack["budget_source"],
            "operational_visibility_policy": pack["operational_visibility_policy"],
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "tree_candidate_records": len(tree_candidate_records),
            "record_scan_visits": _scan_visits[0],
            "record_scan_type_buckets": _type_buckets_enabled,
            "tree_prefilter_dropped_count": tree_prefilter_dropped_count,
            "fanout_dropped_count": fanout_dropped_count,
            "records_declined_by_encoder_change": embedding_model_conflict_records,
            "records_declined_by_vector_width": embedding_width_conflict_records,
            "active_embedding_model": active_embedding_model,
            "max_candidates_per_node": max_candidates_per_node,
            "max_selected_refs": max_selected_refs,
            "created_at_ms": now_ms(),
        }
        visibility_decision = self.append_context_pack_visibility(
            pack=pack,
            audit_record=audit_record,
            query=query,
            scope=scope,
            audit_mode=audit_mode,
            request_metadata=request_metadata,
            audit_sample_rate=audit_sample_rate,
        )
        pack["operational_visibility_policy"] = visibility_decision
        if pack_cache_enabled and not pack.get("partial_context_pack"):
            cached_pack = json.loads(json.dumps(pack))
            cached_recall = cached_pack.get("recall_policy") if isinstance(cached_pack.get("recall_policy"), dict) else {}
            cached_recall["context_pack_cache"] = {"hit": False, "ttl_s": self._context_pack_cache_ttl_s}
            cached_pack["recall_policy"] = cached_recall
            with self._context_pack_cache_lock:
                if len(self._context_pack_cache) >= self._context_pack_cache_max_entries:
                    oldest_key = next(iter(self._context_pack_cache))
                    self._context_pack_cache.pop(oldest_key, None)
                self._context_pack_cache[pack_cache_key] = (time.monotonic(), cached_pack)
        finish_retrieval_stage("audit", audit_started_perf)
        placement = retrieval_scan_stats.get("native_selected_node_locations", {}) if isinstance(retrieval_scan_stats, dict) else {}
        candidate_cache_hit = bool(
            isinstance(retrieval_scan_stats, dict)
            and (
                retrieval_scan_stats.get("cache_hit")
                or retrieval_scan_stats.get("candidate_cache_hit")
                or retrieval_scan_stats.get("native_placement_candidate_cache_hit")
            )
        )
        index_postings_read = (
            int(retrieval_scan_stats.get("index_postings_read") or 0)
            if isinstance(retrieval_scan_stats, dict)
            else 0
        )
        if isinstance(retrieval_scan_stats, dict) and not index_postings_read:
            index_postings_read = int(
                retrieval_scan_stats.get("index_postings_touched")
                or retrieval_scan_stats.get("native_index_postings_found")
                or 0
            )
        dropped_ref_bucket_counts = {
            key: int(value)
            for key, value in dropped_over_budget.items()
            if isinstance(value, int) and key != "deadline_exceeded" and int(value) > 0
        }
        dropped_ref_count = sum(dropped_ref_bucket_counts.values())
        serving_memory_layer_budget_value = serving_memory_layer_budget(memory_layer_budget)
        serving_dropped_memory_layer_budget_value = serving_memory_layer_budget(dropped_memory_layer_budget)
        serving_memory_layer_pressure_value = serving_memory_layer_pressure(memory_layer_pressure)
        pack["retrieval_metrics"] = {
            "query_plan_ms": round(float(stage_latencies_ms.get("query_understanding", 0.0)), 3),
            "node_traversal_ms": round(float(stage_latencies_ms.get("node_traversal", 0.0)), 3),
            "index_prefilter_ms": round(float(stage_latencies_ms.get("candidate_fetch", 0.0)), 3),
            "candidate_fetch_ms": round(float(stage_latencies_ms.get("candidate_fetch", 0.0)), 3),
            "score_ms": round(float(stage_latencies_ms.get("rerank_score", 0.0)), 3),
            "pack_ms": round(float(stage_latencies_ms.get("pack", 0.0)), 3),
            "audit_ms": round(float(stage_latencies_ms.get("audit", 0.0)), 3),
            "append_queue_wait_ms": 0.0,
            "append_engine_ms": 0.0,
            "selected_refs": len(selected),
            "dropped_refs": dropped_ref_count,
            "dropped_ref_bucket_counts": dropped_ref_bucket_counts,
            "stale_dropped_refs": int(dropped_ref_bucket_counts.get("stale", 0)),
            "requested_max_context_tokens": max_context_tokens,
            "used_local_context_tokens": local_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "total_prompt_context_tokens": used_context_tokens + local_tokens,
            "remote_context_budget_tokens": remote_context_budget_tokens,
            "local_context_safety_margin_tokens": safety_margin_tokens,
            "local_context_count": len(local_budget["items"]),
            "remote_is_additive_only_within_remaining_budget": True,
            "memory_layer_budget": serving_memory_layer_budget_value,
            "dropped_memory_layer_budget": serving_dropped_memory_layer_budget_value,
            "memory_layer_pressure": serving_memory_layer_pressure_value,
            "selected_pending_async_ref_count": selected_pending_async_summary["selected_ref_count"],
            "quality_first_underfill": {
                key: value
                for key, value in quality_first_underfill.items()
                if key in {"enabled", "unused_remote_context_tokens", "dropped_ref_count", "dropped_reason_counts"}
            },
            "pre_retrieval_idle_commit": pre_retrieval_idle_commit,
            "pre_retrieval_summary_refresh": pre_retrieval_summary_refresh,
            "async_pipeline_readiness": async_pipeline_readiness,
            "scanned_records": int(retrieval_scan_stats.get("loaded_records") or retrieval_scan_stats.get("scanned_records") or len(records)) if isinstance(retrieval_scan_stats, dict) else len(records),
            "returned_records_after_prefilter": int(retrieval_scan_stats.get("returned_records") or len(records)) if isinstance(retrieval_scan_stats, dict) else len(records),
            "candidate_cache_hit": candidate_cache_hit,
            "cache_hit": candidate_cache_hit,
            "secondary_index_prefilter_enabled": bool(retrieval_scan_stats.get("secondary_index_prefilter_enabled")) if isinstance(retrieval_scan_stats, dict) else False,
            "secondary_index_dropped_records": int(retrieval_scan_stats.get("dropped_by_secondary_index") or 0) if isinstance(retrieval_scan_stats, dict) else 0,
            "index_postings_read": index_postings_read,
            "index_postings_touched": index_postings_read,
            "index_posting_ref_hash_count": int(retrieval_scan_stats.get("index_posting_ref_hash_count") or 0) if isinstance(retrieval_scan_stats, dict) else 0,
            "index_posting_node_hash_count": int(retrieval_scan_stats.get("index_posting_node_hash_count") or 0) if isinstance(retrieval_scan_stats, dict) else 0,
            "retrieval_model_coverage": retrieval_model_coverage,
            "memory_inventory": memory_inventory,
            "placement_partitions_touched": len(placement.get("locations", []) or []) if isinstance(placement, dict) else 0,
            "native_pack_assembly": False,
            "python_pack_fallback": True,
            "raw_candidate_tables_returned": False,
            "source": "python_reference_pack",
        }
        if bool(args.get("include_retrieval_metrics")):
            pack["include_retrieval_metrics"] = True
        pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
        over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
        if over_budget_stages and not any(str(warning).startswith("stage_budget_exceeded:") for warning in quality_warnings):
            quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
            pack["quality_warnings"] = quality_warnings
        # Precision-expand (gated): for exact-fact queries, add matched segments' source raw
        # events so exact hashes/numbers/commands the summaries dropped are recovered.
        if PACK_PRECISION_EXPAND_ENABLED and question_type in PACK_PRECISION_EXPAND_QUESTION_TYPES:
            precision_expand_pack(pack, records, question_type,
                                  max_events=PACK_PRECISION_EXPAND_MAX_EVENTS, budget_tokens=16000)
        if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
            return pack
        return compact_context_pack_for_serving(pack, include_debug=debug_refs)

    def feedback(self, args: Json, *, hook: Json | None = None) -> Json:
        args = {**args, "kind": "feedback"}
        return self.ingest(args, hook=hook)

    def replay(self, args: Json) -> Json:
        if not (ENABLE_CONTEXT_REPLAY or bool(args.get("enable_replay"))):
            raise MatrixArkError("context replay is disabled; set MATRIXARK_ENABLE_REPLAY=1 or pass enable_replay=true for explicit debug runs")
        context_pack_id = require_string(args, "context_pack_id")
        include_debug = bool(args.get("include_debug_records") or args.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS or AUDIT_DEBUG_PAYLOAD)
        self.flush_audits()
        records = self.read_all()
        if include_debug:
            return {
                "context_pack_id": context_pack_id,
                "events": records,
                "replay_payload_policy": "debug_full_store_scan",
            }
        replay_records: list[Json] = []
        for record in records:
            if str(record.get("context_pack_id") or "") != context_pack_id:
                continue
            record_type = str(record.get("record_type") or "")
            if record_type == "context_pack_audit":
                replay_records.append(compact_context_pack_audit_record(record))
            elif record_type == "context_pack_telemetry":
                replay_records.append(
                    {
                        key: record.get(key)
                        for key in [
                            "record_type",
                            "context_pack_id",
                            "query_hash",
                            "question_type",
                            "selected_ref_count",
                            "selected_ref_counts",
                            "dropped_ref_count",
                            "dropped_ref_bucket_counts",
                            "stale_dropped_refs",
                            "used_local_context_tokens",
                            "used_remote_context_tokens",
                            "total_prompt_context_tokens",
                            "remote_context_budget_tokens",
                            "memory_layer_budget",
                            "dropped_memory_layer_budget",
                            "memory_selection_policy_budget",
                            "async_pipeline_readiness",
                            "session_identity",
                            "retrieval_request_metadata",
                            "partial_context_pack",
                            "insufficient_context",
                            "quality_warning_count",
                            "primary_candidate_count",
                            "auxiliary_candidate_count",
                            "created_at_ms",
                        ]
                        if record.get(key) not in (None, "", [], {})
                    }
                )
            else:
                replay_records.append(
                    {
                        key: record.get(key)
                        for key in ["record_type", "context_pack_id", "source_ref_type", "source_ref_hash", "event_id_hash", "node_hash", "reinforced_at_ms", "protected_until_ms", "reason"]
                        if record.get(key) not in (None, "", [], {})
                    }
                )
        return {
            "context_pack_id": context_pack_id,
            "events": replay_records,
            "replay_payload_policy": "compact_context_pack_scope",
            "debug_records_available_with": "include_debug_records=true",
        }

