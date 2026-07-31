#!/usr/bin/env python3
"""MatrixArk query-planning and secondary-index inference helpers."""

from __future__ import annotations

import re
from typing import Any

try:
    from tools.matrixark_mcp_indexing import benchmark_quality_index_terms, context_index_name, normalized_index_value
    from tools.matrixark_mcp_scoring import tokens
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_indexing import benchmark_quality_index_terms, context_index_name, normalized_index_value
    from matrixark_mcp_scoring import tokens


Json = dict[str, Any]


QUERY_TYPE_LABELS: dict[str, str] = {
    "profile_memory": "question asks user profile long term memory profile entities profile summaries cross session memories across sessions",
    "benchmark_quality": "question asks benchmark quality hit rate latency p50 p99 throughput locomo longmemeval memory evaluation",
    "date": "question asks when date before after yesterday tomorrow week month year",
    "current_state": "question asks current latest now still status preference location role valid state",
    "why_emotion": "question asks why reason feeling emotion because",
    "evidence": "question asks quote exact message evidence what did someone say",
    "procedure": "question asks procedure steps troubleshoot debug rollback runbook checklist how to fix",
    "broad_exploration": "question asks overview summarize broad exploration topics inventory what is known",
    "multi_hop": "question requires combining multiple sessions people facts cross conversation reasoning",
    "fact": "question asks a direct factual answer",
}

PROFILE_MEMORY_QUERY_RE = re.compile(
    r"\b(user profile|profile memory|long[- ]term memor(?:y|ies)|cross[- ]session memor(?:y|ies)|profile entit(?:y|ies)|profile summar(?:y|ies)|remember about me|remember about|what should (?:i|you|we) remember|standing instructions?|standing preferences?|persistent instructions?|saved preferences?|know about (?:me|my|the user)|what (?:have|did) i (?:tell|told) you|what (?:are|were) my preferences|my preferences|told you before)\b"
)

QUERY_INDEX_LABELS: dict[str, str] = {
    "entity_type:location": "location city moved lives staying where user is",
    "entity_type:preference": "preference prefer favorite likes language tool choice",
    "event_type:preference_update": "preference update changed choice likes prefers",
    "entity_type:relationship": "relationship manager sister brother teammate family person",
    "entity_type:family_profile": "family pet dog cat child household",
    "entity_type:job_status": "job role work status position responsibility",
    "event_type:status_update": "job status role work update",
    "entity_type:current_plan": "plan current plan upcoming task schedule next milestone",
    "event_type:plan_update": "plan update going to schedule will next",
    "event_type:confirmation": "confirmation approved accepted yes correct confirmed",
    "entity_type:approval_state": "approval budget purchase cost approved",
    "entity_type:resource_fact": "resource document extracted fact approval owner budget",
    "classification:confirmation": "confirmation approved accepted yes correct",
    "classification:resource_fact": "resource fact extracted from document",
    "segment_topic:approval_budget": "approval budget cost purchase decision",
    "entity_type:assistant_decision": "assistant decision final answer done implemented chose decided next action",
    "event_type:assistant_response": "assistant response final answer outcome done implemented fixed decision",
    "event_type:user_prompt": "user prompt request asks asked requirement instruction",
    "entity_type:tool_evidence": "tool evidence tests passed failed exit code commit push rebase validation benchmark blocker",
    "event_type:tool_evidence": "tool event evidence tests passed failed exit code commit push rebase validation benchmark blocker",
    "source_type:resource": "document resource pdf file chunk evidence",
    "source_type:message": "conversation message dialogue user said assistant said",
    "source_type:skill": "skill tool playbook procedure instruction",
}


def _ordered_unique(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


RESOURCE_TYPE_QUERY_ALIASES: dict[str, str] = {
    "pdf": "pdf",
    "markdown": "md",
    "md": "md",
    "readme": "md",
    "text": "txt",
    "txt": "txt",
    "csv": "csv",
    "tsv": "tsv",
    "excel": "xlsx",
    "xlsx": "xlsx",
    "spreadsheet": "xlsx",
    "html": "html",
    "webpage": "html",
    "docx": "docx",
    "word": "docx",
    "pptx": "pptx",
    "slides": "pptx",
    "deck": "pptx",
}

UNIT_KIND_QUERY_ALIASES: dict[str, str] = {
    "paragraph": "paragraph",
    "passage": "paragraph",
    "heading": "heading",
    "section": "heading",
    "table": "table_row_group",
    "row": "table_row_group",
    "rows": "table_row_group",
    "sheet": "table_row_group",
    "page": "page",
    "slide": "slide",
    "slides": "slide",
    "function": "code_symbol",
    "class": "code_symbol",
    "symbol": "code_symbol",
}

QUERY_INDEX_STOPWORDS = {
    "what", "which", "where", "when", "who", "why", "how", "does", "did", "the", "and", "for",
    "from", "with", "that", "this", "into", "about", "show", "give", "list", "find", "current",
    "latest", "now", "need", "needs", "using", "use", "tool", "skill", "resource", "document", "file",
}


def slug_candidates_from_query(query: str) -> list[str]:
    lower = query.lower()
    candidates: list[str] = []
    for pattern in [
        r"(?:heading|section|chapter)\s+['\"]?([a-z0-9][a-z0-9 _./:-]{1,80})",
        r"#\s*([a-z0-9][a-z0-9 _./:-]{1,80})",
    ]:
        for match in re.finditer(pattern, lower):
            raw_value = re.split(r"\b(?:in|from|for|about|with|under)\b", match.group(1).split("?")[0], maxsplit=1)[0]
            value = normalized_index_value(raw_value)
            if value:
                candidates.append(value)
    return _ordered_unique(candidates)[:4]


def path_candidates_from_query(query: str) -> list[str]:
    values: list[str] = []
    for raw in re.findall(r"[a-zA-Z0-9_.-]+/[a-zA-Z0-9_./-]+|[a-zA-Z0-9_.-]+\.(?:md|txt|pdf|csv|tsv|json|jsonl|yaml|yml|html|docx|pptx|xlsx|py|js|ts|go|rs|cpp|h)", query):
        normalized = normalized_index_value(raw)
        if normalized:
            values.append(normalized)
    return _ordered_unique(values)[:6]


def keyword_candidates_from_query(query: str) -> list[str]:
    values = []
    for term in tokens(query):
        if len(term) < 4 or term in QUERY_INDEX_STOPWORDS:
            continue
        values.append(context_index_name("keyword", term))
    return _ordered_unique(values)[:8]


def deterministic_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    lower = query.lower()
    groups: list[set[str]] = []

    def add_group(*terms: str) -> None:
        clean = {term for term in terms if term}
        if clean and clean not in groups:
            groups.append(clean)

    if re.search(r"\b(where|location|located|moved|moving|live|lives|city|home|staying)\b", lower):
        location_terms = [context_index_name("entity_type", "location")]
        if question_type == "date" or re.search(r"\b(before|after|as of|used to|previously|formerly)\b", lower):
            location_terms.append(context_index_name("source_type", "message"))
        add_group(*location_terms)
    if re.search(r"\b(prefer|preference|favorite|like|likes|love|loves)\b", lower):
        add_group(context_index_name("entity_type", "preference"), context_index_name("event_type", "preference_update"))
    if re.search(r"\b(friend|partner|mother|father|sister|brother|wife|husband|manager|teammate|relationship|family|child|children|son|daughter|pet)\b", lower):
        add_group(context_index_name("entity_type", "relationship"), context_index_name("entity_type", "family_profile"))
    if re.search(r"\b(job|role|work|works|position|status|company|employer)\b", lower):
        add_group(context_index_name("entity_type", "job_status"), context_index_name("event_type", "status_update"))
    if re.search(r"\b(plan|plans|planning|going to|schedule|next)\b", lower):
        add_group(context_index_name("entity_type", "current_plan"), context_index_name("event_type", "plan_update"))
    if re.search(r"\b(approval|approved|approve|confirmed|confirmation|budget|purchase|cost|gpu)\b", lower):
        add_group(
            context_index_name("event_type", "confirmation"),
            context_index_name("event_type", "resource_approval_fact"),
            context_index_name("entity_type", "approval_state"),
            context_index_name("entity_type", "confirmation"),
            context_index_name("entity_type", "resource_fact"),
            context_index_name("classification", "confirmation"),
            context_index_name("classification", "resource_fact"),
            context_index_name("segment_topic", "approval_budget"),
            context_index_name("source_type", "resource"),
            context_index_name("source_type", "resource_fact"),
        )
    if re.search(r"\b(correction|corrected|wrong|instead|updated|changed)\b", lower):
        add_group(
            context_index_name("event_type", "correction"),
            context_index_name("entity_type", "correction"),
            context_index_name("classification", "correction"),
            context_index_name("segment_topic", "correction"),
        )
    if re.search(r"\b(assistant|decision|decided|done|implemented|fixed|final answer|what did codex|what was done|next action)\b", lower):
        add_group(
            context_index_name("entity_type", "assistant_decision"),
            context_index_name("event_type", "assistant_response"),
            context_index_name("source_type", "message"),
        )
    if re.search(r"\b(benchmark|workload|latency|p50|p90|p95|p99|throughput|qps|ops/s|req/s|hit[- ]?rate|read[- ]?hit|quality|recall|precision|locomo|longmemeval|memory[- ]?quality)\b", lower):
        add_group(
            context_index_name("entity_type", "tool_evidence"),
            context_index_name("event_type", "tool_evidence"),
            context_index_name("entity_type", "assistant_decision"),
            context_index_name("event_type", "assistant_response"),
        )
        add_group(context_index_name("source_role", "tool"), context_index_name("source_role", "assistant"))
        add_group(
            context_index_name("memory_selection_policy", "selected_tool_evidence_only"),
            context_index_name("memory_selection_policy", "selected_assistant_decision_outcome_only"),
        )
        metric_terms = benchmark_quality_index_terms(query)
        if metric_terms:
            add_group(*metric_terms)
        if re.search(r"\b(hit[- ]?rate|read[- ]?hit|quality|recall|precision|locomo|longmemeval|memory[- ]?quality)\b", lower):
            add_group(context_index_name("memory_scope", "user_profile"), context_index_name("session_continuity", "cross_session"))
    if re.search(r"\b(tool|evidence|test|tests|passed|failed|exit code|commit|pushed|push|rebase|validation|benchmark|blocker)\b", lower):
        add_group(context_index_name("entity_type", "tool_evidence"), context_index_name("event_type", "tool_evidence"))
    if re.search(r"\b(user prompt|prompt|user request|user asked|request|asked codex)\b", lower):
        add_group(context_index_name("event_type", "user_prompt"), context_index_name("source_role", "user"))
    if re.search(r"\b(resource|document|doc|file|pdf|markdown|readme|csv|spreadsheet|excel|html|word|slides?|deck)\b", lower):
        add_group(context_index_name("source_type", "resource"), context_index_name("source_type", "resource_fact"))
    for alias, resource_type in RESOURCE_TYPE_QUERY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", lower):
            add_group(context_index_name("resource_type", resource_type))
    for alias, unit_kind in UNIT_KIND_QUERY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", lower):
            extra_unit_terms = [context_index_name("unit_kind", unit_kind)]
            if unit_kind == "heading":
                extra_unit_terms.append(context_index_name("unit_kind", "markdown_section"))
            if unit_kind == "paragraph":
                extra_unit_terms.append(context_index_name("unit_kind", "text_paragraph"))
            add_group(*extra_unit_terms)
    heading_terms = [context_index_name("heading_slug", slug) for slug in slug_candidates_from_query(query)]
    if heading_terms:
        add_group(*heading_terms)
    path_terms = [context_index_name("relative_path", path) for path in path_candidates_from_query(query)]
    if path_terms:
        add_group(*path_terms)
    keyword_terms = keyword_candidates_from_query(query)
    if keyword_terms and re.search(r"\b(resource|document|doc|file|pdf|markdown|readme|csv|spreadsheet|excel|html|word|slides?|deck|skill|tool|section|heading)\b", lower):
        add_group(*keyword_terms)
    if re.search(r"\b(skill|tool|playbook|procedure|instruction|capability)\b", lower):
        add_group(context_index_name("source_type", "skill"))
        tool_terms = [context_index_name("skill_tool", term) for term in tokens(query) if term.startswith("matrixark_") or term in {"replay", "audit", "retrieve", "ingest"}]
        query_tokens = [term for term in tokens(query) if len(term) >= 4 and term not in QUERY_INDEX_STOPWORDS]
        trigger_values: list[str] = []
        for size in (3, 2):
            trigger_values.extend("_".join(query_tokens[index : index + size]) for index in range(0, max(0, len(query_tokens) - size + 1)))
        trigger_values.extend(query_tokens)
        trigger_terms = [context_index_name("skill_trigger", term) for term in _ordered_unique(trigger_values)]
        if tool_terms:
            add_group(*tool_terms[:6])
        if trigger_terms:
            add_group(*trigger_terms[:24])
    if question_type == "evidence":
        add_group(context_index_name("source_type", "message"), context_index_name("source_type", "feedback"))
    return groups


def secondary_filter_terms_to_fields(groups: list[set[str]]) -> Json:
    fields: Json = {}
    for group in groups:
        for term in sorted(group):
            if ":" not in term:
                continue
            field, value = term.split(":", 1)
            if not field or not value:
                continue
            fields.setdefault(field, [])
            if value not in fields[field]:
                fields[field].append(value)
    return fields


def infer_temporal_window(query: str, question_type: str, *, reference_time_ms: int) -> Json:
    lower = query.lower()
    if re.search(r"\b(current|currently|latest|now|still|today|valid)\b", lower) or question_type == "current_state":
        return {"mode": "latest", "valid_as_of": "now", "reference_time_ms": reference_time_ms}
    if re.search(r"\b(before|prior to|earlier than)\b", lower):
        return {"mode": "before", "valid_as_of": "query_inferred", "reference_time_ms": reference_time_ms}
    if re.search(r"\b(after|since|later than)\b", lower):
        return {"mode": "after", "valid_as_of": "query_inferred", "reference_time_ms": reference_time_ms}
    if re.search(r"\b(as of|valid as of|on)\b", lower):
        return {"mode": "valid_as_of", "valid_as_of": "query_inferred", "reference_time_ms": reference_time_ms}
    if re.search(r"\b(yesterday|tomorrow|last week|next week|last month|next month|last year|next year)\b", lower):
        return {"mode": "relative", "valid_as_of": "query_inferred", "reference_time_ms": reference_time_ms}
    return {"mode": "unbounded", "valid_as_of": "not_applicable", "reference_time_ms": reference_time_ms}


def build_structured_query_plan(
    query: str,
    *,
    question_type: str,
    secondary_index_filter_groups: list[set[str]],
    secondary_index_filter_mode: str,
    reference_time_ms: int,
) -> Json:
    secondary_filters = secondary_filter_terms_to_fields(secondary_index_filter_groups)
    return {
        "query_type": question_type,
        "secondary_filters": secondary_filters,
        "secondary_filter_groups": [sorted(group) for group in secondary_index_filter_groups],
        "secondary_filter_mode": secondary_index_filter_mode,
        "temporal_window": infer_temporal_window(query, question_type, reference_time_ms=reference_time_ms),
        "execution_order": [
            "query_understanding",
            "scope_filter",
            "secondary_index_prefilter",
            "l0_l1_node_traversal",
            "leaf_candidate_fetch",
            "embedding_similarity_time_decay_business_score",
            "budget_pack_contextpack",
        ],
    }


def _core_query_runtime() -> Any:
    try:
        from tools import matrixark_mcp_core as core
    except ModuleNotFoundError:  # Direct script execution from tools/.
        import matrixark_mcp_core as core
    return core


def infer_query_type(query: str) -> str:
    core = _core_query_runtime()
    lower = query.lower()
    if PROFILE_MEMORY_QUERY_RE.search(lower):
        return "profile_memory"
    if re.search(r"\b(benchmark|workload|latency|p50|p90|p95|p99|throughput|qps|ops/s|req/s|hit[- ]?rate|read[- ]?hit|quality|recall|precision|locomo|longmemeval|memory[- ]?quality)\b", lower):
        return "benchmark_quality"
    if core.understanding_provider() == "oss_encoder":
        return oss_encoder_query_type(query)
    if re.search(r"\b(both|together|across|between|compare|combine|sessions|multi-hop|multi session|multi-session|cross session|cross-session|previous sessions|other sessions)\b", lower):
        return "multi_hop"
    if re.search(r"\b(when|what date|which date|day|month|year|yesterday|tomorrow|last week|next week|before|after|as of|valid as of)\b", lower):
        return "date"
    if re.search(r"\b(current|currently|latest|now|still|today|valid|status|preference|prefer|likes|where does|where is)\b", lower):
        return "current_state"
    if re.search(r"\b(?:assistant|codex)\b.{0,64}\b(?:decide|decided|decision|done|implemented|fixed|pushed|committed|changed|updated|validated|verified)\b", lower):
        return "current_state"
    if re.search(r"\b(why|reason|because|feel|felt|emotion|happy|sad|angry|worried|excited)\b", lower):
        return "why_emotion"
    if re.search(r"\b(overview|summarize|summary|explore|broad|what is in|what do we know|topics|map|inventory)\b", lower):
        return "broad_exploration"
    if re.search(r"\b(evidence|quote|exactly|what did .* say|conversation|dialogue|message)\b", lower):
        return "evidence"
    if re.search(r"\b(procedure|steps?|how to|troubleshoot|debug|rollback|runbook|playbook|checklist|fix|remediate|mitigate)\b", lower):
        return "procedure"
    return "fact"


def infer_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    core = _core_query_runtime()
    if core.understanding_provider() == "oss_encoder":
        return oss_encoder_secondary_index_filter_groups(query, question_type)
    return deterministic_secondary_index_filter_groups(query, question_type)


def oss_encoder_query_type(query: str) -> str:
    core = _core_query_runtime()
    ranked = core.oss_encoder_rank_labels(query, QUERY_TYPE_LABELS, limit=2)
    if not ranked:
        return "fact"
    top = str(ranked[0]["label"])
    if len(ranked) > 1 and top == "fact" and float(ranked[1]["score"]) >= float(ranked[0]["score"]) - 0.015:
        return str(ranked[1]["label"])
    return top


def oss_encoder_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    core = _core_query_runtime()
    ranked = core.oss_encoder_rank_labels(f"{question_type}: {query}", QUERY_INDEX_LABELS, limit=5)
    selected = [str(item["label"]) for item in ranked if float(item["score"]) >= 0.46]
    if not selected and ranked:
        selected = [str(ranked[0]["label"])]
    groups: list[set[str]] = []
    by_prefix: dict[str, set[str]] = {}
    for label in selected:
        prefix = label.split(":", 1)[0]
        by_prefix.setdefault(prefix, set()).add(label)
    for labels in by_prefix.values():
        if labels and labels not in groups:
            groups.append(labels)
    return groups[:4]


def candidate_index_terms(
    record: Json,
    index_terms_by_batch: dict[Any, list[str]],
    index_terms_by_node: dict[Any, list[str]],
    index_terms_by_ref: dict[Any, list[str]] | None = None,
) -> set[str]:
    try:
        from tools.matrixark_mcp_indexing import benchmark_quality_index_terms, metadata_index_terms, non_default_classification
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_indexing import benchmark_quality_index_terms, metadata_index_terms, non_default_classification
    core = _core_query_runtime()
    terms: set[str] = set()
    index_terms_by_ref = index_terms_by_ref or {}
    record_type = record.get("record_type")

    def add_direct_layer_terms() -> None:
        for field, prefix in [
            ("memory_scope", "memory_scope"),
            ("session_continuity", "session_continuity"),
            ("extraction_phase", "extraction_phase"),
        ]:
            value = record.get(field)
            if value not in (None, "", [], {}):
                terms.add(context_index_name(prefix, value))

    if record_type == "context_event":
        terms.update(index_terms_by_batch.get(record.get("batch_id_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("event_type", record.get("event_type")))
        if not core.require_oss_understanding() and not record.get("event_type"):
            terms.add(context_index_name("event_type", core.infer_event_type(str(record.get("text", "")))))
        classification = non_default_classification(record.get("classification"))
        if classification:
            terms.add(context_index_name("classification", classification))
        terms.add(context_index_name("status", record.get("status") or "observed"))
        terms.add(context_index_name("source_type", record.get("source_type") or "message"))
        terms.update(benchmark_quality_index_terms(record.get("text"), record.get("summary_text"), record.get("event_type"), record.get("metadata")))
        add_direct_layer_terms()
    elif record_type == "context_entity":
        terms.add(context_index_name("entity_type", record.get("entity_type")))
        terms.update(benchmark_quality_index_terms(record.get("entity_name"), record.get("entity_type"), record.get("state"), record.get("text")))
        add_direct_layer_terms()
    elif record_type == "context_segment":
        terms.add(context_index_name("segment_topic", record.get("topic")))
        terms.update(benchmark_quality_index_terms(record.get("topic"), record.get("text"), record.get("summary_text")))
        add_direct_layer_terms()
    elif record_type == "context_summary":
        terms.add(context_index_name("summary_type", record.get("summary_type")))
        terms.update(benchmark_quality_index_terms(record.get("summary_type"), record.get("summary_text"), record.get("text")))
        add_direct_layer_terms()
    elif record_type == "context_compression_event":
        terms.update(index_terms_by_ref.get(record.get("compression_id_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("context_class", "compression"))
        terms.add(context_index_name("operator", record.get("operator") or "TIME_COMPRESS"))
        terms.update(benchmark_quality_index_terms(record.get("summary_text"), record.get("text")))
        add_direct_layer_terms()
    elif record_type == "resource_chunk":
        terms.update(index_terms_by_ref.get(record.get("chunk_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "resource"))
        terms.add(context_index_name("resource_type", record.get("resource_type")))
        terms.update(metadata_index_terms(record.get("metadata", {})))
    elif record_type == "skill_manifest":
        terms.update(index_terms_by_ref.get(record.get("skill_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "skill"))
        terms.add(context_index_name("resource_type", "skill"))
        terms.add(context_index_name("skill_name", record.get("name")))
        for trigger in record.get("triggers", [])[:8]:
            terms.add(context_index_name("skill_trigger", trigger))
        for tool in record.get("allowed_tools", [])[:8]:
            terms.add(context_index_name("skill_tool", tool))
    elif record_type == "skill_section":
        terms.update(index_terms_by_ref.get(record.get("section_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("source_type", "skill"))
        terms.add(context_index_name("resource_type", "skill"))
        terms.update(metadata_index_terms(record.get("metadata", {})))
    return {term for term in terms if term}


def passes_secondary_index_filters(candidate_terms: set[str], required_groups: list[set[str]], *, mode: str = "all_groups") -> bool:
    if not required_groups:
        return True
    if mode == "any_group":
        return any(bool(candidate_terms.intersection(group)) for group in required_groups)
    return all(bool(candidate_terms.intersection(group)) for group in required_groups)


def passes_applicable_secondary_index_filters(
    candidate_terms: set[str],
    required_groups: list[set[str]],
    *,
    mode: str = "all_groups",
) -> bool:
    """Apply only filter groups whose index prefix is present on this candidate."""
    candidate_prefixes = {term.split(":", 1)[0] for term in candidate_terms if ":" in term}
    candidate_is_context_asset = bool(
        candidate_terms.intersection({"source_type:resource", "source_type:skill"})
    )
    applicable_groups = [
        group
        for group in required_groups
        if candidate_prefixes.intersection({term.split(":", 1)[0] for term in group if ":" in term})
        and not (
            candidate_is_context_asset
            and {term.split(":", 1)[0] for term in group if ":" in term} == {"source_type"}
            and not candidate_terms.intersection(group)
        )
    ]
    return passes_secondary_index_filters(candidate_terms, applicable_groups, mode=mode)
