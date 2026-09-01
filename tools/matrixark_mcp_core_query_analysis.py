# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
import re

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        ACTIVE_MEMORY_GOAL_QUERY_RE,
        CODEX_OUTCOME_QUERY_RE,
        Json,
        PROFILE_MEMORY_QUERY_RE,
        PROFILE_MEMORY_STANDING_RULE_QUERY_RE,
        QUERY_INDEX_LABELS,
        QUERY_TYPE_LABELS,
        benchmark_quality_index_terms,
        codex_outcome_fact_index_terms,
        context_index_name,
        feature_scope_excludes_outcome_evidence,
        normalized_index_value,
        ordered_unique,
        oss_encoder_rank_labels,
        tokens,
        understanding_provider,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        ACTIVE_MEMORY_GOAL_QUERY_RE,
        CODEX_OUTCOME_QUERY_RE,
        Json,
        PROFILE_MEMORY_QUERY_RE,
        PROFILE_MEMORY_STANDING_RULE_QUERY_RE,
        QUERY_INDEX_LABELS,
        QUERY_TYPE_LABELS,
        benchmark_quality_index_terms,
        codex_outcome_fact_index_terms,
        context_index_name,
        feature_scope_excludes_outcome_evidence,
        normalized_index_value,
        ordered_unique,
        oss_encoder_rank_labels,
        tokens,
        understanding_provider,
    )

__all__ = ['infer_query_type', 'RESOURCE_TYPE_QUERY_ALIASES', 'UNIT_KIND_QUERY_ALIASES', 'QUERY_INDEX_STOPWORDS', 'slug_candidates_from_query', 'path_candidates_from_query', 'keyword_candidates_from_query', 'deterministic_secondary_index_filter_groups', 'infer_secondary_index_filter_groups', 'secondary_filter_terms_to_fields', 'infer_temporal_window', 'build_structured_query_plan', 'oss_encoder_query_type', 'oss_encoder_secondary_index_filter_groups']


def infer_query_type(query: str) -> str:
    lower = query.lower()
    if (
        PROFILE_MEMORY_QUERY_RE.search(lower)
        or PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(lower)
        or ACTIVE_MEMORY_GOAL_QUERY_RE.search(lower)
    ):
        return "profile_memory"
    if re.search(r"\b(benchmark|workload|latency|p50|p90|p95|p99|throughput|qps|ops/s|req/s|hit[- ]?rate|read[- ]?hit|quality|recall|precision|locomo|longmemeval|memory[- ]?quality)\b", lower):
        return "benchmark_quality"
    if understanding_provider() == "oss_encoder":
        return oss_encoder_query_type(query)
    if re.search(r"\b(both|together|across|between|compare|combine|sessions|multi-hop|multi session|multi-session|cross session|cross-session|previous sessions|other sessions)\b", lower):
        return "multi_hop"
    if re.search(r"\b(when|what date|which date|day|month|year|yesterday|tomorrow|last week|next week|before|after|as of|valid as of)\b", lower):
        return "date"
    if CODEX_OUTCOME_QUERY_RE.search(lower):
        return "evidence"
    if re.search(r"\b(current|currently|latest|now|still|today|valid|status|preference|prefer|likes|where does|where is|goal|task|requirement|user request|asked codex)\b", lower):
        return "current_state"
    if re.search(r"\b(?:assistant|codex)\b.{0,64}\b(?:decide|decided|decision|done|implemented|fixed|push(?:ed)?|commit(?:ted)?|changed|updated|validated|verified)\b", lower):
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
    return ordered_unique(candidates)[:4]


def path_candidates_from_query(query: str) -> list[str]:
    values: list[str] = []
    for raw in re.findall(r"[a-zA-Z0-9_.-]+/[a-zA-Z0-9_./-]+|[a-zA-Z0-9_.-]+\.(?:md|txt|pdf|csv|tsv|json|jsonl|yaml|yml|html|docx|pptx|xlsx|py|js|ts|go|rs|native|h)", query):
        normalized = normalized_index_value(raw)
        if normalized:
            values.append(normalized)
    return ordered_unique(values)[:6]


def _re_for_cjk_runs():
    """The same CJK class the resource parser indexes with, so query and ingest agree."""
    import re as _re_mod
    ranges = (
        "㐀-䶿一-鿿豈-﫿"
        "぀-ヿ가-힯"
    )
    return _re_mod.compile("[" + ranges + "]{2,}")


# Chinese has no spaces to split on, so the ingest side indexes overlapping character
# bigrams (see `keywords_for_text`). A bigram is TWO characters, and the length floor below
# was written for Latin words -- so every CJK posting was unmatchable and the whole Chinese
# keyword index was write-only. Measured on a CN/EN corpus: 60.2% of emitted keyword terms
# were under four characters, and a pure Chinese query produced ZERO lookup terms.
_QUERY_CJK_RUN_RE = _re_for_cjk_runs()


def keyword_candidates_from_query(query: str) -> list[str]:
    values = []
    for term in tokens(query):
        if len(term) < 4 or term in QUERY_INDEX_STOPWORDS:
            continue
        values.append(context_index_name("keyword", term))
    # Mirror the ingest side exactly: same bigrams, or the two halves cannot meet.
    seen: set[str] = set()
    for run in _QUERY_CJK_RUN_RE.findall(str(query or "")):
        for index in range(len(run) - 1):
            bigram = run[index : index + 2]
            if bigram in seen:
                continue
            seen.add(bigram)
            values.append(context_index_name("keyword", bigram))
    return ordered_unique(values)[:8]


def deterministic_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    lower = query.lower()
    feature_scope_excludes_evidence = feature_scope_excludes_outcome_evidence(query)
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
    if re.search(r"\b(prefer|preference|favorite|like|likes|love|loves|avoid|never|do not|don't|doesn't|should not|must not|standing instruction|standing preference|persistent instruction|saved preference|remember(?:ed)?)\b", lower):
        add_group(context_index_name("entity_type", "preference"), context_index_name("event_type", "preference_update"))
        add_group(context_index_name("profile_memory_kind", "durable_profile"))
        add_group(
            context_index_name("memory_selection_policy", "selected_user_profile_fact"),
            context_index_name("memory_selection_policy", "selected_assistant_profile_fact"),
        )
    if re.search(r"\b(name|nickname|call me|called|pronoun|pronouns|who am i|who is the user|address me)\b", lower):
        add_group(context_index_name("entity_type", "identity_profile"))
        add_group(context_index_name("profile_memory_class", "identity"))
        add_group(context_index_name("profile_memory_kind", "durable_profile"))
        add_group(context_index_name("memory_scope", "user_profile"))
    if re.search(r"\b(language|locale|timezone|time zone|tone|style|format|bullet|bullets|markdown|concise|brief|detailed|reply|respond|answer style|communication style)\b", lower):
        add_group(context_index_name("entity_type", "communication_profile"))
        add_group(context_index_name("profile_memory_class", "communication"))
        add_group(context_index_name("profile_memory_kind", "durable_profile"))
        add_group(context_index_name("memory_scope", "user_profile"))
    if re.search(r"\b(workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b", lower):
        add_group(context_index_name("entity_type", "workspace_profile"))
        add_group(context_index_name("profile_memory_class", "workspace"))
        add_group(context_index_name("profile_memory_kind", "durable_profile"))
        add_group(context_index_name("memory_scope", "user_profile"))
    if re.search(r"\b(mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionality|functionalities|functionality only|algorithms?|algos?|implementation focus|no testing|no teseting|no tests?|skip tests?|without tests?|no monitoring|no debugging|no debug|no evidence|no evident|no eviden[ct]e|feature work only|code changes only|session memory|profile memory|cross[- ]session memory|live ingestion|memory ingestion|threshold|idle batch|batch extraction|profile promotion|retrieval budgets?|memory retrieval|secondary indexes?|context events?|context entit(?:y|ies)|context summaries?|contextpacks?)\b", lower) or ACTIVE_MEMORY_GOAL_QUERY_RE.search(lower):
        add_group(context_index_name("entity_type", "memory_feature_profile"))
        add_group(context_index_name("profile_memory_class", "memory_feature"))
        add_group(context_index_name("profile_memory_kind", "memory_feature"))
        add_group(
            context_index_name("memory_selection_policy", "selected_user_prompt"),
            context_index_name("memory_selection_policy", "selected_user_profile_fact"),
            context_index_name("memory_selection_policy", "selected_assistant_profile_fact"),
            context_index_name("memory_selection_policy", "selected_profile_current_state"),
        )
        add_group(
            context_index_name("memory_layer", "pending_async_memory_feature_event"),
            context_index_name("memory_layer", "same_session_memory_feature_event"),
            context_index_name("memory_layer", "same_session_memory_feature_segment"),
            context_index_name("memory_layer", "same_session_memory_feature_entity"),
            context_index_name("memory_layer", "same_session_memory_feature_summary"),
            context_index_name("memory_layer", "same_session_memory_feature_compression"),
            context_index_name("memory_layer", "cross_session_memory_feature_event"),
            context_index_name("memory_layer", "cross_session_memory_feature_segment"),
            context_index_name("memory_layer", "cross_session_memory_feature_entity"),
            context_index_name("memory_layer", "cross_session_memory_feature_summary"),
            context_index_name("memory_layer", "cross_session_memory_feature_compression"),
        )
        add_group(context_index_name("memory_scope", "user_profile"))
    if re.search(r"\b(friend|partner|mother|father|sister|brother|wife|husband|manager|teammate|relationship|family|child|children|son|daughter|pet)\b", lower):
        add_group(context_index_name("entity_type", "relationship"), context_index_name("entity_type", "family_profile"))
    if re.search(r"\b(job|role|work|works|position|status|company|employer)\b", lower):
        add_group(context_index_name("entity_type", "job_status"), context_index_name("event_type", "status_update"))
    if re.search(r"\b(plan|plans|planning|going to|schedule|next|goal|task|requirement|user request|user asked|asked codex|asked you|implement(?:ed)?|fix(?:ed)?)\b", lower):
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
    if (
        PROFILE_MEMORY_QUERY_RE.search(lower)
        or PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(lower)
        or ACTIVE_MEMORY_GOAL_QUERY_RE.search(lower)
    ):
        add_group(context_index_name("memory_scope", "user_profile"))
        add_group(context_index_name("session_continuity", "cross_session"))
        add_group(context_index_name("profile_entity_current", "true"), context_index_name("profile_summary_current", "true"))
        profile_memory_class_terms = [
            context_index_name("profile_memory_class", "identity"),
            context_index_name("profile_memory_class", "communication"),
            context_index_name("profile_memory_class", "workspace"),
            context_index_name("profile_memory_class", "memory_feature"),
            context_index_name("profile_memory_class", "preference"),
            context_index_name("profile_memory_class", "personal_context"),
            context_index_name("profile_memory_class", "task_context"),
        ]
        if not feature_scope_excludes_evidence:
            profile_memory_class_terms.append(context_index_name("profile_memory_class", "codex_outcome"))
        add_group(*profile_memory_class_terms)
        profile_memory_kind_terms = [
            context_index_name("profile_memory_kind", "durable_profile"),
            context_index_name("profile_memory_kind", "memory_feature"),
        ]
        if not feature_scope_excludes_evidence:
            profile_memory_kind_terms.append(context_index_name("profile_memory_kind", "codex_outcome"))
        add_group(*profile_memory_kind_terms)
        add_group(
            context_index_name("memory_layer", "pending_async_memory_feature_event"),
            context_index_name("memory_layer", "same_session_memory_feature_event"),
            context_index_name("memory_layer", "same_session_memory_feature_segment"),
            context_index_name("memory_layer", "same_session_memory_feature_entity"),
            context_index_name("memory_layer", "same_session_memory_feature_summary"),
            context_index_name("memory_layer", "same_session_memory_feature_compression"),
            context_index_name("memory_layer", "cross_session_memory_feature_event"),
            context_index_name("memory_layer", "cross_session_memory_feature_segment"),
            context_index_name("memory_layer", "cross_session_memory_feature_entity"),
            context_index_name("memory_layer", "cross_session_memory_feature_summary"),
            context_index_name("memory_layer", "cross_session_memory_feature_compression"),
        )
        add_group(
            context_index_name("memory_selection_policy", "selected_user_profile_fact"),
            context_index_name("memory_selection_policy", "selected_assistant_profile_fact"),
        )
    if re.search(r"\b(cross[- ]session|across sessions|between sessions|multi[- ]session|long[- ]term)\b", lower):
        add_group(context_index_name("session_continuity", "cross_session"))
    if re.search(r"\b(session[- ]local|same[- ]session|current session|this session|session specific|session-specific)\b", lower):
        add_group(context_index_name("memory_scope", "session"))
        add_group(context_index_name("session_continuity", "same_session"))
    if not feature_scope_excludes_evidence and (
        CODEX_OUTCOME_QUERY_RE.search(lower)
        or re.search(r"\b(assistant|decision|decided|done|implemented|fixed|changed|updated|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|validated|verified|push(?:ed)?|publish(?:ed)?|deploy(?:ed)?|release(?:d)?|merge(?:d)?|commit(?:ted)?|rebase(?:d)?|failed|blocked|final answer|what did codex|what did you|what did we|what was done|next action)\b", lower)
    ):
        add_group(
            context_index_name("entity_type", "assistant_decision"),
            context_index_name("entity_type", "codex_next_action"),
            context_index_name("entity_type", "codex_blocker"),
            context_index_name("entity_type", "codex_validation"),
            context_index_name("entity_type", "codex_publish_outcome"),
            context_index_name("entity_type", "codex_code_change"),
            context_index_name("entity_type", "codex_benchmark_result"),
            context_index_name("event_type", "assistant_response"),
            context_index_name("source_role", "assistant"),
            context_index_name("entity_type", "tool_evidence"),
            context_index_name("event_type", "tool_evidence"),
            context_index_name("source_role", "tool"),
            context_index_name("memory_selection_policy", "selected_assistant_decision_outcome_only"),
            context_index_name("memory_selection_policy", "selected_tool_evidence_only"),
            context_index_name("source_type", "message"),
            context_index_name("profile_memory_kind", "codex_outcome"),
            context_index_name("memory_layer", "pending_async_codex_outcome_event"),
            context_index_name("memory_layer", "same_session_codex_outcome_event"),
            context_index_name("memory_layer", "cross_session_codex_outcome_event"),
            context_index_name("memory_layer", "same_session_codex_outcome_segment"),
            context_index_name("memory_layer", "cross_session_codex_outcome_segment"),
            context_index_name("memory_layer", "cross_session_codex_outcome_entity"),
            context_index_name("memory_layer", "cross_session_codex_outcome_summary"),
            context_index_name("memory_layer", "cross_session_codex_outcome_compression"),
        )
        outcome_terms = codex_outcome_fact_index_terms(query)
        if outcome_terms:
            add_group(*outcome_terms)
    if not feature_scope_excludes_evidence and re.search(r"\b(benchmark|workload|latency|p50|p90|p95|p99|throughput|qps|ops/s|req/s|hit[- ]?rate|read[- ]?hit|quality|recall|precision|locomo|longmemeval|memory[- ]?quality)\b", lower):
        add_group(
            context_index_name("entity_type", "tool_evidence"),
            context_index_name("event_type", "tool_evidence"),
            context_index_name("entity_type", "assistant_decision"),
            context_index_name("entity_type", "codex_validation"),
            context_index_name("entity_type", "codex_benchmark_result"),
            context_index_name("entity_type", "codex_publish_outcome"),
            context_index_name("event_type", "assistant_response"),
        )
        add_group(context_index_name("source_role", "tool"), context_index_name("source_role", "assistant"))
        add_group(
            context_index_name("memory_selection_policy", "selected_tool_evidence_only"),
            context_index_name("memory_selection_policy", "selected_assistant_decision_outcome_only"),
            context_index_name("profile_memory_kind", "codex_outcome"),
            context_index_name("memory_layer", "pending_async_codex_outcome_event"),
            context_index_name("memory_layer", "same_session_codex_outcome_event"),
            context_index_name("memory_layer", "cross_session_codex_outcome_event"),
            context_index_name("memory_layer", "same_session_codex_outcome_segment"),
            context_index_name("memory_layer", "cross_session_codex_outcome_segment"),
        )
        metric_terms = benchmark_quality_index_terms(query)
        if metric_terms:
            add_group(*metric_terms)
        if re.search(r"\b(hit[- ]?rate|read[- ]?hit|quality|recall|precision|locomo|longmemeval|memory[- ]?quality)\b", lower):
            add_group(context_index_name("memory_scope", "user_profile"), context_index_name("session_continuity", "cross_session"))
    if not feature_scope_excludes_evidence and re.search(r"\b(tool|evidence|test|tests|passed|failed|exit code|commit|pushed|push|published|deploy(?:ed)?|deployment|release(?:d)?|merge(?:d)?|rebase(?:d)?|configured|configuration|enabled|disabled|installed|migrated|recovered|restored|cleaned|promoted|indexed|budgeted|batched|flushed|validation|benchmark|blocker)\b", lower):
        add_group(
            context_index_name("entity_type", "tool_evidence"),
            context_index_name("entity_type", "codex_validation"),
            context_index_name("entity_type", "codex_blocker"),
            context_index_name("entity_type", "codex_publish_outcome"),
            context_index_name("entity_type", "codex_benchmark_result"),
            context_index_name("event_type", "tool_evidence"),
            context_index_name("source_role", "tool"),
            context_index_name("profile_memory_kind", "codex_outcome"),
            context_index_name("memory_layer", "pending_async_codex_outcome_event"),
            context_index_name("memory_layer", "same_session_codex_outcome_event"),
            context_index_name("memory_layer", "cross_session_codex_outcome_event"),
            context_index_name("memory_layer", "same_session_codex_outcome_segment"),
            context_index_name("memory_layer", "cross_session_codex_outcome_segment"),
        )
        outcome_terms = codex_outcome_fact_index_terms(query)
        if outcome_terms:
            add_group(*outcome_terms)
    if re.search(r"\b(user prompt|prompt|user request|user asked|request|asked codex|ask codex|did i ask|what did i ask|asked you)\b", lower):
        add_group(
            context_index_name("event_type", "user_prompt"),
            context_index_name("source_role", "user"),
            context_index_name("entity_type", "current_plan"),
            context_index_name("memory_selection_policy", "selected_user_prompt"),
        )
    if re.search(r"\b(selected|bounded|retained|extracted|memory selection|memory-selection)\b", lower):
        if re.search(r"\b(user prompt|prompt|user request|request)\b", lower):
            add_group(context_index_name("memory_selection_policy", "selected_user_prompt"))
        if re.search(r"\b(profile fact|preference|standing instruction|standing preference|remembered|user profile|long[- ]term memory)\b", lower):
            add_group(context_index_name("memory_selection_policy", "selected_user_profile_fact"))
            add_group(context_index_name("memory_selection_policy", "selected_assistant_profile_fact"))
        if re.search(r"\b(assistant|decision|outcome|final answer|what did codex|what did you|what did we|what was done|done|fixed|implemented)\b", lower):
            add_group(context_index_name("memory_selection_policy", "selected_assistant_decision_outcome_only"))
        if not feature_scope_excludes_evidence and re.search(r"\b(tool|tool output|tool result|evidence|test|tests|exit code|commit|pushed|push|rebase|validation|benchmark|blocker)\b", lower):
            add_group(context_index_name("memory_selection_policy", "selected_tool_evidence_only"))
    if re.search(r"\b(lossy|truncated|dropped|omitted|shortened|summarized output|large output)\b", lower):
        add_group(context_index_name("memory_selection_quality", "lossy"))
    if re.search(r"\b(complete|untruncated|full selected|no loss|lossless)\b", lower):
        add_group(context_index_name("memory_selection_quality", "complete"))
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
        trigger_terms = [context_index_name("skill_trigger", term) for term in ordered_unique(trigger_values)]
        if tool_terms:
            add_group(*tool_terms[:6])
        if trigger_terms:
            add_group(*trigger_terms[:24])
    if question_type == "evidence":
        add_group(context_index_name("source_type", "message"), context_index_name("source_type", "feedback"))
    return groups


# The index KINDS a query can ever ask for. `passes_secondary_index_filters` intersects a
# candidate's terms with the groups this module infers, so a term whose kind never appears in a
# group cannot narrow a search or earn the hint boost -- whatever its value.
#
# This lives next to the inference it mirrors on purpose. If a kind is added to the inference and
# not here, ingest stops writing it and the query that needs it narrows to nothing, silently; the
# two are only correct together.
INFERABLE_SECONDARY_INDEX_KINDS = frozenset({
    "classification",
    "entity_type",
    "event_type",
    "memory_layer",
    "memory_scope",
    "memory_selection_policy",
    "profile_entity_current",
    "profile_memory_class",
    "profile_memory_kind",
    "profile_summary_current",
    "segment_topic",
    "session_continuity",
    "source_role",
    "source_type",
})


def index_term_is_consultable(term: str) -> bool:
    """Whether a query could ever match this term."""
    kind, separator, _ = str(term).partition(":")
    return bool(separator) and kind in INFERABLE_SECONDARY_INDEX_KINDS


def infer_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    if understanding_provider() == "oss_encoder":
        return oss_encoder_secondary_index_filter_groups(query, question_type)
    return deterministic_secondary_index_filter_groups(query, question_type)


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


def oss_encoder_query_type(query: str) -> str:
    ranked = oss_encoder_rank_labels(query, QUERY_TYPE_LABELS, limit=2)
    if not ranked:
        return "fact"
    top = str(ranked[0]["label"])
    if len(ranked) > 1 and top == "fact" and float(ranked[1]["score"]) >= float(ranked[0]["score"]) - 0.015:
        return str(ranked[1]["label"])
    return top


def oss_encoder_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    ranked = oss_encoder_rank_labels(f"{question_type}: {query}", QUERY_INDEX_LABELS, limit=5)
    selected = [str(item["label"]) for item in ranked if float(item["score"]) >= 0.46]
    if not selected and ranked:
        selected = [str(ranked[0]["label"])]
    groups: list[set[str]] = deterministic_secondary_index_filter_groups(query, question_type)
    by_prefix: dict[str, set[str]] = {}
    for label in selected:
        prefix = label.split(":", 1)[0]
        by_prefix.setdefault(prefix, set()).add(label)
    for labels in by_prefix.values():
        if labels and labels not in groups:
            groups.append(labels)
    return groups[:8]


