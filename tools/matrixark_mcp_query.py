#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
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
    "profile_memory": "question asks user profile long term memory profile entities profile summaries cross session memories across sessions feature parity features only no testing monitoring debugging evidence",
    "benchmark_quality": "question asks benchmark quality hit rate latency p50 p99 throughput locomo longmemeval memory evaluation",
    "date": "question asks when date before after yesterday tomorrow week month year",
    "current_state": "question asks current latest now still status preference location role valid state active goal task request requirement",
    "why_emotion": "question asks why reason feeling emotion because",
    "evidence": "question asks quote exact message evidence what did someone say",
    "procedure": "question asks procedure steps troubleshoot debug rollback runbook checklist how to fix",
    "broad_exploration": "question asks overview summarize broad exploration topics inventory what is known",
    "multi_hop": "question requires combining multiple sessions people facts cross conversation reasoning",
    "fact": "question asks a direct factual answer",
}

PROFILE_MEMORY_QUERY_RE = re.compile(
    r"\b(user profile|profile memory|long[- ]term memor(?:y|ies)|cross[- ]session memor(?:y|ies)|profile entit(?:y|ies)|profile summar(?:y|ies)|identity profile|communication profile|workspace profile|mem0|memory feature parity|feature parity|feature[- ]focused memor(?:y|ies)|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionality only|memory functionalit(?:y|ies)|memory algorithms?|memory algos?|no testing|no teseting|no monitoring|no debugging|no evidence|no evident|session memory|remember about me|remember about|what should (?:i|you|we) remember|standing instructions?|standing preferences?|persistent instructions?|saved preferences?|know about (?:me|my|the user)|what (?:have|did) i (?:tell|told) you|what (?:are|were|do|did) my preferences|what do i prefer|do i prefer|my preferences|my .*?(?:policy|policies|instruction|instructions|preference|preferences)|told you before|from previous sessions?|across sessions?|across conversations?|between conversations?|how should (?:you|codex) (?:address|reply|respond|answer)|what (?:is|are) my (?:name|nickname|pronouns?|preferred language|preferred format|communication style|response style|workspace rules?|repo rules?|repository rules?|branch rules?|build rules?|deployment rules?)|what (?:workspace|repo|repository|branch|build|deployment|github|remote) rules? (?:do|should) (?:you|codex) remember|what (?:workflow|workflows|rules?|instructions?|preferences?) (?:do|should) (?:you|codex) follow)\b"
)

PROFILE_MEMORY_STANDING_RULE_QUERY_RE = re.compile(
    r"\b(?:which|what|where|should|must|need)\b.{0,80}\b(?:repo|repository|folder|workspace|worktree|ubuntu|wsl|linux|windows|branch|remote|github|main branch|build|deploy|deployment|push)\b.{0,80}\b(?:use|work|build|push|commit|rebase|download|clone|store|keep|follow|prefer)\b"
    r"|\b(?:use|work|build|push|commit|rebase|download|clone|store|keep|follow|prefer)\b.{0,80}\b(?:repo|repository|folder|workspace|worktree|ubuntu|wsl|linux|windows|branch|remote|github|main branch|build|deploy|deployment|push)\b.{0,80}\b(?:which|what|where|should|must|need)\b"
    r"|\b(?:which|what|where|should|must|need)\b.{0,80}\b(?:use|work|build|push|commit|rebase|download|clone|store|keep|follow|prefer)\b.{0,80}\b(?:repo|repository|folder|workspace|worktree|ubuntu|wsl|linux|windows|branch|remote|github|main branch|temporalstore|rustraft|matrixark)\b"
    r"|\b(?:what|which|how)\b.{0,80}\b(?:always|default|standing|persistent)\b.{0,80}\b(?:do|behav(?:e|ior)|rules?|instructions?|preferences?|follow|remember)\b"
    r"|\b(?:what|which|how)\b.{0,80}\b(?:do|behav(?:e|ior)|follow|remember)\b.{0,80}\b(?:always|by default|default|standing|persistent)\b"
    r"|\b(?:always|default|standing|persistent)\b.{0,80}\b(?:rules?|instructions?|preferences?|behaviou?r|workflow|workflows)\b"
)

ACTIVE_MEMORY_GOAL_QUERY_RE = re.compile(
    r"\b(?:active|current|latest|next|ongoing|standing|persistent)\b.{0,80}\b(?:memory|retrieval|extraction|ingestion|context)\b.{0,80}\b(?:goal|focus|priority|work|feature|functionality|implementation|direction|instruction|preference)\b"
    r"|\b(?:what|which|how)\b.{0,80}\b(?:should|must|need|keep|continue|focus|prioriti[sz]e|work on)\b.{0,80}\b(?:memory|retrieval|extraction|ingestion|context)\b.{0,80}\b(?:goal|focus|priority|feature|functionality|implementation|direction)\b"
    r"|\b(?:memory|retrieval|extraction|ingestion|context)\b.{0,80}\b(?:goal|focus|priority|feature|functionality|implementation|direction)\b.{0,80}\b(?:active|current|latest|next|ongoing|standing|persistent)\b"
)

FEATURE_SCOPE_EXCLUSION_RE = re.compile(
    r"\b(?:no|not|skip|without|exclude|excluding|ignore|omit)\s+"
    r"(?:testing|teseting|tests?|monitoring|debugging|debug|evidence|evident|validation|benchmarks?)\b"
)

CODEX_OUTCOME_QUERY_RE = re.compile(r"\b(?:codex|assistant|agent)\b.{0,80}\b(?:implement(?:ed)?|fixed|changed|updated|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|validated|verified|push(?:ed)?|publish(?:ed)?|deploy(?:ed)?|release(?:d)?|merge(?:d)?|commit(?:ted)?|rebase(?:d)?|failed|blocked|blocker|done|outcome|decision|decided|next action)\b|\bwhat (?:was|were|did)\b.{0,80}\b(?:implement(?:ed)?|fixed|changed|updated|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|validated|verified|push(?:ed)?|publish(?:ed)?|deploy(?:ed)?|release(?:d)?|merge(?:d)?|commit(?:ted)?|failed|blocked|done)\b|\bwhat did (?:you|we)\b.{0,80}\b(?:implement|fix|change|update|configure|enable|disable|install|migrate|recover|restore|clean|validate|verify|push|publish|deploy|release|merge|commit|rebase|fail|block|decide|do)\b|\b(?:show|find|retrieve|summari[sz]e)\b.{0,80}\b(?:assistant decision|tool evidence|validation evidence|pushed commit|blocked work|failed validation|validation result|test result|tests? passed|pushed commit|deployment result|deploy(?:ed)? result|install(?:ed)? result|configured result|configuration result|recovery result|migration result|merge result|publish(?:ed)? result|release result|outcome facts?)\b|\b(?:what|which|show|find|retrieve|summari[sz]e)\b.{0,80}\b(?:tests? passed|validation (?:passed|result|evidence)|pushed commit|commit (?:was )?pushed|push result|rebase result|deploy(?:ed)? result|deployment result|install(?:ed)? result|configured result|configuration result|recovery result|migration result|merge result|publish(?:ed)? result|release result)\b|\bwhat (?:failed|was blocked|blocked)\b.{0,80}\b(?:memory work|work|validation|commit|push|deploy|deployment|install|configuration|migration|recovery|merge|publish|release|tool|codex|temporalstore)\b")

QUERY_INDEX_LABELS: dict[str, str] = {
    "entity_type:location": "location city moved lives staying where user is",
    "entity_type:preference": "preference prefer favorite likes language tool choice",
    "event_type:preference_update": "preference update changed choice likes prefers",
    "entity_type:relationship": "relationship manager sister brother teammate family person",
    "entity_type:family_profile": "family pet dog cat child household",
    "entity_type:job_status": "job role work status position responsibility",
    "event_type:status_update": "job status role work update",
    "entity_type:current_plan": "plan current plan goal user request requirement upcoming task schedule next milestone",
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
    "entity_type:memory_feature_profile": "memory feature parity mem0 extraction retrieval profile session threshold idle batch live ingestion profile promotion retrieval budget secondary index context event context entity context summary feature focused features only no testing monitoring debugging evidence",
    "entity_type:tool_evidence": "tool evidence tests passed failed exit code commit push rebase validation benchmark blocker",
    "event_type:tool_evidence": "tool event evidence tests passed failed exit code commit push rebase validation benchmark blocker",
    "memory_scope:user_profile": "user profile long term memory profile entity profile summary durable cross session user state",
    "memory_scope:session": "same active session local conversation memory session scoped context",
    "session_continuity:cross_session": "cross session previous sessions other conversations across tasks persistent memory bridge",
    "session_continuity:same_session": "same session current conversation active turn local context",
    "profile_entity_current:true": "current profile entity latest durable user state standing preference instruction",
    "profile_summary_current:true": "current profile summary latest durable long term memory profile overview",
    "profile_memory_class:memory_feature": "memory feature profile feature parity extraction retrieval budget live ingestion profile promotion secondary index context event context entity context summary",
    "profile_memory_kind:memory_feature": "memory feature durable preference feature focused features only no testing no monitoring no debugging no evidence no evident live ingestion profile promotion retrieval budget",
    "memory_selection_policy:selected_profile_current_state": "selected profile current state standing instruction durable preference",
    "memory_selection_policy:selected_user_prompt": "selected user prompt explicit request instruction preference",
    "memory_selection_policy:selected_user_profile_fact": "selected user profile fact standing preference durable instruction",
    "memory_selection_policy:selected_assistant_profile_fact": "selected assistant stated user profile preference standing instruction durable memory",
    "memory_selection_policy:selected_assistant_decision_outcome_only": "selected assistant decision outcome final answer implemented changed pushed",
    "memory_selection_policy:selected_tool_evidence_only": "selected tool evidence validation test result commit push benchmark",
    "codex_outcome:next": "codex assistant next action follow up next step",
    "codex_outcome:blocker": "codex blocked blocker failed missing rejected error fatal",
    "codex_outcome:validation": "codex validation tests passed py_compile unittest pytest cargo test",
    "codex_outcome:outcome": "codex outcome pushed commit origin main done",
    "codex_outcome:changed": "codex changed updated implemented added removed fixed",
    "codex_outcome:benchmark": "codex benchmark p50 p99 latency throughput",
    "source_role:user": "user prompt explicit instruction preference request",
    "source_role:assistant": "assistant decision final answer outcome implementation",
    "source_role:tool": "tool evidence command output validation benchmark",
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
    for raw in re.findall(r"[a-zA-Z0-9_.-]+/[a-zA-Z0-9_./-]+|[a-zA-Z0-9_.-]+\.(?:md|txt|pdf|csv|tsv|json|jsonl|yaml|yml|html|docx|pptx|xlsx|py|js|ts|go|rs|native|h)", query):
        normalized = normalized_index_value(raw)
        if normalized:
            values.append(normalized)
    return _ordered_unique(values)[:6]


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
    return _ordered_unique(values)[:8]


def codex_outcome_fact_index_terms(*values: Any) -> set[str]:
    text = " ".join(str(value or "") for value in values)
    lower = text.lower()
    terms: set[str] = set()
    has_real_blocker = bool(
        re.search(r"\b(?:blocked|blocker|failure|error|missing|rejected|fatal)\b", lower)
        or re.search(r"\b[1-9]\d*\s+(?:failed|failures|errors)\b", lower)
        or (re.search(r"\bfailed\b", lower) and not re.search(r"\b0\s+failed\b", lower))
    )
    if re.search(r"\bnext\b", lower):
        terms.add(context_index_name("codex_outcome", "next"))
    if has_real_blocker:
        terms.add(context_index_name("codex_outcome", "blocker"))
    if re.search(r"\b(?:validation|tests?|py_compile|unittest|pytest|cargo test|cargo check)\b", lower):
        terms.add(context_index_name("codex_outcome", "validation"))
    if re.search(r"\b(?:outcome|pushed|commit\s+[0-9a-f]{7,40}|origin/main|refs/heads/main|[0-9a-f]{7,40}\.\.[0-9a-f]{7,40}\s+(?:head|[^\s]+)\s*->\s*(?:main|origin/main)|[0-9a-f]{7,40}\s+(?:head|[^\s]+)\s*->\s*(?:main|origin/main))\b", lower):
        terms.add(context_index_name("codex_outcome", "outcome"))
    if re.search(r"\b(?:changed|updated|implemented|added|removed|fixed)\b", lower):
        terms.add(context_index_name("codex_outcome", "changed"))
    if re.search(r"\b(?:benchmark|p50|p99|throughput|latency)\b", lower):
        terms.add(context_index_name("codex_outcome", "benchmark"))
    return terms


def deterministic_secondary_index_filter_groups(query: str, question_type: str) -> list[set[str]]:
    lower = query.lower()
    feature_scope_excludes_evidence = bool(FEATURE_SCOPE_EXCLUSION_RE.search(lower))
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
        add_group(context_index_name("profile_memory_kind", "durable_profile"))
        add_group(context_index_name("memory_selection_policy", "selected_user_profile_fact"))
    if re.search(r"\b(mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionality|functionalities|functionality only|algorithms?|algos?|implementation focus|no testing|no teseting|no tests?|skip tests?|without tests?|no monitoring|no debugging|no debug|no evidence|no evident|no eviden[ct]e|feature work only|code changes only|session memory|profile memory|cross[- ]session memory|live ingestion|memory ingestion|threshold|idle batch|batch extraction|profile promotion|retrieval budgets?|memory retrieval|secondary indexes?|context events?|context entit(?:y|ies)|context summaries?|contextpacks?)\b", lower) or ACTIVE_MEMORY_GOAL_QUERY_RE.search(lower):
        add_group(context_index_name("entity_type", "memory_feature_profile"))
        add_group(context_index_name("profile_memory_class", "memory_feature"))
        add_group(context_index_name("profile_memory_kind", "memory_feature"))
        add_group(context_index_name("memory_layer", "cross_session_memory_feature_entity"))
        add_group(context_index_name("memory_scope", "user_profile"))
        add_group(context_index_name("memory_selection_policy", "selected_user_profile_fact"))
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
            context_index_name("entity_type", "tool_evidence"),
            context_index_name("event_type", "tool_evidence"),
            context_index_name("source_role", "assistant"),
            context_index_name("source_role", "tool"),
            context_index_name("memory_selection_policy", "selected_assistant_decision_outcome_only"),
            context_index_name("memory_selection_policy", "selected_tool_evidence_only"),
            context_index_name("source_type", "message"),
            context_index_name("profile_memory_kind", "codex_outcome"),
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
    if (
        re.search(r"\b(user profile|profile memory|long[- ]term memory|profile entity|profile entities|profile summary|profile summaries|current profile|latest profile|identity profile|communication profile|workspace profile|preferred language|preferred format|communication style|response style|workspace rules?|repo rules?|repository rules?|branch rules?|build rules?|deployment rules?)\b", lower)
        or question_type == "profile_memory"
        or PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(lower)
        or ACTIVE_MEMORY_GOAL_QUERY_RE.search(lower)
    ):
        add_group(context_index_name("memory_scope", "user_profile"), context_index_name("session_continuity", "cross_session"))
        add_group(context_index_name("profile_entity_current", "true"), context_index_name("profile_summary_current", "true"))
        profile_memory_kind_terms = [
            context_index_name("profile_memory_kind", "durable_profile"),
            context_index_name("profile_memory_kind", "memory_feature"),
        ]
        if not feature_scope_excludes_evidence:
            profile_memory_kind_terms.append(context_index_name("profile_memory_kind", "codex_outcome"))
        add_group(*profile_memory_kind_terms)
        add_group(
            context_index_name("memory_selection_policy", "selected_user_profile_fact"),
            context_index_name("memory_selection_policy", "selected_assistant_profile_fact"),
        )
    if question_type in {"current_state", "latest"} and re.search(r"\b(profile|cross[- ]session|long[- ]term|memory|entity|entities)\b", lower):
        add_group(context_index_name("profile_entity_current", "true"), context_index_name("profile_summary_current", "true"))
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
            context_index_name("memory_selection_policy", "selected_user_profile_fact"),
        )
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
    if (
        PROFILE_MEMORY_QUERY_RE.search(lower)
        or PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(lower)
        or ACTIVE_MEMORY_GOAL_QUERY_RE.search(lower)
    ):
        return "profile_memory"
    if re.search(r"\b(benchmark|workload|latency|p50|p90|p95|p99|throughput|qps|ops/s|req/s|hit[- ]?rate|read[- ]?hit|quality|recall|precision|locomo|longmemeval|memory[- ]?quality)\b", lower):
        return "benchmark_quality"
    if core.understanding_provider() == "oss_encoder":
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
    groups: list[set[str]] = deterministic_secondary_index_filter_groups(query, question_type)
    by_prefix: dict[str, set[str]] = {}
    for label in selected:
        prefix = label.split(":", 1)[0]
        by_prefix.setdefault(prefix, set()).add(label)
    for labels in by_prefix.values():
        if labels and labels not in groups:
            groups.append(labels)
    return groups[:8]


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

    def add_source_lineage_terms() -> None:
        role_values: set[str] = set()
        scalar_role = core.normalize_message_role(record.get("source_role"))
        if scalar_role:
            role_values.add(scalar_role)
        if isinstance(record.get("source_roles"), list):
            role_values.update(
                core.normalize_message_role(role)
                for role in record.get("source_roles", [])[:16]
                if core.normalize_message_role(role)
            )
        if isinstance(record.get("source_role_counts"), dict):
            for role, count in list(record.get("source_role_counts", {}).items())[:16]:
                try:
                    if int(count or 0) <= 0:
                        continue
                except (TypeError, ValueError):
                    continue
                role_name = core.normalize_message_role(role)
                if role_name:
                    role_values.add(role_name)
        for role in sorted(role_values):
            terms.add(context_index_name("source_role", role))
        for field, prefix in [
            ("source_hook_types", "hook_type"),
            ("source_codex_events", "codex_event"),
            ("source_memory_selection_policies", "memory_selection_policy"),
            ("source_memory_scopes", "memory_scope"),
            ("source_session_continuities", "session_continuity"),
            ("source_extraction_phases", "extraction_phase"),
            ("source_profile_promotion_policies", "profile_promotion_policy"),
            ("source_profile_promotion_blockers", "profile_promotion_blocker"),
        ]:
            for value in record.get(field, [])[:16] if isinstance(record.get(field), list) else []:
                terms.add(context_index_name(prefix, value))
        for field, prefix in [
            ("source_hook_type_counts", "hook_type"),
            ("source_codex_event_counts", "codex_event"),
            ("source_memory_selection_policy_counts", "memory_selection_policy"),
        ]:
            counts = record.get(field)
            if not isinstance(counts, dict):
                continue
            for value, count in list(counts.items())[:16]:
                try:
                    if int(count or 0) <= 0:
                        continue
                except (TypeError, ValueError):
                    continue
                terms.add(context_index_name(prefix, value))
        try:
            lossy_count = int(record.get("source_memory_selection_lossy_count") or 0)
        except (TypeError, ValueError):
            lossy_count = 0
        try:
            complete_count = int(record.get("source_memory_selection_complete_count") or 0)
        except (TypeError, ValueError):
            complete_count = 0
        if lossy_count > 0:
            terms.add(context_index_name("memory_selection_quality", "lossy"))
        if complete_count > 0:
            terms.add(context_index_name("memory_selection_quality", "complete"))
        if lossy_count > 0 and complete_count > 0:
            terms.add(context_index_name("memory_selection_quality", "mixed"))

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
        add_source_lineage_terms()
    elif record_type == "context_entity":
        terms.add(context_index_name("entity_type", record.get("entity_type")))
        if record.get("profile_memory_class") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_class", record.get("profile_memory_class")))
        if record.get("profile_memory_kind") not in (None, "", [], {}):
            terms.add(context_index_name("profile_memory_kind", record.get("profile_memory_kind")))
        entity_name = record.get("entity_name")
        if entity_name not in (None, "", [], {}):
            terms.add(context_index_name("entity_name", normalized_index_value(entity_name).replace(":", "_")))
        if bool(record.get("profile_entity_current")):
            terms.add(context_index_name("profile_entity_current", "true"))
        terms.update(codex_outcome_fact_index_terms(record.get("entity_name"), record.get("entity_type"), record.get("state"), record.get("text")))
        terms.update(benchmark_quality_index_terms(record.get("entity_name"), record.get("entity_type"), record.get("state"), record.get("text")))
        add_direct_layer_terms()
        add_source_lineage_terms()
    elif record_type == "context_segment":
        terms.add(context_index_name("segment_topic", record.get("topic")))
        terms.update(benchmark_quality_index_terms(record.get("topic"), record.get("text"), record.get("summary_text")))
        add_direct_layer_terms()
        add_source_lineage_terms()
    elif record_type == "context_summary":
        terms.add(context_index_name("summary_type", record.get("summary_type")))
        if bool(record.get("profile_summary_current")):
            terms.add(context_index_name("profile_summary_current", "true"))
        terms.update(benchmark_quality_index_terms(record.get("summary_type"), record.get("summary_text"), record.get("text")))
        add_direct_layer_terms()
        add_source_lineage_terms()
    elif record_type == "context_compression_event":
        terms.update(index_terms_by_ref.get(record.get("compression_id_hash"), []))
        terms.update(index_terms_by_node.get(record.get("node_hash"), []))
        terms.add(context_index_name("context_class", "compression"))
        terms.add(context_index_name("operator", record.get("operator") or "TIME_COMPRESS"))
        terms.update(benchmark_quality_index_terms(record.get("summary_text"), record.get("text")))
        add_direct_layer_terms()
        add_source_lineage_terms()
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
