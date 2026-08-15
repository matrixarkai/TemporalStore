# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of run_locomo_ingest_once.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import re

try:  # package path (tools.run_locomo_ingest_once)
    from .run_locomo_ingest_once import (
        ACTIVE_RETRIEVAL_EMBEDDING_MODEL,
        RetrievalBudgetConfig,
        _RETRIEVAL_ENCODER,
        add_domain_reference_sources,
        add_temporal_reference_sources,
        compact_retrieval_source,
        defaultdict,
        dense_relevance_from_vectors,
        dense_rerank_window,
        diverse_ranked_sources,
        lexical_relevance_score,
        lru_cache,
        normalize_text,
        percent_cap,
        refill_cross_session_sources,
        retrieval_embedding,
        should_use_cross_session_diversity,
        source_group_identity,
        source_identity,
        warm_retrieval_embeddings,
    )
except ImportError:  # top-level path (run_locomo_ingest_once)
    from run_locomo_ingest_once import (
        ACTIVE_RETRIEVAL_EMBEDDING_MODEL,
        RetrievalBudgetConfig,
        _RETRIEVAL_ENCODER,
        add_domain_reference_sources,
        add_temporal_reference_sources,
        compact_retrieval_source,
        defaultdict,
        dense_relevance_from_vectors,
        dense_rerank_window,
        diverse_ranked_sources,
        lexical_relevance_score,
        lru_cache,
        normalize_text,
        percent_cap,
        refill_cross_session_sources,
        retrieval_embedding,
        should_use_cross_session_diversity,
        source_group_identity,
        source_identity,
        warm_retrieval_embeddings,
    )

__all__ = ['dominant_dataset_name', 'rank_sources', 'limit_source_group_repeats', 'apply_retrieval_budget', 'ordered_sources', 'retrieval_budget_groups', 'retrieval_session_group', 'retrieval_budget_counts', 'source_layer_identity', 'source_layer_identity_for_text']


def dominant_dataset_name(dataset_counts: dict[str, int]) -> str:
    if not dataset_counts:
        return "unknown"
    return sorted(dataset_counts.items(), key=lambda row: (-row[1], row[0]))[0][0]


def rank_sources(
    question: str,
    sources: list[dict[str, str]],
    max_events: int,
    budget: RetrievalBudgetConfig | None = None,
    *,
    max_blocks_per_source_group: int = 0,
) -> list[dict[str, str]]:
    ranked = []
    for index, source in enumerate(sources):
        body = source.get("body", "")
        ranked.append((lexical_relevance_score(question, body), -index, source))
    ranked.sort(key=lambda row: (row[0], row[1]), reverse=True)
    if _RETRIEVAL_ENCODER is not None and ranked:
        window = dense_rerank_window(max(1, max_events), len(ranked))
        q_vec = retrieval_embedding(ACTIVE_RETRIEVAL_EMBEDDING_MODEL, question[:512])
        warm_retrieval_embeddings(row[2].get("body", "")[:4096] for row in ranked[:window])
        reranked = []
        for lexical_score, neg_index, source in ranked[:window]:
            dense_score = dense_relevance_from_vectors(
                q_vec,
                retrieval_embedding(ACTIVE_RETRIEVAL_EMBEDDING_MODEL, source.get("body", "")[:4096]),
            )
            reranked.append((lexical_score + dense_score, neg_index, source))
        reranked.sort(key=lambda row: (row[0], row[1]), reverse=True)
        ranked = [*reranked, *ranked[window:]]
    if should_use_cross_session_diversity(question):
        selected = diverse_ranked_sources(question, ranked, max_events)
    else:
        selected = [source for _, _, source in ranked[: max(1, max_events)]]
    selected = add_temporal_reference_sources(question, selected, sources, max_events)
    selected = add_domain_reference_sources(question, selected, sources, max_events)
    selected = refill_cross_session_sources(question, selected, ranked, max_events)
    if budget is not None:
        selected = apply_retrieval_budget(selected, ranked, max_events, budget)
    selected = limit_source_group_repeats(selected, ranked, max_events, max_blocks_per_source_group)
    return [compact_retrieval_source(question, source) for source in selected]


def limit_source_group_repeats(
    selected: list[dict[str, str]],
    ranked: list[tuple[int, int, dict[str, str]]],
    max_events: int,
    max_blocks_per_group: int,
) -> list[dict[str, str]]:
    limit = max(1, max_events)
    group_limit = int(max_blocks_per_group or 0)
    if group_limit <= 0:
        return selected[:limit]
    candidates = ordered_sources([*selected, *(source for _score, _index, source in ranked)])
    out: list[dict[str, str]] = []
    used: set[str] = set()
    group_counts: defaultdict[str, int] = defaultdict(int)
    deferred: list[dict[str, str]] = []
    for source in candidates:
        key = source_identity(source)
        if key in used:
            continue
        group = source_group_identity(source)
        if group_counts[group] >= group_limit:
            deferred.append(source)
            continue
        out.append(source)
        used.add(key)
        group_counts[group] += 1
        if len(out) >= limit:
            return out[:limit]
    for source in deferred:
        if len(out) >= limit:
            break
        key = source_identity(source)
        if key in used:
            continue
        # The group cap is a soft diversity target. Refill after unique groups are
        # exhausted so answer-bearing repeated turns are not dropped completely.
        out.append(source)
        used.add(key)
    return out[:limit]


def apply_retrieval_budget(
    selected: list[dict[str, str]],
    ranked: list[tuple[int, int, dict[str, str]]],
    max_events: int,
    budget: RetrievalBudgetConfig,
) -> list[dict[str, str]]:
    limit = max(1, max_events)
    candidates = ordered_sources([*selected, *(source for _score, _index, source in ranked)])
    if not candidates:
        return []
    primary_group = source_group_identity(candidates[0])
    caps = {
        "same_session": percent_cap(limit, budget.same_session_percent),
        "cross_session": percent_cap(limit, budget.cross_session_percent),
        "summary": percent_cap(limit, budget.summary_percent),
        "entity": percent_cap(limit, budget.entity_percent),
        "event": percent_cap(limit, budget.event_percent),
    }
    out: list[dict[str, str]] = []
    counts: defaultdict[str, int] = defaultdict(int)
    used: set[str] = set()
    for source in candidates:
        if len(out) >= limit:
            break
        key = source_identity(source)
        if key in used:
            continue
        groups = retrieval_budget_groups(source, primary_group)
        if any(counts[group] >= caps[group] for group in groups if group in caps):
            continue
        out.append(source)
        used.add(key)
        for group in groups:
            counts[group] += 1
    for source in candidates:
        if len(out) >= limit:
            break
        key = source_identity(source)
        if key in used:
            continue
        session_group = retrieval_session_group(source, primary_group)
        if counts[session_group] >= caps[session_group]:
            continue
        out.append(source)
        used.add(key)
        counts[session_group] += 1
    for source in candidates:
        if len(out) >= limit:
            break
        key = source_identity(source)
        if key in used:
            continue
        # Retrieval caps are soft quotas. Once every quota-aware pass is exhausted,
        # fill unused capacity with the best remaining candidates instead of
        # returning a short pack that misses available evidence.
        out.append(source)
        used.add(key)
        for group in retrieval_budget_groups(source, primary_group):
            counts[group] += 1
    return out[:limit]


def ordered_sources(sources: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for source in sources:
        key = source_identity(source)
        if key in seen:
            continue
        out.append(source)
        seen.add(key)
    return out


def retrieval_budget_groups(source: dict[str, str], primary_group: str) -> list[str]:
    groups = [retrieval_session_group(source, primary_group)]
    groups.append(source_layer_identity(source))
    return groups


def retrieval_session_group(source: dict[str, str], primary_group: str) -> str:
    return "same_session" if source_group_identity(source) == primary_group else "cross_session"


def retrieval_budget_counts(blocks: list[dict[str, str]]) -> dict[str, int]:
    if not blocks:
        return {
            "same_session": 0,
            "cross_session": 0,
            "summary": 0,
            "entity": 0,
            "event": 0,
            "other": 0,
        }
    primary_group = source_group_identity(blocks[0])
    counts: defaultdict[str, int] = defaultdict(int)
    for block in blocks:
        for group in retrieval_budget_groups(block, primary_group):
            counts[group] += 1
    for key in ("same_session", "cross_session", "summary", "entity", "event", "other"):
        counts.setdefault(key, 0)
    return dict(sorted(counts.items()))


def source_layer_identity(source: dict[str, str]) -> str:
    return source_layer_identity_for_text(str(source.get("title") or ""), str(source.get("body") or "")[:260])


@lru_cache(maxsize=250_000)
def source_layer_identity_for_text(title_text: str, body_text: str) -> str:
    title = normalize_text(title_text)
    body = normalize_text(body_text)
    text = f"{title} {body}"
    if re.search(r"\b(summary|contextsummary|session_l[01]|node_l[01])\b", text):
        return "summary"
    if re.search(r"\b(entity|observation|contextentity|fact)\b", text):
        return "entity"
    if re.search(r"\b(event|turn|message|contextevent)\b", text):
        return "event"
    return "other"
