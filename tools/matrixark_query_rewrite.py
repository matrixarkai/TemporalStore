#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Conditional query rewriting for follow-up prompts.

A follow-up like "how are tool events handled during *that*?" carries no query terms
for its referent, so retrieval can't find the right content. Rewriting the RETRIEVAL
query to fold in the recent thread ("...hooks ingestion... tool events... during that?")
gives the anaphora its referent terms. Claude-judged A/B: this lifted follow-up answer
quality ~40-70% and eliminated off-topic hallucinations, where relaxing the score floor
did nothing.

CONDITIONAL by design: only genuine follow-ups are rewritten; a standalone question is
left untouched (folding prior turns into a topic-switch query would only add noise). Only
the *retrieval* query is rewritten — the model still answers the raw prompt. Deterministic
(no LLM); an LLM anaphora-resolver can layer on top later without changing the interface.
"""
from __future__ import annotations

import re
from typing import Iterable, Optional

# Anaphoric references whose antecedent lives in the prior turns.
_ANAPHORA = re.compile(
    r"\b(that|it|this|those|these|they|them|its|their|there|"
    r"the ones?|the two|the same|the fix|the change|the issue|the above|the whole)\b",
    re.IGNORECASE,
)
# Continuation openers that signal a follow-up rather than a fresh question.
_CONTINUATION = re.compile(r"^\s*(and|so|then|also|but|plus|why|what about|whats?\s+about|ok|okay)\b", re.IGNORECASE)
# Verbs that implicitly reference the running thread.
_THREAD_VERB = re.compile(r"^\s*(summariz|recap|tl;?dr|continue|elaborat|expand|go on|tell me more|more on|and\b)", re.IGNORECASE)


def is_followup_query(query: str, *, max_words: int = 14) -> bool:
    """Heuristic: does this prompt reference the running thread (so it needs its context)?"""
    q = (query or "").strip()
    if not q:
        return False
    words = q.split()
    short = len(words) <= max_words
    has_anaphora = bool(_ANAPHORA.search(q))
    starts_continuation = bool(_CONTINUATION.match(q))
    thread_verb = bool(_THREAD_VERB.match(q))
    # follow-up if it names an antecedent, or is a short continuation / thread-referencing prompt
    return has_anaphora or thread_verb or (short and starts_continuation)


def rewrite_query(query: str, prior_user_texts: list[str], *, window: int = 3, max_chars: int = 600) -> str:
    """Fold the last `window` prior user prompts in front of the query (retrieval query only)."""
    ctx = " ".join(t.strip() for t in prior_user_texts[-window:] if t and t.strip())
    if not ctx:
        return query
    return (ctx + " " + query).strip()[:max_chars]


def conditional_retrieval_query(
    query: str,
    prior_user_texts: Optional[Iterable[str]] = None,
    *,
    enabled: bool = True,
    window: int = 3,
) -> tuple[str, bool, str]:
    """Return (retrieval_query, was_rewritten, reason). Rewrites ONLY genuine follow-ups."""
    priors = [t for t in (prior_user_texts or []) if isinstance(t, str) and t.strip()]
    if not enabled:
        return query, False, "disabled"
    if not is_followup_query(query):
        return query, False, "standalone"
    if not priors:
        return query, False, "no_prior_context"
    return rewrite_query(query, priors, window=window), True, "followup_rewritten"
