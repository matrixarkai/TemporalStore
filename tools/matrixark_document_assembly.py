# SPDX-License-Identifier: Apache-2.0
"""Assemble retrieval hits into whole documents, contiguous packs, or bare chunks.

A 1 MB markdown/JSON resource shatters into ~2k chunks. When many of them match
one query, returning 2k fragments is both worse to read and more expensive than
returning the document. This picks the cheapest faithful representation:

  whole_document  many/most of a document matched, or the document is small
                  enough to inline -> return the original md/json
  pack            several chunks matched -> merge them into contiguous spans,
                  bridging short breaks, and return those spans with citations
  chunks          isolated matches -> return the chunks as-is

Selection is greedy by best hit score and always respects ``token_budget``.
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

WHOLE_DOC_COVERAGE = float(os.environ.get("MATRIXARK_WHOLE_DOC_COVERAGE", "0.5"))
WHOLE_DOC_MAX_TOKENS = int(os.environ.get("MATRIXARK_WHOLE_DOC_MAX_TOKENS", "4000"))
WHOLE_DOC_MIN_HITS = int(os.environ.get("MATRIXARK_WHOLE_DOC_MIN_HITS", "3"))
PACK_BRIDGE_DISTANCE = int(os.environ.get("MATRIXARK_PACK_BRIDGE_DISTANCE", "1"))
MODE_ORDER = ("whole_document", "pack", "chunks")


@dataclass
class Hit:
    doc_id: str
    chunk_index: int
    score: float
    tokens: int
    text: str = ""
    source_ref: str = ""


@dataclass
class DocMeta:
    doc_id: str
    total_chunks: int
    total_tokens: int
    raw_uri: str = ""
    resource_type: str = ""


@dataclass
class Selection:
    doc_id: str
    mode: str
    tokens: int
    best_score: float
    spans: list[tuple[int, int]] = field(default_factory=list)
    chunk_indexes: list[int] = field(default_factory=list)
    raw_uri: str = ""
    reason: str = ""


def _spans(indexes: list[int], bridge: int) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for i in sorted(set(indexes)):
        if out and i - out[-1][1] <= bridge + 1:
            out[-1] = (out[-1][0], i)
        else:
            out.append((i, i))
    return out


def assemble(
    hits: Iterable[Hit],
    docs: dict[str, DocMeta],
    token_budget: int,
    *,
    allow_whole_document: bool = True,
    chunk_tokens: Callable[[str, int], int] | None = None,
) -> dict[str, Any]:
    by_doc: dict[str, list[Hit]] = {}
    for h in hits:
        by_doc.setdefault(h.doc_id, []).append(h)

    def span_tokens(doc_id: str, lo: int, hi: int, matched: list[Hit]) -> int:
        if chunk_tokens is not None:
            return sum(chunk_tokens(doc_id, i) for i in range(lo, hi + 1))
        known = {h.chunk_index: h.tokens for h in matched}
        if not known:
            return 0
        avg = sum(known.values()) / len(known)
        return int(sum(known.get(i, avg) for i in range(lo, hi + 1)))

    candidates: list[Selection] = []
    for doc_id, dh in by_doc.items():
        meta = docs.get(doc_id)
        best = max(h.score for h in dh)
        idxs = sorted({h.chunk_index for h in dh})
        matched_tokens = sum(h.tokens for h in dh)
        if meta is not None and allow_whole_document:
            coverage = len(idxs) / max(1, meta.total_chunks)
            small = meta.total_tokens <= WHOLE_DOC_MAX_TOKENS
            enough = len(idxs) >= WHOLE_DOC_MIN_HITS
            if (coverage >= WHOLE_DOC_COVERAGE and enough) or (small and enough):
                candidates.append(Selection(
                    doc_id=doc_id, mode="whole_document", tokens=meta.total_tokens,
                    best_score=best, spans=[(0, max(0, meta.total_chunks - 1))],
                    chunk_indexes=idxs, raw_uri=meta.raw_uri,
                    reason=(f"coverage={coverage:.2f}>={WHOLE_DOC_COVERAGE}" if coverage >= WHOLE_DOC_COVERAGE
                            else f"doc_tokens={meta.total_tokens}<={WHOLE_DOC_MAX_TOKENS}")))
                continue
        sp = _spans(idxs, PACK_BRIDGE_DISTANCE)
        if len(idxs) >= 2:
            tok = sum(span_tokens(doc_id, lo, hi, dh) for lo, hi in sp)
            candidates.append(Selection(doc_id=doc_id, mode="pack", tokens=tok, best_score=best,
                spans=sp, chunk_indexes=idxs, raw_uri=(meta.raw_uri if meta else ""),
                reason=f"{len(idxs)} hits in {len(sp)} span(s)"))
        else:
            candidates.append(Selection(doc_id=doc_id, mode="chunks", tokens=matched_tokens,
                best_score=best, spans=sp, chunk_indexes=idxs,
                raw_uri=(meta.raw_uri if meta else ""), reason="isolated hit"))

    candidates.sort(key=lambda s: (-s.best_score, s.tokens))
    chosen: list[Selection] = []
    used = 0
    for c in candidates:
        if used + c.tokens <= token_budget:
            chosen.append(c); used += c.tokens; continue
        # Degrade rather than drop: whole_document -> pack -> chunks
        dh = by_doc[c.doc_id]
        idxs = sorted({h.chunk_index for h in dh})
        sp = _spans(idxs, PACK_BRIDGE_DISTANCE)
        pack_tok = sum(span_tokens(c.doc_id, lo, hi, dh) for lo, hi in sp)
        if c.mode == "whole_document" and used + pack_tok <= token_budget:
            chosen.append(Selection(c.doc_id, "pack", pack_tok, c.best_score, sp, idxs,
                                    c.raw_uri, "downgraded from whole_document: budget"))
            used += pack_tok; continue
        bare = sum(h.tokens for h in dh)
        if used + bare <= token_budget:
            chosen.append(Selection(c.doc_id, "chunks", bare, c.best_score, sp, idxs,
                                    c.raw_uri, "downgraded to chunks: budget"))
            used += bare
            continue
        # Even the bare chunks overflow: keep the best-scoring ones that fit
        # rather than dropping the document entirely.
        kept: list[Hit] = []
        kept_tokens = 0
        for h in sorted(dh, key=lambda x: -x.score):
            if used + kept_tokens + h.tokens > token_budget:
                continue
            kept.append(h)
            kept_tokens += h.tokens
        if kept:
            kidx = sorted({h.chunk_index for h in kept})
            chosen.append(Selection(c.doc_id, "chunks", kept_tokens, c.best_score,
                                    _spans(kidx, PACK_BRIDGE_DISTANCE), kidx, c.raw_uri,
                                    f"truncated to {len(kept)}/{len(dh)} chunks: budget"))
            used += kept_tokens
    return {
        "selections": chosen,
        "tokens_used": used,
        "token_budget": token_budget,
        "docs_considered": len(by_doc),
        "mode_counts": {m: sum(1 for c in chosen if c.mode == m) for m in MODE_ORDER},
    }
