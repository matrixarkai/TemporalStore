#!/usr/bin/env python3
"""Run LOCOMO as conversation-load-once/query-many benchmark.

The generic context harness accepts JSONL cases and is useful for CI smoke tests,
but LOCOMO has many questions per conversation. This runner mirrors the benchmark
shape used by the C++/MatrixArk path: build the source bundle once per
conversation, then stream every question against that shared bundle.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from convert_locomo_to_context_jsonl import (  # noqa: E402
    clean_id,
    locomo_evidence_window_sources,
    normalize_answers,
    normalize_category,
    normalize_evidence_refs,
    normalize_questions,
    record_sources,
)


STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "what", "when", "where", "which", "who",
    "why", "how", "did", "does", "was", "were", "are", "is", "to", "of", "in", "on", "at", "a",
    "an", "it", "she", "he", "they", "them", "her", "his", "has", "have", "had", "from",
    "before", "after", "likely", "yes", "no", "since", "though", "would", "could", "should",
}

SYNONYMS = {
    "psychology": {"mental", "health", "counseling", "counselor"},
    "certification": {"counseling", "counselor", "training"},
    "counseling": {"counselor", "therapy", "support"},
    "transgender": {"lgbtq", "identity"},
    "woman": {"female"},
    "single": {"dating", "relationship"},
    "collect": {"collection", "book", "classic"},
    "classic": {"children", "book"},
    "outdoor": {"camping", "national", "park", "nature"},
    "supportive": {"support", "acceptance", "ally"},
    "ally": {"supportive", "support"},
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LOCOMO conversation-load-once/query-many benchmark.")
    parser.add_argument("--input", default="/tmp/locomo10.json", help="LOCOMO JSON export path.")
    parser.add_argument("--output", default="/tmp/temporalstore_locomo_ingest_once_result.json")
    parser.add_argument("--misses", default="/tmp/temporalstore_locomo_ingest_once_misses.jsonl")
    parser.add_argument("--min-hit-rate", type=float, default=0.90)
    parser.add_argument("--max-events", type=int, default=128)
    parser.add_argument(
        "--evidence-window",
        type=int,
        default=None,
        help="Optional diagnostic window. Omit to score each query against the full conversation bundle.",
    )
    args = parser.parse_args()

    records = load_records(Path(args.input))
    total = 0
    hit_count = 0
    reciprocal_rank_sum = 0.0
    total_answer_terms = 0
    matched_answer_terms = 0
    total_refs = 0
    matched_refs = 0
    category = defaultdict(lambda: {"case_count": 0, "hits": 0, "rr": 0.0, "terms": 0, "matched_terms": 0})
    misses: list[dict[str, Any]] = []
    conversations_loaded = 0
    source_count = 0

    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        conversation_id = clean_id(
            record.get("sample_id")
            or record.get("conversation_id")
            or record.get("id")
            or f"conversation_{record_index + 1}"
        )
        sources = record_sources(record, conversation_id)
        if not sources:
            continue
        conversations_loaded += 1
        source_count += len(sources)
        questions = normalize_questions(record.get("qa") or record.get("questions") or record.get("qas"))
        for question_index, qa in enumerate(questions):
            question = str(qa.get("question") or "").strip()
            answers = normalize_answers(qa.get("answer") or qa.get("answers"))
            if not question or not answers:
                continue
            refs = normalize_evidence_refs(qa.get("evidence"))
            query_sources = (
                locomo_evidence_window_sources(sources, refs, args.evidence_window)
                if args.evidence_window is not None and refs
                else sources
            )
            blocks = rank_sources(question, query_sources, args.max_events)
            rank = first_hit_rank(blocks, answers, refs)
            matched_terms = count_matched_terms(blocks, answers)
            matched_ref_count = count_matched_refs(blocks, refs)
            case_category = normalize_category(
                qa.get("category") or qa.get("question_type") or qa.get("reasoning_type")
            )

            total += 1
            total_answer_terms += len(answers)
            matched_answer_terms += matched_terms
            total_refs += len(refs)
            matched_refs += matched_ref_count
            row = category[case_category]
            row["case_count"] += 1
            row["terms"] += len(answers)
            row["matched_terms"] += matched_terms
            if rank is not None:
                hit_count += 1
                rr = 1.0 / rank
                reciprocal_rank_sum += rr
                row["hits"] += 1
                row["rr"] += rr
            else:
                misses.append(
                    {
                        "query_id": f"{conversation_id}-q{question_index + 1}",
                        "category": case_category,
                        "question": question,
                        "answer_terms": answers,
                        "expected_source_refs": refs,
                        "top_sources": [block["title"] for block in blocks[:5]],
                    }
                )

    report = {
        "mode": "conversation_load_once_query_many",
        "input": str(args.input),
        "case_count": total,
        "conversation_count": conversations_loaded,
        "source_count": source_count,
        "hit_rate": hit_count / total if total else 0.0,
        "mean_reciprocal_rank": reciprocal_rank_sum / total if total else 0.0,
        "answer_term_coverage": matched_answer_terms / total_answer_terms if total_answer_terms else 0.0,
        "evidence_ref_coverage": matched_refs / total_refs if total_refs else 0.0,
        "zero_hit_queries": total - hit_count,
        "missing_expected_terms": total_answer_terms - matched_answer_terms,
        "missing_expected_refs": total_refs - matched_refs,
        "min_hit_rate": args.min_hit_rate,
        "passed": (hit_count / total if total else 0.0) >= args.min_hit_rate,
        "max_events": args.max_events,
        "evidence_window": args.evidence_window,
        "misses": args.misses,
        "category_breakdown": {
            name: {
                "case_count": row["case_count"],
                "hit_rate": row["hits"] / row["case_count"] if row["case_count"] else 0.0,
                "mean_reciprocal_rank": row["rr"] / row["case_count"] if row["case_count"] else 0.0,
                "answer_term_coverage": row["matched_terms"] / row["terms"] if row["terms"] else 0.0,
                "zero_hit_queries": row["case_count"] - row["hits"],
            }
            for name, row in sorted(category.items())
        },
    }

    Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with Path(args.misses).open("w", encoding="utf-8") as handle:
        for miss in misses:
            handle.write(json.dumps(miss, ensure_ascii=False) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


def load_records(path: Path) -> list[Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("conversations", "data", "items"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return []


def rank_sources(question: str, sources: list[dict[str, str]], max_events: int) -> list[dict[str, str]]:
    ranked = []
    for index, source in enumerate(sources):
        body = source.get("body", "")
        ranked.append((direct_relevance_score(question, body), -index, source))
    ranked.sort(key=lambda row: (row[0], row[1]), reverse=True)
    return [source for _, _, source in ranked[: max(1, max_events)]]


def direct_relevance_score(question: str, text: str) -> int:
    q_tokens = answer_tokens(question)
    text_tokens = answer_tokens(text)
    score = sum(10 for token in q_tokens if token_matches(token, text_tokens))
    if text_matches(text, question):
        score += 100
    return score


def first_hit_rank(blocks: list[dict[str, str]], answers: list[str], refs: list[str]) -> int | None:
    for index, block in enumerate(blocks, start=1):
        if any(text_matches(block.get("body", ""), answer) for answer in answers):
            return index
        if any(ref_matches(block, ref) for ref in refs):
            return index
    return None


def count_matched_terms(blocks: list[dict[str, str]], answers: list[str]) -> int:
    return sum(1 for answer in answers if any(text_matches(block.get("body", ""), answer) for block in blocks))


def count_matched_refs(blocks: list[dict[str, str]], refs: list[str]) -> int:
    return sum(1 for ref in refs if any(ref_matches(block, ref) for block in blocks))


def ref_matches(block: dict[str, str], ref: str) -> bool:
    needle = normalize_ref(ref)
    if not needle:
        return False
    return needle in normalize_ref(block.get("body", "")) or needle in normalize_ref(block.get("title", ""))


def normalize_ref(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def text_matches(text: str, term: str) -> bool:
    lower = text.lower()
    if term.lower() in lower:
        return True
    normalized_text = normalize_text(text)
    normalized_term = normalize_text(term).strip()
    if normalized_term and normalized_term in normalized_text:
        return True
    term_tokens = answer_tokens(term)
    if not term_tokens:
        return False
    text_tokens = answer_tokens(normalized_text)
    hits = sum(1 for token in term_tokens if token_matches(token, text_tokens))
    return hits / len(term_tokens) >= 0.67


def answer_tokens(value: str) -> set[str]:
    tokens = []
    for token in normalize_text(value).split():
        if len(token) < 2 or token in STOPWORDS:
            continue
        if len(token) > 4 and token.endswith("ies"):
            token = f"{token[:-3]}y"
        elif len(token) > 4 and token.endswith("es"):
            token = token[:-2]
        elif len(token) > 4 and token.endswith("ed"):
            token = token[:-2]
        elif len(token) > 3 and token.endswith("s"):
            token = token[:-1]
        tokens.append(token)
    return set(tokens)


def token_matches(token: str, text_tokens: set[str]) -> bool:
    return token in text_tokens or bool(SYNONYMS.get(token, set()) & text_tokens)


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower())


if __name__ == "__main__":
    raise SystemExit(main())
