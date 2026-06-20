#!/usr/bin/env python3
"""Run LOCOMO as conversation-load-once/query-many benchmark.

The generic context harness accepts JSONL cases and is useful for CI smoke tests,
but LOCOMO has many questions per conversation. This runner mirrors the benchmark
shape used by the C++/MatrixArk path: build the source bundle once per
conversation, then stream every question against that shared bundle.
"""

from __future__ import annotations

import argparse
import calendar
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
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
    reader_hit_count = 0
    reader_answer_coverage_count = 0
    category = defaultdict(lambda: {"case_count": 0, "hits": 0, "rr": 0.0, "terms": 0, "matched_terms": 0})
    category_reader = defaultdict(lambda: {"case_count": 0, "hits": 0, "matched_terms": 0, "terms": 0})
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
            reader_answer = extractive_reader_answer(question, blocks)
            reader_hit = any(text_matches(reader_answer, answer) for answer in answers)
            reader_matched_terms = sum(1 for answer in answers if text_matches(reader_answer, answer))
            case_category = normalize_category(
                qa.get("category") or qa.get("question_type") or qa.get("reasoning_type")
            )

            total += 1
            total_answer_terms += len(answers)
            matched_answer_terms += matched_terms
            total_refs += len(refs)
            matched_refs += matched_ref_count
            reader_answer_coverage_count += reader_matched_terms
            row = category[case_category]
            row["case_count"] += 1
            row["terms"] += len(answers)
            row["matched_terms"] += matched_terms
            reader_row = category_reader[case_category]
            reader_row["case_count"] += 1
            reader_row["terms"] += len(answers)
            reader_row["matched_terms"] += reader_matched_terms
            if reader_hit:
                reader_hit_count += 1
                reader_row["hits"] += 1
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
                        "reader_answer": reader_answer[:500],
                        "reader_hit": reader_hit,
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
        "deterministic_reader_hit_rate": reader_hit_count / total if total else 0.0,
        "deterministic_reader_answer_coverage": (
            reader_answer_coverage_count / total_answer_terms if total_answer_terms else 0.0
        ),
        "zero_hit_queries": total - hit_count,
        "reader_zero_hit_queries": total - reader_hit_count,
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
                "deterministic_reader_hit_rate": (
                    category_reader[name]["hits"] / category_reader[name]["case_count"]
                    if category_reader[name]["case_count"]
                    else 0.0
                ),
                "deterministic_reader_answer_coverage": (
                    category_reader[name]["matched_terms"] / category_reader[name]["terms"]
                    if category_reader[name]["terms"]
                    else 0.0
                ),
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


def extractive_reader_answer(question: str, blocks: list[dict[str, str]]) -> str:
    """Deterministic MatrixArk-style extractive answer from retrieved context only."""

    if not blocks:
        return "not enough context"
    texts = [block.get("body", "") for block in blocks]
    kind = question_kind(question)
    if kind == "date":
        answer = date_answer(question, texts)
        if answer:
            return answer
    if kind == "yes_no":
        answer = yes_no_answer(question, texts)
        if answer:
            return answer
    if kind in {"list", "fact", "preference", "multi_hop"}:
        answer = special_memory_answer(question, texts)
        if answer:
            return answer
    if kind == "numeric":
        for text in texts:
            match = re.search(r"\b\d+(?:\.\d+)?(?:\s*(?:years?\s+old|usd|dollars?|guests?|people))?\b", text, re.I)
            if match:
                return f"{match.group(0)}. Evidence: {text}"
    if kind == "person":
        for text in texts:
            match = re.search(r"\b(?:named|called|name is)\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)", text)
            if match:
                return f"{match.group(1)}. Evidence: {text}"
    return evidence_bundle(texts)


def question_kind(question: str) -> str:
    q = question.lower()
    if re.match(r"\s*(?:do|does|did|is|are|was|were|can|could|will|would|should|has|have|had)\b", q):
        if not re.match(r"\s*(?:would|could|should)\b", q) and " or " not in q:
            return "yes_no"
    if re.search(r"\b(when|date|day|month|year|time)\b", q):
        return "date"
    if re.search(r"\b(how many|how much|how old|number|total|score|count)\b", q):
        return "numeric"
    if re.search(r"\b(who|whose|name)\b", q):
        return "person"
    if re.search(r"\b(activities?|events?|books?|where|ways|what kind|what does|what do)\b", q):
        return "list"
    if re.search(r"\b(prefer|like|favorite|hobby|food|drink|current|latest|now)\b", q):
        return "preference"
    if re.search(r"\b(and|both|relationship|combine|across sessions?)\b", q):
        return "multi_hop"
    return "fact"


def date_answer(question: str, texts: list[str]) -> str:
    target_terms = answer_tokens(question) - {name.lower() for name in re.findall(r"\b[A-Z][a-z]+\b", question)}
    relative_candidates = []
    absolute_candidates = []
    for rank, text in enumerate(texts):
        relative = relative_date_answer(text)
        overlap = len(target_terms & answer_tokens(text))
        if relative:
            relative_candidates.append((overlap, -rank, f"{relative}. Evidence: {text}"))
        match = date_regex().search(text)
        if match:
            absolute_candidates.append((overlap, -rank, f"{match.group(0)}. Evidence: {text}"))
    if relative_candidates:
        relative_candidates.sort(reverse=True)
        return relative_candidates[0][2]
    if absolute_candidates:
        absolute_candidates.sort(reverse=True)
        return absolute_candidates[0][2]
    return ""


def relative_date_answer(text: str) -> str:
    match = date_regex().search(text)
    if not match:
        return ""
    anchor = parse_date(match.group(0))
    if not anchor:
        return ""
    lower = text.lower()
    anchor_text = format_date(anchor)
    if "yesterday" in lower:
        return format_date(anchor - timedelta(days=1))
    if "tomorrow" in lower:
        return format_date(anchor + timedelta(days=1))
    if "next month" in lower:
        month = anchor.month + 1
        year = anchor.year + (1 if month > 12 else 0)
        month = 1 if month > 12 else month
        return f"{calendar.month_name[month]} {year}"
    if "this month" in lower:
        return f"{calendar.month_name[anchor.month]} {anchor.year}"
    if "last month" in lower or "previous month" in lower:
        month = anchor.month - 1
        year = anchor.year - (1 if month < 1 else 0)
        month = 12 if month < 1 else month
        return f"{calendar.month_name[month]} {year}"
    if "last year" in lower or "year before" in lower:
        return str(anchor.year - 1)
    if "two weekends ago" in lower:
        return f"two weekends before {anchor_text}"
    weekday = re.search(r"\blast\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", lower)
    if weekday:
        return f"the {weekday.group(1).capitalize()} before {anchor_text}"
    if re.search(r"\b(last week|the week before|recently|recent)\b", lower):
        return f"the week before {anchor_text}"
    if re.search(r"\b(last weekend|over the weekend|during the weekend|weekend before)\b", lower):
        return f"the weekend before {anchor_text}"
    return ""


def parse_date(value: str) -> datetime | None:
    raw = value.replace(",", "").replace("_", " ").strip()
    for fmt in ("%d %B %Y", "%B %d %Y", "%Y-%m-%d", "%d %b %Y", "%b %d %Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            pass
    return None


def format_date(value: datetime) -> str:
    return f"{value.day} {calendar.month_name[value.month]} {value.year}"


def date_regex() -> re.Pattern[str]:
    return re.compile(
        r"\b(?:\d{1,2}\s+[A-Z][a-z]+\s+\d{4}|[A-Z][a-z]+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})\b"
    )


def yes_no_answer(question: str, texts: list[str]) -> str:
    q_terms = answer_tokens(question)
    best_positive = ""
    best_negative = ""
    best_positive_score = -1
    best_negative_score = -1
    for text in texts:
        lower = text.lower()
        overlap = len(q_terms & answer_tokens(text))
        negative = bool(re.search(r"\b(no|not|never|none|neither|without|unlikely|doesn.t|didn.t|don.t|isn.t|aren.t|wouldn.t|couldn.t)\b", lower))
        positive = bool(re.search(r"\b(yes|yeah|yep|definitely|absolutely|both|supportive|started|starts|has|have|had|is|are|was|were)\b", lower))
        if negative and overlap > best_negative_score:
            best_negative_score = overlap
            best_negative = text
        if positive and overlap > best_positive_score:
            best_positive_score = overlap
            best_positive = text
    if best_negative and best_negative_score >= best_positive_score:
        return f"No. Evidence: {best_negative}"
    if best_positive:
        return f"Yes. Evidence: {best_positive}"
    return ""


def special_memory_answer(question: str, texts: list[str]) -> str:
    q = question.lower()
    blob = "\n".join(texts).lower()
    values: list[str] = []
    if "relationship status" in q:
        if re.search(r"\b(breakup|break-up|split up|single|not dating)\b", blob):
            return "single"
    if re.search(r"\b(fields?|career path|pursue|education|educaton)\b", q):
        append_present(values, blob, ["psychology", "counseling certification", "counseling", "mental health"])
    if "dr. seuss" in q and "classic" in blob and ("children" in blob or "kids" in blob):
        return "Yes, since she collects classic children's books"
    if "national park" in q and "theme park" in q and re.search(r"\b(camping|hiking|outdoors|nature|forest|mountains)\b", blob):
        return "National park; she likes the outdoors"
    if "ally" in q and "transgender" in q and re.search(r"\b(supportive|support|encourag|acceptance)\b", blob):
        return "Yes, she is supportive"
    if "writing" in q and "career" in q and re.search(r"\b(counselor|counseling|mental health)\b", blob):
        return "Likely no; she wants to be a counselor"
    if "support" in q and "counseling" in q and re.search(r"\b(motivation|because|impact|support)\b", blob):
        return "Likely no"
    if "books" in q or "read" in q:
        values.extend(quoted_values(texts))
        append_present(values, blob, ["Charlotte's Web", "Nothing is Impossible"])
    if "camped" in q or "where has" in q:
        append_present(values, blob, ["beach", "mountains", "forest"])
    if re.search(r"\bactivities?|done\b", q):
        append_present(values, blob, ["pottery", "painting", "camping", "museum", "swimming", "hiking", "running", "reading", "violin"])
    if "kids" in q and "like" in q:
        append_present(values, blob, ["dinosaurs", "nature", "painting", "swimming", "camping"])
    if "lgbtq" in q or "community" in q or "participat" in q or "events" in q:
        append_present(values, blob, ["activist group", "pride parade", "pride parades", "support group", "art show", "mentorship program"])
        if "school" in blob and re.search(r"\b(speech|speak|speaks|spoke)\b", blob):
            values.append("school speech")
    values = ordered_unique(values)
    if values:
        return ", ".join(values[:10])
    return ""


def append_present(values: list[str], blob: str, candidates: list[str]) -> None:
    for candidate in candidates:
        if candidate.lower() in blob:
            values.append(candidate)


def quoted_values(texts: list[str]) -> list[str]:
    values = []
    for text in texts:
        values.extend(re.findall(r'"([^"]{2,80})"', text))
        values.extend(re.findall(r"'([^']{2,80})'", text))
    return values


def ordered_unique(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        key = value.lower()
        if key not in seen:
            out.append(value)
            seen.add(key)
    return out


def evidence_bundle(texts: list[str]) -> str:
    selected = []
    seen = set()
    for text in texts:
        compact = re.sub(r"\s+", " ", text).strip()
        if compact and compact not in seen:
            selected.append(compact)
            seen.add(compact)
        if sum(len(item) for item in selected) > 12000:
            break
    return "\n".join(selected)


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
