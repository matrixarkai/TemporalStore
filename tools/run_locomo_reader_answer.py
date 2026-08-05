"""Split out of run_locomo_ingest_once.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import re
from typing import Any

try:  # package path (tools.run_locomo_ingest_once)
    from .run_locomo_ingest_once import (
        answer_tokens,
        conflicting_concrete_dates,
        date_regex,
        normalize_text,
        split_reader_sentences,
    )
except ImportError:  # top-level path (run_locomo_ingest_once)
    from run_locomo_ingest_once import (
        answer_tokens,
        conflicting_concrete_dates,
        date_regex,
        normalize_text,
        split_reader_sentences,
    )

__all__ = ['parse_openai_compatible_answer', 'hybrid_reader_answer', 'reader_answer_should_use_candidate', 'reader_answer_is_noisy', 'color_tokens']


def parse_openai_compatible_answer(body: dict[str, Any]) -> str:
    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict) and str(message.get("content") or "").strip():
                return str(message.get("content")).strip()
            if str(first.get("text") or "").strip():
                return str(first.get("text")).strip()
    if str(body.get("output_text") or "").strip():
        return str(body.get("output_text")).strip()
    raise ValueError("reader endpoint response did not contain an answer")


def hybrid_reader_answer(question: str, candidate: str, answer: str) -> str:
    candidate = re.sub(r"\s+", " ", str(candidate or "")).strip(" .")
    answer = re.sub(r"\s+", " ", str(answer or "")).strip()
    if not candidate:
        return answer
    if not answer:
        return candidate
    normalized_answer = normalize_text(answer)
    if (
        normalized_answer.startswith("not enough context")
        or normalized_answer.startswith("not enough information")
        or normalized_answer.startswith("insufficient context")
        or normalized_answer.startswith("insufficient information")
    ):
        return candidate
    if reader_answer_should_use_candidate(question, answer, candidate):
        return candidate
    return answer


def reader_answer_should_use_candidate(question: str, answer: str, candidate: str) -> bool:
    if reader_answer_is_noisy(question, answer, candidate):
        return True
    q = normalize_text(question)
    normalized_answer = normalize_text(answer)
    normalized_candidate = normalize_text(candidate)
    if not normalized_answer or not normalized_candidate:
        return False
    temporal_question = bool(re.search(r"\b(when|date|year|month|day|before|after|week)\b", q))
    relative_temporal = bool(
        re.search(
            r"\b(yesterday|today|tomorrow|last year|this year|next year|last month|this month|next month|last week|this week|next week|a few weeks ago|few weeks ago|weeks ago|months ago)\b",
            normalized_answer,
        )
    )
    candidate_has_concrete_time = bool(date_regex().search(candidate) or re.search(r"\b\d{4}\b", candidate))
    candidate_has_relative_time = bool(
        re.search(
            r"\b(yesterday|today|tomorrow|last year|this year|next year|last month|this month|next month|last week|this week|next week|a few weeks ago|few weeks ago|weeks ago|months ago)\b",
            normalized_candidate,
        )
    )
    answer_has_concrete_time = bool(date_regex().search(answer) or re.search(r"\b\d{4}\b", answer))
    answer_is_datetime_only = bool(
        answer_has_concrete_time
        and not temporal_question
        and not re.search(r"[a-z]{4,}", re.sub(r"\b(?:am|pm|sun|mon|tue|wed|thu|fri|sat)\b", "", normalized_answer))
    )
    candidate_has_non_time_fact = bool(
        re.search(r"[a-z]{4,}", normalized_candidate)
        and not re.fullmatch(r"(?:\d{4}[/.-]\d{1,2}[/.-]\d{1,2}|\d{1,2}:\d{2}|\d{4}|am|pm|sun|mon|tue|wed|thu|fri|sat|\s|[().,:/-])+",
                             normalized_candidate)
    )
    if temporal_question and relative_temporal and candidate_has_concrete_time:
        return True
    if temporal_question and candidate_has_relative_time and answer_has_concrete_time:
        return True
    if answer_is_datetime_only and candidate_has_non_time_fact and len(normalized_candidate) <= 160:
        return True
    if normalized_answer in normalized_candidate and len(normalized_candidate) <= 120:
        return True
    generic_answers = {
        "home country",
        "not enough context",
        "unknown",
        "unclear",
        "yes",
        "no",
    }
    if normalized_answer in generic_answers and len(normalized_candidate) <= 120:
        return True
    return False


def reader_answer_is_noisy(question: str, answer: str, candidate: str) -> bool:
    normalized_answer = normalize_text(answer)
    normalized_candidate = normalize_text(candidate)
    if not normalized_answer or not normalized_candidate:
        return False
    if normalized_candidate in normalized_answer:
        return len(answer) > max(180, len(candidate) * 4)
    if conflicting_concrete_dates(answer, candidate):
        return True
    if len(answer) > 260 and len(candidate) <= 120:
        return True
    if len(split_reader_sentences(answer)) >= 3 and len(candidate) <= 120:
        return True
    q = normalize_text(question)
    if re.search(r"\bwhen|date|year|month|day\b", q):
        candidate_has_date = bool(date_regex().search(candidate) or re.search(r"\b\d{4}\b", candidate))
        answer_has_many_dates = len(date_regex().findall(answer)) + len(re.findall(r"\b\d{4}\b", answer)) >= 3
        if candidate_has_date and answer_has_many_dates:
            return True
    if re.search(r"\bcolou?r\b", q) and color_tokens(candidate) and not color_tokens(answer):
        return True
    return False


def color_tokens(value: str) -> set[str]:
    colors = {
        "beige",
        "black",
        "blue",
        "brown",
        "cream",
        "gray",
        "green",
        "grey",
        "orange",
        "pink",
        "purple",
        "red",
        "white",
        "yellow",
    }
    return answer_tokens(value) & colors
