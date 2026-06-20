#!/usr/bin/env python3
"""Convert LOCOMO-style JSON exports to TemporalStore context benchmark JSONL."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert LOCOMO JSON into context_workflow_harness JSONL cases."
    )
    parser.add_argument("input", help="LOCOMO JSON path")
    parser.add_argument("output", help="Output JSONL path")
    parser.add_argument("--max-questions", type=int, default=None)
    parser.add_argument("--max-conversations", type=int, default=None)
    parser.add_argument(
        "--evidence-window",
        type=int,
        default=None,
        help=(
            "For LOCOMO diagnostics, emit only sources within N turns of each gold evidence "
            "dialogue id. Omit for full-conversation retrieval."
        ),
    )
    args = parser.parse_args()

    records = json.loads(Path(args.input).read_text(encoding="utf-8-sig"))
    if isinstance(records, dict):
        records = (
            records.get("conversations")
            or records.get("data")
            or records.get("items")
            or [records]
        )
    if args.max_conversations is not None:
        records = records[: args.max_conversations]

    written = 0
    out = Path(args.output)
    with out.open("w", encoding="utf-8") as handle:
        for record_index, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            conversation_id = clean_id(
                record.get("sample_id")
                or record.get("conversation_id")
                or record.get("id")
                or f"conversation_{record_index + 1}"
            )
            sources = locomo_sources(record, conversation_id)
            for question_index, qa in enumerate(record.get("qa") or record.get("questions") or []):
                if args.max_questions is not None and written >= args.max_questions:
                    break
                question = str(qa.get("question") or "").strip()
                if not question:
                    continue
                answer_terms = normalize_answers(qa.get("answer") or qa.get("answers"))
                if not answer_terms:
                    continue
                evidence_refs = normalize_evidence_refs(qa.get("evidence"))
                case_sources = (
                    locomo_evidence_window_sources(sources, evidence_refs, args.evidence_window)
                    if args.evidence_window is not None
                    else sources
                )
                case = {
                    "dataset": "locomo",
                    "query_id": f"{conversation_id}-q{question_index + 1}",
                    "category": normalize_category(qa.get("category")),
                    "question": question,
                    "answer_terms": answer_terms,
                    "expected_source_refs": evidence_refs,
                    "sources": case_sources,
                }
                handle.write(json.dumps(case, ensure_ascii=False) + "\n")
                written += 1
        if args.max_questions is not None and written >= args.max_questions:
            pass
    print(f"wrote {written} cases to {out}")


def locomo_sources(record: dict[str, Any], conversation_id: str) -> list[dict[str, str]]:
    conversation = record.get("conversation")
    if not isinstance(conversation, dict):
        return []
    sources: list[dict[str, str]] = []
    session_keys = sorted(
        (
            key
            for key, value in conversation.items()
            if re.fullmatch(r"session_\d+", key) and isinstance(value, list)
        ),
        key=lambda value: int(value.split("_", 1)[1]),
    )
    for session_key in session_keys:
        session_date = session_date_text(conversation.get(f"{session_key}_date_time"))
        session_id = clean_id(session_key)
        for turn_index, message in enumerate(conversation.get(session_key) or []):
            body = message_text(message)
            if body:
                sources.append(
                    {
                        "kind": "chat",
                        "title": f"{conversation_id} {session_id} turn {turn_index + 1}",
                        "body": f"{session_date}. {body}" if session_date else body,
                    }
                )
        summary = nested_text(record.get("session_summary"), f"{session_key}_summary")
        if summary:
            sources.append(
                {
                    "kind": "document",
                    "title": f"{conversation_id} {session_id} summary",
                    "body": f"{session_date}. {summary}" if session_date else summary,
                }
            )
        sources.extend(locomo_observation_sources(record, conversation_id, session_id, session_key, session_date))
        sources.extend(locomo_event_sources(record, conversation_id, session_id, session_key, session_date))
    return sources


def locomo_observation_sources(
    record: dict[str, Any],
    conversation_id: str,
    session_id: str,
    session_key: str,
    session_date: str,
) -> list[dict[str, str]]:
    observations = record.get("observation")
    if not isinstance(observations, dict):
        return []
    session_observation = observations.get(f"{session_key}_observation")
    if not isinstance(session_observation, dict):
        return []
    sources = []
    for speaker, facts in session_observation.items():
        if not isinstance(facts, list):
            continue
        for index, fact in enumerate(facts):
            fact_text = ""
            if isinstance(fact, list) and fact:
                fact_text = str(fact[0])
            elif isinstance(fact, str):
                fact_text = fact
            if fact_text.strip():
                sources.append(
                    {
                        "kind": "user_event",
                        "title": f"{conversation_id} {session_id} observation {speaker} {index + 1}",
                        "body": f"{session_date}. {fact_text.strip()}" if session_date else fact_text.strip(),
                    }
                )
    return sources


def locomo_event_sources(
    record: dict[str, Any],
    conversation_id: str,
    session_id: str,
    session_key: str,
    session_date: str,
) -> list[dict[str, str]]:
    event_summary = record.get("event_summary")
    if not isinstance(event_summary, dict):
        return []
    number = session_key.split("_", 1)[-1]
    session_events = event_summary.get(f"events_session_{number}")
    if not isinstance(session_events, dict):
        return []
    sources = []
    for speaker, events in session_events.items():
        if speaker == "date" or not isinstance(events, list):
            continue
        for index, event in enumerate(events):
            event_text = str(event).strip()
            if event_text:
                sources.append(
                    {
                        "kind": "ticket",
                        "title": f"{conversation_id} {session_id} event {speaker} {index + 1}",
                        "body": f"{session_date}. {event_text}" if session_date else event_text,
                    }
                )
    return sources


def locomo_evidence_window_sources(
    sources: list[dict[str, str]],
    evidence_refs: list[str],
    window: int | None,
) -> list[dict[str, str]]:
    if window is None or window < 0 or not evidence_refs:
        return sources
    wanted_indexes: set[int] = set()
    normalized_refs = {normalize_ref_for_match(ref) for ref in evidence_refs}
    normalized_refs.discard("")
    if not normalized_refs:
        return sources
    for index, source in enumerate(sources):
        body = normalize_ref_for_match(source.get("body", ""))
        title = normalize_ref_for_match(source.get("title", ""))
        if any(ref in body or ref in title for ref in normalized_refs):
            start = max(0, index - window)
            end = min(len(sources), index + window + 1)
            wanted_indexes.update(range(start, end))
    if not wanted_indexes:
        return sources
    return [source for index, source in enumerate(sources) if index in wanted_indexes]


def normalize_ref_for_match(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def message_text(message: Any) -> str:
    if isinstance(message, str):
        return message.strip()
    if not isinstance(message, dict):
        return ""
    role = str(message.get("role") or message.get("speaker") or "").strip()
    dia_id = str(message.get("dia_id") or "").strip()
    text = str(
        message.get("content")
        or message.get("text")
        or message.get("message")
        or message.get("utterance")
        or ""
    ).strip()
    prefix_parts = [part for part in (dia_id, role) if part]
    prefix = " ".join(prefix_parts)
    return f"{prefix}: {text}" if prefix and text else text


def nested_text(value: Any, key: str) -> str:
    if isinstance(value, dict):
        return str(value.get(key) or "").strip()
    return ""


def normalize_answers(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    text = str(raw).strip()
    return [text] if text else []


def normalize_evidence_refs(raw: Any) -> list[str]:
    if raw is None:
        return []
    values = raw if isinstance(raw, list) else [raw]
    refs: list[str] = []
    for value in values:
        text = str(value).strip()
        if text:
            refs.append(text)
    return refs


def normalize_category(raw: Any) -> str:
    value = str(raw or "unknown").strip().lower().replace("-", "_").replace(" ", "_")
    if value.isdigit():
        return f"category_{value}"
    return value


def clean_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_") or "unknown"


def session_date_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%B %d, %Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).strftime("%d %B %Y")
        except ValueError:
            pass
    return text


if __name__ == "__main__":
    main()
