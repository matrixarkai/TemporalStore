#!/usr/bin/env python3
"""Shared OSS model contract for MatrixArk/OpenViking benchmark comparisons."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


CONTRACT_FIELDS = (
    "reader_model",
    "embedding_model",
    "max_events",
    "reader_max_context_chars",
    "judge_model",
    "judge_prompt",
)

NON_OSS_MODEL_MARKERS = (
    "anthropic",
    "claude",
    "hash",
    "gemini",
    "gpt-",
    "local-token",
    "matrixark-local",
    "openai",
    "debug",
    "deterministic",
    "fallback",
)


@dataclass(frozen=True)
class OssModelContract:
    reader_model: str
    embedding_model: str
    max_events: int
    reader_max_context_chars: int
    judge_model: str = ""
    judge_prompt: str = ""

    def as_report(self) -> dict[str, Any]:
        return {
            "reader_model": self.reader_model,
            "embedding_model": self.embedding_model,
            "max_events": self.max_events,
            "reader_max_context_chars": self.reader_max_context_chars,
            "judge_model": self.judge_model,
            "judge_prompt": self.judge_prompt,
        }


def normalize_model_name(value: Any) -> str:
    return str(value or "").strip()


def contract_from_values(
    *,
    reader_model: Any,
    embedding_model: Any,
    max_events: Any,
    reader_max_context_chars: Any,
    judge_model: Any = "",
    judge_prompt: Any = "",
) -> OssModelContract:
    return OssModelContract(
        reader_model=normalize_model_name(reader_model),
        embedding_model=normalize_model_name(embedding_model),
        max_events=int(max_events or 0),
        reader_max_context_chars=int(reader_max_context_chars or 0),
        judge_model=normalize_model_name(judge_model),
        judge_prompt=normalize_model_name(judge_prompt),
    )


def validate_shared_oss_contract(
    matrixark: OssModelContract,
    baseline: OssModelContract,
    *,
    require_judge_match: bool = False,
) -> tuple[bool, list[str]]:
    mismatches: list[str] = []
    fields = CONTRACT_FIELDS if require_judge_match else CONTRACT_FIELDS[:4]
    matrixark_values = matrixark.as_report()
    baseline_values = baseline.as_report()
    for field in fields:
        if matrixark_values.get(field) != baseline_values.get(field):
            mismatches.append(
                f"{field}:matrixark={matrixark_values.get(field)!r}:baseline={baseline_values.get(field)!r}"
            )
    for side_name, values in (("matrixark", matrixark_values), ("baseline", baseline_values)):
        for field in ("reader_model", "embedding_model"):
            value = normalize_model_name(values.get(field)).lower()
            if not value:
                mismatches.append(f"{field}:{side_name}=missing")
            elif any(marker in value for marker in NON_OSS_MODEL_MARKERS):
                mismatches.append(f"{field}:{side_name}=not_oss:{values.get(field)!r}")
    return not mismatches, mismatches
