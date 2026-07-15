#!/usr/bin/env python3
"""Small environment parsing helpers for MatrixArk MCP modules."""

from __future__ import annotations

import os


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


def env_text(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_lower(name: str, default: str = "") -> str:
    return env_text(name, default).lower()


def env_bool(name: str, default: bool = False) -> bool:
    value = env_lower(name, "1" if default else "0")
    if value in TRUE_VALUES:
        return True
    if value in FALSE_VALUES:
        return False
    return default


def env_int(name: str, default: int) -> int:
    try:
        return int(env_text(name, str(default)))
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(env_text(name, str(default)))
    except ValueError:
        return default
