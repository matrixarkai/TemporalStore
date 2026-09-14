#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Small environment parsing helpers for MatrixArk MCP modules."""

from __future__ import annotations

import os


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


def env_text(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_lower(name: str, default: str = "") -> str:
    return env_text(name, default).lower()


def flag_bool(value: object, default: bool) -> bool:
    """Read an already-fetched flag VALUE, as `env_bool` reads a variable name.

    `env_bool` is the name-taking half and is what most modules want. A flag captured into a
    module-level constant at import is tested later at a site that no longer has the variable name,
    and that site needs the same decision from the value it is holding. Both halves answer from
    TRUE_VALUES/FALSE_VALUES, so there is one vocabulary rather than two that agree today.

    THE DEFAULT IS THE POINT. A site that writes `if value: return value in TRUE_VALUES` has the
    words right and the fallback wrong: any value in neither set answers False, so a typo does not
    merely fail to turn the flag on, it turns OFF a default that was on. Returning `default` for an
    unrecognised value is what `env_bool` already does, and the sets are narrow on purpose -- `y`,
    `n` and `enabled` are in neither, and fall back.
    """
    normalized = str(value or "").strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    return default


def env_bool(name: str, default: bool = False) -> bool:
    return flag_bool(env_lower(name, "1" if default else "0"), default)


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
