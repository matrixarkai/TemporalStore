#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The last context pack that loaded, kept so a turn that cannot build one still has something.

Emitting nothing tells an agent it has NO history -- wrongly, and silently. Context that is a turn
or two old is a far smaller error, so each pack that loads is remembered and served back when a
later turn comes up empty.

Shared by both hooks on purpose. Claude and Codex run entirely separate entry points
(`matrixark_agent_hook.py` and `matrixark_codex_hook.py`), and a fix applied to one leaves the
other silently broken -- which is exactly what happened: Claude turns were protected while every
Codex turn still returned `{}`. Two copies of a cache-key scheme would also be free to drift until
one agent served the other's pack.
"""
from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path

DEFAULT_MAX_AGE_S = 86400.0


def context_pack_cache_path(agent: str, workspace_root: str) -> Path:
    """Where the last pack that loaded is kept, per agent and per workspace.

    Keyed by BOTH so one project's context can never surface in another and Claude can never be
    served Codex's pack: a stale pack is a small error, the wrong pack is a large one.
    """
    root = os.environ.get("MATRIXARK_HOOK_PACK_CACHE_DIR") or str(
        Path.home() / ".matrixark" / "hook-pack-cache"
    )
    stamp = hashlib.sha256((workspace_root or "-").encode("utf-8")).hexdigest()[:16]
    return Path(root) / f"{agent}-{stamp}.txt"


def remember_context_pack(path: Path, text: str) -> None:
    """Keep this pack, so a later turn that cannot reach the store still has something true."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename: a turn that dies mid-write must not leave a torn pack behind for the
        # next one to serve.
        temporary = path.with_suffix(".tmp")
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(path)
    except OSError:
        pass


def recover_context_pack(path: Path, *, max_age_s: float) -> tuple[str, float]:
    """The last pack that loaded, if it is recent enough to still be worth serving.

    Returns ("", 0.0) when there is nothing usable, so callers keep the fail-open contract and
    never invent context.
    """
    try:
        age_s = time.time() - path.stat().st_mtime
        if age_s > max_age_s:
            return "", 0.0
        return path.read_text(encoding="utf-8"), age_s
    except OSError:
        return "", 0.0


def pack_cache_max_age_s() -> float:
    """How old a pack may be and still be served. Past this, nothing is better than stale."""
    raw = os.environ.get("MATRIXARK_HOOK_PACK_CACHE_MAX_AGE_S", str(int(DEFAULT_MAX_AGE_S)))
    try:
        return max(0.0, float(str(raw).strip()))
    except ValueError:
        return DEFAULT_MAX_AGE_S


def store_answered(retrieve: object) -> bool:
    """Whether the store returned a pack for this turn, whatever was rendered from it.

    This is the question the fallback needs, and it is not "did anything get rendered". A turn whose
    retrieved pack contains only the hook's own heartbeat renders to nothing on purpose -- there was
    an answer, and it had nothing in it worth showing. Serving the previous pack there injects
    stale context into a turn that deliberately has none, which is the opposite of the fallback's
    reason for existing: emitting {} when the store COULD NOT answer tells the agent it has no
    history, which is both wrong and silent.
    """
    if not isinstance(retrieve, dict) or retrieve.get("_hook_tool_timeout"):
        return False
    return any(bool(retrieve.get(key)) for key in
               ("pack_id", "context_pack_id", "context", "refs", "selected_refs"))


def label_previous_pack(previous: str, age_s: float) -> str:
    """Mark a served pack as prior context, in band, with its age.

    The label is not decoration: without it the model cannot tell a stale pack from a fresh one,
    and silently passing off old context as current is the failure this whole path exists to avoid.
    """
    return (
        f"[prior context: the store did not answer this turn; this is the last pack that loaded, "
        f"{age_s / 60.0:.0f} min ago]\n{previous}"
    )
