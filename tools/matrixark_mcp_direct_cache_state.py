#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Shared direct TemporalStore cache state for MatrixArk adapters."""

from __future__ import annotations

import os
import threading
from typing import Any

Json = dict[str, Any]

_DIRECT_RECORD_CACHE: dict[str, tuple[int, list[Json]]] = {}
_DIRECT_RECORD_CACHE_LOCK = threading.RLock()
_DIRECT_RECORD_CACHE_MAX_PREFIXES = 64
_DIRECT_RECORD_LOAD_LOCKS: dict[str, threading.RLock] = {}
_DIRECT_RETRIEVAL_CANDIDATE_CACHE: dict[str, Json] = {}
_DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK = threading.RLock()
_DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES = int(
    os.environ.get("MATRIXARK_DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES", "256")
)
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE: dict[str, list[tuple[str, int, int, Json]]] = {}
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK = threading.RLock()
_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES = int(
    os.environ.get("MATRIXARK_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES", "1024")
)
