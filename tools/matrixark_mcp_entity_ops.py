#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Entity patch and merge helpers for MatrixArk MCP extraction."""

from __future__ import annotations

import re
from typing import Any

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_summaries import summarize_text
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_summaries import summarize_text


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import entity_patch
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import entity_patch


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import parse_entity_patch
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import parse_entity_patch


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import edit_distance
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import edit_distance


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import best_span_by_edit_distance
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import best_span_by_edit_distance


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import apply_entity_patch
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import apply_entity_patch


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import apply_entity_patches
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import apply_entity_patches
