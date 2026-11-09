#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""TemporalStore entry point for the mem0-compatible ``Memory`` client."""

try:
    from matrixark_mem0_compat import Memory
except ImportError:
    from tools.matrixark_mem0_compat import Memory  # type: ignore

__all__ = ["Memory"]
