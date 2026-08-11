#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Audit buffering helpers for TemporalStore-backed MatrixArk adapters."""

from __future__ import annotations

import threading
import time

try:
    from tools.matrixark_mcp_core import Json, _mcp_debug_log
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, _mcp_debug_log


class TemporalAuditAdapterMixin:
    """Buffered/deferred audit behavior for TemporalStore adapters."""

    def append_audit(self, record: Json) -> None:
        if self._audit_mode == "drop":
            _mcp_debug_log("matrixark audit record dropped by MATRIXARK_DIRECT_AUDIT_MODE=drop")
            return
        if self._audit_mode == "sync":
            self.append(record)
            return
        with self._audit_lock:
            self._audit_buffer.append(record)
            if self._audit_mode == "buffered":
                self._ensure_audit_flusher_locked()
            max_pending = self._audit_buffer_max_records * 4
            if len(self._audit_buffer) > max_pending:
                dropped = len(self._audit_buffer) - max_pending
                self._audit_buffer = self._audit_buffer[-max_pending:]
                _mcp_debug_log(f"matrixark audit buffer dropped {dropped} oldest records after flush lag")

    def flush_audits(self) -> None:
        with self._audit_lock:
            if not self._audit_buffer:
                return
            records = self._audit_buffer
            self._audit_buffer = []
        try:
            self.append_many(records)
        except Exception as exc:
            with self._audit_lock:
                self._audit_flush_failures += 1
                remaining_capacity = max(0, self._audit_buffer_max_records * 2 - len(self._audit_buffer))
                if remaining_capacity:
                    self._audit_buffer = records[-remaining_capacity:] + self._audit_buffer
            _mcp_debug_log(f"matrixark audit flush failed: {exc}")

    def _ensure_audit_flusher_locked(self) -> None:
        if self._audit_flusher_started:
            return
        self._audit_flusher_started = True
        thread = threading.Thread(target=self._audit_flush_loop, name="matrixark-audit-flusher", daemon=True)
        thread.start()

    def _audit_flush_loop(self) -> None:
        while True:
            time.sleep(self._audit_flush_interval_s)
            try:
                self.flush_audits()
            except Exception as exc:
                _mcp_debug_log(f"matrixark audit flush loop failed: {exc}")
