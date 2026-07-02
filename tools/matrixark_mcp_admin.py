#!/usr/bin/env python3
"""MatrixArk admin/auth/portal orchestration policy."""

from __future__ import annotations

ADMIN_OPERATION_PREFIXES = ("matrixark_admin_", "matrixark_auth_")
ADMIN_OPERATION_TOOLS = {
    "matrixark_management_portal",
    "matrixark_ingestion_dashboard",
}


def is_admin_tool(name: str) -> bool:
    return name.startswith(ADMIN_OPERATION_PREFIXES) or name in ADMIN_OPERATION_TOOLS
