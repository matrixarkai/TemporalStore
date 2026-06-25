#!/usr/bin/env python3
"""Validate production-readiness workflow coverage.

The readiness gate reports a fixed public service list. CI must keep a
service-readiness matrix entry for every service so no area silently loses its
artifact or remediation output.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INSTALLED_WORKFLOW = ROOT / ".github" / "workflows" / "rust-production-readiness.yml"
DOCUMENTED_WORKFLOW = ROOT / "docs" / "ci" / "rust-production-readiness.workflow.yml"
REQUIRED_SERVICES = [
    "client",
    "proxy",
    "ingestion",
    "data_node",
    "metaserver",
    "storage_cache",
    "feature_modules",
    "context_workflow",
    "fault_tolerance",
    "deployment_ops",
    "scale_testing",
    "raft_replication",
]
REQUIRED_SNIPPETS = [
    "Validate unified C++/Rust corpus",
    "python3 tools/run_temporalstore_unified_tests.py --validate-only",
    "Run unified Rust corpus",
    "tools/run_temporalstore_unified_tests.sh",
    "Capture production readiness report",
]


def workflow_services(text: str) -> set[str]:
    return set(re.findall(r"^\s+-\s+([a-z][a-z0-9_]+)\s*$", text, flags=re.MULTILINE))


def validate(path: Path, *, required: bool) -> None:
    if not path.exists():
        if required:
            raise SystemExit(f"missing readiness workflow: {path}")
        print(f"skipped missing optional workflow {path.relative_to(ROOT)}")
        return
    text = path.read_text(encoding="utf-8")
    services = workflow_services(text)
    missing_services = sorted(set(REQUIRED_SERVICES) - services)
    if missing_services:
        raise SystemExit(
            f"{path}: service-readiness matrix missing {', '.join(missing_services)}"
        )
    missing_snippets = [snippet for snippet in REQUIRED_SNIPPETS if snippet not in text]
    if missing_snippets:
        raise SystemExit(f"{path}: missing required steps: {', '.join(missing_snippets)}")
    print(
        f"validated {path.relative_to(ROOT)}: "
        f"services={len(REQUIRED_SERVICES)} unified_corpus_gate=true"
    )


def main() -> int:
    require_installed = "--require-installed" in sys.argv[1:]
    validate(DOCUMENTED_WORKFLOW, required=True)
    validate(INSTALLED_WORKFLOW, required=require_installed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
