#!/usr/bin/env python3
"""Validate and report MatrixArk layer traversal + cross-session rerank parity.

This is a lightweight source/corpus gate for the native C++ and Rust context
paths. The heavy live benchmark still belongs to the scale runners; this script
keeps the product behavior contract visible and repeatable without relinking the
full local TemporalStore tree.
"""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "docs" / "matrixark_layer_cross_session_rerank_report"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def check_contains(name: str, text: str, needles: list[str]) -> dict[str, Any]:
    missing = [needle for needle in needles if needle not in text]
    return {
        "name": name,
        "status": "pass" if not missing else "fail",
        "missing": missing,
        "checked": needles,
    }


def load_shared_case() -> dict[str, Any]:
    corpus_path = ROOT / "compat" / "unified_temporalstore_cases.json"
    corpus = json.loads(corpus_path.read_text(encoding="utf-8"))
    for case in corpus.get("cases", []):
        if case.get("name") == "context_cross_session_shared_resource_weighted_rerank":
            return case
    return {}


def build_report() -> dict[str, Any]:
    cpp_impl = read(ROOT / "src" / "extension" / "context" / "implement.cc")
    cpp_tests = read(ROOT / "src" / "extension" / "context" / "test.cc")
    rust_workflow = read(ROOT / "crates" / "temporalstore-rust" / "src" / "context_workflow.rs")
    rust_query = read(ROOT / "crates" / "temporalstore-rust" / "src" / "context_workflow" / "query.rs")
    rust_tests = read(ROOT / "crates" / "temporalstore-rust" / "src" / "context_workflow" / "tests.rs")
    shared_case = load_shared_case()

    checks = [
        check_contains(
            "cpp_layer_traversal_native",
            cpp_impl,
            [
                "REGISTER_FUNCTION(CONTEXT, TRAVERSE_CONTEXT_TREE",
                "QueryChildrenInternal",
                "CosineSimilarity(request.query_vector(), embedding.vector())",
                "top_k_per_depth",
                "max_children_scored_per_parent",
            ],
        ),
        check_contains(
            "cpp_temporal_decay_event_query",
            cpp_impl,
            [
                "DecayedEventScore",
                "rank_by_decayed_score",
                "min_decayed_score",
                "decay_half_life_ms",
            ],
        ),
        check_contains(
            "cpp_unit_coverage_for_layer_and_decay",
            cpp_tests,
            [
                "ctx-traverse-global-topk",
                "set_rank_by_decayed_score(true)",
                "ASSERT_GT(response.decayed_scores(0), response.decayed_scores(1))",
                "TRAVERSE_CONTEXT_TREE",
            ],
        ),
        check_contains(
            "rust_native_weighted_rerank",
            rust_query + rust_workflow,
            [
                "context_weighted_rerank_score",
                "context_temporal_decay_boost",
                "context_business_boost",
                "shared",
                "tenant/shared",
                "Reverse(context_weighted_rerank_score",
            ],
        ),
        check_contains(
            "rust_unit_coverage_for_cross_session_and_shared_resource",
            rust_tests,
            [
                "context_retrieval_reranks_cross_session_and_shared_resource_evidence",
                "session:old/project_aurora",
                "session:current/project_aurora",
                "tenant/shared/resources/project_aurora_gpu_runbook.pdf#page=1",
                "expected current cross-session Bob evidence first",
                "expected shared resource evidence first",
            ],
        ),
        {
            "name": "shared_corpus_case",
            "status": "pass" if shared_case else "fail",
            "case": shared_case.get("name", ""),
            "rust_runner": (
                shared_case.get("steps", [{}])[0]
                .get("command", {})
                .get("rust_runner", "")
                if shared_case
                else ""
            ),
            "required_paths": (
                shared_case.get("steps", [{}])[0]
                .get("command", {})
                .get("required_paths", [])
                if shared_case
                else []
            ),
        },
    ]

    behavior = {
        "pipeline": [
            "query understanding creates source/resource/session intent",
            "scope filter runs before candidate eligibility",
            "ContextNode tree traversal scores child L0/L1 embeddings layer by layer",
            "current-session leaf candidates are fetched first",
            "cross-session candidates use bounded session/node fanout",
            "shared resources/skills use separate shared path quota",
            "rerank combines lexical/embedding relevance, temporal freshness, and business boosts",
            "ContextPack assembly spends tokens on selected evidence only",
        ],
        "rerank_factors": {
            "relevance": "primary score from query/text overlap or embedding candidate score",
            "temporal_decay": "fresh/current evidence gets boost; stale evidence can still win if relevance is strong",
            "business_boost": "approval, owner, deadline, cost, policy, procedure, resource, and skill evidence get extra weight",
            "shared_resource_boost": "tenant/shared and global resource evidence gets a modest boost when the query asks for docs/runbooks/policy",
        },
        "budget_defaults": {
            "same_session": "highest priority; fills first when relevant",
            "cross_session": "bounded cap; normally summaries/entities/compressions first, raw events only for high-confidence evidence",
            "shared_resources_skills": "separate quota and eligibility before scoring; selected only when query intent matches",
        },
    }

    status = "pass" if all(check.get("status") == "pass" for check in checks) else "fail"
    return {
        "status": status,
        "title": "MatrixArk Layer Traversal, Shared Resource, Cross-Session Rerank Report",
        "checks": checks,
        "behavior": behavior,
        "validation_commands": [
            "python3 -m json.tool compat/unified_temporalstore_cases.json >/tmp/unified_context_cases.json",
            "python3 tools/run_context_shared_cases.py --validate-only",
            "cargo test -p temporalstore-rust context_retrieval_reranks_cross_session_and_shared_resource_evidence --lib -- --test-threads=1",
        ],
        "notes": [
            "C++ native surface already contains TRAVERSE_CONTEXT_TREE and decayed QUERY_EVENTS primitives.",
            "Rust native retrieve path now applies weighted reranking before the existing relevance/tier/time tie-breakers.",
            "Live full-Cargo validation can be slow in this Windows/WSL workspace; the report records the command separately from the fast source/corpus gate.",
        ],
    }


def markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# {report['title']}",
        "",
        f"Status: **{report['status']}**",
        "",
        "## Behavior Contract",
    ]
    for item in report["behavior"]["pipeline"]:
        lines.append(f"- {item}")
    lines.extend(["", "## Rerank Factors"])
    for key, value in report["behavior"]["rerank_factors"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Budget Defaults"])
    for key, value in report["behavior"]["budget_defaults"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## C++ / Rust Checks"])
    for check in report["checks"]:
        lines.append(f"- `{check['name']}`: **{check['status']}**")
        if check.get("missing"):
            lines.append(f"  - missing: `{check['missing']}`")
        if check.get("rust_runner"):
            lines.append(f"  - rust runner: `{check['rust_runner']}`")
    lines.extend(["", "## Validation Commands"])
    for command in report["validation_commands"]:
        lines.append(f"```bash\n{command}\n```")
    lines.extend(["", "## Notes"])
    for note in report["notes"]:
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def html_page(md: str, report: dict[str, Any]) -> str:
    escaped_md = html.escape(md)
    escaped_json = html.escape(json.dumps(report, indent=2, sort_keys=True))
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>{html.escape(report['title'])}</title>
  <style>
    body {{ font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif; margin: 32px; color: #17202a; background: #f8fafc; }}
    main {{ max-width: 1180px; margin: 0 auto; }}
    h1 {{ font-size: 28px; margin-bottom: 8px; }}
    h2 {{ margin-top: 28px; }}
    .status {{ display: inline-block; padding: 4px 10px; border-radius: 999px; background: #dcfce7; color: #166534; font-weight: 700; }}
    pre {{ background: #0f172a; color: #e2e8f0; padding: 16px; border-radius: 8px; overflow: auto; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }}
    .card {{ background: white; border: 1px solid #dbe3ef; border-radius: 8px; padding: 14px; }}
    code {{ background: #e2e8f0; padding: 1px 4px; border-radius: 4px; }}
  </style>
</head>
<body>
<main>
  <h1>{html.escape(report['title'])}</h1>
  <p class=\"status\">{html.escape(report['status'])}</p>
  <h2>Summary</h2>
  <pre>{escaped_md}</pre>
  <h2>Machine Report</h2>
  <pre>{escaped_json}</pre>
</main>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-prefix", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    report = build_report()
    out_prefix = args.out_prefix
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    md = markdown(report)
    out_prefix.with_suffix(".json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    out_prefix.with_suffix(".md").write_text(md, encoding="utf-8")
    out_prefix.with_suffix(".html").write_text(html_page(md, report), encoding="utf-8")
    print(json.dumps({"status": report["status"], "json": str(out_prefix.with_suffix(".json")), "md": str(out_prefix.with_suffix(".md")), "html": str(out_prefix.with_suffix(".html"))}, indent=2))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
