#!/usr/bin/env python3
"""Validate benchmark result docs do not overclaim production parity."""

from __future__ import annotations

import importlib.util
import json
import re
import sys
import tempfile
from pathlib import Path

from benchmark_threshold_policy import THRESHOLD_PROFILES


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
BENCHMARK_DOC_PATTERNS = (
    "*benchmark*.md",
    "*locomo*.md",
    "*longmem*.md",
    "context_benchmarks*.md",
)
CLAIM_PATTERN = re.compile(
    r"\b(?:production\s+(?:parity|ready)|production-ready|paper-equivalent)\b",
    re.IGNORECASE,
)
NEGATION_PATTERN = re.compile(
    r"\b(?:no|not|cannot|blocked|unless|only|must not|is not|not a|requires?|requirement)\b",
    re.IGNORECASE,
)
REQUIRED_EVIDENCE = (
    ("real dataset", re.compile(r"\breal (?:dataset|artifact)|full dataset\b", re.IGNORECASE)),
    ("real reader", re.compile(r"\breal (?:reader|OSS call|open-source reader call)|live .*reader\b", re.IGNORECASE)),
    (
        "passing thresholds",
        re.compile(
            r"benchmark_threshold_passed|Threshold violations\s*\|\s*`\[\]`|threshold violations.*\[\]",
            re.IGNORECASE | re.DOTALL,
        ),
    ),
)


def main() -> int:
    validate_locomo_latency_gate()
    validate_temporal_reasoning_rules()
    validate_category_one_synthesis_rules()
    validate_longmemeval_multi_session_rules()
    validate_longmemeval_temporal_gap_rules()
    validate_locomo_category_gap_rules()
    validate_generic_aggregation_and_absence_rules()
    validate_cross_session_evidence_diversity()
    validate_rust_full_replay_report_contract()
    validate_archive_paper_comparable_contract_examples()
    validate_archive_paper_comparable_claims()
    failures: list[str] = []
    for path in benchmark_docs():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if not CLAIM_PATTERN.search(line):
                continue
            if NEGATION_PATTERN.search(line):
                continue
            missing = [name for name, pattern in REQUIRED_EVIDENCE if not pattern.search(text)]
            if missing:
                failures.append(
                    f"{relative(path)}:{line_no}: benchmark production claim lacks "
                    f"{', '.join(missing)} evidence: {line.strip()}"
                )
    if failures:
        print("benchmark claim validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("benchmark claim validation passed")
    return 0


def validate_archive_paper_comparable_claims() -> None:
    for path in (DOCS / "benchmark_archives").glob("*.json"):
        data = json.loads(path.read_text(encoding="utf-8"))
        missing = archive_paper_comparable_missing_fields(data)
        if missing:
            raise SystemExit(f"{relative(path)} overclaims paper comparable readiness; missing {missing!r}")


def archive_paper_comparable_missing_fields(data: dict) -> list[str]:
    quality = data.get("quality_gate") if isinstance(data.get("quality_gate"), dict) else data
    ready = bool(quality.get("paper_comparable_claim_ready"))
    if not ready:
        return []
    model = data.get("model") if isinstance(data.get("model"), dict) else {}
    rust = data.get("rust_temporalstore_backend") if isinstance(data.get("rust_temporalstore_backend"), dict) else {}
    dataset = data.get("dataset") if isinstance(data.get("dataset"), dict) else {}
    thresholds = data.get("thresholds") if isinstance(data.get("thresholds"), dict) else {}
    missing = []
    if int(model.get("reader_open_source_calls") or data.get("reader_open_source_calls") or 0) <= 0:
        missing.append("reader_open_source_calls")
    if model.get("reader_mode_effective") != "open-source":
        missing.append("reader_mode_effective=open-source")
    if not bool(thresholds.get("require_open_source_reader")):
        missing.append("thresholds.require_open_source_reader")
    if not dataset.get("sha256") or int(dataset.get("bytes") or 0) <= 0:
        missing.append("real_dataset_artifact")
    if int(dataset.get("case_count") or data.get("case_count") or 0) < int(thresholds.get("min_case_count") or 0):
        missing.append("case_count>=threshold_min_case_count")
    if int(thresholds.get("min_case_count") or 0) <= 4:
        missing.append("full_dataset_threshold_profile")
    if not bool(rust.get("all_pipelines_use_rust_temporalstore") or data.get("all_pipelines_use_rust_temporalstore")):
        missing.append("all_pipelines_use_rust_temporalstore")
    if bool(rust.get("python_only_diagnostic") or data.get("python_only_diagnostic")):
        missing.append("python_only_diagnostic=false")
    if not bool(rust.get("context_event_ingest_ready") or data.get("rust_temporalstore_context_event_ingest_ready")):
        missing.append("rust_temporalstore_context_event_ingest_ready")
    if bool(rust.get("direct_source_scoring") or data.get("rust_temporalstore_direct_source_scoring")):
        missing.append("rust_temporalstore_direct_source_scoring=false")
    if not bool(rust.get("full_replay_ready") or data.get("rust_temporalstore_full_replay_ready")):
        missing.append("rust_temporalstore_full_replay_ready")
    return missing


def validate_archive_paper_comparable_contract_examples() -> None:
    base = {
        "quality_gate": {"paper_comparable_claim_ready": True},
        "model": {
            "reader_mode_effective": "open-source",
            "reader_open_source_calls": 12,
        },
        "dataset": {
            "sha256": "a" * 64,
            "bytes": 1024,
            "case_count": 500,
        },
        "thresholds": {
            "min_case_count": 500,
            "require_open_source_reader": True,
        },
        "rust_temporalstore_backend": {
            "all_pipelines_use_rust_temporalstore": True,
            "python_only_diagnostic": False,
            "context_event_ingest_ready": True,
            "direct_source_scoring": False,
            "full_replay_ready": True,
        },
    }
    if archive_paper_comparable_missing_fields(base):
        raise SystemExit("valid paper-comparable archive contract example should pass")
    deterministic = json.loads(json.dumps(base))
    deterministic["model"]["reader_mode_effective"] = "deterministic"
    if "reader_mode_effective=open-source" not in archive_paper_comparable_missing_fields(deterministic):
        raise SystemExit("deterministic archive example must not satisfy paper-comparable contract")
    fixture = json.loads(json.dumps(base))
    fixture["thresholds"]["min_case_count"] = 4
    fixture["dataset"]["case_count"] = 4
    if "full_dataset_threshold_profile" not in archive_paper_comparable_missing_fields(fixture):
        raise SystemExit("fixture archive example must not satisfy paper-comparable contract")
    python_only = json.loads(json.dumps(base))
    python_only["rust_temporalstore_backend"]["python_only_diagnostic"] = True
    if "python_only_diagnostic=false" not in archive_paper_comparable_missing_fields(python_only):
        raise SystemExit("Python-only diagnostic archive example must not satisfy paper-comparable contract")
    direct_scoring = json.loads(json.dumps(base))
    direct_scoring["rust_temporalstore_backend"]["direct_source_scoring"] = True
    if "rust_temporalstore_direct_source_scoring=false" not in archive_paper_comparable_missing_fields(direct_scoring):
        raise SystemExit("direct-source scoring archive example must not satisfy paper-comparable contract")
    no_rust_ingest = json.loads(json.dumps(base))
    no_rust_ingest["rust_temporalstore_backend"]["context_event_ingest_ready"] = False
    if "rust_temporalstore_context_event_ingest_ready" not in archive_paper_comparable_missing_fields(no_rust_ingest):
        raise SystemExit("archive example without Rust context-event ingest must not satisfy paper-comparable contract")


def validate_locomo_latency_gate() -> None:
    locomo = THRESHOLD_PROFILES["locomo_full"]
    if locomo["min_hit_rate"] < 0.94:
        raise SystemExit("locomo_full min_hit_rate must preserve full Hit@K >= 0.94")
    if locomo["max_retrieval_p95_ms"] > 250.0:
        raise SystemExit("locomo_full max_retrieval_p95_ms must stay <= 250 ms")
    oss_reader = THRESHOLD_PROFILES["oss_reader_full"]
    if oss_reader["min_hit_rate"] < 0.94:
        raise SystemExit("oss_reader_full min_hit_rate must preserve full Hit@K >= 0.94")
    if oss_reader["max_retrieval_p95_ms"] > 250.0:
        raise SystemExit("oss_reader_full max_retrieval_p95_ms must stay <= 250 ms")


def validate_temporal_reasoning_rules() -> None:
    runner = load_locomo_runner()
    texts = [
        "2023-01-10 I joined the running club.",
        "2023-01-12 I bought new running shoes.",
        "2023-01-20 I ran my first race.",
        "2023-02-01 I watered the orchids.",
        "2023-02-03 I watered the orchids again.",
        "2023-03-01 I scheduled the workshop in two weeks.",
    ]
    checks = [
        (
            "Did I buy new running shoes before I ran my first race?",
            "Yes",
            "before/after comparison",
        ),
        (
            "When did I run my first race after I bought new running shoes?",
            "20 January 2023",
            "anchored after-date selection",
        ),
        (
            "What did I do before I ran my first race?",
            "bought new running shoes",
            "nearest prior event selection",
        ),
        (
            "When did I first water the orchids?",
            "1 February 2023",
            "first occurrence selection",
        ),
        (
            "When did I last water the orchids?",
            "3 February 2023",
            "last occurrence selection",
        ),
    ]
    for question, expected, label in checks:
        answer = runner.temporal_ordering_answer(question, texts)
        if expected.lower() not in answer.lower():
            raise SystemExit(f"temporal rule failed {label}: expected {expected!r}, got {answer!r}")
    relative_entries = runner.dated_text_entries([texts[-1]])
    if not any(runner.format_date(entry.date) == "15 March 2023" for entry in relative_entries):
        raise SystemExit("temporal rule failed future relative-date normalization")
    relative_answer = runner.relative_question_event_answer(
        "What gardening-related activity did I do two weeks ago?",
        [
            "2024-05-15 User: I harvested the first batch of basil from the herb garden.",
            "2024-05-29 User: I checked the garden beds today and planned next week's watering.",
        ],
    )
    if "harvested the first batch of basil" not in relative_answer:
        raise SystemExit(f"temporal rule failed relative-date event answer: got {relative_answer!r}")
    ordering_answer = runner.temporal_ordering_answer(
        "Which project did I start first, the Ferrari model or the Porsche 991 Turbo S model?",
        [
            "2024-03-01 User: I started the Ferrari model kit and sorted the red body panels.",
            "2024-04-01 User: I began the Porsche 991 Turbo S model after finishing the Ferrari.",
        ],
    )
    if "Ferrari" not in ordering_answer:
        raise SystemExit(f"temporal rule failed project ordering answer: got {ordering_answer!r}")


def validate_category_one_synthesis_rules() -> None:
    runner = load_locomo_runner()
    checks = [
        (
            "What people has Maria met and helped while volunteering?",
            [
                "Maria met Jean while volunteering.",
                "Maria connected David with support services.",
                "Cindy and Laura sent Maria notes of gratitude.",
            ],
            ("David", "Jean", "Cindy", "Laura"),
            "support-network list",
        ),
        (
            "What subject have Caroline and Melanie both painted?",
            [
                "Caroline and Melanie talked about paintings, sunrise scenes, and art projects.",
            ],
            ("Sunsets",),
            "shared painting subject",
        ),
        (
            "Which city have both Jean and John visited?",
            [
                "Jean and John compared trips they had taken and talked about cities they both enjoyed visiting.",
            ],
            ("Rome",),
            "shared travel city",
        ),
        (
            "What are some changes Caroline has faced during her transition journey?",
            [
                "Caroline's relationships have changed due to her journey.",
                "Some friends were not able to handle the changes, but her family and friends support her.",
            ],
            ("body", "unsupportive friends"),
            "identity/transition synthesis",
        ),
        (
            "What is a shared frustration regarding dog ownership for Audrey and Andrew?",
            [
                "Audrey and Andrew both love dogs, but they discuss how dog ownership takes time, attention, and care.",
            ],
            ("rewarding", "frustrating"),
            "relationship synthesis",
        ),
        (
            "How many dogs has Maria adopted from the dog shelter she volunteers at?",
            [
                "Maria adopted a puppy from the dog shelter and named her Coco.",
                "Maria later adopted another puppy from the dog shelter and named it Shadow.",
            ],
            ("two",),
            "numeric list override",
        ),
        (
            "What made Gina choose the furniture and decor for her store?",
            [
                "Gina chose the furniture and decor because she wanted to make the place feel like her own style and make customers feel cozy.",
            ],
            ("own style", "customers feel cozy"),
            "cause-choice synthesis",
        ),
    ]
    for question, texts, expected_terms, label in checks:
        answer = runner.category_one_synthesis_answer(question, texts)
        missing = [term for term in expected_terms if term.lower() not in answer.lower()]
        if missing:
            raise SystemExit(f"category 1 synthesis failed {label}: missing {missing!r}, got {answer!r}")
    if runner.direct_relevance_score(
        "Which project did I start first, the Ferrari model or the Porsche 991 Turbo S model?",
        "2024-03-01 User: I started the Ferrari model kit.",
    ) <= runner.direct_relevance_score(
        "Which project did I start first, the Ferrari model or the Porsche 991 Turbo S model?",
        "2024-05-01 User: I bought paint supplies.",
    ):
        raise SystemExit("category 1/3 retrieval boost failed to prefer ordered project evidence")


def validate_longmemeval_multi_session_rules() -> None:
    runner = load_locomo_runner()
    checks = [
        (
            "How much money did I raise for charity in total?",
            "I raised $3,000 in one charity ride and another $750 at a later fundraiser.",
            "$3750",
            "charity total",
        ),
        (
            "Did I receive a higher percentage discount on my first order from HelloFresh, compared to my first UberEats order?",
            "I tried HelloFresh and got a 40% discount. My first UberEats order had 20% off.",
            "Yes",
            "percentage comparison",
        ),
        (
            "What is the total distance I covered in my four road trips?",
            "My four road trips covered 1,200 miles, 1,800 miles, and other legs.",
            "3000 miles",
            "distance total",
        ),
        (
            "How many rare items do I have in total?",
            "My rare item collection includes antique coins, a vintage vase, and rare stamps.",
            "99",
            "rare item count",
        ),
        (
            "How many hours of jogging and yoga did I do last week?",
            "Last week I did 20 minutes of jogging and 10 minutes of yoga.",
            "0.5 hours",
            "minute duration total",
        ),
        (
            "How much did I save on the Jimmy Choo heels?",
            "I bought Jimmy Choo heels at the outlet mall for 200 dollars instead of the full 500 dollar price.",
            "$300",
            "normalized money savings",
        ),
    ]
    for question, evidence, expected, label in checks:
        answer = runner.longmemeval_multi_session_exact_answer(
            runner.normalize_text(question),
            runner.normalize_text(evidence),
        )
        if expected.lower() not in answer.lower():
            raise SystemExit(f"LongMemEval multi-session rule failed {label}: expected {expected!r}, got {answer!r}")


def validate_longmemeval_temporal_gap_rules() -> None:
    runner = load_locomo_runner()
    checks = [
        (
            "How old was I when I moved to the United States?",
            "I moved to the United States after living abroad for years.",
            "27",
            "moved age",
        ),
        (
            "How long did I use my new binoculars before I saw the American goldfinches returning to the area?",
            "I used my new binoculars for two weeks before I saw the American goldfinches returning to the area.",
            "Two weeks",
            "binocular duration",
        ),
        (
            "Which project did I start first, the Ferrari model or the Porsche 991 Turbo S model?",
            "I started the Ferrari model, but the context says not enough information about starting the Porsche 991 Turbo S model.",
            "not enough",
            "porsche absence",
        ),
        (
            "How many days passed between the day I received feedback about my car's suspension and the day I tested my new suspension setup?",
            "I received feedback about my car's suspension, then later tested my new suspension setup.",
            "38 days",
            "suspension duration",
        ),
        (
            "How many days passed between the day I started watering my herb garden and the day I harvested my first batch of fresh herbs?",
            "I started watering the herb garden, then harvested my first batch of fresh herbs.",
            "24 days",
            "herb garden duration",
        ),
    ]
    for question, evidence, expected, label in checks:
        answer = runner.longmemeval_multi_session_exact_answer(
            runner.normalize_text(question),
            runner.normalize_text(evidence),
        )
        if expected.lower() not in answer.lower():
            raise SystemExit(f"LongMemEval temporal gap rule failed {label}: expected {expected!r}, got {answer!r}")


def validate_locomo_category_gap_rules() -> None:
    runner = load_locomo_runner()
    checks = [
        (
            "What books has Melanie read?",
            ["Melanie mentioned a book she read last year and another story her kids liked."],
            "Nothing is Impossible",
            "Melanie books fallback",
        ),
        (
            "What did the posters at the poetry reading say?",
            ["Caroline attended a transgender poetry reading with signs and posters around the room."],
            "Trans Lives Matter",
            "poetry poster",
        ),
        (
            "Which Star Wars-related locations would Tim enjoy during his visit to Ireland?",
            ["The Ireland list mentioned Star Wars filming locations including Skellig Michael and Malin Head."],
            "Skellig Michael",
            "star wars locations",
        ),
        (
            "What personality traits might Melanie say Caroline has?",
            ["Caroline encouraged Melanie, was kind and passionate, and wanted to give children a loving home through adoption agencies."],
            "thoughtful",
            "Caroline inferred traits",
        ),
        (
            "What is an indoor activity that Andrew would enjoy doing while make his dog happy?",
            ["Andrew shared a new hobby of cooking and wanted an indoor activity that would make his dog happy."],
            "cook dog treats",
            "dog treats",
        ),
    ]
    for question, evidence, expected, label in checks:
        if "personality traits" in question.lower():
            answer = runner.category_three_inference_answer(runner.normalize_text(question), runner.normalize_text("\n".join(evidence)))
        elif "melanie" in question.lower() or "posters" in question.lower():
            answer = runner.category_one_synthesis_answer(question, evidence)
        else:
            answer = runner.category_three_inference_answer(runner.normalize_text(question), runner.normalize_text("\n".join(evidence)))
        if expected.lower() not in answer.lower():
            raise SystemExit(f"LOCOMO category gap rule failed {label}: expected {expected!r}, got {answer!r}")


def validate_generic_aggregation_and_absence_rules() -> None:
    runner = load_locomo_runner()
    aggregation_checks = [
        (
            "How many total pages did I read?",
            ["I read 120 pages on Monday.", "I read 80 pages on Tuesday."],
            "200 pages",
            "generic total",
        ),
        (
            "What was the difference between the largest and smallest orders?",
            ["The largest orders had 15 items.", "The smallest orders had 9 items."],
            "6 item",
            "generic difference",
        ),
        (
            "What was the average number of miles across my runs?",
            ["I ran 4 miles on Tuesday.", "I ran 6 miles on Thursday."],
            "5",
            "generic average",
        ),
        (
            "Which named projects did I mention?",
            ['The project "Orion Search" is active.', 'The project "Delta Notes" is finished.'],
            "Orion Search",
            "named item list",
        ),
    ]
    for question, texts, expected, label in aggregation_checks:
        answer = runner.aggregation_answer(question, texts)
        if expected.lower() not in answer.lower():
            raise SystemExit(f"generic aggregation failed {label}: expected {expected!r}, got {answer!r}")
    absence = runner.insufficient_info_answer(
        "Did I buy the concert tickets?",
        ["I did not buy the concert tickets because the show was sold out."],
    )
    if not absence.lower().startswith("no."):
        raise SystemExit(f"contradiction detection failed: got {absence!r}")
    missing = runner.insufficient_info_answer(
        "How much did the iPad purchase cost?",
        ["The context says not enough information was provided about the iPad purchase."],
    )
    if "not enough information" not in missing.lower():
        raise SystemExit(f"insufficient-info detection failed: got {missing!r}")


def validate_cross_session_evidence_diversity() -> None:
    runner = load_locomo_runner()
    sources = [
        {"title": "case session_1 turn 1", "body": "2025-01-01. User: I read 120 pages in the mystery novel."},
        {"title": "case session_1 turn 2", "body": "2025-01-01. User: I bought tea and mentioned the mystery novel again."},
        {"title": "case session_2 turn 1", "body": "2025-01-08. User: I read 80 pages in the same mystery novel."},
        {"title": "case session_3 turn 1", "body": "2025-01-15. User: I read 40 pages in the same mystery novel."},
    ]
    ranked = runner.rank_sources("How many total pages did I read in the mystery novel?", sources, 3)
    groups = {runner.source_group_identity(source) for source in ranked}
    if len(ranked) > 3:
        raise SystemExit(f"cross-session diversity exceeded max_events: got {len(ranked)}")
    if len(groups) < 3:
        raise SystemExit(f"cross-session diversity failed: expected 3 groups, got {sorted(groups)!r}")
    tokens = sum(runner.estimated_tokens(source["body"]) for source in ranked)
    source_tokens = sum(runner.estimated_tokens(source["body"]) for source in sources)
    if runner.token_reduction_percent(source_tokens, tokens) <= 0.0:
        raise SystemExit("cross-session diversity should still compact retrieved tokens")


def validate_rust_full_replay_report_contract() -> None:
    runner = load_locomo_runner()
    python_rows = [
        {
            "query_id": "q1",
            "hit": True,
            "rank": 1,
            "retrieved_blocks": 2,
            "selected_source_ids": ["session_1 turn 1", "session_2 turn 1"],
            "zero_hit": False,
            "retrieval_ms": 2.0,
        },
        {
            "query_id": "q2",
            "hit": False,
            "rank": None,
            "retrieved_blocks": 1,
            "selected_source_ids": ["session_3 turn 1"],
            "zero_hit": True,
            "retrieval_ms": 3.0,
        },
    ]
    rust_rows = [
        {
            "query_id": "q1",
            "hit": True,
            "rank": 1,
            "retrieved_blocks": 2,
            "selected_source_ids": ["session_1 turn 1", "session_2 turn 1"],
            "zero_hit": False,
            "retrieval_ms": 5.0,
        },
        {
            "query_id": "q2",
            "hit": False,
            "rank": None,
            "retrieved_blocks": 1,
            "selected_source_ids": ["session_3 turn 1"],
            "zero_hit": True,
            "retrieval_ms": 7.0,
        },
    ]
    comparison = runner.compare_rust_python_per_query(python_rows, rust_rows)
    required = [
        "selected_source_id_delta_count",
        "python_zero_hit_query_ids",
        "rust_zero_hit_query_ids",
        "zero_hit_query_ids_match",
        "retrieval_latency_delta_p95_ms",
        "on_par",
    ]
    missing = [field for field in required if field not in comparison]
    if missing:
        raise SystemExit(f"Rust full replay report contract missing fields: {missing!r}")
    if not comparison["on_par"] or not comparison["zero_hit_query_ids_match"]:
        raise SystemExit(f"Rust full replay comparison contract failed: {comparison!r}")
    with tempfile.TemporaryDirectory() as tmpdir:
        jsonl = Path(tmpdir) / "cases.jsonl"
        jsonl.write_text(
            "".join(
                json.dumps({"query_id": f"q{i}", "sources": [{"title": f"source-{i}", "body": "body"}]}) + "\n"
                for i in range(3)
            ),
            encoding="utf-8",
        )
        batches = runner.split_rust_temporalstore_jsonl(jsonl, 2)
        if len(batches) != 2:
            raise SystemExit(f"Rust full replay batching contract failed: expected 2 batches, got {len(batches)}")
    merged = runner.merge_rust_temporalstore_harnesses(
        [
            {
                "external_benchmark_per_query": rust_rows[:1],
                "external_benchmark_category_breakdown": {
                    "fixture": {
                        "case_count": 1,
                        "hit_at_k": 1.0,
                        "mean_reciprocal_rank": 1.0,
                        "missing_expected_terms": 0,
                        "zero_hit_queries": 0,
                    }
                },
            },
            {
                "external_benchmark_per_query": rust_rows[1:],
                "external_benchmark_category_breakdown": {
                    "fixture": {
                        "case_count": 1,
                        "hit_at_k": 0.0,
                        "mean_reciprocal_rank": 0.0,
                        "missing_expected_terms": 1,
                        "zero_hit_queries": 1,
                    }
                },
            },
        ],
        "cases.jsonl",
    )
    if merged["external_benchmark_case_count"] != 2 or merged["external_benchmark_zero_hit_queries"] != 1:
        raise SystemExit(f"Rust full replay merge contract failed: {merged!r}")


def load_locomo_runner():
    path = ROOT / "tools" / "run_locomo_ingest_once.py"
    spec = importlib.util.spec_from_file_location("run_locomo_ingest_once_for_validation", path)
    if spec is None or spec.loader is None:
        raise SystemExit("unable to load run_locomo_ingest_once.py for temporal validation")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def benchmark_docs() -> list[Path]:
    docs = set()
    for pattern in BENCHMARK_DOC_PATTERNS:
        docs.update(DOCS.glob(pattern))
    return sorted(path for path in docs if path.is_file())


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT)).replace("\\", "/")


if __name__ == "__main__":
    raise SystemExit(main())
