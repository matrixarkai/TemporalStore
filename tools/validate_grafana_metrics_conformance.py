#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate Rust Grafana/Prometheus parity evidence against ops families."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "docs" / "ops" / "temporalstore-dashboard.json"
# Every dashboard whose panels query engine families. The validator read only the first one
# for as long as it existed, so a panel added to any other file was checked by nothing.
DASHBOARDS = (
    ROOT / "docs" / "ops" / "temporalstore-dashboard.json",
    ROOT / "docs" / "ops" / "temporalstore-cluster-dashboard.json",
    # The two a customer is most likely to import first, and until now checked by nothing: their
    # panels query `matrixark_*` families emitted by the Python gateway, and every check here read
    # only the Rust sources. Their metrics happen to be fine -- all 19 resolve -- which is exactly
    # why this is worth wiring before something breaks rather than after.
    ROOT / "docs" / "ops" / "matrixark-gateway-dashboard.json",
    ROOT / "docs" / "ops" / "matrixark-ingestion-dashboard.json",
)

# Python modules that publish Prometheus text. Same declaration rule as the engine: a family counts
# as emitted where a `# HELP` or `# TYPE` line declares it, not merely where its name appears.
PY_SOURCE_ROOT = ROOT / "tools"
ALERTS = ROOT / "docs" / "ops" / "temporalstore-alerts.yml"
# The coverage doc this is checked against. The previous path named a file that has never
# existed, and `DOC.exists()` turned that into ten separate "family is undocumented" failures
# with one root cause -- a missing file reported as a missing description, ten times over.
DOC = ROOT / "docs" / "ops" / "temporalstore-grafana-metrics-coverage.md"
# Files known to emit metrics when this discovery was written. Kept as a FLOOR: if the walk below
# stops returning one of these, something has moved and the scan has silently narrowed, which is the
# failure this whole guard exists to prevent.
RUST_SOURCE_FLOOR = (
    "engine.rs",
    "raft.rs",
    "proxy.rs",
    "ingestion.rs",
    "bin/server.rs",
    "bin/metaserver.rs",
    "bin/ops_scale_readiness_harness.rs",
    "bin/matrixark_rust_proxy.rs",
    "bin/matrixark_rust_direct_sdk.rs",
    # NOT under bin/: the hand-maintained list said bin/matrixark_rust_proxy_impl.rs, which does
    # not exist, and the loader skipped it with `if path.exists()` -- so a file with 33 series was
    # silently absent from a scan that reported itself complete.
    "matrixark_rust_proxy_impl.rs",
)

RUST_SRC_ROOT = ROOT / "crates" / "temporalstore-rust" / "src"


def _discover_rust_sources() -> list:
    """Every non-test Rust file under the crate, not a hand-maintained list.

    The list this replaces named ten files while twenty emit Prometheus series, so the validator
    reported conformance over 15% of its subject. A file added later -- or a metric moved into a
    submodule, which is how `bin/server/metrics.rs` came to hold 26 of them -- would never be seen.

    Test modules are excluded: a series named only inside a `#[cfg(test)]` fixture is not something
    a deployment emits, and counting it would let a panel query a series that exists only in tests.
    """
    out = []
    for path in sorted(RUST_SRC_ROOT.rglob("*.rs")):
        parts = path.relative_to(RUST_SRC_ROOT).parts
        if "tests" in parts or path.name.startswith("test_"):
            continue
        out.append(path)
    return out


RUST_SOURCES = _discover_rust_sources()


FAMILY = r"(?:temporalstore|matrixark)_[A-Za-z0-9_]+"

# A declaration as it appears in the exposition text the process writes. The trailing lookahead
# rejects a templated family: the metaserver declares one whose name ends in a `{name}` placeholder
# filled at runtime, and recording the prefix as a family invents one that is never published.
EXPOSITION_DECLARATION = re.compile(
    r"#\s*(HELP|TYPE)\s+(%s)(?![{A-Za-z0-9_])(?:\s+(\w+))?" % FAMILY)
# The declaration text on a line, so the rest of that line can be read for a sample.
DECLARATION_ON_LINE = re.compile(r"#\s*(?:HELP|TYPE)\s+%s" % FAMILY)

CFG_TEST_ATTRIBUTE = re.compile(r"^\s*#\[cfg\(test\)\]\s*$")

# Modules that render exposition text today. Kept as a FLOOR for the same reason RUST_SOURCE_FLOOR
# is: if renderer discovery stops returning one, the emission check below has narrowed and every
# family that module publishes would start reading as "not emitted" -- or, worse, a narrowing that
# removes the whole check would read as a clean run.
RENDERER_FLOOR = (
    "bin/metaserver.rs",
    "bin/server/metrics.rs",
    "engine/prometheus_metrics.rs",
    "matrixark_rust_proxy_impl.rs",
    "meta/subsystem_metrics.rs",
    "proxy/prometheus.rs",
    "raft.rs",
)

# 480 sample sites across 363 families when this was measured. Both floors sit far below that: they
# catch a scan that has broken, not a release that trimmed a metric. Without them a scan returning
# nothing reports every family as unemitted, and a scan whose regex stopped matching reports every
# family as emitted -- the second failure is silent, which is the one worth a floor.
SAMPLE_SITE_FLOOR = 300
EMITTED_FAMILY_FLOOR = 250


def strip_test_modules(text: str) -> str:
    """The source with `#[cfg(test)] mod ... { ... }` bodies removed.

    A test asserting `metrics.contains("temporalstore_proxy_serving_mode{mode=...")` writes the
    family name in exactly the shape a renderer does. Counting it would let the emission check
    below pass on a family that only a test produces, which is the defect this whole pass is
    about wearing different clothes.
    """
    lines = text.splitlines()
    kept, index, total = [], 0, len(lines)
    while index < total:
        if CFG_TEST_ATTRIBUTE.match(lines[index]):
            cursor = index + 1
            while cursor < total and "{" not in lines[cursor]:
                cursor += 1
            if cursor >= total:
                break
            depth = 0
            while cursor < total:
                depth += lines[cursor].count("{") - lines[cursor].count("}")
                cursor += 1
                if depth <= 0:
                    break
            index = cursor
            continue
        kept.append(lines[index])
        index += 1
    return "\n".join(kept)


def _renderer_bodies() -> dict:
    """path -> test-stripped source, for every discovered file that renders exposition text.

    A file with no `# HELP`/`# TYPE` line of its own is not an exposition renderer, and the names it
    mentions are not emissions: `proxy.rs` carries a family-to-panel mapping table and
    `bin/ops_scale_readiness_harness.rs` a hardcoded list of families it EXPECTS to find. Both state
    an intention. Scoping the emission scan to renderers is what keeps a name in either list from
    standing in for a series nothing writes.
    """
    bodies = {}
    for path in RUST_SOURCES:
        try:
            body = strip_test_modules(path.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            continue
        if EXPOSITION_DECLARATION.search(body):
            bodies[path] = body
    return bodies


RENDERERS = _renderer_bodies()


def emitted_metric_names(bodies: dict = None) -> tuple:
    """(family -> renderer files writing a sample for it, number of sample sites).

    This is the check the family loop below needs and did not have. It asked whether the family
    name appeared ANYWHERE in the Rust source -- and a `# HELP` line carries the name, so a family
    whose sample lines were deleted while its declaration stayed still answered yes. Measured on
    #1785: blinding a dashboard PANEL failed the validator correctly, and deleting the EMITTER did
    not fail it at all. The one failure the tool exists to catch -- a series that silently stops
    being produced while a panel keeps asking for it -- was the one it could not see.

    A sample site is the family name on a renderer line once the declaration text on that line is
    removed: the `push(&mut out, "family", ..)` argument, the `out.push_str("family{kind=\"")`
    prefix, the `format!("family{{..}} {}")` template. Comment lines are skipped -- prose naming a
    family is not a series either.
    """
    bodies = RENDERERS if bodies is None else bodies
    found, sites = {}, 0
    for path, body in bodies.items():
        for line in body.splitlines():
            if line.strip().startswith("//"):
                continue
            for match in re.finditer(FAMILY, DECLARATION_ON_LINE.sub(" ", line)):
                found.setdefault(match.group(0), set()).add(str(path))
                sites += 1
    return found, sites


def declaration_pairs(chunks) -> tuple:
    """(families with a `# HELP` line, family -> type from its `# TYPE` line)."""
    helps, kinds = set(), {}
    for chunk in chunks:
        for word, name, kind in EXPOSITION_DECLARATION.findall(chunk):
            if word == "HELP":
                helps.add(name)
            else:
                kinds[name] = kind
    return helps, kinds


METRIC_FAMILIES = {
    "readiness": {
        "dashboard": [
            "temporalstore_production_readiness_ready",
            "temporalstore_production_readiness_blockers",
            "temporalstore_production_readiness_service_blockers",
        ],
        "alerts": [
            "TemporalStoreProductionReadinessBlocked",
            "TemporalStoreProductionReadinessBlockersHigh",
        ],
        "rust": [
            "temporalstore_production_readiness_ready",
            "temporalstore_production_readiness_blockers",
            "temporalstore_production_readiness_service_ready",
            "temporalstore_production_readiness_service_blockers",
        ],
    },
    "raft": {
        "dashboard": [
            "temporalstore_raft_cluster_commit_index",
            "temporalstore_raft_cluster_has_majority",
            "temporalstore_raft_node_lag",
            "temporalstore_raft_node_apply_lag",
            "temporalstore_raft_leader_lease_valid",
        ],
        "alerts": [
            "TemporalStoreRaftMajorityLost",
            "TemporalStoreRaftSlowFollower",
            "TemporalStoreRaftApplyStuck",
        ],
        "rust": [
            "temporalstore_raft_cluster_commit_index",
            "temporalstore_raft_cluster_live_voters",
            "temporalstore_raft_cluster_has_majority",
            "temporalstore_raft_leader_lease_valid",
            "temporalstore_raft_node_commit_index",
            "temporalstore_raft_node_applied_index",
            "temporalstore_raft_node_lag",
            "temporalstore_raft_node_apply_lag",
        ],
    },
    "metaserver_scheduler": {
        "dashboard": [
            "temporalstore_meta_scheduler_queue_depth",
            "temporalstore_meta_scheduler_executions_total",
            "temporalstore_meta_topology_version",
        ],
        "alerts": [
            "TemporalStoreSchedulerBacklogHigh",
            "TemporalStoreSchedulerRetriesHigh",
        ],
        "rust": [
            "temporalstore_meta_requests_total",
            "temporalstore_meta_inventory",
            "temporalstore_meta_topology_version",
            "temporalstore_meta_scheduler_queue_depth",
            "temporalstore_meta_scheduler_executions_total",
        ],
    },
    "proxy_client": {
        "dashboard": [
            "temporalstore_proxy_route_cache_entries",
            "temporalstore_proxy_route_cache_events_total",
            "temporalstore_proxy_backend_events_total",
            "temporalstore_proxy_serving_mode",
            "temporalstore_proxy_drop_percent",
            "temporalstore_proxy_metric_family_parity",
        ],
        "alerts": [
            "TemporalStoreProxyRouteQuarantineHigh",
            "TemporalStoreProxyNotServing",
        ],
        "rust": [
            "temporalstore_proxy_requests_total",
            "temporalstore_proxy_route_cache_entries",
            "temporalstore_proxy_route_cache_events_total",
            "temporalstore_proxy_backend_events_total",
            "temporalstore_proxy_serving_mode",
            "temporalstore_proxy_drop_percent",
            "temporalstore_proxy_metric_family_parity",
        ],
    },
    "storage_cache": {
        "dashboard": [
            "temporalstore_object_manager_objects",
            "temporalstore_object_manager_block_refs",
            "temporalstore_storage_bucket_block_refs",
            "temporalstore_storage_bucket_bytes",
            "temporalstore_cache_operations_total",
            "temporalstore_block_store_operations_total",
            "temporalstore_block_store_slab_bytes",
        ],
        "alerts": [
            "TemporalStoreStorageCacheBlockers",
            "TemporalStoreBlockStoreReadErrors",
            "TemporalStoreCacheMissPressure",
        ],
        "rust": [
            "temporalstore_object_manager_objects",
            "temporalstore_object_manager_block_refs",
            "temporalstore_object_manager_dirty_buckets",
            "temporalstore_storage_bucket_block_refs",
            "temporalstore_storage_bucket_bytes",
            "temporalstore_cache_operations_total",
            "temporalstore_cache_bytes",
            "temporalstore_block_store_operations_total",
            "temporalstore_block_store_slab_bytes",
        ],
    },
    "data_node": {
        "dashboard": [
            "temporalstore_data_node_runtime_queue_depth",
            "temporalstore_data_node_runtime_background_queue_depth",
            "temporalstore_data_node_dirty_objects",
            "temporalstore_data_node_lifecycle_snapshot_events_total",
        ],
        "alerts": [
            "TemporalStoreDataNodeReadinessBlocked",
            "TemporalStoreDataNodeRuntimeQueueHigh",
            "TemporalStoreLifecycleSnapshotFailures",
        ],
        "rust": [
            "temporalstore_data_node_runtime_jobs_total",
            "temporalstore_data_node_runtime_queue_depth",
            "temporalstore_data_node_runtime_background_queue_depth",
            "temporalstore_data_node_dirty_objects",
            "temporalstore_data_node_lifecycle_snapshot_events_total",
        ],
    },
    "ingestion": {
        "dashboard": [
            "temporalstore_ingestion_kafka_lag",
            "temporalstore_ingestion_kafka_max_lag",
            "temporalstore_ingestion_records_total",
            "temporalstore_ingestion_dead_letters",
            "temporalstore_ingestion_flink_checkpoint_state",
            "temporalstore_ingestion_flink_checkpoints",
        ],
        "alerts": [
            "TemporalStoreIngestionDeadLetters",
        ],
        "rust": [
            "temporalstore_ingestion_records_total",
            "temporalstore_ingestion_kafka_lag",
            "temporalstore_ingestion_kafka_max_lag",
            "temporalstore_ingestion_dead_letters",
            "temporalstore_ingestion_flink_checkpoint_state",
            "temporalstore_ingestion_flink_checkpoints",
        ],
    },
    "matrixark_backend": {
        "dashboard": [
            "matrixark_backend_qps",
            "matrixark_backend_commands_total",
            "matrixark_backend_command_latency_ms",
            "matrixark_backend_records_written_total",
            "matrixark_backend_records_read_total",
            "matrixark_backend_ready",
        ],
        "alerts": [
            "MatrixArkBackendNotReady",
            "MatrixArkBackendErrorsHigh",
        ],
        "rust": [
            "matrixark_backend_qps",
            "matrixark_backend_commands_total",
            "matrixark_backend_errors_total",
            "matrixark_backend_timeouts_total",
            "matrixark_backend_command_latency_ms",
            "matrixark_backend_command_latency_ms_bucket",
            "matrixark_backend_records_written_total",
            "matrixark_backend_records_read_total",
            "matrixark_context_records_total",
            "matrixark_backend_audit_buffered_records",
            "matrixark_backend_audit_flush_failures_total",
            "matrixark_backend_ready",
        ],
    },
}


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def dashboard_text() -> str:
    data = json.loads(read(DASHBOARD))
    return json.dumps(data, sort_keys=True)


def rust_metric_text() -> str:
    return "\n".join(read(path) for path in RUST_SOURCES if path.exists())


def metric_names(text: str) -> set[str]:
    return set(re.findall(r"(?:temporalstore|matrixark)_[A-Za-z0-9_]+", text))


# Queried by a panel or an alert and emitted by nothing, so that panel is blank on every
# deployment. Tracked here rather than left inside a mass failure, because a validator that always
# fails is one nobody runs -- which is how this one came to be red and unnoticed in the first place.
# Each entry is either a panel to remove or a metric to add; neither is a decision this script makes.
KNOWN_UNEMITTED: set = set()


DECLARATION = re.compile(r"#\s*(?:HELP|TYPE)\s+((?:temporalstore|matrixark)_[A-Za-z0-9_]+)")

# The engine declared 229 families when this floor was set. Deliberately far below that: it catches
# a scan that has broken, not a release that trimmed a metric.
DECLARED_FAMILY_FLOOR = 150


TYPE_LINE = re.compile(r"#\s*TYPE\s+((?:temporalstore|matrixark)_[A-Za-z0-9_]+)\s+(\w+)")

# Prometheus renders a histogram as three series -- `_bucket`, `_sum`, `_count` -- from ONE
# declaration of the base name. Same for a summary. Comparing raw names would report every correct
# use of a histogram as undeclared, and the natural way to "fix" that failure is to delete the
# panel, which is the opposite of what this file is for.
COMPONENT_SUFFIXES = ("_bucket", "_sum", "_count")


def declared_kinds(text: str) -> dict:
    """family -> declared type, from `# TYPE` lines."""
    return {name: kind for name, kind in TYPE_LINE.findall(text)}


def expand_component_series(declared: set, kinds: dict) -> set:
    """`declared`, plus the component series each histogram or summary actually publishes."""
    expanded = set(declared)
    for name, kind in kinds.items():
        if kind in ("histogram", "summary"):
            expanded.update(name + suffix for suffix in COMPONENT_SUFFIXES)
    return expanded


def expand_emitted_component_series(emitted: set, kinds: dict) -> set:
    """`emitted`, spelled both ways for every histogram or summary that writes any of its series.

    A histogram publishes `_bucket`, `_sum` and `_count`; there is no series under the base name, so
    a renderer that writes the components has emitted the family even though the bare name never
    appears on a sample line. Deliberately conditional: a family whose sample lines are all gone
    gains nothing here, which is the case the emission check has to keep failing.
    """
    expanded = set(emitted)
    for name, kind in kinds.items():
        if kind not in ("histogram", "summary"):
            continue
        if name in emitted or any(name + suffix in emitted for suffix in COMPONENT_SUFFIXES):
            expanded.add(name)
            expanded.update(name + suffix for suffix in COMPONENT_SUFFIXES)
    return expanded


def check_emission_scan_extent(emitted: set, sites: int, renderers: dict) -> list:
    """The emission check is worthless if its scan comes back empty or nearly so.

    A tool that checks zero metrics passes beautifully. Worse here than for the declaration scan: a
    narrowed emission scan fails loudly (everything reads unemitted) while a BROKEN one -- a regex
    that stopped matching, a renderer set that grew to include a file of intentions -- passes
    quietly. Both floors and the renderer list are asserted before anything derived from them is
    believed.
    """
    failures = []
    found = {str(path.relative_to(RUST_SRC_ROOT)).replace("\\", "/") for path in renderers}
    failures.extend("renderer_no_longer_discovered:%s" % name
                    for name in RENDERER_FLOOR if name not in found)
    if sites < SAMPLE_SITE_FLOOR:
        failures.append("emission_scan_narrowed:sites_%d_expected_at_least_%d"
                        % (sites, SAMPLE_SITE_FLOOR))
    if len(emitted) < EMITTED_FAMILY_FLOOR:
        failures.append("emission_scan_narrowed:families_%d_expected_at_least_%d"
                        % (len(emitted), EMITTED_FAMILY_FLOOR))
    return failures


def check_declarations_are_paired(label: str, helps: set, kinds: dict) -> list:
    """Every family states both a `# HELP` and a `# TYPE` line.

    Cheap, and a real defect twice over: three slab families kept a declaration after the series
    behind them was collapsed, and the gateway publishes two cdylib counters with a `# TYPE` line
    and no `# HELP` at all. A half-declared family still scrapes, so nothing downstream notices --
    it just arrives with no description, or with no type, and a counter read as a gauge rates
    wrongly in every panel that touches it.

    The two halves are reported separately on purpose: they fail for opposite reasons and a single
    combined count would let one hide behind the other.
    """
    if not helps and not kinds:
        return ["declaration_pairing_scan_empty:%s" % label]
    return (["help_without_type:%s:%s" % (label, name) for name in sorted(helps - set(kinds))]
            + ["type_without_help:%s:%s" % (label, name) for name in sorted(set(kinds) - helps)])


def python_metric_text() -> str:
    """Every Python module under tools/ that could publish metrics, tests excluded.

    Read whole rather than from a list: the eight ingestion families are declared in
    `matrixark_ingestion_jobs.py`, not in the two modules named "gateway", and a hand-kept list
    would have missed them exactly as my first scan did.
    """
    chunks = []
    for path in sorted(PY_SOURCE_ROOT.glob("*.py")):
        if path.name.startswith(("test_", "run_")):
            continue
        try:
            chunks.append(path.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            continue
    return "\n".join(chunks)


def declared_metric_names(text: str) -> set:
    """Families the engine actually DECLARES, via a `# HELP` or `# TYPE` line.

    Deliberately narrower than `metric_names`, which matches a name anywhere in the source. A metric
    name also appears in prose, in tests, and -- the case that hid two dead alerts --
    in `ops_scale_readiness_harness.rs`, which carries a hardcoded list of families it EXPECTS the
    dashboards and alerts to mention. Presence in that list states an intention, not an emission, so
    "is the name somewhere in the Rust source" answers yes for a metric nothing ever writes.
    """
    return set(DECLARATION.findall(text))


def alert_rules(text: str) -> list:
    """(name, expr) for every alert rule. Scanned line by line rather than with a multiline
    pattern, because the rule this replaces was easier to get subtly wrong than to read."""
    rules, pending = [], None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- alert:"):
            pending = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("expr:") and pending is not None:
            rules.append((pending, stripped.split(":", 1)[1].strip()))
            pending = None
    return rules


def check_alert_expressions_are_emitted(alerts_text: str, declared: set) -> list:
    """Alert rules whose every metric is undeclared, so the rule can never fire.

    A rule on a metric nothing emits is worse than no rule: the panel is blank, the alert is silent,
    and silence reads as health. Two were found this way -- a scale SLO rule on two p99 families
    that only ever appeared in that expectations list, and a replica-replay rule on a counter no
    subsystem publishes.

    Only reported when NONE of a rule's metrics is declared. A rule mixing a live metric with a dead
    one still fires; `check_alert_metric_names` reports those individually.
    """
    rules = alert_rules(alerts_text)
    if not rules:
        return ["alert_rules:none_parsed"]
    failures = []
    for name, expr in rules:
        used = set(metric_names(expr))
        if used and used.isdisjoint(declared):
            failures.append("alert_never_fires:%s:%s" % (name, ",".join(sorted(used))))
    return failures


def check_alert_metric_names(alerts_text: str, declared: set) -> list:
    """Individual metric names an alert uses that nothing declares."""
    used = set(metric_names(alerts_text))
    return ["alert_metric_undeclared:%s" % name for name in sorted(used - declared)]


def check_declaration_extent(declared: set) -> list:
    # Counted on the EXPANDED set, which is what callers pass. The floor stays far below the real
    # figure either way; it catches a scan that has broken, not a release that trimmed metrics.
    """This guard is worthless if the declaration scan comes back empty.

    The checks above pass when `declared` is large AND when it is empty -- in the empty case nothing
    is reported undeclared only because nothing was compared. Assert the scan found a plausible
    number of families before believing anything derived from it.
    """
    if len(declared) < DECLARED_FAMILY_FLOOR:
        return ["declaration_scan_narrowed:found_%d_expected_at_least_%d"
                % (len(declared), DECLARED_FAMILY_FLOOR)]
    return []


def check_families_state_their_emission(families: dict) -> list:
    """A family may not opt out of the emission check with an empty `rust` list.

    This is the hole both dead panels came through. `scale_slo` and `secondary_replication` each
    declared dashboard metrics and an alert rule, and `"rust": []` -- so the validator confirmed the
    panel existed and the rule existed, and never asked whether anything produced the numbers.
    Both panels were blank on every deployment and both alerts were silent, and the validator
    reported the families ready.

    An empty list is not the same as "no requirement". If a family genuinely has no engine-side
    metric, it does not belong in a spec whose purpose is tying panels to emissions.
    """
    return ["family_states_no_emission:%s" % name
            for name, requirements in sorted(families.items())
            if not requirements.get("rust")]


def dashboard_metric_names() -> set:
    """Every engine family named by a panel expression, across all dashboards."""
    used = set()
    for path in DASHBOARDS:
        if path.exists():
            used |= metric_names(read(path))
    return used


def check_dashboard_metrics_are_emitted(declared: set) -> list:
    """Panel expressions naming a family nothing declares.

    The alert side of this was added first and found two rules that could never fire. The dashboard
    side was still using the loose test -- name appears somewhere in the Rust source -- and it hid a
    quieter version of the same defect: `temporalstore_block_store_slab_oldest_age_ms`, one target
    among five on a panel that therefore rendered with four series and no sign the fifth was
    impossible. A blank panel is at least visible. A missing series is not.
    """
    return ["dashboard_metric_undeclared:%s" % name
            for name in sorted(dashboard_metric_names() - declared)]


def check_no_bare_histogram_targets(kinds: dict) -> list:
    """Panel targets that plot a histogram or summary by its base name.

    Prometheus publishes a histogram as `_bucket`, `_sum` and `_count`. There is no series under
    the base name, so a target naming it draws an empty line -- and it passes every check written
    so far, because the base name IS declared. The emission check cannot catch this: the metric
    exists, it just has no series by that name.

    Found on a panel added in the change that introduced this dashboard, which is the honest reason
    the guard is here rather than the rule being obvious.

    Reported when the base name appears as a WHOLE metric token. It used to require the target to
    be exactly the base name and nothing else, which let through every realistic way of writing the
    mistake -- `max(foo{instance=~"$instance"})` draws the same empty line as `foo` and did not
    match. Found while adding a panel to this dashboard: the mutation that should have failed the
    check passed it.

    A token boundary is what keeps the legitimate uses safe, and they are the reason the match was
    narrow in the first place. `histogram_quantile(...)` over `foo_bucket`, and a `foo_sum /
    foo_count` mean, both mention the base name only as a prefix of a longer token, so neither is
    reported.
    """
    shaped = {name for name, kind in kinds.items() if kind in ("histogram", "summary")}
    if not shaped:
        return []
    failures = []
    for path in DASHBOARDS:
        if not path.exists():
            continue
        try:
            panels = json.loads(read(path)).get("panels", [])
        except ValueError:
            continue
        for panel in panels:
            for target in panel.get("targets", []):
                expr = str(target.get("expr", "")).strip()
                for name in shaped:
                    # The base name as a complete token: not followed by another name character,
                    # which is what distinguishes `foo` from `foo_bucket` and `foo_sum`.
                    if re.search(r"(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])" % re.escape(name), expr):
                        failures.append("bare_histogram_target:%s:%s:%s"
                                        % (path.name, panel.get("title"), expr))
                        break
    return failures


def check_dashboard_extent() -> list:
    """Every dashboard listed must exist, or its panels stop being checked silently."""
    return ["dashboard_missing:%s" % path.name for path in DASHBOARDS if not path.exists()]


def check_scan_extent() -> list:
    """Names from the floor that discovery no longer returns.

    Reported as a failure rather than a warning. A guard whose reach shrinks reports success over
    less and less, and the report reads identically either way.
    """
    found = {str(p.relative_to(RUST_SRC_ROOT)).replace("\\", "/") for p in RUST_SOURCES}
    return [name for name in RUST_SOURCE_FLOOR if name not in found]


def main() -> int:
    dash = dashboard_text()
    alerts = read(ALERTS)
    docs = read(DOC) if DOC.exists() else ""
    rust = rust_metric_text()
    dash_names = metric_names(dash)
    alert_names = metric_names(alerts)
    sample_sites, sample_site_count = emitted_metric_names()
    rust_helps, rust_kinds = declaration_pairs(RENDERERS.values())
    emitted = expand_emitted_component_series(set(sample_sites), rust_kinds)
    missing: list[str] = []
    family_reports = {}
    for family, requirements in METRIC_FAMILIES.items():
        family_missing = []
        for name in requirements["dashboard"]:
            if name not in dash_names:
                family_missing.append(f"dashboard:{name}")
        for name in requirements["alerts"]:
            if name not in alerts:
                family_missing.append(f"alert:{name}")
        for name in requirements["rust"]:
            # EMITTED, not "the name occurs somewhere in the crate". See emitted_metric_names.
            if name not in emitted:
                family_missing.append(f"rust:{name}")
        if family not in docs:
            family_missing.append(f"doc_family:{family}")
        if family_missing:
            missing.extend(f"{family}:{item}" for item in family_missing)
        family_reports[family] = {
            "dashboard_metrics": requirements["dashboard"],
            "alert_rules": requirements["alerts"],
            "rust_metrics": requirements["rust"],
            "ready": not family_missing,
            "missing": family_missing,
        }

    # A known gap stays in the report -- it is not hidden -- but does not fail the run. A gap that
    # gets FIXED does fail, so the list cannot go stale the way the one it replaces did.
    unexpected = [m for m in missing if m not in KNOWN_UNEMITTED]
    fixed_but_listed = sorted(KNOWN_UNEMITTED - set(missing))
    if fixed_but_listed:
        print("These are listed as known-unemitted but now resolve: %s. Remove them from "
              "KNOWN_UNEMITTED." % ", ".join(fixed_but_listed), file=sys.stderr)
    engine_and_gateway = rust + "\n" + python_metric_text()
    declared = expand_component_series(declared_metric_names(engine_and_gateway),
                                      declared_kinds(engine_and_gateway))
    python_text = python_metric_text()
    python_helps, python_kinds = declaration_pairs([python_text])
    alert_failures = (check_declaration_extent(declared)
                      + check_emission_scan_extent(emitted, sample_site_count, RENDERERS)
                      + check_declarations_are_paired("engine", rust_helps, rust_kinds)
                      + check_declarations_are_paired("gateway", python_helps, python_kinds)
                      + check_dashboard_extent()
                      + check_dashboard_metrics_are_emitted(declared)
                      + check_no_bare_histogram_targets(declared_kinds(engine_and_gateway))
                      + check_families_state_their_emission(METRIC_FAMILIES)
                      + check_alert_expressions_are_emitted(alerts, declared)
                      + check_alert_metric_names(alerts, declared))
    report = {
        "schema": "temporalstore_grafana_metrics_parity_report_v1",
        "dashboard": str(DASHBOARD.relative_to(ROOT)),
        "alerts": str(ALERTS.relative_to(ROOT)),
        "doc": str(DOC.relative_to(ROOT)),
        "families": family_reports,
        # The denominators. A run that checked nothing reports the same "ready" as a run that
        # checked everything, and only these numbers tell the two apart.
        "counts": {
            "dashboard_metrics_checked":
                sum(len(r["dashboard"]) for r in METRIC_FAMILIES.values()),
            "alert_rules_checked": sum(len(r["alerts"]) for r in METRIC_FAMILIES.values()),
            "emitter_metrics_checked": sum(len(r["rust"]) for r in METRIC_FAMILIES.values()),
            "renderer_files": len(RENDERERS),
            "sample_sites_found": sample_site_count,
            "families_with_a_sample_site": len(sample_sites),
            "engine_families_declaring_help": len(rust_helps),
            "engine_families_declaring_type": len(rust_kinds),
            "gateway_families_declaring_help": len(python_helps),
            "gateway_families_declaring_type": len(python_kinds),
        },
        # Scoped to family parity, which is what it has always meant. The checks below can fail
        # while this is true, and the exit code -- not this flag -- is the gate.
        "grafana_metrics_parity_ready": not missing,
        "missing": missing,
        # Carried so the machine-readable report says everything the exit code does. Reading
        # `grafana_metrics_parity_ready: true` off a run that exited 1 is the sort of half-truth
        # this file exists to stop.
        "checks_failed": alert_failures,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    print("checked %d emitter metrics against %d sample sites in %d renderer modules "
          "(%d families write a sample); %d engine and %d gateway families declare HELP"
          % (report["counts"]["emitter_metrics_checked"], sample_site_count, len(RENDERERS),
             len(sample_sites), len(rust_helps), len(python_helps)), file=sys.stderr)
    for failure in alert_failures:
        print("ALERT CANNOT FIRE: %s" % failure, file=sys.stderr)
    lost = check_scan_extent()
    if lost:
        print("SCAN NARROWED: these files used to be scanned and are no longer discovered: %s"
              % ", ".join(lost), file=sys.stderr)
    return 0 if (not unexpected and not lost and not fixed_but_listed
                 and not alert_failures) else 1


if __name__ == "__main__":
    sys.exit(main())
