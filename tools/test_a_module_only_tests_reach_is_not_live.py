# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A module only the tests reach is not on any live path.

`test_no_module_is_orphaned_quietly` asks whether a module's name appears ANYWHERE. That is a good
question and this is a different one, because two things get past it:

  * A module a test imports and nothing else. It is named, so it is not an orphan, and no request
    ever reaches it.
  * An ISLAND -- a group that imports only each other. Every member names its mates, so every member
    looks used. The existing check lists exactly ONE of the ten `matrixark_mcp_rust_proxy_*`
    modules; the other nine hide behind it.

Both matter for the same reason the orphan check exists: a copy that is wrong and unreachable cannot
fail today, and is exactly what somebody reaches for tomorrow. Of the 41 modules below, six hold
between them 27 top-level functions whose name is also defined in a REACHABLE module with a
different body -- `matrixark_mcp_extraction_normalization` alone has 10.

It is also how a description of "the live path" goes wrong. Twice in one session I named a module
here as the live one: `matrixark_mcp_retrieve_entity_scan` and its two siblings (the live scorer is
`matrixark_local_adapter_retrieve`, at twelve call sites) and `matrixark_mcp_query` (nothing outside
tools/test_* imports it). Both mistakes were caught by running this, not by reading.

HOW IT DECIDES, and why the controls below are not optional
-----------------------------------------------------------
Build the import graph over `tools/*.py`, seed it, and propagate. Seeds are modules with a
`__main__` guard or a top-level `sys.argv`, plus any module named by a tracked NON-Python file -- a
launcher, a workflow, a document. Tests are deliberately NOT seeds; that is the entire difference
from the check next door.

Edges include names appearing in STRING literals, because
`importlib.import_module("tools.matrixark_mcp_retrieve_entity_scan")` is a real edge and reading
only `import` statements reports the target as unreachable when it is not.

A reachability scan that seeds badly reports live code as dead, which is worse than reporting
nothing: it invites deleting something that runs. So `test_the_live_roots_come_back_reachable`
asserts fourteen modules nobody would argue about. If ANY of them is unreachable, the seeding is
wrong and every name in the list below is unsafe to act on.

What that control does and does not catch, measured rather than assumed: the two seed sources are
REDUNDANT for those fourteen -- the hooks and the server carry `__main__` guards AND are named in
docs -- so removing either one alone leaves them reachable and the control stays quiet. Removing
both fails it. A single broken source is caught instead by the exact-set assertion, which reports
every module the break newly stranded. Both mutations were run; neither passes silently.
"""
from __future__ import annotations

import ast
import collections
import os
import re
import subprocess
import unittest
from typing import Dict, List, Set, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SELF = os.path.join("tools", os.path.basename(__file__))
_WORD = re.compile(r"[A-Za-z0-9_]+")

#: Modules that unarguably run. If the seeding breaks, these stop being reachable and the list at
#: the bottom stops meaning anything -- so this is checked FIRST.
LIVE_ROOTS = (
    "matrixark_mcp_server",
    "matrixark_codex_hook",
    "matrixark_agent_hook",
    "matrixark_mcp_local_adapter",
    "matrixark_local_adapter_retrieve",
    "matrixark_local_adapter_ingest",
    "matrixark_mcp_core",
    "matrixark_mcp_indexing",
    "matrixark_mcp_scoring",
    "matrixark_mcp_serving_records",
    "matrixark_mcp_storage_options",
    "matrixark_mcp_schemas",
    "matrixark_mcp_errors",
    "matrixark_mcp_identity",
)

#: Reached from no production entry point, grouped as they are connected. The grouping is the
#: finding: these are not 41 separate decisions, they are five units and seven singles.
#:
#: This list is NOT an endorsement, and it is asserted exactly -- a new entry fails here rather than
#: accumulating, and one that becomes reachable fails too, because a list allowed to go stale
#: describes a tree that no longer exists.
UNREACHABLE = {
    # An ingest pipeline, ~2,675 lines. Almost every definition is unique to the island -- abandoned
    # rather than duplicated -- which makes it the safest of the three large ones to remove and the
    # least urgent to worry about.
    "ingest": (
        "matrixark_mcp_async_ingest",
        "matrixark_mcp_ingest_message_records",
        "matrixark_mcp_ingest_resource_chunks",
        "matrixark_mcp_ingest_resource_facts",
        "matrixark_mcp_ingest_resource_queue",
        "matrixark_mcp_ingest_resource_runtime",
        "matrixark_mcp_ingest_resource_summary",
        "matrixark_mcp_ingest_response",
        "matrixark_mcp_ingest_setup",
        "matrixark_mcp_local_ingest",
        "matrixark_mcp_resource_import_task",
    ),
    # The proxy caching and coalescing layer, ~2,244 lines, unwired. The orphan check next door
    # lists `matrixark_mcp_rust_proxy_client` alone; these are the nine it cannot see behind it.
    "rust_proxy": (
        "matrixark_mcp_rust_proxy_cache",
        "matrixark_mcp_rust_proxy_cache_mixin",
        "matrixark_mcp_rust_proxy_client",
        "matrixark_mcp_rust_proxy_coalesce",
        "matrixark_mcp_rust_proxy_config",
        "matrixark_mcp_rust_proxy_lane_select",
        "matrixark_mcp_rust_proxy_lanes",
        "matrixark_mcp_rust_proxy_metrics_record",
        "matrixark_mcp_rust_proxy_metrics_snapshot",
        "matrixark_mcp_rust_proxy_metrics_state",
    ),
    # The one to be careful with, ~2,875 lines. Six of these seven hold DIVERGED copies of names
    # that a reachable module also defines -- 27 functions in total, 10 of them in
    # matrixark_mcp_extraction_normalization. Reading any of them as current would be wrong, and
    # nothing in the file says which way it diverged.
    "extraction": (
        "matrixark_mcp_entity_ops",
        "matrixark_mcp_extraction_normalization",
        "matrixark_mcp_extraction_runtime",
        "matrixark_mcp_oss_understanding",
        "matrixark_mcp_resources",
        "matrixark_mcp_segments",
        "matrixark_mcp_summaries",
    ),
    # The retrieval scans, ~789 lines. Every one is imported by a test and by nothing else. These
    # are the modules I twice called "the retrieval path"; the live scorer is
    # matrixark_local_adapter_retrieve.
    "retrieve_scans": (
        "matrixark_mcp_retrieve_candidate_builders",
        "matrixark_mcp_retrieve_compression_scan",
        "matrixark_mcp_retrieve_entity_scan",
        "matrixark_mcp_retrieve_summary_scan",
    ),
    "direct_cache": (
        "matrixark_mcp_direct_cache",
        "matrixark_mcp_direct_cache_state",
    ),
    # Singles, ~4,062 lines between them.
    "singles": (
        "matrixark_mcp_deadline_pack",
        "matrixark_mcp_local_batch_extract_runtime",
        "matrixark_mcp_query",
        "matrixark_mcp_retrieve_index_terms",
        "matrixark_mcp_retrieve_metrics",
        "matrixark_mcp_session_runtime",
        "oss_model_contract",
    ),
}

#: 290 when this was written. A floor, so a scan that stops seeing the tree fails here instead of
#: reporting an empty set and passing.
EXPECTED_LIBRARY_FLOOR = 220


def _tracked(*patterns: str) -> List[str]:
    listed = subprocess.run(["git", "ls-files"] + list(patterns), cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if path != _SELF]


def _parse() -> Dict[str, Tuple[str, ast.Module]]:
    found = {}
    for path in _tracked("tools/*.py"):
        stem = os.path.basename(path)[:-len(".py")]
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                found[stem] = (path, ast.parse(handle.read()))
        except (OSError, SyntaxError):
            continue
    return found


def _edges(modules) -> Dict[str, Set[str]]:
    graph: Dict[str, Set[str]] = collections.defaultdict(set)
    for stem, (_path, tree) in modules.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                target = node.module.rsplit(".", 1)[-1]
                if target in modules:
                    graph[stem].add(target)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    target = alias.name.rsplit(".", 1)[-1]
                    if target in modules:
                        graph[stem].add(target)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                # importlib.import_module("tools.x") is an edge. Reading only import statements
                # reports its target unreachable when a test or a launcher does reach it.
                for word in _WORD.findall(node.value):
                    if word in modules:
                        graph[stem].add(word)
    return graph


def _is_entry_point(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and isinstance(node.test, ast.Compare):
            left = node.test.left
            if isinstance(left, ast.Name) and left.id == "__name__":
                return True
    for node in tree.body:
        for inner in ast.walk(node):
            if isinstance(inner, ast.Attribute) and inner.attr == "argv":
                return True
    return False


def _named_by_a_non_python_file(library: Set[str]) -> Set[str]:
    words: Set[str] = set()
    for path in _tracked(":!*.py"):
        full = os.path.join(REPO, path)
        try:
            if os.path.getsize(full) > 2_000_000:
                continue
            with open(full, encoding="utf-8", errors="replace") as handle:
                words |= set(_WORD.findall(handle.read()))
        except OSError:
            continue
    return library & words


_CACHE: List[Tuple[Set[str], Set[str]]] = []


def reachable_from_production() -> Tuple[Set[str], Set[str]]:
    """(library modules, those reachable from a production seed).

    Computed once. The work is parsing every module and reading every tracked non-Python file, and
    the four assertions below all need the same answer -- recomputing it per test took this from
    thirteen seconds to fifty-two, and a check nobody waits for is a check nobody runs.
    """
    if _CACHE:
        return _CACHE[0]
    modules = _parse()
    library = {stem for stem in modules if not stem.startswith("test_")}
    graph = _edges(modules)
    seeds = ({stem for stem in library if _is_entry_point(modules[stem][1])}
             | _named_by_a_non_python_file(library))
    reached: Set[str] = set()
    queue = list(seeds)
    while queue:
        stem = queue.pop()
        if stem in reached:
            continue
        reached.add(stem)
        queue.extend((graph[stem] & library) - reached)
    _CACHE.append((library, reached))
    return library, reached


class AModuleOnlyTestsReachIsNotLiveTest(unittest.TestCase):

    def test_the_scan_sees_the_tree(self) -> None:
        library, _ = reachable_from_production()
        self.assertGreaterEqual(
            len(library), EXPECTED_LIBRARY_FLOOR,
            "found %d library modules under tools/, expected at least %d -- the listing changed, "
            "so everything below runs on the wrong set"
            % (len(library), EXPECTED_LIBRARY_FLOOR))

    def test_the_live_roots_come_back_reachable(self) -> None:
        """Checked before the list, because a bad seeding reports live code as dead."""
        library, reached = reachable_from_production()
        missing = [name for name in LIVE_ROOTS if name in library and name not in reached]
        self.assertEqual(
            [], missing,
            "%s cannot be reached from any seed. These unarguably run, so the SEEDING is wrong, "
            "not the tree -- do not act on the list of unreachable modules until this passes"
            % ", ".join(missing))

    def test_an_island_is_reported_whole(self) -> None:
        """The mechanism control, and the reason this file exists next to the orphan check.

        Every member of the proxy island names its mates, so a check that asks "is this name
        mentioned anywhere" sees all ten as used and lists one. If this scan ever reports fewer
        than all ten, it has reverted to asking about mentions.
        """
        _library, reached = reachable_from_production()
        island = UNREACHABLE["rust_proxy"]
        still_reachable = sorted(name for name in island if name in reached)
        self.assertEqual(
            [], still_reachable,
            "%s became reachable -- if the layer was wired up, move it out of the list; if this is "
            "the scan crediting an island member for being named by its own island, the scan is "
            "broken" % ", ".join(still_reachable))

    def test_the_set_is_exactly_what_is_recorded(self) -> None:
        library, reached = reachable_from_production()
        recorded = {name for group in UNREACHABLE.values() for name in group}
        found = library - reached
        new = sorted(found - recorded)
        gone = sorted(recorded - found)
        self.assertEqual(
            ([], []), (new, gone),
            "unreachable modules changed.\n"
            "  NEW (nothing production reaches these, and they are not recorded): %s\n"
            "  NO LONGER UNREACHABLE (wired up or deleted -- take them out of the list): %s"
            % (new, gone))


if __name__ == "__main__":
    unittest.main()
