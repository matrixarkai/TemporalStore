#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`reachable_from_production` answers a wider question than its name, and this is how much wider.

`test_a_module_only_tests_reach_is_not_live.reachable_from_production()` seeds from every library
module that has a `__main__` guard or touches `sys.argv`, plus every module named by any tracked
non-Python file, and closes over imports. That is a sound answer to "what can be reached by
running something in this repository". It is not an answer to "what does the serving path reach",
and four guards plus every drift sweep read it as if it were.

MEASURED on the tree as this file was written:

    library modules                                        291
    seeds                                                  145
      with a __main__ guard or a sys.argv touch            127
      named by a tracked non-Python file                    93
      named by a doc and nothing else                       18
    closure from every seed                                248   (85% of the library)
    closure from the declared serving roots (LIVE_ROOTS)   101

So 147 modules are "reachable from production" only because a benchmark, a conformance validator
or a report generator can be executed. Downstream, that inflates what a drift sweep calls live:
the same-name/same-parameter sweep over top-level functions finds 106 diverged pairs where two
reachable modules disagree, and 61 of them survive if only the serving roots seed the closure. A
quarter of the "substantive" pairs -- 45 against 31 -- are two harness scripts, not two serving
paths.

WHY THIS FILE DOES NOT TIGHTEN THE SCAN. Three reasons, each measured rather than argued.

1. THE SCAN IS NOT WRONG, THE WORD IS. A benchmark harness genuinely is reachable, and it is run.
   Narrowing the seeds would move 109 modules from "reachable" to "unreachable" in a guard whose
   UNREACHABLE list is asserted EXACTLY, and the new entries would be scripts that people and CI
   execute. A list that calls a script somebody ran this morning unreachable is worse than a list
   that is too generous.

2. IT FALLS UNDER A VACUITY FLOOR THAT EXISTS FOR GOOD REASON. `test_the_flag_surface_only_shrinks`
   refuses to believe a reachability answer that covers less than half the library -- "below half,
   believe the scan is broken before believing the tree changed shape". Half of 291 is 145. The
   closure from the declared serving roots is 101 and the closure with harness seeds removed is
   139. Both are under it. Tightening would trip a guard whose message says the scan broke, on a
   day when nothing broke. Worse, that same file classifies a flag as retirable when every module
   that reads it is unreachable: tightening would mark flags retirable because their only readers
   are harnesses that do run, which is exactly the false retirement its shipped-SDK carve-out
   exists to prevent.

3. THERE IS NO CLEAN SIGNAL TO TIGHTEN WITH. Two classifiers were tried and they disagree about 66
   of the 145 seeds:

     * by SPELLING (`run_`, `validate_`, `build_`, ...): 99 seeds. It is a heuristic on names, and
       it calls `matrixark_v1_gateway` and `matrixark_mcp_cli` production because of how they are
       spelled.
     * by STRUCTURE (a seed nothing in the library imports, so it can only be run): 73 seeds. It
       calls `matrixark_mcp_cli`, `matrixark_rust_proxy_daemon` and `matrixark_object_store_server`
       harnesses -- they have no importers precisely because they ARE operational entry points --
       and it calls `run_external_baseline_direct_retrieval` production, because another benchmark
       imports it.

   The harness scripts import each other heavily, so "nothing imports it" does not separate the
   two kinds. The only trustworthy narrow set in the tree is `LIVE_ROOTS`, fourteen names
   maintained by hand.

WHAT THIS FILE IS FOR, then: the number. 248 is not a count of serving modules and nothing should
read it as one. The three closures are recorded here in bands, so a change to the seeding shows up
as a failure here with the arithmetic attached, and the next sweep that wants the narrow question
can ask `LIVE_ROOTS` for it instead of discovering this the slow way.

Bands rather than exact integers, deliberately. These are measurements of a tree that grows every
week; an exact assertion would fail on every unrelated module added and would be re-baselined
without being read, which is how a recorded number stops meaning anything. The bands are narrow
enough that redefining the seeding fails them and wide enough that ordinary growth does not. What
pins the RULE is not a band but `test_the_seeding_rule_is_still_the_one_this_file_describes`,
which re-derives the documented rule and compares it against what the function actually returns.

A NOTE ON WHAT A SCAN KEYED ON SAMENESS CANNOT ANSWER, because this file is an argument about
exactly that and it is now a pattern rather than an incident. Two guards in this tree key on
content being identical and go blind at the moment it stops being:

  * `test_a_nested_helper_has_one_copy_too` groups functions by the exact unparsed text of their
    bodies, so it reports duplicated helpers and cannot report DRIFTED ones. The pair recorded in
    `test_the_scope_recovery_helper_has_one_copy` sits inside the two functions that guard already
    names, three lines from a helper it does report, and is invisible to it.
  * `test_a_definition_nothing_reaches` seeds a name as reached when any other tracked file names
    it as an identifier, so it cannot see an orphan whose name is also DEFINED elsewhere -- the
    other module's own `def` line does the seeding.

Both go blind at the moment the thing they watch starts to differ, which is the moment it starts
to matter. Neither is wrong for its own purpose; what is wrong is reading either as an answer to
"has this drifted".

THREE MISTAKES MADE WHILE WRITING THIS FILE, kept because they are one lesson: the control did
something other than what it read as doing.

  * It RE-DERIVED the seeding instead of reading the function's answer. Every band passed while
    `reachable_from_production` was mutated to seed from `LIVE_ROOTS` only -- the file was
    measuring its own copy of the rule and calling that a record of the rule. Three of six
    mutations went undetected until the wide closure came from the function itself.
  * It INTERSECTED `LIVE_ROOTS` with the library before asking whether every root is reachable.
    That silently drops a declared root the scan cannot see and then reports that all the
    remaining ones are fine -- an exclusion that makes the control unable to fail.
  * The mutation evidence itself was produced twice and nearly reported from the wrong run. The
    harness was launched inside a shell fallback chain, and the first branch failed on a broken
    pipe filter rather than on the harness, so the fallback ran the whole mutating harness A
    SECOND TIME against the same tree. A fallback chain around a side-effecting command re-runs
    the side effect whenever the first branch fails FOR ANY REASON, including one that has nothing
    to do with the command. The numbers above come from a single run over a tree nothing else was
    touching.
"""
from __future__ import annotations

import collections
import importlib
import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

#: Measured when this file was written. Recorded as (low, high) bands -- see the docstring.
LIBRARY_BAND = (250, 400)
SEEDS_BAND = (110, 220)
WIDE_CLOSURE_BAND = (200, 330)
SERVING_CLOSURE_BAND = (70, 140)

#: The wide answer must stay materially wider than the narrow one, or this record is describing a
#: tree that no longer has the problem.
MINIMUM_SPREAD = 90

#: LIVE_ROOTS is fourteen names maintained by hand. If it grows to swallow the harness scripts the
#: spread above closes by redefinition rather than by measurement, so the count is recorded too.
SERVING_ROOT_COUNT_BAND = (10, 20)

#: The spelling heuristic, kept only to show that it disagrees with the structural one. It is NOT
#: used for any load-bearing assertion; the refusal below rests on LIVE_ROOTS.
_HARNESS_SPELLING = re.compile(
    r"^(run_|validate_|verify_|generate_|summarize_|compare_|convert_|analyze_|"
    r"fetch_|download_|check_|diagnose_|inspect_|probe_|measure_|archive_|build_|"
    r"resolve_temporalstore|rebalance_|redis_scale|fake_s3|openai_compatible_|"
    r"context_minilm_embed_server)")

#: Seeds the STRUCTURAL classifier calls a harness and that are operational entry points. Each has
#: no importer because running it IS the point.
MISREAD_BY_STRUCTURE = ("matrixark_mcp_cli", "matrixark_rust_proxy_daemon",
                        "matrixark_object_store_server")

#: Seeds the SPELLING classifier calls a harness and the structural one keeps, because another
#: harness imports them.
MISREAD_BY_SPELLING = ("run_external_baseline_direct_retrieval",
                       "validate_storage_engine_9_phase_conformance")


def _guard():
    """Imported HERE rather than at module level.

    Under `unittest discover` a test module is reachable as both `tools.X` and bare `X`, so
    importing one test module from another at import time pulls a second copy into the run and
    shifts what every later module sees -- see `test_matrixark_no_cross_test_imports`.
    """
    try:
        return importlib.import_module("tools.test_a_module_only_tests_reach_is_not_live")
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module("test_a_module_only_tests_reach_is_not_live")


def _world():
    """(library, graph, seeds, wide closure, declared roots).

    The wide closure comes from `reachable_from_production()` ITSELF, not from a second
    implementation of its seeding here. That distinction is load-bearing and was got wrong first:
    a version of this file that re-derived the seeds passed unchanged while the real function was
    mutated to seed from LIVE_ROOTS only -- it was measuring its own copy of the rule and calling
    that a record of the rule.

    The declared roots are returned RAW, not intersected with the library. Intersecting silently
    drops a root the scan cannot see, which is the one condition the control below exists to
    catch.
    """
    guard = _guard()
    modules = guard._parse()
    library = {stem for stem in modules if not stem.startswith("test_")}
    graph = guard._edges(modules)
    seeds = ({stem for stem in library if guard._is_entry_point(modules[stem][1])}
             | guard._named_by_a_non_python_file(library))
    _library, wide = guard.reachable_from_production()
    return library, graph, seeds, wide, tuple(guard.LIVE_ROOTS)


def _close(graph, library, seed_set):
    reached, queue = set(), list(seed_set)
    while queue:
        stem = queue.pop()
        if stem in reached:
            continue
        reached.add(stem)
        queue.extend((graph[stem] & library) - reached)
    return reached


def _leaf_seeds(graph, library, seeds):
    """Seeds nothing in the library imports -- the structural reading of "can only be run"."""
    importers = collections.defaultdict(set)
    for stem, targets in graph.items():
        if stem.startswith("test_"):
            continue
        for target in targets & library:
            if target != stem:
                importers[target].add(stem)
    return {stem for stem in seeds if not importers[stem]}


class TheProductionSeedSetIsWiderThanServing(unittest.TestCase):

    def test_the_scan_is_there_to_measure(self) -> None:
        """A floor. Every number below is a fraction of these two."""
        library, _graph, seeds, _wide, roots = _world()
        low, high = LIBRARY_BAND
        self.assertTrue(
            low <= len(library) <= high,
            "the library is %d modules, recorded between %d and %d. If the tree really changed "
            "that much, re-measure every band in this file rather than moving one"
            % (len(library), low, high))
        low, high = SEEDS_BAND
        self.assertTrue(
            low <= len(seeds) <= high,
            "the scan now seeds from %d modules, recorded between %d and %d -- the seeding rule "
            "has changed, which is what this file is here to notice" % (len(seeds), low, high))
        low, high = SERVING_ROOT_COUNT_BAND
        self.assertTrue(
            low <= len(roots) <= high,
            "LIVE_ROOTS now names %d modules, recorded between %d and %d. If the declared serving "
            "surface really grew, the spread this file records has to be re-measured, not widened"
            % (len(roots), low, high))

    def test_the_seeding_rule_is_still_the_one_this_file_describes(self) -> None:
        """Every number here is an argument about a specific rule. This pins the rule.

        The docstring says the scan seeds from `__main__`/`sys.argv` modules UNION modules named
        by a tracked non-Python file. That is re-derived here and compared against what
        `reachable_from_production()` actually returns. The comparison is the point: a version of
        this file that only re-derived the rule and never compared passed unchanged while the real
        function was mutated to seed from `LIVE_ROOTS` only.

        If the seeding legitimately changes, this fails -- and it should, because the three
        reasons this file gives for leaving it alone are all measured against the rule as it is.
        """
        library, graph, seeds, wide, _roots = _world()
        rederived = _close(graph, library, seeds)
        self.assertEqual(
            rederived, wide,
            "reachable_from_production reaches %d modules and the rule this file documents "
            "reaches %d. The seeding has changed; re-read the three reasons above before "
            "trusting any number in this file. Only in the scan: %s. Only in the rule: %s"
            % (len(wide), len(rederived),
               ", ".join(sorted(wide - rederived)[:6]) or "none",
               ", ".join(sorted(rederived - wide)[:6]) or "none"))

    def test_the_wide_answer_is_much_wider_than_the_serving_one(self) -> None:
        """The measurement this file exists for."""
        library, graph, _seeds, wide, roots = _world()
        serving = _close(graph, library, set(roots) & library)

        low, high = WIDE_CLOSURE_BAND
        self.assertTrue(
            low <= len(wide) <= high,
            "reachable_from_production now reaches %d modules, recorded between %d and %d"
            % (len(wide), low, high))
        low, high = SERVING_CLOSURE_BAND
        self.assertTrue(
            low <= len(serving) <= high,
            "the declared serving roots now reach %d modules, recorded between %d and %d"
            % (len(serving), low, high))
        self.assertGreaterEqual(
            len(wide) - len(serving), MINIMUM_SPREAD,
            "the two answers are now within %d modules of each other. If the harness scripts were "
            "separated out, this record is stale and the sweeps that read the wide number can be "
            "pointed at it safely" % (len(wide) - len(serving)))

    def test_every_serving_root_is_inside_the_wide_answer(self) -> None:
        """The control. The narrow set must be a subset, or one of the two scans is broken.

        Asked of the RAW LIVE_ROOTS, not of LIVE_ROOTS intersected with the library. An
        intersection would drop a root the scan cannot see and then report that every remaining
        root is reachable -- an exclusion that makes the control unable to fail.
        """
        library, _graph, _seeds, wide, roots = _world()
        unseen = sorted(stem for stem in roots if stem not in library)
        self.assertEqual(
            [], unseen,
            "%s are declared serving roots the module scan never saw. A root outside the corpus "
            "is UNKNOWN, not reachable, and every number in this file is measured against a "
            "corpus that does not contain it" % ", ".join(unseen))
        missing = sorted(stem for stem in roots if stem not in wide)
        self.assertEqual(
            [], missing,
            "%s are declared serving roots that the wide scan does not reach. The seeding is "
            "wrong, not the tree -- nothing in this file is trustworthy until that passes"
            % ", ".join(missing))

    def test_tightening_would_fall_under_the_vacuity_floor(self) -> None:
        """The refusal, asserted rather than argued.

        `test_the_flag_surface_only_shrinks` disbelieves a reachability answer covering less than
        half the library. Both candidate narrowings are under that line, so tightening this scan
        would trip a guard whose message says the scan broke, on a day nothing broke. The
        arithmetic is recomputed here rather than restated from that file, so this stays true if
        the tree grows.
        """
        library, graph, seeds, _wide, roots = _world()
        floor = len(library) // 2
        serving = _close(graph, library, set(roots) & library)
        by_spelling = _close(graph, library,
                             {s for s in seeds if not _HARNESS_SPELLING.match(s)})

        self.assertLess(
            len(serving), floor,
            "the serving closure is %d of %d modules, at or above the half-library floor of %d. "
            "Tightening the seeds would no longer trip that floor -- re-open the question"
            % (len(serving), len(library), floor))
        self.assertLess(
            len(by_spelling), floor,
            "dropping the harness seeds now reaches %d of %d, at or above the floor of %d. "
            "Re-open the question" % (len(by_spelling), len(library), floor))

    def test_no_available_signal_separates_a_harness_from_an_entry_point(self) -> None:
        """The third reason, pinned with examples so it cannot decay into folklore."""
        library, graph, seeds, _wide, _roots = _world()
        leaf = _leaf_seeds(graph, library, seeds)
        spelled = {s for s in seeds if _HARNESS_SPELLING.match(s)}

        self.assertGreater(
            len(leaf ^ spelled), len(seeds) // 4,
            "the two classifiers now agree about all but %d of %d seeds. If one of them became "
            "trustworthy, tightening is back on the table" % (len(leaf ^ spelled), len(seeds)))

        for stem in MISREAD_BY_STRUCTURE:
            with self.subTest(module=stem, classifier="structure"):
                self.assertIn(
                    stem, seeds, "%s is no longer a seed, so it cannot illustrate anything" % stem)
                self.assertIn(
                    stem, leaf,
                    "%s is no longer a leaf. It was the example of an operational entry point "
                    "that the structural classifier calls a harness because nothing imports it"
                    % stem)

        for stem in MISREAD_BY_SPELLING:
            with self.subTest(module=stem, classifier="spelling"):
                self.assertIn(stem, seeds, "%s is no longer a seed" % stem)
                self.assertIn(
                    stem, spelled, "%s no longer matches the spelling heuristic" % stem)
                self.assertNotIn(
                    stem, leaf,
                    "%s is now a leaf, so the two classifiers agree about it and it no longer "
                    "shows them disagreeing" % stem)


if __name__ == "__main__":
    unittest.main()
