# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Index terms no query can ask for are not written, and the two halves cannot drift apart.

Retrieval narrows using groups from `infer_secondary_index_filter_groups`, and
`passes_secondary_index_filters` only ever INTERSECTS a candidate's terms with those groups. The
inference emits a fixed set of KINDS, so a term whose kind is outside that set cannot appear in a
group, cannot intersect one, and cannot narrow a search or earn the hint boost -- whatever its
value.

On a 1 MB skill the unreachable terms were a large share of a 1,471 KB index, written and scanned
to affect nothing. The first cut of this filter claimed 1,418 KB by declaring only 14 kinds, but
seven more were emitted with computed values and had to be given back -- `heading_slug` alone is
990.7 KB. Measure the saving from the declared set, never from the first estimate.

The danger is drift. The declared set and the inference are only correct TOGETHER: a kind added to
the inference but not declared is filtered out at ingest, and the query needing it narrows to
nothing with nothing to notice. The first test reads the kinds straight out of the inference source
so that can only happen loudly.

That guard failed once, in the direction it exists to prevent. It scanned a fixed 12,000-character
slice of a 21,839-character function, found 14 of the 21 kinds, and reported full coverage while
seven were being dropped. It now takes the function's whole extent and asserts that extent -- a
scan that silently stops early is the failure mode, so the length is checked, not assumed.
"""
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from matrixark_mcp_core_query_analysis import (
    INFERABLE_SECONDARY_INDEX_KINDS,
    index_term_is_consultable,
)
from matrixark_mcp_core_scoring import passes_secondary_index_filters


class WhatAQueryCanAskFor(unittest.TestCase):
    def test_the_declared_set_covers_every_kind_the_inference_emits(self):
        """The drift guard, and the reason this file exists.

        Read from the inference SOURCE rather than a hand-kept list: a kind added there and not
        here would be dropped at ingest and the query that needed it would silently match nothing.
        This caught `source_role` missing the first time it ran.
        """
        import matrixark_mcp_core_query_analysis as qa
        with open(qa.__file__, encoding="utf-8") as handle:
            source = handle.read()
        start = source.index("def deterministic_secondary_index_filter_groups")
        marker = chr(10) + "def "
        end = source.find(marker, start + 4)
        body = source[start:end if end != -1 else len(source)]
        # The whole function, not a fixed slice. An earlier version read 12,000 characters of a
        # 21,839-character function, saw 14 of its 21 kinds, and reported full coverage while
        # seven were being dropped at ingest.
        self.assertGreater(len(body), 12000,
                           "the function is shorter than expected; check the extent, not the slice")
        # Both call shapes: a literal value and a computed one. Matching only literals is how the
        # six computed emissions were missed.
        emitted = set(re.findall(r'context_index_name\(\s*"([a-z_]+)"', body))
        self.assertTrue(emitted, "found no emitted kinds -- the parse is wrong, not the code")
        self.assertGreaterEqual(
            len(emitted), 21,
            "expected at least the 21 kinds this function emitted when the guard was written; "
            "fewer means the parse stopped early again")
        missing = emitted - set(INFERABLE_SECONDARY_INDEX_KINDS)
        self.assertEqual(
            set(), missing,
            "the inference can emit %s but ingest would filter them out, so a query using them "
            "would narrow to nothing" % sorted(missing))

    def test_a_consultable_kind_is_kept(self):
        for kind in sorted(INFERABLE_SECONDARY_INDEX_KINDS):
            self.assertTrue(index_term_is_consultable("%s:whatever" % kind))

    def test_the_seven_kinds_the_first_guard_missed_are_consultable(self):
        """A named regression guard for the kinds a truncated scan let through.

        Each of these is emitted by the inference with a COMPUTED value -- `context_index_name`
        called on a variable rather than a literal -- and each sat past the 12,000-character
        window the first guard read. A query inferring any of them narrowed to nothing.
        """
        for kind in ("heading_slug", "memory_selection_quality", "relative_path",
                     "resource_type", "skill_tool", "skill_trigger", "unit_kind"):
            self.assertIn(kind, INFERABLE_SECONDARY_INDEX_KINDS)
            self.assertTrue(index_term_is_consultable("%s:whatever" % kind),
                            "%s is emitted by the inference; filtering it at ingest makes a "
                            "query that asks for it match nothing" % kind)

    def test_terms_no_query_can_reach_are_still_dropped(self):
        # What remains genuinely unreachable: no inference path emits either kind, so neither can
        # appear in a group, intersect one, or earn the hint boost -- whatever its value.
        for term in ("keyword:checkout", "skill_name:acme"):
            self.assertFalse(index_term_is_consultable(term),
                             "%s is filtered at ingest; if a query can now ask for it, the "
                             "declared set must say so" % term)

    def test_a_term_with_no_kind_is_not_consultable(self):
        for term in ("", "novalue", ":", "  "):
            self.assertFalse(index_term_is_consultable(term))

    def test_dropping_them_cannot_change_narrowing(self):
        """The claim, exercised rather than argued.

        Narrowing intersects a candidate's terms with the inferred groups. Adding or removing
        terms whose kind is not in any group must not move the outcome either way.
        """
        groups = [{"entity_type:location"}, {"source_type:message"}]
        kept = {"entity_type:location", "source_type:message"}
        dropped = {"keyword:checkout", "skill_name:acme"}
        self.assertEqual(
            passes_secondary_index_filters(kept, groups),
            passes_secondary_index_filters(kept | dropped, groups),
            "un-consultable terms changed the narrowing outcome")
        # and the negative case, so this is not passing because everything passes
        self.assertFalse(passes_secondary_index_filters(dropped, groups))
        self.assertTrue(passes_secondary_index_filters(kept, groups))


MODULE = "matrixark_mcp_ingest_resource_chunk_records"
VARIABLE = "MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS"


class TheFilterIsOnAndActuallyFilters(unittest.TestCase):
    """Reading the flag's default means re-importing, which is only safe if it is undone.

    These tests drop the module from `sys.modules` so its import-time flag is evaluated again.
    Leaving the replacement behind hands every later importer a different module object than the
    one its collaborators already hold, and they fail for reasons unrelated to themselves. Both
    the module table and the environment are put back.
    """

    def setUp(self):
        self._module = sys.modules.get(MODULE)
        self._variable = os.environ.get(VARIABLE)

    def tearDown(self):
        sys.modules.pop(MODULE, None)
        if self._module is not None:
            sys.modules[MODULE] = self._module
        os.environ.pop(VARIABLE, None)
        if self._variable is not None:
            os.environ[VARIABLE] = self._variable

    def _reimport(self):
        import importlib
        sys.modules.pop(MODULE, None)
        return importlib.import_module(MODULE)

    def test_the_default_is_on(self):
        os.environ.pop(VARIABLE, None)
        self.assertTrue(self._reimport().INDEX_ONLY_CONSULTABLE_TERMS)

    def test_the_escape_hatch_works(self):
        os.environ[VARIABLE] = "0"
        self.assertFalse(self._reimport().INDEX_ONLY_CONSULTABLE_TERMS)


class WhatTheIndexTermCapEverSees(unittest.TestCase):
    """`limited_index_terms` ranks by kind, and only resource-chunk kinds ever reach it.

    The priority tuple has twenty-three entries. Its only consumer is `limited_index_terms`, whose
    only two callers are resource-chunk ingest paths building one closed list of nine kinds, so
    fourteen entries -- `benchmark:`, `metric:` and `workload:` among them -- cannot appear in a
    list handed to the ranking. Those three come from `benchmark_quality_index_terms`, which feeds
    `candidate_index_terms` on the READ side. A comment here used to say they "were the first
    dropped by `limited_index_terms`"; they never reach it to be dropped.

    Leaving unreachable entries in the tuple is harmless. The silent failure is the opposite: a
    kind the callers DO produce that is missing from the tuple ranks last and is the first thing
    the cap drops. That is the half asserted below.
    """

    def _ingest_and_watch(self, text):
        """One resource ingest, recording every term kind that reaches the ranking."""
        import tempfile
        from pathlib import Path

        import matrixark_mcp_temporal_adapters  # noqa: F401  (imported first: backend cycle)
        import matrixark_local_adapter_ingest as adapter_ingest
        import matrixark_mcp_ingest_resource_chunk_records as chunk_records
        from matrixark_mcp_local_adapter import MatrixArkLocalAdapter

        reached = set()
        invocations = []
        originals = {}

        def watch(module):
            real = getattr(module, "limited_index_terms", None)
            if real is None:
                return
            originals[module] = real

            def spy(terms, *, limit):
                invocations.append(limit)
                reached.update(str(term).partition(":")[0] for term in terms if term)
                return real(terms, limit=limit)

            module.limited_index_terms = spy

        # Patch the CALLER's own global, which is the binding its function body resolves at call
        # time. Patching the defining module does nothing: both callers took the name by import.
        watch(adapter_ingest)
        watch(chunk_records)
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "index-term-cap.jsonl")
                document = Path(tmp_dir) / "benchmark-notes.md"
                document.write_text(text, encoding="utf-8")
                adapter.ingest(
                    {
                        "kind": "resource",
                        "raw_uri": str(document),
                        "resource_type": "md",
                        "scope": {"account_id": "acct_cap", "tenant_id": "tenant_cap"},
                        "messages": [{"role": "user", "content": "Import the benchmark notes."}],
                        "wait": True,
                    }
                )
                # A conversational ingest as well: `benchmark_quality_index_terms` runs from
                # `candidate_index_terms`, over events and entities, not over resource chunks.
                # Without this the "produced at all" denominator below is empty and the claim is
                # free -- which is how this guard first failed.
                adapter.ingest(
                    {
                        "scope": {
                            "account_id": "acct_cap",
                            "tenant_id": "tenant_cap",
                            "user_id": "user_cap",
                            "session_id": "session_cap",
                        },
                        "async_processing": False,
                        "skip_prior_context": True,
                        "messages": [{"role": "user", "content": text}],
                    }
                )
                written = {
                    str(record.get("index_name") or "").partition(":")[0]
                    for record in adapter.read_all()
                    if record.get("record_type") == "context_index"
                }
        finally:
            for module, real in originals.items():
                module.limited_index_terms = real
        return reached, written, invocations

    #: Text chosen so `benchmark_quality_index_terms` fires on all three of its kinds -- a named
    #: benchmark, several metrics, and a `workload:` phrase. Without that the claim below is free.
    TEXT = """# Benchmark Notes

Locomo results: p99 latency and throughput improved. Recall and precision both rose.

## Workload

workload: mixed-read p95 latency held. LongMemEval hit-rate steady.
"""

    def test_the_kinds_the_comment_names_are_produced_but_never_reach_the_cap(self):
        reached, written, invocations = self._ingest_and_watch(self.TEXT)
        named = {"benchmark", "metric", "workload"}
        # Two denominators. A run where the ranking was never consulted, or where none of the three
        # kinds was produced at all, would satisfy the assertion below for the wrong reason.
        self.assertTrue(invocations, "the ranking was never consulted; this proves nothing")
        self.assertTrue(
            named & written,
            "no benchmark/metric/workload term was produced at all; this proves nothing",
        )
        self.assertEqual(set(), named & reached)

    SKILL = """---
name: retention-inspector
description: Inspect retention and compaction evidence.
triggers:
  - retention
  - compaction
allowed_tools:
  - matrixark_replay
  - matrixark_retrieve
status: active
---

# Retention Inspector

Use this to inspect retention evidence for slabs, buckets and streams.

## Compaction

Slabs fold nightly with throughput and recall notes.
"""

    def test_the_cap_binds_on_a_skill_and_drops_the_lowest_ranked_kind(self):
        """The rank order is not decoration -- it decides what survives every skill import.

        A plain markdown chunk offers at most ten terms against a limit of ten, so nothing is
        dropped and the order never shows. A skill chunk adds skill_name / skill_trigger /
        skill_tool and goes over. Measured: fourteen candidates on one chunk, ten kept, and the
        four dropped were all `keyword`, which ranks last of the nine producible kinds.
        """
        import tempfile
        from pathlib import Path

        import matrixark_mcp_temporal_adapters  # noqa: F401  (imported first: backend cycle)
        import matrixark_local_adapter_ingest as adapter_ingest
        from matrixark_mcp_indexing import SECONDARY_INDEX_PRIORITY_PREFIXES
        from matrixark_mcp_local_adapter import MatrixArkLocalAdapter

        ranked = [prefix.rstrip(":") for prefix in SECONDARY_INDEX_PRIORITY_PREFIXES]
        calls = []
        real = adapter_ingest.limited_index_terms

        def spy(terms, *, limit):
            kept = real(terms, limit=limit)
            unique = list(dict.fromkeys(term for term in terms if term))
            calls.append((unique, kept))
            return kept

        adapter_ingest.limited_index_terms = spy
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "skill-cap.jsonl")
                skill = Path(tmp_dir) / "SKILL.md"
                skill.write_text(self.SKILL, encoding="utf-8")
                adapter.ingest(
                    {
                        "kind": "skill",
                        "raw_uri": str(skill),
                        "resource_type": "skill",
                        "scope": {"account_id": "acct_cap", "tenant_id": "tenant_cap"},
                        "messages": [{"role": "user", "content": "Import the skill."}],
                        "wait": True,
                    }
                )
                manifests = sum(
                    1 for record in adapter.read_all() if record.get("record_type") == "skill_manifest"
                )
        finally:
            adapter_ingest.limited_index_terms = real

        # Denominators. A run that ingested no skill, or never went over the limit, would make
        # every claim below free.
        self.assertEqual(1, manifests, "the skill was not ingested; this proves nothing")
        binding = [(unique, kept) for unique, kept in calls if len(unique) > len(kept)]
        self.assertTrue(binding, "the cap never bound; the rank order decided nothing here")

        for unique, kept in binding:
            dropped = [term for term in unique if term not in kept]
            kept_ranks = [ranked.index(term.partition(":")[0]) for term in kept]
            dropped_ranks = [ranked.index(term.partition(":")[0]) for term in dropped]
            # Everything dropped ranks at or below everything kept: the cap took the tail of the
            # order, not an arbitrary slice.
            self.assertGreaterEqual(min(dropped_ranks), max(kept_ranks))
            self.assertEqual(
                {"keyword"}, {term.partition(":")[0] for term in dropped}
            )

    def test_the_callers_resolve_cores_copy_of_the_cap(self):
        """There are two `limited_index_terms`. Checked by identity, not by reading imports.

        Mutating `matrixark_mcp_indexing`'s body leaves every other guard in this file green,
        because neither caller reaches it. The docstrings on both copies say so; this is what
        keeps them honest, and what fails if the resolution ever moves.
        """
        import matrixark_mcp_temporal_adapters  # noqa: F401  (imported first: backend cycle)
        import matrixark_local_adapter_ingest as adapter_ingest
        import matrixark_mcp_core as core
        import matrixark_mcp_indexing as indexing
        import matrixark_mcp_ingest_resource_chunk_records as chunk_records

        # Denominator: two copies really exist. If they were ever consolidated this test should be
        # deleted, not quietly satisfied by both names pointing at one object.
        self.assertIsNot(core.limited_index_terms, indexing.limited_index_terms)
        for caller in (adapter_ingest, chunk_records):
            self.assertIs(caller.limited_index_terms, core.limited_index_terms)
        # The ORDER, unlike the capping loop, has exactly one definition.
        self.assertIs(core.secondary_index_priority, indexing.secondary_index_priority)

    def test_every_kind_that_reaches_the_cap_has_a_rank(self):
        """The half that fails silently: a producible kind missing from the tuple ranks LAST."""
        from matrixark_mcp_indexing import SECONDARY_INDEX_PRIORITY_PREFIXES

        ranked = {prefix.rstrip(":") for prefix in SECONDARY_INDEX_PRIORITY_PREFIXES}
        reached, _, invocations = self._ingest_and_watch(self.TEXT)
        self.assertTrue(invocations, "the ranking was never consulted; this proves nothing")
        self.assertTrue(reached, "no term reached the ranking; this proves nothing")
        self.assertEqual(set(), reached - ranked)


if __name__ == "__main__":
    unittest.main()
