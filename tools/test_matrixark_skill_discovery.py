#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Tests for the skill discovery / extraction layer."""
import unittest

import matrixark_skill_discovery as sd
from matrixark_skill_discovery import InteractionEvent as E


def _proc_session(sid, ts0, *, pattern=("Grep", "Read", "Edit"), trigger="fix the failing auth bug"):
    """A user turn + a tool procedure, as a list of events."""
    evs = [E(session_id=sid, ts_ms=ts0, role="user", text=trigger, origin="claude")]
    for i, tool in enumerate(pattern, 1):
        evs.append(E(session_id=sid, ts_ms=ts0 + i, role="assistant", tool_name=tool,
                     tool_input={"file_path": "auth.py"} if tool in ("Read", "Edit") else {"pattern": "login"}))
    return evs


class EpisodeMiningTest(unittest.TestCase):
    def test_episodes_bounded_by_user_turns(self):
        evs = _proc_session("s1", 1000) + _proc_session("s1", 2000, trigger="add a test")
        eps = sd.mine_episodes(evs)
        self.assertEqual(2, len(eps))
        self.assertEqual([("Grep", "login"), ("Read", "auth.py"), ("Edit", "auth.py")], eps[0].actions)

    def test_episode_without_tools_is_dropped(self):
        evs = [E("s1", 1, "user", text="just a question"), E("s1", 2, "assistant", text="an answer")]
        self.assertEqual([], sd.mine_episodes(evs))

    def test_signature_collapses_repeats(self):
        ep = sd.Episode("s", "t", [("Read", "a"), ("Read", "b"), ("Edit", "a")], 0, 0)
        self.assertEqual(("Read", "Edit"), sd.episode_signature(ep, collapse_repeats=True))
        self.assertEqual(("Read", "Read", "Edit"), sd.episode_signature(ep, collapse_repeats=False))

    def test_tool_name_normalized(self):
        self.assertEqual("Read", sd.normalize_tool("mcp__fs__Read"))
        self.assertEqual("Edit", sd.normalize_tool("functions.Edit"))


class DiscoveryTest(unittest.TestCase):
    def _corpus(self, n_repeats):
        evs = []
        for i in range(n_repeats):
            evs += _proc_session(f"s{i}", 1000 * (i + 1))
        return evs

    def test_recurring_procedure_becomes_a_skill(self):
        specs = sd.discover_skills(self._corpus(3), min_support=3, min_steps=2)
        self.assertEqual(1, len(specs))
        s = specs[0]
        self.assertEqual(("Grep", "Read", "Edit"), s.signature)
        self.assertEqual(3, s.support)
        self.assertEqual(3, len(s.sessions))
        self.assertEqual(["Edit", "Grep", "Read"], s.allowed_tools)  # sorted set

    def test_below_min_support_is_not_a_skill(self):
        self.assertEqual([], sd.discover_skills(self._corpus(2), min_support=3, min_steps=2))

    def test_below_min_steps_is_not_a_skill(self):
        evs = []
        for i in range(5):
            evs += _proc_session(f"s{i}", 1000 * (i + 1), pattern=("Bash",))  # single step
        self.assertEqual([], sd.discover_skills(evs, min_support=3, min_steps=2))

    def test_triggers_and_name_derived_from_requests(self):
        specs = sd.discover_skills(self._corpus(3), min_support=3)
        s = specs[0]
        self.assertIn("fix", s.triggers)          # from "fix the failing auth bug"
        self.assertIn("auth", s.triggers)
        self.assertTrue(s.name.lower().startswith("fix"))  # intent verb heads the name

    def test_determinism(self):
        a = sd.discover_skills(self._corpus(4), min_support=3)
        b = sd.discover_skills(self._corpus(4), min_support=3)
        self.assertEqual([(x.slug, x.support, x.signature) for x in a],
                         [(x.slug, x.support, x.signature) for x in b])

    def test_highest_support_ranked_first(self):
        evs = self._corpus(5)  # Grep/Read/Edit x5
        for i in range(3):     # a second, rarer procedure
            evs += _proc_session(f"t{i}", 50000 * (i + 1), pattern=("Bash", "Read"), trigger="run the tests")
        specs = sd.discover_skills(evs, min_support=3)
        self.assertEqual(2, len(specs))
        self.assertEqual(5, specs[0].support)      # most-supported first
        self.assertEqual(("Grep", "Read", "Edit"), specs[0].signature)


class CaptureTest(unittest.TestCase):
    def test_markdown_renders_procedure(self):
        s = sd.discover_skills([e for i in range(3) for e in _proc_session(f"s{i}", 1000 * (i + 1))], min_support=3)[0]
        md = s.render_markdown()
        self.assertIn("## Procedure", md)
        self.assertIn("`Grep`", md)
        self.assertIn("Triggers:", md)

    def test_records_feed_the_retrieval_lane(self):
        s = sd.discover_skills([e for i in range(3) for e in _proc_session(f"s{i}", 1000 * (i + 1))], min_support=3)[0]
        recs = sd.skill_records_for_spec(s, scope={"user": "u", "project": "p"}, updated_at_ms=42)
        types = [r["record_type"] for r in recs]
        self.assertIn("skill_manifest", types)
        self.assertIn("skill_registry", types)
        self.assertIn("skill_section", types)          # <- what the retrieve path's skill scan reads
        section = next(r for r in recs if r["record_type"] == "skill_section")
        self.assertEqual("active", next(r for r in recs if r["record_type"] == "skill_manifest")["status"])
        self.assertTrue(section["skill_hash"] == section["resource_hash"])  # section points at its skill
        self.assertGreater(section["token_estimate"], 0)

    def test_ingest_envelope_shape(self):
        s = sd.discover_skills([e for i in range(3) for e in _proc_session(f"s{i}", 1000 * (i + 1))], min_support=3)[0]
        env = sd.skill_ingest_envelope(s, scope={"user": "u"}, ingestion_time_ms=7)
        self.assertEqual("skill", env["kind"])
        self.assertEqual("skill", env["resource_type"])
        self.assertTrue(env["raw_uri"].startswith("skill://discovered/"))
        self.assertIn("triggers", env["metadata"])


class LlmNamingTest(unittest.TestCase):
    def _spec(self):
        return sd.discover_skills([e for i in range(3) for e in _proc_session(f"s{i}", 1000 * (i + 1))], min_support=3)[0]

    def test_llm_name_applied(self):
        spec = self._spec()
        def fake(_prompt):
            return 'sure: {"name": "Fix auth via grep-edit", "description": "Locate and patch the auth bug."}'
        sd.name_skills_with_llm([spec], fake)
        self.assertEqual("Fix auth via grep-edit", spec.name)
        self.assertEqual("fix-auth-via-grep-edit", spec.slug)
        self.assertTrue(spec.description.startswith("Locate and patch"))

    def test_bad_llm_output_keeps_deterministic_name(self):
        spec = self._spec()
        before = spec.name
        sd.name_skills_with_llm([spec], lambda _p: "no json here")
        self.assertEqual(before, spec.name)

    def test_llm_exception_is_swallowed(self):
        spec = self._spec()
        before = spec.name
        def boom(_p):
            raise RuntimeError("model down")
        sd.name_skills_with_llm([spec], boom)
        self.assertEqual(before, spec.name)


class IdentityDedupTest(unittest.TestCase):
    def _specs(self):
        evs = [e for i in range(4) for e in _proc_session(f"s{i}", 1000 * (i + 1))]  # Grep,Read,Edit
        for i in range(3):
            evs += _proc_session(f"t{i}", 90000 * (i + 1), pattern=("Bash", "Write"), trigger="deploy the service")
        return sd.discover_skills(evs, min_support=3)

    def test_slug_match_drops_local_skill(self):
        specs = self._specs()
        target = specs[0]
        kept = sd.dedup_discovered_vs_local(specs, local_names=[target.name])
        self.assertNotIn(target.slug, [s.slug for s in kept])

    def test_tool_set_match_drops(self):
        specs = self._specs()
        kept = sd.dedup_discovered_vs_local(specs, local_tool_sets=[["Grep", "Read", "Edit"]])
        self.assertFalse(any(s.signature == ("Grep", "Read", "Edit") for s in kept))

    def test_net_new_skill_survives(self):
        specs = self._specs()
        kept = sd.dedup_discovered_vs_local(specs, local_tool_sets=[["SomeOther", "Tool"]])
        self.assertEqual(len(specs), len(kept))  # nothing matched local


class LeanedMessageParsingTest(unittest.TestCase):
    def test_parses_tool_markers_into_ordered_actions(self):
        msgs = [
            {"role": "user", "content": "fix the auth bug"},
            {"role": "assistant", "content": "[tool:Grep] login\n[tool:Read] auth.py\n[tool:Edit] auth.py"},
        ]
        evs = sd.events_from_leaned_messages(msgs, "sess")
        self.assertEqual("user", evs[0].role)
        self.assertEqual(["Grep", "Read", "Edit"], [e.tool_name for e in evs if e.tool_name])
        eps = sd.mine_episodes(evs)
        self.assertEqual([("Grep", "login"), ("Read", "auth.py"), ("Edit", "auth.py")], eps[0].actions)

    def test_list_content_and_no_tool_markers(self):
        msgs = [{"role": "user", "content": [{"type": "text", "text": "just asking"}]},
                {"role": "assistant", "content": "no tools here"}]
        evs = sd.events_from_leaned_messages(msgs, "s")
        self.assertEqual(1, len(evs))
        self.assertEqual("user", evs[0].role)


class OrchestratorTest(unittest.TestCase):
    def test_discover_capture_pipeline(self):
        evs = [e for i in range(3) for e in _proc_session(f"s{i}", 1000 * (i + 1))]
        sink = []
        out = sd.discover_capture_for_session(
            evs, scope={"user": "u", "project": "p"},
            ingest_records=lambda recs: sink.append(recs),
            complete=lambda _p: '{"name": "Fix auth bug", "description": "Grep, read, edit auth."}',
        )
        self.assertEqual(1, out["captured"])
        self.assertEqual("Fix auth bug", out["skills"][0]["name"])
        self.assertTrue(any(r["record_type"] == "skill_section" for r in sink[0]))

    def test_all_dup_of_local_is_noop(self):
        evs = [e for i in range(3) for e in _proc_session(f"s{i}", 1000 * (i + 1))]
        local = [{"record_type": "skill_manifest", "name": "x", "allowed_tools": ["Grep", "Read", "Edit"]}]
        out = sd.discover_capture_for_session(evs, scope={"user": "u"}, ingest_records=lambda r: None, local_records=local)
        self.assertEqual(0, out["captured"])
        self.assertEqual("all_dup_of_local_skills", out["reason"])

    def test_ingest_failure_never_raises(self):
        evs = [e for i in range(3) for e in _proc_session(f"s{i}", 1000 * (i + 1))]
        def boom(_recs):
            raise IOError("store down")
        out = sd.discover_capture_for_session(evs, scope={"user": "u"}, ingest_records=boom)
        self.assertIn("error", out)  # swallowed, reported, not raised


if __name__ == "__main__":
    unittest.main()
