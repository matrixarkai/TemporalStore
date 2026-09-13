#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The two hooks keep private copies of two shared helpers, and both copies have drifted.

`matrixark_agent_hook` and `matrixark_codex_hook` are both entry points, and the agent hook
already imports from the Codex hook: `RESOURCE_EVENTS` and `RESOURCE_TYPE_BY_SUFFIX` were
declared in both, agreed, and were consolidated with the comment "they were declared in both and
agreed -- which is what a pair does until one of them is extended". The CONSTANTS were
consolidated. Two FUNCTIONS that read them were not, and those two have since drifted.

    payload_resource_type                      agent hook 22 lines, Codex hook 23
    retrieval_session_identity_from_retrieve   agent hook 21 lines, Codex hook 18

Both copies of both are called from their own hook's live path: `payload_resource_type` at
`matrixark_agent_hook.main` and in the Codex hook's resource handler;
`retrieval_session_identity_from_retrieve` from `agent_retrieval_summary` and from two Codex-hook
retrieve summaries.

MEASURED BY EXECUTION.

1. A FILE CALLED SKILL.md IS A SKILL TO ONE HOOK AND A MARKDOWN FILE TO THE OTHER.

       payload_resource_type({}, "SKILL.md")     agent hook -> "md"      Codex hook -> "skill"
       payload_resource_type({}, "/a/b/skill.md") agent hook -> "md"     Codex hook -> "skill"

   The Codex copy tests the FILENAME before consulting the suffix map. The agent copy consults
   the map first and passes the filename test as the map's DEFAULT:

       RESOURCE_TYPE_BY_SUFFIX.get(suffix, "skill" if Path(raw_uri).name.lower() == "skill.md"
                                           else "")

   A default is only consulted when the key is missing, and `".md"` is in the map -- it is the
   very entry that shadows this. So the `"skill"` arm of that default cannot be reached by any
   input: the only filename that selects it always carries the one suffix that prevents it. The
   guard below pins `".md"` in the map, because that is the fact the claim rests on.

2. THE SESSION-IDENTITY SUMMARY DIFFERS THREE WAYS, and they are not equally reachable. Stated
   separately, because a record that mixes them is not usable:

   a. The Codex copy `.strip()`s `session_id_source` and the agent copy does not, so a padded
      "  explicit  " is `strong_session_identity: true` through one and `false` through the other.
      REACHABLE ON THE CODEX SIDE: its caller reads the value out of request metadata
      (`args["metadata"]["session_id_source"]`), which a client supplies. NOT reachable on the
      agent side, whose value comes from its own `resolve_session_id` and is a fixed token.

   b. The Codex copy always emits `"source": "hook_metadata_fallback"` on a fallback; the agent
      copy omits it when `pack` is not a dict. NOT REACHABLE at the agent hook's own call site,
      which coerces the pack to a dict first. A source-level difference only, recorded so the
      set below is the whole truth.

   c. The Codex copy resolves the pack through `_context_pack_view`, which unwraps
      `{"context_pack": ...}` and `{"extra": {"context_pack": ...}}`. The agent hook has no such
      helper anywhere in the file. Given a wrapped pack that carries a strong identity, the Codex
      copy returns it and the agent copy reports `strong_session_identity: false` with the
      workspace-merge risk. NOT CLAIMED TO OCCUR: nothing on the serving path returns a wrapped
      pack today -- the only producers of that shape in this tree are a debug-trace tool and a
      benchmark. What is recorded is that one hook handles the shape and the other cannot.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE. Making the agent hook classify SKILL.md as a
skill changes what is ingested and how it is indexed on a live hook path; making the Codex hook
stop doing so changes it the other way. Same for the identity summary, which is written into the
audit record. Those are decisions, not cleanups. The divergence is recorded in BOTH directions.
"""
from __future__ import annotations

import ast
import importlib
import os
import unittest
from typing import Dict, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: Every live copy. Both hooks are entry points; each calls its own copy.
LIVE_COPIES = ("matrixark_agent_hook", "matrixark_codex_hook")

FUNCTIONS = ("payload_resource_type", "retrieval_session_identity_from_retrieve")

#: What each copy answers for a filename the two disagree about. Asserted per copy, exactly.
RECORDED_RESOURCE_TYPE: Dict[str, Dict[str, str]] = {
    "SKILL.md": {"matrixark_agent_hook": "md", "matrixark_codex_hook": "skill"},
    "/a/b/skill.md": {"matrixark_agent_hook": "md", "matrixark_codex_hook": "skill"},
    "notes.md": {"matrixark_agent_hook": "md", "matrixark_codex_hook": "md"},
    "thing.unknownsuffix": {"matrixark_agent_hook": "", "matrixark_codex_hook": ""},
    "readme.markdown": {"matrixark_agent_hook": "md", "matrixark_codex_hook": "md"},
}

#: The payload field that wins over the filename in BOTH copies, so the divergence above is
#: about the fallback and not about payload handling.
DIRECT_PAYLOAD = {"type": "text/markdown"}
RECORDED_DIRECT_ANSWER = "markdown"

#: A strong identity as a pack would carry it.
PACKED_IDENTITY = {"session_id_source": "explicit", "strong_session_identity": True}

#: label -> (pack, session_id_source, {module: the exact dict that copy returns}).
RECORDED_SESSION_IDENTITY = {
    "no pack, padded source": (
        None, "  explicit  ",
        {"matrixark_agent_hook": {"session_id_source": "  explicit  ",
                                  "strong_session_identity": False,
                                  "fallback_session_identity": False, "risk": ""},
         "matrixark_codex_hook": {"session_id_source": "explicit",
                                  "strong_session_identity": True,
                                  "fallback_session_identity": False, "risk": "",
                                  "source": "hook_metadata_fallback"}}),
    "no pack, plain source": (
        None, "explicit",
        {"matrixark_agent_hook": {"session_id_source": "explicit",
                                  "strong_session_identity": True,
                                  "fallback_session_identity": False, "risk": ""},
         "matrixark_codex_hook": {"session_id_source": "explicit",
                                  "strong_session_identity": True,
                                  "fallback_session_identity": False, "risk": "",
                                  "source": "hook_metadata_fallback"}}),
    "wrapped pack carrying a strong identity": (
        {"context_pack": {"recall_policy": {"session_identity": PACKED_IDENTITY}}}, "state_file",
        {"matrixark_agent_hook": {
            "session_id_source": "state_file", "strong_session_identity": False,
            "fallback_session_identity": True,
            "risk": "workspace_fallback_may_merge_multiple_codex_tasks",
            "source": "hook_metadata_fallback"},
         "matrixark_codex_hook": dict(PACKED_IDENTITY)}),
    "flat pack carrying a strong identity": (
        {"recall_policy": {"session_identity": PACKED_IDENTITY}}, "state_file",
        {"matrixark_agent_hook": dict(PACKED_IDENTITY),
         "matrixark_codex_hook": dict(PACKED_IDENTITY)}),
}

MODULE_SCAN_FLOOR = 200


def _import(stem):
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


def _defining_modules(function: str) -> Tuple[Set[str], int]:
    found: Set[str] = set()
    scanned = 0
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py") or name.startswith("test_") or name.startswith("__"):
            continue
        scanned += 1
        try:
            with open(os.path.join(TOOLS, name), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):  # pragma: no cover
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function:
                found.add(name[:-3])
    return found, scanned


def _call_sites(stem: str, function: str) -> int:
    with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    return sum(1 for node in ast.walk(tree)
               if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
               and node.func.id == function)


class TheHookHelpersHaveOneDefinition(unittest.TestCase):

    def test_the_copies_are_there_to_compare(self) -> None:
        """A floor and the anchor. Every assertion below passes over an empty read."""
        for function in FUNCTIONS:
            found, scanned = _defining_modules(function)
            with self.subTest(function=function):
                self.assertGreaterEqual(
                    scanned, MODULE_SCAN_FLOOR,
                    "the definition scan read only %d production modules" % scanned)
                self.assertEqual(
                    sorted(LIVE_COPIES), sorted(found),
                    "%s is defined by a different set of production modules than this file "
                    "records" % function)
            objects = {stem: getattr(_import(stem), function) for stem in LIVE_COPIES}
            for stem, bound in objects.items():
                with self.subTest(function=function, module=stem):
                    self.assertEqual(
                        stem, bound.__module__.rsplit(".", 1)[-1],
                        "%s.%s is owned by %s" % (stem, function, bound.__module__))
                    self.assertGreater(
                        _call_sites(stem, function), 0,
                        "%s defines %s and no longer calls it" % (stem, function))
            self.assertEqual(
                len(LIVE_COPIES), len({id(bound) for bound in objects.values()}),
                "the two %s are the same object, so one hook re-exports the other's and the "
                "split is resolved" % function)

    def test_the_hooks_already_share_the_constant_these_functions_read(self) -> None:
        """The reason this pair is worth recording: the CONSTANT was consolidated, not the code."""
        agent = _import("matrixark_agent_hook")
        codex = _import("matrixark_codex_hook")
        self.assertIs(
            agent.RESOURCE_TYPE_BY_SUFFIX, codex.RESOURCE_TYPE_BY_SUFFIX,
            "the hooks no longer share one RESOURCE_TYPE_BY_SUFFIX object. That is a second "
            "divergence, and it would also mean the tables below could differ for a reason this "
            "file is not recording.")
        self.assertEqual(
            "md", agent.RESOURCE_TYPE_BY_SUFFIX.get(".md"),
            "'.md' is no longer in RESOURCE_TYPE_BY_SUFFIX. The whole SKILL.md finding rests on "
            "it: the agent hook's `skill` answer sits in that map's DEFAULT, which only a MISSING "
            "key reaches.")

    def test_the_resource_type_each_hook_answers_is_exactly_this(self) -> None:
        """Asserted per copy and per filename, not as a difference list."""
        for raw_uri, expected in RECORDED_RESOURCE_TYPE.items():
            for stem, answer in expected.items():
                with self.subTest(raw_uri=raw_uri, module=stem):
                    produced = getattr(_import(stem), "payload_resource_type")({}, raw_uri)
                    self.assertEqual(
                        answer, produced,
                        "%s now classifies %s as %r, not %r"
                        % (stem, raw_uri, produced, answer))
        disagreements = [uri for uri, expected in RECORDED_RESOURCE_TYPE.items()
                         if len(set(expected.values())) > 1]
        self.assertEqual(
            ["/a/b/skill.md", "SKILL.md"], sorted(disagreements),
            "the filenames the two hooks disagree about have changed")

    def test_a_payload_resource_type_still_wins_over_the_filename_in_both(self) -> None:
        """A floor on the FIXTURE: the divergence above is about the FALLBACK, not the payload."""
        for stem in LIVE_COPIES:
            with self.subTest(module=stem):
                self.assertEqual(
                    RECORDED_DIRECT_ANSWER,
                    getattr(_import(stem), "payload_resource_type")(dict(DIRECT_PAYLOAD),
                                                                    "SKILL.md"),
                    "%s no longer prefers an explicit payload type over the filename, so the "
                    "SKILL.md cases above are not isolating the fallback" % stem)

    def test_the_session_identity_each_hook_reports_is_exactly_this(self) -> None:
        """Asserted per copy, as whole dicts, in both directions."""
        for label, (pack, source, expected) in RECORDED_SESSION_IDENTITY.items():
            for stem, answer in expected.items():
                with self.subTest(case=label, module=stem):
                    produced = getattr(
                        _import(stem), "retrieval_session_identity_from_retrieve")(
                            pack if pack is None else dict(pack), session_id_source=source)
                    self.assertEqual(
                        answer, produced,
                        "%s now reports a different session identity for %s" % (stem, label))

    def test_only_one_hook_can_unwrap_a_nested_pack(self) -> None:
        """The mechanism behind the wrapped-pack row, stated as the thing that is missing."""
        with open(os.path.join(TOOLS, "matrixark_agent_hook.py"),
                  encoding="utf-8", errors="replace") as handle:
            agent_source = handle.read()
        self.assertNotIn(
            "_context_pack_view", agent_source,
            "matrixark_agent_hook now names _context_pack_view. If it unwraps a nested pack, the "
            "wrapped-pack row above is resolved -- strike it and say so.")
        codex = _import("matrixark_codex_hook")
        self.assertTrue(
            callable(getattr(codex, "_context_pack_view", None)),
            "matrixark_codex_hook no longer defines _context_pack_view, so the wrapped-pack row "
            "is not measuring what this file says it measures")
        wrapped = {"context_pack": {"recall_policy": {"session_identity": PACKED_IDENTITY}}}
        self.assertEqual(
            wrapped["context_pack"], codex._context_pack_view(dict(wrapped)),
            "_context_pack_view no longer unwraps the shape this record is about")


if __name__ == "__main__":
    unittest.main()
