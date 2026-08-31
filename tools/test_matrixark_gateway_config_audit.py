#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Does each setting's `applies` label match where the code actually reads it?

The portal tells a customer whether a change is live or waits on a restart. That claim is only
worth anything if it tracks the code, and the code moves: a variable read per call today becomes a
module constant the moment someone hoists it for speed. Nothing about that refactor looks like it
touches the portal, and the portal would go on saying "live" while the running process kept the old
value -- the exact failure the label exists to prevent.

So the label is verified against the source rather than trusted. This walks the AST of every module
under tools/, finds each occurrence of an environment-variable NAME as a string constant, and
classifies it by whether it sits inside a function (read per call) or at module level (captured
once at import). A setting with any import-time reader must be labelled `restart`.

AST rather than grep because the reads wrap:

    model_ref = os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH") or os.environ.get(
        "MATRIXARK_EMBEDDING_MODEL", default)

and because indirect reads (`_truthy_env("MATRIXARK_REQUIRE_MODEL_EMBEDDINGS")`) carry no
`os.environ` on the line at all -- a line-regex scan reports those as having no reader, which is
how they get mistaken for dead configuration.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as cfgmod  # noqa: E402

TOOLS = os.path.dirname(os.path.abspath(__file__))

# Files that MENTION these variable names without reading them: the config-key mapping table, the
# tenant-policy knob registry, and the portal's own registry. Counting a lookup table as an
# import-time read would mark every setting `restart`.
_NOT_READERS = {
    "matrixark_load_config.py",
    "matrixark_tenant_policy.py",
    "matrixark_gateway_config.py",
    "matrixark_gateway_metrics.py",
}

# Separate PROCESSES with their own lifecycle. context_minilm_embed_server is the co-located
# encoder, started and restarted on its own; a variable it captures at import says nothing about
# whether the gateway picks up a change. The settings it reads carry the caveat in their help text
# instead, which is where a customer will actually see it.
_OTHER_PROCESSES = {"context_minilm_embed_server.py"}

# Functions that run ONCE per process, at startup. A read inside one is syntactically per call and
# behaviourally frozen: GatewayConfig.from_env is called by make_v1_app and never again, so the
# rate limits and quotas it reads are captured for the life of the worker. Classifying those as
# per-call is how a portal ends up telling a customer their new rate limit is live while every
# request keeps going through a limiter built at boot.
# Module-level structures that MENTION variable names without reading them: the API route
# documentation, the scope catalogue, the read-only inventory. Excluding whole files is too blunt
# here -- matrixark_v1_gateway really does read some of these variables elsewhere -- so the
# exclusion is per data structure. ROUTE_DOCS naming MATRIXARK_INGESTION_ROOT in a summary was read
# as an import-time capture and reported the setting as mislabelled.
_DOC_STRUCTURES = {"ROUTE_DOCS", "SCOPE_CATALOG", "SCOPE_PRESETS", "INVENTORY", "PRESETS",
                   "SETTINGS", "GROUPS"}

# Functions that read a variable only to SHOW it. They are not readers in the sense this audit
# cares about -- nothing behaves differently because of what they saw -- and counting them as such
# turns every value the portal displays into a "live" setting. `_encoder_summary` reads the drainer
# flag to tell a customer whether it is on; the thing that acts on it is the ENGINE, once, at its
# own startup.
_DISPLAY_ONLY = {
    ("matrixark_v1_gateway.py", "_model_config_snapshot"),
    ("matrixark_v1_gateway.py", "_encoder_summary"),
    ("matrixark_v1_gateway.py", "_readiness_checks"),
}

_STARTUP_ONLY = {
    ("matrixark_v1_gateway.py", "GatewayConfig.from_env"),
    ("matrixark_v1_gateway.py", "_coerce_config"),
    ("matrixark_v1_gateway.py", "make_v1_app"),
    ("matrixark_v1_gateway.py", "create_v1_app"),
    ("matrixark_v1_gateway.py", "_build_server_from_env"),
}


class _Sites(ast.NodeVisitor):
    """Collect (enclosing function qualname, line) for every string constant we care about."""

    def __init__(self, wanted: Set[str]) -> None:
        self.wanted = wanted
        self.stack: List[str] = []
        self.hits: List[Tuple[str, Optional[str], int]] = []
        self.in_doc_structure = False

    def _push(self, node) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_ClassDef = _push
    visit_FunctionDef = _push
    visit_AsyncFunctionDef = _push

    def visit_Lambda(self, node) -> None:
        self.stack.append("<lambda>")
        self.generic_visit(node)
        self.stack.pop()

    def visit_Assign(self, node) -> None:
        names = {t.id for t in node.targets if isinstance(t, ast.Name)}
        was = self.in_doc_structure
        self.in_doc_structure = was or bool(names & _DOC_STRUCTURES)
        self.generic_visit(node)
        self.in_doc_structure = was

    def visit_AnnAssign(self, node) -> None:
        name = node.target.id if isinstance(node.target, ast.Name) else ""
        was = self.in_doc_structure
        self.in_doc_structure = was or name in _DOC_STRUCTURES
        self.generic_visit(node)
        self.in_doc_structure = was

    def visit_Constant(self, node) -> None:
        if self.in_doc_structure:
            return
        if isinstance(node.value, str) and node.value in self.wanted:
            # A class body is still module-level execution; only a function defers evaluation.
            qual = ".".join(self.stack) if self.stack else None
            self.hits.append((node.value, qual, node.lineno))


def _scan() -> Dict[str, List[Tuple[str, str, int]]]:
    """env var -> [(scope, file, line)] for every string-constant occurrence in a reader module."""
    wanted = {s.env for s in cfgmod.SETTINGS if s.env}
    found: Dict[str, List[Tuple[str, str, int]]] = {name: [] for name in wanted}
    for entry in sorted(os.listdir(TOOLS)):
        if not entry.endswith(".py") or entry.startswith(("test_", "run_")):
            continue
        if entry in _NOT_READERS or entry in _OTHER_PROCESSES:
            continue
        path = os.path.join(TOOLS, entry)
        try:
            tree = ast.parse(open(path, encoding="utf-8", errors="replace").read(), filename=entry)
        except SyntaxError:
            continue
        visitor = _Sites(wanted)
        visitor.visit(tree)
        for name, qual, line in visitor.hits:
            if qual is not None and (entry, qual.split(".")[-1]) in _DISPLAY_ONLY:
                continue
            if qual is None:
                scope = "import-time"
            elif (entry, qual) in _STARTUP_ONLY or any(
                    (entry, part) in _STARTUP_ONLY for part in (qual.split(".")[0], qual)):
                scope = "import-time"
            else:
                scope = "per-call"
            found[name].append((scope, entry, line))
    return found


SITES = _scan()


class AppliesLabelTest(unittest.TestCase):
    def test_every_import_time_reader_is_labelled_restart(self) -> None:
        """The direction that misleads a customer: claiming a change is live when it cannot be."""
        wrong: List[str] = []
        for setting in cfgmod.SETTINGS:
            if not setting.env or setting.applies == "restart":
                continue
            frozen = [s for s in SITES.get(setting.env, []) if s[0] == "import-time"]
            if frozen:
                where = ", ".join("%s:%d" % (f, n) for _s, f, n in frozen[:3])
                wrong.append("%s (%s) is labelled live but captured at import in %s"
                             % (setting.key, setting.env, where))
        self.assertEqual([], wrong, "\n".join(wrong))

    def test_no_setting_claims_a_restart_it_does_not_need(self) -> None:
        """The other direction: a needless restart is a real cost to a running deployment, and a
        label nobody trusts stops being read at all."""
        wrong: List[str] = []
        for setting in cfgmod.SETTINGS:
            if not setting.env or setting.applies != "restart":
                continue
            sites = SITES.get(setting.env, [])
            if sites and not any(scope == "import-time" for scope, _f, _n in sites):
                where = ", ".join("%s:%d" % (f, n) for _s, f, n in sites[:3])
                wrong.append("%s (%s) is labelled restart but every reader is per-call: %s"
                             % (setting.key, setting.env, where))
        self.assertEqual([], wrong, "\n".join(wrong))


class RegistryShapeTest(unittest.TestCase):
    def test_no_two_settings_drive_the_same_variable(self) -> None:
        # Two settings on one variable means the second silently overwrites the first, and the
        # portal shows two fields that disagree about the same value.
        seen: Dict[str, str] = {}
        for setting in cfgmod.SETTINGS:
            if not setting.env:
                continue
            self.assertNotIn(setting.env, seen,
                             "%s and %s both write %s" % (seen.get(setting.env), setting.key,
                                                          setting.env))
            seen[setting.env] = setting.key

    def test_every_group_has_metadata_and_every_setting_has_help(self) -> None:
        for setting in cfgmod.SETTINGS:
            with self.subTest(setting=setting.key):
                self.assertIn(setting.group, cfgmod.GROUPS)
                self.assertTrue(setting.help.strip(), "a setting with no help is a mystery box")
                self.assertIn(setting.applies, ("live", "restart"))
                self.assertIn(setting.kind, ("str", "int", "float", "bool", "secret"))

    def test_the_behaviour_group_is_derived_from_the_tenant_policy_registry(self) -> None:
        # Not retyped: if this stops matching, the portal has grown a second copy to drift from.
        import matrixark_tenant_policy as policy
        derived = {s.key[len("behaviour."):] for s in cfgmod.SETTINGS if s.group == "behaviour"}
        self.assertTrue(derived, "no behaviour knobs were derived at all")
        self.assertTrue(derived.issubset(set(policy.KNOBS)),
                        "behaviour settings not present in KNOBS: %s"
                        % sorted(derived - set(policy.KNOBS)))
        for name in derived:
            with self.subTest(knob=name):
                setting = cfgmod.SETTINGS_BY_KEY["behaviour." + name]
                self.assertEqual(policy.KNOBS[name].env, setting.env)
                self.assertEqual(policy.KNOBS[name].description, setting.help)


class EssentialSettingsTest(unittest.TestCase):
    """Which of the 79 a customer actually has to set.

    "What can I change" and "what must I set" are different questions, and the second is the only
    one a customer setting up has. Marking the answer is only useful while the mark stays true.
    """

    def test_every_essential_key_is_a_real_setting(self) -> None:
        unknown = cfgmod.ESSENTIAL_KEYS - set(cfgmod.SETTINGS_BY_KEY)
        self.assertEqual(set(), unknown,
                         "marked required but not in the registry: %s" % sorted(unknown))

    def test_it_covers_both_model_roles_and_the_ingestion_root(self) -> None:
        # A deployment with no extraction model stores rule-extracted memories; with no encoder it
        # retrieves on hash vectors; with no ingestion root bulk import refuses outright. Those are
        # the three that are not "a tuning decision".
        groups = {cfgmod.SETTINGS_BY_KEY[k].group for k in cfgmod.ESSENTIAL_KEYS}
        self.assertIn("extraction", groups)
        self.assertIn("embedding", groups)
        self.assertIn("ingestion.root", cfgmod.ESSENTIAL_KEYS)

    def test_a_tuning_knob_is_never_marked_required(self) -> None:
        # Everything in these groups has a defensible default; calling one required would send a
        # customer hunting for a value nobody can tell them.
        for key in cfgmod.ESSENTIAL_KEYS:
            with self.subTest(key=key):
                self.assertNotIn(cfgmod.SETTINGS_BY_KEY[key].group,
                                 ("behaviour", "limits", "retrieval", "skills"))

    def test_the_snapshot_marks_them(self) -> None:
        marked = {field["key"]
                  for fields in cfgmod.snapshot()["groups"].values()
                  for field in fields if field["essential"]}
        self.assertEqual(cfgmod.ESSENTIAL_KEYS, marked)

    def test_a_secret_and_the_variable_that_holds_it_travel_together(self) -> None:
        # Marking the key required without the variable that names it sends someone to a field
        # whose destination is still whatever the default was.
        for role in ("extraction", "embedding"):
            with self.subTest(role=role):
                self.assertEqual(
                    "%s.api_key" % role in cfgmod.ESSENTIAL_KEYS,
                    "%s.api_key_env" % role in cfgmod.ESSENTIAL_KEYS)


class InventoryTest(unittest.TestCase):
    def test_the_inventory_is_read_only_and_disjoint_from_the_writable_registry(self) -> None:
        # A variable that appears in both would be shown as un-editable deployment state on one
        # part of the page and editable on another.
        writable = {s.env for s in cfgmod.SETTINGS if s.env}
        for block in cfgmod.INVENTORY:
            for name, _label in block["vars"]:
                with self.subTest(env=name):
                    self.assertNotIn(name, writable)

    def test_a_credential_shaped_variable_is_never_reported_by_value(self) -> None:
        os.environ["MATRIXARK_API_KEYS_FILE"] = "/srv/keys.jsonl"
        try:
            rows = {r["env"]: r for block in cfgmod.inventory() for r in block["rows"]}
            # A path is safe to show; the rule is on the NAME so a variable added later that does
            # hold a secret cannot leak by oversight.
            self.assertEqual("/srv/keys.jsonl", rows["MATRIXARK_API_KEYS_FILE"]["value"])
            self.assertFalse(rows["MATRIXARK_API_KEYS_FILE"]["redacted"])
        finally:
            os.environ.pop("MATRIXARK_API_KEYS_FILE", None)

    def test_the_inventory_cannot_be_written_through_the_settings_endpoint(self) -> None:
        for block in cfgmod.INVENTORY:
            for name, _label in block["vars"]:
                with self.subTest(env=name):
                    with self.assertRaises(cfgmod.UnknownSetting):
                        cfgmod.update({name: "x"})


if __name__ == "__main__":
    unittest.main()
