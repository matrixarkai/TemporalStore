#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every control the co-located encoder server reads says the server needs restarting too.

``applies: live`` is true of the gateway, which re-reads both encoder fields on the next call. A
co-located encoder server does not: ``context_minilm_embed_server`` binds
``MATRIXARK_EMBEDDING_MODEL`` and ``MATRIXARK_EMBEDDING_MODEL_PATH`` at **its** import and keeps
whatever it started with. Only the first control said so.

The path is the one that matters more, because it *wins over* the model name: a customer who sets it
and restarts nothing is running an encoder chosen by a field the portal no longer shows as the
effective one, and nothing anywhere says why.

**The rule is derived, not listed.** The set of variables comes from parsing the server's own
module-scope reads, so a third one starts failing this the day it is added rather than the day
somebody notices. Listing the two by hand would have passed on the same day this was written and
told nobody anything afterwards.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402

SERVER = "context_minilm_embed_server.py"


def bound_at_import(filename: str) -> set:
    """Variables the module reads in a top-level assignment: bound once, when it starts."""
    with open(os.path.join(TOOLS, filename), encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=filename)
    found = set()
    for node in tree.body:                        # top level only
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Call) or not sub.args:
                continue
            target = sub.func
            reads_env = ((isinstance(target, ast.Attribute) and target.attr in {"get", "getenv"})
                         or (isinstance(target, ast.Name) and target.id == "getenv"))
            first = sub.args[0]
            if reads_env and isinstance(first, ast.Constant) and isinstance(first.value, str):
                found.add(first.value)
    return found


def controls_by_variable() -> dict:
    out: dict = {}
    for setting in cfg.SETTINGS:
        name = cfg._env_name(setting, {})
        if name:
            out.setdefault(name, []).append(setting)
    return out


class TheServerIsStillReadThisWayTest(unittest.TestCase):
    """Floors. The rule below is a loop over what this finds."""

    def test_the_server_is_there(self) -> None:
        self.assertTrue(os.path.exists(os.path.join(TOOLS, SERVER)), SERVER)

    def test_it_binds_something_at_import(self) -> None:
        self.assertTrue(bound_at_import(SERVER))

    def test_at_least_one_of_them_is_a_portal_control(self) -> None:
        """If none were, the rule would be inert and would pass forever."""
        shared = bound_at_import(SERVER) & set(controls_by_variable())
        self.assertTrue(shared, "the encoder server reads nothing the portal offers")


class EveryControlItReadsSaysSoTest(unittest.TestCase):

    def test_each_one_names_the_server(self) -> None:
        controls = controls_by_variable()
        for variable in sorted(bound_at_import(SERVER) & set(controls)):
            for setting in controls[variable]:
                with self.subTest(setting=setting.key, variable=variable):
                    self.assertIn("co-located encoder server", setting.help or "",
                                  "%s is read by %s at its import and does not say so"
                                  % (setting.key, SERVER))

    def test_they_say_it_the_same_way(self) -> None:
        """One sentence, shared. Two copies is how one of them came to carry it and the other not.

        The floor was two controls, which is what the file name still refers to. It is one now:
        `embedding.model_path` was a second field for the same value -- read only where it OVERRODE
        the model name -- and the field that absorbed it carries the note. One is not vacuous; zero
        would be, and that is what this guards.
        """
        controls = controls_by_variable()
        carrying = [s for v in bound_at_import(SERVER) & set(controls) for s in controls[v]]
        self.assertGreaterEqual(len(carrying), 1)
        for setting in carrying:
            with self.subTest(setting=setting.key):
                self.assertIn(cfg.ENCODER_SERVER_NOTE, setting.help or "")

    def test_a_control_it_does_not_read_is_left_alone(self) -> None:
        """A note on every control is a note nobody reads."""
        controls = controls_by_variable()
        read = bound_at_import(SERVER)
        for setting in cfg.SETTINGS:
            if cfg._env_name(setting, {}) in read:
                continue
            with self.subTest(setting=setting.key):
                self.assertNotIn("co-located encoder server", setting.help or "")

    def test_the_gateway_half_is_still_true(self) -> None:
        """`live` is not wrong -- it is true of the gateway, which is what the note explains. If
        these ever became `restart`, the note would be saying something the label already says."""
        controls = controls_by_variable()
        for variable in sorted(bound_at_import(SERVER) & set(controls)):
            for setting in controls[variable]:
                with self.subTest(setting=setting.key):
                    self.assertEqual("live", setting.applies)


if __name__ == "__main__":
    unittest.main()
