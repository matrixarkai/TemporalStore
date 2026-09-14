#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An unrecognised value must not disarm a requirement gate.

Nine gates decide whether a requirement is enforced -- whether the backend must be ready, whether
native packing and native candidate prefilter are mandatory, whether the Python record cache may
serve a read. Each was written in this shape:

    if MATRIXARK_REQUIRE_BACKEND_READY:
        return MATRIXARK_REQUIRE_BACKEND_READY in TRUE_VALUES
    return <the default this deployment would otherwise have>

#1677 fixed the WORDS -- `on` used to be missing from those sets and so read as OFF -- and
`test_on_means_on_in_every_boolean_vocabulary` now scans the whole tree for that shape. This file is
about the half that outlives it: the FALLBACK. Any value in neither vocabulary is non-empty, so it
enters the branch, is not in TRUE_VALUES, and answers False. Measured on c15dae4a1, production
profile, a native backend:

    unset     -> True      the requirement the deployment has by default
    =1 =on    -> True
    =off      -> False
    =ture     -> False     <- a typo did not merely fail to turn it on
    =y        -> False        it turned OFF a default that was on
    =enabled  -> False

So `MATRIXARK_REQUIRE_BACKEND_READY=ture` leaves a production deployment on a native backend NOT
requiring backend readiness, and nothing says so. Setting the variable is strictly worse than
leaving it alone, which is the shape where a failure looks like success.

`matrixark_mcp_env.env_bool` has always fallen back to the default instead, and the reasoning is
already settled in this tree: see `test_one_flag_has_one_boolean_vocabulary`, where treating every
unknown value as off would have turned `MATRIXARK_HOOK_FAIL_OPEN=ture` into a blocked turn -- the
same failure, in the same direction, as the defect being fixed. `env_bool` takes a variable NAME
and these gates hold a value already, so `flag_bool` is that one decision over a value and
`env_bool` now delegates to it.

ASKED OF THE SHIPPED GATES, not of the parser. The parser's answer was never in doubt; what was
wrong was at the sites. `test_on_means_on_in_every_boolean_vocabulary` derives its population from
the tree, which is the right shape for a property every module must have. A fallback cannot be read
off an AST -- it depends on what the surrounding function returns when the flag is absent -- so the
nine gates are listed here and called.
"""
from __future__ import annotations

import importlib
import os
import pathlib
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

# matrixark_temporal_direct_backend and matrixark_mcp_temporal_adapters import each other.
import matrixark_mcp_temporal_adapters  # noqa: F401,E402  (adapters first)

#: module, function, the variable it reads, arguments whose DEFAULT is True, arguments whose
#: DEFAULT is False. `None` where the flag has no False-default case:
#: `native_candidate_prefilter_required_for_backend` returns False for a non-native backend BEFORE
#: it consults the flag at all.
GATES = [
    ("matrixark_mcp_backends", "backend_ready_required",
     "MATRIXARK_REQUIRE_BACKEND_READY",
     {"backend": "temporalstore-rust"}, {"backend": "local"}),
    ("matrixark_mcp_server", "backend_ready_required",
     "MATRIXARK_REQUIRE_BACKEND_READY",
     {"backend": "temporalstore-rust-direct"}, {"backend": "local"}),
    ("matrixark_mcp_server", "native_context_pack_required",
     "MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK",
     {"backend": "temporalstore-rust"}, {"backend": "local"}),
    ("matrixark_mcp_server", "native_candidate_prefilter_required_for_backend",
     "MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER",
     {"backend": "temporalstore-rust"}, None),
    ("matrixark_mcp_server", "python_hot_cache_allowed",
     "MATRIXARK_ALLOW_PYTHON_HOT_CACHE",
     {"backend_label": "local"}, {"backend_label": "temporalstore-rust"}),
    ("matrixark_mcp_runtime_config", "native_candidate_prefilter_required",
     "MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER",
     {"backend_label": "temporalstore-rust"}, {"backend_label": "local"}),
    ("matrixark_mcp_runtime_config", "python_hot_cache_allowed",
     "MATRIXARK_ALLOW_PYTHON_HOT_CACHE",
     {"backend_label": "local"}, {"backend_label": "temporalstore-rust"}),
    ("matrixark_mcp_core", "native_candidate_prefilter_required",
     "MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER",
     {"backend_label": "temporalstore-rust"}, {"backend_label": "local"}),
    ("matrixark_mcp_native_pack_policy", "native_context_pack_required_for_backend",
     "require_flag",
     {"backend_label": "temporalstore-rust"}, {"backend_label": "local"}),
]

#: In neither vocabulary. A typo of a real word, two that read like words but are not in the sets,
#: and a number that is not 1 or 0.
UNRECOGNISED = ("ture", "garbage", "y", "n", "enabled", "2")
#: The positive controls. These are #1677's half and are asserted here only so that the fallback
#: tests cannot pass by answering the same thing for everything.
ON_WORDS = ("1", "true", "TRUE", "yes", "on", "ON", " On ")
OFF_WORDS = ("0", "false", "no", "off", "OFF", " Off ")


def _ask(module_name, func_name, variable, value, kwargs):
    """What the SHIPPED gate answers for that value.

    The flag is handed over every way a gate might read it -- the process environment, the module
    constant captured at import, and the keyword argument -- because the nine do not agree about
    which they read, and this file is about the fallback rather than the read timing. A gate that
    stopped reading the flag entirely still fails below, because its `off` answer would change.
    """
    module = importlib.import_module(module_name)
    func = getattr(module, func_name)
    call = dict(kwargs)
    saved_env = os.environ.get(variable)
    had_attr = hasattr(module, variable)
    saved_attr = getattr(module, variable, None)
    try:
        os.environ[variable] = value
        if had_attr:
            setattr(module, variable, value.strip().lower())
        if variable == "require_flag":
            call["require_flag"] = value
        return bool(func(**call))
    finally:
        if saved_env is None:
            os.environ.pop(variable, None)
        else:
            os.environ[variable] = saved_env
        if had_attr:
            setattr(module, variable, saved_attr)


class AnUnrecognisedValueDoesNotDisarmAGate(unittest.TestCase):

    def setUp(self):
        self._saved_env = dict(os.environ)
        # `backend_ready_required` reaches its default through `production_profile_enabled()`,
        # which reads a module constant captured at import. Force it so the True-default column
        # really is True; otherwise these tests would pass by asserting False against False.
        self._saved_profiles = {}
        for module_name in {name for name, *_ in GATES}:
            module = importlib.import_module(module_name)
            if hasattr(module, "MATRIXARK_MCP_PROFILE"):
                self._saved_profiles[module_name] = module.MATRIXARK_MCP_PROFILE
                module.MATRIXARK_MCP_PROFILE = "prod"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._saved_env)
        for module_name, value in self._saved_profiles.items():
            importlib.import_module(module_name).MATRIXARK_MCP_PROFILE = value

    def test_every_gate_in_the_table_resolves_and_its_default_is_really_true(self):
        """The floor, and it is not a formality.

        Every assertion below compares an unrecognised value against a default. If the arguments in
        the table stopped producing a True default -- a renamed backend, a changed profile check --
        the fallback tests would go on passing while checking False against False.
        """
        self.assertEqual(
            9, len(GATES),
            "the gate table changed size. It is a recorded fact: a gate added without a row here "
            "is a gate nothing checks, and a row left for a gate that is gone is decoration.")
        for module_name, func_name, variable, true_kwargs, _false in GATES:
            with self.subTest(gate="%s.%s" % (module_name, func_name)):
                module = importlib.import_module(module_name)
                self.assertTrue(
                    callable(getattr(module, func_name, None)),
                    "%s.%s is gone or is no longer callable" % (module_name, func_name))
                self.assertTrue(
                    _ask(module_name, func_name, variable, "", true_kwargs),
                    "%s.%s no longer defaults to True for %r, so the fallback assertions below "
                    "would compare False against False and could not fail"
                    % (module_name, func_name, true_kwargs))

    def test_an_unrecognised_value_does_not_turn_off_a_default_that_is_on(self):
        """The defect. A typo left a production deployment not enforcing its requirement."""
        for module_name, func_name, variable, true_kwargs, _false in GATES:
            for word in UNRECOGNISED:
                with self.subTest(gate="%s.%s" % (module_name, func_name), value=word):
                    self.assertTrue(
                        _ask(module_name, func_name, variable, word, true_kwargs),
                        "%s.%s reads %r as off. The default here is ON, so setting %s=%s is "
                        "strictly worse than leaving it unset -- the requirement stops being "
                        "enforced and nothing says so."
                        % (module_name, func_name, word, variable, word))

    def test_an_unrecognised_value_does_not_turn_on_a_default_that_is_off(self):
        """The other direction, and it is not symmetry for its own sake.

        Checking only the column above would pass for an implementation that answers True for
        everything it does not recognise. That is the same defect pointing the other way: a typo
        would then arm a requirement the deployment had deliberately left off.
        """
        for module_name, func_name, variable, _true, false_kwargs in GATES:
            if false_kwargs is None:
                continue
            for word in UNRECOGNISED:
                with self.subTest(gate="%s.%s" % (module_name, func_name), value=word):
                    self.assertFalse(
                        _ask(module_name, func_name, variable, word, false_kwargs),
                        "%s.%s reads %r as on, ignoring the default it would otherwise have used"
                        % (module_name, func_name, word))

    def test_a_recognised_value_still_decides(self):
        """#1677's half, kept as a control so the two tests above cannot pass vacuously."""
        for module_name, func_name, variable, true_kwargs, _false in GATES:
            for word in ON_WORDS:
                with self.subTest(gate="%s.%s" % (module_name, func_name), value=word):
                    self.assertTrue(
                        _ask(module_name, func_name, variable, word, true_kwargs), word)
            for word in OFF_WORDS:
                with self.subTest(gate="%s.%s" % (module_name, func_name), value=word):
                    self.assertFalse(
                        _ask(module_name, func_name, variable, word, true_kwargs),
                        "%s.%s reads %r as on. If an unrecognised value and `off` now answer the "
                        "same way, the fallback is being applied to a word that should decide."
                        % (module_name, func_name, word))


class FlagBoolIsTheOneDecision(unittest.TestCase):
    """`env_bool` and `flag_bool` must not become two vocabularies that merely agree today."""

    def test_env_bool_delegates_to_flag_bool(self):
        import matrixark_mcp_env as env_module

        calls = []
        original = env_module.flag_bool

        def counting(value, default):
            calls.append((value, default))
            return original(value, default)

        env_module.flag_bool = counting
        try:
            os.environ["MATRIXARK_TEST_ONE_DECISION"] = "on"
            self.assertTrue(env_module.env_bool("MATRIXARK_TEST_ONE_DECISION", False))
        finally:
            env_module.flag_bool = original
            os.environ.pop("MATRIXARK_TEST_ONE_DECISION", None)
        self.assertEqual(
            1, len(calls),
            "env_bool no longer asks flag_bool, so the two halves are free to grow separate "
            "vocabularies -- which is the defect this pair exists to make impossible")

    def test_flag_bool_falls_back_rather_than_guessing(self):
        from matrixark_mcp_env import flag_bool
        for word in UNRECOGNISED + ("",):
            with self.subTest(value=word):
                self.assertTrue(flag_bool(word, True), word)
                self.assertFalse(flag_bool(word, False), word)
        for word in ON_WORDS:
            self.assertTrue(flag_bool(word, False), word)
        for word in OFF_WORDS:
            self.assertFalse(flag_bool(word, True), word)


if __name__ == "__main__":
    unittest.main()
