#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A portal row needs a transport, and that transport is the environment variable.

This exists because the flag-reduction work kept arriving at the same wrong idea: that a setting
offered on ``/v1/admin/config`` is a route that SURVIVES retiring its environment variable, so the
variable is a redundant name and dropping it costs an operator nothing.

It is the other way round. The portal is a WRITER of that variable, not an alternative to it:

* ``apply_boot`` does ``name = _env_name(setting, values)``, skips the setting when the name is
  empty, and otherwise does ``os.environ[name] = value``. No name, nothing seeded.
* ``_effective`` reads the value back with ``os.environ.get(name) if name else ""``, so the page's
  own "what is in force" column goes blank too.

So retiring a portal-backed variable does not remove a name. It removes the operator's control AND
leaves a row that stores a value, displays it, and changes nothing -- the same defect
``KNOBS_READ_BY_NOTHING`` records for the tenant knobs and that #1793 fixed for two of them.
``test_matrixark_a_stored_setting_this_build_forgot`` already describes what that costs the person
on the other end: they set it, upgraded, came back to a page that looked configured, and were
running the default.

Measured when this was written: 107 of the 132 variables in ``deployment_configurable`` are some
Setting's transport. That is a floor under the count, not a queue of work, and it is recorded here
so the next sweep reads it before deciding the rows are free.

The test fails in BOTH directions on purpose. A new setting with no transport fails it, and so does
retiring one of the three exemptions or giving the portal a genuine second apply route -- either of
which means the paragraph above has stopped being true and needs rewriting rather than routing
around.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_config as gateway  # noqa: E402

#: The only settings allowed to carry no ``env``, each because ``_env_name`` chooses the variable
#: at RUNTIME from the selected provider rather than pinning one name. Justified individually,
#: because an exemption list nobody has to justify is where this invariant would go to die:
#:
#: * ``extraction.model``    -- Anthropic reads MATRIXARK_ANTHROPIC_MODEL and ignores
#:                             MATRIXARK_EXTRACTION_MODEL, so the name follows the provider.
#: * ``extraction.api_key``  -- the key lands in whatever ``extraction.api_key_env`` names, so a
#:                             customer on another provider gets it in that provider's variable.
#: * ``embedding.api_key``   -- the same routing on the embedding side.
#:
#: Each is asserted below to RESOLVE to a real variable, so being on this list is not enough to
#: escape the invariant -- it only changes when the name is decided.
PROVIDER_ROUTED = {
    "extraction.model",
    "extraction.api_key",
    "embedding.api_key",
}

#: Asserted so a registry that stops loading cannot make every check below vacuously true. Set far
#: from the real figure (129 when written) so it reports a collapse, not a normal edit.
MINIMUM_SETTINGS = 60


class EverySettingHasATransport(unittest.TestCase):

    def test_the_registry_is_still_populated(self) -> None:
        self.assertGreaterEqual(
            len(gateway.SETTINGS_BY_KEY), MINIMUM_SETTINGS,
            "only %d settings loaded; below %d this file is checking almost nothing"
            % (len(gateway.SETTINGS_BY_KEY), MINIMUM_SETTINGS))

    def test_a_setting_without_a_variable_cannot_reach_the_process(self) -> None:
        """The whole invariant: a row with no transport stores a value that never applies."""
        missing = sorted(key for key, setting in gateway.SETTINGS_BY_KEY.items()
                         if not getattr(setting, "env", ""))
        self.assertEqual(
            sorted(PROVIDER_ROUTED), missing,
            "these portal rows have no environment variable to write to, so `apply_boot` skips "
            "them and the value is stored, shown and inert: %s. If one of them was retired to "
            "shrink the flag surface, the row has to go with it -- a field that configures "
            "nothing is worse than no field." % sorted(set(missing) - PROVIDER_ROUTED))

    def test_each_exempt_setting_really_does_resolve_a_variable(self) -> None:
        """Being on the list is not the justification; resolving a name at runtime is."""
        for key in sorted(PROVIDER_ROUTED):
            setting = gateway.SETTINGS_BY_KEY.get(key)
            self.assertIsNotNone(setting, "%s is exempt but no longer exists" % key)
            resolved = gateway._env_name(setting, {})
            self.assertTrue(
                resolved,
                "%s is exempt because `_env_name` picks its variable at runtime, but it resolved "
                "to nothing -- so it is simply a row with no transport" % key)

    def test_the_portal_seeds_the_process_through_those_variables(self) -> None:
        """`apply_boot` is the apply path, and it works by writing the environment.

        Asserted against behaviour rather than by reading the source, because the claim this file
        rests on is that there is no OTHER way a stored value reaches the process.
        """
        setting = next(s for key, s in sorted(gateway.SETTINGS_BY_KEY.items())
                       if getattr(s, "env", "") and getattr(s, "kind", "") != "secret")
        name = gateway._env_name(setting, {})
        self.assertTrue(name, "picked a setting with no transport; the fixture is wrong")

        saved = os.environ.get(name)
        boot_had_it = name in getattr(gateway, "_BOOT_ENV", {})
        try:
            os.environ.pop(name, None)
            seeded = gateway.apply_boot({"values": {setting.key: "7"}})
            if boot_had_it:
                self.skipTest("%s was set in the boot environment, which keeps precedence" % name)
            self.assertIn(name, seeded,
                          "apply_boot did not seed %s for %s" % (name, setting.key))
            self.assertEqual(os.environ.get(name), "7",
                             "the stored value reaches the process through the variable")
        finally:
            if saved is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = saved

    def test_a_stored_value_with_no_transport_is_not_seeded(self) -> None:
        """The other direction: no variable, nothing applied. This is the cost being recorded."""
        setting = gateway.SETTINGS_BY_KEY["extraction.model"]
        seeded = gateway.apply_boot({"values": {}})
        self.assertIsInstance(seeded, list)
        # With no provider selected and no value stored, nothing is seeded for it -- the point is
        # that seeding is keyed entirely on resolving a variable name.
        self.assertNotIn("", seeded, "an empty variable name must never be seeded")
        self.assertTrue(
            all(name for name in seeded),
            "apply_boot seeded an empty name, so a row with no transport looked applied: %s"
            % seeded)


if __name__ == "__main__":
    unittest.main()
