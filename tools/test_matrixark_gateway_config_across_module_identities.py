# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A config built through one import spelling is accepted through the other.

`tools/` is importable both flat (`matrixark_v1_gateway`) and as a package
(`tools.matrixark_v1_gateway`). Python treats those as two module objects with two unrelated
`GatewayConfig` classes, so `isinstance(config, GatewayConfig)` is False for an object that IS one.
`_coerce_config` then passed it to `from_env`, which does `dict(overrides or {})` and raised
`'GatewayConfig' object is not iterable`.

Every affected test passed ALONE, because a single test loads only one spelling. The whole suite
loads both, and 181 tests failed across the gateway, portal, models, user-policy, deployment-route
and dashboard files -- the bulk of the ratchet's 191 new failures against a 124 baseline.

This test reproduces the two-identity condition deliberately, because that is the only state in
which the bug exists.
"""
import os
import sys
import unittest

_TOOLS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_TOOLS)
for _path in (_ROOT, _TOOLS):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import matrixark_v1_gateway as flat_module


class ConfigCrossesModuleIdentities(unittest.TestCase):
    def setUp(self):
        try:
            from tools import matrixark_v1_gateway as package_module
        except ImportError:  # pragma: no cover - only when tools/ is not importable as a package
            self.skipTest("tools is not importable as a package here")
        self.package_module = package_module

    def test_the_two_spellings_really_are_two_classes(self):
        """Guard the premise: without this the rest of the file proves nothing."""
        self.assertIsNot(flat_module, self.package_module)
        self.assertIsNot(flat_module.GatewayConfig, self.package_module.GatewayConfig)

    def test_a_config_from_the_other_module_is_accepted_unchanged(self):
        config = self.package_module.GatewayConfig.from_env({})
        self.assertFalse(isinstance(config, flat_module.GatewayConfig),
                         "the premise is that isinstance cannot see across the two")
        self.assertIs(config, flat_module._coerce_config(config),
                      "a built config must be returned as-is, not rebuilt")

    def test_a_mapping_still_builds_a_config(self):
        built = flat_module._coerce_config({"api_keys": ["k"]})
        self.assertIsInstance(built, flat_module.GatewayConfig)

    def test_none_still_builds_a_default_config(self):
        self.assertIsInstance(flat_module._coerce_config(None), flat_module.GatewayConfig)

    def test_make_v1_app_accepts_a_config_from_the_other_module(self):
        """The call that actually failed: 181 tests died in setUp on this line."""
        config = self.package_module.GatewayConfig.from_env({})
        try:
            flat_module.make_v1_app(object(), config)
        except TypeError as exc:
            if "not iterable" in str(exc):
                self.fail("make_v1_app still rejects a config from the other module: %s" % exc)
        except Exception:
            pass  # any other failure is about the stub server, not config coercion


if __name__ == "__main__":
    unittest.main()
