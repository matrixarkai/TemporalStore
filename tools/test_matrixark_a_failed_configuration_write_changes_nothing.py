#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A configuration write that fails leaves the process as it was.

`update()` used to put the new settings into `os.environ` and then persist them. When the persist
failed -- a read-only mount, a full disk, a permission the container does not have -- the route
answered 500 `config_write_failed` and the operator read that nothing had happened. The running
process had in fact changed: the new extraction endpoint, the new embedding host, the new API key
were all live. A restart then silently reverted them, so the deployment behaved one way until it
next restarted and another way after, with nothing on record either way.

The same shape as revoking a key before authorizing the revocation, and as rendering a page into a
file already opened for writing: the step that cannot be undone ran before the step that can fail.

Applying is the half that cannot fail, so it goes last. "The call failed" now means nothing
changed, and "the call succeeded" means both halves did.

The ordering is asserted from inside the persist step rather than from its outcome, because an
outcome cannot tell "applied after" from "applied before and then rolled back" -- and only one of
those is what the code does.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

import matrixark_gateway_config as cfg

KEY = "extraction.model"
ENV = "MATRIXARK_EXTRACTION_MODEL"


class AFailedWriteChangesNothingTest(unittest.TestCase):

    def setUp(self) -> None:
        # Both the environment and the config path are process-global, so both are restored
        # however this test ends.
        self._saved = dict(os.environ)
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(self._saved)))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(tmp.name, "runtime.json")
        os.environ.pop(ENV, None)

    # ---- the control ----------------------------------------------------------------------------

    def test_a_write_that_succeeds_applies_and_persists(self) -> None:
        """Without this, a change that simply stopped writing anything would pass the rest."""
        result = cfg.update({KEY: "deepseek-chat"})
        self.assertEqual("ok", result["status"])
        self.assertEqual("deepseek-chat", os.environ.get(ENV))
        self.assertEqual("deepseek-chat", cfg.load()["values"][KEY])

    # ---- the failure ------------------------------------------------------------------------------

    def test_a_write_that_cannot_persist_leaves_the_process_alone(self) -> None:
        os.environ[ENV] = "before"
        with mock.patch.object(cfg, "_store", side_effect=OSError("read-only file system")):
            with self.assertRaises(OSError):
                cfg.update({KEY: "deepseek-chat"})
        self.assertEqual("before", os.environ.get(ENV),
                         "the write failed and the running process changed anyway")

    def test_the_environment_is_untouched_at_the_moment_of_persisting(self) -> None:
        """Asserted from inside `_store`, because an outcome cannot tell "applied afterwards" from
        "applied first and then put back", and only one of those is what this does."""
        os.environ[ENV] = "before"
        seen = {}
        real = cfg._store

        def watching(document):
            seen["env"] = os.environ.get(ENV)
            return real(document)

        with mock.patch.object(cfg, "_store", side_effect=watching):
            cfg.update({KEY: "deepseek-chat"})

        self.assertEqual("before", seen.get("env"),
                         "the environment already carried the new value while the file was being "
                         "written, so a failed write would leave the two disagreeing")
        self.assertEqual("deepseek-chat", os.environ.get(ENV),
                         "and it must be applied once the write has succeeded")

    def test_what_was_persisted_is_what_takes_effect(self) -> None:
        cfg.update({KEY: "deepseek-chat"})
        self.assertEqual(cfg.load()["values"][KEY], os.environ.get(ENV))

    # ---- the one input that would make applying fail ------------------------------------------------

    def test_a_value_the_environment_cannot_hold_is_refused_before_either_half(self) -> None:
        os.environ[ENV] = "before"
        with self.assertRaises(cfg.InvalidValue):
            cfg.update({KEY: "deep\x00seek"})
        self.assertEqual("before", os.environ.get(ENV))
        self.assertNotIn(KEY, cfg.load().get("values") or {},
                         "a value that cannot be applied was written to the file anyway")


if __name__ == "__main__":
    unittest.main()
