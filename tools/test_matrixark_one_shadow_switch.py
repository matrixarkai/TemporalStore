"""One switch turns the shadow comparison on, and one names where it writes.

There were twelve: MATRIXARK_<OP>_SHADOW_COMPARE and MATRIXARK_<OP>_SHADOW_LOG for each of six
operations, all reading the same two lines and all doing the same thing -- run the narrowed read
and the full read and compare them. Nothing used the granularity: no test set any of them, nothing
outside that module mentioned them, and none was offered on the portal.

The per-operation DEFAULT log path is kept on purpose -- two reads are easier to compare when
their logs are separate files -- so what collapsed is the six ways to override it.
"""
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrixark_mcp_temporal_adapters as adapters


OPERATIONS = ("getall", "sessbuf", "refresh", "delete", "getmem", "prior_context")
GONE = ("GETALL", "SESSBUF", "REFRESH", "DELETE", "GETMEM", "PRIOR_CONTEXT")


class OneShadowSwitch(unittest.TestCase):
    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in
                       ("MATRIXARK_SHADOW_COMPARE", "MATRIXARK_SHADOW_LOG")}
        for key in self._saved:
            os.environ.pop(key, None)

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_it_is_off_unless_asked(self):
        """Non-vacuity: if it read as on with nothing set, the on-case below would prove nothing."""
        self.assertFalse(adapters._shadow_compare_enabled())

    def test_the_one_switch_turns_it_on(self):
        for value in ("1", "true", "yes", "on"):
            os.environ["MATRIXARK_SHADOW_COMPARE"] = value
            self.assertTrue(adapters._shadow_compare_enabled(), "%r should enable it" % value)
        for value in ("0", "false", "no", "off", ""):
            os.environ["MATRIXARK_SHADOW_COMPARE"] = value
            self.assertFalse(adapters._shadow_compare_enabled(), "%r should not enable it" % value)

    def test_each_operation_still_logs_to_its_own_file(self):
        """The point of keeping per-operation defaults: the six logs do not collide."""
        paths = {op: adapters._shadow_log_path(op) for op in OPERATIONS}
        self.assertEqual(
            len(set(paths.values())), len(OPERATIONS),
            "two operations share a default log path, so their comparisons would interleave: %r"
            % (paths,),
        )
        for op, path in paths.items():
            self.assertIn(op, path, "%s's default path does not name it: %s" % (op, path))

    def test_one_override_redirects_every_operation(self):
        os.environ["MATRIXARK_SHADOW_LOG"] = "/tmp/one-place.log"
        for op in OPERATIONS:
            self.assertEqual(adapters._shadow_log_path(op), "/tmp/one-place.log")

    def test_the_per_operation_switches_are_gone(self):
        source = Path(adapters.__file__).read_text(encoding="utf-8")
        # Positive control: the surviving names must be present, or a "not found" below would only
        # mean the search looked in the wrong place.
        self.assertIn("MATRIXARK_SHADOW_COMPARE", source)
        self.assertIn("MATRIXARK_SHADOW_LOG", source)
        for op in GONE:
            for suffix in ("SHADOW_COMPARE", "SHADOW_LOG"):
                name = "MATRIXARK_%s_%s" % (op, suffix)
                self.assertNotIn(
                    name, source,
                    "%s is still read; twelve switches for one diagnostic was the thing being "
                    "removed" % name,
                )


if __name__ == "__main__":
    unittest.main()
