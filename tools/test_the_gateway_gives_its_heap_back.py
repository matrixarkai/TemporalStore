"""The gateway's heap trim must fire when it should and stay quiet when it should not.

A trimmer that never fires is worse than no trimmer, because the resident-memory graph looks the
same and the code says the problem is handled. So the threshold behaviour is asserted in both
directions, and the ledger is asserted to KEEP what it has not yet spent -- a trickle of small
responses retains as much as one large one, and a ledger that zeroed itself each tick would never
reach the threshold under exactly the workload that needs it most.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import matrixark_mcp_temporal_adapters as adapters  # noqa: E402


class _FakeLibc:
    def __init__(self):
        self.trims = 0

    def malloc_trim(self, _pad):
        self.trims += 1
        return 1


class GatewayHeapTrimTests(unittest.TestCase):
    def setUp(self):
        adapters._TRIM_LEDGER = 0
        adapters._TRIM_THREAD_STARTED = False
        self._saved_libc = adapters._LIBC_FOR_TRIM
        self._saved_looked = adapters._LIBC_LOOKED_UP
        self._saved_env = os.environ.get("MATRIXARK_GATEWAY_TRIM_BYTES")

    def tearDown(self):
        adapters._LIBC_FOR_TRIM = self._saved_libc
        adapters._LIBC_LOOKED_UP = self._saved_looked
        adapters._TRIM_LEDGER = 0
        if self._saved_env is None:
            os.environ.pop("MATRIXARK_GATEWAY_TRIM_BYTES", None)
        else:
            os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = self._saved_env

    def test_the_threshold_is_read_every_time(self):
        """An operator changing it mid-run must not find the old value cached."""
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = "1024"
        self.assertEqual(adapters._trim_threshold_bytes(), 1024)
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = "2048"
        self.assertEqual(adapters._trim_threshold_bytes(), 2048)

    def test_zero_switches_it_off(self):
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = "0"
        self.assertEqual(adapters._trim_threshold_bytes(), 0)
        adapters._note_payload_bytes(10 * 1024 * 1024)
        self.assertEqual(adapters._TRIM_LEDGER, 0, "an off trimmer must not even keep a ledger")
        self.assertFalse(adapters._TRIM_THREAD_STARTED, "an off trimmer must not start a thread")

    def test_an_unparsable_threshold_falls_back_rather_than_disabling(self):
        # Falling back to 0 would silently switch the trim off for a typo, which is the failure
        # mode that leaves memory unbounded while the setting looks present.
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = "not-a-number"
        self.assertEqual(adapters._trim_threshold_bytes(), 64 * 1024 * 1024)

    def test_the_ledger_accumulates_across_small_payloads(self):
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = str(1024 * 1024)
        for _ in range(4):
            adapters._note_payload_bytes(1000)
        self.assertEqual(adapters._TRIM_LEDGER, 4000)

    def test_below_the_threshold_nothing_is_trimmed_and_nothing_is_lost(self):
        """The quiet case, and the one that would hide a bug: the bytes must survive the tick."""
        libc = _FakeLibc()
        adapters._LIBC_FOR_TRIM = libc
        adapters._LIBC_LOOKED_UP = True
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = str(1024 * 1024)
        adapters._TRIM_LEDGER = 1000
        adapters._trim_once_for_test()
        self.assertEqual(libc.trims, 0, "trimmed below the threshold")
        self.assertEqual(adapters._TRIM_LEDGER, 1000, "the ledger was reset without trimming")

    def test_at_the_threshold_it_trims_and_resets(self):
        libc = _FakeLibc()
        adapters._LIBC_FOR_TRIM = libc
        adapters._LIBC_LOOKED_UP = True
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = str(1024 * 1024)
        adapters._TRIM_LEDGER = 1024 * 1024
        adapters._trim_once_for_test()
        self.assertEqual(libc.trims, 1)
        self.assertEqual(adapters._TRIM_LEDGER, 0)

    def test_a_platform_without_malloc_trim_does_not_break_the_caller(self):
        adapters._LIBC_FOR_TRIM = None
        adapters._LIBC_LOOKED_UP = True
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = str(1024)
        adapters._TRIM_LEDGER = 4096
        adapters._trim_once_for_test()  # must not raise

    def test_a_libc_that_raises_does_not_break_the_caller(self):
        class Angry:
            def malloc_trim(self, _pad):
                raise OSError("no")

        adapters._LIBC_FOR_TRIM = Angry()
        adapters._LIBC_LOOKED_UP = True
        os.environ["MATRIXARK_GATEWAY_TRIM_BYTES"] = str(1024)
        adapters._TRIM_LEDGER = 4096
        adapters._trim_once_for_test()  # housekeeping must never take the process down


if __name__ == "__main__":
    unittest.main(verbosity=2)
