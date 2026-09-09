"""A full read that goes back to the backend that just refused is not a fallback.

Measured 2026-09-09 on a native one-box: one request with the backend down did TEN whole-corpus
reads and returned 500 anyway. Every one of them called the same client that had refused the scan,
so none could have answered. These tests pin the difference between the two ways a scoped scan
returns None -- it could not be ASKED (full read is correct) versus it was asked and FAILED (the
full read goes back to the same backend).
"""
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matrixark_mcp_temporal_adapters as adapters  # noqa: E402

CLASS = adapters.MatrixArkTemporalStoreDirectAdapter
FAILURES = []


def check(condition, message):
    if not condition:
        FAILURES.append(message)


class Stub:
    """Only the methods under test, so no store or client is needed."""

    _read_all_after_scoped_scan = CLASS._read_all_after_scoped_scan
    _full_read_can_answer_offline = CLASS._full_read_can_answer_offline

    def __init__(self, hot_cache=False, cache=None):
        self._hot = hot_cache
        self._records_cache = cache
        self.read_all_calls = 0

    def python_hot_cache_enabled(self):
        return self._hot

    def read_all(self):
        self.read_all_calls += 1
        return [{"record_type": "context_event"}]


def a_scan_that_could_not_be_asked_still_reads_everything():
    """No scanner on the client means no error: the full read is the correct path and must run."""
    adapters._SCAN_STATE.last_error = None
    os.environ.pop("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", None)
    stub = Stub()
    out = stub._read_all_after_scoped_scan()
    check(stub.read_all_calls == 1, "the full read must still run when the scan was never asked")
    check(out == [{"record_type": "context_event"}], "it must return the records, got %r" % (out,))


def a_failed_scan_with_nowhere_offline_reraises_instead_of_reading_everything():
    """The retirement: no hot cache, no disk fallback, so the full read cannot answer."""
    adapters._SCAN_STATE.last_error = ConnectionRefusedError("backend down")
    os.environ.pop("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", None)
    stub = Stub()
    try:
        stub._read_all_after_scoped_scan()
        check(False, "a failed scan with no offline source must re-raise, not read the store")
    except ConnectionRefusedError as exc:
        check("backend down" in str(exc), "it must re-raise the SCAN's own error, got %r" % (exc,))
    check(stub.read_all_calls == 0,
          "the full read must not run at all, ran %d times" % stub.read_all_calls)


def a_failed_scan_still_reads_when_a_disk_fallback_is_configured():
    """The full read recovers from disk first, so there it can genuinely answer."""
    adapters._SCAN_STATE.last_error = ConnectionRefusedError("backend down")
    os.environ["MATRIXARK_TEMPORALSTORE_LOCAL_STORE"] = "/tmp/some-local-store"
    try:
        stub = Stub()
        stub._read_all_after_scoped_scan()
        check(stub.read_all_calls == 1,
              "a configured disk fallback must still get its full read")
    except Exception as exc:  # noqa: BLE001
        check(False, "it must not raise when a disk fallback exists: %r" % (exc,))
    finally:
        os.environ.pop("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", None)


def a_failed_scan_still_reads_when_a_warm_hot_cache_can_serve():
    adapters._SCAN_STATE.last_error = ConnectionRefusedError("backend down")
    os.environ.pop("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", None)
    stub = Stub(hot_cache=True, cache=[{"record_type": "context_event"}])
    try:
        stub._read_all_after_scoped_scan()
        check(stub.read_all_calls == 1, "a warm hot cache must still get its full read")
    except Exception as exc:  # noqa: BLE001
        check(False, "it must not raise when the hot cache holds records: %r" % (exc,))


def an_empty_hot_cache_cannot_serve_so_it_does_not_count():
    """Positive control for the test above: hot cache ON but holding nothing is not an offline
    source, and treating it as one would restore the futile read this change removes."""
    adapters._SCAN_STATE.last_error = ConnectionRefusedError("backend down")
    os.environ.pop("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", None)
    stub = Stub(hot_cache=True, cache=None)
    try:
        stub._read_all_after_scoped_scan()
        check(False, "an empty hot cache must not count as an offline source")
    except ConnectionRefusedError:
        check(stub.read_all_calls == 0, "and the full read must not run")


def one_threads_outage_does_not_decide_another_threads_fallback():
    """_SCAN_STATE is thread-local because ONE adapter serves every request."""
    adapters._SCAN_STATE.last_error = ConnectionRefusedError("this thread is down")
    os.environ.pop("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", None)
    seen = {}

    def other_thread():
        stub = Stub()
        try:
            stub._read_all_after_scoped_scan()
            seen["ok"] = stub.read_all_calls
        except Exception as exc:  # noqa: BLE001
            seen["raised"] = repr(exc)

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join()
    check(seen.get("ok") == 1,
          "a clean thread must read normally, not inherit another thread's outage: %r" % (seen,))


for test in (
    a_scan_that_could_not_be_asked_still_reads_everything,
    a_failed_scan_with_nowhere_offline_reraises_instead_of_reading_everything,
    a_failed_scan_still_reads_when_a_disk_fallback_is_configured,
    a_failed_scan_still_reads_when_a_warm_hot_cache_can_serve,
    an_empty_hot_cache_cannot_serve_so_it_does_not_count,
    one_threads_outage_does_not_decide_another_threads_fallback,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for item in FAILURES:
        print("  - %s" % item)
    raise SystemExit(1)
print("all checks pass")
