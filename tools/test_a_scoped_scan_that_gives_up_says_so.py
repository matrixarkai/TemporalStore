"""A scoped scan that gives up and reads the whole store must say so.

Measured 2026-09-09: a degraded window did TEN whole-corpus reads and wrote nothing anywhere. Two
reasons, both fixed here and both asserted below -- the notifier returned early unless a debug log
was configured, and the `subset is None` branch (the one that actually fires when the backend
cannot answer) never called the notifier at all.
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from matrixark_mcp_temporal_adapters import (  # noqa: E402
    _note_full_read_fallback,
    full_read_fallback_counts,
)

FAILURES = []


def check(condition, message):
    if not condition:
        FAILURES.append(message)


def a_named_fallback_is_counted_without_any_debug_log():
    os.environ.pop("MATRIXARK_MCP_DEBUG_LOG", None)
    before = full_read_fallback_counts().get("records_for_session_buffer", 0)
    _note_full_read_fallback("records_for_session_buffer", RuntimeError("backend away"))
    after = full_read_fallback_counts().get("records_for_session_buffer", 0)
    check(after == before + 1,
          "counting must not depend on a debug log: %d -> %d" % (before, after))


def an_unnamed_fallback_takes_the_calling_functions_name():
    """The `subset is None` branches call this with no arguments at all."""
    def records_for_a_made_up_thing():
        _note_full_read_fallback()

    before = full_read_fallback_counts().get("records_for_a_made_up_thing", 0)
    records_for_a_made_up_thing()
    after = full_read_fallback_counts().get("records_for_a_made_up_thing", 0)
    check(after == before + 1,
          "an unnamed fallback must be attributed to its caller, got %d -> %d" % (before, after))


def counting_a_fallback_never_raises():
    """A channel that reports a problem must not become one."""
    try:
        _note_full_read_fallback(None, None)
    except Exception as exc:  # noqa: BLE001
        check(False, "the notifier raised: %r" % (exc,))


def the_detail_line_still_needs_the_debug_log():
    """The positive control: without this, 'counts always' could mean 'writes always' too, and the
    detail line would start appearing in deployments that never asked for it."""
    os.environ.pop("MATRIXARK_MCP_DEBUG_LOG", None)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "debug.log")
        _note_full_read_fallback("quiet_case", None)
        check(not os.path.exists(path), "no debug log configured, so nothing may be written")

        os.environ["MATRIXARK_MCP_DEBUG_LOG"] = path
        try:
            _note_full_read_fallback("loud_case", None)
            check(os.path.exists(path), "with the log configured the line must be written")
            body = open(path, encoding="utf-8").read()
            check("loud_case" in body, "the line must name the caller, got %r" % body)
            check("scoped scan unavailable" in body,
                  "a fallback with no exception must still say why, got %r" % body)
        finally:
            os.environ.pop("MATRIXARK_MCP_DEBUG_LOG", None)


def every_silent_branch_now_announces():
    """The six `subset is None` branches must each call the notifier before reading everything."""
    here = os.path.dirname(os.path.abspath(__file__))
    source = open(os.path.join(here, "matrixark_mcp_temporal_adapters.py"), encoding="utf-8").read()
    silent = "        if subset is None:\n            return self.read_all()"
    check(source.count(silent) == 0,
          "%d 'subset is None' branches still read the whole store silently" % source.count(silent))
    announced = "_note_full_read_fallback()\n            return self.read_all()"
    check(source.count(announced) == 6,
          "expected 6 announcing branches, found %d" % source.count(announced))


for test in (
    a_named_fallback_is_counted_without_any_debug_log,
    an_unnamed_fallback_takes_the_calling_functions_name,
    counting_a_fallback_never_raises,
    the_detail_line_still_needs_the_debug_log,
    every_silent_branch_now_announces,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for f in FAILURES:
        print("  - %s" % f)
    raise SystemExit(1)
print("all fallback-visibility checks pass")
