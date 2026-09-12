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
    """No `subset is None` branch may read the whole store without announcing it.

    Written first as a literal match on the announcing lines, which broke the moment those branches
    were routed through a helper -- the invariant is "nothing reads everything silently", not "this
    exact text appears six times". Asserted as a property now: no branch goes straight to
    read_all(), and every one of them goes through the helper that counts.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    source = open(os.path.join(here, "matrixark_mcp_temporal_adapters.py"), encoding="utf-8").read()
    silent = "        if subset is None:\n            return self.read_all()"
    check(source.count(silent) == 0,
          "%d 'subset is None' branches still read the whole store silently" % source.count(silent))
    routed = source.count("return self._read_all_after_scoped_scan()")
    check(routed == 6, "expected 6 branches routed through the counting helper, found %d" % routed)
    check("_note_full_read_fallback(where)" in source,
          "the helper must still count the fallback it takes")


def _direct_backend_probe():
    """A direct backend far enough along to render its own metrics.

    `_backend_prometheus` lives on the mixin and `_ensure_backend_metric_fields` on the adapter
    that mixes it in, so neither alone can render. Borrowing the real initialiser keeps this
    honest: a hand-written stub would let the metric pass here while the shipped path skipped it.
    """
    import matrixark_mcp_temporal_adapters as adapters
    import matrixark_temporal_direct_backend as backend

    initialiser = None
    for name in dir(adapters):
        candidate = getattr(adapters, name, None)
        if isinstance(candidate, type) and "_ensure_backend_metric_fields" in candidate.__dict__:
            initialiser = candidate.__dict__["_ensure_backend_metric_fields"]
            break
    if initialiser is None:
        return None
    probe = object.__new__(backend._TemporalDirectBackendMixin)
    probe.__dict__["_adapters_for_test"] = sys.modules[backend._ADAPTERS_MODULE]
    probe._client = None
    probe._backend_ready = True
    probe._backend_label = lambda: "temporalstore-direct"
    # `_backend_prometheus` calls this itself, so binding it matters more than calling it once.
    probe._ensure_backend_metric_fields = lambda: initialiser(probe)
    initialiser(probe)
    return probe


def the_whole_store_fallback_reaches_the_metrics_surface():
    """Counting it was half the fix. The window this file exists for stayed invisible because the
    number was reported on no surface at all."""
    probe = _direct_backend_probe()
    if probe is None:
        check(False, "no class defines _ensure_backend_metric_fields, so this rendered nothing")
        return
    # Reported through the module object the BACKEND resolved, not the one this file imported.
    # Both spellings of matrixark_mcp_temporal_adapters load here, because the harness puts the
    # repository root and tools/ on the path, and each has its own counter. A deployment loads one
    # and the distinction does not arise; a test that ignores it watches an empty metric and
    # concludes the wiring is broken, which is exactly what happened while this was written.
    adapters = probe.__dict__["_adapters_for_test"]
    adapters._note_full_read_fallback("a_scan_that_gave_up")
    adapters._note_full_read_fallback("a_scan_that_gave_up")
    adapters._note_full_read_fallback("a_different_scan")
    rendered = probe._backend_prometheus()

    check("# TYPE matrixark_full_read_fallbacks_total counter" in rendered,
          "the fallback counter is not declared on the metrics surface")
    check('matrixark_full_read_fallbacks_total{backend="native",scan="a_scan_that_gave_up"} 2'
          in rendered,
          "the count for a named scan is missing or wrong:\n%s"
          % "\n".join(l for l in rendered.splitlines() if "full_read_fallbacks" in l))
    check('scan="a_different_scan"} 1' in rendered,
          "one series per scan site: which scan gave up is the part that says where to look")


def the_counter_is_what_is_being_read():
    """A positive control. A metric block built from a literal would pass the check above."""
    probe = _direct_backend_probe()
    if probe is None:
        return
    adapters = probe.__dict__["_adapters_for_test"]
    before = adapters.full_read_fallback_counts().get("a_control_scan", 0)
    rendered = probe._backend_prometheus()
    check('scan="a_control_scan"' not in rendered,
          "a scan nothing has reported is already on the surface, so this is not reading the "
          "counter")
    adapters._note_full_read_fallback("a_control_scan")
    rendered = probe._backend_prometheus()
    check('matrixark_full_read_fallbacks_total{backend="native",scan="a_control_scan"} %d'
          % (before + 1) in rendered,
          "reporting one more fallback did not change the surface")


for test in (
    a_named_fallback_is_counted_without_any_debug_log,
    an_unnamed_fallback_takes_the_calling_functions_name,
    counting_a_fallback_never_raises,
    the_detail_line_still_needs_the_debug_log,
    every_silent_branch_now_announces,
    the_whole_store_fallback_reaches_the_metrics_surface,
    the_counter_is_what_is_being_read,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for f in FAILURES:
        print("  - %s" % f)
    raise SystemExit(1)
print("all fallback-visibility checks pass")
