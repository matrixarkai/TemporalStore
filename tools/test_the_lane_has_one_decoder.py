"""The lane has one decoder, and both modules that serve the lane use it.

A profile put 53.5% of gateway CPU in the stdlib JSON decoder. orjson was wired into
`matrixark_mcp_temporal_adapters` and measured at -14.1% gateway CPU per message -- but
`matrixark_mcp_rust_proxy_client`, which is the client the one-box HTTP path uses, went on calling
`json.loads`, including for the whole context pack on every retrieve. Two modules serve the lane;
the faster parser reached one; the profile kept naming the decoder that was supposedly replaced.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

from matrixark_json_lane import LANE_DECODER, lane_loads  # noqa: E402

FAILURES = []


def check(condition, message):
    if not condition:
        FAILURES.append(message)


def the_decoder_reads_both_str_and_bytes():
    """Both parsers accept both; the stdlib branch has to decode bytes itself."""
    check(lane_loads('{"a": 1}') == {"a": 1}, "str input failed")
    check(lane_loads(b'{"a": 1}') == {"a": 1}, "bytes input failed")
    check(lane_loads('[1, 2]') == [1, 2], "array failed")


def orjson_is_used_when_it_is_installed():
    """The whole point is the faster parser. If orjson is present and we are not using it, the
    measured win is not being taken."""
    try:
        import orjson  # noqa: F401
    except ImportError:
        print("    (orjson absent here; stdlib branch is correct)")
        return
    check(LANE_DECODER == "orjson", "orjson is installed but the lane reports %r" % LANE_DECODER)


def both_lane_modules_share_one_decoder():
    """The property that was missing. Same function object, not merely similar code."""
    import matrixark_json_lane
    try:
        from matrixark_mcp_rust_proxy_client import _lane_loads as client_loads
    except Exception as exc:  # noqa: BLE001
        FAILURES.append("could not import the client's decoder: %r" % (exc,))
        return
    check(
        client_loads is matrixark_json_lane.lane_loads,
        "the client uses a different decoder object from the shared one",
    )


def the_client_does_not_decode_the_lane_with_the_stdlib_parser():
    """Source-level, because an import can be shadowed by a later local call."""
    body = open(os.path.join(HERE, "matrixark_mcp_rust_proxy_client.py"), encoding="utf-8").read()
    hits = re.findall(r"^\s*\w*\s*=?\s*json\.loads\(", body, re.M)
    check(not hits, "the client still calls stdlib json.loads on the lane: %d site(s)" % len(hits))


def the_context_pack_is_decoded_by_the_shared_decoder():
    """The hot one: the pack is the largest payload on the retrieve path."""
    body = open(os.path.join(HERE, "matrixark_mcp_rust_proxy_client.py"), encoding="utf-8").read()
    check(
        "decoded = _lane_loads(value)" in body,
        "the context pack is no longer decoded by the shared decoder",
    )


for test in (
    the_decoder_reads_both_str_and_bytes,
    orjson_is_used_when_it_is_installed,
    both_lane_modules_share_one_decoder,
    the_client_does_not_decode_the_lane_with_the_stdlib_parser,
    the_context_pack_is_decoded_by_the_shared_decoder,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for item in FAILURES:
        print("  - %s" % item)
    raise SystemExit(1)
print("all lane-decoder checks pass")
