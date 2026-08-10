"""Self-contained tests for the Control State client.

These do not require a running TemporalStore: a small in-process mock RESP server
mirrors the control-state verbs closely enough to exercise the client's wire
protocol, connection pool, retry path, and every use-case method. Run with:

    python -m pytest sdk/python/tests/test_control_state.py     # or:
    python sdk/python/tests/test_control_state.py               # no pytest needed
"""

from __future__ import annotations

import os
import socket
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from temporalstore.control_state import ControlState, ControlStateConfig  # noqa: E402


# --------------------------------------------------------------------------- #
# Mock RESP server: one counter (Counter family) per key + a distinct-set
# (Distinct family) per key + a first/last store (Selection family, the FOL/
# SELECTION verbs). The client still sends the legacy HSETANDGET/FOLSET/... verbs,
# which remain valid aliases of the new COUNTER*/SELECTION*/DISTINCT* spellings.
# Windows/precision are ignored (the client always sends windows that cover the
# test writes), which is sufficient to validate client-side use-case logic.
# --------------------------------------------------------------------------- #
class _MockServer:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.dedup: set[tuple[str, str]] = set()
        self.changes: dict[str, set[str]] = {}
        self.fol: dict[str, str] = {}
        self._srv = socket.socket()
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(16)
        self.port = self._srv.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self._srv.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        f = conn.makefile("rb")
        while True:
            cmd = self._read_cmd(f)
            if cmd is None:
                break
            conn.sendall(self._encode(self._handle(cmd)))
        conn.close()

    @staticmethod
    def _read_cmd(f) -> list[str] | None:
        line = f.readline()
        if not line:
            return None
        n = int(line[1:])
        out = []
        for _ in range(n):
            f.readline()  # $len
            out.append(f.readline().rstrip(b"\r\n").decode())
        return out

    @staticmethod
    def _encode(reply) -> bytes:
        kind, val = reply
        if kind == ":":
            return b":%d\r\n" % val
        if kind == "+":
            return b"+%s\r\n" % val.encode()
        if kind == "-":
            return b"-%s\r\n" % val.encode()
        if kind == "$":
            if val is None:
                return b"$-1\r\n"
            b = val.encode()
            return b"$%d\r\n%s\r\n" % (len(b), b)
        raise AssertionError(kind)

    def _handle(self, c: list[str]):
        v = c[0].upper()
        if v == "HSETANDGET":
            key, amt = c[1], int(c[3])
            self.counts[key] = self.counts.get(key, 0) + amt
            return (":", self.counts[key])
        if v == "HSETANDGETOPT":
            key, amt, uid = c[1], int(c[3]), c[9]
            if uid and (key, uid) in self.dedup:
                return (":", self.counts.get(key, 0))
            if uid:
                self.dedup.add((key, uid))
            self.counts[key] = self.counts.get(key, 0) + amt
            return (":", self.counts[key])
        if v in ("CONTROLSTATEINCR", "CONTROLSTATEINCROPT"):
            key, amt = c[1], int(c[3])
            self.counts[key] = self.counts.get(key, 0) + amt
            return ("+", "OK")
        if v in ("HQUERY", "CONTROLSTATECOUNT"):
            return (":", self.counts.get(c[1], 0))
        if v == "CONTROLSTATECHANGE":
            self.changes.setdefault(c[1], set()).add(c[3])
            return ("+", "OK")
        if v == "CONTROLSTATEQUERY":
            key, agg = c[1], c[4]
            if agg == "change":
                return (":", len(self.changes.get(key, set())))
            return (":", self.counts.get(key, 0))
        if v == "FOLSET":
            self.fol.setdefault(c[1], c[2])  # first-wins for the test
            return ("+", "OK")
        if v == "FOLQUERY":
            return ("$", self.fol.get(c[1]))
        return ("-", f"ERR unknown {v}")


# --------------------------------------------------------------------------- #
# pytest fixture (falls back to a plain factory when pytest is absent)
# --------------------------------------------------------------------------- #
def _make_client():
    server = _MockServer()
    cs = ControlState(ControlStateConfig(host="127.0.0.1", port=server.port, tenant="t1"))
    return cs, server


try:
    import pytest

    @pytest.fixture()
    def cs():
        client, _server = _make_client()
        yield client
        client.close()
except ImportError:  # pragma: no cover - pytest optional
    pytest = None


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_frequency_cap_is_exact(cs):
    results = [cs.frequency_cap(user_id="u42", campaign_id="c1", limit=5) for _ in range(7)]
    assert [d.allowed for d in results] == [True, True, True, True, True, False, False]
    assert sum(d.allowed for d in results) == 5
    assert results[5].reason == "frequency_cap_exceeded"
    assert results[4].remaining == 0


def test_idempotent_ingest_dedups_replays(cs):
    evt = cs.new_event_id()
    counts = [cs.ingest_impression(user_id="u9", campaign_id="c1", limit=5, event_id=evt).count
              for _ in range(3)]
    assert counts == [1, 1, 1]
    assert cs.ingest_impression(user_id="u9", campaign_id="c1", limit=5,
                                event_id=cs.new_event_id()).count == 2


def test_tenant_quota_tracks_and_gates(cs):
    d = cs.tenant_quota(resource="api", limit=10, weight=3)
    assert d.count == 3 and d.allowed and d.remaining == 7
    d = cs.tenant_quota(resource="api", limit=10, weight=8)
    assert d.count == 11 and not d.allowed and d.reason == "quota_exceeded"


def test_pacing_multiplier(cs):
    cs.record_spend(campaign_id="c9", cost=1000)
    p = cs.pacing(campaign_id="c9", budget=5000, target_by_now=1500)
    assert p.spent == 1000 and p.pace == "under" and p.multiplier > 1.0


def test_rolling_suppression_fires_at_threshold(cs):
    seq = [cs.rolling_suppression(entity_kind="device", entity_id="d7",
                                  signal="login:failed", threshold=5) for _ in range(6)]
    assert seq[3].allowed and not seq[4].allowed
    assert seq[4].reason == "suppressed"


def test_rate_limit_sliding_window(cs):
    seq = [cs.rate_limit(name="k1", limit=3, window_ms=60_000) for _ in range(4)]
    assert [d.allowed for d in seq] == [True, True, True, False]
    assert seq[3].reason == "rate_limited"


def test_debounce_blocks_bursts(cs):
    first = cs.debounce(name="alert:x", quiet_ms=60_000)
    second = cs.debounce(name="alert:x", quiet_ms=60_000)
    assert first.allowed and not second.allowed and second.reason == "debounced"


def test_concurrency_limit_and_rollback(cs):
    a = cs.concurrency_acquire(resource="r", limit=2)
    b = cs.concurrency_acquire(resource="r", limit=2)
    c = cs.concurrency_acquire(resource="r", limit=2)
    assert a.allowed and b.allowed and not c.allowed
    assert c.reason == "concurrency_limit_exceeded"
    cs.concurrency_release(resource="r")
    assert cs.concurrency_acquire(resource="r", limit=2).allowed


def test_concurrency_slot_context_manager(cs):
    with cs.concurrency_slot(resource="pdf", limit=1) as slot:
        assert slot.allowed
    # slot released on exit -> can re-acquire
    assert cs.concurrency_acquire(resource="pdf", limit=1).allowed


def test_distinct_unique_count(cs):
    for dev in ("d1", "d2", "d1"):
        cs.add_unique(subject="u42:devices", value=dev)
    assert cs.unique_count(subject="u42:devices") == 2


def test_budget_gate_and_idempotent_charge(cs):
    g1 = cs.budget_gate(budget_id="c9", cost=1300, budget=5000)
    g2 = cs.budget_gate(budget_id="c9", cost=4000, budget=5000)
    assert g1.allowed and g1.remaining == 3700
    assert not g2.allowed and g2.reason == "budget_exhausted"
    evt = cs.new_event_id()
    x = cs.budget_gate(budget_id="c10", cost=100, budget=5000, event_id=evt)
    y = cs.budget_gate(budget_id="c10", cost=100, budget=5000, event_id=evt)
    assert x.count == y.count == 100  # replay does not double-charge


def test_eligibility_first_seen(cs):
    cs.mark_seen(subject="promoA:u42", value="2026-07-24", keep="first")
    assert cs.seen_value(subject="promoA:u42") == "2026-07-24"


# --------------------------------------------------------------------------- #
# Plain runner (no pytest required)
# --------------------------------------------------------------------------- #
def _run_all() -> None:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    for fn in tests:
        client, server = _make_client()
        try:
            fn(client)
            print(f"  ok   {fn.__name__}")
            passed += 1
        finally:
            client.close()
    print(f"{passed}/{len(tests)} passed")
    if passed != len(tests):
        raise SystemExit(1)


if __name__ == "__main__":
    _run_all()
