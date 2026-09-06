"""The two largest constants on the wire must ride in the intern bundle, and survive the trip.

`access_scope` was 9.6% of the durable log and `deployment_scope` a further 1.2%, each carrying
exactly ONE distinct value across the store. Interning them removes both from every data line and
keeps one copy per distinct value.

The number to watch is the SIDECAR count, not the byte count: fields share one bundle, so a field
that varies multiplies tokens rather than removing bytes. A constant adds no combinations, and the
last test here pins that.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrixark_mcp_local_adapter as adapter_module


SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}


def _rows(count):
    return [
        {"record_type": "skill_section",
         "node_id": "n-%d" % i,
         "text": "section %d" % i,
         "access_scope": {"tenant_hash": 11, "user_hash": 22, "scope_key": "t=11|u=22|s=1|"},
         "deployment_scope": "global",
         "storage_route": "local",
         "storage_options": {"durable": True}}
        for i in range(count)
    ]


def _log_lines(path):
    """Every non-blank line of the durable log, whichever form it is written in.

    `read_text` asserts the log is text. It is, until MATRIXARK_LOCAL_JSONL_BLOCK_LOG is on, and
    then it is a stream of compressed blocks. The module's own reader takes either form, and a test
    about durable RECORDS should not depend on the encoding they arrive in.
    """
    try:
        from tools.matrixark_mcp_local_adapter import _iter_shard_lines
    except ImportError:  # Direct script execution from tools/.
        from matrixark_mcp_local_adapter import _iter_shard_lines
    return [line for line in _iter_shard_lines(path) if line.strip()]


class TheTwoLargestConstantsAreInterned(unittest.TestCase):
    def _encode(self, rows):
        emitted = set()
        return adapter_module.encode_interned_records(rows, emitted)

    def test_the_fields_are_in_the_intern_list(self):
        for field in ("access_scope", "deployment_scope"):
            self.assertIn(
                field, adapter_module.INTERN_METADATA_FIELDS,
                "%s is the largest constant on the wire and is still written on every row" % field,
            )

    def test_they_leave_the_data_line(self):
        encoded = self._encode(_rows(20))
        data = [r for r in encoded
                if str(r.get("record_type") or "") != adapter_module.INTERN_DICT_RECORD_TYPE]
        self.assertEqual(len(data), 20)
        for record in data:
            self.assertNotIn("access_scope", record)
            self.assertNotIn("deployment_scope", record)
            self.assertIn(adapter_module.INTERN_BUNDLE_TOKEN_KEY, record)

    def test_a_read_expands_them_back_unchanged(self):
        """The whole contract: interning must be invisible above the log."""
        rows = _rows(20)
        originals = [(r["access_scope"], r["deployment_scope"]) for r in rows]
        expanded = adapter_module.expand_interned_records(self._encode(rows))
        data = [r for r in expanded
                if str(r.get("record_type") or "") != adapter_module.INTERN_DICT_RECORD_TYPE]
        self.assertEqual(len(data), len(originals))
        for (scope, deployment), record in zip(originals, data):
            self.assertEqual(record["access_scope"], scope)
            self.assertEqual(record["deployment_scope"], deployment)

    def test_a_constant_adds_no_sidecars(self):
        """A field that varies would multiply the bundles. These do not vary."""
        encoded = self._encode(_rows(200))
        sidecars = [r for r in encoded
                    if str(r.get("record_type") or "") == adapter_module.INTERN_DICT_RECORD_TYPE]
        self.assertEqual(
            len(sidecars), 1,
            "200 rows carrying one distinct scope produced %d sidecars; a field that varies has "
            "been added to the bundle" % len(sidecars),
        )

    def test_it_survives_a_real_write_and_read(self):
        """End to end through the adapter, not just the codec."""
        store = Path(tempfile.mkdtemp())
        adapter = adapter_module.MatrixArkLocalAdapter(store / "events.jsonl")
        adapter.ingest({"kind": "skill", "scope": SCOPE, "text": "a runbook section",
                        "metadata": {"raw_uri": "file:///s/r.md", "title": "r"}})
        adapter.close(timeout_s=120)

        raw = [json.loads(line) for line in
               _log_lines(store / "events.jsonl") if line.strip()]
        data = [r for r in raw
                if str(r.get("record_type") or "") != adapter_module.INTERN_DICT_RECORD_TYPE]
        self.assertTrue(data, "nothing was written, so the assertions below would pass emptily")
        inline = [r for r in data if "access_scope" in r]
        self.assertEqual(
            inline, [],
            "%d rows still carry access_scope inline on the durable log" % len(inline),
        )

        served = adapter_module.MatrixArkLocalAdapter(store / "events.jsonl").read_all()
        with_scope = [r for r in served if isinstance(r.get("access_scope"), dict)]
        self.assertTrue(
            with_scope,
            "no served record carries access_scope, so interning it lost the field",
        )


if __name__ == "__main__":
    unittest.main()
