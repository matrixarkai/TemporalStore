"""The meta a folded embedding leaves behind must not carry that row's own identity.

`embedding_meta` is 12.3% of the durable log, and 36.7% of its bytes were `row_key` -- the identity
of the context_embedding row that no longer exists once it is folded onto its owner. Every other
member of that dict holds one distinct value across a store, so this single unique field was what
kept the whole dict from being shared: 164 rows, 164 objects, 164 distinct values.

Two reasons it goes, and the second is the one that matters:

  it is unread -- no reader in any Python module outside the adapter, none in the Rust crates, and
  a runtime probe over read_all + retrieve never saw it asked for while `model` was asked for
  99,324 times;

  and it is wrong to carry. `record_with_embedding_defaults` copies every meta key onto an owner
  whose value is empty, with no exclusion list, so an owner without a top-level row_key inherits
  the RETIRED row's identity -- and `latest_value_record_key` prefers a stamped key over deriving
  one.
"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrixark_mcp_local_adapter as adapter_module


SECTIONS = 12


def _skill_text(i):
    out = ["# Runbook %d" % i, "", "A procedure for case %d." % i, ""]
    for s in range(SECTIONS):
        out += ["## Step %d" % s, "",
                "Check the queue depth for case %d step %d and drain it in order. Record the "
                "outcome against the case identifier." % (i, s), ""]
    return "\n".join(out)


class MetaDropsTheRetiredRowKey(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        store = Path(tempfile.mkdtemp())
        adapter = adapter_module.MatrixArkLocalAdapter(store / "events.jsonl")
        scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}
        for i in range(3):
            adapter.ingest({"kind": "skill", "scope": scope, "text": _skill_text(i),
                            "metadata": {"raw_uri": "file:///s/r-%d.md" % i, "title": "r-%d" % i}})
        adapter.close(timeout_s=300)
        cls.records = adapter_module.MatrixArkLocalAdapter(store / "events.jsonl").read_all()
        cls.metas = [r["embedding_meta"] for r in cls.records
                     if isinstance(r.get("embedding_meta"), dict)]

    def test_there_is_meta_to_examine(self):
        """Non-vacuity: with no embedding_meta anywhere, every assertion below passes emptily."""
        self.assertGreater(len(self.metas), 10,
                           "the fixture produced almost no embedding_meta")

    def test_the_meta_still_carries_the_model_identity(self):
        """A positive control, and a real requirement: the model guard reads these."""
        for field in ("model", "model_ref"):
            carrying = [m for m in self.metas if m.get(field)]
            self.assertTrue(
                carrying,
                "%s vanished from embedding_meta; the model-identity guard reads it" % field,
            )

    def test_no_meta_carries_the_retired_row_key(self):
        offenders = [m for m in self.metas if "row_key" in m]
        self.assertEqual(
            [], offenders[:3],
            "%d of %d embedding_meta values still carry the folded row's own identity"
            % (len(offenders), len(self.metas)),
        )

    def test_the_meta_is_shared_rather_than_one_object_per_row(self):
        """The point of removing it: one unique field kept the whole dict unshareable."""
        objects = {id(m) for m in self.metas}
        self.assertLess(
            len(objects), len(self.metas),
            "%d meta values are held as %d separate objects; nothing is shared"
            % (len(self.metas), len(objects)),
        )

    def test_no_owner_inherits_an_identity_from_its_embedding(self):
        """The reason this is a correctness fix and not only a size one."""
        for record in self.records:
            meta = record.get("embedding_meta")
            if isinstance(meta, dict) and meta.get("row_key"):
                stamped = record.get(adapter_module.ROW_KEY_FIELD)
                self.assertEqual(
                    stamped, meta["row_key"],
                    "a record carries a meta row_key that is not its own; self-repair would "
                    "hand it that identity",
                )


if __name__ == "__main__":
    unittest.main()
