"""A skill manifest's preview must be a preview, not a copy of the skill.

`clip_context_text` defaults to MAX_CONTEXT_REF_CHARS (4096) -- the size a served context REF may
reach, because a ref carries content. A manifest preview is not a ref, and at that default it was
3,096 characters per skill.

Why it matters more than it looks: the index costs a FLAT ~11 KB per document whatever the
document's size (measured at 8 KB, 40 KB, 200 KB and 806 KB per document -- 0.05 MB of index in
every case), and this field was 29% of it. On small skills that is around 10% of everything stored
at production embedding width; on 1 MB documents it disappears into the denominator.

It is trimmed rather than removed because matrixark_http falls back to it for display.
"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
# The adapter first: importing the ingest mixin on its own trips the cycle between them
# (matrixark_local_adapter_ingest imports tools.matrixark_mcp_local_adapter, which imports the
# mixin back), and the failure reads as a missing 'tools' package rather than as an ordering bug.
import matrixark_mcp_local_adapter as adapter_module
import matrixark_local_adapter_ingest as ingest_module


def _skill_text(i, sections=60):
    out = ["# Runbook %d" % i, "", "A procedure for case %d." % i, ""]
    for s in range(sections):
        out += ["## Step %d" % s, "",
                "Check the queue depth for case %d step %d and drain it in order. Record the "
                "outcome against the case identifier." % (i, s), ""]
    return "\n".join(out)


class ManifestPreviewIsPreviewSized(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        store = Path(tempfile.mkdtemp())
        adapter = adapter_module.MatrixArkLocalAdapter(store / "events.jsonl")
        scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}
        cls.source = {}
        for i in range(3):
            text = _skill_text(i)
            cls.source[i] = text
            adapter.ingest({"kind": "skill", "scope": scope, "text": text,
                            "metadata": {"raw_uri": "file:///s/r-%d.md" % i, "title": "r-%d" % i}})
        adapter.close(timeout_s=300)
        records = adapter_module.MatrixArkLocalAdapter(store / "events.jsonl").read_all()
        cls.manifests = [r for r in records
                         if str(r.get("record_type") or "") == "skill_manifest"]

    def test_there_are_manifests_to_examine(self):
        """Non-vacuity: with no manifest, every assertion below passes emptily."""
        self.assertGreaterEqual(len(self.manifests), 3)

    #: Read with a default so this file also runs against a build that has no such constant --
    #: the bound below then fails on its own claim, which is the only way to know it discriminates.
    LIMIT = getattr(ingest_module, "SKILL_PREVIEW_CHARS", 260)

    def test_the_source_is_long_enough_to_be_clipped(self):
        """Also non-vacuity: a short skill would satisfy the bound without anything being clipped."""
        longest = max(len(t) for t in self.source.values())
        self.assertGreater(
            longest, self.LIMIT * 4,
            "the fixture's skills are too short to show a clip happening",
        )

    def test_the_preview_is_bounded(self):
        limit = self.LIMIT
        for manifest in self.manifests:
            preview = manifest.get("text_preview") or ""
            self.assertLessEqual(
                len(preview), limit + 32,          # room for the truncation marker
                "a manifest preview is %d chars against a %d-char budget; it is carrying the "
                "skill, not a preview of it" % (len(preview), limit),
            )

    def test_the_preview_is_still_there(self):
        """It is trimmed, not removed -- matrixark_http falls back to it for display."""
        carrying = [m for m in self.manifests if (m.get("text_preview") or "").strip()]
        self.assertEqual(
            len(carrying), len(self.manifests),
            "a manifest lost its preview entirely; the display fallback reads this field",
        )

    def test_the_preview_still_starts_the_skill(self):
        """A preview that no longer previews the thing would be worse than a long one."""
        for manifest in self.manifests:
            preview = (manifest.get("text_preview") or "").strip()
            self.assertTrue(
                preview.startswith("# Runbook"),
                "the preview does not begin with the skill's own opening: %r" % preview[:40],
            )


if __name__ == "__main__":
    unittest.main()
