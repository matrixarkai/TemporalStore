# SPDX-License-Identifier: Apache-2.0
"""Input discovery, resume state, and identity for the batch skill ingester.

The behaviours worth pinning are the ones a customer's long-running import depends on: that the same
directory always yields the same ordered list, that a manifest can be maintained by hand (comments,
blanks, JSON Lines), that a document's identity is stable across runs so re-ingesting replaces rather
than duplicates, and that a damaged state file degrades to "do the work again" instead of failing.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_batch_ingest as batch  # noqa: E402


class DiscoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix="batch-ingest-test-")
        os.makedirs(os.path.join(self.root, "acme"))
        for name in ("acme/checkout.md", "acme/returns.markdown", "pricing.json", "notes.txt"):
            path = os.path.join(self.root, name)
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("# " + name)

    def test_default_globs_take_documents_and_skip_everything_else(self) -> None:
        found = batch.discover_from_dir(self.root, batch.DEFAULT_GLOBS)
        names = sorted(os.path.basename(path) for path in found)
        self.assertEqual(names, ["checkout.md", "pricing.json", "returns.markdown"])
        self.assertNotIn("notes.txt", names)

    def test_discovery_is_sorted_so_reruns_are_reproducible(self) -> None:
        self.assertEqual(
            batch.discover_from_dir(self.root, batch.DEFAULT_GLOBS),
            sorted(batch.discover_from_dir(self.root, batch.DEFAULT_GLOBS)),
        )

    def test_an_explicit_glob_narrows_the_selection(self) -> None:
        found = batch.discover_from_dir(self.root, ["*.json"])
        self.assertEqual([os.path.basename(p) for p in found], ["pricing.json"])


class ManifestTest(unittest.TestCase):
    def _manifest(self, text: str) -> str:
        handle = tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8")
        handle.write(text)
        handle.close()
        return handle.name

    def test_plain_list_ignores_blanks_and_comments(self) -> None:
        path = self._manifest("# playbooks\n\na/one.md\n\n  b/two.json  \n# trailing\n")
        self.assertEqual(batch.discover_from_manifest(path), ["a/one.md", "b/two.json"])

    def test_json_lines_manifest_reads_the_path_field(self) -> None:
        path = self._manifest(
            json.dumps({"path": "a/one.md"}) + "\n" + json.dumps({"file": "b/two.json"}) + "\n"
        )
        self.assertEqual(batch.discover_from_manifest(path), ["a/one.md", "b/two.json"])

    def test_a_json_line_without_a_path_is_a_clear_error(self) -> None:
        path = self._manifest(json.dumps({"merchant": "acme"}) + "\n")
        with self.assertRaises(SystemExit):
            batch.discover_from_manifest(path)


class IdentityAndStateTest(unittest.TestCase):
    def test_identity_is_stable_and_absolute_so_reingest_replaces(self) -> None:
        first = batch.identity_key_for("playbooks/acme/checkout.md")
        second = batch.identity_key_for("./playbooks/acme/checkout.md")
        self.assertEqual(first, second)
        self.assertTrue(first.startswith("skill:"))

    def test_distinct_documents_get_distinct_identities(self) -> None:
        self.assertNotEqual(
            batch.identity_key_for("a/one.md"), batch.identity_key_for("a/two.md")
        )

    def test_state_round_trips(self) -> None:
        path = os.path.join(tempfile.mkdtemp(), "state.json")
        batch.save_state(path, {"skill:/a/one.md": "http 202"})
        self.assertEqual(batch.load_state(path), {"skill:/a/one.md": "http 202"})

    def test_a_corrupt_state_file_degrades_to_redoing_the_work(self) -> None:
        path = os.path.join(tempfile.mkdtemp(), "state.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        # Empty state means "nothing known done" -- safe, because ingest is a keyed upsert.
        self.assertEqual(batch.load_state(path), {})

    def test_a_missing_state_file_is_not_an_error(self) -> None:
        self.assertEqual(batch.load_state("/nonexistent/state.json"), {})
        self.assertEqual(batch.load_state(None), {})


class ResourceTypeTest(unittest.TestCase):
    def test_json_documents_are_typed_as_json_and_the_rest_as_markdown(self) -> None:
        self.assertEqual(batch.resource_type_for("a/pricing.JSON"), "json")
        self.assertEqual(batch.resource_type_for("a/checkout.md"), "markdown")
        self.assertEqual(batch.resource_type_for("a/notes.markdown"), "markdown")


class EnvelopeShapeTest(unittest.TestCase):
    """The wire shape a document is sent in.

    Two mistakes here are silent rather than loud, which is why they are pinned: the ingest envelope
    reads resource content from ``text``/``resource_text`` and IGNORES an unknown ``content`` field,
    and an ingest that is neither finalized nor waited on is accepted as a deferred event and is not
    retrievable when the call returns. Either one makes a batch import report success while storing
    nothing useful.
    """

    def setUp(self) -> None:
        self.captured = {}
        self.doc = os.path.join(tempfile.mkdtemp(), "playbook.md")
        with open(self.doc, "w", encoding="utf-8") as handle:
            handle.write("# Playbook" + chr(10) + chr(10) +
                         "## Step 1" + chr(10) + "Apply the coupon." + chr(10))

        class FakeResponse:
            status = 202

            def read(self):
                return b"{}"

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def fake_urlopen(request, timeout=None):
            self.captured["url"] = request.full_url
            self.captured["body"] = json.loads(request.data.decode("utf-8"))
            return FakeResponse()

        self._real = batch.urllib.request.urlopen
        batch.urllib.request.urlopen = fake_urlopen

    def tearDown(self) -> None:
        batch.urllib.request.urlopen = self._real

    def test_resource_content_is_sent_as_text_not_content(self) -> None:
        ok, _ = batch.post_document(
            "http://h:8080", self.doc, user_id="u", api_key="", timeout_s=5
        )
        self.assertTrue(ok)
        body = self.captured["body"]
        self.assertIn("text", body)
        self.assertIn("Apply the coupon", body["text"])
        # `content` is not part of the envelope; sending it would be silently dropped.
        self.assertNotIn("content", body)

    def test_documents_are_finalized_and_waited_on_by_default(self) -> None:
        batch.post_document("http://h:8080", self.doc, user_id="u", api_key="", timeout_s=5)
        body = self.captured["body"]
        self.assertTrue(body["finalize"])
        self.assertTrue(body["wait"])

    def test_finalize_can_be_turned_off_for_streaming_callers(self) -> None:
        batch.post_document(
            "http://h:8080", self.doc, user_id="u", api_key="", timeout_s=5, finalize=False
        )
        self.assertFalse(self.captured["body"]["finalize"])

    def test_identity_and_scope_travel_with_the_document(self) -> None:
        batch.post_document("http://h:8080", self.doc, user_id="acme", api_key="", timeout_s=5)
        body = self.captured["body"]
        self.assertEqual(body["identity_key"], batch.identity_key_for(self.doc))
        self.assertEqual(body["scope"], {"user_id": "acme"})
        self.assertTrue(self.captured["url"].endswith("/v1/ingest"))


class ArgumentTest(unittest.TestCase):
    def test_resume_without_a_state_file_is_rejected(self) -> None:
        self.assertEqual(batch.main(["--dir", ".", "--resume"]), 2)

    def test_sources_combine_and_deduplicate_preserving_order(self) -> None:
        args = batch.build_parser().parse_args(["a/one.md", "a/one.md", "b/two.md"])
        self.assertEqual(batch.resolve_inputs(args), ["a/one.md", "b/two.md"])


if __name__ == "__main__":
    unittest.main()
