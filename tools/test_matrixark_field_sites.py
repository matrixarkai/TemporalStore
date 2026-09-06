#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The site counter has to be right about the distinction a grep cannot make.

Three changes in a row were shipped here on the belief that one routine wrote a field, and the field
was written in four places, then five, then six. What makes this tool worth having is not that it
counts, but that it counts READS and WRITES separately and ignores text that only looks like an
access -- a comment, a docstring, a string literal.

The fixtures are written to a temporary directory rather than asserted against the repository, so
the expected numbers do not drift every time someone adds a call.
"""
import os
import tempfile
import unittest

try:
    from tools.matrixark_field_sites import field_sites
except ImportError:  # run from tools/
    from matrixark_field_sites import field_sites

MODULE = '''
"""A docstring mentioning storage_record_kind, which is not an access."""

# a comment mentioning storage_record_kind, also not an access
LABEL = "storage_record_kind"          # a string literal, still not an access


def read_it(record):
    kind = record.get("storage_record_kind")
    other = record["storage_record_kind"]
    return kind, other


def write_it(record):
    record["storage_record_kind"] = "index"
    return {"storage_record_kind": "index", "unrelated": 1}


def pop_it(record):
    record.pop("storage_record_kind", None)
    record.setdefault("storage_record_kind", "index")
'''


class FieldSitesTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        with open(os.path.join(self.dir, "sample.py"), "w", encoding="utf-8") as fh:
            fh.write(MODULE)

    def _sites(self, field="storage_record_kind"):
        return field_sites(field, root=self.dir)

    def test_reads_are_counted(self):
        """`.get(...)` and a subscript read, and nothing else."""
        self.assertEqual(2, len(self._sites()["reads"]))

    def test_writes_are_counted(self):
        """An assignment target, a dict literal, and pop/setdefault which mutate."""
        self.assertEqual(4, len(self._sites()["writes"]))

    def test_a_subscript_assignment_is_a_write_not_a_read(self):
        """The distinction a grep cannot make, and the one that decides tractability."""
        writes = self._sites()["writes"]
        self.assertTrue(any(subject == "record" for _, _, subject in writes))
        for _, _, subject in self._sites()["reads"]:
            self.assertEqual("record", subject)

    def test_prose_and_string_literals_are_not_accesses(self):
        """A docstring, a comment and a bare string all mention the field; none is a site.

        This is the positive control for the whole tool: a grep would report three extra hits here
        and the count would be wrong in the direction that makes a change look harder than it is.
        """
        total = len(self._sites()["reads"]) + len(self._sites()["writes"])
        self.assertEqual(6, total, "prose was counted as an access")

    def test_an_absent_field_has_no_sites(self):
        result = self._sites("a_field_that_is_not_there")
        self.assertEqual([], result["reads"])
        self.assertEqual([], result["writes"])

    def test_the_subject_expression_is_reported(self):
        """Reading a name off a request is a different thing from reading it off a record."""
        for _, _, subject in self._sites()["reads"]:
            self.assertTrue(subject, "a site with no subject cannot be judged")

    def test_test_modules_are_skipped_by_default(self):
        with open(os.path.join(self.dir, "test_sample.py"), "w", encoding="utf-8") as fh:
            fh.write(MODULE)
        self.assertEqual(2, len(self._sites()["reads"]))
        with_tests = field_sites("storage_record_kind", root=self.dir, include_tests=True)
        self.assertEqual(4, len(with_tests["reads"]))

    def test_a_file_that_does_not_parse_is_skipped(self):
        with open(os.path.join(self.dir, "broken.py"), "w", encoding="utf-8") as fh:
            fh.write("def (:\n")
        self.assertEqual(2, len(self._sites()["reads"]))


if __name__ == "__main__":
    unittest.main()
