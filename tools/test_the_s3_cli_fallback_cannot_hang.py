#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The aws-cli S3 fallback cannot hang forever.

`upload_file_to_s3` and `download_s3_to_file` try boto3 first and fall back to shelling out to
`aws s3 cp`. The boto3 path is bounded by botocore's own socket timeouts. The CLI path was
bounded by nothing, so an endpoint that accepts the connection and never answers held a resource
ingest open indefinitely -- no error, no diagnosis, nothing to retry.

WHY THIS IS NOT THE RARE PATH IT LOOKS LIKE. `dependencies` in `pyproject.toml` is empty and
boto3 is declared in no manifest, so a pip-installed copy of this package has no boto3,
`_s3_client()` returns None, and **the CLI is the S3 path** -- for exactly the outside user this
repository exists for. `_s3_client()` also catches `Exception` broadly, so a bad region or
endpoint lands on the same fallback.

The checks below are behavioural rather than a read of the source: the timeout is observed by
capturing what `subprocess.run` is actually called with, and the failure mode is observed by
making that call raise. A structural check sits beside them for the thing behaviour cannot see --
a NEW shell-out appearing in this module without a deadline of its own.
"""
from __future__ import annotations

import ast
import io
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_mcp_core_resource_io as resource_io  # noqa: E402
from matrixark_mcp_core import MatrixArkError  # noqa: E402

#: A floor, not the value. 900 today. The point is that nobody tightens this to something that
#: cannot clear a large attachment: botocore bounds each READ, this bounds the WHOLE transfer,
#: so matching botocore's 60s here would fail uploads that are merely big rather than stuck.
MINIMUM_SENSIBLE_TIMEOUT_S = 300


class _Recorder:
    """Stands in for `subprocess.run`, remembering how it was called."""

    def __init__(self, outcome=None):
        self.calls = []
        self.outcome = outcome

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


class _Completed:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TheS3CliFallbackCannotHangTest(unittest.TestCase):

    def setUp(self) -> None:
        self._real_run = resource_io.subprocess.run
        self.addCleanup(setattr, resource_io.subprocess, "run", self._real_run)

    def test_the_copy_is_given_a_deadline(self) -> None:
        recorder = _Recorder(_Completed())
        resource_io.subprocess.run = recorder
        resource_io._aws_cli_s3_cp("s3://bucket/key", "/tmp/target")

        self.assertEqual(1, len(recorder.calls))
        _, kwargs = recorder.calls[0]
        self.assertIn("timeout", kwargs,
                      "the aws-cli copy was invoked with no timeout, so a stalled endpoint holds "
                      "the request forever")
        self.assertEqual(resource_io.AWS_CLI_S3_TIMEOUT_S, kwargs["timeout"])

    def test_the_deadline_is_generous_enough_for_a_real_transfer(self) -> None:
        self.assertGreaterEqual(
            resource_io.AWS_CLI_S3_TIMEOUT_S, MINIMUM_SENSIBLE_TIMEOUT_S,
            "this caps the WHOLE transfer, not each read; tightening it towards botocore's "
            "per-read 60s turns a large upload into a failure")

    def test_a_stalled_copy_raises_the_error_callers_already_handle(self) -> None:
        """Not `TimeoutExpired`: every caller of this already handles `MatrixArkError`."""
        resource_io.subprocess.run = _Recorder(
            subprocess.TimeoutExpired(cmd=["aws"], timeout=resource_io.AWS_CLI_S3_TIMEOUT_S))
        with self.assertRaises(MatrixArkError) as caught:
            resource_io._aws_cli_s3_cp("s3://bucket/key", "/tmp/target")
        self.assertIn("timed out", str(caught.exception))

    def test_a_failed_copy_still_reports_what_the_cli_said(self) -> None:
        """The control: the pre-existing failure path must be unchanged by the deadline."""
        resource_io.subprocess.run = _Recorder(
            _Completed(returncode=1, stderr="NoSuchBucket: nope"))
        with self.assertRaises(MatrixArkError) as caught:
            resource_io._aws_cli_s3_cp("s3://bucket/key", "/tmp/target")
        self.assertIn("NoSuchBucket", str(caught.exception))

    def test_no_other_shell_out_in_this_module_lacks_one(self) -> None:
        """What behaviour cannot see: a SECOND `subprocess.run` added later without a deadline.

        The module has exactly one today. A new one is not necessarily wrong -- it just has to
        say how long it is willing to wait.
        """
        with io.open(os.path.join(TOOLS, "matrixark_mcp_core_resource_io.py"),
                     encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        undeadlined = []
        seen = 0
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            try:
                name = ast.unparse(node.func)
            except Exception:                       # pragma: no cover - unparse is total here
                continue
            if name not in ("subprocess.run", "subprocess.check_output",
                            "subprocess.check_call", "subprocess.call"):
                continue
            seen += 1
            if not any(kw.arg in ("timeout", None) for kw in node.keywords):
                undeadlined.append("%s at line %d" % (name, node.lineno))
        self.assertGreater(seen, 0, "the scan found no shell-out at all; it has stopped matching")
        self.assertEqual([], undeadlined,
                         "a blocking shell-out in the resource path with no deadline: "
                         + ", ".join(undeadlined))


if __name__ == "__main__":
    unittest.main()
