#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""WAL record compression is on, offered, and named in the shipped config.

The engine has compressed durability-log records for a while. It was built **off**, so every
deployment paid to store a log it had the code to shrink, and no surface mentioned it: not the
portal, not `config/temporalstore.toml`, not the loader that turns that file into an environment.

Its neighbours were already on -- `TS_WAL_BINARY_RECORDS` and `TS_WAL_BINARY_FRAME`, the latter of
which is what carries the footer hint the tail scan uses -- and all three share the property that
makes flipping safe: which encoding a record is in is a property of the RECORD, not of the flag, so
a log written across a change reads end to end and turning it off again is not a one-way door.

This file covers the surfaces. The engine's own tests cover what reaches the file.
"""
from __future__ import annotations

import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TOOLS)
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_load_config as loader  # noqa: E402

SETTING = "storage_engine.wal_compress_records"
VARIABLE = "TS_WAL_COMPRESS_RECORDS"
PROTO = os.path.join(ROOT, "crates", "temporalstore-rust", "src", "wal_proto.rs")
FRAMING = os.path.join(ROOT, "crates", "temporalstore-rust", "src", "log_framing.rs")


def reader_body(path: str, function: str) -> str:
    """The body of a Rust flag reader, so its default can be asked for rather than assumed."""
    with open(path, encoding="utf-8") as handle:
        source = handle.read()
    start = source.index("fn %s() -> bool {" % function)
    return source[start:source.index("\n}", start)]


def defaults_on(path: str, function: str) -> bool:
    """True when the reader OPTS OUT -- `!matches!(..., "0" | "false" | ...)`.

    Read from the source rather than restated, because the whole point of this change is which way
    round that negation goes.
    """
    body = reader_body(path, function)
    return body.lstrip().startswith("!matches!") or "\n    !matches!" in body


class TheEngineCompressesByDefaultTest(unittest.TestCase):

    def test_the_reader_opts_out_rather_than_in(self) -> None:
        self.assertTrue(defaults_on(PROTO, "compress_records_enabled"),
                        "compression is opt-IN again; every unconfigured deployment stopped "
                        "compressing its log")

    def test_it_accepts_every_spelling_its_neighbours_accept(self) -> None:
        """`off` and `no` turn the neighbours off. A flag that only understood `0` and `false` was
        a real defect on the one beside this."""
        body = reader_body(PROTO, "compress_records_enabled")
        for spelling in ('"0"', '"false"', '"no"', '"off"'):
            with self.subTest(spelling=spelling):
                self.assertIn(spelling, body)

    def test_its_neighbours_are_still_on_too(self) -> None:
        """The floor, and the reason this one was the odd one out: the record encoding and the
        frame -- which is what carries the footer hint the tail scan reads -- were already on."""
        self.assertTrue(defaults_on(PROTO, "binary_records_enabled"))
        self.assertTrue(defaults_on(FRAMING, "binary_frame_enabled"))

    def test_compression_is_still_only_applied_where_it_pays(self) -> None:
        """Turning it on is safe because it is not unconditional: a small record is left alone, and
        one that does not shrink is written raw. If either check went, this became a tax."""
        with open(PROTO, encoding="utf-8") as handle:
            source = handle.read()
        start = source.index("fn compress_payload(")
        body = source[start:source.index("\n}", start)]
        self.assertIn("COMPRESSION_MIN_BYTES", body)
        self.assertIn("compressed.len() < encoded.len()", body)


class ACustomerCanSeeAndChangeItTest(unittest.TestCase):
    """It was on none of the three surfaces a deployment is configured through."""

    def test_the_portal_offers_it(self) -> None:
        self.assertIn(SETTING, cfg.SETTINGS_BY_KEY)
        self.assertEqual(VARIABLE, cfg.SETTINGS_BY_KEY[SETTING].env)
        self.assertEqual("bool", cfg.SETTINGS_BY_KEY[SETTING].kind)

    def test_the_offered_default_is_the_engine_default(self) -> None:
        """A portal that shows `0` beside a build that compresses is the defect this file exists to
        prevent, one layer up."""
        self.assertTrue(defaults_on(PROTO, "compress_records_enabled"))
        self.assertEqual("1", cfg.SETTINGS_BY_KEY[SETTING].default)

    def test_the_help_says_it_is_reversible(self) -> None:
        """The one question an operator has about a log format: can I change my mind."""
        help_text = cfg.SETTINGS_BY_KEY[SETTING].help
        self.assertIn("one-way door", help_text)
        self.assertIn("a payload says what encoding it is in", help_text)

    def test_the_help_no_longer_explains_a_default_it_does_not_have(self) -> None:
        """It said "It ships OFF because ..." -- a sentence that became false the moment the
        default moved, and the kind a reader trusts precisely because it explains itself."""
        help_text = cfg.SETTINGS_BY_KEY[SETTING].help
        self.assertNotIn("ships OFF", help_text)
        self.assertIn("ships ON", help_text)
        self.assertIn("8.61x", help_text)

    def test_the_registry_has_no_duplicate_keys(self) -> None:
        """Added after this change introduced one: a second Setting with the same key is not an
        error anywhere -- the dict keeps the last and the first silently stops existing, so the
        portal shows one and a reader of the source finds the other."""
        keys = [setting.key for setting in cfg.SETTINGS]
        duplicated = sorted({key for key in keys if keys.count(key) > 1})
        self.assertEqual([], duplicated)

    def test_the_shipped_config_names_it(self) -> None:
        with open(os.path.join(ROOT, "config", "temporalstore.toml"), encoding="utf-8") as handle:
            toml = handle.read()
        row = [line for line in toml.splitlines() if line.startswith("wal_compress_records")]
        self.assertEqual(1, len(row), "the shipped config should name it exactly once")
        self.assertIn(VARIABLE, row[0])
        self.assertTrue(re.match(r"wal_compress_records\s*=\s*1\b", row[0]), row[0])

    def test_the_loader_exports_it(self) -> None:
        """Naming it in the file does nothing unless the loader turns it into an environment."""
        self.assertEqual(VARIABLE, loader.ENV_MAP["wal.wal_compress_records"])

    def test_it_sits_in_the_wal_section_with_its_neighbours(self) -> None:
        """A WAL choice filed anywhere else is one an operator reads past."""
        wal_keys = [k for k in loader.ENV_MAP if k.startswith("wal.")]
        self.assertIn("wal.wal_compress_records", wal_keys)
        self.assertIn("wal.wal_legacy_recovery", wal_keys)


if __name__ == "__main__":
    unittest.main()
