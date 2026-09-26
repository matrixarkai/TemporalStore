#!/usr/bin/env python3
"""A PRODUCTION SHARD LOAD TAKES ITS ROUTING RANGE FROM THE NAMED DEFAULT.

WHY THIS EXISTS. Routing decides which bucket a key's page is FILED under -- the bucket is the
key's hash modulo the range WIDTH -- so two production sites that default the range differently
file the same key's pages in two different bucket groups. That was the state of this repository:
the datanode's startup path defaulted to the whole keyspace, the metaserver's reassignment path
spelled the whole keyspace as a literal, the proxy's record-log open spelled it again, and the
metaserver's own scheduler paths used 1023. A shard created on 1023 and then reassigned was loaded
on a range it was not built on, and nothing said so.

The engine now REFUSES such a load (`routing_range_mismatch`), which turns the silence into a
failure -- but a refusal at run time is a worse place to find this than a guard at build time, and
a new call site spelling a literal would reintroduce it.

THE SUBJECT LIST IS DERIVED FROM THE SOURCE, NOT WRITTEN HERE, and the count is FLOORED. A
hand-written list of call sites goes stale and nothing fails; a floor makes a scan that stopped
matching fail instead of reporting a clean zero.
"""
import pathlib
import re
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "crates" / "temporalstore-rust" / "src"

# What a site is allowed to say. Either the named constant, or a read of the documented
# environment variables (which is what the named constant is the fallback for).
ALLOWED = re.compile(r"DEFAULT_(START|END)_ROUTING_BUCKET|env_number_first")

# Files whose `LoadShardRequest` literals are FIXTURES, not production defaults, named with the
# reason each is exempt. Every entry is justified, because an exemption list with an unjustified
# entry is a hiding place.
EXEMPT = {
    # The convenience loader, deliberately on the whole keyspace so that several hundred existing
    # fixtures keep exercising the arm they were written against. Held by
    # `the_convenience_load_is_deliberately_not_the_production_default`.
    "engine/lifecycle.rs": "the test convenience loader, divergence asserted by a Rust guard",
    # A crash-reproduction harness binary. It writes and reopens one throwaway store in one
    # process, so the range never crosses a process boundary.
    "bin/wal_single_barrier_crash_harness.rs": "single-process crash harness, throwaway store",
}

# FLOOR on the number of `LoadShardRequest` construction sites found outside test modules. Set
# below the count at the time of writing (6) so ordinary growth does not fail it, and well above
# zero so a scan that stops finding them does.
SITE_FLOOR = 4

# `LoadShardRequest {` is also how the struct is DEFINED and how a function's return type is
# spelled before its body. Both forms brace-match to a body mentioning `routing_bucket` -- the
# definition because the fields are declared there, the signature because the literal is inside the
# function -- so both would be reported, one as a false offender (`start_routing_bucket: u32`, the
# field's TYPE) and one as a duplicate of the literal it contains.
NOT_A_CONSTRUCTION = re.compile(r"(struct|->)\s+$")


def rust_sources():
    return sorted(p for p in SRC.rglob("*.rs") if "/tests/" not in str(p).replace("\\", "/"))


def strip_test_modules(text):
    """Blank out `#[cfg(test)] mod ... { ... }` bodies by brace matching.

    A line-oriented filter would not do: a fixture inside a test module is indented exactly like
    production code and reads identically to a scan that only looks at lines.
    """
    out = list(text)
    for match in re.finditer(r"#\[cfg\(test\)\]\s*(?:pub\s+)?mod\s+\w+\s*\{", text):
        depth = 0
        i = match.end() - 1
        start = i
        while i < len(text):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        for j in range(start, min(i + 1, len(out))):
            if out[j] != "\n":
                out[j] = " "
    return "".join(out)


def load_shard_request_sites():
    """Every `LoadShardRequest { ... }` literal outside a test module, with its body."""
    sites = []
    for path in rust_sources():
        rel = str(path.relative_to(SRC)).replace("\\", "/")
        text = strip_test_modules(path.read_text(errors="replace"))
        for match in re.finditer(r"LoadShardRequest\s*\{", text):
            before = text[max(0, match.start() - 24):match.start()]
            # Drop the struct DEFINITION (`pub struct LoadShardRequest {`) and the return-type
            # form (`-> LoadShardRequest {`). Both brace-match to a body mentioning
            # `routing_bucket` and neither is a construction.
            if NOT_A_CONSTRUCTION.search(before):
                continue
            depth = 0
            i = match.end() - 1
            while i < len(text):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        break
                i += 1
            body = text[match.end():i]
            if "routing_bucket" not in body:
                continue
            line = text[:match.start()].count("\n") + 1
            sites.append((rel, line, body))
    return sites


class ProductionShardLoadRoutingRange(unittest.TestCase):
    def test_the_scan_finds_sites_at_all(self):
        """THE FLOOR. A scan that stops matching reports zero offenders, which reads as clean."""
        sites = load_shard_request_sites()
        print("\n  %d production LoadShardRequest sites carrying a routing range:" % len(sites))
        for rel, line, _ in sites:
            print("     %s:%d" % (rel, line))
        self.assertGreaterEqual(
            len(sites), SITE_FLOOR,
            "the scan found %d LoadShardRequest sites outside test modules against a floor of %d. "
            "Either the construction syntax changed or the test-module stripper is eating "
            "production code; either way a clean result below means nothing."
            % (len(sites), SITE_FLOOR))

    def test_every_exemption_names_a_file_that_exists_and_has_a_site(self):
        """AN EXEMPTION LIST IS A HIDING PLACE UNLESS EVERY ENTRY IS STILL EARNING ITS PLACE."""
        sites = {rel for rel, _, _ in load_shard_request_sites()}
        for rel, reason in sorted(EXEMPT.items()):
            self.assertTrue((SRC / rel).exists(), "exempt file %s no longer exists" % rel)
            self.assertIn(
                rel, sites,
                "%s is exempt (%s) but the scan no longer finds a LoadShardRequest site in it, so "
                "the exemption is dead and hiding nothing -- remove it" % (rel, reason))
            self.assertTrue(reason.strip(), "%s is exempt with no reason given" % rel)

    def test_no_production_site_spells_the_routing_range_as_a_literal(self):
        """THE CLAIM. Every non-exempt site takes the range from the named default or the env."""
        offenders = []
        checked = 0
        for rel, line, body in load_shard_request_sites():
            if rel in EXEMPT:
                continue
            checked += 1
            for field in ("start_routing_bucket", "end_routing_bucket"):
                m = re.search(field + r"\s*:\s*([^,\n]+)", body)
                if not m:
                    offenders.append((rel, line, field, "<absent>"))
                    continue
                value = m.group(1).strip()
                if not ALLOWED.search(value):
                    offenders.append((rel, line, field, value))
        print("\n  checked %d non-exempt site(s), %d offender(s)" % (checked, len(offenders)))
        self.assertGreater(
            checked, 0,
            "every site is exempt, so this test checked nothing. An exemption list that covers "
            "the whole subject is not an exemption list.")
        self.assertEqual(
            [], offenders,
            "these production shard loads spell a routing range instead of taking the named "
            "default:\n  %s\nTwo sites that default the range differently file the same key's "
            "pages in two different bucket groups. Use "
            "temporalstore_rust::DEFAULT_START_ROUTING_BUCKET / DEFAULT_END_ROUTING_BUCKET."
            % "\n  ".join("%s:%d %s = %s" % o for o in offenders))

    def test_the_named_default_is_a_single_definition(self):
        """ONE FLAG, ONE DEFAULT. Two definitions of the same default drift apart silently."""
        hits = []
        for path in rust_sources():
            rel = str(path.relative_to(SRC)).replace("\\", "/")
            for i, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
                if re.search(r"(pub )?const DEFAULT_(START|END)_ROUTING_BUCKET", line):
                    hits.append((rel, i, line.strip()))
        print("\n  definitions of the named default:")
        for rel, i, line in hits:
            print("     %s:%d %s" % (rel, i, line))
        self.assertEqual(
            2, len(hits),
            "expected exactly one definition of each of the two named defaults, found %d:\n  %s"
            % (len(hits), "\n  ".join("%s:%d" % (r, i) for r, i, _ in hits)))

    def test_the_negative_control_a_planted_literal_is_caught(self):
        """EVERY GATE NEEDS AN INPUT ON WHICH IT MUST FAIL.

        The matcher is run against a planted body rather than against a planted FILE, so the
        control cannot leave a modified tree behind if it fails partway.
        """
        planted = ("shard_id: 1, start_routing_bucket: 0, end_routing_bucket: u32::MAX,")
        offenders = []
        for field in ("start_routing_bucket", "end_routing_bucket"):
            m = re.search(field + r"\s*:\s*([^,\n]+)", planted)
            self.assertIsNotNone(m, "the planted body does not even parse")
            if not ALLOWED.search(m.group(1).strip()):
                offenders.append(field)
        self.assertEqual(
            ["start_routing_bucket", "end_routing_bucket"], offenders,
            "a planted literal routing range was not caught by the matcher, so the clean result "
            "in the test above says nothing. Caught: %s" % offenders)

        accepted = ("start_routing_bucket: temporalstore_rust::DEFAULT_START_ROUTING_BUCKET, "
                    "end_routing_bucket: temporalstore_rust::DEFAULT_END_ROUTING_BUCKET,")
        for field in ("start_routing_bucket", "end_routing_bucket"):
            m = re.search(field + r"\s*:\s*([^,\n]+)", accepted)
            self.assertTrue(
                ALLOWED.search(m.group(1).strip()),
                "the matcher rejects the named default itself for %s, so it would fail every "
                "correct site" % field)


if __name__ == "__main__":
    unittest.main()
