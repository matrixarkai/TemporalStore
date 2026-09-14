"""Which copy of `canonical_storage_route` is wider, measured rather than remembered.

Two copies exist -- `matrixark_mcp_core` and `matrixark_mcp_storage_options` -- and the single
cross-module call site in `matrixark_mcp_core_compact` pins core's, with this reason beside it::

    `canonical_storage_route` does NOT -- core's honours `durability` and
    matrixark_mcp_storage_options' returns a `read_preference` core's does not, with neither a
    superset -- so that one still comes from core

Measured, the second half of that is not so. Both copies honour `durability`; nothing is honoured
by core alone; and every field core emits, the other emits too. The relationship is not "neither a
superset" but a **strict superset in one direction**.

That matters because "neither a superset" is the stated reason for pinning core's copy. If it were
true, choosing either implementation would lose something and the pin would be a considered
trade. Since it is not, pinning core's copy means the call site produces a route dict with three
fields simply missing: `durability`, `read_preference`, `replica_read`.

This test does not change the pin. Switching it changes what `storage_route` contains on records
the store already holds, which is a decision. What it does is stop the comment and the code drifting
apart again: the relationship is asserted, in both directions, so that

* a key becoming honoured by core alone fails -- the superset would have been broken;
* a key leaving the wider copy fails;
* and the two copies becoming identical fails, because then the pin has no subject and the comment
  beside it should go.

The input vocabulary is DERIVED from each function's source rather than guessed, because a guessed
list cannot establish a superset claim -- the key you did not think of is exactly the one that
would refute it.
"""

import ast
import pathlib
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

import matrixark_mcp_core as core_module
import matrixark_mcp_storage_options as options_module

FUNCTION = "canonical_storage_route"

# Values wide enough to move any of the keys these functions read. Not a vocabulary claim; just
# enough spellings that a key which has an effect shows one.
PROBES = ("sync", "async", "strong", "eventual", "primary", "nearest", True, False, 1, 0)


def _lookup_keys(path, func_name):
    """Every string literal used as a subscript or as the first argument to .get()/.pop()."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    target = next((n for n in ast.walk(tree)
                   if isinstance(n, ast.FunctionDef) and n.name == func_name), None)
    assert target is not None, "%s not found in %s" % (func_name, path.name)
    keys = set()
    for node in ast.walk(target):
        if (isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)):
            keys.add(node.slice.value)
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in ("get", "pop") and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)):
            keys.add(node.args[0].value)
    return keys


def _honoured(fn, baseline, key):
    for value in PROBES:
        try:
            if fn({key: value}) != baseline:
                return True
        except Exception:  # a key this copy rejects is not one it honours
            continue
    return False


def _classify():
    core_fn = getattr(core_module, FUNCTION)
    opts_fn = getattr(options_module, FUNCTION)
    keys = (_lookup_keys(TOOLS / "matrixark_mcp_core.py", FUNCTION)
            | _lookup_keys(TOOLS / "matrixark_mcp_storage_options.py", FUNCTION))
    base_core, base_opts = core_fn({}), opts_fn({})
    only_core, only_opts, both = [], [], []
    for key in sorted(keys):
        c, o = _honoured(core_fn, base_core, key), _honoured(opts_fn, base_opts, key)
        (both if c and o else only_core if c else only_opts if o else []).append(key)
    return {
        "keys": keys, "both": both, "only_core": only_core, "only_opts": only_opts,
        "fields_only_core": sorted(set(base_core) - set(base_opts)),
        "fields_only_opts": sorted(set(base_opts) - set(base_core)),
        "same_object": core_fn is opts_fn,
    }


class WhichStorageRouteCopyIsWiderTest(unittest.TestCase):

    def test_the_vocabulary_scan_finds_something(self):
        """A derived key list that collapses would make every assertion below vacuously true."""
        found = _classify()
        self.assertGreaterEqual(
            len(found["keys"]), 8,
            "only %d lookup keys parsed out of the two bodies; the shape being matched has "
            "changed and the comparison no longer means anything" % len(found["keys"]))

    def test_the_two_copies_are_still_two(self):
        """If they ever become one object the pin is moot and this file should be deleted."""
        self.assertFalse(
            _classify()["same_object"],
            "the two copies are now the same object -- the call site's pin has no subject, so "
            "remove the pin, its comment, and this file together")

    def test_core_honours_nothing_the_other_copy_does_not(self):
        found = _classify()
        self.assertEqual(
            [], found["only_core"],
            "core's copy now honours %s, which the storage_options copy does not. That would make "
            "the two genuinely incomparable -- which is what the call site's comment claims today "
            "and what this test exists to check. Update the comment, and re-read whether pinning "
            "core's copy is still the right choice." % found["only_core"])

    def test_core_emits_no_field_the_other_copy_omits(self):
        found = _classify()
        self.assertEqual(
            [], found["fields_only_core"],
            "core's copy now emits %s, which the storage_options copy does not."
            % found["fields_only_core"])

    def test_the_wider_copy_is_still_wider(self):
        """The other direction. If the difference disappears, "neither a superset" becomes true by
        collapse rather than by correction, and the pin's reasoning needs re-reading either way."""
        found = _classify()
        self.assertTrue(
            found["only_opts"] or found["fields_only_opts"],
            "the two copies no longer differ in either direction. That is the good outcome and it "
            "makes this file the wrong shape: replace it with one implementation.")

    def test_the_recorded_difference_is_the_one_present(self):
        """Pin the actual shape, so a change in WHAT differs is as loud as a change in whether."""
        found = _classify()
        self.assertEqual(["read_preference"], found["only_opts"])
        self.assertEqual(["durability", "read_preference", "replica_read"],
                         found["fields_only_opts"])


if __name__ == "__main__":
    unittest.main()
