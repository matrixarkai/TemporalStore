"""The two `session_commit` paths record different memory-layer lineage, and neither is wider.

`session_commit` exists twice: a mixin method in `matrixark_local_adapter_session_commit` and a free
function in `matrixark_mcp_session_runtime`. Both build the `memory_layers_written` record that goes
with a `context_batch_commit`. One calls the extracted helper
`session_commit_memory_layers_written`; the other builds the same dict inline.

The field sets differ in **both** directions:

    only the helper : source_profile_memory_classes, source_profile_memory_kinds
    only the inline : source_memory_selection_policies
    shared          : 17

So this is not one copy being poorer -- it is two copies each recording something the other drops.
Which matters because the fields are lineage: a consumer asking "which profile-memory classes did
this commit touch" gets an answer on one path and silence on the other, and a consumer asking about
selection policies gets the reverse.

This file does not merge them. Recording the union would start writing fields into records a store
does not have today, and dropping either would lose lineage something may already read -- both are
decisions about stored data rather than tidying. What it does is stop the split moving unnoticed:

* a field appearing on one side and not the other fails, in either direction;
* the two sets becoming equal fails, because then the split is resolved and this file is the wrong
  shape -- it should be replaced by an assertion that one implementation serves both;
* and the parse is floored, since a scan that stopped finding dict keys would report two empty sets
  as agreement.

The keys are read from the AST rather than by calling the functions: `session_commit` is ~600 lines
with a store, an extractor and a commit behind it, so constructing a call is a larger fixture than
the thing being checked, and the dict literal is what decides the field set either way.
"""

import ast
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

# What each side records today. Recorded, not derived -- a table derived from the tree can only
# agree with the tree.
ONLY_HELPER = {"source_profile_memory_classes", "source_profile_memory_kinds"}
ONLY_INLINE = {"source_memory_selection_policies"}
SHARED_AT_LEAST = 15


def _function(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            found = node  # a later definition of the same name hides an earlier one
    assert found is not None, "%s not found in %s" % (name, path.name)
    return found


def _helper_keys():
    node = _function(TOOLS / "matrixark_mcp_session_runtime.py",
                     "session_commit_memory_layers_written")
    keys = set()
    for inner in ast.walk(node):
        if isinstance(inner, ast.Dict):
            for k in inner.keys:
                if isinstance(k, ast.Constant) and isinstance(k.value, str):
                    keys.add(k.value)
    return keys


def _inline_keys():
    node = _function(TOOLS / "matrixark_local_adapter_session_commit.py", "session_commit")
    keys = set()
    for inner in ast.walk(node):
        if not isinstance(inner, (ast.Assign, ast.AnnAssign)):
            continue
        targets = inner.targets if isinstance(inner, ast.Assign) else [inner.target]
        if not any(isinstance(t, ast.Name) and t.id == "memory_layers_written" for t in targets):
            continue
        if isinstance(inner.value, ast.Dict):
            for k in inner.value.keys:
                if isinstance(k, ast.Constant) and isinstance(k.value, str):
                    keys.add(k.value)
    return keys


class TwoSessionCommitsRecordDifferentLineageTest(unittest.TestCase):

    def test_both_sides_still_parse_to_something(self):
        """Two empty sets agree, which would make every assertion below pass over nothing."""
        helper, inline = _helper_keys(), _inline_keys()
        self.assertGreater(len(helper), 10,
                           "the helper parsed to %d keys; the dict it builds has moved" % len(helper))
        self.assertGreater(len(inline), 10,
                           "the inline construction parsed to %d keys; it has moved or been "
                           "replaced by a call" % len(inline))
        self.assertGreaterEqual(len(helper & inline), SHARED_AT_LEAST)

    def test_the_recorded_split_is_the_one_present(self):
        helper, inline = _helper_keys(), _inline_keys()
        self.assertEqual(
            sorted(ONLY_HELPER), sorted(helper - inline),
            "the fields only the helper records have changed. These are lineage: a consumer "
            "reading them gets an answer on one commit path and silence on the other, so a change "
            "here changes what a store can be asked.")
        self.assertEqual(
            sorted(ONLY_INLINE), sorted(inline - helper),
            "the fields only the inline construction records have changed.")

    def test_neither_side_is_a_superset(self):
        """The interesting property. If one becomes a superset the choice stops being a trade and
        becomes a straight loss, which is worth failing over so someone re-reads it."""
        helper, inline = _helper_keys(), _inline_keys()
        self.assertTrue(helper - inline, "the helper no longer records anything the inline copy "
                                         "lacks -- the inline copy is now a superset")
        self.assertTrue(inline - helper, "the inline copy no longer records anything the helper "
                                         "lacks -- the helper is now a superset")

    def test_the_split_has_not_been_resolved(self):
        """If the two agree completely, this file is describing a question that no longer exists."""
        helper, inline = _helper_keys(), _inline_keys()
        self.assertNotEqual(
            helper, inline,
            "the two session_commit paths now record the same lineage fields. That is the good "
            "outcome and it makes this file the wrong shape: replace it with a guard that one "
            "implementation builds this record for both paths.")


if __name__ == "__main__":
    unittest.main()
