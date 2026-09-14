"""A backend name with a stray space selects the same backend as one without.

`MATRIXARK_MCP_BACKEND` is read by three selector functions and was read again, directly, by two of
their callers. They did not agree about what a value means:

* every selector passed a value through untrimmed, so ``temporalstore-direct `` -- with the
  trailing space a ``.env`` line or a compose ``environment:`` entry produces without anyone seeing
  it -- reached the validator, which refused to start and quoted the operator the exact backend name
  they had just set;
* the selectors treated the empty string as unset, while ``os.environ.get(VAR, default())`` in
  ``matrixark_asgi`` and ``matrixark_v1_gateway`` returned the empty string itself;
* every shell entry point uses ``${MATRIXARK_MCP_BACKEND:-...}``, which treats empty as unset --
  so the two Python callers disagreed with both the shell and the rest of Python.

``matrixark_asgi`` already had the right idiom one line below the wrong one::

    os.environ.get("MATRIXARK_ACCESS_MODE", "").strip() or "enforced"

These tests pin the agreement rather than the implementation: they ask what each selector RETURNS
for a set of spellings, so a future reader that trims differently still passes as long as it agrees.
"""

import importlib
import os
import pathlib
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

SELECTORS = (
    ("matrixark_mcp_server", "default_mcp_backend"),
    ("matrixark_mcp_backends", "default_mcp_backend"),
    ("matrixark_codex_hook", "default_hook_backend"),
)

# Read in a fresh process each time: these modules do work at import, and the value under test is
# the environment, which cannot be varied honestly inside one interpreter that has already imported
# them.
PROBE = """
import sys
sys.path.insert(0, %r)
import %s as m
print(m.%s())
"""


def resolve(module, function, value):
    env = dict(os.environ)
    env.pop("MATRIXARK_MCP_BACKEND", None)
    if value is not None:
        env["MATRIXARK_MCP_BACKEND"] = value
    out = subprocess.run(
        [sys.executable, "-c", PROBE % (str(TOOLS), module, function)],
        capture_output=True, text=True, cwd=str(TOOLS), env=env)
    if out.returncode != 0:
        raise AssertionError("%s.%s did not run for %r:\n%s"
                             % (module, function, value, out.stderr[-400:]))
    return out.stdout.strip()


class ABackendNameWithAStraySpaceTest(unittest.TestCase):

    def test_surrounding_whitespace_selects_the_same_backend(self):
        for module, function in SELECTORS:
            for spelling in ("temporalstore-direct ", " temporalstore-direct",
                             "\ttemporalstore-direct\n"):
                with self.subTest(module=module, spelling=spelling):
                    self.assertEqual(
                        resolve(module, function, "temporalstore-direct"),
                        resolve(module, function, spelling),
                        "%s.%s reads %r as a different backend from the same name without the "
                        "whitespace; the validator then refuses to start and quotes the operator "
                        "the name they set" % (module, function, spelling))

    def test_a_blank_value_counts_as_unset(self):
        for module, function in SELECTORS:
            for blank in ("", "   ", "\t\n"):
                with self.subTest(module=module, blank=blank):
                    self.assertEqual(
                        resolve(module, function, None),
                        resolve(module, function, blank),
                        "%s.%s treats %r as a backend name rather than as unset. Every shell "
                        "entry point uses ${MATRIXARK_MCP_BACKEND:-...}, which treats it as unset."
                        % (module, function, blank))

    def test_an_exact_value_is_unchanged(self):
        """The floor: this change must move no deployment that already sets the variable cleanly."""
        for module, function in SELECTORS:
            with self.subTest(module=module):
                self.assertEqual("temporalstore-rust",
                                 resolve(module, function, "temporalstore-rust"))

    def test_no_caller_reads_the_variable_beside_the_selector(self):
        """`os.environ.get(VAR, default_mcp_backend())` is the same as `default_mcp_backend()` for
        every value except the empty string, where it returns the empty string and the selector
        returns the policy default. It reads as a harmless default and is the one shape that
        disagrees, so it must not come back."""
        offenders = []
        for path in sorted(TOOLS.glob("*.py")):
            if path.name.startswith("test_"):
                continue
            text = path.read_text(encoding="utf-8")
            for line_no, line in enumerate(text.splitlines(), 1):
                if "MATRIXARK_MCP_BACKEND" not in line:
                    continue
                if "environ.get" in line and "default_" in line and "backend()" in line:
                    offenders.append("    %s:%d  %s" % (path.name, line_no, line.strip()[:96]))
        self.assertFalse(
            offenders,
            "a caller reads MATRIXARK_MCP_BACKEND with the selector as its fallback:\n"
            + "\n".join(offenders)
            + "\n\nCall the selector instead -- it reads the variable itself, and this shape "
              "returns the empty string where the selector returns the default.")

    def test_the_probe_is_actually_varying_something(self):
        """A floor on the harness: if the probe stopped passing the variable through, every case
        above would compare a default against a default and agree for the wrong reason."""
        self.assertNotEqual(
            resolve("matrixark_mcp_server", "default_mcp_backend", None),
            resolve("matrixark_mcp_server", "default_mcp_backend", "temporalstore-rust"),
            "the probe returns the same backend whether or not the variable is set, so it is not "
            "exercising the environment at all")


if __name__ == "__main__":
    unittest.main()
