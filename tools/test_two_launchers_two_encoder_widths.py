"""Two launchers default to two encoders, and the vectors are 16x different in width.

`tools/matrixark_mcp_rust_server.sh` -- `matrixark_agent_config.DEFAULT_LAUNCHER`, the launcher every
agent integration uses -- exports::

    MATRIXARK_EMBEDDING_PROVIDER=oss      MATRIXARK_REQUIRE_OSS_EMBEDDINGS=1

`tools/matrixark_codex_rust_hook.sh` exports::

    MATRIXARK_EMBEDDING_PROVIDER=hash     MATRIXARK_REQUIRE_OSS_EMBEDDINGS=0

Both point `MATRIXARK_EMBEDDING_MODEL_PATH` at the same model directory, so the difference is not
visible from the paths. Measured by running each provider:

    launcher default          provider        model                            width
    codex_rust_hook           hash            matrixark-local-token-hash-v1       32
    DEFAULT_LAUNCHER          oss             intfloat/multilingual-e5-large     512

A vector of 32 dimensions cannot be compared with one of 512. Records ingested through one launcher
are therefore not densely retrievable alongside records ingested through the other, and this is not
theoretical: the live corpus was measured holding thousands of hash-width vectors.

What makes it hard to notice is that nothing in the record says which encoder produced it. The
`context_model_registry` row carries `model_hash`, and the default backend does not emit one --
`materialize_serving_record_batch` has two implementations and the reachable one omits the call
(see `test_the_batch_shaper_has_two_answers`). So width really is the only discriminator.

## This file changes neither launcher

Which encoder a launcher should default to is a product decision -- the hash encoder loads nothing
and answers instantly, the OSS encoder is the one that retrieves well -- and changing either alters
what lands in the stores of everyone already running it. What this does is stop the split moving, or
widening, without someone saying so:

* either launcher changing its exported default fails;
* the two producing the SAME width fails, because then this file describes a question that no longer
  exists and should be replaced by an assertion that they agree;
* and the probe is floored, since two providers that both failed to load would agree on `None` and
  read as consensus.
"""

import json
import os
import pathlib
import re
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
ROOT = TOOLS.parent

LAUNCHERS = {
    "matrixark_mcp_rust_server.sh": {"MATRIXARK_EMBEDDING_PROVIDER": "oss",
                                     "MATRIXARK_REQUIRE_OSS_EMBEDDINGS": "1"},
    "matrixark_codex_rust_hook.sh": {"MATRIXARK_EMBEDDING_PROVIDER": "hash",
                                     "MATRIXARK_REQUIRE_OSS_EMBEDDINGS": "0"},
}

# Recorded, not derived: a table read out of the tree can only agree with the tree.
RECORDED_WIDTH = {"hash": 32, "oss": 512}

PROBE = """
import json, sys
sys.path.insert(0, %r)
import matrixark_mcp_embeddings as e
v = e.embedding_for_text("the quick brown fox jumps over the lazy dog")
print(json.dumps({"provider": e.embedding_provider_name(),
                  "model": e.embedding_model_name(),
                  "width": None if v is None else len(v)}))
"""


def _exported_default(filename, variable):
    """The value a launcher exports when the operator has not set the variable."""
    text = (TOOLS / filename).read_text(encoding="utf-8")
    m = re.search(r'export\s+%s="\$\{%s:-([^}"]*)\}"' % (variable, variable), text)
    return None if m is None else m.group(1).strip()


def _measure(env_extra):
    env = dict(os.environ)
    for key in ("MATRIXARK_EMBEDDING_PROVIDER", "MATRIXARK_REQUIRE_OSS_EMBEDDINGS"):
        env.pop(key, None)
    env.update(env_extra)
    res = subprocess.run([sys.executable, "-c", PROBE % str(TOOLS)],
                         capture_output=True, text=True, cwd=str(TOOLS), env=env)
    if res.returncode != 0:
        raise AssertionError("probe failed for %r:\n%s" % (env_extra, res.stderr[-400:]))
    return json.loads(res.stdout.strip().splitlines()[-1])


class TwoLaunchersTwoEncoderWidthsTest(unittest.TestCase):

    def test_each_launcher_still_exports_the_default_recorded_here(self):
        for filename, expected in LAUNCHERS.items():
            for variable, value in expected.items():
                with self.subTest(launcher=filename, variable=variable):
                    found = _exported_default(filename, variable)
                    self.assertIsNotNone(
                        found, "%s no longer exports %s with a default; the split may have been "
                               "resolved or the export reshaped" % (filename, variable))
                    self.assertEqual(
                        value, found,
                        "%s now exports %s=%s where this file records %s. If that is deliberate, "
                        "say so and update the table -- it changes which encoder writes the "
                        "vectors for everyone launching that way."
                        % (filename, variable, found, value))

    def test_the_probe_produced_real_vectors(self):
        """Two providers that both failed to load would agree on None and read as consensus."""
        for provider, env in (("hash", LAUNCHERS["matrixark_codex_rust_hook.sh"]),
                              ("oss", LAUNCHERS["matrixark_mcp_rust_server.sh"])):
            with self.subTest(provider=provider):
                data = _measure(env)
                self.assertIsInstance(data["width"], int,
                                      "%s produced no vector at all (%r)" % (provider, data))
                self.assertGreater(data["width"], 0)

    def test_the_two_launchers_produce_different_widths(self):
        narrow = _measure(LAUNCHERS["matrixark_codex_rust_hook.sh"])
        wide = _measure(LAUNCHERS["matrixark_mcp_rust_server.sh"])
        self.assertNotEqual(
            narrow["width"], wide["width"],
            "the two launchers now produce the same vector width. That is the good outcome and it "
            "makes this file the wrong shape: replace it with an assertion that they agree.")
        self.assertEqual(RECORDED_WIDTH["hash"], narrow["width"])
        self.assertEqual(RECORDED_WIDTH["oss"], wide["width"])

    def test_the_default_launcher_is_still_the_one_named(self):
        """The split matters because one side of it is what every agent integration launches."""
        sys.path.insert(0, str(TOOLS))
        import matrixark_agent_config as cfg
        self.assertEqual("tools/matrixark_mcp_rust_server.sh", cfg.DEFAULT_LAUNCHER,
                         "DEFAULT_LAUNCHER moved; re-read which encoder the common path now uses")


if __name__ == "__main__":
    unittest.main()
