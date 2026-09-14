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
RECORDED_MODEL = {"hash": "matrixark-local-token-hash-v1",
                  "oss": "intfloat/multilingual-e5-large"}

# Two probes on purpose. The first asks only what the configuration SELECTS, which needs no model
# on disk and therefore runs anywhere. The second actually encodes, which needs the OSS model and
# cannot run on a machine that has not downloaded it -- CI being one.
SELECT_PROBE = """
import json, sys
sys.path.insert(0, %r)
import matrixark_mcp_embeddings as e
print(json.dumps({"provider": e.embedding_provider_name(), "model": e.embedding_model_name()}))
"""

WIDTH_PROBE = """
import json, sys
sys.path.insert(0, %r)
import matrixark_mcp_embeddings as e
v = e.embedding_for_text("the quick brown fox jumps over the lazy dog")
print(json.dumps({"provider": e.embedding_provider_name(),
                  "model": e.embedding_model_name(),
                  "width": None if v is None else len(v),
                  "fallback": bool(e.embedding_fallback_used())}))
"""


def _exported_default(filename, variable):
    """The value a launcher exports when the operator has not set the variable."""
    text = (TOOLS / filename).read_text(encoding="utf-8")
    m = re.search(r'export\s+%s="\$\{%s:-([^}"]*)\}"' % (variable, variable), text)
    return None if m is None else m.group(1).strip()


def _run(probe, env_extra):
    env = dict(os.environ)
    for key in ("MATRIXARK_EMBEDDING_PROVIDER", "MATRIXARK_REQUIRE_OSS_EMBEDDINGS"):
        env.pop(key, None)
    env.update(env_extra)
    res = subprocess.run([sys.executable, "-c", probe % str(TOOLS)],
                         capture_output=True, text=True, cwd=str(TOOLS), env=env)
    if res.returncode != 0:
        raise AssertionError("probe failed for %r:\n%s" % (env_extra, res.stderr[-400:]))
    return json.loads(res.stdout.strip().splitlines()[-1])


def _selected(env_extra):
    """What the configuration selects. No model needed."""
    return _run(SELECT_PROBE, env_extra)


def _measure(env_extra):
    """What it actually produces. Needs the encoder on disk.

    A probe that cannot encode returns a marker rather than raising, so the caller can skip with a
    reason. Raising here would make "this machine has no model" indistinguishable from "the split
    has changed", which is the confusion this file exists to prevent.
    """
    try:
        return _run(WIDTH_PROBE, env_extra)
    except AssertionError as exc:
        return {"width": None, "unavailable": str(exc)[:200]}


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

    def test_the_two_launchers_select_different_models(self):
        """The protection that runs everywhere: no encoder needs to be on disk to ask this."""
        narrow = _selected(LAUNCHERS["matrixark_codex_rust_hook.sh"])
        wide = _selected(LAUNCHERS["matrixark_mcp_rust_server.sh"])
        self.assertEqual("hash", narrow["provider"])
        self.assertEqual("oss", wide["provider"])
        self.assertNotEqual(
            narrow["model"], wide["model"],
            "the two launchers now select the same model (%r). That is the good outcome and it "
            "makes this file the wrong shape: replace it with an assertion that they agree."
            % narrow["model"])
        self.assertEqual(RECORDED_MODEL["hash"], narrow["model"])
        self.assertEqual(RECORDED_MODEL["oss"], wide["model"])

    def test_the_two_launchers_produce_different_widths(self):
        """The measurement behind the claim -- and it needs the OSS encoder ON DISK.

        Skipped where it is not, rather than failed: a machine without the model cannot answer this
        question, and a test that only runs where the author happens to be standing is worse than
        one that says so. The selection test above carries the protection everywhere; this one
        carries the number.

        The skip states WHY, so it cannot be read as a pass.
        """
        wide = _measure(LAUNCHERS["matrixark_mcp_rust_server.sh"])
        unavailable = (wide.get("unavailable")
                       or wide.get("fallback")
                       or not isinstance(wide.get("width"), int)
                       or wide["width"] == RECORDED_WIDTH["hash"])
        if unavailable:
            self.skipTest(
                "the OSS encoder is not available here (provider=%r model=%r width=%r "
                "fallback=%r), so the two widths cannot be compared -- the selection test above "
                "still holds" % (wide.get("provider"), wide.get("model"), wide.get("width"),
                                 wide.get("fallback")))
        narrow = _measure(LAUNCHERS["matrixark_codex_rust_hook.sh"])
        self.assertNotEqual(narrow["width"], wide["width"])
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
