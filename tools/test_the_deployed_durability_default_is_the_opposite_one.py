"""The engine says async storage must never be the deployed default. Every launcher makes it one.

`matrixark_rust_proxy_impl.rs` reads the flag and defaults it OFF, with the reason written beside
it::

    Inherit the durable engine-library default (async_storage=false, i.e. every write is
    fsync-committed to the WAL before it is acked). The async path buffers the WAL with no
    barrier, so a crash before the next flush drops an acked write -- that must never be the
    deployed front-door default. Async is opt-in only, via an explicit truthy
    MATRIXARK_RUST_PROXY_ASYNC_STORAGE.

Four shipped launchers export that explicit truthy value when the operator has not::

    tools/matrixark_claude_hook.sh          ${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}
    tools/matrixark_codex_dual_hook.sh      ${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}
    tools/matrixark_codex_rust_hook.sh      ${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}
    tools/matrixark_agent_config.py         "MATRIXARK_RUST_PROXY_ASYNC_STORAGE": "true"

So the engine's opt-in is opted into for everybody, and the deployed front-door default is the one
the engine says it must never be.

## The docs disagree because "default" means two things

    docs/ops/temporalstore-engine-flags.md   off        <- GENERATED from the engine's own default
    docs/SYNC_STORAGE_SCALE_VALIDATION.md    defaults on
    docs/CLOUD_API_REFERENCE.md              =1 async storage
    docs/DEPLOY_CLOUD_API.md                 =1

Both are accurate about different things: the code default is off and the deployed default is on.
A reader of the generated inventory takes "off" to mean what their deployment runs, and it is not.

## This file changes nothing, and should not

Flipping the launchers to sync changes durability AND throughput for every existing deployment, and
async was chosen on measured performance grounds -- `docs/ENTERPRISE_INGESTION_RETRIEVAL_PERF.md`
names it as the storage backend the numbers were taken on. Both sides have a real argument, which is
exactly what makes it a decision rather than a defect.

What this does is hold the contradiction still: the engine default, the four launcher exports, and
the fact that they disagree. If any of them moves, someone has decided something, and this says so.
"""

import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
TOOLS = ROOT / "tools"
FLAG = "MATRIXARK_RUST_PROXY_ASYNC_STORAGE"

PROXY = ROOT / "crates" / "temporalstore-rust" / "src" / "matrixark_rust_proxy_impl.rs"

SHELL_LAUNCHERS = ("matrixark_claude_hook.sh", "matrixark_codex_dual_hook.sh",
                   "matrixark_codex_rust_hook.sh")


class TheDeployedDurabilityDefaultIsTheOppositeOneTest(unittest.TestCase):

    def test_the_engine_still_defaults_it_off(self):
        """`unwrap_or(false)` is the durable default the comment is about."""
        if not PROXY.exists():
            self.skipTest("the Rust proxy source is not in this checkout")
        text = PROXY.read_text(encoding="utf-8", errors="replace")
        self.assertIn(FLAG, text, "the proxy no longer reads %s" % FLAG)
        idx = text.index(FLAG)
        window = text[idx:idx + 400]
        self.assertIn("unwrap_or(false)", window,
                      "the engine's default for %s is no longer false -- if that is deliberate, "
                      "the comment beside it and this file both need re-reading" % FLAG)

    def test_the_engine_still_says_it_must_not_be_the_deployed_default(self):
        """The sentence is the whole reason this file exists. If it goes, the rule went with it."""
        if not PROXY.exists():
            self.skipTest("the Rust proxy source is not in this checkout")
        text = PROXY.read_text(encoding="utf-8", errors="replace")
        self.assertIn("must never be the deployed front-door default", text,
                      "the engine no longer states the rule that the launchers contradict; "
                      "either it was withdrawn deliberately or it was lost in an edit")

    def test_every_shell_launcher_still_exports_it_on(self):
        found = {}
        for name in SHELL_LAUNCHERS:
            path = TOOLS / name
            if not path.exists():
                continue
            m = re.search(r'export\s+%s="\$\{%s:-([^}"]*)\}"' % (FLAG, FLAG),
                          path.read_text(encoding="utf-8"))
            found[name] = None if m is None else m.group(1).strip().lower()
        self.assertTrue(found, "none of the shell launchers were found")
        for name, value in sorted(found.items()):
            with self.subTest(launcher=name):
                self.assertEqual(
                    "true", value,
                    "%s exports %s=%r. If a launcher has moved to the durable default that is "
                    "good news and a deliberate change -- update this file to say so."
                    % (name, FLAG, value))

    def test_the_python_launcher_config_still_sets_it_on(self):
        path = TOOLS / "matrixark_agent_config.py"
        text = path.read_text(encoding="utf-8")
        self.assertIn(FLAG, text, "matrixark_agent_config no longer sets %s" % FLAG)
        idx = text.index(FLAG)
        self.assertIn("true", text[idx:idx + 80].lower(),
                      "matrixark_agent_config no longer sets %s on" % FLAG)

    def test_the_generated_inventory_still_reports_the_code_default(self):
        """The generated page says "off" because it reads the engine. That is correct and it is
        also what makes it misleading on its own: a reader takes it for what their deployment runs.
        Pinned so that if the generator ever starts reporting the DEPLOYED default instead, the
        change is noticed rather than assumed."""
        doc = ROOT / "docs" / "ops" / "temporalstore-engine-flags.md"
        if not doc.exists():
            self.skipTest("the generated inventory is absent")
        for line in doc.read_text(encoding="utf-8").splitlines():
            if FLAG in line:
                self.assertIn("off", line.lower(),
                              "the generated inventory now reports %s as something other than "
                              "off; it reads the engine default, so either the engine moved or "
                              "the generator changed what it reports" % FLAG)
                return
        self.skipTest("%s is not in the generated inventory" % FLAG)


if __name__ == "__main__":
    unittest.main()
