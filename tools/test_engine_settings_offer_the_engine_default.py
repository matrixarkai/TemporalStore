#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An engine setting must offer the default the engine actually uses.

mx#959 found the portal advertising retrieval budgets 10x to 156x larger than any deployment
gets, because the display was derived from a registry the serving path had been taught to
distrust. The display and the code that consumes it were both correct in isolation; nothing
compared them. This asks the same question one layer down, of the storage engine.

`crates/temporalstore-rust/src/storage_config.rs` is the whole answer for this family: it declares
the env NAME and the DEFAULT as neighbouring consts, and `from_getter` reads one with the other.
So the engine's default is derivable, and the portal's copy of it can be checked rather than
trusted.

The pairing is on the IDENTIFIER, not the string, and that distinction is the point:

    pub const TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME: &str = "TS_BLOCK_SLAB_TARGET_BYTES";
    pub const DEFAULT_BLOCK_SLAB_TARGET_BYTES: u64 = 1 << 30;

A previous-name const spells itself differently from the variable it names, because it exists to
keep an older spelling working. Matching the portal's env name against the const IDENTIFIER would
find nothing for those and quietly pass, so the identifier pairs the two consts and the string is
what the portal must match.

Only literal const expressions are evaluated -- integers, `*`, `<<`, parentheses, and the two
booleans. Anything else is left UNCOMPARED rather than guessed at, so a wrong reading cannot
present itself as a finding.
"""
from __future__ import annotations

import io
import os
import re
import sys
import unittest
from typing import Dict, Optional

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
STORAGE_CONFIG = os.path.join(
    REPO, "crates", "temporalstore-rust", "src", "storage_config.rs")
CONFIG_FILE = os.path.join(REPO, "config", "temporalstore.toml")
sys.path.insert(0, TOOLS)

_NAME_CONST = re.compile(
    r'pub const (TS_[A-Z0-9_]+)\s*:\s*&str\s*=\s*"([A-Z0-9_]+)"\s*;')
_DEFAULT_CONST = re.compile(
    r'pub const DEFAULT_([A-Z0-9_]+)\s*:\s*[a-z0-9]+\s*=\s*([^;]+);')

_LITERAL = re.compile(r"^[\d_ ()*<]+$")

# 9 name/default pairs when this was written.
EXPECTED_PAIR_FLOOR = 8


def _value(expr: str) -> Optional[str]:
    expr = expr.strip()
    if expr in ("true", "false"):
        return "1" if expr == "true" else "0"
    if not _LITERAL.match(expr):
        return None
    try:
        return str(eval(expr.replace("_", ""), {"__builtins__": {}}, {}))  # noqa: S307
    except Exception:
        return None


def _engine_defaults() -> Dict[str, str]:
    """env name -> the default the engine uses, paired through the const identifier."""
    with open(STORAGE_CONFIG, encoding="utf-8") as handle:
        source = handle.read()
    defaults = {}
    for match in _DEFAULT_CONST.finditer(source):
        value = _value(match.group(2))
        if value is not None:
            defaults[match.group(1)] = value
    out = {}
    for match in _NAME_CONST.finditer(source):
        identifier, env = match.group(1), match.group(2)
        suffix = identifier[len("TS_"):]
        if suffix in defaults:
            out[env] = defaults[suffix]
    return out


class EngineSettingsOfferTheEngineDefaultTest(unittest.TestCase):

    def test_the_storage_config_is_where_this_says_it_is(self) -> None:
        self.assertTrue(
            os.path.exists(STORAGE_CONFIG),
            "%s is gone, so every assertion below compares an empty set" % STORAGE_CONFIG)

    def test_the_scan_still_pairs_names_with_defaults(self) -> None:
        pairs = _engine_defaults()
        self.assertGreaterEqual(
            len(pairs), EXPECTED_PAIR_FLOOR,
            "paired %d env names with a default, expected at least %d -- if the const shape "
            "changed, the comparison below runs on nothing"
            % (len(pairs), EXPECTED_PAIR_FLOOR))

    def test_the_pairing_survives_a_name_that_differs_from_its_identifier(self) -> None:
        # The case this file exists to not miss: identifier SLAB, variable SEGMENT.
        pairs = _engine_defaults()
        self.assertIn(
            "TS_BLOCK_SLAB_TARGET_BYTES", pairs,
            "the slab/segment pair is no longer resolved. Either it was renamed -- fine, say so "
            "here -- or the pairing has quietly gone back to matching on the env string, which "
            "would drop every const whose identifier differs from the variable it names.")

    def test_the_portal_offers_what_the_engine_uses(self) -> None:
        import matrixark_gateway_config as cfgmod
        pairs = _engine_defaults()
        checked = 0
        for setting in cfgmod.SETTINGS:
            engine = pairs.get(setting.env)
            if engine is None:
                continue
            checked += 1
            with self.subTest(env=setting.env):
                self.assertEqual(
                    engine, setting.default,
                    "the portal offers %s as the default for %s; the engine uses %s when nothing "
                    "is set" % (setting.default, setting.env, engine))
        self.assertTrue(
            checked, "no engine setting on the portal matched a name in storage_config.rs, so "
                     "this compared nothing at all")


#: Keys the shipped config file sets to something OTHER than the engine's default, with the reason.
#: Not a skip list -- the set is asserted exactly, so a new disagreement fails here and a resolved
#: one fails too. The file's own header says it "documents and pins the defaults", so a key that
#: pins something else is either a deliberate deployment choice that should say so, or drift.
CONFIG_DISAGREES = {
    # The engine defaults this ON -- a cold scan does not evict warm entries for pages it will not
    # read again. The file pins it OFF, actively, while its neighbours in the same block
    # (stream_max_blob_size, compaction_watermark_bytes, page_index_cache_bytes,
    # block_index_cache_bytes) are all commented out with "0/unset = engine default", which is the
    # file's convention for leaving the engine alone. So this one line makes every deployment
    # launched through with_config.sh fill the cache on cold scans, while the portal displays 1.
    # Unexplained: it reads as an accident rather than a choice, and flipping it is a behaviour
    # change on the serving path, so it is recorded here for a decision rather than corrected in
    # passing.
    "TS_COLD_SCAN_NO_CACHE_FILL": ("1", "0"),
}


class TheShippedConfigMatchesTheEngineTest(unittest.TestCase):
    """The file a deployment loads must not quietly pin something the engine does not default to.

    `test_the_portal_offers_what_the_engine_uses` above compares the PORTAL against the engine, and
    that is a different surface from the one a deployment reads. `tools/deploy_config.sh` installs
    `config/temporalstore.toml` beside `scripts/with_config.sh`, and that script exports every
    active key before the service starts -- so the file wins over the engine's default and loses to
    an explicit environment variable. A disagreement there is invisible to the portal check and to
    the operator, who sees the portal's number.
    """

    @staticmethod
    def _file_values():
        """env var -> the value the shipped file sets ACTIVELY (commented-out keys are not set)."""
        import matrixark_load_config as loader

        with io.open(CONFIG_FILE, encoding="utf-8") as handle:
            text = handle.read()
        by_env = {}
        for key, env in dict(getattr(loader, "ENV_MAP", {})).items():
            by_env.setdefault(env, []).append(key)
        values = {}
        for env, keys in by_env.items():
            for key in keys:
                name = key.split(".", 1)[1]
                match = re.search(r"^[ \t]*%s[ \t]*=[ \t]*([^#\n]+)" % re.escape(name),
                                  text, re.M)
                if match:
                    values[env] = match.group(1).strip()
                    break
        return values

    def test_it_is_comparing_something(self) -> None:
        """A file whose keys stopped resolving would agree with everything."""
        shared = set(self._file_values()) & set(_engine_defaults())
        self.assertGreaterEqual(
            len(shared), 3,
            "only %d engine knobs are set actively by the shipped file and resolvable from "
            "storage_config.rs, so this compares almost nothing" % len(shared))

    def test_the_file_pins_what_the_engine_defaults_to(self) -> None:
        values = self._file_values()
        disagreeing = {}
        for env, engine in _engine_defaults().items():
            declared = values.get(env)
            if declared is None:
                continue
            if str(declared) != str(engine):
                disagreeing[env] = (str(engine), str(declared))
        self.assertEqual(
            CONFIG_DISAGREES, disagreeing,
            "the shipped config file pins a value the engine does not default to. Every "
            "deployment launched through with_config.sh gets the file's value while the portal "
            "displays the engine's -- so say why here, or make the file agree.")


ENGINE_SRC = os.path.join(REPO, "crates", "temporalstore-rust", "src")

_READ_SITE = re.compile(r'std::env::var\s*\(\s*"([A-Z0-9_]+)"\s*\)')
_WORD_SET = re.compile(r'(?:"[a-z0-9]+"\s*\|\s*)+"[a-z0-9]+"')
_UNWRAP_OR = re.compile(r'\.unwrap_or\s*\(\s*([0-9_]+)\s*\)')

#: The two vocabularies a boolean read compares against, and what each says about the default.
#: `!matches!(value, "0"|"false"|"no"|"off")` is a flag that OPTS OUT -- unset reads as on. Its
#: mirror `matches!(value, "1"|"true"|"yes"|"on")` OPTS IN -- unset reads as off. Any other word
#: set is left uncompared rather than guessed at, which is the rule the const reader above
#: applies to a non-literal expression.
_OPT_OUT_WORDS = frozenset(("0", "false", "no", "off"))
_OPT_IN_WORDS = frozenset(("1", "true", "yes", "on"))

#: A floor for the read-site scan. Set from what it is FOR, not from a measurement: a scan that
#: stopped recognising the shapes returns approximately nothing, and one that still reads the
#: tree returns many times this. 20 fails loudly on the first and never tracks the second.
EXPECTED_READ_SITE_FLOOR = 20

#: Portal settings in the storage_engine section that no engine read site names. Asserted
#: exactly, so a NEW engine knob arriving on the page without a comparable read fails here
#: rather than joining a silent majority -- which is what fourteen of the twenty-two were.
#:
#: All six are MATRIXARK_ rather than TS_, and that is the reason: they are read by the Python
#: index writer, not by the Rust engine. They sit in the storage_engine SECTION because that is
#: what they decide about, not because the engine reads them.
UNCOMPARED_ENGINE_SETTINGS = (
    "MATRIXARK_INDEX_KEYWORD_LIMIT",
    "MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS",
    "MATRIXARK_INDEX_POSTING_LISTS",
    "MATRIXARK_INDEX_SKIP_OWNER_DERIVABLE_TERMS",
    "MATRIXARK_MAX_SECONDARY_INDEX_REFS_PER_POSTING",
    "MATRIXARK_MAX_SECONDARY_INDEX_TERMS_PER_RECORD",
)

#: env name -> (what the read site applies, what the portal declares), with the reason beside it.
#: Asserted exactly in both directions, like CONFIG_DISAGREES above.
INLINE_DISAGREES = {
    # The portal says 128000, and so does the PYTHON resolver it was written from:
    # `matrixark_mcp_runtime_config.DEFAULT_HOOK_MAX_CONTEXT_TOKENS`. The RUST codex context hook
    # falls back to 1024 at its own read site, in bin/codex_context_hook.rs, so on a deployment
    # that has not set the variable the budget an agent gets is decided by which hook binary ran
    # -- 128000 or 1024, a factor of 125.
    #
    # The help text on this setting already carries the 10000-against-500000 story the
    # installation manual creates. This is a THIRD number, and none of that prose mentions it,
    # because nothing compared the portal against a read site until now. Which fallback is right
    # for an unconfigured agent hook is a product decision, so it is recorded rather than changed
    # in a test.
    "MATRIXARK_HOOK_MAX_CONTEXT_TOKENS": ("1024", "128000"),
}


def _read_site_defaults() -> Dict[str, str]:
    """env name -> the default its OWN read site applies, for reads outside storage_config.rs.

    `_engine_defaults` above answers for one family: the knobs whose name and default are
    declared as neighbouring consts. Most engine reads are not in that family -- they call
    `std::env::var` where they are used and keep the fallback at the call. That is where the
    other fourteen of the portal twenty-two storage_engine knobs live, and none was compared
    against anything.

    A name read in more than one place must agree with itself, or it is left out. That rule does
    real work rather than being careful in the abstract: a `#[cfg(test)]` block that sets a flag
    and reads it back would otherwise be able to supply the default this compares against.
    """
    sites: Dict[str, set] = {}
    for base, _dirs, files in os.walk(ENGINE_SRC):
        for name in sorted(files):
            if not name.endswith(".rs"):
                continue
            with io.open(os.path.join(base, name), encoding="utf-8", errors="replace") as handle:
                text = handle.read()
            for found in _READ_SITE.finditer(text):
                variable = found.group(1)
                before = text[max(0, found.start() - 60):found.start()]
                after = text[found.end():found.end() + 260]
                value = None
                if "matches!" in before:
                    words = _WORD_SET.search(after)
                    if words:
                        got = frozenset(part.strip().strip(chr(34))
                                        for part in words.group(0).split("|"))
                        if "!matches!" in before and got == _OPT_OUT_WORDS:
                            value = "1"
                        elif "!matches!" not in before and got == _OPT_IN_WORDS:
                            value = "0"
                else:
                    number = _UNWRAP_OR.search(after)
                    if number:
                        value = str(int(number.group(1).replace("_", "")))
                if value is not None:
                    sites.setdefault(variable, set()).add(value)
    return {variable: next(iter(values))
            for variable, values in sites.items() if len(values) == 1}


class ThePortalIsComparedToTheReadSiteTooTest(unittest.TestCase):
    """The same question as the first test, asked of the reads that declare no const.

    Eight of the portal twenty-two storage_engine knobs were compared against the engine. The
    other fourteen sat on the page under the same mirror design and were checked by nothing,
    because the const reader can only see the family that declares a name const and a default
    const side by side.
    """

    def test_the_scan_reads_the_engine(self) -> None:
        found = _read_site_defaults()
        self.assertGreaterEqual(
            len(found), EXPECTED_READ_SITE_FLOOR,
            "only %d engine read sites resolved to a default, so the comparison below is about "
            "almost nothing -- the shapes this recognises have probably changed" % len(found))

    def test_it_reaches_the_knobs_the_const_reader_cannot(self) -> None:
        """A positive control. Agreement is also what a scan that matched nothing reports."""
        import matrixark_gateway_config as cfgmod
        consts, sites = _engine_defaults(), _read_site_defaults()
        reached = [setting.env for setting in cfgmod.SETTINGS
                   if setting.env in sites and setting.env not in consts]
        self.assertGreaterEqual(
            len(reached), 8,
            "this reaches %d portal settings the const reader does not; it reached twelve when "
            "it was written, and below eight it has stopped adding coverage: %r"
            % (len(reached), reached))

    def test_the_portal_offers_what_the_read_site_uses(self) -> None:
        import matrixark_gateway_config as cfgmod
        sites = _read_site_defaults()
        disagreeing = {}
        for setting in cfgmod.SETTINGS:
            engine = sites.get(setting.env)
            if engine is None or str(engine) == str(setting.default):
                continue
            disagreeing[setting.env] = (str(engine), str(setting.default))
        self.assertEqual(
            INLINE_DISAGREES, disagreeing,
            "the portal displays a default that the code reading the variable does not apply. An "
            "operator who has not set it sees this number and gets the other one, so say why "
            "here, or make them agree.")

    def test_every_engine_setting_is_compared_or_named(self) -> None:
        import matrixark_gateway_config as cfgmod
        consts, sites = _engine_defaults(), _read_site_defaults()
        uncompared = tuple(
            setting.env for setting in cfgmod.SETTINGS
            if setting.key.split(".", 1)[0] == "storage_engine"
            and setting.env not in consts and setting.env not in sites)
        self.assertEqual(
            UNCOMPARED_ENGINE_SETTINGS, uncompared,
            "a storage_engine setting is compared against nothing. The claim the section makes is "
            "that the page mirrors what the engine does, so either the read is findable and this "
            "should compare it, or it is not an engine knob and belongs beside the six above "
            "with the reason why.")


if __name__ == "__main__":
    unittest.main()
