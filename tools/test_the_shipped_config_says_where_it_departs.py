"""The shipped config file says where it departs from what this build runs.

``config/temporalstore.toml`` opens by saying it "documents and pins the defaults". For most keys
that is exactly true, and it is the reason the file is safe to launch through: a deployment that
runs ``scripts/with_config.sh`` gets the behaviour the code was built with.

For a handful of keys it is deliberately not true. The file ships a tuned profile -- ``top_k_per_
layer`` is raised from 8 to 24 "for richer default packs", cross-session breadth from 24 to 200 --
and those are decisions, not drift. The file names the old value in a comment next to each one.

The hazard is the key that departs and says nothing. Three ways it happens, all of them silent:

* a build default is retuned and the file keeps the old number, which now reads as a pin;
* a value is tuned in the file and the note is never written;
* a key is copied from a neighbouring line and lands a digit off.

Any of those makes the header's promise false for a key nobody knows about, and the file is the
documented way to launch: an operator following the README gets the departure without being told.
The same shape already bit the portal -- ``export_settings(include_defaults=True)`` wrote declared
defaults to a target as explicit values and multiplied a cloned deployment's budgets -- and
``_EXPLICIT_BUILD_DEFAULT`` in matrixark_gateway_config exists to stop it. Nothing was watching the
config file.

So: every key in the shipped file must either match what this build runs, or be recorded below with
a reason. The recorded set is asserted in BOTH directions. A new departure fails because it is not
in the table. A departure that goes away ALSO fails, because a stale entry describing a difference
that no longer exists is how a table like this rots into decoration.
"""

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import matrixark_gateway_config as gateway_config
import matrixark_load_config as load_config


REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
CONFIG_FILE = REPO_ROOT / "config" / "temporalstore.toml"


# variable -> (what the file pins, what this build runs, why the file departs)
#
# "why" is not decoration. A departure with no reason is indistinguishable from a mistake, and the
# point of the table is that someone had to write the sentence.
RECORDED_DEPARTURES = {
    "MATRIXARK_TOP_K_PER_LAYER": (
        "24", "8",
        "raised for richer default packs; the file says so on the line"),
    "MATRIXARK_MAX_GLOBAL_CANDIDATES": (
        "2048", "512",
        "raised with the rest of the breadth profile; min_score keeps junk out as the caps rise"),
    "MATRIXARK_CROSS_SESSION_MAX_CANDIDATES": (
        "200", "24",
        "raised for breadth; the file says so on the line -- was the main breadth limiter"),
    "MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES": (
        "200", "48",
        "raised to match the cross-session lane; the file says so on the line"),
    "MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS": (
        "500000", "262144",
        "the absolute cross-session cap is aligned with gateway_default_max_context_tokens "
        "(500000) rather than left at the 256Ki build constant"),
    "MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE": (
        "1024", "256",
        "raw-event retention is kept wider than the build constant so compression has a window "
        "to work over"),
    # MATRIXARK_GATEWAY_DEFAULT_MAX_CONTEXT_TOKENS was recorded here as the one departure
    # pointing the other way -- the file and the build both said 500000 and the PORTAL was
    # blank -- pending a sweep that could follow a name through an import fallback. That is
    # done, the setting is in _EXPLICIT_BUILD_DEFAULT, and all three registries now agree,
    # so the entry is struck. This guard asserts its record in both directions, which is
    # what required striking it rather than leaving a true-sounding line behind.
    "TS_COLD_SCAN_NO_CACHE_FILL": (
        "0", "1",
        "UNEXPLAINED. The engine default is true -- cold scans bypass cache fill to avoid "
        "polluting the cache -- and the file turns that off with no note. Recorded so it is "
        "visible, NOT endorsed: whoever knows why should write the reason here or drop the line."),
}


def _shipped_values():
    """Every SECTION.key actually written in the shipped file, resolved to its variable."""
    doc = load_config.load_toml(CONFIG_FILE)
    env_map = dict(load_config.ENV_MAP)
    shipped = {}
    for section, body in doc.items():
        if not isinstance(body, dict):
            continue
        for key, value in body.items():
            variable = env_map.get("%s.%s" % (section, key))
            if variable is not None:
                shipped[variable] = (("%s.%s" % (section, key)), value)
    return shipped


def _build_defaults():
    """What this build runs for each variable, as the portal reports it.

    The portal is the right source rather than a second hand-written table: it has already been
    taught to report the build's number rather than a declared one, by ``_build_default`` for the
    tenant knobs and ``_EXPLICIT_BUILD_DEFAULT`` for the hand-written settings. Reading it here
    chains the three registries into one answer -- file, portal and build agree, or a test says
    which pair does not.
    """
    settings = (gateway_config.all_settings() if hasattr(gateway_config, "all_settings")
                else gateway_config.SETTINGS)
    defaults = {}
    for setting in settings:
        variable = getattr(setting, "env", None) or getattr(setting, "env_var", None)
        if variable:
            defaults[variable] = setting.default
    return defaults


def _same(left, right):
    """Compare as numbers when both are numbers, so 0.50 and 0.5 are not a difference."""
    if isinstance(left, bool):
        left = "1" if left else "0"
    if isinstance(right, bool):
        right = "1" if right else "0"
    left, right = str(left).strip(), str(right).strip()
    try:
        return abs(float(left) - float(right)) < 1e-12
    except (TypeError, ValueError):
        return left == right


class ShippedConfigDeparturesTest(unittest.TestCase):

    def test_the_scan_sees_the_file(self):
        """The guard below is worthless if nothing was compared. Fail on an empty scan, loudly.

        A key rename, a parser that quietly returns {} on a syntax it does not know, a moved file:
        each of those turns the real test into a pass over nothing.
        """
        shipped = _shipped_values()
        defaults = _build_defaults()
        comparable = set(shipped) & set(defaults)
        self.assertGreater(
            len(shipped), 50,
            "read %d keys from %s -- the file has ~86; the parser or the path is wrong"
            % (len(shipped), CONFIG_FILE))
        self.assertGreater(
            len(comparable), 40,
            "only %d of %d shipped keys resolve to a portal setting to compare against; "
            "the comparison is too thin to mean anything" % (len(comparable), len(shipped)))

    def test_every_departure_from_the_build_is_recorded_with_a_reason(self):
        shipped = _shipped_values()
        defaults = _build_defaults()

        found = {}
        for variable, (key, value) in sorted(shipped.items()):
            if variable not in defaults:
                continue
            build = defaults[variable]
            if not _same(value, build):
                found[variable] = (key, value, build)

        unrecorded = sorted(set(found) - set(RECORDED_DEPARTURES))
        self.assertFalse(
            unrecorded,
            "the shipped config departs from what this build runs, with nothing saying so:\n"
            + "\n".join(
                "    %s  (%s) file=%s build=%s" % (v, found[v][0], found[v][1], found[v][2])
                for v in unrecorded)
            + "\n\nEither make the file match the build, or add the key to RECORDED_DEPARTURES "
              "with the reason it differs. The file's own header promises it pins the defaults, "
              "and an operator launching through scripts/with_config.sh believes it.")

        # The other direction. A recorded departure that no longer exists means the table is
        # describing the past, and the next reader trusts it anyway.
        resolved = sorted(set(RECORDED_DEPARTURES) - set(found))
        self.assertFalse(
            resolved,
            "these are recorded as departures but the file and the build now agree:\n"
            + "\n".join("    %s" % v for v in resolved)
            + "\n\nDrop them from RECORDED_DEPARTURES. A table that keeps entries after they stop "
              "being true stops being read.")

    def test_a_recorded_departure_still_says_the_same_two_numbers(self):
        """The reason is attached to a specific pair of values. If either moves, it may not hold."""
        shipped = _shipped_values()
        defaults = _build_defaults()
        drifted = []
        for variable, (was_file, was_build, _reason) in sorted(RECORDED_DEPARTURES.items()):
            if variable not in shipped or variable not in defaults:
                continue
            now_file = shipped[variable][1]
            now_build = defaults[variable]
            if not _same(now_file, was_file) or not _same(now_build, was_build):
                drifted.append("    %s  recorded file=%s build=%s, now file=%s build=%s"
                               % (variable, was_file, was_build, now_file, now_build))
        self.assertFalse(
            drifted,
            "a recorded departure now names different numbers than when its reason was written:\n"
            + "\n".join(drifted)
            + "\n\nRe-read the reason and update it, or the entry now explains a difference that "
              "is not the one present.")

    def test_every_recorded_departure_has_a_reason_worth_reading(self):
        """A one-word reason is the same as no reason; it just gets past the other test."""
        for variable, (_file_value, _build_value, reason) in sorted(RECORDED_DEPARTURES.items()):
            self.assertGreaterEqual(
                len(reason.split()), 6,
                "%s is recorded with a reason too short to tell anyone anything: %r"
                % (variable, reason))


if __name__ == "__main__":
    unittest.main()
