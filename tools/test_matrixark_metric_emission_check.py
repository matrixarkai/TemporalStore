#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The metric conformance tool could not see an emitter that stopped emitting.

`tools/validate_grafana_metrics_conformance.py` exists to keep panels and emitted series in step.
Measured on the tree before this file was written:

* blinding a PANEL -- renaming the family in the dashboard JSON -- failed the tool correctly;
* deleting the family from the EMITTER did NOT fail it, because the engine side asked whether the
  name appeared ANYWHERE in the Rust source, and a `# HELP` line carries the name.

So the failure it most needs to catch -- a series that silently stops being produced while a panel
keeps asking for it -- was the one failure it could not see. A dashboard would draw a flat line and
the tool would stay green.

The engine side now asks whether the family is EMITTED: a sample line written by a module that
renders exposition text, declaration lines and comments removed. Mutating the tree in all four
directions, with the old tool run against the same tree as a control:

    emitter sample deleted, panel kept        new FIRED   old passed
    panel blinded, emitter kept               new FIRED   old FIRED
    declaration and a comment only            new FIRED   old passed
    emitted, named by no panel or alert        new passed  old passed

The last row is the one that keeps the fix from being noisy: a family nothing queries is not a
failure, and a check that reported it would be turned off within a week.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import validate_grafana_metrics_conformance as validator  # noqa: E402


HEALTHY = '''
fn render(state: &State) -> String {
    let mut out = String::new();
    out.push_str("# HELP temporalstore_probe_live Written every round.\\n");
    out.push_str("# TYPE temporalstore_probe_live gauge\\n");
    push(&mut out, "temporalstore_probe_live", &[], state.live);
    out
}
'''

DECLARED_ONLY = '''
fn render(state: &State) -> String {
    let mut out = String::new();
    out.push_str("# HELP temporalstore_probe_live Written every round.\\n");
    out.push_str("# TYPE temporalstore_probe_live gauge\\n");
    // temporalstore_probe_live used to be pushed here.
    out
}
'''


class TheEngineSideAsksWhetherAFamilyIsEmittedTest(unittest.TestCase):
    """The defect this file is named for: declared is not emitted."""

    def _emitted(self, body: str) -> tuple:
        names, sites = validator.emitted_metric_names({"probe.rs": body})
        return set(names), sites

    def test_a_family_with_a_sample_line_is_emitted(self) -> None:
        names, sites = self._emitted(HEALTHY)
        self.assertIn("temporalstore_probe_live", names)
        self.assertEqual(1, sites, "one sample line should be one sample site")

    def test_a_family_left_with_only_a_declaration_is_not_emitted(self) -> None:
        """The whole finding in one assertion.

        `# HELP` and `# TYPE` both carry the family name, so "does the name appear in the source"
        answers yes for a family whose sample lines are gone. It has to answer no.
        """
        names, sites = self._emitted(DECLARED_ONLY)
        self.assertNotIn("temporalstore_probe_live", names)
        self.assertEqual(0, sites)

    def test_the_loose_test_this_replaces_cannot_tell_them_apart(self) -> None:
        """A control: the old rule passes on BOTH bodies, so it was measuring something else."""
        self.assertIn("temporalstore_probe_live", validator.metric_names(HEALTHY))
        self.assertIn("temporalstore_probe_live", validator.metric_names(DECLARED_ONLY))

    def test_a_module_that_declares_nothing_is_not_a_renderer(self) -> None:
        """`proxy.rs` maps families to panels and `ops_scale_readiness_harness.rs` lists the ones
        it EXPECTS. Both write the name in the shape a renderer does and neither emits anything."""
        intentions = '''
        fn expectations() -> Vec<&'static str> {
            vec!["temporalstore_probe_live", "temporalstore_probe_other"]
        }
        '''
        self.assertIsNone(validator.EXPOSITION_DECLARATION.search(intentions))

    def test_a_test_module_does_not_count_as_an_emitter(self) -> None:
        body = HEALTHY + '''
#[cfg(test)]
mod tests {
    #[test]
    fn it_renders() {
        assert!(out.contains("temporalstore_probe_test_only{kind=\\"x\\"} 1"));
    }
}
'''
        names, _ = self._emitted(validator.strip_test_modules(body))
        self.assertIn("temporalstore_probe_live", names)
        self.assertNotIn("temporalstore_probe_test_only", names,
                         "a family only a test writes is not one a deployment emits")


class EveryFamilyDeclaresBothHalvesTest(unittest.TestCase):
    """`# HELP` and `# TYPE` are cheap to check and were wrong twice.

    Three slab families kept a declaration after the series behind them was collapsed, and the
    gateway published two cdylib counters with a type and no description. The halves are asserted
    separately because they fail for opposite reasons.
    """

    def test_help_without_type_is_reported(self) -> None:
        helps, kinds = validator.declaration_pairs([
            '"# HELP temporalstore_probe_half Only half declared.\\n"'])
        self.assertEqual(["help_without_type:probe:temporalstore_probe_half"],
                         validator.check_declarations_are_paired("probe", helps, kinds))

    def test_type_without_help_is_reported(self) -> None:
        helps, kinds = validator.declaration_pairs(['"# TYPE temporalstore_probe_half counter"'])
        self.assertEqual(["type_without_help:probe:temporalstore_probe_half"],
                         validator.check_declarations_are_paired("probe", helps, kinds))

    def test_a_fully_declared_family_is_not_reported(self) -> None:
        helps, kinds = validator.declaration_pairs([HEALTHY])
        self.assertEqual([], validator.check_declarations_are_paired("probe", helps, kinds))

    def test_an_empty_scan_is_reported_rather_than_passing(self) -> None:
        """Nothing to compare is not the same as nothing wrong."""
        self.assertEqual(["declaration_pairing_scan_empty:probe"],
                         validator.check_declarations_are_paired("probe", set(), {}))

    def test_a_templated_family_name_is_not_recorded(self) -> None:
        """The metaserver builds one family name at runtime. Recording the fixed prefix would
        invent a family that is never published, and then report it as half-declared forever."""
        helps, kinds = validator.declaration_pairs([
            '"# HELP temporalstore_meta_server_{name}_total {help}\\n"'])
        self.assertEqual(set(), helps)
        self.assertEqual({}, kinds)

    def test_the_live_tree_declares_both_halves_everywhere(self) -> None:
        engine_helps, engine_kinds = validator.declaration_pairs(validator.RENDERERS.values())
        gateway_helps, gateway_kinds = validator.declaration_pairs(
            [validator.python_metric_text()])
        self.assertEqual([], validator.check_declarations_are_paired(
            "engine", engine_helps, engine_kinds))
        self.assertEqual([], validator.check_declarations_are_paired(
            "gateway", gateway_helps, gateway_kinds))


class TheEmissionScanSaysHowMuchItSawTest(unittest.TestCase):
    """A tool that checks zero metrics passes beautifully."""

    def setUp(self) -> None:
        self.sites, self.count = validator.emitted_metric_names()

    def test_the_scan_found_a_plausible_number_of_sample_sites(self) -> None:
        self.assertEqual([], validator.check_emission_scan_extent(
            set(self.sites), self.count, validator.RENDERERS))
        self.assertGreaterEqual(self.count, validator.SAMPLE_SITE_FLOOR)
        self.assertGreaterEqual(len(self.sites), validator.EMITTED_FAMILY_FLOOR)

    def test_an_empty_scan_fails_instead_of_passing_vacuously(self) -> None:
        failures = validator.check_emission_scan_extent(set(), 0, {})
        self.assertTrue(failures)
        self.assertEqual(len(validator.RENDERER_FLOOR),
                         len([f for f in failures if f.startswith("renderer_no_longer")]))

    def test_every_renderer_in_the_floor_is_still_discovered(self) -> None:
        found = {str(path.relative_to(validator.RUST_SRC_ROOT)).replace("\\", "/")
                 for path in validator.RENDERERS}
        self.assertEqual([], sorted(set(validator.RENDERER_FLOOR) - found))

    def test_every_family_the_spec_names_has_a_sample_site(self) -> None:
        """The denominator, stated: 56 families across 8 groups, and none of them exempt."""
        emitted = validator.expand_emitted_component_series(
            set(self.sites), validator.declaration_pairs(validator.RENDERERS.values())[1])
        named = [name for group in validator.METRIC_FAMILIES.values() for name in group["rust"]]
        self.assertGreater(len(named), 40, "almost nothing is being checked")
        self.assertEqual([], sorted(name for name in named if name not in emitted))

    def test_a_histogram_counts_as_emitted_through_its_component_series(self) -> None:
        """There is no series under a histogram's base name, only `_bucket`/`_sum`/`_count`."""
        kinds = {"temporalstore_probe_hist": "histogram"}
        emitted = validator.expand_emitted_component_series(
            {"temporalstore_probe_hist_bucket"}, kinds)
        self.assertIn("temporalstore_probe_hist", emitted)

    def test_a_histogram_with_no_series_at_all_stays_unemitted(self) -> None:
        """The expansion is conditional, or it would hand every declared histogram a free pass."""
        kinds = {"temporalstore_probe_hist": "histogram"}
        self.assertEqual(set(), validator.expand_emitted_component_series(set(), kinds))


if __name__ == "__main__":
    unittest.main()
