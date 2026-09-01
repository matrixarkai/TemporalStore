#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A deployment plan has to describe the deployment that will exist, not the one that was asked for.

Every case here is one where the engine accepts a setting and produces something else. None of them
raise, none are logged as a refusal, and all of them yield a deployment that starts and serves --
which is exactly why a chooser that only records the request is worse than no chooser at all: it
puts a confident label on the wrong thing.

The resolution order under test is `StorageBackendConfig::resolve_decision`, and the standalone
derivation is the datanode's. Both are mirrored here rather than guessed; if either moves, these
tests are the thing that should break.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_deployment_plan as dp  # noqa: E402


class SilentFallthroughTest(unittest.TestCase):
    """The three ways a choice becomes a different deployment with nothing said."""

    def test_shared_without_a_directory_is_not_shared(self) -> None:
        # `TS_STORAGE_BACKEND=shared` with no directory falls through to auto rather than failing,
        # so the deployment comes up on whatever auto picks. Asking for shared storage and getting
        # single-node raft is the kind of thing that is only noticed under load.
        plan = dp.plan("shared", "path", nodes=3)
        self.assertFalse(plan["ok"])
        self.assertTrue(any("TS_SHARED_STORE_DIR" in b for b in plan["blocking"]),
                        plan["blocking"])

    def test_matrixobject_without_the_feature_is_not_matrixobject(self) -> None:
        plan = dp.plan("shared", "matrixobject", nodes=3, matrixobject_available=False)
        self.assertNotEqual("matrixobject", plan["resolved_backend"])
        self.assertTrue(any("falls through to auto" in w for w in plan["warnings"]),
                        plan["warnings"])

    def test_matrixobject_with_the_feature_is_honoured(self) -> None:
        plan = dp.plan("shared", "matrixobject", nodes=3, matrixobject_available=True)
        self.assertTrue(plan["ok"], plan["blocking"])
        self.assertEqual("matrixobject", plan["resolved_backend"])
        self.assertEqual([], [w for w in plan["warnings"] if "falls through" in w])

    def test_a_metaserver_address_alone_makes_a_box_distributed(self) -> None:
        # Standalone is derived, not defaulted: !(meta_addr_is_real || TS_DISTRIBUTED). So a
        # metaserver address arriving from a config file is enough to change the topology of a
        # deployment nobody meant to change.
        self.assertTrue(dp.is_standalone({}))
        self.assertFalse(dp.is_standalone({"TS_META_ADDR": "10.0.0.4:17001"}))
        # ...unless it is one of the sentinels the engine reads as "no metaserver".
        for sentinel in dp.META_SENTINELS:
            with self.subTest(sentinel=sentinel):
                self.assertTrue(dp.is_standalone({"TS_META_ADDR": sentinel}))

    def test_a_onebox_plan_pins_standalone_rather_than_relying_on_the_default(self) -> None:
        plan = dp.plan("onebox", "ebs")
        self.assertTrue(plan["ok"], plan["blocking"])
        self.assertEqual("1", plan["env"]["TS_STANDALONE"])
        # The pin has to survive a metaserver address turning up later, which is the whole point.
        polluted = dict(plan["env"])
        polluted["TS_META_ADDR"] = "10.0.0.4:17001"
        self.assertTrue(dp.is_standalone(polluted),
                        "an inherited metaserver address flipped a one-box deployment")


class BackendResolutionTest(unittest.TestCase):
    """The order auto walks, which decides what every unpinned deployment gets."""

    def test_auto_prefers_a_reachable_endpoint_then_local_then_shared_then_raft(self) -> None:
        reachable = dp.resolve_backend({"MATRIXARK_OBJECT_RPC_URL": "http://store:9000"},
                                       matrixobject_available=True, endpoint_reachable=True)
        self.assertEqual("matrixobject", reachable["backend"])

        # Configured but unreachable degrades rather than wedging the node on a store it cannot
        # reach -- and lands somewhere other than MatrixObject.
        unreachable = dp.resolve_backend({"MATRIXARK_OBJECT_RPC_URL": "http://store:9000"},
                                         matrixobject_available=True, endpoint_reachable=False)
        self.assertNotEqual("matrixobject", unreachable["backend"])

        shared = dp.resolve_backend({"TS_SHARED_STORE_DIR": "/srv/shared"},
                                    matrixobject_available=False)
        self.assertEqual("shared_path", shared["backend"])

        nothing = dp.resolve_backend({}, matrixobject_available=False)
        self.assertEqual("raft", nothing["backend"])

    def test_raft_is_forced_regardless_of_what_else_is_configured(self) -> None:
        forced = dp.resolve_backend(
            {"TS_STORAGE_BACKEND": "raft", "TS_SHARED_STORE_DIR": "/srv/shared"},
            matrixobject_available=True, endpoint_reachable=True)
        self.assertEqual("raft", forced["backend"])

    def test_the_spellings_the_engine_accepts_are_accepted_here(self) -> None:
        for spelling in ("shared", "shared_path", "shared_store", "path"):
            with self.subTest(spelling=spelling):
                got = dp.resolve_backend({"TS_STORAGE_BACKEND": spelling,
                                          "TS_SHARED_STORE_DIR": "/srv/s"})
                self.assertEqual("shared_path", got["backend"])
        for spelling in ("raft", "raft_replication", "replication"):
            with self.subTest(spelling=spelling):
                self.assertEqual("raft", dp.resolve_backend(
                    {"TS_STORAGE_BACKEND": spelling})["backend"])
        # An unknown value is not an error to the engine either: it means auto.
        self.assertEqual("raft", dp.resolve_backend(
            {"TS_STORAGE_BACKEND": "nonsense"}, matrixobject_available=False)["backend"])


class ShapeTest(unittest.TestCase):

    def test_raft_refuses_an_even_node_count(self) -> None:
        for count in (2, 4, 6):
            with self.subTest(nodes=count):
                plan = dp.plan("raft", "ebs", nodes=count)
                self.assertFalse(plan["ok"])
        for count in (3, 5):
            with self.subTest(nodes=count):
                self.assertTrue(dp.plan("raft", "ebs", nodes=count)["ok"])

    def test_raft_gets_its_own_wal_directory(self) -> None:
        plan = dp.plan("raft", "ebs", nodes=3, root="/data")
        self.assertEqual("/data/raft-wal", plan["env"]["TS_RAFT_WAL_DIR"])
        self.assertNotIn("TS_RAFT_WAL_DIR", dp.plan("onebox", "ebs")["env"])

    def test_instance_ssd_says_the_store_is_erased_on_stop(self) -> None:
        plan = dp.plan("onebox", "ssd")
        self.assertTrue(any("erased" in w for w in plan["warnings"]), plan["warnings"])
        self.assertEqual([], [w for w in dp.plan("onebox", "ebs")["warnings"] if "erased" in w])

    def test_a_onebox_plan_says_which_backend_this_build_will_pick(self) -> None:
        # The same plan resolves differently depending on what was compiled in, so leaving it
        # unstated means the answer is only discoverable from a log line after launch.
        with_feature = dp.plan("onebox", "ebs", matrixobject_available=True)
        without = dp.plan("onebox", "ebs", matrixobject_available=False)
        self.assertNotEqual(with_feature["resolved_backend"], without["resolved_backend"])
        for plan in (with_feature, without):
            self.assertTrue(any(plan["resolved_backend"] in n for n in plan["notes"]),
                            plan["notes"])

    def test_a_key_name_is_planned_but_its_value_never_is(self) -> None:
        plan = dp.plan("onebox", "ebs", key_envs=["DEEPSEEK_API_KEY"])
        rendered = dp.as_env_file(plan)
        self.assertNotIn("DEEPSEEK_API_KEY=", rendered,
                         "the plan document must never carry a credential")
        self.assertTrue(any("DEEPSEEK_API_KEY" in n for n in plan["notes"]))
        self.assertFalse(dp.plan("onebox", "ebs", key_envs=["not a var name"])["ok"])

    def test_the_catalogue_reports_what_this_build_can_honour(self) -> None:
        available = dp.catalogue(matrixobject_available=True)
        missing = dp.catalogue(matrixobject_available=False)
        self.assertTrue(available["matrixobject_available"])
        self.assertFalse(missing["matrixobject_available"])

        def store(doc, shape_id, store_id):
            shape = [s for s in doc["shapes"] if s["id"] == shape_id][0]
            return [s for s in shape["storage"] if s["id"] == store_id][0]

        self.assertTrue(store(available, "shared", "matrixobject")["available"])
        self.assertFalse(store(missing, "shared", "matrixobject")["available"])

    def test_an_unknown_shape_is_refused_rather_than_guessed(self) -> None:
        plan = dp.plan("wishful")
        self.assertFalse(plan["ok"])
        self.assertEqual({}, plan["env"])


class LaunchArtifactTest(unittest.TestCase):
    """The script a customer will actually run on a machine that will actually bill them.

    Two of these are security or data properties rather than formatting:

    * Instance user-data is readable from the metadata service by anything running on the box, and
      through `describe-instance-attribute` by anyone with the permission. A key written into it is
      a key published, so the artifact names variables and never carries a value.
    * An EBS volume that already holds a store must not be reformatted on boot. Instance SSD must
      be, because it comes back blank. Getting these the wrong way round destroys a durable store
      on an ordinary reboot, and the two scripts differ by one conditional.
    """

    def test_no_key_value_can_reach_user_data(self) -> None:
        plan = dp.plan("onebox", "ebs", key_envs=["DEEPSEEK_API_KEY", "OPENAI_API_KEY"])
        script = dp.cloud_init(plan)
        for name in ("DEEPSEEK_API_KEY", "OPENAI_API_KEY"):
            with self.subTest(key=name):
                # Named, and named only as a commented, unset line.
                self.assertIn("# %s=" % name, script)
                self.assertNotIn("\n%s=" % name, script,
                                 "a key variable was written with a value in user-data")

    def test_a_pinned_flag_is_not_listed_as_a_secret(self) -> None:
        # Regression: key names were recovered by reading the first word of each note, and the
        # one-box note opens "TS_STANDALONE is pinned to 1 ...", so a topology flag that is already
        # set in the same file was listed among the secrets to go and fetch.
        script = dp.cloud_init(dp.plan("onebox", "ebs", key_envs=["DEEPSEEK_API_KEY"]))
        self.assertNotIn("# TS_STANDALONE=", script)
        self.assertIn("TS_STANDALONE=1", script)

    def test_ephemeral_and_durable_disks_are_prepared_differently(self) -> None:
        ssd = dp.cloud_init(dp.plan("onebox", "ssd"))
        ebs = dp.cloud_init(dp.plan("onebox", "ebs"))
        # The ephemeral disk comes back blank, so it is formatted every boot.
        self.assertIn("mkfs.ext4 -F /dev/nvme1n1", ssd)
        self.assertNotIn("blkid", ssd)
        # The durable one is formatted only when it has no filesystem.
        self.assertIn("blkid", ebs)
        self.assertNotIn("\nmkfs.ext4 -F /dev/xvdf", ebs,
                         "an EBS volume is formatted unconditionally, which destroys the store "
                         "it already holds on the next reboot")

    def test_a_blocked_plan_produces_no_launch_script(self) -> None:
        for bad in (dp.plan("raft", "ebs", nodes=4),
                    dp.plan("shared", "path", nodes=3),
                    dp.plan("wishful")):
            with self.subTest(shape=bad["shape"], nodes=bad.get("nodes")):
                with self.assertRaises(ValueError):
                    dp.cloud_init(bad)

    def test_the_script_carries_exactly_the_plan_environment(self) -> None:
        plan = dp.plan("raft", "ebs", nodes=3, root="/data")
        script = dp.cloud_init(plan)
        for name, value in plan["env"].items():
            with self.subTest(var=name):
                self.assertIn("%s=%s" % (name, value), script)

    def test_teardown_comes_with_the_launch_not_after_it(self) -> None:
        # An instance whose termination command has to be reconstructed later is one that stays
        # running and keeps billing.
        commands = dp.launch_commands(dp.plan("onebox", "ebs"))
        self.assertIn("run-instances", commands["launch"])
        self.assertIn("terminate-instances", commands["teardown"])
        self.assertIn("matrixark-onebox", commands["teardown"])

    def test_a_durable_volume_is_requested_only_when_one_is_chosen(self) -> None:
        ebs = dp.launch_commands(dp.plan("onebox", "ebs"))
        ssd = dp.launch_commands(dp.plan("onebox", "ssd"))
        self.assertIn("block-device-mappings", ebs["launch"])
        self.assertNotIn("block-device-mappings", ssd["launch"])

    def test_the_node_count_reaches_the_launch(self) -> None:
        commands = dp.launch_commands(dp.plan("raft", "ebs", nodes=5))
        self.assertIn("--count 5", commands["launch"])
        self.assertEqual(5, commands["instances"])

    def test_the_warnings_travel_with_the_script(self) -> None:
        script = dp.cloud_init(dp.plan("onebox", "ssd"))
        self.assertIn("erased", script,
                      "the script that formats an ephemeral disk does not say it is ephemeral")


if __name__ == "__main__":
    unittest.main()
