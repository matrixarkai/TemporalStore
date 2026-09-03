"""An operator sizing a deployment must be able to see what the store keeps.

The event log rotates into fixed-size shards and keeps a bounded number of them, so those two
numbers are the disk ceiling AND a retention policy: records in a dropped shard are gone. They are
also what bounds ingest cost, because the retained window is what a read holds and compaction
walks — measured on 1 MB documents, per-document ingest climbed 2.68 s, 4.83 s, 7.35 s at 15, 30
and 60 documents and then flattened, and it flattened because rotation had begun.

Neither was offered on the portal, so neither could be seen or tuned.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrixark_gateway_config as config_module
import matrixark_mcp_local_adapter as adapter_module


OFFERED = {
    "ingestion.local_log_max_bytes": "MATRIXARK_LOCAL_JSONL_MAX_BYTES",
    "ingestion.local_log_retention_count": "MATRIXARK_LOCAL_JSONL_RETENTION_COUNT",
    "ingestion.durable_read_cache": "MATRIXARK_LOCAL_DURABLE_READ_CACHE_ENABLED",
}


class ThePortalShowsWhatTheStoreKeeps(unittest.TestCase):
    def setUp(self):
        self.by_key = {s.key: s for s in config_module.SETTINGS}

    def test_the_retention_knobs_are_offered(self):
        for key, env in sorted(OFFERED.items()):
            # assertTrue, not assertIn: assertIn renders the whole registry into the failure and
            # buries the one name that matters.
            self.assertTrue(
                key in self.by_key,
                "%s decides how much the store keeps and is not offered anywhere on the portal"
                % env,
            )
            self.assertEqual(self.by_key[key].env, env)

    def test_they_are_in_a_group_the_page_renders(self):
        for key in OFFERED:
            group = self.by_key[key].group
            self.assertIn(
                group, config_module.GROUPS,
                "%s is in group %r, which has no metadata, so the page cannot render it"
                % (key, group),
            )

    def test_the_defaults_match_what_the_store_actually_uses(self):
        """A portal that shows a default the code does not use is worse than showing nothing."""
        self.assertEqual(
            int(self.by_key["ingestion.local_log_max_bytes"].default),
            adapter_module.LOCAL_JSONL_MAX_BYTES,
        )
        self.assertEqual(
            int(self.by_key["ingestion.local_log_retention_count"].default),
            adapter_module.LOCAL_JSONL_RETENTION_COUNT,
        )

    def test_they_are_labelled_restart_because_they_are_read_at_import(self):
        """Claiming a change is live when it cannot be is the direction that misleads a customer."""
        for key in OFFERED:
            self.assertEqual(
                self.by_key[key].applies, "restart",
                "%s is read once at import, so offering it as a live change would be a lie" % key,
            )

    def test_each_one_says_what_it_costs(self):
        for key in OFFERED:
            note = self.by_key[key].help or ""
            self.assertGreater(
                len(note), 40,
                "%s is offered with no help text; a knob that bounds retention needs to say so"
                % key,
            )


if __name__ == "__main__":
    unittest.main()
