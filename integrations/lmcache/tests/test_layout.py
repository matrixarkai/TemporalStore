# SPDX-License-Identifier: Apache-2.0
import unittest

from lmcache_temporalstore.layout import layout_table_name


class LayoutTableNameTest(unittest.TestCase):
    def test_model_and_both_world_sizes_select_the_table(self):
        first = layout_table_name("model/ø", 8, 4)
        self.assertRegex(first, r"^kv_[0-9a-f]{16}$")
        self.assertEqual(first, layout_table_name("model/ø", 8, 4))
        self.assertNotEqual(first, layout_table_name("model/ø", 8, 8))
        self.assertNotEqual(first, layout_table_name("model/ø", 4, 4))
        self.assertNotEqual(first, layout_table_name("model/ö", 8, 4))
