# SPDX-License-Identifier: Apache-2.0
import unittest

from temporalstore_sdk import commands, decode_hash_entries, decode_integer, decode_value, decode_values


class BinaryResponseTest(unittest.TestCase):
    def test_missing_and_empty_scalar_values_remain_distinct(self):
        self.assertIsNone(decode_value(commands.CommandResponse(value_present=False)))
        self.assertEqual(decode_value(commands.CommandResponse(value_present=True)), b"")
        with self.assertRaisesRegex(ValueError, "presence"):
            decode_value(commands.CommandResponse())

    def test_projection_preserves_presence_across_bitmap_bytes(self):
        response = commands.CommandResponse(
            values=[b"", b"", bytes((0, 255)), b"", b"", b"", b"", b"", b"", b""],
            values_present=bytes((5, 2)),
        )
        decoded = decode_values(commands.CommandResponse.FromString(response.SerializeToString()))
        self.assertEqual(decoded, [b"", None, bytes((0, 255)), None, None, None, None, None, None, b""])
        self.assertEqual(decode_values(commands.CommandResponse(values_present=b"")), [])
        with self.assertRaisesRegex(ValueError, "presence"):
            decode_values(commands.CommandResponse(values=[b""]))
        with self.assertRaisesRegex(ValueError, "invalid"):
            decode_values(commands.CommandResponse(values=[b""], values_present=b""))

    def test_hash_entries_keep_empty_data_and_field_order(self):
        response = commands.CommandResponse(values=[b"", bytes((128, 255))], entry_names=["b", "a"])
        self.assertEqual(decode_hash_entries(response), [("b", b""), ("a", bytes((128, 255)))])
        with self.assertRaisesRegex(ValueError, "field names"):
            decode_hash_entries(commands.CommandResponse(values=[b"old response"]))


    def test_signed_integer_preserves_ttl_sentinels_and_zero(self):
        for value in (-2, -1, 0, 10):
            self.assertEqual(decode_integer(commands.CommandResponse(integer=value)), value)
        with self.assertRaisesRegex(ValueError, "signed integer"):
            decode_integer(commands.CommandResponse())
