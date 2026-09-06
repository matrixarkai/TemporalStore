#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A message with no text carried 572 bytes describing how its text was selected.

Every message the hook builds gets a `codex_memory_selection` block -- policy, policy_counts,
selected and original char and line counts, ratios, stage. On a message with no content that block
reads `selected_text_chars: 0`, `original_text_chars: 0`, `dropped_text_chars: 0`,
`retained_text_ratio: 1.0`: a description of a selection that never happened.

A hook event carrying no text produces exactly that, and mx#1065 made those the common case by
stopping the event envelope from being used as the message text.

Only the both-empty case is dropped. The block earns its bytes precisely when a selection HAD text
and kept little or none of it, and `test_a_selection_that_dropped_everything_is_still_recorded` is
what keeps that from being thrown away with it.
"""
import json
import unittest

try:
    from tools.matrixark_agent_hook import hook_messages_from_payload
except ImportError:  # run from tools/
    from matrixark_agent_hook import hook_messages_from_payload


def _only(payload, *, event, text):
    messages = hook_messages_from_payload(payload, event=event, text=text)
    assert len(messages) == 1, messages
    return messages[0]


class SelectionOfNothingTests(unittest.TestCase):
    def test_an_empty_message_carries_no_selection_block(self):
        message = _only({"cwd": "x", "hook_event_name": "SessionStart"},
                        event="SessionStart", text="")
        self.assertEqual("", message["content"])
        self.assertNotIn("metadata", message)

    def test_the_block_was_worth_removing(self):
        """Positive control: it really is ~572 bytes, not a field or two."""
        payload = {"cwd": "x", "hook_event_name": "SessionStart"}
        message = _only(payload, event="SessionStart", text="some real text here")
        block = json.dumps(message.get("metadata") or {}, separators=(",", ":"))
        self.assertGreater(len(block), 300,
                           "the selection block is small, so this change is not worth having")

    def test_a_message_with_text_keeps_its_selection_block(self):
        message = _only({"hook_event_name": "UserPromptSubmit"},
                        event="UserPromptSubmit", text="please refactor the parser")
        self.assertIn("please refactor", message["content"])
        self.assertIn("codex_memory_selection", message.get("metadata", {}))

    def test_a_selection_that_dropped_everything_is_still_recorded(self):
        """The case the block exists for: text went in, nothing came out.

        This must NOT be swept up with the empty-message case -- a lossy selection is exactly what
        someone reading this telemetry later wants to find.
        """
        payload = {"hook_event_name": "PostToolUse",
                   "messages": [{"role": "tool", "content": "   \\n   \\n  "}]}
        messages = hook_messages_from_payload(payload, event="PostToolUse", text="")
        for message in messages:
            if not message.get("content"):
                self.assertIn("metadata", message,
                              "a selection with original text must still be described")

    def test_the_role_and_content_are_unchanged(self):
        message = _only({"cwd": "x", "hook_event_name": "SessionStart"},
                        event="SessionStart", text="")
        self.assertEqual({"role", "content"}, set(message))
        self.assertEqual("user", message["role"])


if __name__ == "__main__":
    unittest.main()
