#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A hook event with no message content must not become a remembered turn.

`payload_text` falls back to `json.dumps(payload)[:4000]` when it finds no message anywhere, guarded
by `identity_only_payload`. The guard did not know `session_title`, `source`, `transcript_path`,
`model` or `prompt_id`, so a Claude Code `SessionStart` payload failed on its last keys and its whole
envelope was stored as a 313-character "user" message.

Retrieval then served those back as memory. Measured on a pack the hook actually injected: 16 of 18
items were `SessionStart` envelopes, filling 92% of the 8,000-character context budget with `cwd`,
`session_id` and `transcript_path` -- and crowding out the turns a reader wanted. Every one of those
characters is a token the model pays for on that turn.

The tests that matter here are the ones proving content still gets through: a guard that suppressed
real turns would be far worse than the waste it fixes.
"""
import unittest

try:
    from tools.matrixark_codex_hook import (
        IDENTITY_ONLY_PAYLOAD_KEYS,
        identity_only_payload,
        payload_text,
    )
    from tools.matrixark_agent_hook import hook_messages_from_payload
except ImportError:  # run from tools/
    from matrixark_codex_hook import (
        IDENTITY_ONLY_PAYLOAD_KEYS,
        identity_only_payload,
        payload_text,
    )
    from matrixark_agent_hook import hook_messages_from_payload

# Verbatim from a pack the hook injected on this machine.
SESSION_START = {
    "cwd": "C:/Users/Deeproute/.claude",
    "hook_event_name": "SessionStart",
    "session_id": "7face037-3318-4aac-bf5d-4f5121c92f61",
    "session_title": "TemporalStore Rust meta server parity",
    "source": "resume",
    "transcript_path": "C:/Users/Deeproute/.claude/projects/x.jsonl",
}


class EnvelopeIsNotATurnTests(unittest.TestCase):
    def test_a_session_start_envelope_produces_no_text(self):
        self.assertTrue(identity_only_payload(SESSION_START))
        self.assertEqual("", payload_text(SESSION_START, event="SessionStart"))

    def test_it_is_the_added_keys_that_do_it(self):
        """Positive control: without them the payload is not envelope-only.

        Removing any one of the five from the set must make the guard reject this payload again,
        which is what says the fix is these keys and not something incidental.
        """
        for key in ("session_title", "source", "transcript_path"):
            self.assertIn(key, IDENTITY_ONLY_PAYLOAD_KEYS)
            reduced = {k: v for k, v in IDENTITY_ONLY_PAYLOAD_KEYS.items()} \
                if isinstance(IDENTITY_ONLY_PAYLOAD_KEYS, dict) else set(IDENTITY_ONLY_PAYLOAD_KEYS)
            reduced.discard(key)

            def guard(value, allowed=reduced):
                if isinstance(value, dict):
                    if not value:
                        return True
                    for k, item in value.items():
                        if str(k) not in allowed:
                            return False
                        if isinstance(item, (dict, list)) and not guard(item, allowed):
                            return False
                    return True
                if isinstance(value, list):
                    return all(guard(i, allowed) for i in value)
                return True

            self.assertFalse(guard(SESSION_START),
                             f"without {key!r} the payload should not read as envelope-only")

    def test_a_prompt_still_gets_through(self):
        """The load-bearing one: a payload carrying content must not be suppressed."""
        payload = dict(SESSION_START, prompt="please refactor the parser")
        self.assertFalse(identity_only_payload(payload))
        self.assertIn("refactor", payload_text(payload, event="UserPromptSubmit"))

    def test_messages_still_get_through(self):
        payload = dict(SESSION_START, messages=[{"role": "user", "content": "hello there"}])
        self.assertFalse(identity_only_payload(payload))
        self.assertIn("hello there", payload_text(payload, event="UserPromptSubmit"))

    def test_the_envelope_does_not_reach_the_ingest_messages(self):
        """End to end: what the hook would store for this event carries no envelope text."""
        text = payload_text(SESSION_START, event="SessionStart")
        messages = hook_messages_from_payload(SESSION_START, event="SessionStart", text=text)
        for message in messages:
            content = str(message.get("content") or "")
            self.assertNotIn("transcript_path", content)
            self.assertNotIn("session_id", content)
            self.assertEqual("", content.strip())

    def test_an_envelope_with_a_nested_content_field_is_not_suppressed(self):
        """The guard recurses, so content nested under a known key must still count."""
        payload = dict(SESSION_START, metadata={"prompt": "do the thing"})
        self.assertFalse(identity_only_payload(payload))

    def test_the_five_keys_are_present(self):
        for key in ("session_title", "source", "transcript_path", "model", "prompt_id"):
            self.assertIn(key, IDENTITY_ONLY_PAYLOAD_KEYS)


if __name__ == "__main__":
    unittest.main()
