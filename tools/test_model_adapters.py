#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Client-side message-format adapters (matrixark_model_adapters):
``from_openai`` / ``from_anthropic`` / ``to_messages`` normalize provider-shaped
model SDK data into MatrixArk ``[{role, content}]`` messages. Pure functions,
stdlib only, no live services and no provider SDKs imported."""
import unittest

from matrixark_model_adapters import (
    VALID_ROLES,
    from_anthropic,
    from_openai,
    to_messages,
)


class _Obj:
    """Duck-typed stand-in for an SDK model object (attribute access, no dict)."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


class FromOpenAITest(unittest.TestCase):
    def test_chat_completions_request(self):
        req = {"model": "gpt-x", "messages": [
            {"role": "system", "content": "be terse"},
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello"},
        ]}
        self.assertEqual(
            [{"role": "system", "content": "be terse"},
             {"role": "user", "content": "hi"},
             {"role": "assistant", "content": "hello"}],
            from_openai(req))

    def test_response_choices(self):
        resp = {"id": "cmpl-1", "choices": [
            {"index": 0, "message": {"role": "assistant", "content": "the answer is 42"}},
        ]}
        self.assertEqual([{"role": "assistant", "content": "the answer is 42"}],
                         from_openai(resp))

    def test_content_parts_list_flattened(self):
        req = {"messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "first line"},
                {"type": "text", "text": "second line"},
                {"type": "image_url", "image_url": {"url": "http://x/y.png"}},
            ]},
        ]}
        self.assertEqual([{"role": "user", "content": "first line\nsecond line"}],
                         from_openai(req))

    def test_function_role_maps_to_tool(self):
        req = {"messages": [
            {"role": "function", "name": "lookup", "content": "result payload"},
        ]}
        out = from_openai(req)
        self.assertEqual("tool", out[0]["role"])
        self.assertEqual("result payload", out[0]["content"])

    def test_single_message(self):
        self.assertEqual([{"role": "user", "content": "just me"}],
                         from_openai({"role": "user", "content": "just me"}))

    def test_raw_list_of_messages(self):
        msgs = [{"role": "user", "content": "a"}, {"role": "assistant", "content": "b"}]
        self.assertEqual(msgs, from_openai(msgs))

    def test_duck_typed_object_response(self):
        resp = _Obj(choices=[_Obj(message=_Obj(role="assistant", content="obj answer"))])
        self.assertEqual([{"role": "assistant", "content": "obj answer"}],
                         from_openai(resp))

    def test_empty_and_none(self):
        self.assertEqual([], from_openai(None))
        self.assertEqual([], from_openai({"messages": []}))
        self.assertEqual([], from_openai([]))

    def test_empty_content_turn_skipped(self):
        req = {"messages": [
            {"role": "assistant", "content": ""},
            {"role": "user", "content": "kept"},
        ]}
        self.assertEqual([{"role": "user", "content": "kept"}], from_openai(req))

    def test_malformed_numeric_content_raises(self):
        with self.assertRaises(ValueError):
            from_openai({"messages": [{"role": "user", "content": 123}]})

    def test_malformed_top_level_scalar_raises(self):
        with self.assertRaises(ValueError):
            from_openai(3.14)

    def test_all_roles_within_valid_set(self):
        req = {"messages": [
            {"role": "system", "content": "s"},
            {"role": "developer", "content": "d"},
            {"role": "user", "content": "u"},
            {"role": "assistant", "content": "a"},
            {"role": "tool", "content": "t"},
            {"role": "function", "content": "f"},
        ]}
        for entry in from_openai(req):
            self.assertIn(entry["role"], VALID_ROLES)


class FromAnthropicTest(unittest.TestCase):
    def test_messages_request_string_content(self):
        req = {"model": "claude-x", "messages": [
            {"role": "user", "content": "what is the capital of France?"},
            {"role": "assistant", "content": "Paris"},
        ]}
        self.assertEqual(
            [{"role": "user", "content": "what is the capital of France?"},
             {"role": "assistant", "content": "Paris"}],
            from_anthropic(req))

    def test_content_blocks_flattened(self):
        req = {"messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "line A"},
                {"type": "text", "text": "line B"},
            ]},
        ]}
        self.assertEqual([{"role": "user", "content": "line A\nline B"}],
                         from_anthropic(req))

    def test_messages_response_object(self):
        resp = {"id": "msg_1", "type": "message", "role": "assistant",
                "content": [{"type": "text", "text": "the response text"}],
                "stop_reason": "end_turn"}
        self.assertEqual([{"role": "assistant", "content": "the response text"}],
                         from_anthropic(resp))

    def test_tool_result_user_turn_maps_to_tool(self):
        req = {"messages": [
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "tu1", "name": "get_weather",
                 "input": {"city": "SF"}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu1",
                 "content": [{"type": "text", "text": "sunny, 22C"}]}]},
        ]}
        out = from_anthropic(req)
        # assistant tool_use turn is described (not dropped), then tool_result -> tool
        self.assertEqual("assistant", out[0]["role"])
        self.assertEqual("tool", out[1]["role"])
        self.assertEqual("sunny, 22C", out[1]["content"])

    def test_tool_result_string_content(self):
        req = {"messages": [
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t", "content": "plain string result"}]},
        ]}
        out = from_anthropic(req)
        self.assertEqual([{"role": "tool", "content": "plain string result"}], out)

    def test_duck_typed_object(self):
        resp = _Obj(type="message", role="assistant",
                    content=[_Obj(type="text", text="obj block")])
        self.assertEqual([{"role": "assistant", "content": "obj block"}],
                         from_anthropic(resp))

    def test_empty_and_none(self):
        self.assertEqual([], from_anthropic(None))
        self.assertEqual([], from_anthropic({"messages": []}))
        self.assertEqual([], from_anthropic([]))

    def test_empty_text_block_turn_skipped(self):
        req = {"messages": [
            {"role": "assistant", "content": [{"type": "text", "text": ""}]},
            {"role": "user", "content": "kept"},
        ]}
        self.assertEqual([{"role": "user", "content": "kept"}], from_anthropic(req))

    def test_malformed_numeric_content_raises(self):
        with self.assertRaises(ValueError):
            from_anthropic({"messages": [{"role": "user", "content": 5}]})


class ToMessagesTest(unittest.TestCase):
    def test_auto_sniff_openai_response(self):
        resp = {"choices": [{"message": {"role": "assistant", "content": "oai"}}]}
        self.assertEqual([{"role": "assistant", "content": "oai"}], to_messages(resp))

    def test_auto_sniff_anthropic_response(self):
        resp = {"type": "message", "role": "assistant",
                "content": [{"type": "text", "text": "ant"}]}
        self.assertEqual([{"role": "assistant", "content": "ant"}], to_messages(resp))

    def test_auto_sniff_anthropic_request_blocks(self):
        req = {"messages": [{"role": "user", "content": [{"type": "text", "text": "blk"}]}]}
        self.assertEqual([{"role": "user", "content": "blk"}], to_messages(req))

    def test_auto_sniff_openai_request(self):
        req = {"messages": [{"role": "user", "content": "hi"}]}
        self.assertEqual([{"role": "user", "content": "hi"}], to_messages(req))

    def test_passthrough_already_normalized(self):
        msgs = [{"role": "user", "content": "already"},
                {"role": "assistant", "content": "normalized"}]
        self.assertEqual(msgs, to_messages(msgs))
        # explicit passthrough provider too
        self.assertEqual(msgs, to_messages(msgs, provider="matrixark"))

    def test_explicit_provider_forces_adapter(self):
        # A function-role message forced through openai maps to tool.
        req = {"messages": [{"role": "function", "content": "x"}]}
        self.assertEqual("tool", to_messages(req, provider="openai")[0]["role"])

    def test_empty_and_none(self):
        self.assertEqual([], to_messages(None))
        self.assertEqual([], to_messages([]))
        self.assertEqual([], to_messages("", provider="auto"))

    def test_bare_string(self):
        self.assertEqual([{"role": "user", "content": "hey"}], to_messages("hey"))

    def test_unknown_provider_raises(self):
        with self.assertRaises(ValueError):
            to_messages({"messages": []}, provider="cohere")

    def test_malformed_raises(self):
        with self.assertRaises(ValueError):
            to_messages(42)


if __name__ == "__main__":
    unittest.main(verbosity=2)
