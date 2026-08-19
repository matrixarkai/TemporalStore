#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The OSS causal-LM extractor must accept BOTH chat-template return shapes.

``apply_chat_template(..., return_tensors="pt")`` returned a bare tensor on older transformers and
returns a BatchEncoding (a mapping of input_ids + attention_mask) on transformers >= 5. The extractor
passed whatever it got positionally to ``generate()``, so on a current transformers it read ``.shape``
off a dict-like and raised a bare ``AttributeError`` -- segment_provider=oss could not run at all.
Nothing covered this path, which is why it went unnoticed; these tests stub the tokenizer and model so
both shapes are exercised without downloading a model.
"""
from __future__ import annotations

import json
import unittest

import matrixark_mcp_core as core


class _FakeTensor:
    """Minimal stand-in for a torch tensor of token ids."""

    def __init__(self, ids):
        self._ids = list(ids)

    @property
    def shape(self):
        return (1, len(self._ids))

    def to(self, _device):
        return self

    def __getitem__(self, index):
        if isinstance(index, slice):
            return _FakeTensor(self._ids[index])
        return self._ids[index]


class _FakeBatchEncoding(dict):
    """BatchEncoding is dict-like -- and crucially has no usable ``.shape``."""

    def to(self, _device):
        return self


class _FakeTokenizer:
    chat_template = "present"

    def __init__(self, encoded):
        self._encoded = encoded
        self.decoded_with = None

    def apply_chat_template(self, chat, add_generation_prompt=True, return_tensors="pt"):
        return self._encoded

    def decode(self, generated, skip_special_tokens=True):
        self.decoded_with = generated
        return json.dumps({"segments": [{"topic": "t", "message_indexes": [0],
                                         "saliency_score": 0.5, "summary_text": "s"}]})


class _FakeModel:
    def __init__(self):
        self.called_with_kwargs = None
        self.called_positionally = False

    def generate(self, *args, **kwargs):
        if args:
            self.called_positionally = True
        self.called_with_kwargs = kwargs
        # Prompt tokens followed by the generated continuation.
        return [_FakeTensor([1, 2, 3, 4, 5, 6])]


class OssModelChatTemplateShapeCase(unittest.TestCase):
    def setUp(self):
        core._OSS_SEGMENT_MODEL_CACHE.clear()
        self.addCleanup(core._OSS_SEGMENT_MODEL_CACHE.clear)

    def _run(self, encoded):
        tokenizer = _FakeTokenizer(encoded)
        model = _FakeModel()
        core._OSS_SEGMENT_MODEL_CACHE["stub:64"] = {
            "tokenizer": tokenizer, "model": model, "device": "cpu"}
        result = core.oss_model_memory_segments(
            [{"role": "user", "content": "hello"}], model="stub", max_new_tokens=64)
        return result, tokenizer, model

    def test_batch_encoding_is_expanded_into_generate_kwargs(self):
        encoded = _FakeBatchEncoding(input_ids=_FakeTensor([1, 2, 3]),
                                     attention_mask=_FakeTensor([1, 1, 1]))
        result, _tokenizer, model = self._run(encoded)
        self.assertIn("segments", result)
        self.assertFalse(model.called_positionally,
                         "a BatchEncoding must be expanded, not passed positionally")
        self.assertIn("input_ids", model.called_with_kwargs)

    def test_bare_tensor_is_still_passed_positionally(self):
        result, _tokenizer, model = self._run(_FakeTensor([1, 2, 3]))
        self.assertIn("segments", result)
        self.assertTrue(model.called_positionally)

    def test_prompt_tokens_are_trimmed_from_the_decoded_output(self):
        """The prompt length comes from input_ids either way, so the reply excludes the prompt."""
        encoded = _FakeBatchEncoding(input_ids=_FakeTensor([1, 2, 3]))
        _result, tokenizer, _model = self._run(encoded)
        self.assertEqual(3, len(tokenizer.decoded_with._ids),
                         "decode must see only the 3 generated tokens, not all 6")


if __name__ == "__main__":
    unittest.main()
