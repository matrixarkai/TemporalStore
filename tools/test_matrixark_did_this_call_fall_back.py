#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
""""Did this call fall back?" is a question about this call.

Every retrieve reports ``embedding_fallback_used`` and ``embedding_execution_mode`` in its context
pack. Both read one module global, which was set True in three places and reset **nowhere** -- the
only assignment to False was the import-time initialiser.

So one fallback, anywhere, at any point in the process, made every later retrieve report that it
had fallen back. Including the ones a recovered encoder served correctly. The pack states it as a
fact about that retrieve, and after the first fallback it was never true again.

Measured on main before the change, with the same two probes this suite uses::

    after a call that fell back      True
    after a call that did not        True   <- and for the rest of the process
    execution mode                   local_hash_embedding_fallback

**Per call and per thread.** The gateway serves retrieves through ``asyncio.to_thread``, so two
requests can be inside the encoder at once. A single global reset at entry would let one request's
reset erase the other's answer, which is a worse failure than the one being fixed: intermittent
rather than permanent, and therefore much harder to see.
"""
from __future__ import annotations

import itertools
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_embeddings as embeddings  # noqa: E402

FELL_BACK = {"MATRIXARK_EMBEDDING_PROVIDER": "openai"}      # configured, no key -> falls back
DID_NOT = {"MATRIXARK_EMBEDDING_PROVIDER": "deterministic"}  # the encoder that always answers


class _Env:
    """Set some variables, and put the environment back exactly as it was."""

    def __init__(self, **values):
        self.values = values
        self.original = {}

    def __enter__(self):
        for name, value in self.values.items():
            self.original[name] = os.environ.get(name)
            os.environ[name] = value
        self.original["OPENAI_API_KEY"] = os.environ.pop("OPENAI_API_KEY", None)
        return self

    def __exit__(self, *exc):
        for name, value in self.original.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        return False


_PROBE = itertools.count()


def _fresh_text() -> str:
    """A text no earlier call has encoded.

    Vectors are cached, and a cache hit never reaches the provider -- so it cannot fall back, and
    reports that it did not. That is the right answer, and it made the first version of this suite
    order-dependent: tests run alphabetically, an earlier one had already encoded the shared
    string, and the floor below then measured a cache hit instead of the encoder.
    """
    return "a probe sentence number %d" % next(_PROBE)


def encode(**env) -> bool:
    """Encode one text under `env`, and report whether THAT call fell back."""
    with _Env(**env):
        embeddings.embedding_for_text(_fresh_text())
        return embeddings.embedding_fallback_used()


class TheAnswerFollowsTheCallTest(unittest.TestCase):

    def test_a_call_that_fell_back_says_so(self) -> None:
        """The floor. Everything below passes on a flag hard-wired to False."""
        self.assertTrue(encode(**FELL_BACK))

    def test_a_call_that_did_not_fall_back_says_that(self) -> None:
        """The fix. On main this was True -- for ever, from the first fallback onward."""
        encode(**FELL_BACK)
        self.assertFalse(encode(**DID_NOT))

    def test_the_execution_mode_follows_too(self) -> None:
        """It is derived from the same flag, and it is the string a customer reads."""
        encode(**FELL_BACK)
        with _Env(**DID_NOT):
            embeddings.embedding_for_text(_fresh_text())
            self.assertNotEqual("local_hash_embedding_fallback",
                                embeddings.embedding_execution_mode_name())

    def test_a_batch_reports_its_own_answer(self) -> None:
        """The other entry point. A batch that fell back must say so, and one after it must not
        inherit that."""
        with _Env(**FELL_BACK):
            embeddings.embeddings_for_texts([_fresh_text(), _fresh_text()])
            self.assertTrue(embeddings.embedding_fallback_used())
        with _Env(**DID_NOT):
            embeddings.embeddings_for_texts([_fresh_text(), _fresh_text()])
            self.assertFalse(embeddings.embedding_fallback_used())


    def test_a_fully_cached_batch_still_answers_for_itself(self) -> None:
        """The batch entry point's own reset, which nothing else covers.

        `embeddings_for_texts` delegates to `embedding_for_text` for anything it has to encode, and
        that resets -- so removing the batch's own reset changes nothing on any path that encodes
        something. A mutation doing exactly that survived the first version of this suite.

        A batch served entirely from the cache never delegates. It is the one call that reaches the
        batch entry and nothing beneath it, so it is the only place the entry's own reset is
        observable.
        """
        texts = [_fresh_text(), _fresh_text()]
        # The SAME provider both times. The cache is keyed on (model, text), so switching provider
        # switches the model name and misses every entry -- which is why a first attempt at this
        # test delegated after all, and the mutation it was written for survived it.
        with _Env(**FELL_BACK):
            embeddings.embeddings_for_texts(texts)
            self.assertTrue(embeddings.embedding_fallback_used(), "the fixture did not fall back")
        with _Env(**FELL_BACK):
            embeddings.embeddings_for_texts(texts)   # every one already cached: no encoder call
            self.assertFalse(embeddings.embedding_fallback_used(),
                             "a batch served entirely from cache inherited the previous call's "
                             "answer, so the batch entry point is not starting a fresh one")


class OneThreadDoesNotAnswerForAnotherTest(unittest.TestCase):
    """Why it is a thread-local and not one global reset at entry.

    Retrieves are served through `asyncio.to_thread`, so two can be inside the encoder at once.
    With a single global, whichever reset last would speak for both -- an intermittent wrong answer,
    which is harder to notice than the permanent one this replaces.
    """

    def test_a_fallback_in_one_thread_is_not_reported_in_another(self) -> None:
        """Driven through the flag itself rather than through the encoder.

        Making each thread encode would mean each setting MATRIXARK_EMBEDDING_PROVIDER, and the
        environment is process-wide: the two would stomp each other and the test would be measuring
        that race rather than thread isolation. The property under test is that one thread's answer
        does not become another's, and that is exactly what these two calls exercise.
        """
        answers = {}
        both_marked = threading.Barrier(2, timeout=30)

        def falls_back():
            embeddings._begin_embedding_call()
            embeddings._mark_embedding_fallback()
            both_marked.wait()          # hold until the other thread has begun its own call
            answers["fell_back"] = embeddings.embedding_fallback_used()

        def stays_clean():
            embeddings._begin_embedding_call()
            both_marked.wait()
            answers["clean"] = embeddings.embedding_fallback_used()

        threads = [threading.Thread(target=falls_back), threading.Thread(target=stays_clean)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        self.assertTrue(answers.get("fell_back"), "the falling-back thread lost its answer")
        self.assertFalse(answers.get("clean"), "one thread's fallback was reported in another")

    def test_the_main_thread_starts_with_no_answer(self) -> None:
        """A thread that has not encoded anything has not fallen back, and must not inherit a
        default of True from anywhere."""
        state = embeddings._EMBEDDING_CALL_STATE
        had = hasattr(state, "fallback_used")
        previous = getattr(state, "fallback_used", None)
        try:
            if had:
                del state.fallback_used
            self.assertFalse(embeddings.embedding_fallback_used())
        finally:
            if had:
                state.fallback_used = previous


if __name__ == "__main__":
    unittest.main()
