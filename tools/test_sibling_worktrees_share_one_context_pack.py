#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Sibling worktrees share one context pack; different projects never do.

Keying the pack cache on the checkout directory fragments it as soon as an agent works across git
worktrees, and Codex does: a live box held 18 cache files for 2 agents, spread over wt-l1cache,
wt-model, wt-flags, wt-snapbin, wt-scratch and more. Every new worktree started with nothing to
fall back to -- which is exactly the turn the fallback exists for.

The checkout directory was never the thing worth distinguishing. Mixing one PROJECT's context into
another is the error that matters, and a git common dir draws that line precisely.
"""
import os
import shutil
import subprocess
import tempfile
import unittest

import matrixark_hook_pack_cache as pack_cache

GIT = shutil.which("git")


def _git(*args, cwd=None):
    subprocess.run(
        [GIT, *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )


@unittest.skipIf(GIT is None, "git is not available")
class WorkspaceIdentityTest(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.root = self._dir.name
        self.repo = os.path.join(self.root, "project")
        os.makedirs(self.repo)
        _git("init", "-q", cwd=self.repo)
        _git("config", "user.email", "t@example.com", cwd=self.repo)
        _git("config", "user.name", "t", cwd=self.repo)
        with open(os.path.join(self.repo, "f.txt"), "w", encoding="utf-8") as handle:
            handle.write("x\n")
        _git("add", "f.txt", cwd=self.repo)
        _git("commit", "-qm", "first", cwd=self.repo)

        self.worktree = os.path.join(self.root, "wt-feature")
        _git("worktree", "add", "-q", "-b", "feature", self.worktree, cwd=self.repo)

    def tearDown(self):
        self._dir.cleanup()

    def test_a_worktree_shares_its_repository_identity(self):
        self.assertEqual(
            pack_cache.workspace_identity(self.repo),
            pack_cache.workspace_identity(self.worktree),
            "a worktree and its repository are the same project and must share one pack",
        )

    def test_a_worktree_shares_the_cache_file(self):
        self.assertEqual(
            pack_cache.context_pack_cache_path("codex", self.repo),
            pack_cache.context_pack_cache_path("codex", self.worktree),
        )

    def test_a_different_repository_does_not_share(self):
        """The control: without this, collapsing everything to one key would pass the tests above
        and let one project's context surface in another."""
        other = os.path.join(self.root, "other-project")
        os.makedirs(other)
        _git("init", "-q", cwd=other)
        self.assertNotEqual(
            pack_cache.workspace_identity(self.repo),
            pack_cache.workspace_identity(other),
            "two repositories are two projects and must not share a pack",
        )

    def test_a_path_that_is_not_a_repository_still_keys(self):
        """Not every workspace is a checkout; the cache must keep working, not disappear."""
        plain = os.path.join(self.root, "plain-dir")
        os.makedirs(plain)
        self.assertEqual(pack_cache.workspace_identity(plain), plain)
        self.assertTrue(pack_cache.context_pack_cache_path("claude", plain).name.endswith(".txt"))

    def test_a_missing_path_falls_back_rather_than_raising(self):
        missing = os.path.join(self.root, "does-not-exist")
        self.assertEqual(pack_cache.workspace_identity(missing), missing)

    def test_an_absent_workspace_is_still_addressable(self):
        self.assertEqual(pack_cache.workspace_identity(""), "-")
        self.assertTrue(pack_cache.context_pack_cache_path("codex", "").name.endswith(".txt"))

    def test_agents_still_do_not_share_a_file(self):
        """Repository identity must not undo the agent split."""
        self.assertNotEqual(
            pack_cache.context_pack_cache_path("claude", self.repo),
            pack_cache.context_pack_cache_path("codex", self.repo),
        )


if __name__ == "__main__":
    unittest.main()
