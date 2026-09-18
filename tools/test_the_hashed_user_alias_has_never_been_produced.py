#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The hashed user alias has never been produced, and the obvious repair would be wrong.

`_user_aliases(tenant_id, user_id)` is meant to return every key a per-user policy answers to --
the plain form and the hashed one. It returns only the plain forms, because the call that would
produce the hash raises on every invocation:

    identity_hashes({"tenant_id": tenant_id, "user_id": user_id})
    TypeError: identity_hashes() missing 1 required positional argument: 'tenant_id'

`identity_hashes(account_id, tenant_id, user_id="", ...)` takes positional strings; one dict is one
argument. The `except Exception` beneath it swallows the TypeError on every call, so the branch is
dead code rather than a tolerated absence. Nothing depends on the alias existing -- the write path
keys by these same aliases, so the hash form was never written either.

WHY THIS FILE EXISTS RATHER THAN A FIX. Passing the arguments "properly" does not repair it, and
that is the part worth pinning, because it is the repair anyone would reach for first:

    identity_hashes("", "acme", "alice")   ->  tenant_hash 1004798881946030977
    tenant_hash_of("acme")                 ->  tenant_hash 4168368968138697317

`identity_hashes` hashes its raw arguments; `tenant_hash_of` canonicalises first. So a user alias
built that way is keyed on a tenant hash nothing else in the policy layer uses, and it matches
nothing -- silently, because an alias that matches nothing looks exactly like a policy that does
not apply.

`_user_aliases` is given only a tenant and a user, while every production caller of
`identity_hashes` supplies a real `account_id` and the `user_hash` a record carries is derived from
it. So repairing this means deciding which account the policy layer hashes with: a signature
change, and a decision about who starts matching a policy they did not match before.

Recorded in both directions. If the call starts succeeding, or a user-hash alias appears, this file
fails and asks for that decision rather than letting the alias ship.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_tenant_policy as policy  # noqa: E402
from matrixark_mcp_identity import identity_hashes  # noqa: E402

TENANT = "acme"
USER = "alice"


class TheHashedUserAliasHasNeverBeenProduced(unittest.TestCase):

    def test_the_fixture_tenant_produces_a_hash_alias(self) -> None:
        """The floor. If the tenant aliases were the plain form alone, every count below is a
        different statement and the 'no USER hash' finding would be indistinguishable from 'no
        hashes at all'."""
        aliases = policy._tenant_aliases(TENANT)
        self.assertIn(TENANT, aliases, "the plain tenant form is missing, so the fixture changed")
        hashed = [alias for alias in aliases if alias != TENANT]
        self.assertTrue(
            hashed,
            "the tenant aliases carry no hashed form, so this file can no longer tell a missing "
            "USER hash from a policy layer that hashes nothing",
        )

    def test_the_call_as_written_raises(self) -> None:
        """The branch is dead code, not a tolerated absence."""
        with self.assertRaises(TypeError):
            identity_hashes({"tenant_id": TENANT, "user_id": USER})

    def test_no_alias_carries_a_user_hash(self) -> None:
        aliases = policy._user_aliases(TENANT, USER)
        self.assertTrue(aliases, "no aliases at all, so nothing below is being checked")
        tenant_aliases = policy._tenant_aliases(TENANT)
        expected = {policy.user_key(tenant_alias, USER) for tenant_alias in tenant_aliases}
        self.assertEqual(
            expected, set(aliases),
            "a user alias appeared that is not (tenant alias, plain user). If the hashed user "
            "alias is now produced, that is the decision this file is holding -- strike it and "
            "say which account the policy layer hashes with",
        )

    def test_the_obvious_repair_would_key_on_a_tenant_hash_nothing_else_uses(self) -> None:
        """The reason not to just fix the call, with both numbers.

        This is the assertion worth having: without it, the next person passes `""` for the
        account, gets an alias that matches nothing, and the only symptom is a policy quietly not
        applying.
        """
        naive = identity_hashes("", TENANT, USER)
        canonical = policy.tenant_hash_of(TENANT)
        self.assertNotEqual(
            canonical, naive.get("tenant_hash"),
            "identity_hashes('', tenant, user) now agrees with tenant_hash_of. If the two "
            "canonicalise the same way, the obvious repair may be correct after all -- check, and "
            "strike this assertion if so",
        )


if __name__ == "__main__":
    unittest.main()
