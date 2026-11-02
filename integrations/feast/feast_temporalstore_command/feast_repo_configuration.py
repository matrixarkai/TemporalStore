# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Point Feast's own universal test suite at this online store.

Feast builds its integration-test matrix from ``AVAILABLE_ONLINE_STORES`` in
``tests/universal/feature_repos/repo_configuration.py``, and lets a third party
replace that matrix by naming a module in ``FULL_REPO_CONFIGS_MODULE``. This is
that module.

From a checkout of Feast (the version this provider is tested against is
pinned in the README), with this package importable::

    PYTHONPATH='.:/path/to/integrations/feast' \\
    FULL_REPO_CONFIGS_MODULE=feast_temporalstore_command.feast_repo_configuration \\
    PYTEST_PLUGINS=feast_temporalstore_command.feast_online_store_creator \\
    IS_TEST=True \\
    python -m pytest -x --integration sdk/python/tests

BOTH NAMES ARE EXPORTED ON PURPOSE. Feast reads ``AVAILABLE_ONLINE_STORES`` and
``AVAILABLE_OFFLINE_STORES`` together and falls back to ``FULL_REPO_CONFIGS``
only if the first pair is absent -- so a module that defines just one of the
pair gets the fallback path instead of the one it meant. Defining the pair and
the fallback means the same matrix is produced whichever branch that code takes.

THE IMPORTS BELOW ONLY RESOLVE INSIDE A FEAST CHECKOUT. ``tests.universal`` is
not shipped in the Feast wheel, so this module is importable only when running
against Feast's source tree -- which is exactly when it is wanted. It is kept
out of the package ``__init__`` for that reason, so importing
``feast_temporalstore_command`` never depends on it.
"""

from __future__ import annotations

from feast_temporalstore_command.feast_online_store_creator import (
    TemporalStoreOnlineStoreCreator,
)

try:  # pragma: no cover - present only inside a Feast checkout
    from tests.universal.feature_repos.integration_test_repo_config import (
        IntegrationTestRepoConfig,
    )
    from tests.universal.feature_repos.repo_configuration import (
        AVAILABLE_OFFLINE_STORES,
    )
except ImportError as exc:  # pragma: no cover
    # NAME THE MISSING MODULE RATHER THAN GUESSING THE REASON. A first draft of
    # this message asserted that `tests.universal` was absent from the wheel --
    # true, but not what had failed: the real cause was that `pytest` was not
    # installed, and Feast's test tree imports it at module scope. A diagnostic
    # that confidently blames the wrong thing costs more than no diagnostic.
    raise ImportError(
        f"could not import Feast's universal test fixtures: {exc!s}.\n"
        "This module is importable only from a Feast SOURCE CHECKOUT with the "
        "test dependencies installed -- `tests.universal` is not part of the "
        "published Feast wheel, and that package imports pytest at module "
        "scope. From a Feast clone:\n"
        "    pip install pytest pytest-asyncio\n"
        "    PYTHONPATH='.:/path/to/integrations/feast' ..."
    ) from exc

__all__ = [
    "AVAILABLE_ONLINE_STORES",
    "AVAILABLE_OFFLINE_STORES",
    "FULL_REPO_CONFIGS",
]

# {name: (online_store config or type string, creator class or None)}.
# The creator is given rather than a literal config because the store needs a
# running data node, and leaving that to the person running the suite is how a
# suite ends up never being run.
AVAILABLE_ONLINE_STORES = {
    "temporalstore": (None, TemporalStoreOnlineStoreCreator),
}

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        online_store="temporalstore",
        online_store_creator=TemporalStoreOnlineStoreCreator,
    ),
]
