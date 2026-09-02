# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local TemporalStore tooling package."""

# One module, one object.
#
# Every module here can be imported under two names: `tools.matrixark_x`, and bare `matrixark_x`
# once this directory is on sys.path -- which the test suite puts there, and which is also the
# working directory the suite runner uses. Python treats those as two DIFFERENT modules and
# executes the file twice, so the process ends up holding two of everything: two class objects that
# compare unequal, and two copies of every module-level constant, each seeing only its own writes.
#
# Measured on this suite: 106 modules duplicated in a single discovery pass.
#
# The consequences did not look like an import problem, which is why they went unfixed. A test
# asserting a class is the class it re-exported failed with the SAME name printed on both sides. A
# budget assertion read 65 where another module had written 55. A module was unimportable ALONE and
# fine in the suite, because some other module had put the root on sys.path first.
#
# The fix canonicalises on the bare name -- 147 test files already use it against 32 that do not --
# by resolving `tools.X` to the module object bare `X` already produced. When bare `X` is not
# importable, as when running from the repository root, this declines and the normal import runs,
# so nothing changes for callers outside the suite.
import importlib
import importlib.abc
import importlib.util
import os
import sys


class _SameObjectLoader(importlib.abc.Loader):
    """Hands back an already-executed module instead of executing the file a second time."""

    def __init__(self, bare_name: str) -> None:
        self._bare_name = bare_name

    def create_module(self, spec):
        return importlib.import_module(self._bare_name)

    def exec_module(self, module) -> None:
        return None  # already executed under its bare name


class _SingleModuleIdentityFinder(importlib.abc.MetaPathFinder):
    """Resolves `tools.X` to the same object as bare `X`, for modules that live in this directory."""

    _PREFIX = "tools."
    _HERE = os.path.dirname(os.path.abspath(__file__))

    def find_spec(self, fullname, path=None, target=None):
        if not fullname.startswith(self._PREFIX):
            return None
        bare = fullname[len(self._PREFIX):]
        if "." in bare:
            return None
        if not os.path.exists(os.path.join(self._HERE, bare + ".py")):
            return None
        existing = sys.modules.get(bare)
        if existing is None:
            try:
                if importlib.util.find_spec(bare) is None:
                    return None
            except (ImportError, ValueError):
                return None
        return importlib.util.spec_from_loader(fullname, _SameObjectLoader(bare))


if not any(isinstance(finder, _SingleModuleIdentityFinder) for finder in sys.meta_path):
    sys.meta_path.insert(0, _SingleModuleIdentityFinder())
