#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate repository-level open-source readiness files."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# THE ROOT COPIES ARE REQUIRED TOO, and that is not tidiness.
#
# This list named only the `docs/` pair, so the gate reported success on a tree with no
# `CONTRIBUTING.md` at the root -- the copy GitHub surfaces to someone arriving at the
# project. It was found by planting that deletion, not by reading the list, which is the
# only way a passing check is ever shown to work.
#
# The two pairs are not the same documents. The root `CONTRIBUTING.md` is 105 lines and
# says pull requests are welcome "from anyone"; `docs/CONTRIBUTING.md` is 67 lines. The
# root `CODE_OF_CONDUCT.md` adopts the Contributor Covenant 2.1 by name and link in 42
# lines; the `docs/` one is 13. So the fuller pair was the unchecked pair, and either copy
# could have been deleted or left to rot behind a green gate.
#
# Which copy should be canonical -- and whether one should become a pointer to the other --
# is a content decision and is NOT made here. Requiring both makes the gate cover what it
# already claimed to cover, and that does not need the question settled.
REQUIRED_FILES = [
    "README.md",
    "LICENSE",
    "NOTICE",
    "CONTRIBUTING.md",
    "CODE_OF_CONDUCT.md",
    "docs/CONTRIBUTING.md",
    "docs/SECURITY.md",
    "docs/CODE_OF_CONDUCT.md",
    ".gitignore",
    "docs/open_source_readiness.md",
]

README_TOKENS = [
    "Apache-2.0",
    "rust-main",
    "no brpc/thrift",
    "Rust-native",
    "production-readiness claims",
]

SECURITY_TOKENS = [
    "Do not open a public issue with exploit details",
    "Do not commit API keys",
    "TS_RAFT_AUTH_TOKEN",
]

CONTRIBUTING_TOKENS = [
    "shared-corpus",
    "rust-internal",
    "Do not add brpc or thrift",
]

IGNORE_TOKENS = [
    "target/",
    ".codex/",
    ".local/",
    "build-ubuntu22/",
    "output/",
    "target-rust-bench/",
    "thirdparty/",
    "benchmark_reports/",
]

# Keep private account markers out of the published rule source itself.
PRIVATE_ACCOUNT_MARKER = bytes.fromhex("64656570726f757465").decode("ascii")

PRIVATE_PATH_TOKENS = [
    "C:\\Users\\",
    "/mnt/c/Users",
    "/mnt/c/Users/",
    "/root/src/",
    "Vincent Jiang",
]

FALLBACK_SCAN_SKIP_DIRS = {
    ".git",
    "target",
    "target-rust-bench",
    "build-ubuntu22",
    "output",
    "benchmark_reports",
    "thirdparty",
    "__pycache__",
}

# Names of products, toolchains and internal systems that do not belong in an
# open-source repository. Nothing else in CI has an opinion about prose, so a
# change can add hundreds of occurrences and still pass every other gate.
#
# `matrixobjectstore` is deliberately NOT here: it is the published name of an
# Apache-2.0 crate this tree depends on, so it appears in a Cargo.toml dependency, in
# the feature that enables it and in the bridge whose name is public API.
# validate_no_enterprise_object_store draws the distinction that matters -- depending on
# that crate is ordinary, containing a copy of it is not -- and a blanket match here
# would fail on all four.
FOREIGN_VOCABULARY: list[tuple[str, str, int]] = [
    (
        "language and toolchain names",
        r"c\+\+|cplusplus|c_plus_plus|(?<![A-Za-z])cxx(?![A-Za-z])"
        r"|(?<![A-Za-z])cpp(?![A-Za-z])|(?<![A-Za-z])gtest(?![A-Za-z])",
        re.IGNORECASE,
    ),
    (
        "third-party product names",
        r"(?<![A-Za-z])(lark|feishu|wukong|bytedance|byteraft|bytestore|mtcache"
        r"|openviking|vikingmem)(?![A-Za-z])|volcano engine",
        re.IGNORECASE,
    ),
    # "abase" is a substring of "database", so it only counts on a token boundary.
    ("internal datastore name", r"(?<![A-Za-z])abase(?![A-Za-z])", re.IGNORECASE),
    # The C-FFI spelling is CIpsFoo, which a token-start-only pattern misses, and
    # this one is case-sensitive on purpose: "ips" alone is ordinary English.
    ("retired data-model name", r"(?<![A-Za-z0-9])(C?Ips[A-Z]|ips_|IPS(?![a-zA-Z]))", 0),
]

# These files carry the patterns above as fixtures or as the check itself, in the
# same way PRIVATE_PATH_FIXTURE_FILES carries the private-path patterns.
FOREIGN_VOCABULARY_FIXTURE_FILES = {
    "tools/validate_open_source_readiness.py",
}

PRIVATE_PATH_FIXTURE_FILES = {
    "tools/validate_open_source_readiness.py",
    "tools/validate_temporalstore_performance_execution_redaction.py",
    "tools/test_temporalstore_performance_execution_redaction.py",
}

def read(relative_path: str) -> str:
    path = ROOT / relative_path
    if not path.exists():
        raise SystemExit(f"missing required file: {relative_path}")
    return path.read_text(encoding="utf-8", errors="replace")


def require_tokens(relative_path: str, tokens: list[str]) -> None:
    text = read(relative_path)
    normalized = " ".join(text.split())
    missing = [token for token in tokens if token not in text and token not in normalized]
    if missing:
        raise SystemExit(f"{relative_path} missing tokens: {', '.join(missing)}")


def validate_cargo_license() -> None:
    cargo = read("Cargo.toml")
    if 'license = "Apache-2.0"' not in cargo:
        raise SystemExit("Cargo.toml must declare workspace Apache-2.0 license")
    for crate in ["crates/temporalstore-rust/Cargo.toml", "crates/temporalstore-snapshot/Cargo.toml"]:
        text = read(crate)
        if "license.workspace = true" not in text:
            raise SystemExit(f"{crate} must inherit workspace license")


def validate_license() -> None:
    license_text = read("LICENSE")
    if "Apache License" not in license_text or "Version 2.0" not in license_text:
        raise SystemExit("LICENSE must contain Apache License 2.0 text")
    notice = read("NOTICE")
    if "MatrixArkAI contributors" not in notice:
        raise SystemExit("NOTICE must include contributor attribution")


def validate_no_tracked_local_codex_config() -> None:
    tracked = repository_files()
    local_codex_files = [path for path in tracked if path == ".codex" or path.startswith(".codex/")]
    if local_codex_files:
        raise SystemExit(
            "tracked local Codex config is not open-source safe: "
            + ", ".join(local_codex_files)
        )


def validate_no_private_paths() -> None:
    tracked = repository_files()
    offenders: list[str] = []
    for relative_path in tracked:
        if PRIVATE_ACCOUNT_MARKER in relative_path.casefold():
            offenders.append("tracked filename contains a private account marker")
        path = ROOT / relative_path
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if PRIVATE_ACCOUNT_MARKER in text.casefold():
            offenders.append(f"{relative_path}: private account marker")
        if relative_path in PRIVATE_PATH_FIXTURE_FILES:
            continue
        for token in PRIVATE_PATH_TOKENS:
            if token in text:
                offenders.append(f"{relative_path}: {token}")
                break
    if offenders:
        raise SystemExit(
            "tracked files contain local/private path markers:\n" + "\n".join(offenders[:50])
        )


def validate_no_enterprise_object_store() -> None:
    """Fail if this repository CONTAINS the object store rather than depending on it.

    The object-store crate is Apache-2.0 open source and public, with its own openness
    gate, and this tree depends on it by public git revision exactly as it depends on
    the cache and consensus crates. So a dependency is ordinary and is no longer
    refused here -- the two checks that rejected one were written while the crate had
    no public source, and they turned this gate red the moment it was declared.

    What is still refused is CONTAINING it:

    * ``crates/matrixobject/`` tracked here, or a Cargo.toml naming it by path or as a
      workspace member -- vendoring a copy of another repository's crate duplicates its
      source instead of depending on it, which is wrong however open that crate is; and
    * a ``src/bin/`` executable that speaks the object-store wire protocol -- that is
      the store itself rather than a client of it.

    DEPEND versus CONTAIN is the distinction that survives the crate becoming public.
    """
    tracked = repository_files()
    offenders: list[str] = []

    if any(p == "crates/matrixobject" or p.startswith("crates/matrixobject/") for p in tracked):
        offenders.append(
            "crates/matrixobject/ is tracked -- depend on the published crate instead of "
            "vendoring a copy of it here"
        )

    for relative_path in tracked:
        if not relative_path.endswith("Cargo.toml"):
            continue
        path = ROOT / relative_path
        if not path.is_file():
            continue
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "crates/matrixobject" in line:
                offenders.append(
                    f"{relative_path}: references crates/matrixobject (workspace member or path dep)"
                )
            # A DEPENDENCY ON THE OBJECT-STORE CRATE IS NO LONGER AN OFFENCE. Two checks
            # here rejected one -- a `matrixobjectstore-rs = ...` line and a
            # `dep:matrixobjectstore` feature entry -- and both were written while that
            # crate had no public source. It is Apache-2.0 and public now, pinned by git
            # revision beside the cache and consensus crates, so those checks only served
            # to turn this gate red when the dependency was declared. What remains refused
            # is CONTAINING the store, checked above and below.

    # A standalone executable that SPEAKS the object-store wire protocol is the store itself,
    # whatever it is named. The sanctioned open-source surface is the feature-gated bridge and
    # its clients -- never a binary that serves objects. This is the hole the 20260817 sync
    # slipped through: the checks above look only for the crate directory and the Cargo
    # dependency, so a src/bin/ server imported clean and shipped for two days.
    for relative_path in tracked:
        if not (relative_path.endswith(".rs") and "/src/bin/" in relative_path):
            continue
        path = ROOT / relative_path
        if not path.is_file():
            continue
        if "MORP1" in path.read_text(encoding="utf-8", errors="replace"):
            offenders.append(
                f"{relative_path}: a src/bin/ executable speaks the enterprise object-store "
                "wire protocol -- that server is enterprise-only"
            )

    if offenders:
        raise SystemExit(
            "this repository must DEPEND on the object store, not CONTAIN it:\n"
            + "\n".join(offenders[:50])
        )


def validate_no_foreign_vocabulary() -> None:
    """Fails if a tracked file names a product, toolchain or internal system.

    Scanning the whole tree rather than a diff, so that a name cannot arrive by
    a route the diff check misses -- a rename, a merge, a file added before the
    check existed.
    """
    tracked = [path for path in repository_files() if path not in FOREIGN_VOCABULARY_FIXTURE_FILES]
    skipped = len(repository_files()) - len(tracked)
    if skipped != len(FOREIGN_VOCABULARY_FIXTURE_FILES):
        # A renamed fixture would silently stop being skipped, or worse, a typo
        # here would skip nothing and the scan would fail on its own patterns.
        raise SystemExit(
            f"expected to skip {len(FOREIGN_VOCABULARY_FIXTURE_FILES)} vocabulary "
            f"fixture file(s), skipped {skipped}"
        )
    if not tracked:
        raise SystemExit("no tracked files to scan for vocabulary")

    offenders: list[str] = []
    for relative_path in tracked:
        path = ROOT / relative_path
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for label, pattern, flags in FOREIGN_VOCABULARY:
            for match in re.finditer(pattern, text, flags):
                line = text.count("\n", 0, match.start()) + 1
                offenders.append(f"{relative_path}:{line}: {label}: {match.group(0)!r}")
                break

    if offenders:
        raise SystemExit(
            "names that must not appear in this repository "
            "(use the project's own vocabulary):\n" + "\n".join(offenders[:50])
        )


def repository_files() -> list[str]:
    try:
        return subprocess.run(
            ["git", "ls-files"],
            cwd=ROOT,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.splitlines()
    except (FileNotFoundError, subprocess.CalledProcessError):
        files: list[str] = []
        for directory, dirnames, filenames in os.walk(ROOT):
            dirnames[:] = [
                dirname for dirname in dirnames if dirname not in FALLBACK_SCAN_SKIP_DIRS
            ]
            for filename in filenames:
                path = Path(directory) / filename
                files.append(path.relative_to(ROOT).as_posix())
        return sorted(files)


def main() -> int:
    for path in REQUIRED_FILES:
        read(path)
    validate_cargo_license()
    validate_license()
    validate_no_tracked_local_codex_config()
    validate_no_private_paths()
    validate_no_enterprise_object_store()
    validate_no_foreign_vocabulary()
    require_tokens("README.md", README_TOKENS)
    require_tokens("docs/SECURITY.md", SECURITY_TOKENS)
    require_tokens("docs/CONTRIBUTING.md", CONTRIBUTING_TOKENS)
    require_tokens(".gitignore", IGNORE_TOKENS)
    require_tokens(
        "docs/open_source_readiness.md",
        [
            "brpc/thrift compatibility remains explicitly out of scope",
            "generated build outputs",
            "`workflow` scope",
        ],
    )
    report = {
        "status": "ok",
        "required_files": REQUIRED_FILES,
        "license": "Apache-2.0",
        "open_source_ready": True,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
