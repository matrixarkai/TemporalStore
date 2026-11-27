# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run the physical checkpoint oracle on an isolated hosted CI worker only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

import owned_children

ROOT = Path(__file__).resolve().parent
OLD = "d771121eeebfc833873dc37a54bfbdcd268c660a"
OLD_TREE = "edbb3df83148c8eb65082c5e71d7a2f94570a1c4"
LIBRARY = Path("crates/temporalstore-rust/Cargo.toml")
FILES = ("ci.py", "common.rs", "producer/main.rs", "producer/Cargo.toml",
         "consumer/main.rs", "consumer/Cargo.toml", "owned_children.py")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def one_json(output, fields):
    rows = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
    matches = [row for row in rows if isinstance(row, dict) and set(fields) <= row.keys()]
    if len(matches) != 1:
        raise ValueError("expected exactly one structured result")
    return matches[0]


def declared_format(body, role):
    if role not in ("producer", "consumer"):
        raise ValueError("unknown bridge role")
    versions = re.findall(r"pub\(super\) const SHARD_INDEX_FORMAT_VERSION:\s*u32\s*=\s*(\d+);", body)
    if len(versions) != 1 or (role == "producer" and versions != ["32"]):
        raise ValueError("product format declaration is not the expected source contract")
    value = int(versions[0])
    if role == "consumer" and value <= 32:
        raise ValueError("this upgrade gate requires a newer consumer format")
    return value


def render_manifest(template, library):
    if template.count("PRODUCT_PATH_JSON") != 1 or not library.is_absolute():
        raise ValueError("expected one absolute product path slot")
    str(library).encode("utf-8")
    return template.replace("PRODUCT_PATH_JSON", json.dumps(str(library), ensure_ascii=False))


def product_package(metadata, library_manifest, bridge_manifest):
    packages = [row for row in metadata["packages"] if row["name"] == "temporalstore-rust"]
    if len(packages) != 1:
        raise ValueError("expected one product library in each bridge graph")
    package = packages[0]
    def exact_manifest(value, expected):
        path = Path(value)
        return path.is_absolute() and path.resolve() == expected.resolve()

    if package["source"] is not None or not exact_manifest(package["manifest_path"], library_manifest):
        raise ValueError("product must be the exact bound path package")
    root_id = metadata["resolve"]["root"]
    roots = [row for row in metadata["packages"] if row["id"] == root_id]
    if len(roots) != 1 or not exact_manifest(roots[0]["manifest_path"], bridge_manifest):
        raise ValueError("metadata root is not the generated bridge")
    node = next(row for row in metadata["resolve"]["nodes"]
                if row["id"] == metadata["resolve"]["root"])
    deps = [row for row in node["deps"] if row["name"] == "temporalstore"]
    if len(deps) != 1 or deps[0]["pkg"] != package["id"]:
        raise ValueError("bridge dependency is not the bound product package")
    return package


def checkout_binding(root, revision, expected_tree=None, workspace_target=False):
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise RuntimeError("checkout path is not its fixed physical directory")
    env = clean_env()
    def git(value):
        return subprocess.check_output(["git", "-C", str(root), "rev-parse", value],
                                       env=env, text=True, timeout=30).strip()
    if Path(git("--show-toplevel")).resolve(strict=True) != root:
        raise RuntimeError("expected an independent checkout root")
    actual, tree = git("HEAD"), git("HEAD^{tree}")
    if (actual != revision or not re.fullmatch("[0-9a-f]{40}", tree)
            or (expected_tree is not None and tree != expected_tree)):
        raise RuntimeError("checkout revision or tree does not match its binding")
    clean_checkout(root, workspace_target=workspace_target, env=env)
    return {"path": str(root), "head": actual, "tree": tree, "clean": True}


def hosted_context():
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("hosted CI only; this script never admits a local build")
    if os.environ.get("GITHUB_REPOSITORY") != "matrixarkai/TemporalStore":
        raise RuntimeError("unexpected CI repository")
    workspace = Path(os.environ["GITHUB_WORKSPACE"]).resolve(strict=True)
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    target = Path(os.environ["CARGO_TARGET_DIR"]).resolve(strict=True)
    if (workspace / "target").is_symlink() or target != workspace / "target":
        raise RuntimeError("use the existing CI workspace target")
    if not ROOT.is_relative_to(workspace):
        raise RuntimeError("oracle must come from the tested checkout")
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    expected = event.get("pull_request", {}).get("head", {}).get("sha", os.environ["GITHUB_SHA"])
    if not re.fullmatch("[0-9a-f]{40}", expected):
        raise RuntimeError("missing immutable tested revision")
    bound = checkout_binding(workspace, expected, workspace_target=True)
    return workspace, temporary, target, bound["head"], bound["tree"]


def clean_checkout(root, workspace_target=False, env=None):
    subprocess.run(["git", "-C", str(root), "diff", "HEAD", "--exit-code", "--quiet",
                    "--ignore-submodules=none"], env=env, check=True, timeout=30)
    command = ["git", "-C", str(root), "status", "--porcelain=v1", "-z",
               "--untracked-files=all", "--ignored"]
    if workspace_target:
        command += ["--", ".", ":(top,literal,exclude)target"]
    status = subprocess.check_output(command, env=env, timeout=30)
    unknown = []
    for row in status.split(b"\0"):
        if not row:
            continue
        artifact = row[:3] in (b"?? ", b"!! ") and (
            row[3:] == b"target" or row[3:].startswith(b"target/"))
        if workspace_target and artifact:
            continue
        unknown.append(row)
    if unknown:
        paths = [os.fsdecode(row[:512]) for row in unknown[:16]]
        raise RuntimeError("staged, modified or unknown source in a product checkout: "
                           + json.dumps(paths, ensure_ascii=True))


def run(command, log, env, timeout):
    if owned_children.children():
        raise RuntimeError("a prior CI phase still owns child processes")
    with log.open("xb") as stream:
        process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                   env=env, start_new_session=True)
        deadline = time.monotonic() + timeout
        failure = None
        try:
            while True:
                code = process.poll()
                rows = owned_children.children()
                owned_children.reap(rows, process.pid)
                live = [child for child in rows if child.state != "Z"]
                if time.monotonic() > deadline:
                    failure = "phase exceeded its bounded duration"
                elif log.stat().st_size > 32 << 20:
                    failure = "phase output exceeded its bound"
                elif min(shutil.disk_usage(log.parent).free,
                         shutil.disk_usage(env["CARGO_TARGET_DIR"]).free) < 256 << 20:
                    failure = "CI scratch reserve exhausted"
                if failure:
                    break
                if code is not None:
                    if live:
                        failure = "phase left surviving descendants"
                    break
                time.sleep(0.2)
        finally:
            owned_children.terminate(process)
        code = process.wait()
    if log.stat().st_size > 32 << 20:
        failure = "phase output exceeded its bound"
    if failure or code:
        raise RuntimeError(f"{failure or 'phase failed'}: exit={code}, log={log.name}")
    return log.read_text()


def clean_env():
    env = dict(os.environ)
    for name in list(env):
        if name.startswith(("MATRIXARK_", "TS_", "CARGO_PROFILE_", "GIT_", "CARGO_UNSTABLE_")) or name in (
                "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTDOC", "RUSTC_WRAPPER",
                "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER",
                "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET", "RUSTC_BOOTSTRAP"):
            env.pop(name)
    env.update(CARGO_TERM_COLOR="never", CARGO_BUILD_JOBS="1", GIT_CONFIG_GLOBAL="/dev/null",
               GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0", GIT_CONFIG_COUNT="2",
               GIT_CONFIG_KEY_0="maintenance.auto", GIT_CONFIG_VALUE_0="false",
               GIT_CONFIG_KEY_1="gc.auto", GIT_CONFIG_VALUE_1="0")
    return env


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    workspace, temporary, target, consumer, tree = hosted_context()
    owned_children.enable()
    source = {name: sha((ROOT / name).read_bytes()) for name in FILES}
    fixture = sha(json.dumps(source, sort_keys=True, separators=(",", ":")).encode())
    work = temporary / ("checkpoint-upgrade-" + consumer)
    work.mkdir()
    report = {"consumer": consumer, "consumer_tree": tree, "producer": OLD,
              "fixture_sha256": fixture, "sources": source, "builds": {},
              "phases": [], "commands": [], "passed": False, "latency_measured": False}
    receipt = work / "receipt.json"

    def save():
        pending = receipt.with_suffix(".pending")
        pending.write_text(json.dumps(report, indent=2) + "\n")
        pending.replace(receipt)

    save()
    env = clean_env()
    env.update(CARGO_TARGET_DIR=str(target), UPGRADE_FIXTURE_SOURCE_SHA256=fixture,
               UPGRADE_CONSUMER_COMMIT=consumer)
    report["git_policy"] = {"maintenance.auto": False, "gc.auto": 0}
    report["git_version"] = subprocess.check_output(["git", "--version"], env=env, text=True).strip()
    checkouts = {"producer": target / "checkpoint-producer", "consumer": workspace}
    bindings = {
        "producer": checkout_binding(checkouts["producer"], OLD, OLD_TREE),
        "consumer": checkout_binding(workspace, consumer, tree, workspace_target=True),
    }
    report["source_checkouts"] = bindings
    save()

    def execute(command, log, timeout):
        row = {"command": command, "log": log.name, "timeout_seconds": timeout,
               "completed": False, "configuration": {key: env[key] for key in (
                   "CARGO_TARGET_DIR", "CARGO_BUILD_JOBS", "RUSTC_WORKSPACE_WRAPPER",
                   "UPGRADE_FIXTURE_SOURCE_SHA256", "UPGRADE_CONSUMER_COMMIT",
                   "UPGRADE_TARGET_FORMAT", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0",
                   "GIT_CONFIG_VALUE_0", "GIT_CONFIG_KEY_1", "GIT_CONFIG_VALUE_1") if key in env}}
        report["commands"].append(row)
        save()
        try:
            output = run(command, log, env, timeout)
        except Exception as error:
            row["error"] = str(error)
            save()
            raise
        row.update(completed=True, log_sha256=sha(log.read_bytes()))
        save()
        return output
    wrapper = work / ("workspace-" + fixture + ".sh")
    wrapper.write_text('#!/bin/sh\nexec "$@"\n')
    wrapper.chmod(0o755)
    env["RUSTC_WORKSPACE_WRAPPER"] = str(wrapper)
    report["toolchains"] = {tool: subprocess.check_output([tool, "+1.89.0", "-vV"], text=True)
                            for tool in ("rustc", "cargo")}
    executables = {}
    for role, revision in (("producer", OLD), ("consumer", consumer)):
        project = work / role
        project.mkdir()
        library_manifest = checkouts[role] / LIBRARY
        if library_manifest.is_symlink() or library_manifest.resolve(strict=True) != library_manifest:
            raise RuntimeError("library manifest escaped the bound checkout layout")
        manifest_text = render_manifest((ROOT / role / "Cargo.toml").read_text(), library_manifest.parent)
        (project / "Cargo.toml").write_text(manifest_text)
        shutil.copyfile(ROOT / role / "main.rs", project / "main.rs")
        shutil.copyfile(ROOT / "common.rs", work / "common.rs")
        manifest = ["--manifest-path", str(project / "Cargo.toml")]
        execute(["cargo", "+1.89.0", "generate-lockfile", *manifest], work / (role + "-resolve.log"), 180)
        metadata = one_json(execute(["cargo", "+1.89.0", "metadata", "--locked", "--format-version", "1", *manifest],
                                   work / (role + "-metadata.log"), 180), ("packages", "resolve"))
        product = product_package(metadata, library_manifest, project / "Cargo.toml")
        product_node = next(row for row in metadata["resolve"]["nodes"] if row["id"] == product["id"])
        library = library_manifest.parent
        if checkout_binding(checkouts[role], revision, bindings[role]["tree"],
                            workspace_target=role == "consumer") != bindings[role]:
            raise RuntimeError("source checkout changed before compilation")
        engine_bytes = (library / "src/engine.rs").read_bytes()
        body = engine_bytes.decode("utf-8")
        version = declared_format(body, role)
        # The producer uses the same oracle bytes but never emits the consumer format.
        env["UPGRADE_TARGET_FORMAT"] = str(version)
        output = execute(["cargo", "+1.89.0", "build", "--locked", "--release", "-j", "1", *manifest,
                          "--message-format=json-render-diagnostics"], work / (role + "-build.log"), 1800)
        binary_name = "checkpoint-v32-producer" if role == "producer" else "checkpoint-upgrade-consumer"
        artifacts = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        binaries = [row["executable"] for row in artifacts if row.get("reason") == "compiler-artifact"
                    and row.get("package_id") == metadata["resolve"]["root"]
                    and row.get("target", {}).get("name") == binary_name and row.get("executable")]
        if len(binaries) != 1:
            raise RuntimeError("missing exact bridge executable")
        binary = Path(binaries[0]).resolve(strict=True)
        if not binary.is_relative_to(target):
            raise RuntimeError("bridge executable escaped its build target")
        identity = one_json(execute([str(binary), "--identity"], work / (role + "-identity.log"), 30),
                            ("role", "lib_commit", "fixture_sha256", "expected_format"))
        if identity["lib_commit"] != revision or identity["fixture_sha256"] != fixture or identity["role"] != role:
            raise RuntimeError("compiled bridge identity mismatch")
        if identity["expected_format"] != version:
            raise RuntimeError("compiled bridge format is not its library's declared format")
        after = checkout_binding(checkouts[role], revision, bindings[role]["tree"],
                                 workspace_target=role == "consumer")
        if after != bindings[role]:
            raise RuntimeError("source checkout changed during compilation")
        report["builds"][role] = {"commit": revision, "source_kind": "path", "cargo_source": None,
            "manifest_path": str(library_manifest), "source_checkout": after,
            "engine_sha256": sha(engine_bytes), "format": version,
            "features": product_node["features"],
            "artifact_profiles": [row["profile"] for row in artifacts
                                  if row.get("package_id") == product["id"]
                                  and row.get("reason") == "compiler-artifact"],
            "lock_sha256": sha((project / "Cargo.lock").read_bytes()),
            "metadata_sha256": sha(json.dumps(metadata, sort_keys=True).encode()),
            "manifest_sha256": sha((project / "Cargo.toml").read_bytes()),
            "binary_sha256": sha(binary.read_bytes()), "identity": identity}
        executables[role] = binary
        save()
    for case in ("empty", "retained"):
        corpus = work / case
        for role, phase in (("producer", "produce"), ("consumer", "upgrade"), ("consumer", "reopen")):
            summary = one_json(execute([str(executables[role]), phase, str(corpus), case],
                                       work / (case + "-" + phase + ".log"), 120),
                               ("identity", "passed", "case", "phase"))
            expected = OLD if role == "producer" else consumer
            if (summary.get("passed") is not True or summary.get("case") != case
                    or summary.get("phase") != phase or summary["identity"]["lib_commit"] != expected
                    or summary["identity"]["fixture_sha256"] != fixture):
                raise RuntimeError("phase did not produce exact bound successful evidence")
            report["phases"].append(summary)
            save()
    if source != {name: sha((ROOT / name).read_bytes()) for name in FILES}:
        raise RuntimeError("oracle source changed during validation")
    for role, revision in (("producer", OLD), ("consumer", consumer)):
        if checkout_binding(checkouts[role], revision, bindings[role]["tree"],
                            workspace_target=role == "consumer") != bindings[role]:
            raise RuntimeError("source checkout changed during the upgrade")
    for role, binary in executables.items():
        if sha(binary.read_bytes()) != report["builds"][role]["binary_sha256"]:
            raise RuntimeError("oracle executable changed during validation")
    report["passed"] = True
    save()
    print(json.dumps({"passed": True, "consumer": consumer, "tree": tree,
                      "receipt": str(receipt), "phases": len(report["phases"])}))


if __name__ == "__main__":
    main()
