#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Compose a deployment's shape, storage and keys, and say what the engine will actually do with it.

Why this is a separate surface from the settings registry: the storage directory, the metaserver
address and the topology are deliberately NOT writable from the portal's settings form, and that is
the right call -- repointing a running deployment's storage from a browser does not reconfigure it,
it strands its data. But "you cannot change it here" and "you cannot choose it at all" are different
things, and only the first was true. Choosing happens when a deployment is *launched*, so that is
where this lives.

The part worth having is not the form. It is that three of these choices resolve to something other
than what was asked, with no error anywhere:

* `TS_STORAGE_BACKEND=shared` with no `TS_SHARED_STORE_DIR` falls through to auto. The engine says so
  in its resolved-backend log line and nowhere else.
* `TS_STORAGE_BACKEND=matrixobject` on a build without the feature compiled also falls through to
  auto -- the same silent path, reached a different way.
* A real `TS_META_ADDR` turns a standalone box into a distributed one on its own, because standalone
  is derived (`!(meta_addr_is_real || TS_DISTRIBUTED)`) rather than defaulted. A single-box plan that
  carries a metaserver address is a distributed plan wearing the wrong name.

Each of those produces a deployment that starts, serves, and is not the one that was asked for. So
`plan()` resolves the choice the same way the engine does and reports the backend that will actually
be selected, next to the one that was requested.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

Json = Dict[str, Any]

# Values of TS_META_ADDR the engine reads as "no metaserver". Empty is one of them, so a plan can
# express standalone either by omitting the variable or by pinning a sentinel.
META_SENTINELS = ("", "local", "none", "standalone", "off")

# The disk a datanode's pages, blobs, index and cache sit on. All three are ordinary filesystem
# paths to the engine -- it has no notion of which device is underneath -- so the difference is
# what a customer is choosing operationally, and the durability note is the honest part.
DISK_TIERS: Tuple[Json, ...] = (
    {"id": "ebs", "label": "EBS volume", "default_root": "/var/lib/temporalstore",
     "note": "Survives instance stop/start and can be snapshotted. The default for anything whose "
             "loss would matter."},
    {"id": "ssd", "label": "Instance SSD", "default_root": "/mnt/nvme/temporalstore",
     "note": "Fastest, and erased when the instance stops. Appropriate when the store can be "
             "rebuilt from somewhere else, and a bad choice when it cannot."},
    {"id": "local", "label": "Local disk", "default_root": "/var/lib/temporalstore",
     "note": "Whatever the machine already has. The right answer off AWS and for a laptop."},
)

SHAPES: Tuple[Json, ...] = (
    {"id": "onebox", "label": "One box",
     "summary": "Everything in a single process. No metaserver, no peers.",
     "when": "Trying it out, a single-tenant deployment, or any load one machine can hold. This is "
             "the default and it needs no configuration to work.",
     "nodes": 1, "storage_kinds": ("disk",)},
    {"id": "raft", "label": "Replicated (Raft)",
     "summary": "An odd number of nodes replicating through Raft, each with its own disk.",
     "when": "The store must survive losing a machine. Costs a write barrier per commit and needs a "
             "majority to serve writes at all.",
     "nodes": 3, "storage_kinds": ("disk",)},
    {"id": "shared", "label": "Shared storage",
     "summary": "Nodes are stateless in front of one shared store; MatrixObject by default.",
     "when": "Nodes should be replaceable and capacity should grow without moving data between "
             "them. Durability belongs to the shared store rather than to any node.",
     "nodes": 2, "storage_kinds": ("matrixobject", "path")},
)

_SHAPES_BY_ID = {shape["id"]: shape for shape in SHAPES}
_TIERS_BY_ID = {tier["id"]: tier for tier in DISK_TIERS}


def catalogue(matrixobject_available: bool = True) -> Json:
    """Everything the portal needs to render the chooser, including what this build can honour.

    `matrixobject_available` is reported rather than assumed: the shared shape's default storage is
    unreachable on a build without that feature compiled, and offering it as the default on such a
    build is offering a choice that silently becomes something else.
    """
    shapes: List[Json] = []
    for shape in SHAPES:
        entry = dict(shape)
        entry["storage"] = [dict(tier) for tier in DISK_TIERS] if "disk" in shape["storage_kinds"] \
            else _shared_stores(matrixobject_available)
        shapes.append(entry)
    return {
        "shapes": shapes,
        "matrixobject_available": bool(matrixobject_available),
        "default_shape": "onebox",
    }


def _shared_stores(matrixobject_available: bool) -> List[Json]:
    return [
        {"id": "matrixobject", "label": "MatrixObject", "default_root": "",
         "available": bool(matrixobject_available),
         "note": "Content-addressed shared object storage, and the default for this shape. "
                 + ("" if matrixobject_available else
                    "This build does not have it compiled in, so selecting it resolves to "
                    "auto-detection instead -- which is not an error and not logged as a refusal.")},
        {"id": "path", "label": "Shared filesystem", "default_root": "/srv/temporalstore/shared",
         "available": True,
         "note": "A directory every node can reach -- NFS, EFS, or a shared mount. Needs the path "
                 "to actually be shared; a per-node directory of the same name looks identical at "
                 "configuration time and diverges silently once nodes write to it."},
    ]


def _clean(value: Optional[str]) -> str:
    return str(value or "").strip()


def resolve_backend(env: Json, matrixobject_available: bool = True,
                    endpoint_reachable: bool = False) -> Json:
    """What the engine will select, given this environment.

    Mirrors `StorageBackendConfig::resolve_decision`: raft is forced; matrixobject is forced when
    compiled; shared is forced only when a directory is configured; everything else -- including a
    shared or matrixobject request that cannot be honoured -- falls through to auto. The fall-through
    is the whole reason this function exists, because from the outside it is indistinguishable from
    having been honoured.
    """
    requested = _clean(env.get("TS_STORAGE_BACKEND")).lower()
    shared_dir = _clean(env.get("TS_SHARED_STORE_DIR"))
    endpoint = _clean(env.get("MATRIXARK_OBJECT_RPC_URL"))

    if requested in ("raft", "raft_replication", "replication"):
        return {"backend": "raft", "honoured": True,
                "reason": "TS_STORAGE_BACKEND=raft forces raft replication."}
    if requested in ("matrixobject", "matrix_object", "object"):
        if matrixobject_available:
            return {"backend": "matrixobject", "honoured": True,
                    "reason": "TS_STORAGE_BACKEND=matrixobject is forced without probing."}
        return dict(_auto(matrixobject_available, endpoint, endpoint_reachable, shared_dir),
                    honoured=False,
                    reason="TS_STORAGE_BACKEND=matrixobject, but this build has no MatrixObject "
                           "compiled in, so selection falls through to auto-detection.")
    if requested in ("shared", "shared_path", "shared_store", "path"):
        if shared_dir:
            return {"backend": "shared_path", "honoured": True,
                    "reason": "TS_STORAGE_BACKEND=shared with a configured directory (%s)."
                              % shared_dir}
        return dict(_auto(matrixobject_available, endpoint, endpoint_reachable, shared_dir),
                    honoured=False,
                    reason="TS_STORAGE_BACKEND=shared with no TS_SHARED_STORE_DIR set, so selection "
                           "falls through to auto-detection rather than failing.")
    return dict(_auto(matrixobject_available, endpoint, endpoint_reachable, shared_dir),
                honoured=True)


def _auto(matrixobject_available: bool, endpoint: str, endpoint_reachable: bool,
          shared_dir: str) -> Json:
    """Auto's order: a reachable networked object store, then node-local, then a shared path, then
    raft."""
    if matrixobject_available:
        if endpoint and endpoint_reachable:
            return {"backend": "matrixobject",
                    "reason": "auto: the MatrixObject endpoint %s answered." % endpoint}
        if not endpoint:
            return {"backend": "matrixobject",
                    "reason": "auto: node-local MatrixObject, no endpoint configured."}
    if shared_dir:
        return {"backend": "shared_path",
                "reason": "auto: a shared store directory is configured (%s)." % shared_dir}
    return {"backend": "raft", "reason": "auto: nothing shared is reachable, so raft replication."}


def is_standalone(env: Json) -> bool:
    """The engine's own derivation, which is not "TS_STANDALONE defaults to on"."""
    forced = _clean(env.get("TS_STANDALONE")).lower()
    if forced in ("1", "true", "yes", "on"):
        return True
    if forced in ("0", "false", "no", "off"):
        return False
    meta = _clean(env.get("TS_META_ADDR")).lower()
    meta_is_real = meta not in META_SENTINELS
    distributed = _clean(env.get("TS_DISTRIBUTED")).lower() in ("1", "true", "yes", "on")
    return not (meta_is_real or distributed)


_KEY_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,63}$")


def plan(shape: str, storage: str = "", nodes: int = 0, root: str = "",
         shared_dir: str = "", key_envs: Optional[List[str]] = None,
         matrixobject_available: bool = True, endpoint_reachable: bool = False) -> Json:
    """Compose one deployment. Returns the environment, plus what it will really resolve to.

    `blocking` is for choices that cannot produce the deployment asked for at all; `warnings` is for
    ones that produce a working deployment that differs from the request. They are separate because
    they need different answers -- a block is a mistake to fix, a warning is a fact to accept.
    """
    blocking: List[str] = []
    warnings: List[str] = []
    notes: List[str] = []

    spec = _SHAPES_BY_ID.get(shape)
    if spec is None:
        return {"ok": False, "env": {}, "blocking": ["Unknown deployment shape %r." % shape],
                "warnings": [], "notes": [], "shape": shape}

    count = int(nodes or spec["nodes"])
    env: Json = {}

    if shape == "onebox":
        if count != 1:
            blocking.append("A one-box deployment is one node; %d was requested. Pick the "
                            "replicated shape for more than one." % count)
        # Pinned rather than omitted. Standalone is DERIVED from the metaserver address, so a
        # deployment that later inherits a TS_META_ADDR from a shared config file or a systemd
        # drop-in silently becomes distributed. Pinning it means that cannot happen quietly.
        env["TS_STANDALONE"] = "1"
        env["TS_DISTRIBUTED"] = "0"
        notes.append("TS_STANDALONE is pinned to 1 rather than left to the default. Standalone is "
                     "derived from whether a metaserver address is set, so an address arriving "
                     "later from a config file would otherwise flip this box to distributed with "
                     "nothing said.")
    elif shape == "raft":
        if count < 3:
            blocking.append("Raft needs at least 3 nodes to tolerate losing one; %d was "
                            "requested." % count)
        elif count % 2 == 0:
            blocking.append("Raft needs an odd node count to have a majority; %d gives the same "
                            "fault tolerance as %d while costing one more machine."
                            % (count, count - 1))
        env["TS_STANDALONE"] = "0"
        env["TS_DISTRIBUTED"] = "1"
        env["TS_STORAGE_BACKEND"] = "raft"
        env["TS_META_RAFT"] = "1"
        env["TS_RAFT_AUTO_FAILOVER"] = "1"
        notes.append("Writes need a majority: %d nodes serve writes while %d are reachable, and go "
                     "read-only below that." % (count, count // 2 + 1))
    elif shape == "shared":
        if count < 2:
            blocking.append("Shared storage exists so nodes are replaceable; %d node is a one-box "
                            "deployment with extra moving parts." % count)
        env["TS_STANDALONE"] = "0"
        env["TS_DISTRIBUTED"] = "1"

    kinds = spec["storage_kinds"]
    choice = _clean(storage) or ("matrixobject" if "matrixobject" in kinds else "ebs")

    if "disk" in kinds:
        tier = _TIERS_BY_ID.get(choice)
        if tier is None:
            blocking.append("Unknown storage tier %r for the %s shape." % (choice, shape))
        else:
            base = _clean(root) or tier["default_root"]
            env["TS_PAGE_STORE_DIR"] = base + "/pages"
            env["TS_BLOB_STORE_DIR"] = base + "/blobs"
            env["TS_INDEX_DIR"] = base + "/index"
            env["TS_CACHE_DIR"] = base + "/cache"
            if shape == "raft":
                env["TS_RAFT_WAL_DIR"] = base + "/raft-wal"
            if choice == "ssd":
                warnings.append("Instance SSD is erased when the instance stops. Everything under "
                                "%s goes with it, including the write-ahead log, so this is a "
                                "choice to make only when the store can be rebuilt from "
                                "elsewhere." % base)
            notes.append("%s: %s" % (tier["label"], tier["note"]))
    else:
        if choice == "matrixobject":
            env["TS_STORAGE_BACKEND"] = "matrixobject"
            if not matrixobject_available:
                warnings.append("This build has no MatrixObject compiled in. The engine will not "
                                "refuse the setting -- it falls through to auto-detection, so the "
                                "deployment starts and serves on a different backend than the one "
                                "chosen here.")
        elif choice == "path":
            directory = _clean(shared_dir)
            if not directory:
                blocking.append("A shared filesystem needs a directory every node can reach. "
                                "Without TS_SHARED_STORE_DIR the engine falls through to "
                                "auto-detection and quietly selects a different backend.")
            else:
                env["TS_STORAGE_BACKEND"] = "shared"
                env["TS_SHARED_STORE_DIR"] = directory
                warnings.append("Every node must see %s as the SAME storage. A per-node directory "
                                "of that name is indistinguishable from a shared one at launch and "
                                "diverges only once nodes start writing." % directory)
        else:
            blocking.append("Unknown shared store %r." % choice)

    planned_keys: List[str] = []
    for name in (key_envs or []):
        clean = _clean(name)
        if not _KEY_NAME.match(clean):
            blocking.append("%r is not usable as an environment variable name for a key." % name)
        else:
            planned_keys.append(clean)
            # The NAME is part of the plan; the value never is. Keys are written through the
            # existing write-only secret path, so a plan can be shown, exported and diffed without
            # carrying a credential into any of those.
            notes.append("%s is named by the plan; its value is entered separately and is never "
                         "part of the plan document." % clean)

    resolved = resolve_backend(env, matrixobject_available, endpoint_reachable)
    if not resolved.get("honoured", True):
        warnings.append(resolved["reason"])

    if shape == "onebox":
        # A one-box plan deliberately names no backend, so the engine auto-selects -- which means
        # the same plan produces a different backend on a build with MatrixObject compiled in than
        # on one without. That is fine, and it is not something to discover from a log line after
        # the fact, so the plan says which one this build will pick and why.
        notes.append("No storage backend is pinned, so the engine auto-selects. On this build that "
                     "is %s (%s). The directories above are where the data lands either way."
                     % (resolved["backend"], resolved["reason"]))

    if shape == "onebox" and not is_standalone(env):
        blocking.append("This one-box plan does not resolve to standalone.")
    if shape in ("raft", "shared") and is_standalone(env):
        blocking.append("This %s plan resolves to standalone, which is not a cluster." % shape)

    return {
        "ok": not blocking,
        "shape": shape,
        "nodes": count,
        "storage": choice,
        "env": env,
        # Carried as data. Recovering these by reading the notes meant any note beginning with a
        # capitalised token was mistaken for a key -- the one-box note starts "TS_STANDALONE is
        # pinned...", so a pinned topology flag was being listed as a secret to go and fetch.
        "key_envs": planned_keys,
        "resolved_backend": resolved["backend"],
        "backend_reason": resolved["reason"],
        "blocking": blocking,
        "warnings": warnings,
        "notes": notes,
    }


# ------------------------------------------------------------------------------------------------
# Launch artifact
#
# The portal composes what to run; it never runs it. That keeps AWS credentials out of the gateway
# and keeps a browser click from creating billable infrastructure, and it produces something equally
# usable by hand, from Terraform, or in CI.
#
# Two properties are load-bearing rather than stylistic:
#
# * **No secret ever enters user-data.** Instance user-data is readable from the instance metadata
#   service by anything running on the box, and it is visible in the console and in
#   `describe-instance-attribute` to anyone with those permissions. A key pasted here is a key
#   published. So the artifact names the variables and leaves them unset, and the box is expected to
#   get their values from a secret store or an operator.
# * **Ephemeral disks are prepared as ephemeral.** Instance SSD is erased on stop, so the artifact
#   formats it on every boot; an EBS volume is formatted only if it has no filesystem, because
#   formatting one that does is how a durable store is destroyed by a reboot.
# ------------------------------------------------------------------------------------------------

# Where the disk tiers land. Instance SSD is an NVMe device that is re-attached blank; EBS and a
# plain local disk are expected to persist.
_DEVICE = {
    "ssd": {"device": "/dev/nvme1n1", "ephemeral": True},
    "ebs": {"device": "/dev/xvdf", "ephemeral": False},
    "local": {"device": "", "ephemeral": False},
}

PUBLIC_REPO = "https://github.com/matrixarkai/TemporalStore.git"


def cloud_init(plan_doc: Json, repo: str = PUBLIC_REPO, ref: str = "main") -> str:
    """The user-data script for a box running this plan.

    Refuses to render a plan that is blocked: emitting a launch script for a configuration already
    known not to produce the requested deployment is how the warning gets skipped.
    """
    if not plan_doc.get("ok", False):
        raise ValueError("refusing to build a launch artifact for a blocked plan: %s"
                         % "; ".join(plan_doc.get("blocking") or ["unknown"]))

    env = plan_doc.get("env") or {}
    storage = str(plan_doc.get("storage") or "")
    disk = _DEVICE.get(storage, _DEVICE["local"])
    root = ""
    for name in ("TS_PAGE_STORE_DIR", "TS_BLOB_STORE_DIR"):
        if env.get(name):
            root = str(env[name]).rsplit("/", 1)[0]
            break
    root = root or "/var/lib/temporalstore"

    lines = [
        "#!/bin/bash",
        "set -euxo pipefail",
        "",
        "# MatrixArk %s deployment, %s storage. Generated from a deployment plan."
        % (plan_doc.get("shape"), storage or "local"),
        "# Resolves to the %s backend: %s"
        % (plan_doc.get("resolved_backend"), plan_doc.get("backend_reason")),
        "",
    ]
    for warning in plan_doc.get("warnings") or []:
        lines.append("# WARNING: " + warning)
    if plan_doc.get("warnings"):
        lines.append("")

    lines += [
        "export DEBIAN_FRONTEND=noninteractive",
        "apt-get update -y",
        "apt-get install -y git curl build-essential pkg-config libssl-dev",
        "",
    ]

    if disk["device"]:
        lines += ["# ---- data disk ----"]
        if disk["ephemeral"]:
            lines += [
                "# Instance SSD is re-attached blank on every start, so it is formatted every boot.",
                "mkfs.ext4 -F %s" % disk["device"],
            ]
        else:
            lines += [
                "# Formatted ONLY when it has no filesystem. Formatting a volume that already has",
                "# one is how a durable store is destroyed by an ordinary reboot.",
                "if ! blkid %s >/dev/null 2>&1; then mkfs.ext4 -F %s; fi"
                % (disk["device"], disk["device"]),
            ]
        lines += [
            "mkdir -p %s" % root,
            "mount %s %s" % (disk["device"], root),
            "",
        ]

    lines += ["mkdir -p %s" % root, "", "# ---- configuration ----",
              "cat > /etc/temporalstore.env <<'MATRIXARK_ENV'"]
    for name in sorted(env):
        lines.append("%s=%s" % (name, env[name]))

    key_names = sorted(_key_names(plan_doc))
    if key_names:
        lines += [
            "",
            "# Named, deliberately unset. User-data is readable from the instance metadata service",
            "# by anything on this box and via describe-instance-attribute, so a key written here",
            "# is a key published. Populate these from a secret store before starting the service.",
        ]
        for name in key_names:
            lines.append("# %s=" % name)
    lines += ["MATRIXARK_ENV", "chmod 0600 /etc/temporalstore.env", ""]

    lines += [
        "# ---- install ----",
        "git clone --depth 1 --branch %s %s /opt/temporalstore-src" % (ref, repo),
        "cd /opt/temporalstore-src",
        "set -a; . /etc/temporalstore.env; set +a",
        "bash tools/install_linux_temporalstore.sh",
        "",
    ]
    return "\n".join(lines) + "\n"


def _key_names(plan_doc: Json) -> List[str]:
    """Key variable names carried by a plan, as data rather than scraped from its prose."""
    return [str(name) for name in (plan_doc.get("key_envs") or [])]


def launch_commands(plan_doc: Json, instance_type: str = "m6i.xlarge",
                    region: str = "us-east-1", ami: str = "",
                    key_name: str = "", subnet: str = "",
                    security_group: str = "", tag: str = "matrixark-onebox") -> Json:
    """The commands to create and to destroy this deployment.

    Teardown is produced alongside launch, never as a follow-up: an instance whose termination
    command has to be reconstructed later is one that stays running.
    """
    nodes = int(plan_doc.get("nodes", 1) or 1)
    parts = [
        "aws ec2 run-instances",
        "  --region %s" % region,
        "  --image-id %s" % (ami or "<AMI-ID>"),
        "  --instance-type %s" % instance_type,
        "  --count %d" % nodes,
        "  --user-data file://user-data.sh",
        "  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=%s}]'" % tag,
    ]
    if key_name:
        parts.append("  --key-name %s" % key_name)
    if subnet:
        parts.append("  --subnet-id %s" % subnet)
    if security_group:
        parts.append("  --security-group-ids %s" % security_group)
    if plan_doc.get("storage") == "ebs":
        parts.append("  --block-device-mappings "
                     "'[{\"DeviceName\":\"/dev/xvdf\",\"Ebs\":{\"VolumeSize\":200,"
                     "\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]'")

    teardown = (
        "# Destroy everything this launched. Run it -- an idle instance bills until it is gone.\n"
        "aws ec2 describe-instances --region %s \\\n"
        "  --filters 'Name=tag:Name,Values=%s' 'Name=instance-state-name,Values=running,stopped' \\\n"
        "  --query 'Reservations[].Instances[].InstanceId' --output text \\\n"
        "  | xargs -r aws ec2 terminate-instances --region %s --instance-ids"
        % (region, tag, region))

    return {
        "launch": " \\\n".join(parts),
        "teardown": teardown,
        "instances": nodes,
        "note": "user-data.sh is the script above. Nothing here runs from the portal -- it holds no "
                "AWS credentials and creating billable infrastructure is not something a page "
                "should do on a click.",
    }


def as_env_file(plan_doc: Json) -> str:
    """The plan as something that can be pasted into a unit file or an env file."""
    lines = ["# MatrixArk deployment: %s, %d node(s), %s storage."
             % (plan_doc.get("shape"), plan_doc.get("nodes", 0), plan_doc.get("storage")),
             "# Resolves to the %s backend: %s"
             % (plan_doc.get("resolved_backend"), plan_doc.get("backend_reason"))]
    for warning in plan_doc.get("warnings") or []:
        lines.append("# WARNING: " + warning)
    for name in sorted(plan_doc.get("env") or {}):
        lines.append("%s=%s" % (name, plan_doc["env"][name]))
    return "\n".join(lines) + "\n"
