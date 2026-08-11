# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of run_matrixark_cpp_rust_scale_report.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import os
import resource
import sys
import time

try:  # package path (tools.run_matrixark_cpp_rust_scale_report)
    from .run_matrixark_cpp_rust_scale_report import (
        Json,
        Path,
    )
except ImportError:  # top-level path (run_matrixark_cpp_rust_scale_report)
    from run_matrixark_cpp_rust_scale_report import (
        Json,
        Path,
    )

__all__ = ['_proc_stat', '_current_rss_mb', '_child_ppid_map', '_descendant_pids', 'process_resource_snapshot', 'process_resource_delta']


def _proc_stat(pid: int) -> tuple[float, float, float] | None:
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        ticks_per_second = os.sysconf("SC_CLK_TCK")
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as fh:
            stat = fh.read()
        after_comm = stat.rsplit(") ", 1)[1].split()
        utime_ticks = float(after_comm[11])
        stime_ticks = float(after_comm[12])
        with open(f"/proc/{pid}/statm", "r", encoding="utf-8") as fh:
            statm = fh.read().split()
        rss_mb = (int(statm[1]) * page_size) / (1024.0 * 1024.0) if len(statm) >= 2 else 0.0
        return (utime_ticks / ticks_per_second, stime_ticks / ticks_per_second, rss_mb)
    except Exception:
        return None


def _current_rss_mb() -> float:
    current = _proc_stat(os.getpid())
    return round(current[2], 3) if current is not None else 0.0


def _child_ppid_map() -> dict[int, int]:
    mapping: dict[int, int] = {}
    proc_root = Path("/proc")
    try:
        entries = list(proc_root.iterdir())
    except Exception:
        return mapping
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text(encoding="utf-8")
            after_comm = stat.rsplit(") ", 1)[1].split()
            mapping[int(entry.name)] = int(after_comm[1])
        except Exception:
            continue
    return mapping


def _descendant_pids(root_pid: int) -> list[int]:
    ppid_by_pid = _child_ppid_map()
    descendants: list[int] = []
    frontier = [root_pid]
    seen = {root_pid}
    while frontier:
        parent = frontier.pop()
        children = [pid for pid, ppid in ppid_by_pid.items() if ppid == parent and pid not in seen]
        for pid in children:
            seen.add(pid)
            descendants.append(pid)
            frontier.append(pid)
    return descendants


def process_resource_snapshot() -> Json:
    if resource is not None:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        max_rss_raw = float(getattr(usage, "ru_maxrss", 0.0) or 0.0)
        max_rss_mb = max_rss_raw / 1024.0 if sys.platform != "darwin" else max_rss_raw / (1024.0 * 1024.0)
        user_cpu_s = float(getattr(usage, "ru_utime", 0.0) or 0.0)
        system_cpu_s = float(getattr(usage, "ru_stime", 0.0) or 0.0)
    else:
        max_rss_mb = 0.0
        user_cpu_s = 0.0
        system_cpu_s = 0.0
    child_user_cpu_s = 0.0
    child_system_cpu_s = 0.0
    child_rss_mb = 0.0
    child_pids = _descendant_pids(os.getpid())
    for pid in child_pids:
        stat = _proc_stat(pid)
        if stat is None:
            continue
        child_user_cpu_s += stat[0]
        child_system_cpu_s += stat[1]
        child_rss_mb += stat[2]
    current_rss_mb = _current_rss_mb()
    tree_user_cpu_s = user_cpu_s + child_user_cpu_s
    tree_system_cpu_s = system_cpu_s + child_system_cpu_s
    tree_current_rss_mb = current_rss_mb + child_rss_mb
    return {
        "wall_time_s": time.perf_counter(),
        "user_cpu_s": user_cpu_s,
        "system_cpu_s": system_cpu_s,
        "total_cpu_s": user_cpu_s + system_cpu_s,
        "max_rss_mb": round(max_rss_mb, 3),
        "current_rss_mb": current_rss_mb,
        "child_process_count": len(child_pids),
        "child_pids": child_pids[:16],
        "child_user_cpu_s": child_user_cpu_s,
        "child_system_cpu_s": child_system_cpu_s,
        "child_total_cpu_s": child_user_cpu_s + child_system_cpu_s,
        "child_current_rss_mb": round(child_rss_mb, 3),
        "tree_user_cpu_s": tree_user_cpu_s,
        "tree_system_cpu_s": tree_system_cpu_s,
        "tree_total_cpu_s": tree_user_cpu_s + tree_system_cpu_s,
        "tree_current_rss_mb": round(tree_current_rss_mb, 3),
        "tree_max_rss_mb": round(max(max_rss_mb, tree_current_rss_mb), 3),
    }


def process_resource_delta(start: Json, end: Json, *, work_units: int = 0) -> Json:
    wall_s = max(0.0, float(end.get("wall_time_s", 0.0)) - float(start.get("wall_time_s", 0.0)))
    self_cpu_s = max(0.0, float(end.get("total_cpu_s", 0.0)) - float(start.get("total_cpu_s", 0.0)))
    self_user_cpu_s = max(0.0, float(end.get("user_cpu_s", 0.0)) - float(start.get("user_cpu_s", 0.0)))
    self_system_cpu_s = max(0.0, float(end.get("system_cpu_s", 0.0)) - float(start.get("system_cpu_s", 0.0)))
    tree_cpu_s = max(0.0, float(end.get("tree_total_cpu_s", end.get("total_cpu_s", 0.0))) - float(start.get("tree_total_cpu_s", start.get("total_cpu_s", 0.0))))
    tree_user_cpu_s = max(0.0, float(end.get("tree_user_cpu_s", end.get("user_cpu_s", 0.0))) - float(start.get("tree_user_cpu_s", start.get("user_cpu_s", 0.0))))
    tree_system_cpu_s = max(0.0, float(end.get("tree_system_cpu_s", end.get("system_cpu_s", 0.0))) - float(start.get("tree_system_cpu_s", start.get("system_cpu_s", 0.0))))
    work_units = max(0, int(work_units or 0))
    return {
        "wall_ms": round(wall_s * 1000.0, 3),
        "cpu_time_ms": round(tree_cpu_s * 1000.0, 3),
        "user_cpu_ms": round(tree_user_cpu_s * 1000.0, 3),
        "system_cpu_ms": round(tree_system_cpu_s * 1000.0, 3),
        "cpu_utilization_pct": round((tree_cpu_s / wall_s) * 100.0, 3) if wall_s > 0 else 0.0,
        "cpu_ms_per_unit": round((tree_cpu_s * 1000.0) / work_units, 6) if work_units > 0 else 0.0,
        "max_rss_mb": round(float(end.get("tree_max_rss_mb", end.get("max_rss_mb", 0.0)) or 0.0), 3),
        "current_rss_mb": round(float(end.get("tree_current_rss_mb", end.get("current_rss_mb", 0.0)) or 0.0), 3),
        "work_units": work_units,
        "resource_accounting": "process_tree_including_live_children",
        "self_cpu_time_ms": round(self_cpu_s * 1000.0, 3),
        "self_user_cpu_ms": round(self_user_cpu_s * 1000.0, 3),
        "self_system_cpu_ms": round(self_system_cpu_s * 1000.0, 3),
        "self_current_rss_mb": round(float(end.get("current_rss_mb", 0.0) or 0.0), 3),
        "self_max_rss_mb": round(float(end.get("max_rss_mb", 0.0) or 0.0), 3),
        "child_cpu_time_ms": round(max(0.0, tree_cpu_s - self_cpu_s) * 1000.0, 3),
        "child_current_rss_mb": round(float(end.get("child_current_rss_mb", 0.0) or 0.0), 3),
        "child_process_count": int(end.get("child_process_count", 0) or 0),
    }
