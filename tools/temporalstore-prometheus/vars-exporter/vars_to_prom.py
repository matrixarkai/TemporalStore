#!/usr/bin/env python3
# Convert TemporalStore /vars output to Prometheus textfile-collector format.

from __future__ import annotations

import argparse
import math
import os
import re
import signal
import sys
import time
import urllib.request
from pathlib import Path


LINE_RE = re.compile(r"^\s*(\S+)\s*[:=]\s*(.+?)\s*$")
METRIC_RE = re.compile(r"^(?P<name>[^{}\s]+)(?:\{(?P<labels>[^}]*)\})?$")
LABEL_RE = re.compile(r'^\s*([^=\s]+)\s*=\s*(.+?)\s*$')
VALUE_RE = re.compile(r"^[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$")
LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape TemporalStore /vars text and expose Prometheus textfile metrics."
    )
    parser.add_argument(
        "--targets",
        required=True,
        help="Comma-separated list of <source>=<url> pairs, e.g. "
        'server=http://127.0.0.1:18001/vars,metaserver=http://127.0.0.1:18000/vars',
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Scrape interval in seconds (default: 5).",
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp/temporalstore-metrics",
        help="Directory where textfile collector reads metrics.",
    )
    parser.add_argument(
        "--output-file",
        default="temporalstore-vars.prom",
        help="Text file name written under output-dir.",
    )
    parser.add_argument("--timeout", type=float, default=2.5, help="Per-target HTTP timeout in seconds.")
    parser.add_argument("--once", action="store_true", help="Scrape once and exit.")
    return parser.parse_args()


def sanitize_metric_name(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_:]", "_", name)
    if not sanitized:
        return "temporalstore_metric"
    if not re.match(r"^[a-zA-Z_:]", sanitized[0]):
        sanitized = f"m_{sanitized}"
    return sanitized


def sanitize_label_name(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if not sanitized:
        return "label"
    if not LABEL_NAME_RE.match(sanitized):
        sanitized = f"label_{sanitized}"
    return sanitized


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")


def parse_label_text(raw: str):
    text = raw.strip()
    if not text:
        return []
    out = []
    for pair in text.split(","):
        pair = pair.strip()
        if not pair:
            continue
        m = LABEL_RE.match(pair)
        if not m:
            continue
        key = sanitize_label_name(m.group(1))
        val = m.group(2).strip()
        if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")):
            val = val[1:-1]
        out.append((key, escape_label(val)))
    return out


def parse_target_list(raw_targets: str):
    result = []
    for item in raw_targets.split(","):
        if not item.strip():
            continue
        if "=" not in item:
            raise ValueError(f"invalid target '{item}', expected <source>=<url>")
        source, url = item.split("=", 1)
        source = source.strip()
        url = url.strip()
        if not source or not url:
            raise ValueError(f"invalid target '{item}', empty source or url")
        result.append((source, url))
    if not result:
        raise ValueError("no valid targets provided")
    return result


def parse_line(line: str, source_label: str):
    m = LINE_RE.match(line)
    if not m:
        return None
    raw_name = m.group(1).strip()
    raw_value = m.group(2).strip()
    if not raw_name or not raw_value:
        return None

    metric_match = METRIC_RE.match(raw_name)
    if not metric_match:
        return None
    name = metric_match.group("name")
    value_text = raw_value.replace(",", "")
    value_tokens = value_text.split()
    if not value_tokens:
        return None
    if not VALUE_RE.match(value_tokens[0]):
        return None

    try:
        value = float(value_tokens[0])
    except ValueError:
        return None
    if not math.isfinite(value):
        return None

    labels = parse_label_text(metric_match.group("labels") or "")
    labels.append(("source", escape_label(source_label)))
    labels_str = ",".join(f'{k}="{v}"' for k, v in labels) if labels else ""
    return sanitize_metric_name(name), value, labels_str


def scrape_target(source: str, url: str, timeout: float):
    req = urllib.request.Request(url, headers={"Accept": "text/plain", "User-Agent": "temporalstore-vars-exporter/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = resp.read().decode("utf-8", errors="ignore")
    return payload.splitlines()


def write_prom(output_dir: Path, output_file: str, samples):
    output_dir.mkdir(parents=True, exist_ok=True)
    tmp_file = output_dir / f".{output_file}.tmp"
    target_file = output_dir / output_file
    lines = []
    last_metric_name = None
    for metric_name, metric_value, metric_labels in sorted(samples):
        if metric_name != last_metric_name:
            lines.append(f"# HELP {metric_name} TemporalStore metric converted from /vars\n")
            lines.append(f"# TYPE {metric_name} gauge\n")
            last_metric_name = metric_name
        metric_series = metric_name
        if metric_labels:
            metric_series += f"{{{metric_labels}}}"
        lines.append(f"{metric_series} {metric_value}\n")
    with tmp_file.open("w", encoding="utf-8") as f:
        if lines:
            f.writelines(lines)
    tmp_file.replace(target_file)


def run_once(targets, interval, output_dir, output_file, timeout):
    all_samples = set()
    for source, url in targets:
        try:
            for line in scrape_target(source, url, timeout):
                sample = parse_line(line, source)
                if sample is not None:
                    all_samples.add(sample)
        except Exception as exc:
            print(f"[WARN] failed scrape {source} {url}: {exc}", file=sys.stderr)
            return 1
    write_prom(Path(output_dir), output_file, all_samples)
    return 0


def main():
    args = parse_args()
    targets = parse_target_list(os.environ.get("VARS_TARGETS", args.targets))
    interval = max(1, args.interval)
    output_dir = os.environ.get("VARS_OUTPUT_DIR", args.output_dir)
    output_file = os.environ.get("VARS_OUTPUT_FILE", args.output_file)
    timeout = max(0.5, args.timeout)

    stop = False
    def _stop(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    if args.once:
        sys.exit(run_once(targets, interval, output_dir, output_file, timeout))

    while not stop:
        run_once(targets, interval, output_dir, output_file, timeout)
        if stop:
            break
        time.sleep(interval)


if __name__ == "__main__":
    main()
