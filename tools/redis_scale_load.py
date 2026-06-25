#!/usr/bin/env python3
import argparse
import socket
import statistics
import threading
import time


def frame(*parts):
    chunks = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        data = str(part).encode()
        chunks.append(f"${len(data)}\r\n".encode())
        chunks.append(data)
        chunks.append(b"\r\n")
    return b"".join(chunks)


def recv_resp(sock):
    data = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
        if is_complete_resp(data):
            break
    return bytes(data)


def is_complete_resp(data):
    if not data:
        return False
    if data[0:1] in (b"+", b"-", b":"):
        return data.endswith(b"\r\n")
    if data[0:1] == b"$":
        header, _, rest = data.partition(b"\r\n")
        if not rest:
            return False
        size = int(header[1:])
        return size == -1 or len(rest) >= size + 2
    if data[0:1] == b"*":
        return data.endswith(b"\r\n")
    return False


def call(host, port, *parts):
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(frame(*parts))
        return recv_resp(sock)


def worker(worker_id, args, stats, errors):
    deadline = time.monotonic() + args.duration_seconds
    i = 0
    while time.monotonic() < deadline:
        key = f"scale:{worker_id}:{i % args.keyspace}"
        value = f"value-{worker_id}-{i}"
        start = time.perf_counter()
        try:
            set_resp = call(args.host, args.port, "SET", key, value)
            get_resp = call(args.host, args.port, "GET", key)
            if set_resp != b"+OK\r\n" or value.encode() not in get_resp:
                errors.append((set_resp, get_resp))
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            stats.append(elapsed_ms)
        except Exception as exc:
            errors.append(repr(exc))
        i += 1


def main():
    parser = argparse.ArgumentParser(description="Redis-compatible TemporalStore scale load")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=16379)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--duration-seconds", type=int, default=30)
    parser.add_argument("--keyspace", type=int, default=10000)
    args = parser.parse_args()

    stats = []
    errors = []
    threads = [
        threading.Thread(target=worker, args=(idx, args, stats, errors), daemon=True)
        for idx in range(args.concurrency)
    ]
    start = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - start

    stats_sorted = sorted(stats)
    total = len(stats_sorted)
    qps = total / elapsed if elapsed > 0 else 0
    p50 = percentile(stats_sorted, 50)
    p95 = percentile(stats_sorted, 95)
    p99 = percentile(stats_sorted, 99)

    print(f"ops={total}")
    print(f"errors={len(errors)}")
    print(f"elapsed_seconds={elapsed:.3f}")
    print(f"qps={qps:.2f}")
    print(f"latency_ms_p50={p50:.3f}")
    print(f"latency_ms_p95={p95:.3f}")
    print(f"latency_ms_p99={p99:.3f}")
    if errors:
        print(f"first_error={errors[0]}")
        raise SystemExit(1)


def percentile(values, pct):
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[pct - 1]


if __name__ == "__main__":
    main()
