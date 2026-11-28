# SPDX-License-Identifier: Apache-2.0
"""Verify that vLLM reloads KV from TemporalStore after clearing LMCache L1."""

import argparse
import json
import time
from urllib.request import Request, urlopen


def request(url, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    headers = {} if data is None else {"Content-Type": "application/json"}
    with urlopen(Request(url + path, data=data, headers=headers), timeout=120) as response:
        return json.load(response)


def wait_for(read, condition, label):
    result = read()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if condition(result):
            return result
        time.sleep(0.1)
        result = read()
    raise TimeoutError(f"{label} did not advance: {result}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--vllm-url", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--reload-vllm-url", help="vLLM worker that replays the prompt"
    )
    parser.add_argument("--lmcache-url", default="http://127.0.0.1:8080")
    parser.add_argument("--reload-lmcache-url", help="LMCache server attached to the replay worker")
    args = parser.parse_args()
    if args.reload_lmcache_url and not args.reload_vllm_url:
        parser.error("--reload-lmcache-url requires --reload-vllm-url")
    writer = args.lmcache_url.rstrip("/")
    reader = (args.reload_lmcache_url or writer).rstrip("/")

    def adapter_status(url):
        adapters = request(url, "/status")["storage_manager"]["l2_adapters"]
        if not adapters or any(item.get("adapter") != "temporalstore" for item in adapters):
            raise RuntimeError("start LMCache with one or more TemporalStore adapters")
        return adapters

    def changes(current, original, *counters):
        if len(current) != len(original):
            raise RuntimeError("the number of TemporalStore adapters changed during the test")
        return [
            sum(latest.get(counter, 0) - previous.get(counter, 0) for counter in counters)
            for latest, previous in zip(current, original, strict=True)
        ]

    # Use fresh input so a previous smoke run cannot supply this test's store.
    prompt = f"Session {time.time_ns()}. " + "A cache holds computed key and value tensors. " * 96
    payload = {"model": args.model, "prompt": prompt, "max_tokens": 4, "temperature": 0}
    store_counters = ("stores", "stores_confirmed_by_read")
    before = adapter_status(writer)
    first = request(args.vllm_url, "/v1/completions", payload)
    stored = wait_for(
        lambda: adapter_status(writer),
        lambda status: all(count > 0 for count in changes(status, before, *store_counters)),
        "L2 store confirmations on every writer adapter",
    )
    for url in dict.fromkeys((writer, reader)):
        cleared = request(url, "/cache/clear", {})
        if cleared.get("status") != "ok":
            raise RuntimeError(f"L1 clear failed at {url}: {cleared}")
    before_reload = adapter_status(reader)
    second = request(
        args.reload_vllm_url or args.vllm_url, "/v1/completions", payload
    )
    loaded = wait_for(
        lambda: adapter_status(reader),
        lambda status: any(count > 0 for count in changes(status, before_reload, "loads")),
        "L2 loads on the replay adapter",
    )
    if first["choices"][0]["text"] != second["choices"][0]["text"]:
        raise AssertionError("completion changed after loading the cached prefix")
    print(
        f"PASS: TemporalStore store confirmations {changes(stored, before, *store_counters)}, "
        f"loads {changes(loaded, before_reload, 'loads')}, "
        f"prompt tokens={first['usage']['prompt_tokens']}"
    )


if __name__ == "__main__":
    main()
