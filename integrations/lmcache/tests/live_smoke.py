# SPDX-License-Identifier: Apache-2.0
"""Run the installed LMCache plugin against a local TemporalStore server."""

import argparse
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request

import torch
from lmcache.v1.distributed.api import ObjectKey
from lmcache.v1.distributed.l2_adapters.plugin_l2_adapter import (
    PluginL2AdapterConfig, _create_plugin_adapter,
)
from lmcache.v1.memory_management import MemoryObjMetadata, TensorMemoryObj

from lmcache_temporalstore.adapter import _names
from lmcache_temporalstore.blob import BlobClient


def memory(payload: bytes) -> TensorMemoryObj:
    raw = torch.tensor(list(payload), dtype=torch.uint8)
    meta = MemoryObjMetadata(
        shape=torch.Size([len(payload)]), dtype=torch.uint8,
        address=raw.data_ptr(), phy_size=len(payload), ref_count=1,
    )
    return TensorMemoryObj(raw, meta, parent_allocator=None)


def completed(adapter, event, get_result):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result = get_result()
        if result is not None:
            return result
        time.sleep(0.01)
    raise TimeoutError(f"LMCache task did not complete; fd={event}")


def table_smoke(meta_url: str, namespace: str, model: str) -> None:
    config = PluginL2AdapterConfig.from_dict({
        "module_path": "lmcache_temporalstore.adapter",
        "class_name": "TemporalStoreL2Adapter",
        "adapter_params": {"storage_backend": "kv", "meta_url": meta_url, "namespace": namespace},
    })
    # A fresh key lets repeated runs share a model table.
    key = ObjectKey(chunk_hash=os.urandom(32), model_name=model, kv_rank=(1 << 24) | (1 << 8))
    payload = bytes((0, 128, 255, 17))
    writer = _create_plugin_adapter(config)
    reader = _create_plugin_adapter(config)
    try:
        for value in (payload, b"diff"):
            store = writer.submit_store_task([key], [memory(value)])
            result = completed(
                writer, writer.get_store_event_fd(),
                lambda: writer.pop_completed_store_tasks().get(store),
            )
            assert result.is_successful()
        assert writer.report_status()["stores"] == 1

        def lookup():
            task = reader.submit_lookup_and_lock_task([key], {})
            return completed(
                reader, reader.get_lookup_and_lock_event_fd(),
                lambda: reader.query_lookup_and_lock_result(task),
            ).test(0)

        assert lookup()
        for size in (len(payload) - 1, len(payload)):
            target = memory(bytes(size))
            task = reader.submit_load_task([key], [target])
            found = completed(
                reader, reader.get_load_event_fd(), lambda: reader.query_load_result(task),
            ).test(0)
            assert found == (size == len(payload))
            assert bytes(target.byte_array) == (payload if found else bytes(size))
        reader.submit_unlock([key])
        assert reader.report_status()["loads"] == 1

        writer.delete([key])
        until = time.monotonic() + 10
        while lookup():
            reader.submit_unlock([key])
            if time.monotonic() >= until:
                raise AssertionError("deleted payload remains visible")
            time.sleep(0.02)
        print("PASS: two adapters shared a table and verified conditional store, lookup, load, and delete")
    finally:
        reader.close()
        writer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("server", type=Path, nargs="?")
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--meta-url")
    parser.add_argument("--namespace", default="kvcache")
    parser.add_argument("--model", default="smoke")
    args = parser.parse_args()
    if args.meta_url:
        table_smoke(args.meta_url, args.namespace, args.model)
        return
    if args.server is None or args.target_dir is None:
        parser.error("provide server and --target-dir, or --meta-url for a provisioned table")
    args.target_dir.mkdir(parents=True, exist_ok=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory(prefix="lmcache-live-", dir=args.target_dir) as work:
        env = os.environ.copy()
        env["TS_STANDALONE"] = "1"
        env["TS_SERVER_BIND_ADDR"] = f"127.0.0.1:{port}"
        env["TS_CACHE_DIR"] = str(Path(work) / "cache")
        env["TS_BLOCK_STORE_DIR"] = str(Path(work) / "blocks")
        env["TS_INDEX_DIR"] = str(Path(work) / "indexes")
        env["TS_BLOB_STORE_DIR"] = str(Path(work) / "blobs")
        with open(Path(work) / "server.log", "wb") as log:
            process = subprocess.Popen(
                [str(args.server)], cwd=work, env=env,
                stdout=log, stderr=subprocess.STDOUT,
            )
            try:
                for _ in range(200):
                    if process.poll() is not None:
                        raise RuntimeError("TemporalStore server exited during startup")
                    try:
                        with urllib.request.urlopen(f"{url}/health", timeout=0.2):
                            break
                    except Exception:
                        time.sleep(0.05)
                else:
                    raise TimeoutError("TemporalStore server did not become healthy")

                config = PluginL2AdapterConfig.from_dict({
                    "module_path": "lmcache_temporalstore.adapter",
                    "class_name": "TemporalStoreL2Adapter",
                    "adapter_params": {
                        "blob_url": url, "data_url": url,
                        "max_capacity_gb": 12 / 1024**3,
                    },
                })
                adapter = _create_plugin_adapter(config)
                try:
                    first = ObjectKey(
                        chunk_hash=bytes.fromhex("11" * 32),
                        model_name="smoke", kv_rank=0,
                    )
                    second = ObjectKey(
                        chunk_hash=bytes.fromhex("22" * 32),
                        model_name="smoke", kv_rank=0,
                    )
                    payload = b"\x00\xffabc123"
                    task = adapter.submit_store_task([first], [memory(payload)])
                    result = completed(
                        adapter, adapter.get_store_event_fd(),
                        lambda: adapter.pop_completed_store_tasks().get(task),
                    )
                    assert result.is_successful(), "store failed"
                    blob_name, _ = _names(first)
                    blob_path = Path(work) / "blobs" / blob_name
                    assert blob_path.read_bytes() == payload, "payload differs on disk"

                    task = adapter.submit_lookup_and_lock_task([first], {})
                    result = completed(
                        adapter, adapter.get_lookup_and_lock_event_fd(),
                        lambda: adapter.query_lookup_and_lock_result(task),
                    )
                    assert result.test(0), "lookup missed stored key"
                    target = memory(bytes(len(payload)))
                    task = adapter.submit_load_task([first], [target])
                    result = completed(
                        adapter, adapter.get_load_event_fd(),
                        lambda: adapter.query_load_result(task),
                    )
                    assert result.test(0), "load missed stored key"
                    assert bytes(target.byte_array) == payload, "payload changed on load"
                    adapter.submit_unlock([first])

                    short = bytearray(len(payload) - 1)
                    assert not BlobClient(url).get_into(blob_name, memoryview(short))
                    task = adapter.submit_store_task([second], [memory(payload)])
                    result = completed(
                        adapter, adapter.get_store_event_fd(),
                        lambda: adapter.pop_completed_store_tasks().get(task),
                    )
                    assert result.is_successful(), "store after eviction failed"
                    assert not blob_path.exists(), "evicted payload remains on disk"
                    status = adapter.report_status()
                    assert status["stores"] == 2 and status["loads"] == 1
                    assert adapter.get_usage().total_bytes_used == len(payload)
                    print(f"PASS: stores={status['stores']} loads={status['loads']}")
                finally:
                    adapter.close()

                reopened = _create_plugin_adapter(config)
                try:
                    task = reopened.submit_lookup_and_lock_task([second], {})
                    found = completed(
                        reopened, reopened.get_lookup_and_lock_event_fd(),
                        lambda: reopened.query_lookup_and_lock_result(task),
                    )
                    assert found.test(0), "lookup after adapter restart missed"
                    target = memory(bytes(len(payload)))
                    task = reopened.submit_load_task([second], [target])
                    loaded = completed(
                        reopened, reopened.get_load_event_fd(),
                        lambda: reopened.query_load_result(task),
                    )
                    assert loaded.test(0) and bytes(target.byte_array) == payload
                    reopened.submit_unlock([second])
                    print("PASS: adapter restart read")
                finally:
                    reopened.close()

                isolated_config = PluginL2AdapterConfig.from_dict({
                    "module_path": "lmcache_temporalstore.adapter",
                    "class_name": "TemporalStoreL2Adapter",
                    "adapter_params": {
                        "blob_url": url, "data_url": url, "namespace": "worker-b",
                        "max_capacity_gb": 12 / 1024**3,
                    },
                })
                isolated = _create_plugin_adapter(isolated_config)
                try:
                    task = isolated.submit_lookup_and_lock_task([second], {})
                    found = completed(
                        isolated, isolated.get_lookup_and_lock_event_fd(),
                        lambda: isolated.query_lookup_and_lock_result(task),
                    )
                    assert not found.test(0), "lookup crossed adapter namespaces"
                    other_payload = b"different"
                    task = isolated.submit_store_task([second], [memory(other_payload)])
                    result = completed(
                        isolated, isolated.get_store_event_fd(),
                        lambda: isolated.pop_completed_store_tasks().get(task),
                    )
                    assert result.is_successful()
                    task = isolated.submit_store_task([first], [memory(other_payload)])
                    result = completed(
                        isolated, isolated.get_store_event_fd(),
                        lambda: isolated.pop_completed_store_tasks().get(task),
                    )
                    assert result.is_successful()
                    default_blob, _ = _names(second)
                    isolated_blob, _ = _names(second, "worker-b")
                    assert (Path(work) / "blobs" / default_blob).read_bytes() == payload
                    assert not (Path(work) / "blobs" / isolated_blob).exists()
                    print("PASS: namespace isolation across adapters")
                finally:
                    isolated.close()

                ttl_config = PluginL2AdapterConfig.from_dict({
                    "module_path": "lmcache_temporalstore.adapter",
                    "class_name": "TemporalStoreL2Adapter",
                    "adapter_params": {
                        "blob_url": url, "data_url": url,
                        "max_capacity_gb": 12 / 1024**3,
                        "ttl_seconds": 1,
                    },
                })
                ttl_adapter = _create_plugin_adapter(ttl_config)
                try:
                    expiring = ObjectKey(
                        chunk_hash=bytes.fromhex("33" * 32),
                        model_name="smoke", kv_rank=0,
                    )
                    task = ttl_adapter.submit_store_task([expiring], [memory(payload)])
                    stored = completed(
                        ttl_adapter, ttl_adapter.get_store_event_fd(),
                        lambda: ttl_adapter.pop_completed_store_tasks().get(task),
                    )
                    assert stored.is_successful(), "TTL store failed"
                    time.sleep(1.3)
                    task = ttl_adapter.submit_lookup_and_lock_task([expiring], {})
                    found = completed(
                        ttl_adapter, ttl_adapter.get_lookup_and_lock_event_fd(),
                        lambda: ttl_adapter.query_lookup_and_lock_result(task),
                    )
                    assert not found.test(0), "expired metadata remained visible"
                    print("PASS: metadata TTL expired")
                finally:
                    ttl_adapter.close()
            except Exception:
                log.flush()
                print(Path(work, "server.log").read_text(errors="replace")[-4000:])
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
