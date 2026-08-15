#!/usr/bin/env python3
"""Re-run the 2-node/2-shard add+drop rebalance validation against the
auto-rebalance build. Proves: auto-reassign on drop, data intact on move,
metaserver-driven placement on add."""
import json, os, shutil, signal, socket, subprocess, sys, time, urllib.request

ROOT = os.environ.get("TS_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BIN = f"{ROOT}/target/debug"
BASE = "/tmp/ts-rebal2"
SHARED = f"{BASE}/shared"
CLUSTER = "rebal-test"
META = "127.0.0.1:18001"
DNA = "127.0.0.1:18101"
DNB = "127.0.0.1:18102"
DNC = "127.0.0.1:18103"

procs = {}

def http(method, host, path, body=None, timeout=5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://{host}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw else {})
    except Exception as e:
        return None, {"error": str(e)}

def wait_port(host, timeout=20):
    ip, port = host.split(":")
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((ip, int(port)), 1):
                return True
        except OSError:
            time.sleep(0.2)
    return False

def start(name, binary, addr, env_extra):
    logf = open(f"{BASE}/{name}.log", "w")
    env = dict(os.environ)
    env["PATH"] = os.path.expanduser("~/.cargo/bin") + ":" + env.get("PATH", "")
    env.update(env_extra)
    p = subprocess.Popen([f"{BIN}/{binary}"], stdout=logf, stderr=subprocess.STDOUT, env=env)
    procs[name] = (p, logf)
    return p

def stop(name, hard=False):
    if name not in procs:
        return
    p, logf = procs.pop(name)
    try:
        p.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
        p.wait(timeout=5)
    except Exception:
        pass
    logf.close()

def cleanup_all():
    for n in list(procs):
        stop(n, hard=True)

def dn_env(shard_id, page, index, cache, extra=None):
    e = {
        "TS_META_ADDR": META, "TS_DISTRIBUTED": "1",
        "TS_SHARD_ID": str(shard_id),
        "TS_PAGE_STORE_DIR": page, "TS_INDEX_DIR": index, "TS_CACHE_DIR": cache,
        "TS_STORAGE_BACKEND": "shared", "TS_SHARED_STORE_DIR": SHARED,
        "TS_SHARED_STORE_CLUSTER_ID": CLUSTER,
        "TS_AUTO_REBALANCE_DATA_MOVE": "1",
        "TS_SERVER_HEARTBEAT_INTERVAL_MS": "1000",
    }
    if extra:
        e.update(extra)
    return e

def sset(host, shard, key, val):
    return http("POST", host, "/execute", {"shard_id": shard,
        "command": {"kind": "string_set", "key": key, "value": list(val.encode())}})

def sget(host, shard, key):
    _, r = http("POST", host, "/execute", {"shard_id": shard,
        "command": {"kind": "string_get", "key": key}})
    resp = r.get("response", {})
    if resp.get("kind") == "bytes" and resp.get("value") is not None:
        return bytes(resp["value"]).decode()
    return None

def shard_owner(shard):
    _, r = http("GET", META, f"/shards/{shard}")
    loc = r.get("location") or {}
    return loc.get("server_addr")

def server_states():
    _, r = http("GET", META, "/servers")
    return {s["server_addr"]: s["state"] for s in r.get("servers", [])}

results = {}

def main():
    shutil.rmtree(BASE, ignore_errors=True)
    os.makedirs(SHARED, exist_ok=True)

    print("== start metaserver (auto-rebalance ON) ==")
    start("meta", "matrixark_rust_metaserver", META, {
        "TS_META_BIND_ADDR": META,
        "TS_META_STALE_AFTER_MS": "4000",
        "TS_META_FAILURE_DETECTOR_INTERVAL_MS": "1000",
        "TS_META_AUTO_REBALANCE": "1",
        "TS_META_AUTO_REBALANCE_INTERVAL_MS": "1000",
    })
    assert wait_port(META), "metaserver did not start"

    print("== start dn-a (shard 1), dn-b (shard 2) ==")
    start("dna", "matrixark_rust_datanode", DNA,
          dn_env(1, f"{BASE}/dna/pages", f"{BASE}/dna/idx", f"{BASE}/dna/cache",
                 {"TS_SERVER_BIND_ADDR": DNA, "TS_SERVER_ADVERTISE_ADDR": DNA}))
    start("dnb", "matrixark_rust_datanode", DNB,
          dn_env(2, f"{BASE}/dnb/pages", f"{BASE}/dnb/idx", f"{BASE}/dnb/cache",
                 {"TS_SERVER_BIND_ADDR": DNB, "TS_SERVER_ADVERTISE_ADDR": DNB}))
    assert wait_port(DNA) and wait_port(DNB), "datanodes did not start"
    time.sleep(2)

    print("== write 3 keys per shard ==")
    for i in range(1, 4):
        sset(DNA, 1, f"s1key{i}", f"shard1-val-{i}")
        sset(DNB, 2, f"s2key{i}", f"shard2-val-{i}")
    base_a = sget(DNA, 1, "s1key2")
    base_b = sget(DNB, 2, "s2key2")
    print(f"   baseline read dn-a s1key2={base_a!r}  dn-b s2key2={base_b!r}")
    results["baseline_write_read"] = (base_a == "shard1-val-2" and base_b == "shard2-val-2")

    print("== publish shard checkpoints to shared storage ==")
    _, pa = http("POST", DNA, "/shard/publish_checkpoint", {"shard_id": 1})
    _, pb = http("POST", DNB, "/shard/publish_checkpoint", {"shard_id": 2})
    print(f"   publish shard1 -> {pa}")
    print(f"   publish shard2 -> {pb}")

    print("== baseline topology ==")
    print(f"   /shards/1 -> {shard_owner(1)}   /shards/2 -> {shard_owner(2)}")
    print(f"   servers: {server_states()}")

    print("== DROP dn-b (kill) and wait for failure-detect + auto-rebalance ==")
    stop("dnb", hard=True)
    time.sleep(9)
    owner2 = shard_owner(2)
    states = server_states()
    print(f"   /shards/2 -> {owner2}   servers: {states}")
    results["drop_auto_reassign_off_dead"] = (owner2 == DNA)
    results["dead_node_frozen"] = (states.get(DNB) == "frozen")

    moved_b = [sget(DNA, 2, f"s2key{i}") for i in range(1, 4)]
    still_a = [sget(DNA, 1, f"s1key{i}") for i in range(1, 4)]
    print(f"   shard2 read on new owner dn-a: {moved_b}")
    print(f"   shard1 read on dn-a (still owned): {still_a}")
    results["data_intact_on_drop_move"] = (moved_b == [f"shard2-val-{i}" for i in range(1, 4)])
    results["still_owned_shard_intact"] = (still_a == [f"shard1-val-{i}" for i in range(1, 4)])

    print("== ADD dn-c (join-empty) -> metaserver should place a shard onto it ==")
    start("dnc", "matrixark_rust_datanode", DNC,
          dn_env(9, f"{BASE}/dnc/pages", f"{BASE}/dnc/idx", f"{BASE}/dnc/cache",
                 {"TS_SERVER_BIND_ADDR": DNC, "TS_SERVER_ADVERTISE_ADDR": DNC,
                  "TS_SERVER_JOIN_EMPTY": "1"}))
    assert wait_port(DNC), "dn-c did not start"
    time.sleep(9)
    o1, o2 = shard_owner(1), shard_owner(2)
    print(f"   after add: /shards/1 -> {o1}   /shards/2 -> {o2}   servers: {server_states()}")
    placed = {o1, o2}
    results["placement_on_add"] = (DNC in placed)

    # read the shard that landed on dn-c to prove data moved on the balance step
    moved_shard = 1 if o1 == DNC else (2 if o2 == DNC else None)
    if moved_shard is not None:
        vals = [sget(DNC, moved_shard, f"s{moved_shard}key{i}") for i in range(1, 4)]
        print(f"   shard{moved_shard} read on dn-c: {vals}")
        results["data_intact_on_add_move"] = (vals == [f"shard{moved_shard}-val-{i}" for i in range(1, 4)])
    else:
        results["data_intact_on_add_move"] = False

    print("\n==== PER-CLAIM RESULTS ====")
    for k, v in results.items():
        print(f"   {'PASS' if v else 'FAIL'}  {k}")
    ok = all(results.values())
    print(f"\n==== {'ALL PASS' if ok else 'SOME FAILED'} ====")
    return 0 if ok else 1

if __name__ == "__main__":
    code = 1
    try:
        code = main()
    except Exception as e:
        print(f"TEST ERROR: {e}")
        import traceback; traceback.print_exc()
    finally:
        cleanup_all()
    # confirm resident stack untouched
    try:
        with socket.create_connection(("127.0.0.1", 17102), 1):
            print("resident datanode 17102 still listening (untouched)")
    except OSError:
        print("WARNING: resident 17102 not reachable")
    sys.exit(code)
