#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Known command results for fresh-node direct/gateway recovery checks."""
import base64
import json
import pathlib
import re
import sys


def byte_value(text):
    return {"kind": "bytes", "value": None if text is None else list(text.encode())}


def integer(value):
    return {"kind": "integer", "value": value}


def members(*values):
    return {"kind": "members", "members": [list(value.encode()) for value in values]}


def values(*items):
    return {"kind": "values", "values": [None if item is None else base64.b64encode(item.encode()).decode() for item in items]}


def read_cases(second_batch):
    def case(family, kind, expected, **fields):
        return family, dict(kind=kind, **fields), expected
    return [
        case("string", "string_get", byte_value("v1"), key="s1"),
        case("string", "string_get", byte_value("v2b" if second_batch else "v2"), key="s2"),
        case("string", "string_get", byte_value(None if second_batch else "v3"), key="s3"),
        case("string", "string_len", integer(2), key="s1"),
        case("hash", "hash_get", byte_value(None if second_batch else "a"), key="h1", field="f1"),
        case("hash", "hash_get", byte_value("b"), key="h1", field="f2"),
        case("hash", "hash_get", byte_value("d" if second_batch else None), key="h1", field="f3"),
        case("hash", "hash_multi_get", values(None if second_batch else "a", "b", "d" if second_batch else None), key="h1", fields=["f1", "f2", "f3"]),
        case("hash", "hash_get", byte_value("c"), key="h2", field="f1"),
        case("set", "set_members", members("w", "x", "z") if second_batch else members("x", "y", "z"), key="st1"),
        case("set", "set_card", integer(3), key="st1"),
        case("set", "set_is_member", integer(0 if second_batch else 1), key="st1", member=[121]),
        case("set", "set_members", members("p"), key="st2"),
        case("zset", "z_set_range", members("m1", "1.5", "m2", "9") if second_batch else members("m3", "0.5", "m1", "1.5", "m2", "2.5"), key="z1", start=0, stop=-1, rev=False),
        case("zset", "z_set_score", byte_value("9" if second_batch else "2.5"), key="z1", member=[109, 50]),
        case("zset", "z_set_card", integer(2 if second_batch else 3), key="z1"),
        case("list", "list_range", members("e2", "e3", "e4") if second_batch else members("e1", "e2", "e3"), key="l1", start=0, stop=-1),
        case("list", "list_len", integer(3), key="l1"),
    ]


def records(path):
    return [tuple(line.split("\t", 1)) for line in path.read_text().splitlines()]


def successful_response(raw):
    value = json.loads(raw)
    if value.get("status", {}).get("ok") is not True or not isinstance(value.get("response"), dict):
        raise ValueError("command did not return a successful ExecuteResponse: " + raw[:300])
    return value["response"]


def assert_response(command, raw, expected):
    actual = successful_response(raw)
    if command["kind"] == "set_members" and actual.get("kind") == "members":
        actual = dict(actual, members=sorted(actual["members"]))
        expected = dict(expected, members=sorted(expected["members"]))
    if actual != expected:
        raise ValueError("expected " + json.dumps(expected) + ", received " + json.dumps(actual))


def check_harness(log, exit_code):
    fields = dict(re.findall(r"^([a-z_]+)\s+(\S+)", log, re.M))
    if str(exit_code).strip() != "0":
        raise ValueError("harness exited unsuccessfully")
    requested = int(fields.get("ops", "0"))
    completed = int(fields.get("ops_ok", "-1"))
    if requested <= 0 or completed != requested:
        raise ValueError("harness did not complete every requested operation")
    if fields.get("arrival_verified") != "true":
        raise ValueError("harness did not verify stored writes")


def verify(work):
    errors = {family: [] for family in ("string", "hash", "set", "zset", "list")}
    queries = records(work / "r.txt")
    cases = read_cases(False)
    if len(queries) != len(cases):
        raise ValueError("read command count differs from the expected fixture")
    for (family, raw), (expected_family, command, _) in zip(queries, cases):
        if family != expected_family or json.loads(raw) != command:
            raise ValueError("read command differs from the expected fixture: " + raw)
    for name in ("w1.resp", "w2.resp"):
        writes = records(work / name)
        commands = records(work / name.replace(".resp", ".txt"))
        if len(writes) != len(commands) or not writes:
            raise ValueError("write result count differs: " + name)
        for (family, raw), (expected_family, _) in zip(writes, commands):
            if family != expected_family or family not in errors:
                raise ValueError("write family differs: " + name)
            try:
                successful_response(raw)
            except (ValueError, TypeError) as error:
                errors[family].append(name + ": " + str(error))
    for name, second in (("reads1.txt", False), ("reads2.txt", False), ("reads2b.txt", True), ("reads3.txt", True)):
        actual = records(work / name)
        expected = read_cases(second)
        if len(actual) != len(expected):
            raise ValueError("read result count differs: " + name)
        for (family, raw), (expected_family, command, result) in zip(actual, expected):
            if family != expected_family:
                raise ValueError("read family differs: " + name)
            try:
                assert_response(command, raw, result)
            except (ValueError, TypeError, KeyError) as error:
                errors[family].append(name + ": " + str(error))
    for family, failures in errors.items():
        print("FAMILY", family, "FAIL: " + "; ".join(failures) if failures else "PASS (expected values on three fresh nodes)")
    failed = any(errors.values())
    for family in ("feature-append", "sequence"):
        code = (work / (family + ".status")).read_text().strip()
        log_name = "featappend.log" if family == "feature-append" else "seqadd.log"
        try:
            check_harness((work / "direct-first" / log_name).read_text(), code)
            print("FAMILY", family, "WRITES_ONLY (all operations and same-node arrival verified)")
        except ValueError as error:
            print("FAMILY", family, "FAIL:", error)
            failed = True
    replayed = []
    for label in ("gateway-after-direct", "direct-after-both"):
        log = re.sub(r"\x1b\[[0-9;]*m", "", (work / label / "server.log").read_text(errors="replace"))
        match = re.search(r"replayed shard WAL tail from networked matrixobject storage[^\n]*records=(\d+)", log)
        replayed.append(int(match[1]) if match else None)
    progressed = all(value is not None for value in replayed) and 0 < replayed[0] < replayed[1]
    print("WAL replay", "PASS" if progressed else "NOT COVERED", "records=" + str(replayed))
    print("FAMILY object surface NOT COVERED: run direct_surface_cluster")
    return int(failed or not progressed)


def main():
    if sys.argv[1:] == ["--commands"]:
        for family, command, _ in read_cases(False):
            print(family + "\t" + json.dumps(command, separators=(",", ":")))
        return 0
    try:
        return verify(pathlib.Path(sys.argv[1]))
    except (ValueError, KeyError, IndexError, OSError, TypeError) as error:
        print("FAIL recovery assertions:", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
