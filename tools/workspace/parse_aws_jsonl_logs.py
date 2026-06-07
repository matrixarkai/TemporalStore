#!/usr/bin/env python3
from pathlib import Path
import json
import sys

pattern = sys.argv[1] if len(sys.argv) > 1 else "aws_bytekv_logs_post_reset_*.jsonl"
p = max(Path("outputs").glob(pattern), key=lambda x: x.stat().st_mtime)
print("FILE", p)
raw = p.read_text(errors="replace")
pos = 0
objs = []
while True:
    start = raw.find("{", pos)
    if start < 0:
        break
    depth = 0
    in_str = False
    esc = False
    complete = False
    for i, ch in enumerate(raw[start:], start):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        objs.append(json.loads(raw[start : i + 1]))
                    except Exception as exc:
                        print("json fail", exc)
                    pos = i + 1
                    complete = True
                    break
    if not complete:
        break

patterns = [
    "Register",
    "register",
    "store",
    "Store",
    "CreatePartition",
    "create partition",
    "leader",
    "normal",
    "fault",
    "failed",
    "ERROR",
    "WARN",
    "report",
    "readonly",
]
for idx, obj in enumerate(objs):
    print(f"\n===== OBJ {idx} STATUS {obj.get('Status')} =====")
    lines = obj.get("Stdout", "").splitlines()
    print("\n".join(lines[:10]))
    for pat in patterns:
        hits = [ln for ln in lines if pat in ln]
        if hits:
            print(f"\n-- {pat} --")
            for ln in hits[-20:]:
                print(ln[:1000])
