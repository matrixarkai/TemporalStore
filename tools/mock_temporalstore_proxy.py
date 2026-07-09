#!/usr/bin/env python3
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


strings = {}
sequences = {}


def proxy_execute(response=None):
    return {"status": {"ok": True, "code": "ok", "message": ""}, "response": response or {}}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        data = self.handle_request(self.path, body)
        payload = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        return

    def handle_request(self, path, body):
        key = body.get("key", "")
        if path == "/ProxyService/Set":
            value = body.get("value", "")
            if isinstance(value, list):
                value = bytes(value).decode("utf-8")
            strings[key] = value
            return proxy_execute({})
        if path == "/ProxyService/Get":
            return proxy_execute(
                {"kind": "value", "value": list(strings.get(key, "").encode("utf-8"))}
            )
        if path == "/ProxyService/SequenceAdd":
            sequences.setdefault(key, []).extend(body.get("rows", []))
            return proxy_execute({})
        if path == "/ProxyService/SequenceQuery":
            start_ts = int(body.get("start_ms", 0))
            end_ts = int(body.get("end_ms", 2**63 - 1))
            count = int(body.get("count", 100))
            rows = [
                row
                for row in sequences.get(key, [])
                if start_ts <= int(row.get("timestamp", 0)) <= end_ts
            ]
            for item in body.get("filters", []):
                field = item.get("field", "")
                op = int(item.get("op", 0))
                value = int(item.get("value", 0))
                if op == 0:
                    rows = [row for row in rows if int(row.get(field, 0)) == value]
                elif op == 1:
                    rows = [row for row in rows if int(row.get(field, 0)) != value]
                elif op == 2:
                    rows = [row for row in rows if int(row.get(field, 0)) > value]
                elif op == 3:
                    rows = [row for row in rows if int(row.get(field, 0)) < value]
            return proxy_execute({"kind": "sequence_rows", "rows": rows[:count]})
        return proxy_execute({})


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 8080), Handler).serve_forever()
