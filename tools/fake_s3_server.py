#!/usr/bin/env python3
import argparse
import html
import os
import shutil
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def safe_path(root, bucket, key):
    bucket = bucket.strip("/")
    key = key.lstrip("/")
    path = os.path.abspath(os.path.join(root, bucket, key))
    root_abs = os.path.abspath(root)
    if not path.startswith(root_abs + os.sep):
        raise ValueError("path escaped fake S3 root")
    return path


class FakeS3Handler(BaseHTTPRequestHandler):
    server_version = "TemporalStoreFakeS3/1.0"

    def log_message(self, fmt, *args):
        if self.server.verbose:
            super().log_message(fmt, *args)

    def parse_path(self):
        parsed = urllib.parse.urlparse(self.path)
        parts = parsed.path.lstrip("/").split("/", 1)
        if not parts or not parts[0]:
            return None, None, parsed
        bucket = urllib.parse.unquote(parts[0])
        key = urllib.parse.unquote(parts[1] if len(parts) > 1 else "")
        return bucket, key, parsed

    def send_status(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def do_PUT(self):
        bucket, key, _ = self.parse_path()
        if not bucket or not key:
            self.send_status(400, b"bucket and key required")
            return
        dst = safe_path(self.server.root, bucket, key)
        copy_source = self.headers.get("x-amz-copy-source")
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if copy_source:
            src_parts = urllib.parse.unquote(copy_source).lstrip("/").split("/", 1)
            if len(src_parts) != 2:
                self.send_status(400, b"invalid copy source")
                return
            src = safe_path(self.server.root, src_parts[0], src_parts[1])
            if not os.path.exists(src):
                self.send_status(404, b"not found")
                return
            shutil.copyfile(src, dst)
            self.send_status(200, b"<CopyObjectResult/>")
            return
        if self.headers.get("If-None-Match") == "*" and os.path.exists(dst):
            self.send_status(412, b"precondition failed")
            return
        length = int(self.headers.get("Content-Length", "0"))
        data = self.rfile.read(length)
        with open(dst, "wb") as fh:
            fh.write(data)
        self.send_status(200)

    def do_GET(self):
        bucket, key, parsed = self.parse_path()
        query = urllib.parse.parse_qs(parsed.query)
        if query.get("list-type") == ["2"]:
            max_keys = int(query.get("max-keys", ["1000"])[0])
            continuation_token = query.get("continuation-token", [None])[0]
            self.list_objects(
                bucket,
                query.get("prefix", [""])[0],
                max(max_keys, 1),
                continuation_token,
            )
            return
        self.read_object(bucket, key, head_only=False)

    def do_HEAD(self):
        bucket, key, _ = self.parse_path()
        self.read_object(bucket, key, head_only=True)

    def do_POST(self):
        bucket, _, parsed = self.parse_path()
        if parsed.query == "delete":
            self.delete_objects(bucket)
            return
        self.send_status(405, b"method not allowed")

    def do_DELETE(self):
        bucket, key, _ = self.parse_path()
        if not bucket or not key:
            self.send_status(400, b"bucket and key required")
            return
        path = safe_path(self.server.root, bucket, key)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        self.send_status(204)

    def read_object(self, bucket, key, head_only):
        if not bucket or not key:
            self.send_status(400, b"bucket and key required")
            return
        path = safe_path(self.server.root, bucket, key)
        if not os.path.exists(path):
            self.send_status(404, b"not found")
            return
        size = os.path.getsize(path)
        begin, end = 0, size - 1
        range_header = self.headers.get("Range")
        if range_header and range_header.startswith("bytes="):
            span = range_header[len("bytes="):]
            first, _, last = span.partition("-")
            begin = int(first) if first else 0
            end = int(last) if last else size - 1
        if begin < 0 or end < begin or begin >= size:
            self.send_status(416, b"invalid range")
            return
        end = min(end, size - 1)
        length = end - begin + 1
        self.send_response(206 if range_header else 200)
        self.send_header("Content-Length", str(length))
        if range_header:
            self.send_header("Content-Range", f"bytes {begin}-{end}/{size}")
        self.end_headers()
        if not head_only:
            with open(path, "rb") as fh:
                fh.seek(begin)
                self.wfile.write(fh.read(length))

    def list_objects(self, bucket, prefix, max_keys, continuation_token):
        if not bucket:
            self.send_status(400, b"bucket required")
            return
        bucket_dir = os.path.abspath(os.path.join(self.server.root, bucket))
        keys = []
        if os.path.isdir(bucket_dir):
            for dirpath, _, filenames in os.walk(bucket_dir):
                for filename in filenames:
                    path = os.path.join(dirpath, filename)
                    rel = os.path.relpath(path, bucket_dir).replace(os.sep, "/")
                    if rel.startswith(prefix):
                        keys.append(rel)
        keys = sorted(keys)
        start = 0
        if continuation_token:
            for idx, key in enumerate(keys):
                if key > continuation_token:
                    start = idx
                    break
            else:
                start = len(keys)
        page = keys[start:start + max_keys]
        truncated = start + len(page) < len(keys)
        body = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            "<ListBucketResult>",
            f"<IsTruncated>{str(truncated).lower()}</IsTruncated>",
        ]
        for key in page:
            body.append(f"<Contents><Key>{html.escape(key)}</Key></Contents>")
        if truncated and page:
            body.append(
                f"<NextContinuationToken>{html.escape(page[-1])}</NextContinuationToken>"
            )
        body.append("</ListBucketResult>")
        payload = "".join(body).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def delete_objects(self, bucket):
        if not bucket:
            self.send_status(400, b"bucket required")
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        keys = []
        remainder = body
        while "<Key>" in remainder:
            _, _, after_open = remainder.partition("<Key>")
            value, _, remainder = after_open.partition("</Key>")
            if value:
                keys.append(html.unescape(value))
        for key in keys:
            try:
                os.remove(safe_path(self.server.root, bucket, key))
            except FileNotFoundError:
                pass
        response = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            "<DeleteResult>",
        ]
        for key in keys:
            response.append(f"<Deleted><Key>{html.escape(key)}</Key></Deleted>")
        response.append("</DeleteResult>")
        payload = "".join(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class FakeS3Server(ThreadingHTTPServer):
    def __init__(self, address, handler, root, verbose):
        super().__init__(address, handler)
        self.root = root
        self.verbose = verbose


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    os.makedirs(args.root, exist_ok=True)
    server = FakeS3Server((args.host, args.port), FakeS3Handler, args.root, args.verbose)
    print(f"fake-s3 listening on {args.host}:{args.port}, root={args.root}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
