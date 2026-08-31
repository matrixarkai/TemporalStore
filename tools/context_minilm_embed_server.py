# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Tiny OpenAI-compatible /v1/embeddings server wrapping the cached
sentence-transformers/all-MiniLM-L6-v2 model. Stdlib HTTP only (no flask).

POST /v1/embeddings  {"model": "...", "input": "text" | ["t1","t2",...]}
  -> {"object":"list","data":[{"object":"embedding","index":i,"embedding":[...]}],
      "model": "...","usage":{...}}

Run detached; prints the bound port to stdout as: LISTENING <port>
"""
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from sentence_transformers import SentenceTransformer

# The model is configurable so a deployment can run the encoder its retrieval was tuned for --
# e.g. a multilingual model for mixed CN/EN corpora. MATRIXARK_EMBEDDING_MODEL_PATH takes a
# local directory; MATRIXARK_EMBEDDING_MODEL takes a hub id. Both MiniLM variants emit 384
# dimensions, so switching between them does not change the stored vector width.
MODEL_NAME = (
    os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH", "").strip()
    or os.environ.get("MATRIXARK_EMBEDDING_MODEL", "").strip()
    or "intfloat/multilingual-e5-large"
)
print("loading model...", file=sys.stderr, flush=True)
_MODEL = SentenceTransformer(MODEL_NAME)
_LOCK = threading.Lock()
# The sequence window is configurable, and it must agree with the ingest side's
# MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS. A cap below the window asks the model for a short sequence it
# could have filled -- paying per-sequence overhead without the coverage or the amortisation -- while
# a cap above it is silently truncated here. The two settings are one decision.
#
# sentence-transformers ships this model with max_seq_length 128 while its position embeddings allow
# 512, so the default leaves throughput and coverage unused. Raising the window means proportionally
# fewer chunks for the same document, which is why it moves ingest time and vector memory together.
_WINDOW_ENV = "MATRIXARK_EMBEDDING_MAX_SEQ_LENGTH"


def _configure_window(model):
    raw = os.environ.get(_WINDOW_ENV, "").strip()
    if not raw:
        return
    try:
        requested = int(raw)
    except ValueError:
        print("%s=%r is not an integer; keeping %d"
              % (_WINDOW_ENV, raw, model.max_seq_length), file=sys.stderr, flush=True)
        return
    limit = getattr(model[0].auto_model.config, "max_position_embeddings", None)
    if requested < 1:
        print("%s must be positive; keeping %d" % (_WINDOW_ENV, model.max_seq_length),
              file=sys.stderr, flush=True)
        return
    if limit and requested > limit:
        print("%s=%d exceeds the model's %d position embeddings; clamping"
              % (_WINDOW_ENV, requested, limit), file=sys.stderr, flush=True)
        requested = limit
    model.max_seq_length = requested


_configure_window(_MODEL)
print("sequence window: %d tokens" % _MODEL.max_seq_length, file=sys.stderr, flush=True)
print("model loaded", file=sys.stderr, flush=True)


def embed(texts):
    with _LOCK:
        vecs = _MODEL.encode(
            texts, normalize_embeddings=False, convert_to_numpy=True
        )
    return [[float(x) for x in row] for row in vecs]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if not self.path.rstrip("/").endswith("/embeddings"):
            self._send(404, {"error": "not found"})
            return
        try:
            n = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(n) or b"{}")
        except Exception as exc:  # noqa: BLE001
            self._send(400, {"error": str(exc)})
            return
        model = payload.get("model", MODEL_NAME)
        inp = payload.get("input", [])
        if isinstance(inp, str):
            inp = [inp]
        inp = ["" if t is None else str(t) for t in inp]
        try:
            vectors = embed(inp) if inp else []
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"error": str(exc)})
            return
        data = [
            {"object": "embedding", "index": i, "embedding": v}
            for i, v in enumerate(vectors)
        ]
        self._send(
            200,
            {
                "object": "list",
                "data": data,
                "model": model,
                "usage": {"prompt_tokens": 0, "total_tokens": 0},
            },
        )

    def do_GET(self):
        # health check
        self._send(200, {"status": "ok", "model": MODEL_NAME})


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    bound = server.server_address[1]
    print("LISTENING %d" % bound, flush=True)
    with open("/tmp/minilm_embed_server.port", "w") as fh:
        fh.write(str(bound))
    server.serve_forever()


if __name__ == "__main__":
    main()
