"""A self-hosted encoder takes no key, so a missing key must not silence it.

Measured twice on a live one-box: every vector in the store was a 32-dimension token hash while a
healthy 1024-dimension e5-large answered on the same network. The gateway returned 200 throughout,
retrieval looked plausible, and the only outward sign was that the encoder burned no CPU. The cause
was `if not api_key: return deterministic` -- correct for api.openai.com, wrong for an endpoint the
operator named themselves.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matrixark_mcp_embeddings as embeddings  # noqa: E402

FAILURES = []


def check(condition, message):
    if not condition:
        FAILURES.append(message)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        import json
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def run_with_stub(dims=1024):
    """Capture the request the module would send, and answer it with a wide vector."""
    seen = {}

    import urllib.request as real_urlreq

    def fake_urlopen(request, timeout=None):
        # Record what the module WOULD have sent. Patching the attribute on the real module, not
        # sys.modules: the function does  at call time, which
        # rebinds from the package either way, so a sys.modules swap is simply not seen -- the
        # first version of this test swapped sys.modules, the real urlopen ran, the connection was
        # refused, and the generic except returned the hash fallback. The test then read that as
        # "the endpoint was never called" when the endpoint HAD been called.
        seen["headers"] = dict(getattr(request, "headers", {}) or {})
        seen["url"] = getattr(request, "full_url", "")
        return FakeResponse({"data": [{"index": 0, "embedding": [0.01] * dims}]})

    original = real_urlreq.urlopen
    real_urlreq.urlopen = fake_urlopen
    try:
        vector = embeddings.api_embedding_for_texts(["hello"], "openai")[0]
    finally:
        real_urlreq.urlopen = original
    return vector, seen


def clear_env():
    for name in ("OPENAI_API_KEY", "MATRIXARK_EMBEDDING_API_KEY", "MATRIXARK_REQUIRE_API_EMBEDDINGS"):
        os.environ.pop(name, None)


def a_named_endpoint_is_called_even_with_no_key():
    clear_env()
    os.environ["MATRIXARK_EMBEDDING_API_BASE"] = "http://127.0.0.1:8081/v1"
    try:
        vector, seen = run_with_stub(dims=1024)
        check("url" in seen, "the encoder was never called: a named endpoint must be reached")
        check(len(vector) == 1024,
              "expected the encoder's 1024 dims, got %d (the hash fallback is 32)" % len(vector))
        auth = [k for k in seen.get("headers", {}) if k.lower() == "authorization"]
        check(not auth, "no key was configured, so no Authorization header may be sent: %r" % auth)
    finally:
        os.environ.pop("MATRIXARK_EMBEDDING_API_BASE", None)


def the_hosted_default_still_needs_a_key():
    """Positive control. Without this, "always call" would fire blind requests at api.openai.com,
    which only 401 -- the missing-key fallback is correct THERE and must stay."""
    clear_env()
    os.environ.pop("MATRIXARK_EMBEDDING_API_BASE", None)
    vector = embeddings.api_embedding_for_texts(["hello"], "openai")[0]
    check(len(vector) == embeddings.EMBEDDING_DIM,
          "with no key and no named endpoint the deterministic encoder must still answer, got %d"
          % len(vector))


def a_configured_key_is_still_sent():
    clear_env()
    os.environ["MATRIXARK_EMBEDDING_API_BASE"] = "http://127.0.0.1:8081/v1"
    os.environ["OPENAI_API_KEY"] = "sk-test-value"
    try:
        _, seen = run_with_stub()
        auth = {k.lower(): v for k, v in seen.get("headers", {}).items()}.get("authorization", "")
        check("sk-test-value" in str(auth), "a configured key must still be sent, got %r" % (auth,))
    finally:
        os.environ.pop("OPENAI_API_KEY", None)
        os.environ.pop("MATRIXARK_EMBEDDING_API_BASE", None)


def the_require_flag_still_fails_fast_on_the_hosted_default():
    clear_env()
    os.environ.pop("MATRIXARK_EMBEDDING_API_BASE", None)
    os.environ["MATRIXARK_REQUIRE_API_EMBEDDINGS"] = "1"
    try:
        embeddings.api_embedding_for_texts(["hello"], "openai")
        check(False, "with the require flag and no key, it must raise rather than fall back")
    except Exception as exc:  # noqa: BLE001
        check("require" in str(exc).lower() or "api embeddings" in str(exc).lower(),
              "it must say a key is required, got %r" % (exc,))
    finally:
        os.environ.pop("MATRIXARK_REQUIRE_API_EMBEDDINGS", None)


for test in (
    a_named_endpoint_is_called_even_with_no_key,
    the_hosted_default_still_needs_a_key,
    a_configured_key_is_still_sent,
    the_require_flag_still_fails_fast_on_the_hosted_default,
):
    test()
    print("  ran %s" % test.__name__)

if FAILURES:
    print("FAILED:")
    for item in FAILURES:
        print("  - %s" % item)
    raise SystemExit(1)
print("all checks pass")
