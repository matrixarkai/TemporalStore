#!/usr/bin/env python3
"""Real Google ID-token (OIDC) verification for MatrixArk, using only the stdlib.

MatrixArk normally trusts a product gateway to verify OAuth and pass down claims.
This module lets MatrixArk verify a Google-issued ID token itself: it checks the
RS256 signature against Google's published JWKS and validates the standard OIDC
claims (issuer, audience, expiry, verified email, optional hosted domain). There
are no third-party dependencies, so it runs in the same minimal environment as
the rest of the MatrixArk MCP server. The raw token is never persisted.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.request

try:  # names owned by the core module
    from tools.matrixark_mcp_core import MatrixArkError
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import MatrixArkError


GOOGLE_CERTS_URL = "https://www.googleapis.com/oauth2/v3/certs"
GOOGLE_ISSUERS = {"https://accounts.google.com", "accounts.google.com"}
# ASN.1 DigestInfo prefix for SHA-256 (RFC 8017, section 9.2, notes on EMSA-PKCS1-v1_5).
_SHA256_DIGEST_PREFIX = bytes.fromhex("3031300d060960864801650304020105000420")


def _b64url_decode(segment: str) -> bytes:
    padding = "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(segment + padding)


def _b64url_uint(value: str) -> int:
    return int.from_bytes(_b64url_decode(value), "big")


def decode_jwt_segments(token: str):
    """Return (header, payload, signature_bytes, signing_input_bytes) without verifying."""
    parts = token.split(".")
    if len(parts) != 3:
        raise MatrixArkError("malformed JWT: expected header.payload.signature")
    try:
        header = json.loads(_b64url_decode(parts[0]))
        payload = json.loads(_b64url_decode(parts[1]))
        signature = _b64url_decode(parts[2])
    except (ValueError, json.JSONDecodeError) as exc:
        raise MatrixArkError("malformed JWT: could not base64/JSON decode segments") from exc
    signing_input = (parts[0] + "." + parts[1]).encode("ascii")
    return header, payload, signature, signing_input


def emsa_pkcs1_v15_encode(message: bytes, k: int) -> bytes:
    """EMSA-PKCS1-v1_5 encoding of SHA-256(message) for an RSA modulus of k bytes."""
    digest_info = _SHA256_DIGEST_PREFIX + hashlib.sha256(message).digest()
    if k < len(digest_info) + 11:
        raise MatrixArkError("RSA modulus too small for PKCS#1 v1.5 SHA-256")
    padding = b"\xff" * (k - len(digest_info) - 3)
    return b"\x00\x01" + padding + b"\x00" + digest_info


def rsa_pkcs1_v15_sha256_verify(message: bytes, signature: bytes, n: int, e: int) -> bool:
    """Verify an RSASSA-PKCS1-v1_5 SHA-256 signature with pure-Python big-int math."""
    k = (n.bit_length() + 7) // 8
    if len(signature) > k:
        return False
    signature = signature.rjust(k, b"\x00")
    s = int.from_bytes(signature, "big")
    if s >= n:
        return False
    recovered = pow(s, e, n).to_bytes(k, "big")
    expected = emsa_pkcs1_v15_encode(message, k)
    return hmac.compare_digest(recovered, expected)


def _jwk_rsa_pubkey(jwk) -> tuple[int, int]:
    return _b64url_uint(jwk["n"]), _b64url_uint(jwk["e"])


def fetch_google_certs(url: str = GOOGLE_CERTS_URL, *, timeout: float = 5.0, opener=None):
    opener = opener or urllib.request.urlopen
    with opener(url, timeout=timeout) as response:  # noqa: S310 (fixed Google URL)
        return json.loads(response.read().decode("utf-8"))


def verify_google_claims(payload, *, audience, allowed_hosted_domains=None, now=None, leeway_s: int = 60):
    """Validate the OIDC claims of an already signature-verified Google token."""
    now_s = int(now if now is not None else time.time())
    issuer = str(payload.get("iss", ""))
    if issuer not in GOOGLE_ISSUERS:
        raise MatrixArkError(f"unexpected token issuer {issuer!r}")
    allowed_aud = {audience} if isinstance(audience, str) else set(audience or [])
    allowed_aud.discard("")
    if not allowed_aud:
        raise MatrixArkError("google client id (audience) is required to verify the token")
    if payload.get("aud", "") not in allowed_aud:
        raise MatrixArkError("token audience does not match the configured Google client id")
    exp = int(payload.get("exp", 0) or 0)
    if exp and now_s > exp + leeway_s:
        raise MatrixArkError("google id token is expired")
    iat = int(payload.get("iat", 0) or 0)
    if iat and iat - leeway_s > now_s:
        raise MatrixArkError("google id token used before its issued-at time")
    if allowed_hosted_domains:
        if str(payload.get("hd", "")) not in set(allowed_hosted_domains):
            raise MatrixArkError("google account is not in an allowed hosted domain")
    email = str(payload.get("email", ""))
    email_verified = payload.get("email_verified", False)
    if isinstance(email_verified, str):
        email_verified = email_verified.strip().lower() == "true"
    if email and not email_verified:
        raise MatrixArkError("google email address is not verified")
    subject = str(payload.get("sub", ""))
    if not subject:
        raise MatrixArkError("google token is missing the subject (sub) claim")
    return {
        "sub": subject,
        "email": email,
        "email_verified": bool(email_verified),
        "hd": str(payload.get("hd", "")),
        "name": str(payload.get("name", "")),
        "aud": payload.get("aud", ""),
        "iss": issuer,
    }


def verify_google_id_token(
    id_token: str,
    *,
    audience,
    allowed_hosted_domains=None,
    now=None,
    certs=None,
    cert_fetcher=None,
    leeway_s: int = 60,
):
    """Verify a Google ID token end to end and return its trusted claims.

    ``certs`` may be supplied directly (a JWKS dict, as returned by Google's
    /oauth2/v3/certs); otherwise ``cert_fetcher`` or the default network fetch is
    used. Raises MatrixArkError on any signature or claim failure.
    """
    header, payload, signature, signing_input = decode_jwt_segments(id_token)
    if header.get("alg") != "RS256":
        raise MatrixArkError(f"unsupported token alg {header.get('alg')!r}; expected RS256")
    if certs is None:
        certs = (cert_fetcher or fetch_google_certs)()
    keys = certs.get("keys", []) if isinstance(certs, dict) else list(certs or [])
    if not keys:
        raise MatrixArkError("no Google signing keys available to verify the token")
    kid = header.get("kid")
    candidates = [key for key in keys if key.get("kid") == kid] or keys
    verified = False
    for jwk in candidates:
        try:
            n, e = _jwk_rsa_pubkey(jwk)
        except (KeyError, ValueError):
            continue
        if rsa_pkcs1_v15_sha256_verify(signing_input, signature, n, e):
            verified = True
            break
    if not verified:
        raise MatrixArkError("google id token signature verification failed")
    return verify_google_claims(
        payload,
        audience=audience,
        allowed_hosted_domains=allowed_hosted_domains,
        now=now,
        leeway_s=leeway_s,
    )
