#!/usr/bin/env python3
"""Offline tests for in-process Google ID-token verification.

A throwaway 2048-bit RSA key (generated once, safe to publish) is used to sign
Google-style ID tokens in-process, so signature + claim verification and the
sso_callback integration are exercised without any network access.
"""
from __future__ import annotations

import base64
import json
import tempfile
import unittest
from pathlib import Path

from matrixark_google_oauth import emsa_pkcs1_v15_encode, verify_google_claims, verify_google_id_token
from matrixark_mcp_core import MatrixArkError
from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer

# Throwaway key — for tests only, never used to sign anything real.
_N = 25360159745134149945360093676021925609612776820736190272483230169379058720768801535837308988640571451167523815829664668652315740408654715202781339962031964943495143008683300119739200652971297826990514495271760518039137140017165739061900079807339003770421324314120487150606456136509308702423584377416190740498723570954496888150387397284225492317243949944690495225223860262178095972458183034711248521303233149360792210503356904302585415285680714386157924452913230999143207692521531157482784052454885223253987296853528574292456961512222216960409639021855026491421845242923862241979476088775969506817125331170368245204167
_E = 65537
_D = 678167887622430327038610517810793618940314171627345247159582440218868549577060467326788895632878685152009726731263426798183513254541049454495543781411230268658669302456732147554725510270392264632737376738706512946938048751919196115211169871474058091817269546476074488586626439417871374038629911152793378040701904008197639652040768712741139604240322421792260019416685242227317460573606895354012416410499569212532765985515214724410582779422475017990200508282906192882097592355741304456091413206002750595049179103522577267634807366058149725958869313471894305884437699123172148574408225082909795870955159923643601816033


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _int_to_b64url(value: int) -> str:
    return _b64url(value.to_bytes((value.bit_length() + 7) // 8, "big"))


_CERTS = {"keys": [{"kid": "test-kid", "kty": "RSA", "alg": "RS256", "use": "sig", "n": _int_to_b64url(_N), "e": _int_to_b64url(_E)}]}
_CLIENT_ID = "1234567890-abc.apps.googleusercontent.com"


def make_google_token(payload, *, kid="test-kid", alg="RS256", tamper=False) -> str:
    header = {"alg": alg, "kid": kid, "typ": "JWT"}
    encoded_header = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    encoded_payload = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
    k = (_N.bit_length() + 7) // 8
    signature = pow(int.from_bytes(emsa_pkcs1_v15_encode(signing_input, k), "big"), _D, _N).to_bytes(k, "big")
    if tamper:
        signature = bytes([signature[0] ^ 0x01]) + signature[1:]
    return f"{encoded_header}.{encoded_payload}.{_b64url(signature)}"


def base_payload(**overrides):
    payload = {
        "iss": "https://accounts.google.com",
        "aud": _CLIENT_ID,
        "sub": "google-sub-abc-123",
        "email": "alice@gmail.com",
        "email_verified": True,
        "name": "Alice",
        "iat": 1_000_000_000,
        "exp": 40_000_000_000,  # year ~3237, so real-clock verification stays valid
    }
    payload.update(overrides)
    return payload


class MatrixArkGoogleOAuthTest(unittest.TestCase):
    def test_valid_token_verifies_signature_and_claims(self) -> None:
        token = make_google_token(base_payload())
        claims = verify_google_id_token(token, audience=_CLIENT_ID, certs=_CERTS, now=1_000_000_100)
        self.assertEqual("google-sub-abc-123", claims["sub"])
        self.assertEqual("alice@gmail.com", claims["email"])
        self.assertTrue(claims["email_verified"])

    def test_wrong_audience_is_rejected(self) -> None:
        token = make_google_token(base_payload(aud="someone-else.apps.googleusercontent.com"))
        with self.assertRaises(MatrixArkError):
            verify_google_id_token(token, audience=_CLIENT_ID, certs=_CERTS, now=1_000_000_100)

    def test_expired_token_is_rejected(self) -> None:
        token = make_google_token(base_payload(exp=1_000_000_050))
        with self.assertRaises(MatrixArkError):
            verify_google_id_token(token, audience=_CLIENT_ID, certs=_CERTS, now=1_000_100_000)

    def test_unverified_email_is_rejected(self) -> None:
        token = make_google_token(base_payload(email_verified=False))
        with self.assertRaises(MatrixArkError):
            verify_google_id_token(token, audience=_CLIENT_ID, certs=_CERTS, now=1_000_000_100)

    def test_wrong_issuer_is_rejected(self) -> None:
        token = make_google_token(base_payload(iss="https://evil.example.com"))
        with self.assertRaises(MatrixArkError):
            verify_google_id_token(token, audience=_CLIENT_ID, certs=_CERTS, now=1_000_000_100)

    def test_tampered_signature_is_rejected(self) -> None:
        token = make_google_token(base_payload(), tamper=True)
        with self.assertRaises(MatrixArkError):
            verify_google_id_token(token, audience=_CLIENT_ID, certs=_CERTS, now=1_000_000_100)

    def test_non_rs256_alg_is_rejected(self) -> None:
        token = make_google_token(base_payload(), alg="HS256")
        with self.assertRaises(MatrixArkError):
            verify_google_id_token(token, audience=_CLIENT_ID, certs=_CERTS, now=1_000_000_100)

    def test_hosted_domain_enforced_when_required(self) -> None:
        token = make_google_token(base_payload(hd="acme.com"))
        claims = verify_google_id_token(token, audience=_CLIENT_ID, certs=_CERTS, now=1_000_000_100, allowed_hosted_domains=["acme.com"])
        self.assertEqual("acme.com", claims["hd"])
        wrong = make_google_token(base_payload(hd="other.com"))
        with self.assertRaises(MatrixArkError):
            verify_google_id_token(wrong, audience=_CLIENT_ID, certs=_CERTS, now=1_000_000_100, allowed_hosted_domains=["acme.com"])

    def test_claims_only_helper_rejects_bad_audience(self) -> None:
        with self.assertRaises(MatrixArkError):
            verify_google_claims(base_payload(aud="nope"), audience=_CLIENT_ID, now=1_000_000_100)

    def test_sso_callback_verifies_google_token_in_process_without_storing_it(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        server = MatrixArkMcpServer(MatrixArkLocalAdapter(Path(tmp.name) / "events.jsonl"), line_json=True, access_mode="dev")
        server.access._google_cert_fetcher = lambda: _CERTS  # inject JWKS, no network

        token = make_google_token(base_payload(email="grace@gmail.com", sub="google-grace-999"))
        result = server.call_tool(
            "matrixark_auth_sso_callback",
            {
                "provider": "google",
                "id_token": token,
                "google_client_id": _CLIENT_ID,
                "account_id": "acct_g",
                "tenant_id": "tenant_g",
                "scope": {"account_id": "acct_g", "tenant_id": "tenant_g"},
            },
        )
        self.assertEqual("sso_callback_mapped", result["status"])
        self.assertTrue(result["google_verified_in_process"])
        self.assertEqual("grace@gmail.com", result.get("email"))
        self.assertFalse(result["stored_oauth_tokens"])

        # The raw ID token must never be persisted.
        record_dump = str(server.adapter.read_all())
        self.assertNotIn(token, record_dump)
        self.assertNotIn(token.split(".")[2], record_dump)

        # A token for the wrong audience is rejected end to end.
        bad = make_google_token(base_payload(aud="attacker.apps.googleusercontent.com"))
        with self.assertRaises(MatrixArkError):
            server.call_tool(
                "matrixark_auth_sso_callback",
                {"provider": "google", "id_token": bad, "google_client_id": _CLIENT_ID, "account_id": "acct_g", "tenant_id": "tenant_g", "scope": {"account_id": "acct_g", "tenant_id": "tenant_g"}},
            )


if __name__ == "__main__":
    unittest.main()
