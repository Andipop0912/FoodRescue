"""Tests for nifi/deploy_flow.py authentication handling.

Tests both NiFi authentication modes:
  - supportsLogin=false (HTTP/unsecured): no token obtained, no Authorization header.
  - supportsLogin=true  (HTTPS/secured):  token obtained, Authorization header set.

Plus the HTTP 409 error case when _get_token is called against an unsecured NiFi.

No live NiFi instance required — all network calls are mocked.
"""

import importlib.util
import json
import os
import sys
import urllib.error
from io import BytesIO
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Load deploy_flow module from the nifi/ directory.
# ---------------------------------------------------------------------------

_NIFI_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "nifi")
)

def _load_deploy_flow():
    spec = importlib.util.spec_from_file_location(
        "deploy_flow",
        os.path.join(_NIFI_DIR, "deploy_flow.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


df = _load_deploy_flow()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_urlopen_response(body: bytes, status: int = 200):
    """Return a context-manager mock that behaves like urllib.request.urlopen."""
    resp = MagicMock()
    resp.read.return_value = body
    resp.status = status
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def _http_error(code: int, body: bytes = b"") -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="http://nifi/", code=code, msg="error",
        hdrs=None, fp=BytesIO(body)
    )


# ---------------------------------------------------------------------------
# _check_auth_mode
# ---------------------------------------------------------------------------

class TestCheckAuthMode:
    def test_returns_false_when_supports_login_false(self):
        body = json.dumps({"config": {"supportsLogin": False}}).encode()
        with patch("urllib.request.urlopen", return_value=_fake_urlopen_response(body)):
            assert df._check_auth_mode("http://localhost:8080") is False

    def test_returns_true_when_supports_login_true(self):
        body = json.dumps({"config": {"supportsLogin": True}}).encode()
        with patch("urllib.request.urlopen", return_value=_fake_urlopen_response(body)):
            assert df._check_auth_mode("http://localhost:8080") is True

    def test_defaults_to_false_on_network_error(self):
        with patch("urllib.request.urlopen", side_effect=OSError("refused")):
            result = df._check_auth_mode("http://localhost:8080")
        assert result is False

    def test_defaults_to_false_on_unexpected_json(self):
        body = json.dumps({"other": "shape"}).encode()
        with patch("urllib.request.urlopen", return_value=_fake_urlopen_response(body)):
            assert df._check_auth_mode("http://localhost:8080") is False

    def test_probes_correct_endpoint(self):
        body = json.dumps({"config": {"supportsLogin": False}}).encode()
        captured = []
        def _fake_open(req, timeout=None):
            captured.append(req.full_url)
            return _fake_urlopen_response(body)
        with patch("urllib.request.urlopen", side_effect=_fake_open):
            df._check_auth_mode("http://nifi:8080")
        assert captured == ["http://nifi:8080/nifi-api/access/config"]


# ---------------------------------------------------------------------------
# _get_token
# ---------------------------------------------------------------------------

class TestGetToken:
    def test_returns_token_string(self):
        with patch("urllib.request.urlopen",
                   return_value=_fake_urlopen_response(b"my-token-value")):
            token = df._get_token("http://localhost:8080", "admin", "secret")
        assert token == "my-token-value"

    def test_http_409_raises_with_clear_message(self):
        with patch("urllib.request.urlopen", side_effect=_http_error(409)):
            with pytest.raises(RuntimeError) as exc_info:
                df._get_token("http://localhost:8080", "admin", "secret")
        assert "409" in str(exc_info.value)
        assert "supportsLogin" in str(exc_info.value)

    def test_http_401_raises_generic(self):
        with patch("urllib.request.urlopen", side_effect=_http_error(401)):
            with pytest.raises(RuntimeError, match="401"):
                df._get_token("http://localhost:8080", "admin", "wrong")

    def test_token_url_is_correct(self):
        captured = []
        def _fake_open(req, timeout=None):
            captured.append(req.full_url)
            return _fake_urlopen_response(b"tok")
        with patch("urllib.request.urlopen", side_effect=_fake_open):
            df._get_token("http://nifi:8080", "u", "p")
        assert captured == ["http://nifi:8080/nifi-api/access/token"]


# ---------------------------------------------------------------------------
# _api — Authorization header behavior
# ---------------------------------------------------------------------------

class TestApiAuthHeader:
    def test_no_auth_header_when_token_is_none(self):
        captured_headers = {}
        def _fake_open(req, timeout=None):
            captured_headers.update(req.headers)
            return _fake_urlopen_response(json.dumps({"id": "abc"}).encode())
        with patch("urllib.request.urlopen", side_effect=_fake_open):
            df._api("http://nifi:8080", "/nifi-api/flow/process-groups/root",
                    token=None)
        assert "Authorization" not in captured_headers

    def test_auth_header_set_when_token_provided(self):
        captured_headers = {}
        def _fake_open(req, timeout=None):
            captured_headers.update(req.headers)
            return _fake_urlopen_response(json.dumps({"id": "abc"}).encode())
        with patch("urllib.request.urlopen", side_effect=_fake_open):
            df._api("http://nifi:8080", "/nifi-api/flow/process-groups/root",
                    token="my-token")
        assert captured_headers.get("Authorization") == "Bearer my-token"


# ---------------------------------------------------------------------------
# build_flow — token propagation
# ---------------------------------------------------------------------------

class TestBuildFlowTokenPropagation:
    """Verify that build_flow passes the token (or None) to every _api call."""

    def _stub_api(self, token_values: list):
        """Return a _api replacement that records the token on each call."""
        call_tokens = []
        call_count = [0]

        responses = [
            # root process group
            {"processGroupFlow": {"id": "root-id"}},
            # list child process groups (preflight)
            {"processGroups": []},
            # create process group
            {"id": "pg-id", "revision": {"version": 0}},
            # GetFile
            {"id": "gf-id"},
            # UpdateAttribute
            {"id": "ua-id"},
            # InvokeHTTP
            {"id": "ih-id", "revision": {"version": 0}},
            # PutFile
            {"id": "pf-id"},
            # connect GetFile→UpdateAttribute
            {"id": "c1"},
            # connect UpdateAttribute→InvokeHTTP
            {"id": "c2"},
            # connect InvokeHTTP→PutFile (Failure)
            {"id": "c3"},
            # connect InvokeHTTP→PutFile (No Retry)
            {"id": "c4"},
            # connect InvokeHTTP→PutFile (Retry)
            {"id": "c5"},
            # PUT InvokeHTTP auto-terminate update
            {"id": "ih-id"},
        ]

        def _fake_api(nifi_url, path, method="GET", body=None, token=None):
            call_tokens.append(token)
            idx = call_count[0]
            call_count[0] += 1
            if idx < len(responses):
                return responses[idx]
            return {"id": f"extra-{idx}"}

        return _fake_api, call_tokens

    def test_unsecured_mode_all_calls_use_none_token(self, monkeypatch):
        fake_api, call_tokens = self._stub_api([])
        monkeypatch.setattr(df, "_api", fake_api)
        df.build_flow("http://nifi:8080", token=None,
                      backend_url="http://backend:8000", auth_token="jwt")
        assert all(t is None for t in call_tokens), (
            f"Expected all token=None, got: {call_tokens}"
        )

    def test_secured_mode_all_calls_use_token(self, monkeypatch):
        fake_api, call_tokens = self._stub_api([])
        monkeypatch.setattr(df, "_api", fake_api)
        df.build_flow("http://nifi:8080", token="bearer-tok",
                      backend_url="http://backend:8000", auth_token="jwt")
        assert all(t == "bearer-tok" for t in call_tokens), (
            f"Expected all token='bearer-tok', got: {call_tokens}"
        )


# ---------------------------------------------------------------------------
# main() integration — auth mode detection drives behavior
# ---------------------------------------------------------------------------

class TestMainAuthDetection:
    def _run_main(self, argv: list[str], monkeypatch,
                  supports_login: bool = False,
                  build_flow_ok: bool = True):
        monkeypatch.setattr(sys, "argv", ["deploy_flow.py"] + argv)
        monkeypatch.setattr(df, "_check_auth_mode", lambda url: supports_login)
        built = []
        if build_flow_ok:
            monkeypatch.setattr(df, "build_flow", lambda **kw: built.append(kw) or "pg-id")
        token_calls = []
        monkeypatch.setattr(df, "_get_token",
                            lambda url, u, p: token_calls.append((url, u, p)) or "tok")
        df.main()
        return built, token_calls

    def test_unsecured_mode_skips_get_token(self, monkeypatch):
        built, token_calls = self._run_main(
            ["--auth-token", "jwt123"],
            monkeypatch, supports_login=False,
        )
        assert token_calls == [], "get_token must not be called in unsecured mode"
        assert built
        assert built[0]["token"] is None

    def test_unsecured_mode_no_password_required(self, monkeypatch):
        """main() must succeed without --password when supportsLogin=false."""
        built, _ = self._run_main(
            ["--auth-token", "jwt123"],
            monkeypatch, supports_login=False,
        )
        assert built

    def test_secured_mode_calls_get_token(self, monkeypatch):
        built, token_calls = self._run_main(
            ["--auth-token", "jwt123", "--password", "nifi-pw"],
            monkeypatch, supports_login=True,
        )
        assert len(token_calls) == 1
        assert built[0]["token"] == "tok"

    def test_secured_mode_without_password_exits(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["deploy_flow.py", "--auth-token", "jwt"])
        monkeypatch.setattr(df, "_check_auth_mode", lambda url: True)
        monkeypatch.setattr(df, "build_flow", lambda **kw: None)
        with pytest.raises(SystemExit) as exc_info:
            df.main()
        assert exc_info.value.code == 1

    def test_auth_token_always_required(self, monkeypatch):
        """main() must exit when --auth-token is missing regardless of NiFi mode."""
        monkeypatch.setattr(sys, "argv", ["deploy_flow.py"])
        monkeypatch.setattr(df, "_check_auth_mode", lambda url: False)
        with pytest.raises(SystemExit) as exc_info:
            df.main()
        assert exc_info.value.code == 1

    def test_backend_auth_token_passed_to_build_flow(self, monkeypatch):
        """The ADMIN_JWT (--auth-token) must be forwarded to build_flow, not used for NiFi."""
        built, _ = self._run_main(
            ["--auth-token", "admin-jwt-for-backend"],
            monkeypatch, supports_login=False,
        )
        assert built[0]["auth_token"] == "admin-jwt-for-backend"
        assert built[0]["token"] is None  # NiFi token is separate
