"""Tests for scripts/refresh_nifi_token.py."""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

# Make the scripts/ directory importable without installing it.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))

import refresh_nifi_token as rnt  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_proc(proc_id="proc-1", state="RUNNING", revision_version=3):
    return {
        "id": proc_id,
        "revision": {"version": revision_version},
        "component": {
            "type": "org.apache.nifi.processors.standard.InvokeHTTP",
            "name": "InvokeHTTP Test",
            "state": state,
            "config": {
                "properties": {"HTTP Method": "POST", "Authorization": "Bearer old.token"}
            },
        },
    }


def _pg_response(processors):
    return {"processors": processors}


# ---------------------------------------------------------------------------
# TestGetFreshToken
# ---------------------------------------------------------------------------

class TestGetFreshToken(unittest.TestCase):
    def _post(self, url, body):
        return {"access_token": "fresh.jwt.abc"}

    def test_returns_token_on_success(self):
        with patch.object(rnt, "_post_json", self._post):
            tok = rnt.get_fresh_token("http://localhost:8000", "a@b.c", "pw")
        self.assertEqual(tok, "fresh.jwt.abc")

    def test_raises_when_no_access_token(self):
        with patch.object(rnt, "_post_json", return_value={"detail": "bad"}):
            with self.assertRaises(RuntimeError) as ctx:
                rnt.get_fresh_token("http://localhost:8000", "a@b.c", "pw")
        self.assertIn("access_token", str(ctx.exception))

    def test_correct_url_used(self):
        captured = {}

        def _fake_post(url, body):
            captured["url"] = url
            return {"access_token": "t"}

        with patch.object(rnt, "_post_json", _fake_post):
            rnt.get_fresh_token("http://localhost:8000", "a@b.c", "pw")
        self.assertTrue(captured["url"].endswith("/auth/login"), captured["url"])

    def test_trailing_slash_stripped_from_base(self):
        captured = {}

        def _fake_post(url, body):
            captured["url"] = url
            return {"access_token": "t"}

        with patch.object(rnt, "_post_json", _fake_post):
            rnt.get_fresh_token("http://localhost:8000/", "a@b.c", "pw")
        self.assertFalse(captured["url"].startswith("http://localhost:8000//"))


# ---------------------------------------------------------------------------
# TestFindInvokeHttpProcessors
# ---------------------------------------------------------------------------

class TestFindInvokeHttpProcessors(unittest.TestCase):
    def test_returns_matching_processors(self):
        procs = [_make_proc("p1"), _make_proc("p2")]
        with patch.object(rnt, "_nifi", return_value=_pg_response(procs)):
            result = rnt.find_invoke_http_processors("http://nifi", "pg-1")
        self.assertEqual(len(result), 2)

    def test_filters_non_invoke_http(self):
        procs = [_make_proc("p1")]
        other = {"id": "p2", "revision": {"version": 1},
                 "component": {"type": "org.apache.nifi.processors.standard.GetFile",
                               "state": "RUNNING", "config": {"properties": {}}}}
        with patch.object(rnt, "_nifi", return_value=_pg_response([*procs, other])):
            result = rnt.find_invoke_http_processors("http://nifi", "pg-1")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "p1")

    def test_returns_empty_when_no_processors(self):
        with patch.object(rnt, "_nifi", return_value={"processors": []}):
            result = rnt.find_invoke_http_processors("http://nifi", "pg-1")
        self.assertEqual(result, [])

    def test_correct_nifi_path_queried(self):
        captured = {}

        def _fake_nifi(url, path, **kw):
            captured["path"] = path
            return {"processors": []}

        with patch.object(rnt, "_nifi", _fake_nifi):
            rnt.find_invoke_http_processors("http://nifi", "pg-abc")
        self.assertIn("pg-abc", captured["path"])


# ---------------------------------------------------------------------------
# TestUpdateProcessorToken
# ---------------------------------------------------------------------------

class TestUpdateProcessorToken(unittest.TestCase):
    def _run(self, proc, new_token="new.jwt"):
        """Run update_processor_token with mocked _nifi calls."""
        calls = []
        fresh_proc = {
            "revision": {"version": 5},
            "component": {
                "config": {
                    "properties": {"HTTP Method": "POST", "Authorization": "Bearer old"}
                }
            },
        }

        def fake_nifi(nifi_url, path, method="GET", body=None):
            calls.append((method, path, body))
            if method == "GET":
                return fresh_proc
            # stop / update / start all return a revision
            return {"revision": {"version": 6}}

        with patch.object(rnt, "_nifi", fake_nifi):
            rnt.update_processor_token("http://nifi", proc, new_token)

        return calls

    def test_running_proc_is_stopped_before_update(self):
        proc = _make_proc(state="RUNNING")
        calls = self._run(proc)
        methods = [c[0] for c in calls]
        stop_idx = next(i for i, (m, p, _) in enumerate(calls)
                        if m == "PUT" and "run-status" in p)
        update_idx = next(i for i, (m, p, _) in enumerate(calls)
                          if m == "PUT" and "run-status" not in p)
        self.assertLess(stop_idx, update_idx)

    def test_stopped_proc_not_stopped_again(self):
        proc = _make_proc(state="STOPPED")
        calls = self._run(proc)
        run_status_puts = [(m, p) for m, p, _ in calls
                           if m == "PUT" and "run-status" in p]
        # Stopped processor: no stop PUT, no start PUT either
        self.assertEqual(run_status_puts, [])

    def test_new_token_in_update_body(self):
        proc = _make_proc()
        calls = self._run(proc, new_token="brand.new.tok")
        update_call = next((m, p, b) for m, p, b in calls
                           if m == "PUT" and "run-status" not in p)
        props = update_call[2]["component"]["config"]["properties"]
        self.assertEqual(props["Authorization"], "Bearer brand.new.tok")

    def test_other_properties_preserved(self):
        proc = _make_proc()
        calls = self._run(proc)
        update_call = next((m, p, b) for m, p, b in calls
                           if m == "PUT" and "run-status" not in p)
        props = update_call[2]["component"]["config"]["properties"]
        self.assertIn("HTTP Method", props)

    def test_running_proc_restarted_after_update(self):
        proc = _make_proc(state="RUNNING")
        calls = self._run(proc)
        update_idx = next(i for i, (m, p, _) in enumerate(calls)
                          if m == "PUT" and "run-status" not in p)
        restart_calls = [(m, p) for m, p, _ in calls[update_idx + 1:]
                         if m == "PUT" and "run-status" in p]
        self.assertTrue(restart_calls, "Processor was not restarted after update")


# ---------------------------------------------------------------------------
# TestMain
# ---------------------------------------------------------------------------

class TestMain(unittest.TestCase):
    def _run_main(self, args, pg_processors, login_token="tok"):
        """Call main() with mocked network, return printed output."""
        output_lines = []
        procs = [_make_proc(f"proc-{i}") for i in range(pg_processors)]
        fresh_proc = {
            "revision": {"version": 2},
            "component": {"config": {"properties": {"Authorization": "Bearer old"}}},
        }

        def fake_post_json(url, body):
            return {"access_token": login_token}

        def fake_nifi(nifi_url, path, method="GET", body=None):
            if method == "GET" and "processors" in path and "run-status" not in path:
                if path.endswith("/processors"):
                    return {"processors": procs}
                return fresh_proc
            return {"revision": {"version": 3}}

        import io
        buf = io.StringIO()

        with patch.object(rnt, "_post_json", fake_post_json), \
             patch.object(rnt, "_nifi", fake_nifi), \
             patch("sys.stdout", buf):
            import argparse
            sys.argv = [
                "refresh_nifi_token.py",
                "--process-group-id", "pg-test",
                "--nifi-url", "http://nifi",
                "--backend-url", "http://backend",
                "--admin-email", "admin@test.com",
                "--admin-password", "pw",
            ]
            # Clear any cached FOODRESCUE_AUTH_TOKEN
            import os
            os.environ.pop("FOODRESCUE_AUTH_TOKEN", None)
            rnt.main()
        return buf.getvalue()

    def test_main_exits_without_pg_id(self):
        import os
        os.environ.pop("NIFI_PG_ID", None)
        sys.argv = ["refresh_nifi_token.py"]
        with self.assertRaises(SystemExit) as ctx:
            rnt.main()
        self.assertEqual(ctx.exception.code, 1)

    def test_main_exits_when_no_processors_found(self):
        import io, os
        os.environ.pop("FOODRESCUE_AUTH_TOKEN", None)
        buf = io.StringIO()
        with patch.object(rnt, "_post_json", return_value={"access_token": "t"}), \
             patch.object(rnt, "_nifi", return_value={"processors": []}), \
             patch("sys.stderr", buf):
            sys.argv = [
                "refresh_nifi_token.py",
                "--process-group-id", "pg-1",
                "--nifi-url", "http://nifi",
                "--backend-url", "http://b",
                "--admin-email", "a@b.c",
                "--admin-password", "pw",
            ]
            with self.assertRaises(SystemExit) as ctx:
                rnt.main()
        self.assertEqual(ctx.exception.code, 1)

    def test_main_uses_auth_token_arg_without_login(self):
        """When --auth-token is supplied, no login call should occur."""
        login_called = []
        procs = [_make_proc()]
        fresh = {"revision": {"version": 2},
                 "component": {"config": {"properties": {"Authorization": "Bearer x"}}}}

        def fake_post(url, body):
            login_called.append(url)
            return {"access_token": "should-not-reach"}

        def fake_nifi(nifi_url, path, method="GET", body=None):
            if path.endswith("/processors"):
                return {"processors": procs}
            return fresh

        import os
        os.environ.pop("FOODRESCUE_AUTH_TOKEN", None)
        sys.argv = [
            "refresh_nifi_token.py",
            "--process-group-id", "pg-1",
            "--auth-token", "pre-built.jwt",
        ]
        with patch.object(rnt, "_post_json", fake_post), \
             patch.object(rnt, "_nifi", fake_nifi), \
             patch("sys.stdout", __import__("io").StringIO()):
            rnt.main()
        self.assertEqual(login_called, [], "Login was called even though --auth-token was supplied")

    def test_main_success_output_mentions_refresh_complete(self):
        out = self._run_main([], pg_processors=1)
        self.assertIn("refresh complete", out.lower())


if __name__ == "__main__":
    unittest.main()
