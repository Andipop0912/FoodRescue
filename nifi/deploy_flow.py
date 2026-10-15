#!/usr/bin/env python3
"""Bootstrap the FoodRescue NiFi ingestion flow via the NiFi REST API.

This script creates the following topology in NiFi:

  [GetFile /data/inbox]
        │  FlowFile (raw CSV content)
        ▼
  [UpdateAttribute]  — adds X-Ingest-Source, X-Ingest-Filename
        │
        ▼
  [InvokeHTTP]  POST http://backend:8000/ingest/batch  (Content-Type: text/csv)
        │                                  │
        ▼ (Response — 2xx body)            │
   auto-terminate                          │
        │                                  ├── Failure (connection/timeout)
        ▼ (Original — auto-terminate)      ├── No Retry (4xx)
                                           └── Retry (5xx)
                                                    │
                                                    ▼
                                         [PutFile /data/nifi-failed]

Request contract
----------------
POST /ingest/batch
Content-Type: text/csv
Authorization: Bearer <admin JWT>
X-Ingest-Source: nifi-inbox       (set by UpdateAttribute → sent as HTTP header)
X-Ingest-Filename: <filename>     (set by UpdateAttribute → sent as HTTP header)

Body: raw CSV text (UTF-8, RFC 4180, header row required)

Authentication modes
--------------------
The script probes GET /nifi-api/access/config before attempting to log in.

  supportsLogin=false  NiFi is in HTTP/unsecured mode.  --password is not
                       required and must not be passed to /nifi-api/access/token
                       (that endpoint returns HTTP 409 in this mode).  All NiFi
                       API requests are made without an Authorization header.

  supportsLogin=true   NiFi is secured (HTTPS + identity provider).  --password
                       is required.  A Bearer token is obtained from
                       /nifi-api/access/token and used for all API requests.

Usage
-----
  # Unsecured / HTTP mode (supportsLogin=false) — no --password needed:
  python nifi/deploy_flow.py --nifi-url http://localhost:8080 \\
      --backend-url http://backend:8000 \\
      --auth-token <ADMIN_JWT>

  # Secured / HTTPS mode (supportsLogin=true):
  python nifi/deploy_flow.py --nifi-url https://localhost:8443 \\
      --username admin --password <NIFI_PASSWORD> \\
      --backend-url http://backend:8000 \\
      --auth-token <ADMIN_JWT>

  # Inside the docker network (after docker compose --profile pipeline up -d):
  docker exec -it foodrescue-nifi-1 \\
      python3 /nifi-config/deploy_flow.py \\
      --nifi-url http://localhost:8080 \\
      --backend-url http://backend:8000 \\
      --auth-token <ADMIN_JWT>

Secrets
-------
Pass --auth-token via environment variable or a secrets manager.
Never commit real tokens to version control.

  export FOODRESCUE_AUTH_TOKEN=$(cat /run/secrets/nifi_auth_token)
  python nifi/deploy_flow.py ... --auth-token "$FOODRESCUE_AUTH_TOKEN"

Preflight guard
---------------
If a process group named "FoodRescue Inbox Ingestion" already exists in the
root process group, this script exits with a clear message rather than creating
a duplicate.

Live integration status
-----------------------
Live integration (broker actually running) was NOT verified in this session.
Run this script after `docker compose --profile pipeline up -d` to deploy the
flow and perform live end-to-end verification.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error


# ---------------------------------------------------------------------------
# NiFi REST API helpers (no external dependencies)
# ---------------------------------------------------------------------------

def _api(nifi_url: str, path: str, method: str = "GET",
         body: dict | None = None, token: str | None = None) -> dict:
    url = nifi_url.rstrip("/") + path
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:500]
        raise RuntimeError(f"NiFi API {method} {path} → HTTP {exc.code}: {detail}") from exc


def _check_auth_mode(nifi_url: str) -> bool:
    """Return True if NiFi supports username/password login, False if unsecured.

    Calls GET /nifi-api/access/config and reads config.supportsLogin.

    When supportsLogin is False, NiFi is running in HTTP/unsecured mode.
    Calling POST /nifi-api/access/token in this mode returns HTTP 409 because
    the token endpoint is disabled.  All API calls must be made without any
    Authorization header.

    When supportsLogin is True (HTTPS + identity provider configured), use
    _get_token() to obtain a Bearer token and pass it to every _api() call.
    """
    try:
        url = nifi_url.rstrip("/") + "/nifi-api/access/config"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            config = json.loads(resp.read())
        return bool(config.get("config", {}).get("supportsLogin", False))
    except Exception as exc:
        print(f"WARNING: could not read NiFi auth config ({exc}); assuming unsecured mode.")
        return False


def _get_token(nifi_url: str, username: str, password: str) -> str:
    """Obtain a Bearer token via username/password.  Only call when supportsLogin=True."""
    url = nifi_url.rstrip("/") + "/nifi-api/access/token"
    data = f"username={username}&password={password}".encode()
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode().strip()
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            raise RuntimeError(
                "NiFi login failed: HTTP 409 — the token endpoint is disabled.\n"
                "This usually means NiFi is running in HTTP/unsecured mode "
                "(supportsLogin=false).  Do not pass --password in this mode; "
                "the script will detect it automatically."
            ) from exc
        raise RuntimeError(f"NiFi login failed: HTTP {exc.code}") from exc


def wait_for_nifi(nifi_url: str, retries: int = 20, delay: float = 10.0) -> None:
    print(f"Waiting for NiFi at {nifi_url} …", end="", flush=True)
    for _ in range(retries):
        try:
            urllib.request.urlopen(nifi_url + "/nifi/", timeout=5)
            print(" ready.")
            return
        except Exception:
            print(".", end="", flush=True)
            time.sleep(delay)
    raise RuntimeError("NiFi did not become ready in time.")


# ---------------------------------------------------------------------------
# Preflight guard
# ---------------------------------------------------------------------------

FLOW_GROUP_NAME = "FoodRescue Inbox Ingestion"


def _preflight_check(nifi_url: str, token: str | None, root_id: str) -> None:
    """Abort if a process group with the expected name already exists.

    This prevents duplicate flows being created on repeated script runs.
    If a group exists, print instructions for manual inspection and exit.
    """
    try:
        resp = _api(nifi_url, f"/nifi-api/process-groups/{root_id}/process-groups", token=token)
    except RuntimeError as exc:
        print(f"WARNING: could not list existing process groups: {exc}")
        print("Proceeding anyway — check the NiFi UI for duplicates after deployment.")
        return

    existing = [
        pg["component"]["name"]
        for pg in resp.get("processGroups", [])
        if pg.get("component", {}).get("name") == FLOW_GROUP_NAME
    ]
    if existing:
        print(
            f"\nERROR: A process group named '{FLOW_GROUP_NAME}' already exists in NiFi.\n"
            "\nThis script will NOT create a duplicate. To redeploy:\n"
            "  1. Open the NiFi UI and stop all processors in that group.\n"
            "  2. Delete the process group manually.\n"
            "  3. Re-run this script.\n"
            "\nTo inspect the existing group via the API:\n"
            f"  GET {nifi_url}/nifi-api/flow/process-groups/{root_id}\n"
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Flow builder
# ---------------------------------------------------------------------------

def _require_id(obj: dict, label: str) -> str:
    """Extract the 'id' field from a NiFi API response, raising on failure."""
    obj_id = obj.get("id")
    if not obj_id:
        raise RuntimeError(f"NiFi did not return an id for {label}; response: {obj}")
    return obj_id


def build_flow(nifi_url: str, token: str | None, backend_url: str, auth_token: str,
               source: str = "nifi-inbox",
               inbox_dir: str = "/data/inbox",
               failed_dir: str = "/data/nifi-failed") -> str:
    """Create the FoodRescue ingestion flow in NiFi.  Returns the process-group ID."""

    # 1. Get root process group ID.
    root_pg = _api(nifi_url, "/nifi-api/flow/process-groups/root", token=token)
    root_id = root_pg["processGroupFlow"]["id"]
    print(f"Root process group: {root_id}")

    # 2. Preflight: abort if the flow already exists.
    _preflight_check(nifi_url, token, root_id)

    # 3. Create a named process group for isolation.
    pg = _api(nifi_url, f"/nifi-api/process-groups/{root_id}/process-groups",
              method="POST", token=token,
              body={"revision": {"version": 0},
                    "component": {"name": FLOW_GROUP_NAME,
                                  "position": {"x": 0.0, "y": 0.0}}})
    pg_id = _require_id(pg, "process group")
    print(f"Created process group: {pg_id}")

    # 4. GetFile processor — reads CSVs from /data/inbox.
    #    Keep Source File=false so successfully retrieved files are consumed
    #    (deleted from /data/inbox) rather than reprocessed on every poll.
    #    The inbox mount must be rw (not ro) for this to work.
    get_file = _api(nifi_url, f"/nifi-api/process-groups/{pg_id}/processors",
                    method="POST", token=token,
                    body={"revision": {"version": 0},
                          "component": {
                              "type": "org.apache.nifi.processors.standard.GetFile",
                              "name": "GetFile inbox",
                              "position": {"x": 0.0, "y": 0.0},
                              "config": {
                                  "properties": {
                                      "Input Directory": inbox_dir,
                                      "File Filter": "[^\\.].*\\.csv",
                                      "Keep Source File": "false",
                                      "Recurse Subdirectories": "false",
                                  },
                                  "schedulingStrategy": "TIMER_DRIVEN",
                                  "schedulingPeriod": "10 sec",
                              }}})
    get_file_id = _require_id(get_file, "GetFile processor")
    print(f"  GetFile processor: {get_file_id}")

    # 5. UpdateAttribute — attach X-Ingest-Source and X-Ingest-Filename.
    #    These FlowFile attributes are sent as HTTP request headers by InvokeHTTP
    #    when "Attributes to Send" regex matches their names.
    update_attr = _api(nifi_url, f"/nifi-api/process-groups/{pg_id}/processors",
                       method="POST", token=token,
                       body={"revision": {"version": 0},
                             "component": {
                                 "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
                                 "name": "Set batch metadata",
                                 "position": {"x": 0.0, "y": 200.0},
                                 "config": {
                                     "properties": {
                                         "X-Ingest-Source": source,
                                         "X-Ingest-Filename": "${filename}",
                                     }}}})
    update_attr_id = _require_id(update_attr, "UpdateAttribute processor")
    print(f"  UpdateAttribute processor: {update_attr_id}")

    # 6. InvokeHTTP — POST raw CSV to /ingest/batch.
    #    - Content-Type: text/csv  (backend _read_rows handles raw CSV bodies)
    #    - Authorization is set as a dynamic property (sent as an HTTP header in NiFi 1.x)
    #    - Attributes to Send: regex matches X-Ingest-Source and X-Ingest-Filename
    #      FlowFile attributes → they are forwarded as HTTP request headers
    #
    #    Relationship routing (NiFi 1.28.1):
    #      Response   — 2xx response body FlowFile         → auto-terminate
    #      Original   — original FlowFile on success       → auto-terminate
    #      Failure    — connection/timeout errors           → PutFile (failure dir)
    #      No Retry   — 4xx responses (client error)        → PutFile (failure dir)
    #      Retry      — 5xx responses (server error)        → PutFile (failure dir)
    #
    #    NOTE: Retry must NOT be auto-terminated — that would silently discard
    #    server-side failures.  We route it to PutFile for manual inspection.
    invoke_http = _api(nifi_url, f"/nifi-api/process-groups/{pg_id}/processors",
                       method="POST", token=token,
                       body={"revision": {"version": 0},
                             "component": {
                                 "type": "org.apache.nifi.processors.standard.InvokeHTTP",
                                 "name": "POST to /ingest/batch",
                                 "position": {"x": 0.0, "y": 400.0},
                                 "config": {
                                     "properties": {
                                         "Remote URL": f"{backend_url}/ingest/batch",
                                         "HTTP Method": "POST",
                                         # "Content-Type" is the NiFi 1.x property name
                                         # (not "Request Content-Type" which is NiFi 2.x):
                                         "Content-Type": "text/csv",
                                         # Dynamic property — sent as Authorization HTTP header:
                                         "Authorization": f"Bearer {auth_token}",
                                         # Regex: send X-Ingest-Source and X-Ingest-Filename
                                         # FlowFile attributes as HTTP request headers:
                                         "Attributes to Send": "X-Ingest-.*",
                                         # NiFi 1.x internal property names (kebab-case).
                                         # Title-case variants would be treated as dynamic
                                         # FlowFile attributes and sent as HTTP headers,
                                         # which fails because HTTP headers cannot have spaces.
                                         "send-message-body": "true",
                                         "ignore-response-content": "false",
                                         "Connection Timeout": "30 sec",
                                         "Read Timeout": "60 sec",
                                     },
                                     # No relationships auto-terminated here yet —
                                     # set after connections are wired.
                                     "autoTerminatedRelationships": [],
                                 }}})
    invoke_http_id = _require_id(invoke_http, "InvokeHTTP processor")
    print(f"  InvokeHTTP processor: {invoke_http_id}")

    # 7. PutFile — failed files go to /data/nifi-failed.
    #    Receives: Failure (connection error), No Retry (4xx), Retry (5xx).
    #    Its own success/failure relationships are auto-terminated since
    #    the archived copy in nifi-failed is the final destination.
    put_file = _api(nifi_url, f"/nifi-api/process-groups/{pg_id}/processors",
                    method="POST", token=token,
                    body={"revision": {"version": 0},
                          "component": {
                              "type": "org.apache.nifi.processors.standard.PutFile",
                              "name": "Save failed files",
                              "position": {"x": 400.0, "y": 400.0},
                              "config": {
                                  "properties": {
                                      "Directory": failed_dir,
                                      "Conflict Resolution Strategy": "replace",
                                  },
                                  "autoTerminatedRelationships": ["success", "failure"],
                              }}})
    put_file_id = _require_id(put_file, "PutFile processor")
    print(f"  PutFile (failure) processor: {put_file_id}")

    # 8. Connect processors.
    def connect(src_id: str, src_rel: str, dst_id: str) -> str:
        conn = _api(nifi_url, f"/nifi-api/process-groups/{pg_id}/connections",
                    method="POST", token=token,
                    body={"revision": {"version": 0},
                          "component": {
                              "source": {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
                              "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
                              "selectedRelationships": [src_rel],
                          }})
        conn_id = _require_id(conn, f"connection {src_id}→{dst_id} [{src_rel}]")
        return conn_id

    connect(get_file_id, "success", update_attr_id)
    connect(update_attr_id, "success", invoke_http_id)
    # All three failure relationships route to PutFile:
    connect(invoke_http_id, "Failure", put_file_id)    # connection/timeout
    connect(invoke_http_id, "No Retry", put_file_id)   # 4xx
    connect(invoke_http_id, "Retry", put_file_id)      # 5xx — must not be auto-terminated
    print("  Connections created.")

    # 9. Auto-terminate only the success-path relationships on InvokeHTTP.
    #    Response = 2xx HTTP response body FlowFile (logged, not needed downstream)
    #    Original = original FlowFile copy produced on success
    #    Do NOT auto-terminate Failure, No Retry, or Retry — those go to PutFile.
    _api(nifi_url, f"/nifi-api/processors/{invoke_http_id}",
         method="PUT", token=token,
         body={"revision": invoke_http["revision"],
               "component": {
                   "id": invoke_http_id,
                   "config": {"autoTerminatedRelationships": ["Response", "Original"]}}})
    print("  Auto-terminate set for Response and Original.")

    print(f"\nFlow created in process group: {pg_id}")
    print("\nTo start the flow, run:")
    print(f"  PUT {nifi_url}/nifi-api/flow/process-groups/{pg_id}")
    print(f'  Body: {{"id":"{pg_id}","state":"RUNNING"}}')
    print("\nOr start it from the NiFi UI.")
    return pg_id


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap FoodRescue NiFi flow")
    parser.add_argument("--nifi-url", default="http://localhost:8080")
    parser.add_argument("--username", default=os.environ.get("NIFI_USER", "admin"))
    parser.add_argument("--password", default=os.environ.get("NIFI_PASSWORD", ""))
    parser.add_argument("--backend-url", default=os.environ.get("BACKEND_URL", "http://backend:8000"))
    parser.add_argument("--auth-token", default=os.environ.get("FOODRESCUE_AUTH_TOKEN", ""))
    parser.add_argument("--source", default="nifi-inbox")
    parser.add_argument("--inbox-dir", default="/data/inbox")
    parser.add_argument("--failed-dir", default="/data/nifi-failed")
    parser.add_argument("--wait", action="store_true", help="Wait for NiFi to start before deploying")
    args = parser.parse_args()

    if not args.auth_token:
        print("ERROR: --auth-token or FOODRESCUE_AUTH_TOKEN env var required", file=sys.stderr)
        sys.exit(1)

    if args.wait:
        wait_for_nifi(args.nifi_url)

    # Detect NiFi authentication mode before attempting login.
    supports_login = _check_auth_mode(args.nifi_url)

    if supports_login:
        # Secured mode (HTTPS + identity provider): username/password required.
        if not args.password:
            print(
                "ERROR: NiFi requires authentication (supportsLogin=true) but "
                "--password / NIFI_PASSWORD was not provided.",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"Authenticating with NiFi at {args.nifi_url} (secured mode) …")
        # Password is not echoed here; auth_token is never printed.
        nifi_token: str | None = _get_token(args.nifi_url, args.username, args.password)
    else:
        # Unsecured mode (HTTP, supportsLogin=false): no NiFi auth needed.
        # Calling _get_token() in this mode returns HTTP 409.
        print(f"NiFi at {args.nifi_url} is in unsecured mode (supportsLogin=false); skipping login.")
        nifi_token = None

    build_flow(
        nifi_url=args.nifi_url,
        token=nifi_token,
        backend_url=args.backend_url,
        auth_token=args.auth_token,
        source=args.source,
        inbox_dir=args.inbox_dir,
        failed_dir=args.failed_dir,
    )


if __name__ == "__main__":
    main()
