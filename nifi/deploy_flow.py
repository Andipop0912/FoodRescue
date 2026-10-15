#!/usr/bin/env python3
"""Bootstrap the FoodRescue NiFi ingestion flow via the NiFi REST API.

This script creates (or verifies) the following topology in NiFi:

  [GetFile /data/inbox]
        │  FlowFile (CSV file content + attributes)
        ▼
  [UpdateAttribute]  — adds batch metadata (source, filename, content-type)
        │
        ▼
  [InvokeHTTP]  POST http://backend:8000/ingest/batch
        │                                  │
        ▼ (Response.Status=2xx)            ▼ (Response.Status=4xx/5xx or timeout)
  [LogAttribute success]           [PutFile /data/nifi-failed]
        │
        ▼ (auto-terminate)

Usage
-----
  # Wait for NiFi to start, then run once:
  python nifi/deploy_flow.py --nifi-url http://localhost:8080 \\
      --username admin --password <NIFI_PASSWORD> \\
      --backend-url http://backend:8000 \\
      --auth-token <ADMIN_JWT>

  # Inside the docker network (after docker compose --profile pipeline up -d):
  docker exec -it foodrescue-nifi-1 \\
      python3 /nifi-config/deploy_flow.py \\
      --nifi-url http://localhost:8080 \\
      --username admin --password <NIFI_PASSWORD> \\
      --backend-url http://backend:8000 \\
      --auth-token <ADMIN_JWT>

Requirements
------------
  pip install requests

Secrets
-------
Pass --auth-token via environment variable or a secrets manager.
Never commit real tokens to version control.

  export FOODRESCUE_AUTH_TOKEN=$(cat /run/secrets/nifi_auth_token)
  python nifi/deploy_flow.py ... --auth-token "$FOODRESCUE_AUTH_TOKEN"

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


def _get_token(nifi_url: str, username: str, password: str) -> str:
    url = nifi_url.rstrip("/") + "/nifi-api/access/token"
    data = f"username={username}&password={password}".encode()
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode().strip()
    except urllib.error.HTTPError as exc:
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
# Flow builder
# ---------------------------------------------------------------------------

def build_flow(nifi_url: str, token: str, backend_url: str, auth_token: str,
               source: str = "nifi-inbox",
               inbox_dir: str = "/data/inbox",
               failed_dir: str = "/data/nifi-failed") -> str:
    """Create the FoodRescue ingestion flow in NiFi.  Returns the process-group ID."""

    # 1. Get root process group ID.
    root_pg = _api(nifi_url, "/nifi-api/flow/process-groups/root", token=token)
    root_id = root_pg["processGroupFlow"]["id"]
    print(f"Root process group: {root_id}")

    # 2. Create a named process group for isolation.
    pg = _api(nifi_url, f"/nifi-api/process-groups/{root_id}/process-groups",
              method="POST", token=token,
              body={"revision": {"version": 0},
                    "component": {"name": "FoodRescue Inbox Ingestion",
                                  "position": {"x": 0.0, "y": 0.0}}})
    pg_id = pg["id"]
    print(f"Created process group: {pg_id}")

    # 3. GetFile processor — reads CSVs from /data/inbox.
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
    get_file_id = get_file["id"]
    print(f"  GetFile processor: {get_file_id}")

    # 4. UpdateAttribute — attach batch metadata.
    update_attr = _api(nifi_url, f"/nifi-api/process-groups/{pg_id}/processors",
                       method="POST", token=token,
                       body={"revision": {"version": 0},
                             "component": {
                                 "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
                                 "name": "Set batch metadata",
                                 "position": {"x": 0.0, "y": 200.0},
                                 "config": {
                                     "properties": {
                                         "ingest.source": source,
                                         "ingest.filename": "${filename}",
                                     }}}})
    update_attr_id = update_attr["id"]
    print(f"  UpdateAttribute processor: {update_attr_id}")

    # 5. InvokeHTTP — POST to /ingest/batch.
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
                                         "Request Content-Type": "multipart/form-data",
                                         "Authorization": f"Bearer {auth_token}",
                                         # Custom attributes as form fields
                                         "Attributes to Send": "ingest\\..*",
                                         "Send Message Body": "true",
                                         "Connection Timeout": "30 sec",
                                         "Read Timeout": "60 sec",
                                         "Response Body Ignored": "false",
                                     },
                                     "autoTerminatedRelationships": [],
                                 }}})
    invoke_http_id = invoke_http["id"]
    print(f"  InvokeHTTP processor: {invoke_http_id}")

    # 6. PutFile — failed files go to /data/nifi-failed.
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
    put_file_id = put_file["id"]
    print(f"  PutFile (failure) processor: {put_file_id}")

    # 7. Connect processors.
    def connect(src_id: str, src_rel: str, dst_id: str) -> str:
        conn = _api(nifi_url, f"/nifi-api/process-groups/{pg_id}/connections",
                    method="POST", token=token,
                    body={"revision": {"version": 0},
                          "component": {
                              "source": {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
                              "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
                              "selectedRelationships": [src_rel],
                          }})
        return conn["id"]

    connect(get_file_id, "success", update_attr_id)
    connect(update_attr_id, "success", invoke_http_id)
    connect(invoke_http_id, "Failure", put_file_id)
    connect(invoke_http_id, "No Retry", put_file_id)
    print("  Connections created.")

    # Auto-terminate success relationships on InvokeHTTP (2xx responses).
    _api(nifi_url, f"/nifi-api/processors/{invoke_http_id}",
         method="PUT", token=token,
         body={"revision": invoke_http["revision"],
               "component": {
                   "id": invoke_http_id,
                   "config": {"autoTerminatedRelationships": ["Response", "Original", "Retry"]}}})

    print(f"\nFlow created in process group: {pg_id}")
    print("Start the flow in the NiFi UI or via:")
    print(f"  PUT /nifi-api/flow/process-groups/{pg_id} {{\"id\":\"{pg_id}\","
          f"\"state\":\"RUNNING\"}}")
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

    if not args.password:
        print("ERROR: --password or NIFI_PASSWORD env var required", file=sys.stderr)
        sys.exit(1)
    if not args.auth_token:
        print("ERROR: --auth-token or FOODRESCUE_AUTH_TOKEN env var required", file=sys.stderr)
        sys.exit(1)

    if args.wait:
        wait_for_nifi(args.nifi_url)

    print(f"Authenticating with NiFi at {args.nifi_url} …")
    token = _get_token(args.nifi_url, args.username, args.password)

    build_flow(
        nifi_url=args.nifi_url,
        token=token,
        backend_url=args.backend_url,
        auth_token=args.auth_token,
        source=args.source,
        inbox_dir=args.inbox_dir,
        failed_dir=args.failed_dir,
    )


if __name__ == "__main__":
    main()
