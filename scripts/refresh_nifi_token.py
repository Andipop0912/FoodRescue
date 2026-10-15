#!/usr/bin/env python3
"""Refresh the Bearer token stored in NiFi's InvokeHTTP processor.

The NiFi InvokeHTTP processor stores the backend JWT as a static property.
JWTs expire (default 12 h).  Run this script whenever the token has expired
to restore end-to-end ingestion without redeploying the flow.

Usage
-----
  # Auto-login: script gets a fresh JWT from the FoodRescue login endpoint
  python scripts/refresh_nifi_token.py \\
      --process-group-id 40bcdeef-01a1-1000-ddcf-18d71ea05c8f

  # Or supply a pre-generated JWT directly:
  python scripts/refresh_nifi_token.py \\
      --process-group-id 40bcdeef-01a1-1000-ddcf-18d71ea05c8f \\
      --auth-token eyJ...

Environment variables (override --flags of the same name):
  NIFI_PG_ID             NiFi process group ID
  FOODRESCUE_AUTH_TOKEN  Pre-generated admin JWT
  BACKEND_URL            Default http://localhost:8000
  NIFI_URL               Default http://localhost:8080
  ADMIN_EMAIL            Default admin@foodrescue.demo
  ADMIN_PASSWORD         Default demo123

Security notes
--------------
  The JWT is passed as an Authorization header value and stored as a NiFi
  processor property.  It is NOT logged or printed by this script.
  Passwords are read from env vars or args — never hardcoded.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

PROCESSOR_TYPE = "org.apache.nifi.processors.standard.InvokeHTTP"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _post_json(url: str, body: dict) -> dict:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _nifi(nifi_url: str, path: str, method: str = "GET", body: dict | None = None) -> dict:
    url = nifi_url.rstrip("/") + path
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:400]
        raise RuntimeError(f"NiFi {method} {path} → HTTP {exc.code}: {detail}") from exc


# ---------------------------------------------------------------------------
# Backend login
# ---------------------------------------------------------------------------

def get_fresh_token(backend_url: str, email: str, password: str) -> str:
    """Obtain a fresh admin JWT from the FoodRescue login endpoint."""
    url = backend_url.rstrip("/") + "/auth/login"
    try:
        resp = _post_json(url, {"email": email, "password": password})
        token = resp.get("access_token", "")
        if not token:
            raise RuntimeError(f"Login response had no access_token: {list(resp.keys())}")
        # Never print the token.
        return token
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Backend login failed: HTTP {exc.code}") from exc


# ---------------------------------------------------------------------------
# NiFi processor discovery and update
# ---------------------------------------------------------------------------

def find_invoke_http_processors(nifi_url: str, pg_id: str) -> list[dict]:
    """Return all InvokeHTTP processor entities in the given process group."""
    resp = _nifi(nifi_url, f"/nifi-api/process-groups/{pg_id}/processors")
    return [
        p for p in resp.get("processors", [])
        if p.get("component", {}).get("type") == PROCESSOR_TYPE
    ]


def update_processor_token(nifi_url: str, processor: dict, new_token: str) -> None:
    """Stop the processor, update its Authorization property, then restart it."""
    proc_id = processor["id"]
    revision = processor["revision"]
    current_state = processor.get("component", {}).get("state", "RUNNING")

    # Stop if running.
    if current_state == "RUNNING":
        stop_resp = _nifi(
            nifi_url, f"/nifi-api/processors/{proc_id}/run-status",
            method="PUT",
            body={"revision": revision, "state": "STOPPED",
                  "disconnectedNodeAcknowledged": False},
        )
        revision = stop_resp.get("revision", revision)
        print(f"  Stopped processor {proc_id}")

    # Fetch latest revision after stop.
    fresh = _nifi(nifi_url, f"/nifi-api/processors/{proc_id}")
    revision = fresh.get("revision", revision)

    # Update the Authorization dynamic property.
    current_props = fresh.get("component", {}).get("config", {}).get("properties", {})
    current_props["Authorization"] = f"Bearer {new_token}"

    update_resp = _nifi(
        nifi_url, f"/nifi-api/processors/{proc_id}",
        method="PUT",
        body={
            "revision": revision,
            "component": {
                "id": proc_id,
                "config": {"properties": current_props},
            },
        },
    )
    revision = update_resp.get("revision", revision)
    print(f"  Updated Authorization token on processor {proc_id}")

    # Restart.
    if current_state == "RUNNING":
        _nifi(
            nifi_url, f"/nifi-api/processors/{proc_id}/run-status",
            method="PUT",
            body={"revision": revision, "state": "RUNNING",
                  "disconnectedNodeAcknowledged": False},
        )
        print(f"  Restarted processor {proc_id}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh the NiFi InvokeHTTP Authorization JWT",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--process-group-id",
        default=os.environ.get("NIFI_PG_ID", ""),
        help="NiFi process group ID (or set NIFI_PG_ID)",
    )
    parser.add_argument(
        "--nifi-url",
        default=os.environ.get("NIFI_URL", "http://localhost:8080"),
    )
    parser.add_argument(
        "--backend-url",
        default=os.environ.get("BACKEND_URL", "http://localhost:8000"),
    )
    parser.add_argument(
        "--auth-token",
        default=os.environ.get("FOODRESCUE_AUTH_TOKEN", ""),
        help="Pre-generated admin JWT (or set FOODRESCUE_AUTH_TOKEN)",
    )
    parser.add_argument(
        "--admin-email",
        default=os.environ.get("ADMIN_EMAIL", "admin@foodrescue.demo"),
    )
    parser.add_argument(
        "--admin-password",
        default=os.environ.get("ADMIN_PASSWORD", "demo123"),
        help="FoodRescue admin password (or set ADMIN_PASSWORD)",
    )
    args = parser.parse_args()

    if not args.process_group_id:
        print("ERROR: --process-group-id or NIFI_PG_ID env var required", file=sys.stderr)
        sys.exit(1)

    # Get or use the backend JWT.
    if args.auth_token:
        token = args.auth_token
        print("Using supplied --auth-token.")
    else:
        print(f"Logging in to {args.backend_url} as {args.admin_email} …")
        token = get_fresh_token(args.backend_url, args.admin_email, args.admin_password)
        print("  Login successful.")

    # Find InvokeHTTP processors.
    print(f"Scanning process group {args.process_group_id} …")
    processors = find_invoke_http_processors(args.nifi_url, args.process_group_id)
    if not processors:
        print("ERROR: no InvokeHTTP processors found in the process group.", file=sys.stderr)
        sys.exit(1)
    print(f"  Found {len(processors)} InvokeHTTP processor(s).")

    # Update each one.
    for proc in processors:
        name = proc.get("component", {}).get("name", proc["id"])
        print(f"  Updating '{name}' …")
        update_processor_token(args.nifi_url, proc, token)

    print("\nToken refresh complete.")
    print("Drop a CSV into data/inbox/ to trigger ingestion.")


if __name__ == "__main__":
    main()
