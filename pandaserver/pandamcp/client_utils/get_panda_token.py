#!/usr/bin/env python3
"""
get_panda_token.py
Replicates panda-client's device authorization flow to get an id_token.
Usage: python get_panda_token.py
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from urllib import error, parse, request

PANDA_SERVER = os.environ.get("PANDA_SERVER", "https://pandaserver.cern.ch:25443")
VO = os.environ.get("VO", "atlas")
TOKEN_FILE = os.environ.get("TOKEN_FILE", os.path.join(os.path.expanduser("~"), ".panda_id_token"))


def fetch_json(url, post_data=None):
    import ssl

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    if post_data:
        data = parse.urlencode(post_data).encode()
        req = request.Request(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    else:
        req = request.Request(url)

    with request.urlopen(req, context=ctx) as resp:
        return json.loads(resp.read().decode())


def main():
    print(f"==> Fetching auth config from {PANDA_SERVER}/auth/{VO}_auth_config.json")
    try:
        auth_config = fetch_json(f"{PANDA_SERVER}/auth/{VO}_auth_config.json")
    except Exception as e:
        print(f"ERROR: Failed to fetch auth config: {e}")
        sys.exit(1)

    client_id = auth_config["client_id"]
    client_secret = auth_config.get("client_secret") or ""
    audience = auth_config["audience"]
    oidc_config_url = auth_config["oidc_config_url"]

    print(f"==> client_id: {client_id}")
    print(f"==> audience:  {audience}")

    try:
        oidc_config = fetch_json(oidc_config_url)
    except Exception as e:
        print(f"ERROR: Failed to fetch OIDC config: {e}")
        sys.exit(1)

    device_endpoint = oidc_config["device_authorization_endpoint"]
    token_endpoint = oidc_config["token_endpoint"]

    print("==> Requesting device code...")
    try:
        device_response = fetch_json(
            device_endpoint,
            {
                "client_id": client_id,
                "scope": "openid profile email offline_access",
                "audience": audience,
            },
        )
    except Exception as e:
        print(f"ERROR: Failed to request device code: {e}")
        sys.exit(1)

    device_code = device_response["device_code"]
    verification_uri = device_response["verification_uri_complete"]
    expires_in = int(device_response["expires_in"])
    interval = int(device_response.get("interval", 5))

    print()
    print("==> Please open the following URL in your browser and sign in:")
    print()
    print(f"    {verification_uri}")
    print()
    input("Press Enter once you have signed in...")

    print("==> Polling for token...")
    elapsed = 0
    id_token = None
    token_expires_in = None

    while elapsed < expires_in:
        try:
            token_response = fetch_json(
                token_endpoint,
                {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_code,
                },
            )
        except Exception as e:
            print(f"ERROR: Token request failed: {e}")
            sys.exit(1)

        err = token_response.get("error", "")

        if err == "authorization_pending":
            # RFC 8628: wait at least interval seconds between polls; +1 adds a small buffer
            sleep_time = interval + 1
            time.sleep(sleep_time)
            elapsed += sleep_time
            continue
        elif not err:
            id_token = token_response["id_token"]
            token_expires_in = token_response.get("expires_in")
            with open(TOKEN_FILE, "w") as f:
                json.dump(token_response, f)
            expiry_str = (datetime.now() + timedelta(seconds=int(token_expires_in))).strftime("%Y-%m-%d %H:%M:%S") if token_expires_in else "unknown"
            print()
            print(f"==> Token saved to {TOKEN_FILE}")
            print(f"==> Token expires at: {expiry_str}")
            break
        else:
            print(f"ERROR: {json.dumps(token_response)}")
            sys.exit(1)

    if not id_token:
        print("ERROR: Timed out waiting for authentication")
        sys.exit(1)

    print("==> Token generated successfully!")
    print()
    print("Run the following to set your token:")
    print()
    if sys.platform == "win32":
        print("  PowerShell:")
        print(f'    $env:ACCESS_TOKEN="{id_token}"')
        print(f'    $env:TOKEN_FILE="{TOKEN_FILE}"')
        print("  CMD:")
        print(f"    set ACCESS_TOKEN={id_token}")
        print(f"    set TOKEN_FILE={TOKEN_FILE}")
    else:
        print(f"  export ACCESS_TOKEN={id_token}")
        print(f"  export TOKEN_FILE={TOKEN_FILE}")


if __name__ == "__main__":
    main()
