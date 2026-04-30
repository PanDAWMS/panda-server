#!/usr/bin/env bash
# get_panda_token.sh
# Replicates panda-client's device authorization flow to get an id_token.
# Usage: source get_panda_token.sh

PANDA_SERVER=${PANDA_SERVER:-"https://pandaserver.cern.ch:25443"}
VO=${VO:-"atlas"}
TOKEN_FILE=${TOKEN_FILE:-"${HOME}/.panda_id_token"}

echo "==> Fetching auth config from ${PANDA_SERVER}/auth/${VO}_auth_config.json"
# -k disables TLS certificate verification; acceptable for a developer helper script
AUTH_CONFIG=$(curl -sk "${PANDA_SERVER}/auth/${VO}_auth_config.json")
if [ -z "$AUTH_CONFIG" ]; then
    echo "ERROR: Failed to fetch auth config"
    return 1 2>/dev/null || exit 1
fi

# Parse the fields from the auth config
read -r CLIENT_ID CLIENT_SECRET AUDIENCE OIDC_CONFIG_URL < <(
    python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['client_id'], d.get('client_secret') or '', d['audience'], d['oidc_config_url'])
" <<< "$AUTH_CONFIG")

echo "==> client_id: $CLIENT_ID"
echo "==> audience:  $AUDIENCE"

# Fetch OIDC endpoints
OIDC_CONFIG=$(curl -sk "$OIDC_CONFIG_URL")
read -r DEVICE_ENDPOINT TOKEN_ENDPOINT < <(
    python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['device_authorization_endpoint'], d['token_endpoint'])
" <<< "$OIDC_CONFIG")

echo "==> Requesting device code..."
DEVICE_RESPONSE=$(curl -sk -X POST "$DEVICE_ENDPOINT" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "client_id=${CLIENT_ID}&scope=openid profile email offline_access&audience=${AUDIENCE}")

# Parse device response
# device_code: code to include in token request
# verification_uri_complete: URL to open in browser for user authentication
# expires_in: time in seconds before device code expires
# Interval: polling interval in seconds
read -r DEVICE_CODE VERIFICATION_URI EXPIRES_IN INTERVAL < <(
    python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['device_code'], d['verification_uri_complete'], d['expires_in'], d.get('interval', 5))
" <<< "$DEVICE_RESPONSE")

if ! [[ "$EXPIRES_IN" =~ ^[0-9]+$ ]] || ! [[ "$INTERVAL" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Invalid expires_in or interval in device response"
    return 1 2>/dev/null || exit 1
fi

echo ""
echo "==> Please open the following URL in your browser and sign in:"
echo ""
echo "    $VERIFICATION_URI"
echo ""
read -rp "Press Enter once you have signed in..."

# Poll for token
echo "==> Polling for token..."
ELAPSED=0
while [ "$ELAPSED" -lt "$EXPIRES_IN" ]; do
    # client_secret is empty for public clients; included here for confidential client support
    TOKEN_RESPONSE=$(curl -sk -X POST "$TOKEN_ENDPOINT" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&grant_type=urn:ietf:params:oauth:grant-type:device_code&device_code=${DEVICE_CODE}")

    ERROR=$(echo "$TOKEN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('error',''))" 2>/dev/null)

    if [ "$ERROR" == "authorization_pending" ]; then
        # RFC 8628 requires waiting at least interval seconds between polls; +1 adds a small buffer
        sleep $((INTERVAL + 1))
        ELAPSED=$((ELAPSED + INTERVAL + 1))
        continue
    elif [ -z "$ERROR" ]; then
        read -r ID_TOKEN TOKEN_EXPIRES_IN < <(
            python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['id_token'], d.get('expires_in', ''))
" <<< "$TOKEN_RESPONSE")
        echo "$TOKEN_RESPONSE" > "$TOKEN_FILE"
        EXPIRY_STR=$(date -d "+${TOKEN_EXPIRES_IN} seconds" 2>/dev/null || date -v "+${TOKEN_EXPIRES_IN}S" 2>/dev/null || echo "unknown")
        echo ""
        echo "==> Token saved to $TOKEN_FILE"
        echo "==> Token expires at: $EXPIRY_STR"
        break
    else
        echo "ERROR: $TOKEN_RESPONSE"
        return 1 2>/dev/null || exit 1
    fi
done

if [ -z "$ID_TOKEN" ]; then
    echo "ERROR: Timed out waiting for authentication"
    return 1 2>/dev/null || exit 1
fi

echo "==> Token generated successfully!"

# Executed as source get_panda_token.sh: export token to current shell
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    # id_token is exported as ACCESS_TOKEN to match the name panda-client tools expect
    export ACCESS_TOKEN=$ID_TOKEN
    # TOKEN_FILE exported so callers can extract other fields (e.g. refresh_token) without hardcoding the path
    export TOKEN_FILE
    echo "==> ACCESS_TOKEN exported to current shell (full response in \$TOKEN_FILE: $TOKEN_FILE)"
# Executed as ./get_panda_token.sh: print instructions to export token manually since we can't modify the parent shell's environment
else
    echo ""
    echo "Run the following to set your token:"
    echo ""
    echo "  export ACCESS_TOKEN=$ID_TOKEN"
    echo "  export TOKEN_FILE=$TOKEN_FILE"
fi