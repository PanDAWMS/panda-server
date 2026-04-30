#!/bin/bash
# panda_mcp_wrapper.sh
# Launches mcp-remote with a cached PanDA id_token.
# Requires a valid token in TOKEN_FILE (run: source get_panda_token.sh first).
# The MCP configuration could look like this:
#   "mcpServers": {
#     "aipanda120-mcp": {
#       "command": "/Users/fbarreir/panda_mcp_wrapper.sh",
#       "args": [],
#       "env": {
#         "NODE_EXTRA_CA_CERTS": "/Users/fbarreir/all-certs.pem"
#       }
#     }
#   },

PANDA_SERVER=${PANDA_SERVER:-"https://pandaserver.cern.ch:25443"}
VO=${VO:-"atlas"}
TOKEN_FILE=${TOKEN_FILE:-"${HOME}/.panda_id_token"}
MCP_URL=${MCP_URL:-"https://aipanda120.cern.ch:8443/mcp/"}

# Check if we have a cached valid token
if [ -f "$TOKEN_FILE" ]; then
    ID_TOKEN=$(python3 -c "
import json, base64, time
with open('$TOKEN_FILE') as f:
    data = json.load(f)
id_token = data.get('id_token', '')
if id_token:
    payload = id_token.split('.')[1]
    payload += '=' * (-len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload))
    # check if token expires in more than 5 minutes. The user not even see the error message.
    if claims.get('exp', 0) - time.time() > 300:
        print(id_token)
" 2>/dev/null)
fi

# If no valid id_token, try refreshing silently using the refresh_token
if [ -z "$ID_TOKEN" ] && [ -f "$TOKEN_FILE" ]; then
    REFRESH_TOKEN=$(python3 -c "
import json
with open('$TOKEN_FILE') as f:
    data = json.load(f)
print(data.get('refresh_token', ''))
" 2>/dev/null)

    if [ -n "$REFRESH_TOKEN" ]; then
        echo "==> id_token expired, attempting silent refresh..." >&2

        AUTH_CONFIG=$(curl -sk "${PANDA_SERVER}/auth/${VO}_auth_config.json")
        read -r CLIENT_ID CLIENT_SECRET OIDC_CONFIG_URL < <(
            python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['client_id'], d.get('client_secret') or '', d['oidc_config_url'])
" <<< "$AUTH_CONFIG")

        TOKEN_ENDPOINT=$(curl -sk "$OIDC_CONFIG_URL" | python3 -c "
import sys, json
print(json.load(sys.stdin)['token_endpoint'])
")

        TOKEN_RESPONSE=$(curl -sk -X POST "$TOKEN_ENDPOINT" \
            -H "Content-Type: application/x-www-form-urlencoded" \
            -d "grant_type=refresh_token&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&refresh_token=${REFRESH_TOKEN}")

        ID_TOKEN=$(python3 -c "
import sys, json
d = json.load(sys.stdin)
if 'id_token' in d:
    print(d['id_token'])
" <<< "$TOKEN_RESPONSE" 2>/dev/null)

        if [ -n "$ID_TOKEN" ]; then
            echo "$TOKEN_RESPONSE" > "$TOKEN_FILE"
            echo "==> Token refreshed and cached to $TOKEN_FILE" >&2
        else
            echo "==> Silent refresh failed (refresh_token may be expired)" >&2
        fi
    fi
fi

if [ -z "$ID_TOKEN" ]; then
    echo "ERROR: No valid token found. Authenticate first by running: source get_panda_token.sh" >&2
    exit 1
fi

# Launch mcp-remote with the token
exec npx mcp-remote "$MCP_URL" \
    --header "Authorization: Bearer ${ID_TOKEN}" \
    --header "Origin: atlas" \
    --header "X-Auth-Token: Bearer ${ID_TOKEN}"