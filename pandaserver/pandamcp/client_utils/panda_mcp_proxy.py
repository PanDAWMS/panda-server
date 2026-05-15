#!/usr/bin/env python3
"""
panda_mcp_proxy.py

Cross-platform MCP proxy with automatic token refresh.
Drop-in replacement for panda_mcp_wrapper.sh that works on Windows, macOS, and Linux.

The core problem it solves: panda_mcp_wrapper.sh passes the id_token as a static
header to mcp-remote at startup. After 15 minutes the token expires and every MCP
call fails until Claude Desktop is restarted. This proxy refreshes the token
transparently on every outbound request, so sessions can run indefinitely.

Architecture:
    LLM client as Claude Desktop or LM Studio <--stdio (JSON-RPC)--> this proxy <--HTTPS/SSE--> remote MCP server

Requirements:
    pip install httpx

Configuration in LLM client (claude_desktop_config.json or mcp.json):
    {
      "mcpServers": {
        "panda-mcp": {
          "command": "python3",
          "args": ["/path/to/panda_mcp_proxy.py"]
        }
      }
    }

Environment variables (all optional, same defaults as panda_mcp_wrapper.sh):
    PANDA_SERVER     PanDA server URL          (default: https://pandaserver.cern.ch:25443)
    VO               Virtual organisation      (default: atlas)
    TOKEN_FILE       Path to token cache file  (default: ~/.panda_id_token)
    MCP_URL          Remote MCP server URL     (default: https://aipanda120.cern.ch:8443/mcp/)
    SSL_CERT_FILE    Path to CA bundle file    (optional, for custom CAs)
    REQUESTS_CA_BUNDLE  Alternative to SSL_CERT_FILE
"""

import asyncio
import base64
import json
import logging
import os
import pathlib
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    import httpx
except ImportError:
    print("ERROR: httpx is required. Install with: pip install httpx", file=sys.stderr)
    sys.exit(1)


# ── Configuration ─────────────────────────────────────────────────────────────

PANDA_SERVER = os.environ.get("PANDA_SERVER", "https://pandaserver.cern.ch:25443")
VO = os.environ.get("VO", "atlas")
TOKEN_FILE = pathlib.Path(os.environ.get("TOKEN_FILE", pathlib.Path.home() / ".panda_id_token"))
MCP_URL = os.environ.get("MCP_URL", "https://aipanda120.cern.ch:8443/mcp/")

# Refresh the token this many seconds before it actually expires
TOKEN_REFRESH_MARGIN = 300

# How long to wait before retrying a failed SSE connection (seconds)
SSE_RECONNECT_DELAY = 5


# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [panda_mcp_proxy] %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)


# ── SSL context ───────────────────────────────────────────────────────────────


def _build_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ca_file = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
    if ca_file:
        ctx.load_verify_locations(cafile=ca_file)
    return ctx


# ── Token management ──────────────────────────────────────────────────────────


class TokenManager:
    """Thread-safe token cache with silent refresh via refresh_token."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._id_token: str = ""
        self._exp: float = 0.0

    # ── private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _decode_exp(id_token: str) -> float:
        try:
            payload = id_token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
            return float(claims.get("exp", 0))
        except Exception:
            return 0.0

    @staticmethod
    def _load_file() -> dict:
        try:
            return json.loads(TOKEN_FILE.read_text())
        except Exception:
            return {}

    @staticmethod
    def _save_file(data: dict) -> None:
        try:
            TOKEN_FILE.write_text(json.dumps(data))
        except Exception as exc:
            log.warning("Could not update token file: %s", exc)

    # urllib is used here (no httpx) so this can be called without an async client
    @staticmethod
    def _http_get_json(url: str, ssl_ctx: ssl.SSLContext) -> dict:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=15) as r:
            return json.load(r)

    @staticmethod
    def _http_post_form(url: str, data: dict, ssl_ctx: ssl.SSLContext) -> dict:
        encoded = urllib.parse.urlencode(data).encode()
        req = urllib.request.Request(url, data=encoded, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=15) as r:
            return json.load(r)

    async def _do_refresh(self, refresh_token: str) -> str:
        """Use refresh_token to obtain a fresh id_token. Returns '' on failure."""
        ssl_ctx = _build_ssl_context()
        loop = asyncio.get_event_loop()

        try:
            auth_cfg = await loop.run_in_executor(None, self._http_get_json, f"{PANDA_SERVER}/auth/{VO}_auth_config.json", ssl_ctx)
            oidc_cfg = await loop.run_in_executor(None, self._http_get_json, auth_cfg["oidc_config_url"], ssl_ctx)
            token_resp = await loop.run_in_executor(
                None,
                self._http_post_form,
                oidc_cfg["token_endpoint"],
                {
                    "grant_type": "refresh_token",
                    "client_id": auth_cfg["client_id"],
                    "client_secret": auth_cfg.get("client_secret", ""),
                    "refresh_token": refresh_token,
                },
                ssl_ctx,
            )
        except Exception as exc:
            log.error("Token refresh request failed: %s", exc)
            return ""

        id_token = token_resp.get("id_token", "")
        if id_token:
            self._save_file(token_resp)
            log.warning("id_token refreshed successfully.")
        else:
            log.error("Refresh response missing id_token: %s", token_resp.get("error", "unknown"))
        return id_token

    # ── public API ────────────────────────────────────────────────────────────

    async def get(self) -> str:
        """Return a valid id_token, refreshing silently if needed."""
        async with self._lock:
            now = time.time()

            # In-memory token still good
            if self._id_token and self._exp - now > TOKEN_REFRESH_MARGIN:
                return self._id_token

            # Try token file
            data = self._load_file()
            id_token = data.get("id_token", "")
            if id_token:
                exp = self._decode_exp(id_token)
                if exp - now > TOKEN_REFRESH_MARGIN:
                    self._id_token = id_token
                    self._exp = exp
                    return id_token

            # Attempt silent refresh
            refresh_token = data.get("refresh_token", "")
            if refresh_token:
                log.warning("id_token expired or close to expiry — refreshing silently…")
                id_token = await self._do_refresh(refresh_token)
                if id_token:
                    self._id_token = id_token
                    self._exp = self._decode_exp(id_token)
                    return id_token

            raise RuntimeError("No valid token found. Run get_panda_token.sh (or get_panda_token.py) first.")


# ── SSE helpers ───────────────────────────────────────────────────────────────


def _parse_sse(lines: list[str]) -> tuple[str, str]:
    """Parse a complete SSE block (list of non-empty lines) into (event, data)."""
    event = "message"
    data = ""
    for line in lines:
        if line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data = line[5:].strip()
    return event, data


# ── Stdio helpers (cross-platform) ────────────────────────────────────────────


async def _read_stdin_lines(queue: asyncio.Queue) -> None:
    """Read newline-delimited JSON from stdin and push to queue. Runs in a thread."""
    loop = asyncio.get_event_loop()

    def _blocking_read():
        # sys.stdin.readline returns '' on EOF; works on Windows and Unix
        return sys.stdin.readline()

    while True:
        line = await loop.run_in_executor(None, _blocking_read)
        if not line:
            break
        line = line.strip()
        if line:
            await queue.put(line)


async def _write_stdout_lines(queue: asyncio.Queue) -> None:
    """Write newline-delimited JSON from queue to stdout."""
    loop = asyncio.get_event_loop()

    def _blocking_write(msg: str):
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()

    while True:
        msg = await queue.get()
        await loop.run_in_executor(None, _blocking_write, msg)


# ── MCP proxy (Streamable HTTP transport) ────────────────────────────────────
#
# MCP spec 2025-03-26+ uses "Streamable HTTP":
#   - Every client message is a POST to MCP_URL
#   - The POST response is either plain JSON or an SSE stream (both handled below)
#   - An optional GET to MCP_URL opens a server-push SSE channel; 405 means unsupported
#   - The server returns Mcp-Session-Id on the initialize response; subsequent
#     requests include it so the server can correlate the session


class MCPProxy:
    def __init__(self):
        self.tokens = TokenManager()
        self._session_id: str | None = None
        self._outbound: asyncio.Queue = asyncio.Queue()  # stdin  → remote
        self._inbound: asyncio.Queue = asyncio.Queue()  # remote → stdout

    async def _headers(self, extra: dict | None = None) -> dict:
        token = await self.tokens.get()
        h = {
            "Authorization": f"Bearer {token}",
            "Origin": VO,
            "X-Auth-Token": f"Bearer {token}",
        }
        if self._session_id:
            h["Mcp-Session-Id"] = self._session_id
        if extra:
            h.update(extra)
        return h

    async def _handle_response(self, resp: httpx.Response) -> None:
        """Read a POST response — either plain JSON or an SSE stream."""
        # Capture session ID returned on initialize
        if sid := resp.headers.get("Mcp-Session-Id"):
            if self._session_id != sid:
                self._session_id = sid
                log.warning("MCP session ID: %s", sid)

        content_type = resp.headers.get("content-type", "")
        if "text/event-stream" in content_type:
            block: list[str] = []
            async for raw_line in resp.aiter_lines():
                if raw_line:
                    block.append(raw_line)
                elif block:
                    _, data = _parse_sse(block)
                    block.clear()
                    if data:
                        await self._inbound.put(data)
        else:
            body = await resp.aread()
            if body.strip():
                await self._inbound.put(body.decode())

    async def _sender_loop(self, client: httpx.AsyncClient) -> None:
        """Forward every stdin message to the remote server via POST."""
        while True:
            msg = await self._outbound.get()
            try:
                headers = await self._headers(
                    {
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                    }
                )
                async with client.stream("POST", MCP_URL, content=msg, headers=headers) as resp:
                    resp.raise_for_status()
                    await self._handle_response(resp)
            except Exception as exc:
                log.error("POST failed: %s", exc)

    async def _server_push_loop(self, client: httpx.AsyncClient) -> None:
        """Optional GET SSE channel for server-initiated messages. Exits if unsupported (405)."""
        while True:
            try:
                headers = await self._headers({"Accept": "text/event-stream"})
                log.warning("Opening server-push SSE channel to %s", MCP_URL)
                async with client.stream("GET", MCP_URL, headers=headers) as resp:
                    if resp.status_code == 405:
                        log.warning("Server does not support GET SSE push — skipping.")
                        return
                    resp.raise_for_status()
                    block: list[str] = []
                    async for raw_line in resp.aiter_lines():
                        if raw_line:
                            block.append(raw_line)
                        elif block:
                            _, data = _parse_sse(block)
                            block.clear()
                            if data:
                                await self._inbound.put(data)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (400, 405):
                    log.warning("Server does not support GET SSE push (%s) — skipping.", exc.response.status_code)
                    return
                log.error("Server-push SSE failed (%s) — retrying in %ds", exc.response.status_code, SSE_RECONNECT_DELAY)
            except Exception as exc:
                log.error("Server-push SSE error: %s — retrying in %ds", exc, SSE_RECONNECT_DELAY)

            await asyncio.sleep(SSE_RECONNECT_DELAY)

    async def run(self) -> None:
        # Fail fast if no token is available before accepting any MCP traffic
        await self.tokens.get()

        ssl_ctx = _build_ssl_context()
        async with httpx.AsyncClient(verify=ssl_ctx, timeout=None, follow_redirects=True) as client:
            await asyncio.gather(
                _read_stdin_lines(self._outbound),
                _write_stdout_lines(self._inbound),
                self._sender_loop(client),
                self._server_push_loop(client),
            )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # On Windows, stdin/stdout must be in binary-compatible text mode
    if sys.platform == "win32":
        import msvcrt

        msvcrt.setmode(sys.stdin.fileno(), os.O_BINARY)
        msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)

    try:
        asyncio.run(MCPProxy().run())
    except KeyboardInterrupt:
        pass
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
