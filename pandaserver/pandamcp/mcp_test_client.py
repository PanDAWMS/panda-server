import argparse
import asyncio

from fastmcp import Client
from fastmcp.client.transports import SSETransport, StreamableHttpTransport

# argparse setup
parser = argparse.ArgumentParser(description="MCP test client")
parser.add_argument(
    "--transport",
    type=str,
    choices=("streamable-http", "sse"),
    default="streamable-http",
    help="Transport to use: streamable-http or sse (default: streamable-http)",
)
parser.add_argument("--host", type=str, default="localhost", help="PanDA Server host (default: localhost)")
parser.add_argument("--port", type=int, default=25443, help="PanDA Server port (default: 25443)")
parser.add_argument("--use_http", action=argparse.BooleanOptionalAction, default=False, help="Use HTTP instead of HTTPS (default: False)")
parser.add_argument("--token", type=str, default=None, help="OIDC ID token for write-operations (default: None)")
parser.add_argument("--vo", type=str, default=None, help="Virtual organization with ID token is given (default: None)")
parser.add_argument("--tool", type=str, default="is_alive", help="A tool name to test (default: is_alive)")
parser.add_argument(
    "--kv",
    dest="kv",
    metavar="KEY=VAL",
    action="append",
    help="Arguments to invoke the test function. Key=Value pairs repeat for multiple (e.g. --kv a=1 --kv b=2 --kv c=True)",
)
args = parser.parse_args()

# parse --kv KEY=VAL (repeatable) into a dict stored back on args.kv
raw_kv = args.kv or []
kv = {}
for item in raw_kv:
    if "=" not in item:
        parser.error(f"invalid --kv value {item!r}, expected KEY=VALUE")
    k, v = item.split("=", 1)
    v = v.strip()
    if v == "True":
        v = True
    elif v == "False":
        v = False
    kv[k] = v
args.kv = kv

# construct base URL
if args.use_http:
    base_url = f"http://{args.host}:{args.port}/mcp/"
else:
    base_url = f"https://{args.host}:{args.port}/mcp/"

headers = {"Origin": args.vo} if args.token else None

# select transport
if args.transport == "streamable-http":
    transport = StreamableHttpTransport(url=base_url, auth=args.token, headers=headers)
else:
    transport = SSETransport(url=base_url, auth=args.token, headers=headers)

# create client
client = Client(transport)


# test function
async def cl():
    # Connection is established here
    async with client:
        if client.is_connected():
            print("Client connected")
        else:
            print("Client failed to connect")
            return

        # Make MCP calls within the context
        tools = await client.list_tools()
        print(f"\nAvailable tools:")
        for tool in tools:
            print(f"- {tool.name} -\n")
            print(f" Description: {tool.description}")

        print("\n" * 2)
        print(f"Testing {args.tool}:")
        result = await client.call_tool(args.tool, args.kv)
        print(f"Result: {result}\n")

    # Connection is closed automatically here
    if not client.is_connected():
        print("Client disconnected")
    else:
        print("Client still connected")
    print("Done")


if __name__ == "__main__":
    asyncio.run(cl())
