import os

import uvicorn
from fastmcp import FastMCP

from pandaserver.api.v1.statistics_api import job_stats_by_cloud
from pandaserver.api.v1.system_api import is_alive
from pandaserver.pandamcp.mcp_utils import create_tool

main_mcp = FastMCP(name="Main")

# add tools
for func in [is_alive, job_stats_by_cloud]:
    tool = create_tool(func)
    main_mcp.add_tool(tool)

http_app = main_mcp.http_app()

if __name__ == "__main__":
    uvicorn.run(
        http_app,
        port=os.getenv("PANDA_SERVER_CONF_PORT_MCP", 25888),
        #        ssl_keyfile="/etc/grid-security/hostkey.pem",
        #        ssl_certfile="/etc/grid-security/hostcert.pem",
    )
