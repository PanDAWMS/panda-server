import inspect
import os.path
from collections.abc import Callable

from fastmcp.server.dependencies import get_http_headers
from fastmcp.tools.tool import Tool
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl
from pandaserver.srvcore.panda_request import PandaRequest

logger = PandaLogger().getLogger(__name__.split(".")[-1])


def create_tool(func: Callable) -> Tool:
    """
    Create an MCP tool that wraps the API call.

    :param func: The function to wrap. It should be a callable that takes PandaRequest as the first argument.
    :return: An MCP tool.
    """

    # construct the URL based on the module and function name
    mod_path = inspect.getfile(inspect.getmodule(func))
    mod_name = os.path.basename(mod_path).split("_")[0]

    # determine http method based on the docstring
    http_method = None
    for line in func.__doc__.splitlines():
        line = line.strip()
        if line.startswith("HTTP Method:"):
            http_method = line.split(":")[1].strip().lower()
            if http_method not in ["get", "post"]:
                raise ValueError(f"Unsupported HTTP method: {http_method}")
            break
    if http_method is None:
        raise ValueError("HTTP Method not specified in the function docstring")

    url = f"{api_url_ssl}/{mod_name}/{func.__name__}"

    # remove PandaRequest from the signature and annotations
    sig = inspect.signature(func)
    params = []
    for p in sig.parameters.values():
        if p.annotation != PandaRequest and p.name != "req":
            params.append(p)

    annotations = {}
    for k, v in func.__annotations__.items():
        if v != PandaRequest and k != "req":
            annotations[k] = v

    # create a new function that wraps the API call
    def wrapped_func(**kwarg):
        nonlocal url, http_method
        kwarg.pop("req", None)
        # extract the id_token and auth_vo from the headers
        original_headers = get_http_headers()
        id_token = original_headers.get("x-auth-token") or original_headers.get("authorization")
        if id_token and id_token.startswith("Bearer "):
            id_token = id_token.split(" ")[1]
        auth_vo = original_headers.get("origin")
        oidc = id_token is not None

        # set the id_token and auth_vo in the HttpClient
        http_client = HttpClient()
        http_client.override_oidc(oidc, id_token, auth_vo)
        status, output = getattr(http_client, http_method)(url, kwarg)

        # Check for issues processing the request
        if status != 0:
            raise RuntimeError(output)

        return output

    # set the signature and annotations to the wrapped function to align with the original API call
    wrapped_func.__signature__ = sig.replace(parameters=params)
    wrapped_func.__annotations__ = annotations

    return Tool.from_function(wrapped_func, name=func.__name__, description=func.__doc__)
