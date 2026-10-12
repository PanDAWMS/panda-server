from typing import Any

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    MESSAGE_DATABASE,
    generate_response,
    request_validation,
)
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_metaconfig")

# These global variables are initialized in the init_task_buffer method
# Installed by init_task_buffer() before any handler runs, so these are declared
# non-Optional for the same reason as BaseModule.conn/cur: an Optional type would
# only push a None check onto every handler without making any of them safer.
global_task_buffer: TaskBuffer = None  # type: ignore[assignment]


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """

    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True, request_method="GET")
def get_banned_users(req: PandaRequest) -> dict[str, Any]:
    """
    Get banned users

    Gets the list of banned users from the system (users with `status=disabled` in ATLAS_PANDAMETA.users). Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/metaconfig/get_banned_users

    Args:
        req(PandaRequest): internally generated request object

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.

    Response data:
        dict: The disabled users, keyed by user name, each with the value false.
        On failure: null.
    """
    tmp_logger = LogWrapper(_logger, "get_banned_users")

    tmp_logger.debug("Start")
    success, users = global_task_buffer.get_ban_users()
    tmp_logger.debug("Done")
    return generate_response(success, data=users)


@request_validation(_logger, secure=False, request_method="GET")
def get_site_specs(req: PandaRequest, type: str = "analysis") -> dict[str, Any]:
    """
    Get site specs

    Gets a dictionary of site specs. By default `analysis` sites are returned. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/metaconfig/get_site_specs

    Args:
        req(PandaRequest): internally generated request object
        type(str, optional): type of site as defined in CRIC (currently `unified`, `production`, `analysis`, `all`). Defaults to `analysis`.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.

    Response data:
        dict: The specifications of the sites of the requested type, keyed by site name. Each value
            holds the site's SiteSpec attributes, without the DDM endpoint and slot details.
    """

    tmp_logger = LogWrapper(_logger, "get_site_specs")
    tmp_logger.debug("Start")

    site_specs = {}
    site_mapper = SiteMapper(global_task_buffer)

    excluded_attrs = {"ddm_endpoints_input", "ddm_endpoints_output", "ddm_input", "ddm_output", "setokens_input", "num_slots_map"}

    for site_id, site_spec in site_mapper.siteSpecList.items():
        if type == "all" or site_spec.type == type:
            # Convert site_spec attributes to a dictionary, excluding specific attributes
            site_specs[site_id] = {attr: value for attr, value in vars(site_spec).items() if attr not in excluded_attrs}

    tmp_logger.debug("Done")
    return generate_response(True, data=site_specs)


@request_validation(_logger, secure=True, request_method="GET")
def get_resource_types(req: PandaRequest) -> dict[str, Any]:
    """
    Get resource types

    Gets the resource types (`SCORE`, `MCORE`, etc.) together with their definitions. Requires a secure connection and production role.

    API details:
        HTTP Method: GET
        Path: /v1/metaconfig/get_resource_types

    Args:
        req(PandaRequest): Internally generated request object containing the environment variables.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.

    Response data:
        list[dict]: The resource types with their core count and memory-per-core limits.
            {"resource_name": str, "mincore": int, "maxcore": int, "minrampercore": int, "maxrampercore": int}
    """

    tmp_logger = LogWrapper(_logger, "get_resource_types")
    tmp_logger.debug("Start")

    resource_types = global_task_buffer.getResourceTypes()

    # Didn't get any resource types
    if not resource_types:
        tmp_logger.debug("Done with error")
        return generate_response(False, MESSAGE_DATABASE)

    # Success
    tmp_logger.debug("Done")
    return generate_response(True, data=resource_types)
