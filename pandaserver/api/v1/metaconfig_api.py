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
global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """

    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True, request_method="GET")
def get_banned_users(req: PandaRequest) -> dict:
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
              When successful, the data field contains the banned users in the format `{"user1": False, "user2": False}`
    """
    tmp_logger = LogWrapper(_logger, f"get_banned_users")

    tmp_logger.debug("Start")
    success, users = global_task_buffer.get_ban_users()
    tmp_logger.debug("Done")
    return generate_response(success, data=users)


@request_validation(_logger, secure=False, request_method="GET")
def get_site_specs(req: PandaRequest, type: str = "analysis") -> dict:
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
              When successful, the data field contains a dictionary with the site data of the requested type.
    """

    tmp_logger = LogWrapper(_logger, f"get_site_specs")
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


@request_validation(_logger, secure=True, production=True, request_method="GET")
def get_resource_types(req: PandaRequest):
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
              When successful, the data field contains a list of resource types.
    """

    tmp_logger = LogWrapper(_logger, f"get_resource_types")
    tmp_logger.debug("Start")

    resource_types = global_task_buffer.getResourceTypes()

    # Didn't get any resource types
    if not resource_types:
        tmp_logger.debug("Done with error")
        return generate_response(False, MESSAGE_DATABASE)

    # Success
    tmp_logger.debug("Done")
    return generate_response(True, data=resource_types)
