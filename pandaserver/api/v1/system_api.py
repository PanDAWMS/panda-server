from typing import Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import generate_response, request_validation
from pandaserver.config import panda_config
from pandaserver.srvcore.panda_request import PandaRequest

_logger = PandaLogger().getLogger("system_api")


@request_validation(_logger, request_method="GET")
def get_attributes(req: PandaRequest, **kwargs: dict) -> Dict:
    """
    Get attributes

    Gets all parameters and environment variables.

    API details:
        HTTP Method: GET
        Path: /system/v1/get_attributes

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        **kwargs(dict): arbitrary keyword parameters that will be printed out

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the message field contains a string with all the attributes,
              and the data field contains the dictionary representation.
    """

    # Add the parameters
    parameter_dictionary = {key: value for key, value in sorted(kwargs.items())}
    parameter_section = "===== param =====\n" + "\n".join(f"{key} = {value}" for key, value in parameter_dictionary.items())

    # Add the environment variables
    environment_dictionary = {key: req.subprocess_env[key] for key in sorted(req.subprocess_env)}
    environment_section = "\n====== env ======\n" + "\n".join(f"{key} : {value}" for key, value in environment_dictionary.items())

    # Combine sections
    text_representation = parameter_section + "\n" + environment_section + "\n"

    # Combine data in dictionary form
    combined_data = {"parameters": parameter_dictionary, "environment": environment_dictionary}

    return generate_response(True, text_representation, combined_data)


@request_validation(_logger, request_method="GET")
def get_voms_attributes(req: PandaRequest) -> Dict:
    """
    Get VOMS attributes

    Gets the VOMS attributes (i.e. the ones starting with GRST) in sorted order.

    API details:
        HTTP Method: GET
        Path: /system/v1/get_voms_attributes

    Args:
        req(PandaRequest): internally generated request object containing the env variables

    Returns:
        dict: The system response with text representation and dictionary of attributes.
              Example: `{"success": True, "message": "<formatted string>", "data": {"key": "value"}}`
    """
    tmp_logger = LogWrapper(_logger, "get_voms_attributes")
    tmp_logger.debug("Start")

    # Iterate over all the environment variables, keep only the ones related to GRST credentials (GRST: Grid Security Technology)
    attributes = {}
    for tmp_key, tmp_value in req.subprocess_env.items():
        if tmp_key.startswith("GRST_CRED_"):
            attributes[tmp_key] = tmp_value

    # Create a sorted text representation
    text_representation = "\n".join(f"{key} : {value}" for key, value in sorted(attributes.items()))

    tmp_logger.debug("Done")
    return generate_response(True, text_representation, attributes)


@request_validation(_logger, request_method="GET")
def get_https_endpoint(req: PandaRequest) -> Dict:
    """
    Get the HTTPS endpoint

    Gets the server name and port for HTTPS.

    API details:
        HTTP Method: GET
        Path: /system/v1/get_https_endpoint

    Args:
        req(PandaRequest): internally generated request object containing the env variables

    Returns:
        dict: The system response with the name of the endpoint
              Example: `{"success": True, "data": "server:port"}`
    """
    tmp_logger = LogWrapper(_logger, "get_https_endpoint")
    tmp_logger.debug("Start")

    try:
        endpoint = f"{panda_config.pserverhost}:{panda_config.pserverport}"
    except Exception as e:
        tmp_logger.error(f"Failed to get HTTPS endpoint: {str(e)}")
        return generate_response(False, str(e))

    tmp_logger.debug("Done")
    return generate_response(True, data=endpoint)


@request_validation(_logger, request_method="GET")
def get_http_endpoint(req: PandaRequest) -> Dict:
    """
    Get the HTTP endpoint

    Gets the server name and port for HTTP.

    API details:
        HTTP Method: GET
        Path: /system/v1/get_http_endpoint

    Args:
        req(PandaRequest): internally generated request object containing the env variables

    Returns:
        dict: The system response with the name of the endpoint
              Example: `{"success": True, "data": "server:port"}`
    """
    tmp_logger = LogWrapper(_logger, "get_http_endpoint")
    tmp_logger.debug("Start")

    try:
        endpoint = f"{panda_config.pserverhosthttp}:{panda_config.pserverporthttp}"
    except Exception as e:
        tmp_logger.error(f"Failed to get HTTPS endpoint: {str(e)}")
        return generate_response(False, str(e))

    tmp_logger.debug("Done")
    return generate_response(True, data=endpoint)


@request_validation(_logger, request_method="GET")
def is_alive(req: PandaRequest) -> Dict:
    """
    Is alive

    Check if the server is alive. Basic function for the health check used in SLS monitoring.

    API details:
        HTTP Method: GET
        Path: /system/v1/is_alive

    Args:
        req(PandaRequest): internally generated request object containing the env variables

    Returns:
        dict: The system response with the name of the endpoint
              Example: `{"success": True}`
    """
    tmp_logger = LogWrapper(_logger, "is_alive")
    tmp_logger.debug("Start")
    tmp_logger.debug("Done")

    return generate_response(True)
