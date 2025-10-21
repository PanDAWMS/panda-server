from typing import Dict

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    extract_primary_production_working_group,
    extract_production_working_groups,
    generate_response,
    get_dn,
    get_email_address,
    get_fqan,
    has_production_role,
    request_validation,
)
from pandaserver.config import panda_config
from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.srvcore.panda_request import PandaRequest

_logger = PandaLogger().getLogger("api_system")


@request_validation(_logger, secure=True, request_method="GET")
def get_attributes(req: PandaRequest, **kwargs: dict) -> Dict:
    """
    Get attributes

    Gets all parameters and environment variables.

    API details:
        HTTP Method: GET
        Path: /v1/system/get_attributes

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        **kwargs(dict): arbitrary keyword parameters that will be printed out

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the message field contains a string with all the attributes.
    """
    tmp_logger = LogWrapper(_logger, "get_attributes")
    tmp_logger.debug("Start")

    # Add the parameters
    parameter_dictionary = {key: str(value) for key, value in sorted(kwargs.items())}
    parameter_section = "===== param =====\n" + "\n".join(f"{key} = {value}" for key, value in parameter_dictionary.items())

    # Add the environment variables
    environment_dictionary = {key: str(req.subprocess_env[key]) for key in sorted(req.subprocess_env)}
    environment_section = "\n====== env ======\n" + "\n".join(f"{key} : {value}" for key, value in environment_dictionary.items())

    # Combine sections
    text_representation = parameter_section + "\n" + environment_section + "\n"

    # Combine data in dictionary form
    combined_data = {"parameters": parameter_dictionary, "environment": environment_dictionary}
    tmp_logger.debug("Done")
    return generate_response(True, text_representation, combined_data)


@request_validation(_logger, secure=True, request_method="GET")
def get_voms_attributes(req: PandaRequest) -> Dict:
    """
    Get VOMS attributes

    Gets the VOMS attributes (i.e. the ones starting with GRST) in sorted order. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/system/get_voms_attributes

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


@request_validation(_logger, secure=True, request_method="GET")
def get_user_attributes(req: PandaRequest) -> Dict:
    """
    Get user attributes

    Gets user attributes as seen by PanDA for debug purposes. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/system/get_user_attributes

    Args:
        req(PandaRequest): internally generated request object containing the env variables

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the message field contains a string with the user attributes,
              and the data field contains the dictionary representation.
    """
    tmp_logger = LogWrapper(_logger, "get_user_attributes")
    tmp_logger.debug("Start")

    # Raw user DN
    user_raw = req.subprocess_env["SSL_CLIENT_S_DN"]

    # Bare user DN
    user_bare = get_dn(req)

    # Clean user DN
    user_clean = clean_user_id(user_raw)

    # FQANs
    fqans = get_fqan(req)

    # Email address
    email_address = get_email_address(user_clean, tmp_logger)

    # Has production role
    is_production_user = has_production_role(req)

    # Production working groups
    production_working_groups = extract_production_working_groups(fqans)

    # Primary production working group
    primary_working_group = extract_primary_production_working_group(fqans)

    # Combine sections
    text_representation = (
        f"User DN (raw): {user_raw}\nUser DN (bare): {user_bare}\nUser DN (clean): {user_clean}\n"
        f"Email address: {email_address}\n"
        f"FQANs: {fqans}\nProduction user: {is_production_user}\n"
        f"Production working groups: {production_working_groups}\nPrimary production working group: {primary_working_group}\n"
    )
    dictionary_representation = {
        "user_dn_raw": user_raw,
        "user_dn_clean": user_clean,
        "fqans": fqans,
        "is_production_user": is_production_user,
        "production_working_groups": production_working_groups,
        "primary_working_group": primary_working_group,
    }

    tmp_logger.debug(text_representation)

    tmp_logger.debug("Done")
    return generate_response(True, text_representation, dictionary_representation)


@request_validation(_logger, secure=False, request_method="GET")
def is_alive(req: PandaRequest) -> Dict:
    """
    Is alive

    Check if the server is alive. Basic function for the health check used in SLS monitoring.

    API details:
        HTTP Method: GET
        Path: /v1/system/is_alive

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
