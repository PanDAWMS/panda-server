import inspect
import json
import re
from functools import wraps
from types import ModuleType

import pandaserver.jobdispatcher.Protocol as Protocol
from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils

MESSAGE_SSL = "SSL secure connection is required"
MESSAGE_PROD_ROLE = "production or pilot role required"
MESSAGE_TASK_ID = "jediTaskID must be an integer"
MESSAGE_DATABASE = "database error in the PanDA server"
MESSAGE_JSON = "failed to load JSON"


# get FQANs
def get_fqan(req):
    fqans = []
    for tmp_key in req.subprocess_env:
        tmp_value = req.subprocess_env[tmp_key]
        # Scan VOMS attributes
        # compact style
        if tmp_key.startswith("GRST_CRED_") and tmp_value.startswith("VOMS"):
            fqan = tmp_value.split()[-1]
            fqans.append(fqan)

        # old style
        elif tmp_key.startswith("GRST_CONN_"):
            tmp_items = tmp_value.split(":")
            if len(tmp_items) == 2 and tmp_items[0] == "fqan":
                fqans.append(tmp_items[-1])

    return fqans


# get DN
def get_dn(req):
    real_dn = ""
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        # remove redundant CN
        real_dn = CoreUtils.get_bare_dn(req.subprocess_env["SSL_CLIENT_S_DN"], keep_proxy=True)
    return real_dn


# check role
def has_production_role(req):
    # check DN
    user = get_dn(req)
    for sdn in panda_config.production_dns:
        if sdn in user:
            return True
    # get FQANs
    fqans = get_fqan(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in [
            "/atlas/usatlas/Role=production",
            "/atlas/Role=production",
            "^/[^/]+/Role=production",
        ]:
            if fqan.startswith(rolePat):
                return True
            if re.search(rolePat, fqan):
                return True
    return False


# get primary working group with prod role
def get_production_working_groups(req):
    try:
        fqans = get_fqan(req)
        for fqan in fqans:
            tmp_match = re.search("/[^/]+/([^/]+)/Role=production", fqan)
            if tmp_match is not None:
                # ignore usatlas since it is used as atlas prod role
                tmp_working_group = tmp_match.group(1)
                if tmp_working_group not in ["", "usatlas"]:
                    return tmp_working_group.split("-")[-1].lower()
    except Exception:
        pass
    return None


# security check
def is_secure(req, logger=None):
    # check security
    if not Protocol.isSecure(req):
        return False

    # disable limited proxy
    if "/CN=limited proxy" in req.subprocess_env["SSL_CLIENT_S_DN"]:
        if logger:
            logger.warning(f"access via limited proxy : {req.subprocess_env['SSL_CLIENT_S_DN']}")
        return False

    return True


def request_validation(logger, secure=False, production=False):
    def decorator(func):
        @wraps(func)
        def wrapper(req, *args, **kwargs):
            # check SSL if required
            if secure and not is_secure(req, logger):
                logger.error(f"'{func.__name__}': {MESSAGE_SSL}")
                return generate_response(False, message=MESSAGE_SSL)

            # check production role if required
            if production and not has_production_role(req):
                logger.error(f"'{func.__name__}': {MESSAGE_PROD_ROLE}")
                return generate_response(False, message=MESSAGE_PROD_ROLE)

            return func(req, *args, **kwargs)

        return wrapper

    return decorator


def validate_types(type_mapping, logger=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for param, expected_type in type_mapping.items():
                # Find the argument either in args or kwargs
                if param in kwargs:
                    value = kwargs[param]
                else:
                    arg_names = func.__code__.co_varnames
                    param_index = arg_names.index(param)
                    value = args[param_index]

                try:
                    converted_value = expected_type(value)
                except (ValueError, TypeError):
                    error_message = f"{param} must be {expected_type.__name__}"
                    if logger:
                        logger.error(f"'{func.__name__}': {error_message}")
                    return json.dumps((False, error_message))

                if param in kwargs:
                    kwargs[param] = converted_value
                else:
                    args = list(args)
                    args[param_index] = converted_value

            return func(*args, **kwargs)

        return wrapper

    return decorator


def extract_allowed_methods(module: ModuleType) -> list:
    """
    Generate the allowed methods dynamically with all function names present in the API module, excluding
    functions imported from other modules or the init_task_buffer function

    :param module: The module to extract the allowed methods from
    :return: A list of allowed method names
    """
    return [
        name
        for name, obj in inspect.getmembers(module, inspect.isfunction)
        if obj.__module__ == module.__name__ and name != "init_task_buffer" and name.startswith("_") is False
    ]


def generate_response(success, message="", data=None):
    response = {"success": success, "message": message, "data": data}
    return response
