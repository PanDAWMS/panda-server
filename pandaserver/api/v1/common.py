import inspect
import json
import re
from functools import wraps
from types import ModuleType
from typing import get_args, get_origin

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


def validate_request_method(req, expected_method):
    environ = req.subprocess_env
    request_method = environ.get("REQUEST_METHOD", None)  # GET, POST, PUT, DELETE
    if request_method and request_method == expected_method:
        return True
    return False


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


def request_validation(logger, secure=False, production=False, request_method=None):
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

            # check method if required
            if request_method and not validate_request_method(req, request_method):
                message = f"expecting {request_method}, received {req.subprocess_env.get('REQUEST_METHOD', None)}"
                logger.error(f"'{func.__name__}': {message}")
                return generate_response(False, message=message)

            # Get function signature and type hints
            sig = inspect.signature(func)
            args_tmp = (req,) + args
            bound_args = sig.bind(*args_tmp, **kwargs)
            bound_args.apply_defaults()

            for param_name, param_value in bound_args.arguments.items():
                logger.debug(f"Got parameter '{param_name}' with value '{param_value}' and type '{type(param_value)}'")

                # Skip if no type hint
                expected_type = sig.parameters[param_name].annotation
                if expected_type is inspect.Parameter.empty:
                    continue

                # Skip if value is the default value
                default_value = sig.parameters[param_name].default
                if default_value == param_value:
                    continue

                # GET methods are URL encoded. Parameters will lose the type and come as string. We need to cast them to the expected type
                if request_method == "GET":
                    try:
                        logger.debug(f"Casting '{param_name}' to type {expected_type.__name__}.")
                        param_value = expected_type(param_value)
                    except (ValueError, TypeError):
                        message = f"Type error: '{param_name}' could not be casted to type {expected_type.__name__}."
                        logger.error(message)
                        return generate_response(False, message=message)

                # Handle generics like List[int]
                origin = get_origin(expected_type)
                args = get_args(expected_type)

                # Check type
                if origin:  # Handle generics (e.g., List[int])
                    if not isinstance(param_value, origin):
                        message = f"Type error: '{param_name}' must be of type {origin.__name__}, got {type(param_value).__name__}."
                        logger.error(message)
                        return generate_response(False, message=message)

                    if args:  # Check inner types for lists, dicts, etc.
                        if origin is list and not all(isinstance(i, args[0]) for i in param_value):
                            message = f"Type error: All elements in '{param_name}' must be {args[0].__name__}."
                            logger.error(message)
                            return generate_response(False, message=message)
                else:
                    if not isinstance(param_value, expected_type):
                        message = f"Type error: '{param_name}' must be of type {expected_type.__name__}, got {type(param_value).__name__}."
                        logger.error(message)
                        return generate_response(False, message=message)

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
