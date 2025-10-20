import ast
import inspect
import re
import sys
import threading
import time
import typing
from functools import wraps
from types import ModuleType, UnionType
from typing import Union, get_args, get_origin

from pandacommon.pandalogger.LogWrapper import LogWrapper

import pandaserver.jobdispatcher.Protocol as Protocol
from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.srvcore import CoreUtils

TIME_OUT = "TimeOut"

MESSAGE_SSL = "SSL secure connection is required"
MESSAGE_PROD_ROLE = "production or pilot role required"
MESSAGE_TASK_ID = "jediTaskID must be an integer"
MESSAGE_DATABASE = "database error in the PanDA server"
MESSAGE_JSON = "failed to load JSON"


def get_endpoint(protocol):
    if protocol not in ["http", "https"]:
        return False, "Protocol must be either 'http' or 'https'"

    try:
        if protocol == "https":
            endpoint = f"{panda_config.pserverhost}:{panda_config.pserverport}"
        else:
            endpoint = f"{panda_config.pserverhosthttp}:{panda_config.pserverporthttp}"
    except Exception as e:
        return False, str(e)

    return True, endpoint


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


def get_email_address(user, tmp_logger):
    tmp_logger.debug(f"Getting mail address for {user}")
    n_tries = 3
    email = None
    try:
        for attempt in range(n_tries):
            status, user_info = rucioAPI.finger(user)
            if status:
                email = user_info["email"]
                tmp_logger.debug(f"User {user} got email {email}")
                break
            else:
                tmp_logger.debug(f"Attempt {attempt + 1} of {n_tries} failed. Retrying...")
            time.sleep(1)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        tmp_logger.error(f"Failed to convert email address {user} : {error_type} {error_value}")

    return email


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


def extract_production_working_groups(fqans):
    # Extract working groups with production role from FQANs
    wg_prod_roles = []
    for fqan in fqans:
        # Match FQANs with 'Role=production' and extract the working group
        match = re.search(r"/atlas/([^/]+)/Role=production", fqan)
        if match:
            working_group = match.group(1)
            # Exclude 'usatlas' and ensure uniqueness
            if working_group and working_group not in ["usatlas"] + wg_prod_roles:
                wg_prod_roles.extend([working_group, f"gr_{working_group}"])  # Add group and prefixed variant

    return wg_prod_roles


def extract_primary_production_working_group(fqans):
    working_group = None
    for fqan in fqans:
        match = re.search("/[^/]+/([^/]+)/Role=production", fqan)
        if match:
            # ignore usatlas since it is used as atlas prod role
            tmp_working_group = match.group(1)
            if tmp_working_group not in ["", "usatlas"]:
                working_group = tmp_working_group.split("-")[-1].lower()

    return working_group


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


def normalize_type(t):
    mapping = {
        typing.List: list,
        typing.Dict: dict,
        typing.Set: set,
        typing.Tuple: tuple,
    }
    return mapping.get(t, t)


def request_validation(logger, secure=True, production=False, request_method=None):
    def decorator(func):
        @wraps(func)
        def wrapper(req, *args, **kwargs):
            # Generate a logger with the underlying function name
            tmp_logger = LogWrapper(logger, func.__name__)

            # check SSL if required
            if secure and not is_secure(req, tmp_logger):
                tmp_logger.error(f"{MESSAGE_SSL}")
                return generate_response(False, message=MESSAGE_SSL)

            # check production role if required
            if production and not has_production_role(req):
                tmp_logger.error(f"{MESSAGE_PROD_ROLE}")
                return generate_response(False, message=MESSAGE_PROD_ROLE)

            # check method if required
            if request_method and not validate_request_method(req, request_method):
                message = f"expecting {request_method}, received {req.subprocess_env.get('REQUEST_METHOD', None)}"
                tmp_logger.error(f"{message}")
                return generate_response(False, message=message)

            # Get function signature and type hints
            sig = inspect.signature(func)
            args_tmp = (req,) + args
            try:
                bound_args = sig.bind(*args_tmp, **kwargs)
            except TypeError as e:
                message = f"Argument error: {str(e)}"
                tmp_logger.error(message)
                return generate_response(False, message=message)
            bound_args.apply_defaults()

            for param_name, param_value in bound_args.arguments.items():
                # tmp_logger.debug(f"Got parameter '{param_name}' with value '{param_value}' and type '{type(param_value)}'")

                # Skip the first argument (req)
                if param_name == "req":
                    continue

                # Skip if no type hint
                expected_type = sig.parameters[param_name].annotation
                if expected_type is inspect.Parameter.empty:
                    continue

                # Skip if value is the default value
                default_value = sig.parameters[param_name].default
                if default_value == param_value:
                    continue

                # Handle generics like List[int]
                origin = get_origin(expected_type)
                args = get_args(expected_type)

                # GET methods are URL encoded. Parameters will lose the type and come as string. We need to cast them to the expected type
                if request_method == "GET":
                    try:
                        tmp_logger.debug(f"Casting '{param_name}' to type {expected_type.__name__}.")
                        tmp_logger.debug(type(param_value))
                        if param_value == "None" and default_value is None:
                            param_value = None
                        # Don't cast if the type is already a string
                        elif expected_type is str:
                            pass
                        # Booleans need to be handled separately, since bool("False") == True
                        elif expected_type is bool:
                            param_value = param_value.lower() in ("true", "1")
                        # Convert to float first, then to int. This is a courtesy for cases passing decimal numbers.
                        elif expected_type is int:
                            param_value = int(float(param_value))
                        elif origin is list and args:
                            element_type = args[0]  # Get the type inside List[<type>]

                            # If only one element, convert it to a list
                            if isinstance(param_value, str):
                                param_value = [param_value]

                            # Convert the elements of the list to the expected type
                            if element_type is int:
                                param_value = [int(float(i)) for i in param_value]  # Convert list items to int
                            elif element_type is float:
                                param_value = [float(i) for i in param_value]  # Convert list items to float
                            elif element_type is bool:
                                param_value = [i.lower() in ("true", "1") for i in param_value]  # Convert list items to bool
                        else:
                            # Normalize type, e.g. typing.Dict -> dict
                            expected_type = normalize_type(expected_type)
                            if not isinstance(param_value, expected_type):
                                param_value = ast.literal_eval(param_value)
                            if not isinstance(param_value, expected_type):
                                raise TypeError(f"Expected {expected_type}, received {type(param_value)}")
                        bound_args.arguments[param_name] = param_value  # Ensure the cast value is used
                    except (ValueError, TypeError):
                        message = f"Type error: '{param_name}' with value '{param_value}' could not be casted to type {expected_type.__name__} from {type(param_value).__name__}."
                        tmp_logger.error(message)
                        return generate_response(False, message=message)

                # Check type
                if origin and (origin is not Union and origin is not UnionType):  # Handle generics (e.g., List[int])
                    if not isinstance(param_value, origin):
                        message = f"Type error: '{param_name}' must be of type {origin.__name__}, got {type(param_value).__name__}."
                        tmp_logger.error(message)
                        return generate_response(False, message=message)

                    if args:  # Check inner types for lists, dicts, etc.
                        if origin is list and not all(isinstance(i, args[0]) for i in param_value):
                            message = f"Type error: All elements in '{param_name}' must be {args[0].__name__}."
                            tmp_logger.error(message)
                            return generate_response(False, message=message)
                elif not isinstance(param_value, expected_type) and not (param_value is None and param_value == default_value):
                    message = f"Type error: '{param_name}' must be of type {expected_type.__name__}, got {type(param_value).__name__}."
                    tmp_logger.error(message)
                    return generate_response(False, message=message)

            return func(*bound_args.args, **bound_args.kwargs)

        return wrapper

    return decorator


# a wrapper to install timeout into a method
class TimedMethod:
    def __init__(self, method, timeout):
        self.method = method
        self.timeout = timeout
        self.result = TIME_OUT

    # method emulation
    def __call__(self, *var, **kwargs):
        self.result = self.method(*var, **kwargs)

    # run
    def run(self, *var, **kwargs):
        thr = threading.Thread(target=self, args=var, kwargs=kwargs)
        thr.start()
        thr.join()
