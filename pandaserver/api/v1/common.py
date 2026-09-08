import ast
import collections.abc
import inspect
import json
import logging
import re
import sys
import threading
import time
import typing
from collections.abc import Callable
from functools import wraps
from types import ModuleType, UnionType
from typing import Any, Union, get_args, get_origin

from pandacommon.pandalogger.LogWrapper import LogWrapper

import pandaserver.jobdispatcher.Protocol as Protocol
from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import (
    STRUCTURED_RESULT_KEY,
)

TIME_OUT = "TimeOut"

MESSAGE_SSL = "SSL secure connection is required"
MESSAGE_PROD_ROLE = "production or pilot role required"
MESSAGE_TASK_OWNER = "not the task owner or no production role"
MESSAGE_TASK_ID = "jediTaskID must be an integer"
MESSAGE_DATABASE = "database error in the PanDA server"
MESSAGE_JSON = "failed to load JSON"


def get_endpoint(protocol: str) -> tuple[bool, str]:
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


def extract_allowed_methods(module: ModuleType) -> list[str]:
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


def generate_response(success: bool, message: str | None = "", data: Any = None) -> dict[str, Any]:
    response = {"success": success, "message": message, "data": data}
    return response


# get FQANs
def get_fqan(req: PandaRequest) -> list[str]:
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


# the address rucio has on file for the user, or None when the lookup did not answer.
# clean_user_id, which is where every caller's user comes from, can hand back None; the
# except below is what turns that into the same answer
def get_email_address(user: str | None, tmp_logger: LogWrapper) -> str | None:
    if user is None:
        # clean_user_id answers None for a DN it could not read, and there is nothing to
        # ask rucio about; the callers already treat a missing address as no address
        tmp_logger.debug("No user to get a mail address for")
        return None
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


def get_request_method(req: PandaRequest) -> str | None:
    # Extract the http method like GET, POST, ... from the request environment
    environ = req.subprocess_env
    request_method = environ.get("REQUEST_METHOD", None)  # GET, POST, PUT, DELETE
    return request_method


# get DN
def get_dn(req: PandaRequest) -> str:
    real_dn = ""
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        # remove redundant CN
        real_dn = CoreUtils.get_bare_dn(req.subprocess_env["SSL_CLIENT_S_DN"], keep_proxy=True)
    return real_dn


# check role
def has_production_role(req: PandaRequest) -> bool:
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


# valid access levels for reading back async request results
ACCESS_LEVELS = ("owner", "production", "anyone")

# Annotations whose values arrive from a URL as one string per element, and whose elements
# are checked one by one. A str is itself a Sequence, so leaving Sequence out of this would
# let a single GET value through as the string rather than as a one-element list.
SEQUENCE_ORIGINS = (list, tuple, set, frozenset, collections.abc.Sequence)


def set_owner_info(parameters: dict[str, Any], req: PandaRequest, access: str = "owner", structured_result: bool = False) -> dict[str, Any]:
    """
    Embed the requester, access level and result format into an async request's parameters dict.
    Used by the endpoints submitting async requests when building parameters_json.

    Args:
        parameters(dict): the request's parameters dict to be augmented in place
        req(PandaRequest): request object, used to derive the requester's compact DN
        access(str): access level controlling who may read results; one of
            "owner", "production", "anyone" (default "owner")
        structured_result(bool): True when the handler stores a {"success", "message", "data"}
            payload rather than raw output, which makes get_result report that payload at the
            top level of its response instead of the per-machine shape (default False)

    Returns:
        dict: the same parameters dict, with "requester", "access" and, when asked for,
            "structured_result" set
    """
    parameters["requester"] = clean_user_id(get_dn(req))
    parameters["access"] = access
    if structured_result:
        parameters[STRUCTURED_RESULT_KEY] = True
    return parameters


def is_authorized_to_read(req: PandaRequest, req_row: dict[str, Any]) -> tuple[bool, str]:
    """
    Authorize the caller to read an async request's results based on its access level.

    Args:
        req(PandaRequest): request object, used to derive the caller's compact DN
            and (for the "production" level) the production role
        req_row(dict): the row dict from TaskBuffer.get_async_request();
            only req_row["parameters"] (the JSON holding requester/access) is used

    Returns:
        tuple[bool, str]: (authorized, message)
    """
    caller = clean_user_id(get_dn(req))
    try:
        params = json.loads(req_row["parameters"] or "{}")
    except json.JSONDecodeError:
        params = {}
    requester = params.get("requester")
    access = params.get("access", "owner")
    if access == "owner":
        authorized = caller == requester
    elif access == "production":
        authorized = caller == requester or has_production_role(req)
    else:  # "anyone"; any unknown value falls through to not authorized
        authorized = access == "anyone"
    if not authorized:
        return False, f"'{caller}' is not authorized to read results (access='{access}', requester='{requester}')"
    return True, f"'{caller}' is authorized (access='{access}')"


def extract_production_working_groups(fqans: list[str]) -> list[str]:
    # Extract working groups with production role from FQANs
    wg_prod_roles: list[str] = []
    for fqan in fqans:
        # Match FQANs with 'Role=production' and extract the working group
        match = re.search(r"/atlas/([^/]+)/Role=production", fqan)
        if match:
            working_group = match.group(1)
            # Exclude 'usatlas' and ensure uniqueness
            if working_group and working_group not in ["usatlas"] + wg_prod_roles:
                wg_prod_roles.extend([working_group, f"gr_{working_group}"])  # Add group and prefixed variant

    return wg_prod_roles


def extract_primary_production_working_group(fqans: list[str]) -> str | None:
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
def is_secure(req: PandaRequest, logger: LogWrapper | None = None) -> bool:
    # check security
    if not Protocol.isSecure(req):
        return False

    # disable limited proxy
    if "/CN=limited proxy" in req.subprocess_env["SSL_CLIENT_S_DN"]:
        if logger:
            logger.warning(f"access via limited proxy : {req.subprocess_env['SSL_CLIENT_S_DN']}")
        return False

    return True


def normalize_type(t: Any) -> Any:
    mapping: dict[Any, Any] = {
        typing.List: list,
        typing.Dict: dict,
        typing.Set: set,
        typing.Tuple: tuple,
    }
    return mapping.get(t, t)


def type_name(expected_type: Any) -> str:
    """
    Name of an annotation, for a log line or an error message.

    A PEP 604 union such as `int | None` has no __name__ at all, so reading it directly
    raises AttributeError -- and the casting block in request_validation catches only
    ValueError and TypeError, so that AttributeError leaves the decorator and the endpoint
    answers a 500 instead of a response.
    """
    return getattr(expected_type, "__name__", None) or str(expected_type)


def isinstance_types(expected_type: Any) -> tuple[Any, ...]:
    """
    The classes isinstance can be called with for an annotation.

    isinstance refuses a typing.Union and a subscripted generic, so a union becomes its
    members and a generic becomes its origin: `List[str] | None` checks as (list, NoneType)
    and whether the elements are str is a separate test. Any becomes object, which every
    value satisfies -- the alternative is isinstance raising on it.
    """
    if expected_type is Any:
        return (object,)
    origin = get_origin(expected_type)
    if origin is Union or origin is UnionType:
        return tuple(t for member in get_args(expected_type) for t in isinstance_types(member))
    return (origin or expected_type,)


def request_validation(
    logger: logging.Logger,
    secure: bool = True,
    production: bool = False,
    request_method: str | None = None,
    task_owner: bool = False,
    task_buffer: Any = None,
    task_id_param: str = "task_id",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator that validates an incoming API request before the handler runs.

    Args:
        logger: Logger instance passed to LogWrapper for the decorated function.
        secure(bool): If True, requires an SSL connection without a limited proxy. Defaults to True.
        production(bool): If True, requires the caller to have a production role. Defaults to False.
        request_method(str): If set, requires the HTTP method to match (e.g. "GET", "POST"). Defaults to None (any method).
        task_owner(bool): If True, requires the caller to be the task owner or have a production role.
                          The task ID is read from the parameter named by task_id_param after type casting.
                          Requires task_buffer to be provided. Defaults to False.
        task_buffer(callable or TaskBuffer): Used when task_owner=True to call validate_task_permissions.
                          Pass as a lambda (e.g. ``lambda: global_task_buffer``) so it is resolved at
                          request time rather than at import time when it may still be None.
        task_id_param(str): Name of the task ID parameter in the decorated function's signature.
                          Defaults to "task_id". Override to "jedi_task_id" for endpoints that use that name.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        # answers whatever the wrapped endpoint answers, or the response saying why the
        # request was refused before it ran
        def wrapper(req: PandaRequest, *args: Any, **kwargs: Any) -> Any:
            # Generate a logger with the underlying function name
            tmp_logger = LogWrapper(logger, func.__name__)
            tmp_logger_context = LogWrapper(logger, f"{func.__name__} args:{args} kwargs:{kwargs}")

            # expected and received request methods
            expected_request_method = request_method
            received_request_method = get_request_method(req)

            # check SSL if required
            if secure and not is_secure(req, tmp_logger):
                tmp_logger.error(f"{MESSAGE_SSL}")
                return generate_response(False, message=MESSAGE_SSL)

            # check production role if required
            if production and not has_production_role(req):
                tmp_logger.error(f"{MESSAGE_PROD_ROLE}")
                return generate_response(False, message=MESSAGE_PROD_ROLE)

            # check method if required
            if expected_request_method and expected_request_method != received_request_method:
                message = f"expecting {expected_request_method}, received {req.subprocess_env.get('REQUEST_METHOD', None)}"
                tmp_logger.error(f"{message}")
                return generate_response(False, message=message)

            # Get function signature and type hints
            sig = inspect.signature(func)
            args_tmp = (req,) + args
            try:
                bound_args = sig.bind(*args_tmp, **kwargs)
            except TypeError as e:
                message = f"Argument error: {str(e)}"
                tmp_logger_context.error(message)
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

                # Handle generics like List[int]. Named type_args rather than args because
                # the enclosing wrapper's own *args is still in scope here
                origin = get_origin(expected_type)
                type_args = get_args(expected_type)

                # An optional parameter is annotated `X | None` or Optional[X], and both
                # the casting and the check below work on X: the casting compares the
                # annotation against str/bool/int by identity, and a union is neither. So
                # look through a union with one non-None member to that member. The check
                # further down keeps expected_type, where None still has to be accepted.
                cast_type = expected_type
                if origin is Union or origin is UnionType:
                    non_none_args = [a for a in type_args if a is not type(None)]
                    if len(non_none_args) == 1:
                        cast_type = non_none_args[0]
                        origin = get_origin(cast_type)
                        type_args = get_args(cast_type)

                # GET methods are URL encoded. Parameters will lose the type and come as string. We need to cast them to the expected type
                if received_request_method == "GET":
                    try:
                        tmp_logger.debug(f"Casting '{param_name}' to type {type_name(cast_type)}.")
                        tmp_logger.debug(type(param_value))
                        if param_value == "None" and default_value is None:
                            param_value = None
                        # Don't cast if the type is already a string
                        elif cast_type is str:
                            pass
                        # Booleans need to be handled separately, since bool("False") == True
                        elif cast_type is bool:
                            param_value = param_value.lower() in ("true", "1")
                        # Convert to float first, then to int. This is a courtesy for cases passing decimal numbers.
                        elif cast_type is int:
                            param_value = int(float(param_value))
                        elif origin in SEQUENCE_ORIGINS and type_args:
                            element_type = type_args[0]  # Get the type inside List[<type>]

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
                            cast_type = normalize_type(cast_type)
                            if not isinstance(param_value, isinstance_types(cast_type)):
                                param_value = ast.literal_eval(param_value)
                            if not isinstance(param_value, isinstance_types(cast_type)):
                                raise TypeError(f"Expected {cast_type}, received {type(param_value)}")
                        bound_args.arguments[param_name] = param_value  # Ensure the cast value is used
                    except (ValueError, TypeError):
                        message = f"Type error: '{param_name}' with value '{param_value}' could not be casted to type {type_name(cast_type)} from {type(param_value).__name__}."
                        tmp_logger_context.error(message)
                        return generate_response(False, message=message)

                # Check type
                if origin and (origin is not Union and origin is not UnionType):  # Handle generics (e.g., List[int])
                    if not isinstance(param_value, origin) and not (param_value is None and param_value == default_value):
                        message = f"Type error: '{param_name}' must be of type {type_name(origin)}, got {type(param_value).__name__}."
                        tmp_logger_context.error(message)
                        return generate_response(False, message=message)

                    if type_args and param_value is not None:  # Check inner types for lists, dicts, etc.
                        if origin in SEQUENCE_ORIGINS and not all(isinstance(i, isinstance_types(type_args[0])) for i in param_value):
                            message = f"Type error: All elements in '{param_name}' must be {type_name(type_args[0])}."
                            tmp_logger_context.error(message)
                            return generate_response(False, message=message)
                elif not isinstance(param_value, isinstance_types(expected_type)) and not (param_value is None and param_value == default_value):
                    message = f"Type error: '{param_name}' must be of type {type_name(expected_type)}, got {type(param_value).__name__}."
                    tmp_logger_context.error(message)
                    return generate_response(False, message=message)

            # check task ownership if required
            if task_owner:
                task_id = bound_args.arguments.get(task_id_param)
                if task_id:
                    dn = get_dn(req)
                    prod_role = has_production_role(req)
                    resolved_buffer = task_buffer() if callable(task_buffer) else task_buffer
                    if not resolved_buffer.validate_ownership_or_production_role(task_id, dn, prod_role):
                        tmp_logger.error(MESSAGE_TASK_OWNER)
                        return generate_response(False, message=MESSAGE_TASK_OWNER)

            return func(*bound_args.args, **bound_args.kwargs)

        return wrapper

    return decorator


# a wrapper to install timeout into a method
class TimedMethod:
    # whatever the wrapped method returns, or the TIME_OUT token while it has not returned
    result: typing.Any

    def __init__(self, method: Callable[..., Any], timeout: int | None) -> None:
        self.method = method
        # kept for the callers that pass one, but not read: run() below joins without a
        # timeout. 27f8bc38 made that change on purpose in 2009, moving the timeout to the
        # DB proxy in the same commit. None is what pilot update_job passes for a job going
        # to holding, which it documents as being updated without a timeout
        self.timeout = timeout
        self.result = TIME_OUT

    # method emulation
    def __call__(self, *var: Any, **kwargs: Any) -> None:
        self.result = self.method(*var, **kwargs)

    # run
    def run(self, *var: Any, **kwargs: Any) -> None:
        thr = threading.Thread(target=self, args=var, kwargs=kwargs)
        thr.start()
        thr.join()
