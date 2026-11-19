import ast
import collections.abc
import datetime
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
from pandaserver.srvcore import CoreUtils, task_command_log
from pandaserver.srvcore.CoreUtils import clean_user_id, shorten_for_log
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
MESSAGE_COMMAND_LOG = "failed to record the command in the task command log, so it was not run"

# names of the parameters in which an endpoint takes the task, or the list of tasks, it acts on.
# A POST endpoint of a module registered with log_task_commands is recorded when it has one
COMMAND_TASK_ID_PARAMETERS = ("task_id", "jedi_task_id", "task_id_list")
# largest JSON size, in bytes, of an argument recorded in the task command log. A larger one is
# recorded as {"omitted_bytes": size}
COMMAND_ARGUMENT_SIZE_LIMIT = 64 * 1024
# longest message, in characters, and largest JSON size of the data, in bytes, recorded from a response
COMMAND_RESPONSE_FIELD_LIMIT = 1024


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


def parse_utc_time(value: str) -> datetime.datetime:
    """
    Read a time given in ISO 8601, e.g. `2026-10-12 14:30:45`, as a naive datetime in UTC.

    Args:
        value(str): the time; without a UTC offset it is taken to be in UTC already

    Returns:
        datetime: the time in UTC without tzinfo, as the database columns hold it
    """
    parsed = datetime.datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed


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


# modules whose POST endpoints are recorded in the task command log, with what returns the
# TaskBuffer to record through
_command_logging_modules: dict[str, Callable[[], Any]] = {}


def log_task_commands(module_name: str, task_buffer: Callable[[], Any]) -> None:
    """
    Record the commands the POST endpoints of an API module receive in the task command log, TASKS_COMMANDLOG.

    request_validation records an endpoint of a registered module when the endpoint is called with
    POST and takes the tasks it acts on in a parameter named in COMMAND_TASK_ID_PARAMETERS. So a new
    endpoint is recorded without being listed anywhere.

    Args:
        module_name(str): __name__ of the API module
        task_buffer(callable): returns the TaskBuffer to record through, e.g. ``lambda: global_task_buffer``,
            so that it is resolved at request time rather than at import time when it may still be None
    """
    _command_logging_modules[module_name] = task_buffer


def get_command_task_ids(arguments: dict[str, Any]) -> list[int]:
    """
    Get the tasks a command acts on from the arguments of its endpoint.

    Args:
        arguments(dict): the bound arguments of the endpoint, by parameter name

    Returns:
        list: the jediTaskIDs from the first parameter named in COMMAND_TASK_ID_PARAMETERS, or an empty list when there is none
    """
    for name in COMMAND_TASK_ID_PARAMETERS:
        value = arguments.get(name)
        if value is None:
            continue
        if isinstance(value, (list, tuple, set, frozenset)):
            return [int(task_id) for task_id in value]
        return [int(value)]
    return []


def _encode_json_capped(value: Any, limit: int) -> Any:
    # the value itself when its JSON fits in limit bytes, else {"omitted_bytes": size}. A value
    # JSON cannot hold, e.g. NaN, which IS JSON (STRICT) rejects, is kept as its str
    try:
        encoded = json.dumps(value, default=str, allow_nan=False)
    except ValueError:
        value = str(value)
        encoded = json.dumps(value)
    size = len(encoded.encode())
    if size > limit:
        return {"omitted_bytes": size}
    return value


def encode_command_parameters(arguments: dict[str, Any]) -> str:
    """
    Encode the arguments of a task command for the task command log.

    Args:
        arguments(dict): the bound arguments of the endpoint, by parameter name

    Returns:
        str: JSON of the arguments other than req and the tasks, each replaced by
            {"omitted_bytes": size} when its JSON is over COMMAND_ARGUMENT_SIZE_LIMIT bytes
    """
    parameters = {
        name: _encode_json_capped(value, COMMAND_ARGUMENT_SIZE_LIMIT)
        for name, value in arguments.items()
        if name != "req" and name not in COMMAND_TASK_ID_PARAMETERS
    }
    return json.dumps(parameters, default=str)


def encode_command_response(response: Any) -> str:
    """
    Encode the response an endpoint returned for a task command for the task command log.

    Args:
        response(Any): what the endpoint returned, normally {"success", "message", "data"} from generate_response

    Returns:
        str: JSON of {"success", "message", "data"}, with the message cut to COMMAND_RESPONSE_FIELD_LIMIT
            characters and the data replaced by {"omitted_bytes": size} when its JSON is over
            COMMAND_RESPONSE_FIELD_LIMIT bytes. Anything else is kept as {"response": its str, cut}
    """
    if isinstance(response, dict) and "success" in response:
        message = response.get("message")
        encoded = {
            "success": response.get("success"),
            "message": None if message is None else str(message)[:COMMAND_RESPONSE_FIELD_LIMIT],
            "data": _encode_json_capped(response.get("data"), COMMAND_RESPONSE_FIELD_LIMIT),
        }
    else:
        encoded = {"response": str(response)[:COMMAND_RESPONSE_FIELD_LIMIT]}
    return json.dumps(encoded, default=str)


def encode_command_exception(exc: BaseException) -> str:
    """
    Encode the exception an endpoint raised instead of responding to a task command, for the task command log.

    Args:
        exc(Exception): the exception

    Returns:
        str: JSON of {"exception": "<type>: <message>"}, cut to COMMAND_RESPONSE_FIELD_LIMIT characters
    """
    return json.dumps({"exception": f"{type(exc).__name__}: {exc}"[:COMMAND_RESPONSE_FIELD_LIMIT]})


def get_client_address(req: PandaRequest) -> str | None:
    """
    Get the address of the client that sent a request.

    Args:
        req(PandaRequest): the request

    Returns:
        str or None: the client's host name when httpd looked it up (HostnameLookups), else its IP
            address, or None when neither is known. A proxy between the client and httpd gives its own
    """
    return req.get_remote_host() or req.subprocess_env.get("REMOTE_ADDR")


def run_logged_task_command(
    func: Callable[..., Any],
    bound_args: inspect.BoundArguments,
    req: PandaRequest,
    task_ids: list[int],
    task_buffer: Any,
    tmp_logger: LogWrapper,
) -> Any:
    """
    Run a task command and record it in the task command log, TASKS_COMMANDLOG.

    The rows are written as received before the command runs, and the command is refused when they
    cannot be written, so that every command that ran has its rows. While the command runs, the key
    of the rows is in task_command_log.current_task_command, where TaskBuffer reads it to mark the
    rows queued when the command is registered for JEDI. The response is recorded after; a failure
    to record it is only logged, since the command has run by then.

    Args:
        func(callable): the endpoint
        bound_args(inspect.BoundArguments): its arguments, validated and cast
        req(PandaRequest): the request
        task_ids(list): the tasks the command acts on
        task_buffer(TaskBuffer): the TaskBuffer to record through
        tmp_logger(LogWrapper): the logger of the endpoint

    Returns:
        Any: what the endpoint returned, or the response refusing the command
    """
    dn = get_dn(req)
    requester = clean_user_id(dn)
    if requester in ["", "NULL", None]:
        requester = dn
    try:
        command_key = task_buffer.insert_task_command(
            task_ids,
            func.__name__,
            encode_command_parameters(bound_args.arguments),
            requester,
            has_production_role(req),
            get_client_address(req),
        )
    except Exception as e:
        tmp_logger.error(f"failed to record the command with {type(e).__name__}: {e}")
        command_key = None
    if command_key is None:
        tmp_logger.error(MESSAGE_COMMAND_LOG)
        return generate_response(False, message=MESSAGE_COMMAND_LOG)
    tmp_logger.debug(f"recorded in the task command log as command_id={command_key['command_id']}")

    def record_response(encoded: str, is_error: bool) -> None:
        try:
            is_ok = task_buffer.set_task_command_response(command_key, encoded, is_error)
        except Exception as e:
            tmp_logger.error(f"failed to record the response with {type(e).__name__}: {e}")
            is_ok = False
        if not is_ok:
            tmp_logger.error(f"failed to record the response of command_id={command_key['command_id']} in the task command log")

    token = task_command_log.current_task_command.set(command_key)
    try:
        response = func(*bound_args.args, **bound_args.kwargs)
    except Exception as e:
        record_response(encode_command_exception(e), True)
        raise
    finally:
        task_command_log.current_task_command.reset(token)
    record_response(encode_command_response(response), False)
    return response


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

    For a module registered with log_task_commands, a POST request that names the tasks it acts on
    is also recorded in the task command log once it has passed validation; see run_logged_task_command.

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

            # log an error together with the request arguments. The context is only built when an error is logged,
            # and every argument is shortened so that large payloads do not flood the logs
            def log_error_with_context(message: str) -> None:
                args_str = ", ".join(shorten_for_log(arg) for arg in args)
                kwargs_str = ", ".join(f"{key}={shorten_for_log(value)}" for key, value in kwargs.items())
                LogWrapper(logger, f"{func.__name__} args:({args_str}) kwargs:{{{kwargs_str}}}").error(message)

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
                log_error_with_context(message)
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
                        message = f"Type error: '{param_name}' with value {shorten_for_log(param_value)} could not be casted to type {type_name(cast_type)} from {type(param_value).__name__}."
                        log_error_with_context(message)
                        return generate_response(False, message=message)

                # Check type
                if origin and (origin is not Union and origin is not UnionType):  # Handle generics (e.g., List[int])
                    if not isinstance(param_value, origin) and not (param_value is None and param_value == default_value):
                        message = f"Type error: '{param_name}' must be of type {type_name(origin)}, got {type(param_value).__name__}."
                        log_error_with_context(message)
                        return generate_response(False, message=message)

                    if type_args and param_value is not None:  # Check inner types for lists, dicts, etc.
                        if origin in SEQUENCE_ORIGINS and not all(isinstance(i, isinstance_types(type_args[0])) for i in param_value):
                            message = f"Type error: All elements in '{param_name}' must be {type_name(type_args[0])}."
                            log_error_with_context(message)
                            return generate_response(False, message=message)
                elif not isinstance(param_value, isinstance_types(expected_type)) and not (param_value is None and param_value == default_value):
                    message = f"Type error: '{param_name}' must be of type {type_name(expected_type)}, got {type(param_value).__name__}."
                    log_error_with_context(message)
                    return generate_response(False, message=message)

            # check task ownership if required
            if task_owner:
                task_id = bound_args.arguments.get(task_id_param)
                if task_id:
                    dn = get_dn(req)
                    prod_role = has_production_role(req)
                    resolved_buffer = task_buffer() if callable(task_buffer) else task_buffer
                    # task_owner without a task_buffer is a mistake in the decorator's arguments;
                    # refuse the request rather than fail on the missing buffer
                    if resolved_buffer is None:
                        message = "task ownership cannot be checked: request_validation has no task_buffer"
                        tmp_logger.error(message)
                        return generate_response(False, message=message)
                    if not resolved_buffer.validate_ownership_or_production_role(task_id, dn, prod_role):
                        tmp_logger.error(MESSAGE_TASK_OWNER)
                        return generate_response(False, message=MESSAGE_TASK_OWNER)

            # record a task command in the task command log while running it
            command_task_buffer = _command_logging_modules.get(func.__module__)
            if command_task_buffer is not None and expected_request_method == "POST":
                command_task_ids = get_command_task_ids(bound_args.arguments)
                if command_task_ids:
                    return run_logged_task_command(func, bound_args, req, command_task_ids, command_task_buffer(), tmp_logger)

            return func(*bound_args.args, **bound_args.kwargs)

        return wrapper

    return decorator


# a wrapper to install timeout into a method
class TimedMethod:
    # whatever the wrapped method returns, or the TIME_OUT token while it has not returned
    # NOTE: The timeout would have to be added to thr.join(), but has been ignored on purpose since 2009.
    #       It was moved to the ConBridge dbtimeout, which retries rather than fails.
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
