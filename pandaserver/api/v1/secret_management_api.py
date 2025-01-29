import datetime
import os
import socket

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import generate_response, get_dn, request_validation
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("secret_management_api")

global_task_buffer = None
global_dispatch_parameter_cache = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer

    global global_dispatch_parameter_cache
    global_dispatch_parameter_cache = CoreUtils.CachedObject("dispatcher_params", 60 * 10, get_dispatch_parameters, _logger)


def get_dispatch_parameters():
    """
    Wrapper function around taskBuffer.get_special_dispatch_params to convert list to set since task buffer cannot return set
    """
    parameters = global_task_buffer.get_special_dispatch_params()
    for client_name, client_data in parameters["tokenKeys"].items():
        client_data["fullList"] = set(client_data["fullList"])

    return True, parameters


@request_validation(_logger, secure=True, request_method="POST")
def set_user_secrets(req: PandaRequest, key: str = None, value: str = None):
    """
    TODO
    """
    tmp_log = LogWrapper(_logger, f"set_user_secret-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")

    # Get client DN and clean it. The request validation has already verified that the DN is present.
    dn = get_dn(req)
    owner = clean_user_id(dn)
    tmp_log.debug(f"Setting secret for client={owner}")
    success, message = global_task_buffer.set_user_secret(owner, key, value)
    return generate_response(success, message)


@request_validation(_logger, secure=True, request_method="GET")
def get_user_secrets(req: PandaRequest, keys: str = None):
    """
    keys: list of comma separated keys
    """
    tmp_log = LogWrapper(_logger, f"get_user_secrets-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")

    # Get client DN and clean it. The request validation has already verified that the DN is present.
    dn = get_dn(req)
    owner = clean_user_id(dn)
    tmp_log.debug(f"Returning secret for client={owner}")
    success, data_or_message = global_task_buffer.get_user_secrets(owner, keys)
    message, data = "", {}
    if success:
        data = data_or_message
    else:
        message = data_or_message

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, request_method="POST")
def get_key_pair(req: PandaRequest, public_key_name: str, private_key_name: str):
    """
    This function retrieves the distinguished name (DN) from the request and uses it to get a key pair.

    Args:
        req: The request object containing the environment variables.
        public_key_name (str): The name of the public key.
        private_key_name (str): The name of the private key.

    Returns:
        dict: The key pair for the user.
    """
    tmp_logger = LogWrapper(_logger, f"get_key_pair {public_key_name}/{private_key_name}")

    real_dn = get_dn(req)

    # get compact DN
    compact_dn = clean_user_id(real_dn)

    # check permission
    global_dispatch_parameter_cache.update()
    allowed_keys = global_dispatch_parameter_cache.get("allow_keyPair", [])
    if compact_dn not in allowed_keys:
        # permission denied
        tmp_msg = f"failed since '{compact_dn}' not authorized with 'k' in {panda_config.schemaMETA}.USERS.GRIDPREF"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # retrieve the key pairs
    key_pair_dict = global_dispatch_parameter_cache.get("keyPair", {})

    if public_key_name not in key_pair_dict:  # public key is missing
        tmp_msg = f"failed for '{compact_dn}' since {public_key_name} is missing on {socket.getfqdn()}"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    if private_key_name not in key_pair_dict:  # private key is missing
        tmp_msg = f"failed for '{compact_dn}' since {private_key_name} is missing on {socket.getfqdn()}"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # key pair is available
    data = {"public_key": key_pair_dict[public_key_name], "private_key": key_pair_dict[private_key_name]}
    tmp_logger.debug(f"sent key-pair to '{compact_dn}'")

    return generate_response(True, data=data)


@request_validation(_logger, secure=True, request_method="GET")
def get_token_key(req: PandaRequest, client_name: str):
    """
    This function retrieves the distinguished name (DN) from the request and uses it to get a token key for the specified client.

    Args:
        req: The request object containing the environment variables.
        client_name (str): The name of the client requesting the token key.
    Returns:
        str: The token key for the client.
    """
    tmp_log = LogWrapper(_logger, f"get_token_key client={client_name} PID={os.getpid()}")

    # get DN and clean it
    real_dn = get_dn(req)
    compact_name = clean_user_id(real_dn)

    global_dispatch_parameter_cache.update()

    # check if user is allowed to retrieved token keys
    allowed_users = global_dispatch_parameter_cache.get("allowTokenKey", [])
    if compact_name not in allowed_users:
        # permission denied
        tmp_msg = f"denied since '{compact_name}' not authorized with 't' in {panda_config.schemaMETA}.USERS.GRIDPREF"
        tmp_log.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # token key is missing
    if client_name not in global_dispatch_parameter_cache["tokenKeys"]:
        tmp_msg = f"token key is missing for '{client_name}"
        tmp_log.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # send token key
    data = {"tokenKey": global_dispatch_parameter_cache["tokenKeys"][client_name]["latest"]}
    tmp_log.debug(f"sent token key to '{compact_name}'")

    return generate_response(True, data=data)


@request_validation(_logger, secure=True, request_method="GET")
def get_proxy(req: PandaRequest, role: str = None, target_dn: str = None):
    """
    Get proxy for a user with a role.

    Args:
        req: The request object containing the environment variables.
        role (str, optional): The role of the user. Defaults to None.
        dn (str, optional): The distinguished name of the user. Defaults to None.
    Returns:
        dict: The proxy for the user.
    """
    real_dn = get_dn(req)
    if role == "":
        role = None
    return jobDispatcher.get_proxy(real_dn, role, dn, False, None)


def validate_user_permissions(compact_name, tokenized=False):
    allowed_names = global_dispatch_parameter_cache.get("allowProxy", [])

    # The user is allowed to get a proxy or token
    if compact_name in allowed_names:
        return True, ""

    # Permission denied for user
    secret = "proxy" if not tokenized else "access token"
    tmp_msg = f"failed: '{compact_name}' not in authorized users with 'p' in {panda_config.schemaMETA}.USERS.GRIDPREF to get {secret}"

    return False, tmp_msg


def get_proxy(real_distinguished_name: str, role: str | None, target_distinguished_name: str | None, tokenized: bool, token_key: str | None) -> dict:
    # TODO: this needs to be reviewed further
    """
    Get proxy for a user with a role

    :param real_distinguished_name: actual distinguished name of the user
    :param role: role of the user
    :param target_distinguished_name: target distinguished name if the user wants to get proxy for someone else.
                                      This is one of client_name defined in token_cache_config when getting a token
    :param tokenized: whether the response should contain a token instead of a proxy
    :param token_key: key to get the token from the token cache

    :return: response in dictionary
    """
    if target_distinguished_name is None:
        target_distinguished_name = real_distinguished_name

    tmp_log = LogWrapper(_logger, f"get_proxy PID={os.getpid()}")
    tmp_msg = f"""start DN="{real_distinguished_name}" role={role} target="{target_distinguished_name}" tokenized={tokenized} token_key={token_key}"""
    tmp_log.debug(tmp_msg)

    # get compact DN
    compact_name = clean_user_id(real_distinguished_name)

    # check permission
    global_dispatch_parameter_cache.update()
    allowed, tmp_msg = validate_user_permissions(compact_name, tokenized)
    if not allowed:
        tmp_log.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # check if the token key is valid
    if (
        tokenized
        and target_distinguished_name in self.token_cache_config
        and self.token_cache_config[target_distinguished_name].get("use_token_key") is True
        and (
            target_distinguished_name not in global_dispatch_parameter_cache["tokenKeys"]
            or token_key not in global_dispatch_parameter_cache["tokenKeys"][target_distinguished_name]["fullList"]
        )
    ):
        tmp_msg = f"failed since token key is invalid for {target_distinguished_name}"
        tmp_log.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # get proxy
    tmp_status, tmp_msg = self.set_user_proxy(response, target_distinguished_name, role, tokenized)
    if not tmp_status:
        tmp_log.debug(tmp_msg)
        response.appendNode("StatusCode", Protocol.SC_ProxyError)
    else:
        tmp_msg = "successfully sent proxy"
        tmp_log.debug(tmp_msg)

    return generate_response(True, tmp_msg)
