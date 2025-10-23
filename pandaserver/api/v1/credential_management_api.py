import json
import os
import socket
from threading import Lock
from typing import List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.api.v1.common import generate_response, get_dn, request_validation
from pandaserver.config import panda_config
from pandaserver.proxycache import panda_proxy_cache, token_cache
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_user_id, get_bare_dn
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_cred_management")

# These global variables are initialized in the init_task_buffer method
global_task_buffer = None
global_dispatch_parameter_cache = None

# These global variables don't depend on DB access and can be initialized here
global_proxy_cache = panda_proxy_cache.MyProxyInterface()
global_token_cache = token_cache.TokenCache()

global_lock = Lock()


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """

    with global_lock:
        global global_task_buffer
        global_task_buffer = task_buffer

        # This variable depends on having an initialized task buffer
        global global_dispatch_parameter_cache
        global_dispatch_parameter_cache = CoreUtils.CachedObject("dispatcher_params", 60 * 10, _get_dispatch_parameters, _logger)

        global global_token_cache_config
        global_token_cache_config = _read_token_cache_configuration()


def _read_token_cache_configuration():
    # config of token cacher
    try:
        with open(panda_config.token_cache_config) as f:
            return json.load(f)
    except Exception:
        return {}


def _get_dispatch_parameters():
    """
    Wrapper function around taskBuffer.get_special_dispatch_params to convert list to set since task buffer cannot return set
    """
    parameters = global_task_buffer.get_special_dispatch_params()
    for client_name, client_data in parameters["tokenKeys"].items():
        client_data["fullList"] = set(client_data["fullList"])

    return True, parameters


def _validate_user_permissions(compact_name, tokenized=False) -> dict:
    allowed_names = global_dispatch_parameter_cache.get("allowProxy", [])

    # The user is allowed to get a proxy or token
    if compact_name in allowed_names:
        return True, ""

    # Permission denied for user
    secret = "proxy" if not tokenized else "access token"
    tmp_msg = f"failed: '{compact_name}' not in authorized users with 'p' in {panda_config.schemaMETA}.USERS.GRIDPREF to get {secret}"

    return False, tmp_msg


@request_validation(_logger, secure=True, request_method="POST")
def set_user_secrets(req: PandaRequest, key: str = None, value: str = None) -> dict:
    """
    Set user secrets

    Set a key-value pair to store in PanDA. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/creds/set_user_secrets

    Args:
        req(PandaRequest): internally generated request object
        key(str): Key to reference the secret
        value(str): Value of the secret

    Returns:
        dict: The system response. True for success, False for failure, and an error message.
    """

    tmp_logger = LogWrapper(_logger, f"set_user_secret-{naive_utcnow().isoformat('/')}")

    # Get client DN and clean it. The request validation has already verified that the DN is present.
    dn = get_dn(req)
    owner = clean_user_id(dn)
    tmp_logger.debug(f"Setting secret for client={owner}")
    success, message = global_task_buffer.set_user_secret(owner, key, value)
    return generate_response(success, message)


@request_validation(_logger, secure=True, request_method="GET")
def get_user_secrets(req: PandaRequest, keys: List[str] = None) -> dict:
    """
    Get user secrets

    Get the secrets for a user identified by a list of keys. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/creds/get_user_secrets

    Args:
        req(PandaRequest): internally generated request object
        keys(list of str, optional): List of keys to reference the secrets to retrieve

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the user secrets. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(_logger, f"get_user_secrets-{naive_utcnow().isoformat('/')}")

    # Get client DN and clean it. The request validation has already verified that the DN is present.
    dn = get_dn(req)
    owner = clean_user_id(dn)
    tmp_logger.debug(f"Start for client={owner}")

    # The task buffer method expects a comma-separated string of keys
    if keys is None:
        keys_str = ""
    else:
        keys_str = ",".join(keys)

    success, data_or_message = global_task_buffer.get_user_secrets(owner, keys_str)
    message, data = "", {}
    if success:
        data = data_or_message
    else:
        message = data_or_message
    tmp_logger.debug(f"Done for client={owner}")
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, request_method="GET")
def get_key_pair(req: PandaRequest, public_key_name: str, private_key_name: str) -> dict:
    """
    Get key pair

    This function retrieves the distinguished name (DN) from the request and uses it to get a key pair. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/creds/get_key_pair

    Args:
        req(PandaRequest): internally generated request object
        public_key_name(str): The name of the public key.
        private_key_name(str): The name of the private key.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the user secrets. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(_logger, f"get_key_pair {public_key_name}/{private_key_name}")
    tmp_logger.debug("Start")

    real_dn = get_dn(req)

    # get compact DN
    compact_dn = clean_user_id(real_dn)

    # check permission
    global_dispatch_parameter_cache.update()
    allowed_keys = global_dispatch_parameter_cache.get("allowKeyPair", [])
    if compact_dn not in allowed_keys:
        # permission denied
        tmp_msg = f"Failed since '{compact_dn}' not authorized with 'k' in {panda_config.schemaMETA}.USERS.GRIDPREF"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # retrieve the key pairs
    key_pair_dict = global_dispatch_parameter_cache.get("keyPair", {})

    if public_key_name not in key_pair_dict:  # public key is missing
        tmp_msg = f"Failed for '{compact_dn}' since {public_key_name} is missing on {socket.getfqdn()}"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    if private_key_name not in key_pair_dict:  # private key is missing
        tmp_msg = f"Failed for '{compact_dn}' since {private_key_name} is missing on {socket.getfqdn()}"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # key pair is available
    data = {"public_key": key_pair_dict[public_key_name], "private_key": key_pair_dict[private_key_name]}
    tmp_logger.debug(f"Done with key-pair for '{compact_dn}'")

    return generate_response(True, data=data)


@request_validation(_logger, secure=True, request_method="GET")
def get_proxy(req: PandaRequest, role: str = None, dn: str = None) -> dict:
    """
    Get proxy

    Get the x509 proxy certificate for a user with a role. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/creds/get_proxy

    Args:
        req(PandaRequest): internally generated request object
        role(str, optional): The role of the user. Defaults to None.
        dn(str, optional): The distinguished name of the user. Defaults to None.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the x509 proxy. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(_logger, f"get_proxy PID={os.getpid()}")

    # Set the requested DN or, if not set, the requestor DN
    real_dn = get_dn(req)
    target_dn = dn if dn else real_dn

    # Default roles
    if role == "":
        role = None

    tmp_logger.debug(f"Start for DN='{real_dn}' role={role}")

    # check permission
    global_dispatch_parameter_cache.update()
    real_dn_compact = clean_user_id(real_dn)
    allowed, tmp_msg = _validate_user_permissions(real_dn_compact, tokenized=False)
    if not allowed:
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # remove redundant extensions and retrieve the proxy
    target_dn_bare = get_bare_dn(target_dn, keep_digits=False)
    output = global_proxy_cache.retrieve(target_dn_bare, role=role)

    # proxy not found
    if output is None:
        tmp_msg = f"'proxy' not found for {target_dn_bare}"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    data = {"user_proxy": output}
    tmp_logger.debug(f"Done")
    return generate_response(True, data=data)


@request_validation(_logger, secure=True, request_method="GET")
def get_access_token(req: PandaRequest, client_name: str, token_key: str = None) -> dict:
    """
    Get access token

    Get the OAuth access token for the specified client. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/creds/get_access_token

    Args:
        req(PandaRequest): internally generated request object
        client_name(str): client_name for the token as defined in token_cache_config
        token_key(str, optional): key to get the token from the token cache. Defaults to None.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the access token. When unsuccessful, the message field contains the error message.
    """

    tmp_logger = LogWrapper(_logger, f"get_proxy PID={os.getpid()}")

    # Set the requested DN or, if not set, the requestor DN
    real_dn = get_dn(req)
    target_dn = client_name if client_name else real_dn

    tmp_logger.debug(f"Start for DN='{real_dn}' target_dn='{target_dn}' token_key={token_key}")

    # check permission
    global_dispatch_parameter_cache.update()
    real_dn_compact = clean_user_id(real_dn)
    allowed, tmp_msg = _validate_user_permissions(real_dn_compact, tokenized=True)
    if not allowed:
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # check if the token key is valid
    if target_dn in global_token_cache_config and global_token_cache_config[target_dn].get("use_token_key") is True:
        if (
            target_dn not in global_dispatch_parameter_cache["tokenKeys"]
            or token_key not in global_dispatch_parameter_cache["tokenKeys"][target_dn]["fullList"]
        ):
            tmp_msg = f"failed since token key is invalid for {target_dn}"
            tmp_logger.debug(tmp_msg)
            return generate_response(False, tmp_msg)

    # remove redundant extensions
    target_dn_bare = get_bare_dn(target_dn, keep_digits=False)

    # get token
    output = global_token_cache.get_access_token(target_dn_bare)

    # access token not found
    if output is None:
        tmp_msg = f"'token' not found for {target_dn_bare}"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    data = {"user_proxy": output}
    tmp_logger.debug("Done")
    return generate_response(True, data=data)


@request_validation(_logger, secure=True, request_method="GET")
def get_token_key(req: PandaRequest, client_name: str) -> dict:
    """
    Get token key

    This function retrieves the distinguished name (DN) from the request and uses it to get a token key for the specified client. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/creds/get_token_key

    Args:
        req(PandaRequest): internally generated request object
        client_name (str): The name of the client requesting the token key

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the token key. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(_logger, f"get_token_key client={client_name} PID={os.getpid()}")
    tmp_logger.debug("Start")

    # get DN and clean it
    real_dn = get_dn(req)
    compact_name = clean_user_id(real_dn)

    global_dispatch_parameter_cache.update()

    # check if user is allowed to retrieved token keys
    allowed_users = global_dispatch_parameter_cache.get("allowTokenKey", [])
    if compact_name not in allowed_users:
        # permission denied
        tmp_msg = f"Denied since '{compact_name}' not authorized with 't' in {panda_config.schemaMETA}.USERS.GRIDPREF"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # token key is missing
    if client_name not in global_dispatch_parameter_cache["tokenKeys"]:
        tmp_msg = f"Token key is missing for '{client_name}'"
        tmp_logger.debug(tmp_msg)
        return generate_response(False, tmp_msg)

    # send token key
    data = {"tokenKey": global_dispatch_parameter_cache["tokenKeys"][client_name]["latest"]}
    tmp_logger.debug(f"Done getting token key for '{compact_name}'")

    return generate_response(True, data=data)
