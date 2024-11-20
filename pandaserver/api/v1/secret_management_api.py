import datetime
import os
from typing import List, Tuple

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import MESSAGE_DATABASE, get_dn, request_validation
from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("secret_management_api")

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True)
def set_user_secrets(req, key=None, value=None):
    """
    TODO
    """
    tmp_log = LogWrapper(_logger, f"set_user_secret-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")

    # Get client DN and clean it. The request validation has already verified that the DN is present.
    dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
    owner = clean_user_id(dn)
    tmp_log.debug(f"Setting secret for client={owner}")
    return global_task_buffer.set_user_secret(owner, key, value)


# get user secrets
@request_validation(_logger, secure=True)
def get_user_secrets(req, keys=None):
    """
    TODO
    """
    tmp_log = LogWrapper(_logger, f"get_user_secrets-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")

    # Get client DN and clean it. The request validation has already verified that the DN is present.
    dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
    owner = clean_user_id(dn)
    tmp_log.debug(f"Returning secret for client={owner}")
    return global_task_buffer.get_user_secrets(owner, keys)


# get proxy
def get_proxy(real_distinguished_name: str, role: str | None, target_distinguished_name: str | None, tokenized: bool, token_key: str | None) -> dict:
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
    if real_distinguished_name is None:
        # cannot extract DN
        tmp_msg = "failed since DN cannot be extracted"
        tmp_log.debug(tmp_msg)
        response = Protocol.Response(Protocol.SC_Perms, "Cannot extract DN from proxy. not HTTPS?")
    else:
        # get compact DN
        compact_name = global_task_buffer.cleanUserID(real_distinguished_name)
        # check permission
        self.specialDispatchParams.update()
        if "allowProxy" not in self.specialDispatchParams:
            allowed_names = []
        else:
            allowed_names = self.specialDispatchParams["allowProxy"]
        if compact_name not in allowed_names:
            # permission denied
            tmp_msg = f"failed since '{compact_name}' not in the authorized user list who have 'p' in {panda_config.schemaMETA}.USERS.GRIDPREF "
            if not tokenized:
                tmp_msg += "to get proxy"
            else:
                tmp_msg += "to get access token"
            tmp_log.debug(tmp_msg)
            response = Protocol.Response(Protocol.SC_Perms, tmp_msg)
        elif (
            tokenized
            and target_distinguished_name in self.token_cache_config
            and self.token_cache_config[target_distinguished_name].get("use_token_key") is True
            and (
                target_distinguished_name not in self.specialDispatchParams["tokenKeys"]
                or token_key not in self.specialDispatchParams["tokenKeys"][target_distinguished_name]["fullList"]
            )
        ):
            # invalid token key
            tmp_msg = f"failed since token key is invalid for {target_distinguished_name}"
            tmp_log.debug(tmp_msg)
            response = Protocol.Response(Protocol.SC_Invalid, tmp_msg)
        else:
            # get proxy
            response = Protocol.Response(Protocol.SC_Success, "")
            tmp_status, tmp_msg = self.set_user_proxy(response, target_distinguished_name, role, tokenized)
            if not tmp_status:
                tmp_log.debug(tmp_msg)
                response.appendNode("StatusCode", Protocol.SC_ProxyError)
            else:
                tmp_msg = "successful sent proxy"
                tmp_log.debug(tmp_msg)
    # return
    return response.encode(True)


def getProxy(req, role=None, dn=None):
    """
    Get proxy for a user with a role.

    Args:
        req: The request object containing the environment variables.
        role (str, optional): The role of the user. Defaults to None.
        dn (str, optional): The distinguished name of the user. Defaults to None.
    Returns:
        dict: The proxy for the user.
    """
    real_dn = _getDN(req)
    if role == "":
        role = None
    return jobDispatcher.get_proxy(real_dn, role, dn, False, None)
