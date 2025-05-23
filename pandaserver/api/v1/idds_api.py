import datetime
import json
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.api.v1.common import generate_response, request_validation
from pandaserver.srvcore.CoreUtils import clean_user_id

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
    from idds.client.clientmanager import ClientManager as iDDS_ClientManager
except ImportError:
    pass

_logger = PandaLogger().getLogger("api_idds")


# json decoder for idds constants
def decode_idds_enum(d):
    if "__idds_const__" in d:
        items = d["__idds_const__"].split(".")
        obj = idds.common.constants
        for item in items:
            obj = getattr(obj, item)
        return obj
    else:
        return d


@request_validation(_logger, secure=True, request_method="POST")
def relay_idds_command(req, command_name: str, args: str = None, kwargs: str = None, manager: bool = False, json_outputs: bool = False):
    tmp_log = LogWrapper(
        _logger,
        f"relay_idds_command-{naive_utcnow().isoformat('/')}",
    )

    try:
        if "+" in command_name:
            command_name, idds_host = command_name.split("+")
        else:
            idds_host = idds.common.utils.get_rest_host()

        if manager:
            c = iDDS_ClientManager(idds_host)
        else:
            c = iDDS_Client(idds_host)

        if not hasattr(c, command_name):
            tmp_str = f"{command_name} is not a command of iDDS {c.__class__.__name__}"
            tmp_log.error(tmp_str)
            return generate_response(False, tmp_str)

        if args:
            try:
                args = idds.common.utils.json_loads(args)
            except Exception as e:
                tmp_log.warning(f"failed to load args json with {str(e)}")
                args = json.loads(args, object_hook=decode_idds_enum)
        else:
            args = []

        if kwargs:
            try:
                kwargs = idds.common.utils.json_loads(kwargs)
            except Exception as e:
                tmp_log.warning(f"failed to load kwargs json with {str(e)}")
                kwargs = json.loads(kwargs, object_hook=decode_idds_enum)
        else:
            kwargs = {}

        # json outputs
        if json_outputs and manager:
            c.setup_json_outputs()

        # set original username
        dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
        if dn:
            c.set_original_user(user_name=clean_user_id(dn))

        tmp_log.debug(f"execute: class={c.__class__.__name__} com={command_name} host={idds_host} args={str(args)[:200]} kwargs={str(kwargs)[:200]}")
        ret = getattr(c, command_name)(*args, **kwargs)
        tmp_log.debug(f"ret: {str(ret)[:200]}")

        try:
            return generate_response(True, "", ret)
        except Exception:
            # TODO: I don't know how to handle this
            return idds.common.utils.json_dumps((True, ret))

    except Exception as e:
        tmp_str = f"failed to execute command with {str(e)}"
        tmp_log.error(f"{tmp_str} {traceback.format_exc()}")
        return generate_response(False, tmp_str)


# relay iDDS workflow command with ownership check
@request_validation(_logger, secure=True, request_method="POST")
def execute_idds_workflow_command(req, command_name: str, kwargs: str = None, json_outputs: bool = False):
    tmp_log = LogWrapper(
        _logger,
        f"execute_idds_workflow_command-{naive_utcnow().isoformat('/')}",
    )
    try:
        if kwargs:
            try:
                kwargs = idds.common.utils.json_loads(kwargs)
            except Exception:
                kwargs = json.loads(kwargs, object_hook=decode_idds_enum)
        else:
            kwargs = {}

        if "+" in command_name:
            command_name, idds_host = command_name.split("+")
        else:
            idds_host = idds.common.utils.get_rest_host()

        # check permission
        if command_name in ["get_status"]:
            check_owner = False
        elif command_name in ["abort", "suspend", "resume", "retry", "finish"]:
            check_owner = True
        else:
            tmp_message = f"{command_name} is unsupported"
            tmp_log.error(tmp_message)
            return generate_response(False, tmp_message)

        # check owner
        c = iDDS_ClientManager(idds_host)
        if json_outputs:
            c.setup_json_outputs()
        dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
        if check_owner:
            # requester
            if not dn:
                tmp_message = "SSL_CLIENT_S_DN is missing in HTTP request"
                tmp_log.error(tmp_message)
                return generate_response(False, tmp_message)
            requester = clean_user_id(dn)

            # get request_id
            request_id = kwargs.get("request_id")
            if request_id is None:
                tmp_message = "request_id is missing"
                tmp_log.error(tmp_message)
                return generate_response(False, tmp_message)

            # get request
            req = c.get_requests(request_id=request_id)
            if not req:
                tmp_message = f"request {request_id} is not found"
                tmp_log.error(tmp_message)
                return generate_response(False, tmp_message)

            user_name = req[0].get("username")
            if user_name and user_name != requester:
                tmp_message = f"request {request_id} is not owned by {requester}"
                tmp_log.error(tmp_message)
                return generate_response(False, tmp_message)

        # set original username
        if dn:
            c.set_original_user(user_name=clean_user_id(dn))

        # execute command
        tmp_log.debug(f"com={command_name} host={idds_host} kwargs={str(kwargs)}")
        ret = getattr(c, command_name)(**kwargs)
        tmp_log.debug(str(ret))

        if isinstance(ret, dict) and "message" in ret:
            return generate_response(True, ret["message"], ret["status"])

        return generate_response(True, "", ret)
    except Exception as e:
        tmp_log.error(f"failed with {str(e)} {traceback.format_exc()}")
        return generate_response(False, f"server failed with {str(e)}")
