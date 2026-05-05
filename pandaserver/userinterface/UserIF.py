"""
provide web interface to users

"""

import json
import re
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

import pandaserver.jobdispatcher.Protocol as Protocol
from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_user_id, resolve_bool

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
    from idds.client.clientmanager import ClientManager as iDDS_ClientManager
except ImportError:
    pass

MESSAGE_SSL = "SSL secure connection is required"
MESSAGE_PROD_ROLE = "production or pilot role required"
MESSAGE_TASK_ID = "jediTaskID must be an integer"
MESSAGE_DATABASE = "database error in the PanDA server"
MESSAGE_JSON = "failed to load JSON"

CODE_SSL = 100
CODE_LOGIC = 101
CODE_OTHER_PARAMS = 102


_logger = PandaLogger().getLogger("UserIF")


def resolve_true(variable):
    return variable == "True"


def resolve_false(variable):
    return variable != "False"


# main class
class UserIF:
    # constructor
    def __init__(self):
        self.taskBuffer = None

    # initialize
    def init(self, taskBuffer):
        self.taskBuffer = taskBuffer

    # set num slots for workload provisioning
    def setNumSlotsForWP(self, pandaQueueName, numSlots, gshare, resourceType, validPeriod):
        return_value = self.taskBuffer.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)
        return json.dumps(return_value)


# Singleton
userIF = UserIF()
del UserIF


# get FQANs
def _getFQAN(req):
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
def _getDN(req):
    real_dn = ""
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        # remove redundant CN
        real_dn = CoreUtils.get_bare_dn(req.subprocess_env["SSL_CLIENT_S_DN"], keep_proxy=True)
    return real_dn


# check role
def _has_production_role(req):
    # check DN
    user = _getDN(req)
    for sdn in panda_config.production_dns:
        if sdn in user:
            return True
    # get FQANs
    fqans = _getFQAN(req)
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


# security check
def isSecure(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # disable limited proxy
    if "/CN=limited proxy" in req.subprocess_env["SSL_CLIENT_S_DN"]:
        _logger.warning(f"access via limited proxy : {req.subprocess_env['SSL_CLIENT_S_DN']}")
        return False
    return True


"""
web service interface

"""


# set num slots for workload provisioning
def setNumSlotsForWP(req, pandaQueueName, numSlots, gshare=None, resourceType=None, validPeriod=None):
    # check security
    if not isSecure(req):
        return json.dumps((CODE_SSL, MESSAGE_SSL))
    # check role
    if not _has_production_role(req):
        return json.dumps((CODE_LOGIC, "production role is required in the certificate"))
    # convert
    try:
        numSlots = int(numSlots)
    except Exception:
        return json.dumps((CODE_OTHER_PARAMS, "numSlots must be int"))
    # execute
    return userIF.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)


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


# relay iDDS command
def relay_idds_command(req, command_name, args=None, kwargs=None, manager=None, json_outputs=None):
    tmp_log = LogWrapper(
        _logger,
        f"relay_idds_command-{naive_utcnow().isoformat('/')}",
    )
    # check security
    if not isSecure(req):
        tmp_log.error(MESSAGE_SSL)
        return json.dumps((False, MESSAGE_SSL))
    try:
        manager = resolve_bool(manager)
        if not manager:
            manager = False
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
            return json.dumps((False, tmp_str))
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
            return json.dumps((True, ret))
        except Exception:
            return idds.common.utils.json_dumps((True, ret))
    except Exception as e:
        tmp_str = f"failed to execute command with {str(e)}"
        tmp_log.error(f"{tmp_str} {traceback.format_exc()}")
        return json.dumps((False, tmp_str))


# relay iDDS workflow command with ownership check
def execute_idds_workflow_command(req, command_name, kwargs=None, json_outputs=None):
    try:
        tmp_log = LogWrapper(
            _logger,
            f"execute_idds_workflow_command-{naive_utcnow().isoformat('/')}",
        )
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
            return json.dumps((False, tmp_message))
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
                return json.dumps((False, tmp_message))
            requester = clean_user_id(dn)
            # get request_id
            request_id = kwargs.get("request_id")
            if request_id is None:
                tmp_message = "request_id is missing"
                tmp_log.error(tmp_message)
                return json.dumps((False, tmp_message))
            # get request
            req = c.get_requests(request_id=request_id)
            if not req:
                tmp_message = f"request {request_id} is not found"
                tmp_log.error(tmp_message)
                return json.dumps((False, tmp_message))
            user_name = req[0].get("username")
            if user_name and user_name != requester:
                tmp_message = f"request {request_id} is not owned by {requester}"
                tmp_log.error(tmp_message)
                return json.dumps((False, tmp_message))
        # set original username
        if dn:
            c.set_original_user(user_name=clean_user_id(dn))
        # execute command
        tmp_log.debug(f"com={command_name} host={idds_host} kwargs={str(kwargs)}")
        ret = getattr(c, command_name)(**kwargs)
        tmp_log.debug(str(ret))
        if isinstance(ret, dict) and "message" in ret:
            return json.dumps((True, [ret["status"], ret["message"]]))
        return json.dumps((True, ret))
    except Exception as e:
        tmp_log.error(f"failed with {str(e)} {traceback.format_exc()}")
        return json.dumps((False, f"server failed with {str(e)}"))
