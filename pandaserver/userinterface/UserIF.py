"""
Used to provide web interface to users. Nowadays some IDDS leftover calls
"""

import json
import os
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow
from werkzeug.datastructures import FileStorage

import pandaserver.jobdispatcher.Protocol as Protocol
from pandaserver.config import panda_config
from pandaserver.srvcore.CoreUtils import clean_user_id, resolve_bool
from pandaserver.srvcore.panda_request import PandaRequest

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
    from idds.client.clientmanager import ClientManager as iDDS_ClientManager
except ImportError:
    pass

MESSAGE_SSL = "SSL secure connection is required"

# File size limits
MB = 1024 * 1024
EVENT_PICKING_LIMIT = 10 * MB
CHECKPOINT_LIMIT = 500 * MB

_logger = PandaLogger().getLogger("UserIF")


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


def putEventPickingRequest(
    panda_request: PandaRequest,
    runEventList="",
    eventPickDataType="",
    eventPickStreamName="",
    eventPickDS="",
    eventPickAmiTag="",
    userDatasetName="",
    lockedBy="",
    params="",
    inputFileList="",
    eventPickNumSites="",
    userTaskName="",
    ei_api="",
    giveGUID=None,
) -> str:
    """
    Upload event picking request to the server.

    Args:
        panda_request (PandaRequest): PanDA request object.
        runEventList (str): run and event list.
        eventPickDataType (str): data type.
        eventPickStreamName (str): stream name.
        eventPickDS (str): dataset name.
        eventPickAmiTag (str): AMI tag.
        userDatasetName (str): user dataset name.
        lockedBy (str): locking agent.
        params (str): parameters.
        inputFileList (str): input file list.
        eventPickNumSites (str): number of sites.
        userTaskName (str): user task name.
        ei_api (str): event index API.
        giveGUID (str): give GUID.

    Returns:
        string: "True" if the upload was successful, otherwise an error message.

    """
    if not Protocol.isSecure(panda_request):
        return MESSAGE_SSL

    user_name = panda_request.subprocess_env["SSL_CLIENT_S_DN"]

    tmp_log = LogWrapper(_logger, f"putEventPickingRequest-{naive_utcnow().isoformat('/')}")
    tmp_log.debug(f"start for {user_name}")

    creation_time = naive_utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # get total size
    try:
        content_length = int(panda_request.headers_in["content-length"])
    except Exception:
        error_message = "cannot get content-length from HTTP request."
        tmp_log.error(f"{error_message}")
        tmp_log.debug("end")
        return "ERROR : " + error_message
    tmp_log.debug(f"size {content_length}")

    if content_length > EVENT_PICKING_LIMIT:
        error_message = f"Run/event list is too large. Exceeded size limit {content_length}>{EVENT_PICKING_LIMIT}."
        tmp_log.error(f"{error_message} ")
        tmp_log.debug("end")
        return "ERROR : " + error_message

    if giveGUID == "True":
        giveGUID = True
    else:
        giveGUID = False

    try:
        # generate the filename
        file_name = f"{panda_config.cache_dir}/evp.{str(uuid.uuid4())}"
        tmp_log.debug(f"file: {file_name}")

        # write the information to file
        file_content = (
            f"userName={user_name}\n"
            f"creationTime={creation_time}\n"
            f"eventPickDataType={eventPickDataType}\n"
            f"eventPickStreamName={eventPickStreamName}\n"
            f"eventPickDS={eventPickDS}\n"
            f"eventPickAmiTag={eventPickAmiTag}\n"
            f"eventPickNumSites={eventPickNumSites}\n"
            f"userTaskName={userTaskName}\n"
            f"userDatasetName={userDatasetName}\n"
            f"lockedBy={lockedBy}\n"
            f"params={params}\n"
            f"inputFileList={inputFileList}\n"
            f"ei_api={ei_api}\n"
        )

        with open(file_name, "w") as file_object:
            file_object.write(file_content)
            run_event_guid_map = {}
            for tmp_line in runEventList.split("\n"):
                tmp_items = tmp_line.split()
                if (len(tmp_items) != 2 and not giveGUID) or (len(tmp_items) != 3 and giveGUID):
                    continue
                file_object.write("runEvent=%s,%s\n" % tuple(tmp_items[:2]))
                if giveGUID:
                    run_event_guid_map[tuple(tmp_items[:2])] = [tmp_items[2]]
            file_object.write(f"runEvtGuidMap={str(run_event_guid_map)}\n")

    except Exception as e:
        error_message = f"cannot put request due to {str(e)}"
        tmp_log.error(error_message + traceback.format_exc())
        return f"ERROR : {error_message}"

    tmp_log.debug("end")
    return "True"


# upload lost file recovery request
def put_file_recovery_request(panda_request: PandaRequest, jediTaskID: str, dryRun: bool = None) -> str:
    """
    Upload lost file recovery request to the server.

    Args:
        panda_request (PandaRequest): PanDA request object.
        jediTaskID (string): task ID.
        dryRun (bool): dry run flag.

    Returns:
        string: String in json format with (boolean, message)
    """
    if not Protocol.isSecure(panda_request):
        return json.dumps((False, MESSAGE_SSL))
    user_name = panda_request.subprocess_env["SSL_CLIENT_S_DN"]
    creation_time = naive_utcnow().strftime("%Y-%m-%d %H:%M:%S")

    tmp_log = LogWrapper(_logger, f"put_file_recovery_request < jediTaskID={jediTaskID}")
    tmp_log.debug(f"start user={user_name}")
    # get total size
    try:
        jedi_task_id = int(jediTaskID)

        # generate the filename
        file_name = f"{panda_config.cache_dir}/recov.{str(uuid.uuid4())}"
        tmp_log.debug(f"file={file_name}")

        # write the file content
        with open(file_name, "w") as file_object:
            data = {
                "userName": user_name,
                "creationTime": creation_time,
                "jediTaskID": jedi_task_id,
            }
            if dryRun:
                data["dryRun"] = True

            json.dump(data, file_object)
    except Exception as exc:
        error_message = f"cannot put request due to {str(exc)} "
        tmp_log.error(error_message + traceback.format_exc())
        return json.dumps((False, error_message))

    tmp_log.debug("done")
    return json.dumps((True, "request was accepted and will be processed in a few minutes"))


def put_workflow_request(panda_request: PandaRequest, data: str, check: bool = False, sync: bool = False) -> str:
    """
    Upload workflow request to the server.
    Args:
        panda_request (PandaRequest): PanDA request object.
        data (string): workflow request data.
        check (bool): check flag.
        sync (bool): synchronous processing.
    Returns:
        string: String in json format with (boolean, message)
    """

    if not Protocol.isSecure(panda_request):
        return json.dumps((False, MESSAGE_SSL))

    user_name = panda_request.subprocess_env["SSL_CLIENT_S_DN"]
    creation_time = naive_utcnow().strftime("%Y-%m-%d %H:%M:%S")

    tmp_log = LogWrapper(_logger, "put_workflow_request")

    tmp_log.debug(f"start user={user_name} check={check}")

    if check in ("True", True):
        check = True
    elif sync in ("True", True):
        sync = True

    try:
        # generate the filename
        file_name = f"{panda_config.cache_dir}/workflow.{str(uuid.uuid4())}"
        tmp_log.debug(f"file={file_name}")

        # write
        with open(file_name, "w") as file_object:
            data_dict = {
                "userName": user_name,
                "creationTime": creation_time,
                "data": json.loads(data),
            }
            json.dump(data_dict, file_object)

        if sync or check:
            from pandaserver.taskbuffer.workflow_processor import WorkflowProcessor

            processor = WorkflowProcessor(log_stream=_logger)
            if check:
                ret = processor.process(file_name, True, True, True, True)
            else:
                ret = processor.process(file_name, True, False, True, False)
            if os.path.exists(file_name):
                try:
                    os.remove(file_name)
                except Exception:
                    pass
            tmp_log.debug("done")
            return json.dumps((True, ret))

    except Exception as exc:
        error_message = f"cannot put request due to {str(exc)} "
        tmp_log.error(error_message + traceback.format_exc())
        return json.dumps((False, error_message))

    tmp_log.debug("done")
    return json.dumps((True, "request was accepted and will be processed in a few minutes"))


def get_checkpoint_filename(task_id: str, sub_id: str) -> str:
    """
    Get the checkpoint file name.

    Args:
        task_id (str): task ID.
        sub_id (str): sub ID.

    Returns:
        string: checkpoint file name.
    """
    return f"hpo_cp_{task_id}_{sub_id}"


def put_checkpoint(panda_request: PandaRequest, file: FileStorage) -> str:
    """
    Upload a HPO checkpoint file to the server.

    Args:
        panda_request (PandaRequest): PanDA request object.
        file (FileStorage): werkzeug.FileStorage object to be uploaded.

    Returns:
        string: json formatted string with status and message.
    """

    tmp_log = LogWrapper(_logger, f"put_checkpoint <jediTaskID_subID={file.filename}>")

    # operation status, will be set to True if successful
    status = False

    if not Protocol.isSecure(panda_request):
        error_message = "insecure request"
        tmp_log.error(error_message)
        return json.dumps({"status": status, "message": error_message})

    tmp_log.debug(f"start {panda_request.subprocess_env['SSL_CLIENT_S_DN']}")

    # extract task ID and sub ID
    try:
        task_id, sub_id = file.filename.split("/")[-1].split("_")
    except Exception:
        error_message = "failed to extract ID"
        tmp_log.error(error_message)
        return json.dumps({"status": status, "message": error_message})

    # get the file size
    try:
        content_length = int(panda_request.headers_in["content-length"])
    except Exception as exc:
        error_message = f"cannot get int(content-length) due to {str(exc)}"
        tmp_log.error(error_message)
        return json.dumps({"status": status, "message": error_message})
    tmp_log.debug(f"size {content_length}")

    # compare the size against the limit for checkpoints
    if content_length > CHECKPOINT_LIMIT:
        error_message = f"exceeded size limit {content_length}>{CHECKPOINT_LIMIT}"
        tmp_log.error(error_message)
        return json.dumps({"status": status, "message": error_message})

    # write the file to the cache directory
    try:
        full_path = os.path.join(panda_config.cache_dir, get_checkpoint_filename(task_id, sub_id))
        # write
        with open(full_path, "wb") as file_object:
            file_object.write(file.read())
    except Exception as exc:
        error_message = f"cannot write file due to {str(exc)}"
        tmp_log.error(error_message)
        return json.dumps({"status": status, "message": error_message})

    status = True
    success_message = f"successfully placed at {full_path}"
    tmp_log.debug(success_message)
    return json.dumps({"status": status, "message": success_message})


def delete_checkpoint(panda_request: PandaRequest, task_id: str, sub_id: str) -> str:
    """
    Delete a HPO checkpoint file from the server.

    Args:
        panda_request (PandaRequest): PanDA request object.
        task_id (str): task ID.
        sub_id (str): sub ID.

    Returns:
        string: json formatted string with status and message.
    """

    tmp_log = LogWrapper(_logger, f"delete_checkpoint <jediTaskID={task_id} ID={sub_id}>")

    if not Protocol.isSecure(panda_request):
        tmp_log.error(MESSAGE_SSL)
        return json.dumps({"status": False, "message": MESSAGE_SSL})

    tmp_log.debug(f"start {panda_request.subprocess_env['SSL_CLIENT_S_DN']}")
    # operation status
    status = True
    try:
        full_path = os.path.join(panda_config.cache_dir, get_checkpoint_filename(task_id, sub_id))
        os.remove(full_path)
        message = "done"
        tmp_log.debug(message)
    except Exception as exc:
        message = f"failed to delete file due to {str(exc)}"
        tmp_log.error(message)
        status = False

    return json.dumps({"status": status, "message": message})
