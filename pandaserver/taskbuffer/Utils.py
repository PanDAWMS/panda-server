import gc
import gzip
import json
import os
import re
import struct
import sys
import traceback
import uuid
import zlib
from typing import Generator

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow
from werkzeug.datastructures import FileStorage

from pandaserver.config import panda_config
from pandaserver.jobdispatcher import Protocol
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.userinterface import Client

_logger = PandaLogger().getLogger("Utils")

IGNORED_SUFFIX = [".out"]

# File size limits
MB = 1024 * 1024
EVENT_PICKING_LIMIT = 10 * MB
LOG_LIMIT = 100 * MB
CHECKPOINT_LIMIT = 500 * MB
SANDBOX_NO_BUILD_LIMIT = 100 * MB
SANDBOX_LIMIT = 768 * MB

# Error messages
ERROR_NOT_SECURE = "ERROR : no HTTPS"
ERROR_LIMITED_PROXY = "ERROR: rejected due to the usage of limited proxy"
ERROR_OVERWRITE = "ERROR: cannot overwrite file"
ERROR_WRITE = "ERROR: cannot write file"
ERROR_SIZE_LIMIT = "ERROR: upload failure. Exceeded size limit"


def get_content_length(panda_request: PandaRequest, tmp_log: LogWrapper) -> int:
    """
    Get the content length of the request.

    Args:
        panda_request (PandaRequest): PanDA request object.
        tmp_log (LogWrapper): logger object of the calling function.

    Returns:
        int: content length of the request.
    """
    content_length = 0
    try:
        content_length = int(panda_request.headers_in["content-length"])
    except Exception:
        if "content-length" in panda_request.headers_in:
            tmp_log.error(f"cannot get content_length: {panda_request.headers_in['content-length']}")
        else:
            tmp_log.error("no content_length for {method_name}")

    tmp_log.debug(f"size {content_length}")
    return content_length


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
        return ERROR_NOT_SECURE

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
        return json.dumps((False, ERROR_NOT_SECURE))
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
        return json.dumps((False, ERROR_NOT_SECURE))

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


# get server name:port for HTTP
def getServerHTTP(panda_request: PandaRequest) -> str:
    """
    Get the HTTP server name and port for HTTP.
    Args:
        panda_request (PandaRequest): PanDA request object.

    Returns:
        string: String with server:port
    """
    return f"{panda_config.pserverhosthttp}:{panda_config.pserverporthttp}"


def create_shards(input_list: list, size: int) -> Generator:
    """
    Partitions input into shards of a given size for bulk operations.
    @author: Miguel Branco in DQ2 Site Services code

    Args:
        input_list (list): list to be partitioned
        size (int): size of the shards

    Returns:
        list: list of shards

    """
    shard, i = [], 0
    for element in input_list:
        shard.append(element)
        i += 1
        if i == size:
            yield shard
            shard, i = [], 0

    if i > 0:
        yield shard


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
        tmp_log.error(ERROR_NOT_SECURE)
        return json.dumps({"status": False, "message": ERROR_NOT_SECURE})

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
