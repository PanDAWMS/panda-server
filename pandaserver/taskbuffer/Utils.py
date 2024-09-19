import datetime
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


def isAlive(panda_request: PandaRequest) -> str:
    """
    Check if the server is alive. Basic function for the health check and used in SLS monitoring.

    Args:
        panda_request (PandaRequest): PanDA request object.

    Returns:
        str: "alive=yes"
    """
    return "alive=yes"


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


# upload file
def putFile(panda_request: PandaRequest, file: FileStorage) -> str:
    """
    Upload a file to the server.

    Args:
        panda_request (PandaRequest): PanDA request object.
        file (FileStorage): werkzeug.FileStorage object to be uploaded.

    Returns:
        string: "True" if the upload was successful, otherwise an error message.
    """

    tmp_log = LogWrapper(_logger, f"putFile-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    tmp_log.debug(f"start")

    # check if using secure connection and the proxy is not limited
    if not Protocol.isSecure(panda_request):
        tmp_log.error("no HTTPS")
        tmp_log.debug("trigger garbage collection")
        gc.collect()
        tmp_log.debug("end")
        return ERROR_NOT_SECURE
    if "/CN=limited proxy" in panda_request.subprocess_env["SSL_CLIENT_S_DN"]:
        tmp_log.error("limited proxy is used")
        tmp_log.debug("trigger garbage collection")
        gc.collect()
        tmp_log.debug("end")
        return ERROR_LIMITED_PROXY

    # user name
    user_name = CoreUtils.clean_user_id(panda_request.subprocess_env["SSL_CLIENT_S_DN"])
    tmp_log.debug(f"user_name={user_name} file_path={file.filename}")

    # get file size limit
    if not file.filename.startswith("sources."):
        no_build = True
        size_limit = SANDBOX_NO_BUILD_LIMIT
    else:
        no_build = False
        size_limit = SANDBOX_LIMIT

    # get actual file size
    content_length = get_content_length(panda_request, tmp_log)

    # check if we are above the size limit
    if content_length > size_limit:
        error_message = f"{ERROR_SIZE_LIMIT} {content_length}>{size_limit}."
        if no_build:
            error_message += " Please submit the job without --noBuild/--libDS since those options impose a tighter size limit"
        else:
            error_message += " Please remove redundant files from your work area"
        tmp_log.error(error_message)
        tmp_log.debug("trigger garbage collection")
        gc.collect()
        tmp_log.debug("end")
        return error_message

    # write to file
    try:
        file_name = file.filename.split("/")[-1]
        full_path = f"{panda_config.cache_dir}/{file_name}"

        # avoid overwriting
        if os.path.exists(full_path) and file.filename.split(".")[-1] != "__ow__":
            # touch
            os.utime(full_path, None)
            # send error message
            error_message = ERROR_OVERWRITE
            tmp_log.debug(f"{ERROR_OVERWRITE} {file_name}")
            tmp_log.debug("end")
            return error_message

        # write the file to the cache directory
        with open(full_path, "wb") as file_object:
            file_content = file.read()
            if hasattr(panda_config, "compress_file_names") and [
                True for patt in panda_config.compress_file_names.split(",") if re.search(patt, file_name) is not None
            ]:
                file_content = gzip.compress(file_content)
            file_object.write(file_content)

    except Exception:
        error_message = ERROR_WRITE
        tmp_log.error(error_message)
        tmp_log.debug("trigger garbage collection")
        gc.collect()
        tmp_log.debug("end")
        return error_message

    # calculate the checksum
    try:
        # decode Footer
        footer = file_content[-8:]
        checksum, _ = struct.unpack("II", footer)
        tmp_log.debug(f"CRC from gzip Footer {checksum}")
    except Exception:
        # use None to avoid delay for now
        checksum = None
        tmp_log.debug(f"No CRC calculated {checksum}")

    # calculate the file size
    file_size = len(file_content)

    # log the full file information
    tmp_log.debug(f"written dn={user_name} file={full_path} size={file_size} crc={checksum}")

    # record the file information to DB
    if panda_config.record_sandbox_info:
        # ignore some suffixes, e.g. out
        to_insert = True
        for patt in IGNORED_SUFFIX:
            if file.filename.endswith(patt):
                to_insert = False
                break
        if not to_insert:
            tmp_log.debug("skipped to insert to DB")
        else:
            status_client, output_client = Client.insertSandboxFileInfo(user_name, file.filename, file_size, checksum)
            if status_client != 0 or output_client.startswith("ERROR"):
                error_message = f"ERROR : failed to register sandbox to DB with {status_client} {output_client}"
                tmp_log.error(error_message)
                tmp_log.debug("end")
                return error_message

            tmp_log.debug(f"inserted sandbox to DB with {output_client}")

    tmp_log.debug("trigger garbage collection")
    gc.collect()
    tmp_log.debug("end")

    return "True"


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

    tmp_log = LogWrapper(_logger, f"putEventPickingRequest-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    tmp_log.debug(f"start for {user_name}")

    creation_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

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
    creation_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

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
    creation_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

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


# delete file
def deleteFile(panda_request: PandaRequest, file: FileStorage) -> str:
    """
    Delete a file from the cache directory.
    Args:
        panda_request (PandaRequest): PanDA request object.
        file (string): file name to be deleted

    Returns:
        string: String with "True" or "False"
    """
    if not Protocol.isSecure(panda_request):
        return ERROR_NOT_SECURE

    try:
        # may be reused for re-brokerage
        # os.remove('%s/%s' % (panda_config.cache_dir,file.split('/')[-1]))
        return "True"
    except Exception:
        return "False"


# touch file
def touchFile(panda_request: PandaRequest, filename: str) -> str:
    """
    Touch a file in the cache directory.
    Args:
        panda_request (PandaRequest): PanDA request object.
        filename (string): file name to be deleted

    Returns:
        string: String with "True" or "False"
    """
    if not Protocol.isSecure(panda_request):
        return "False"

    try:
        os.utime(f"{panda_config.cache_dir}/{filename.split('/')[-1]}", None)
        return "True"
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        _logger.error(f"touchFile : {error_type} {error_value}")
        return "False"


# get server name:port for SSL
def getServer(panda_request: PandaRequest) -> str:
    """
    Get the server name and port for HTTPS.
    Args:
        panda_request (PandaRequest): PanDA request object.

    Returns:
        string: String with server:port
    """
    return f"{panda_config.pserverhost}:{panda_config.pserverport}"


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


def updateLog(panda_request: PandaRequest, file: FileStorage) -> str:
    """
    Update the log file, appending more content at the end of the file.
    Args:
        panda_request (PandaRequest): PanDA request object.
        file (FileStorage): werkzeug.FileStorage object to be updated.

    Returns:
        string: String with "True" or error message
    """
    tmp_log = LogWrapper(_logger, f"updateLog < {file.filename} >")
    tmp_log.debug("start")

    # write to file
    try:
        # expand
        new_content = zlib.decompress(file.read())

        # stdout name
        log_name = f"{panda_config.cache_dir}/{file.filename.split('/')[-1]}"

        # append to file end
        with open(log_name, "a") as file_object:
            file_object.write(new_content)

    except Exception:
        error_type, error_value, _ = sys.exc_info()
        tmp_log.error(f"{error_type} {error_value}")
        return f"ERROR: cannot update file with {error_type} {error_value}"

    tmp_log.debug("end")
    return "True"


def fetchLog(panda_request: PandaRequest, logName: str, offset: int = 0) -> str:
    """
    Fetch the log file, if required at a particular offset.
    Args:
        panda_request (PandaRequest): PanDA request object.
        logName (string): log file name
        offset (int): offset in the file

    Returns:
        string: String with the log content
    """
    tmp_log = LogWrapper(_logger, f"fetchLog <{logName}>")
    tmp_log.debug(f"start offset={offset}")

    # put dummy char to avoid Internal Server Error
    return_string = " "
    try:
        # stdout name
        full_log_name = f"{panda_config.cache_dir}/{logName.split('/')[-1]}"

        # read at offset of the file
        with open(full_log_name, "r") as file_object:
            file_object.seek(int(offset))
            return_string += file_object.read()

    except Exception:
        error_type, error_value, _ = sys.exc_info()
        tmp_log.error(f"{error_type} {error_value}")

    tmp_log.debug(f"end read={len(return_string)}")
    return return_string


def getVomsAttr(panda_request: PandaRequest) -> str:
    """
    Get the VOMS attributes in sorted order.
    Args:
        panda_request (PandaRequest): PanDA request object.

    Returns:
        string: String with the VOMS attributes
    """
    attributes = []

    # Iterate over all the environment variables, keep only the ones related to GRST credentials (GRST: Grid Security Technology)
    for tmp_key in panda_request.subprocess_env:
        tmp_val = panda_request.subprocess_env[tmp_key]

        # compact credentials
        if tmp_key.startswith("GRST_CRED_"):
            attributes.append(f"{tmp_key} : {tmp_val}\n")

    return "".join(sorted(attributes))


def getAttr(panda_request: PandaRequest, **kv: dict) -> str:
    """
    Get all parameters and environment variables from the environment.
    Args:
        panda_request (PandaRequest): PanDA request object.
        kv (dict): dictionary with key-value pairs

    Returns:
        string: String with the attributes
    """
    # add the parameters
    return_string = "===== param =====\n"
    for tmp_key in sorted(kv.keys()):
        tmp_val = kv[tmp_key]
        return_string += f"{tmp_key} = {tmp_val}\n"

    # add the environment variables
    attributes = []
    for tmp_key in panda_request.subprocess_env:
        tmp_val = panda_request.subprocess_env[tmp_key]
        attributes.append(f"{tmp_key} : {tmp_val}\n")

    return_string += "\n====== env ======\n"
    attributes.sort()
    for attribute in sorted(attributes):
        return_string += attribute

    return return_string


def uploadLog(panda_request: PandaRequest, file: FileStorage) -> str:
    """
    Upload a JEDI log file
    Args:
        panda_request (PandaRequest): PanDA request object.
        file (FileStorage): werkzeug.FileStorage object to be uploaded.

    Returns:
        string: String with the URL to the file
    """

    if not Protocol.isSecure(panda_request):
        return ERROR_NOT_SECURE
    if "/CN=limited proxy" in panda_request.subprocess_env["SSL_CLIENT_S_DN"]:
        return ERROR_LIMITED_PROXY

    tmp_log = LogWrapper(_logger, f"uploadLog <{file.filename}>")
    tmp_log.debug(f"start {panda_request.subprocess_env['SSL_CLIENT_S_DN']}")

    # get file size
    content_length = 0
    try:
        content_length = int(panda_request.headers_in["content-length"])
    except Exception:
        if "content-length" in panda_request.headers_in:
            tmp_log.error(f"cannot get CL : {panda_request.headers_in['content-length']}")
        else:
            tmp_log.error("no CL")
    tmp_log.debug(f"size {content_length}")

    # check against the size limit for logs
    if content_length > LOG_LIMIT:
        error_message = ERROR_SIZE_LIMIT
        tmp_log.error(error_message)
        tmp_log.debug("end")
        return error_message

    jedi_log_directory = "/jedilog"
    try:
        file_base_name = file.filename.split("/")[-1]
        full_path = f"{panda_config.cache_dir}{jedi_log_directory}/{file_base_name}"

        # delete old file
        if os.path.exists(full_path):
            os.remove(full_path)

        # write the new file
        with open(full_path, "wb") as file_object:
            file_content = file.read()
            file_object.write(file_content)
        tmp_log.debug(f"written to {full_path}")

        # return the URL depending on the protocol
        if panda_config.disableHTTP:
            protocol = "https"
            server = getServer(None)
        else:
            protocol = "http"
            server = getServerHTTP(None)
        return_string = f"{protocol}://{server}/cache{jedi_log_directory}/{file_base_name}"

    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_message = f"failed to write log with {error_type.__name__}:{error_value}"
        tmp_log.error(error_message)
        tmp_log.debug("end")
        return error_message

    tmp_log.debug("end")
    return return_string


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
