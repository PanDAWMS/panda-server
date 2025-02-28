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
from typing import Dict, Generator

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from werkzeug.datastructures import FileStorage

from pandaserver.api.v1.common import generate_response, get_dn, request_validation
from pandaserver.api.v1.system_api import get_http_endpoint, get_https_endpoint
from pandaserver.config import panda_config
from pandaserver.jobdispatcher import Protocol
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import get_bare_dn
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer
from pandaserver.userinterface import Client

_logger = PandaLogger().getLogger("file_server")

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

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method with DB access in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


def _get_content_length(req: PandaRequest, tmp_logger: LogWrapper) -> int:
    """
    Get the content length of the request.

    Args:
        req (PandaRequest): PanDA request object.
        tmp_logger (LogWrapper): logger object of the calling function.

    Returns:
        int: content length of the request.
    """
    content_length = 0
    try:
        content_length = int(req.headers_in["content-length"])
    except Exception:
        if "content-length" in req.headers_in:
            tmp_logger.error(f"cannot get content_length: {req.headers_in['content-length']}")
        else:
            tmp_logger.error("no content_length for {method_name}")

    tmp_logger.debug(f"size {content_length}")
    return content_length


@request_validation(_logger, secure=True, production=True, request_method="POST")
def upload_jedi_log(req: PandaRequest, file: FileStorage) -> str:
    """
    Upload a JEDI log file

    Uploads a JEDI log file and returns the URL to the file. If there is already a log file for the task, it will be overwritten.

    Args:
        req (PandaRequest): PanDA request object.
        file (FileStorage): werkzeug.FileStorage object to be uploaded.

    Returns:
        string: String with the URL to the file
    """
    tmp_logger = LogWrapper(_logger, f"upload_jedi_log <{file.filename}>")
    tmp_logger.debug(f"start {req.subprocess_env['SSL_CLIENT_S_DN']}")

    # get file size
    content_length = 0
    try:
        content_length = int(req.headers_in["content-length"])
    except Exception:
        if "content-length" in req.headers_in:
            tmp_logger.error(f"cannot get CL : {req.headers_in['content-length']}")
        else:
            tmp_logger.error("no CL")
    tmp_logger.debug(f"size {content_length}")

    # check against the size limit for logs
    if content_length > LOG_LIMIT:
        error_message = ERROR_SIZE_LIMIT
        tmp_logger.error(error_message)
        tmp_logger.debug("Done")
        return generate_response(False, error_message)

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
        tmp_logger.debug(f"written to {full_path}")

        # return the URL depending on the protocol
        if panda_config.disableHTTP:
            protocol = "https"
            server = get_https_endpoint(req)
        else:
            protocol = "http"
            server = get_http_endpoint(req)
        file_url = f"{protocol}://{server}/cache{jedi_log_directory}/{file_base_name}"

    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_message = f"failed to write log with {error_type.__name__}:{error_value}"
        tmp_logger.error(error_message)
        tmp_logger.debug("Done")
        return generate_response(False, error_message)

    tmp_logger.debug("Done")
    return generate_response(True, data=file_url)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_jedi_log(req: PandaRequest, file: FileStorage) -> str:
    """
    Update the log file, appending more content at the end of the file.
    Args:
        req (PandaRequest): PanDA request object.
        file (FileStorage): werkzeug.FileStorage object to be updated.

    Returns:
        string: String with "True" or error message
    """
    tmp_logger = LogWrapper(_logger, f"update_jedi_log < {file.filename} >")
    tmp_logger.debug("Start")

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
        tmp_logger.error(f"{error_type} {error_value}")
        return generate_response(False, f"ERROR: cannot update file with {error_type} {error_value}")

    tmp_logger.debug("Done")
    return generate_response(True)


@request_validation(_logger, request_method="GET")
def download_jedi_log(panda_request: PandaRequest, log_name: str, offset: int = 0) -> str:
    """
    Fetch the log file, if required at a particular offset.
    Args:
        panda_request (PandaRequest): PanDA request object.
        log_name (string): log file name
        offset (int): offset in the file

    Returns:
        string: String with the log content
    """

    tmp_logger = LogWrapper(_logger, f"download_jedi_log <{log_name}>")
    tmp_logger.debug(f"Start offset={offset}")

    # put dummy char to avoid Internal Server Error
    return_string = " "
    try:
        # stdout name
        full_log_name = f"{panda_config.cache_dir}/{log_name.split('/')[-1]}"

        # read at offset of the file
        with open(full_log_name, "r") as file_object:
            file_object.seek(int(offset))
            return_string += file_object.read()

    except Exception:
        error_type, error_value, _ = sys.exc_info()
        tmp_logger.error(f"Failed with: {error_type} {error_value}")

    tmp_logger.debug(f"Read {len(return_string)} bytes")
    tmp_logger.debug("Done")
    return return_string


@request_validation(_logger, request_method="POST")
def upload_cache_file(req: PandaRequest, file: FileStorage) -> str:
    """
    Upload a file to the server.

    Args:
        req (PandaRequest): PanDA request object.
        file (FileStorage): werkzeug.FileStorage object to be uploaded.

    Returns:
        string: "True" if the upload was successful, otherwise an error message.
    """

    tmp_logger = LogWrapper(_logger, f"upload_cache_file-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    tmp_logger.debug(f"Start")

    # check if using secure connection and the proxy is not limited
    # we run these checks explicitly to trigger garbage collection
    if not Protocol.isSecure(req):
        tmp_logger.error("No HTTPS. Triggering garbage collection...")
        gc.collect()
        tmp_logger.debug("Done")
        return generate_response(False, ERROR_NOT_SECURE)

    if "/CN=limited proxy" in req.subprocess_env["SSL_CLIENT_S_DN"]:
        tmp_logger.error("Limited proxy is used. Triggering garbage collection...")
        gc.collect()
        tmp_logger.debug("Done")
        return generate_response(False, ERROR_LIMITED_PROXY)

    # user name
    user_name = CoreUtils.clean_user_id(req.subprocess_env["SSL_CLIENT_S_DN"])
    tmp_logger.debug(f"user_name={user_name} file_path={file.filename}")

    # get file size limit
    if not file.filename.startswith("sources."):
        no_build = True
        size_limit = SANDBOX_NO_BUILD_LIMIT
    else:
        no_build = False
        size_limit = SANDBOX_LIMIT

    # get actual file size
    content_length = _get_content_length(req, tmp_logger)

    # check if we are above the size limit
    if content_length > size_limit:
        error_message = f"{ERROR_SIZE_LIMIT} {content_length}>{size_limit}."
        if no_build:
            error_message += " Please submit the job without --noBuild/--libDS since those options impose a tighter size limit"
        else:
            error_message += " Please remove redundant files from your work area"
        tmp_logger.error(error_message)
        tmp_logger.debug("Triggering garbage collection...")
        gc.collect()
        tmp_logger.debug("Done")
        return generate_response(False, error_message)

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
            tmp_logger.debug(f"{ERROR_OVERWRITE} {file_name}")
            tmp_logger.debug("end")
            return generate_response(False, error_message)

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
        tmp_logger.error(error_message)
        tmp_logger.debug("Triggering garbage collection...")
        gc.collect()
        tmp_logger.debug("Done")
        return generate_response(False, error_message)

    # calculate the checksum
    try:
        # decode Footer
        footer = file_content[-8:]
        checksum, _ = struct.unpack("II", footer)
        tmp_logger.debug(f"CRC from gzip Footer {checksum}")
    except Exception:
        # use None to avoid delay for now
        checksum = None
        tmp_logger.debug(f"No CRC calculated {checksum}")

    # calculate the file size
    file_size = len(file_content)

    # log the full file information
    tmp_logger.debug(f"written dn={user_name} file={full_path} size={file_size} crc={checksum}")

    # record the file information to DB
    if panda_config.record_sandbox_info:
        # ignore some suffixes, e.g. out
        to_insert = True
        for patt in IGNORED_SUFFIX:
            if file.filename.endswith(patt):
                to_insert = False
                break
        if not to_insert:
            tmp_logger.debug("skipped to insert to DB")
        else:
            # TODO: change to the new API method once implemented
            status_client, output_client = Client.insertSandboxFileInfo(user_name, file.filename, file_size, checksum)
            if status_client != 0 or output_client.startswith("ERROR"):
                error_message = f"ERROR : failed to register file in database with {status_client} {output_client}"
                tmp_logger.error(error_message)
                tmp_logger.debug("Done")
                return generate_response(False, error_message)

            tmp_logger.debug(f"Registered file in database with: {output_client}")

    tmp_logger.debug("Triggering garbage collection...")
    gc.collect()
    tmp_logger.debug("Done")

    return generate_response(True)


@request_validation(_logger, secure=True, request_method="POST")
def touch_cache_file(req: PandaRequest, filename: str) -> str:
    """
    Touch file in the cache directory.

    Touches a file in the cache directory. It avoids the file to expire and being deleted by the server clean up.

    Args:
        req (PandaRequest): PanDA request object.
        filename (string): file name to be deleted

    Returns:
        string: String with "True" or "False"
    """

    tmp_logger = LogWrapper(_logger, f"touch_cache_file < {filename} >")
    tmp_logger.debug(f"Start")

    try:
        os.utime(f"{panda_config.cache_dir}/{filename.split('/')[-1]}", None)
        tmp_logger.debug(f"Done")
        return generate_response(True)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        _logger.error(f"Failed to touch file with: {error_type} {error_value}")
        return generate_response(False)


@request_validation(_logger, secure=True, request_method="POST")
def delete_cache_file(req: PandaRequest, file_name: str) -> str:
    """
    Delete a file from the cache directory.

    Args:
        req (PandaRequest): PanDA request object.
        file (string): file name to be deleted

    Returns:
        string: String with "True" or "False"
    """
    tmp_logger = LogWrapper(_logger, f"delete_cache_file <{file_name}>")
    tmp_logger.debug(f"Start")

    try:
        # may be reused for re-brokerage
        # os.remove('%s/%s' % (panda_config.cache_dir, file_name.split('/')[-1]))
        return generate_response(True)
    except Exception:
        return generate_response(False)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def register_cache_file(req: PandaRequest, user_name: str, file_name: str, file_size: int, checksum: str):

    tmp_logger = LogWrapper(_logger, f"register_cache_file {user_name} {file_name}")
    tmp_logger.debug("Start")

    # the files are on a particular server and not accessible through the LB endpoint
    # therefore we need to register a preconfigured hostname or hostname of the caller
    if hasattr(panda_config, "sandboxHostname") and panda_config.sandboxHostname:
        host_name = panda_config.sandboxHostname
    else:
        host_name = req.get_remote_host()

    message = global_task_buffer.insertSandboxFileInfo(user_name, host_name, file_name, file_size, checksum)
    if message != "OK":
        return generate_response(False, message)

    return generate_response(True)


@request_validation(_logger, secure=True, request_method="GET")
def validate_cache_file(req: PandaRequest, file_size: int, checksum: str):

    user = get_dn(req)
    message = global_task_buffer.checkSandboxFile(user, file_size, checksum)

    # The file was not found or there was an exception
    if message and not message.startswith("FOUND"):
        return generate_response(False, message)

    # The file was found
    return generate_response(True, message)


def _get_checkpoint_filename(jedi_task_id: str, sub_id: str) -> str:
    """
    Get the checkpoint file name.

    Args:
        jedi_task_id (str): task ID.
        sub_id (str): sub ID.

    Returns:
        string: checkpoint file name.
    """
    return f"hpo_cp_{jedi_task_id}_{sub_id}"


@request_validation(_logger, secure=True, request_method="POST")
def upload_hpo_checkpoint(req: PandaRequest, file: FileStorage) -> str:
    """
    Upload a HPO checkpoint file to the server.

    Args:
        req (PandaRequest): PanDA request object.
        file (FileStorage): werkzeug.FileStorage object to be uploaded.

    Returns:
        string: json formatted string with status and message.
    """

    tmp_logger = LogWrapper(_logger, f"upload_hpo_checkpoint <jediTaskID_subID={file.filename}>")

    tmp_logger.debug(f"Start {req.subprocess_env['SSL_CLIENT_S_DN']}")

    # extract task ID and sub ID
    try:
        task_id, sub_id = file.filename.split("/")[-1].split("_")
    except Exception:
        error_message = "Failed to extract task and sub IDs"
        tmp_logger.error(error_message)
        return generate_response(False, error_message)

    # get the file size
    try:
        content_length = int(req.headers_in["content-length"])
        tmp_logger.debug(f"Size {content_length}")
    except Exception as exc:
        error_message = f"Cannot get int(content-length) due to {str(exc)}"
        tmp_logger.error(error_message)
        return generate_response(False, error_message)

    # compare the size against the limit for checkpoints
    if content_length > CHECKPOINT_LIMIT:
        error_message = f"Exceeded size limit {content_length}>{CHECKPOINT_LIMIT}"
        tmp_logger.error(error_message)
        return generate_response(False, error_message)

    # write the file to the cache directory
    try:
        full_path = os.path.join(panda_config.cache_dir, _get_checkpoint_filename(task_id, sub_id))
        # write
        with open(full_path, "wb") as file_object:
            file_object.write(file.read())
    except Exception as exc:
        error_message = f"cannot write file due to {str(exc)}"
        tmp_logger.error(error_message)
        return generate_response(False, error_message)

    success_message = f"Successfully placed at {full_path}"
    tmp_logger.debug(success_message)
    tmp_logger.debug("Done")
    return generate_response(True, message=success_message, data=full_path)


@request_validation(_logger, secure=True, request_method="POST")
def delete_hpo_checkpoint(req: PandaRequest, jedi_task_id: str, sub_id: str) -> str:
    """
    Delete a HPO checkpoint file from the server.

    Args:
        req (PandaRequest): PanDA request object.
        jedi_task_id (str): task ID.
        sub_id (str): sub ID.

    Returns:
        string: json formatted string with status and message.
    """

    tmp_logger = LogWrapper(_logger, f"delete_hpo_checkpoint <jediTaskID={jedi_task_id} ID={sub_id}>")

    tmp_logger.debug(f"Start {req.subprocess_env['SSL_CLIENT_S_DN']}")

    try:
        full_path = os.path.join(panda_config.cache_dir, _get_checkpoint_filename(jedi_task_id, sub_id))
        os.remove(full_path)
        tmp_logger.debug("Done")
        return generate_response(True)
    except Exception as exc:
        message = f"Failed to delete file due to {str(exc)}"
        tmp_logger.error(message)
        return generate_response(False, message=message)


@request_validation(_logger, secure=True, request_method="POST")
def upload_file_recovery_request(req: PandaRequest, jedi_task_id: int, dry_run: bool = None) -> str:
    """
    Upload lost file recovery request to the server.

    Args:
        req (PandaRequest): PanDA request object.
        jedi_task_id (string): task ID.
        dryRun (bool): dry run flag.

    Returns:
        string: String in json format with (boolean, message)
    """

    user_name = req.subprocess_env["SSL_CLIENT_S_DN"]
    creation_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    tmp_logger = LogWrapper(_logger, f"put_file_recovery_request < jedi_task_id={jedi_task_id} >")
    tmp_logger.debug(f"Start user={user_name}")

    try:
        # generate the filename
        file_name = f"{panda_config.cache_dir}/recov.{str(uuid.uuid4())}"
        tmp_logger.debug(f"file={file_name}")

        # write the file content
        with open(file_name, "w") as file_object:
            data = {
                "userName": user_name,
                "creationTime": creation_time,
                "jediTaskID": jedi_task_id,
            }
            if dry_run:
                data["dryRun"] = True

            json.dump(data, file_object)
    except Exception as exc:
        error_message = f"cannot put request due to {str(exc)} "
        tmp_logger.error(error_message + traceback.format_exc())
        return generate_response(False, error_message)

    tmp_logger.debug("done")
    return generate_response(True, message="The request was accepted and will be processed in a few minutes")


@request_validation(_logger, secure=True, request_method="POST")
def upload_workflow_request(req: PandaRequest, data: str, check: bool = False, sync: bool = False) -> str:
    """
    Upload workflow request to the server.
    Args:
        req (PandaRequest): PanDA request object
        data (string): workflow request data
        check (bool): check flag
        sync (bool): requests the workflow to be processed synchronously
    Returns:
        string: String in json format with (boolean, message)
    """

    user_name = req.subprocess_env["SSL_CLIENT_S_DN"]
    creation_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    tmp_logger = LogWrapper(_logger, "upload_workflow_request")

    tmp_logger.debug(f"Start user={user_name} check={check}")

    try:
        # Generate the filename
        file_name = f"{panda_config.cache_dir}/workflow.{str(uuid.uuid4())}"
        tmp_logger.debug(f"file={file_name}")

        # Write out the workflow request
        with open(file_name, "w") as file_object:
            data_dict = {
                "userName": user_name,
                "creationTime": creation_time,
                "data": json.loads(data),
            }
            json.dump(data_dict, file_object)

        # Submitter requested synchronous processing
        if sync or check:
            tmp_logger.debug("Starting synchronous processing of the workflow")
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
            tmp_logger.debug("Done")
            return generate_response(True, data=ret)

    except Exception as exc:
        error_message = f"Cannot upload the workflow request: {str(exc)} "
        tmp_logger.error(error_message + traceback.format_exc())
        return generate_response(False, error_message)

    # Submitter did not request synchronous processing
    tmp_logger.debug("Done")
    return generate_response(True, message="The request was accepted and will be processed in a few minutes")


@request_validation(_logger, secure=True, request_method="POST")
def upload_event_picking_request(
    req: PandaRequest,
    run_event_list: str = "",
    data_type: str = "",
    stream_name: str = "",
    dataset_name: str = "",
    ami_tag: str = "",
    user_dataset_name: str = "",
    locked_by: str = "",
    parameters: str = "",
    input_file_list: str = "",
    n_sites: str = "",
    user_task_name: str = "",
    ei_api: str = "",
    include_guids: bool = False,
) -> str:
    """
    Upload event picking request to the server.

    Args:
        req (PandaRequest): PanDA request object.
        run_event_list (str): run and event list.
        data_type (str): data type.
        stream_name (str): stream name.
        dataset_name (str): dataset name.
        ami_tag (str): AMI tag.
        user_dataset_name (str): user dataset name.
        locked_by (str): locking agent.
        parameters (str): parameters.
        input_file_list (str): input file list.
        n_sites (str): number of sites.
        user_task_name (str): user task name.
        ei_api (str): event index API.
        include_guids (bool): flag to indicate if GUIDs are included with the run-event list

    Returns:
        string: "True" if the upload was successful, otherwise an error message.

    """

    user_name = req.subprocess_env["SSL_CLIENT_S_DN"]

    tmp_logger = LogWrapper(_logger, f"upload_event_picking_request-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    tmp_logger.debug(f"Start for {user_name}")

    creation_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    # get total size
    try:
        content_length = int(req.headers_in["content-length"])
    except Exception:
        error_message = "Cannot get content-length from HTTP request."
        tmp_logger.error(f"{error_message}")
        tmp_logger.debug("end")
        return generate_response(False, f"ERROR: {error_message}")

    tmp_logger.debug(f"Size {content_length}")

    if content_length > EVENT_PICKING_LIMIT:
        error_message = f"Run/event list is too large. Exceeded size limit {content_length}>{EVENT_PICKING_LIMIT}."
        tmp_logger.error(f"{error_message} ")
        tmp_logger.debug("end")
        return generate_response(False, f"ERROR: {error_message}")

    try:
        # generate the filename
        file_name = f"{panda_config.cache_dir}/evp.{str(uuid.uuid4())}"
        tmp_logger.debug(f"file: {file_name}")

        # write the information to file
        file_content = (
            f"userName={user_name}\n"
            f"creationTime={creation_time}\n"
            f"eventPickDataType={data_type}\n"
            f"eventPickStreamName={stream_name}\n"
            f"eventPickDS={dataset_name}\n"
            f"eventPickAmiTag={ami_tag}\n"
            f"eventPickNumSites={n_sites}\n"
            f"userTaskName={user_task_name}\n"
            f"userDatasetName={user_dataset_name}\n"
            f"lockedBy={locked_by}\n"
            f"params={parameters}\n"
            f"inputFileList={input_file_list}\n"
            f"ei_api={ei_api}\n"
        )

        with open(file_name, "w") as file_object:
            file_object.write(file_content)
            run_event_guid_map = {}

            valid_entry_length = 3 if include_guids else 2

            for tmp_line in run_event_list.split("\n"):
                tmp_items = tmp_line.split()

                # Skip invalid entries
                if len(tmp_items) != valid_entry_length:
                    continue

                file_object.write(f"runEvent={tmp_items[0]},{tmp_items[1]}\n")

                if include_guids:
                    run_event_guid_map[(tmp_items[0], tmp_items[1])] = [tmp_items[2]]

            file_object.write(f"runEvtGuidMap={str(run_event_guid_map)}\n")

    except Exception as e:
        error_message = f"Cannot upload the Event Picking request: {str(e)}"
        tmp_logger.error(error_message + traceback.format_exc())
        return generate_response(False, f"ERROR: {error_message}")

    tmp_logger.debug("Done")
    return generate_response(True)
