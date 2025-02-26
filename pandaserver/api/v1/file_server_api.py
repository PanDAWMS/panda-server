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

from pandaserver.api.v1.common import generate_response, request_validation
from pandaserver.api.v1.system_api import get_http_endpoint, get_https_endpoint
from pandaserver.config import panda_config
from pandaserver.jobdispatcher import Protocol
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.panda_request import PandaRequest
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


def get_content_length(req: PandaRequest, tmp_logger: LogWrapper) -> int:
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

    tmp_log = LogWrapper(_logger, f"download_jedi_log <{log_name}>")
    tmp_log.debug(f"Start offset={offset}")

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
        tmp_log.error(f"Failed with: {error_type} {error_value}")

    tmp_log.debug(f"Read {len(return_string)} bytes")
    tmp_log.debug("Done")
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
    content_length = get_content_length(req, tmp_logger)

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
