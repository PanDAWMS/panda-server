"""
utility service

"""
import datetime
import gzip
import json
import os
import re
import struct
import sys
import traceback
import uuid
import zlib

import pandaserver.jobdispatcher.Protocol as Protocol
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.userinterface import Client

_logger = PandaLogger().getLogger("Utils")

IGNORED_SUFFIX = [".out"]


# check if server is alive
def isAlive(req):
    return "alive=yes"


# upload file
def putFile(req, file):
    tmpLog = LogWrapper(_logger, f"putFile-{datetime.datetime.now(datetime.UTC).isoformat('/')}")
    if not Protocol.isSecure(req):
        tmpLog.error("No SSL_CLIENT_S_DN")
        return False
    if "/CN=limited proxy" in req.subprocess_env["SSL_CLIENT_S_DN"]:
        return False
    # user name
    username = CoreUtils.clean_user_id(req.subprocess_env["SSL_CLIENT_S_DN"])
    tmpLog.debug(f"start {username} {file.filename}")
    # size check
    fullSizeLimit = 768 * 1024 * 1024
    if not file.filename.startswith("sources."):
        noBuild = True
        sizeLimit = 100 * 1024 * 1024
    else:
        noBuild = False
        sizeLimit = fullSizeLimit
    # get file size
    contentLength = 0
    try:
        contentLength = int(req.headers_in["content-length"])
    except Exception:
        if "content-length" in req.headers_in:
            tmpLog.error(f"cannot get CL : {req.headers_in['content-length']}")
        else:
            tmpLog.error("no CL")
    tmpLog.debug(f"size {contentLength}")
    if contentLength > sizeLimit:
        errStr = f"ERROR : Upload failure. Exceeded size limit {contentLength}>{sizeLimit}."
        if noBuild:
            errStr += " Please submit the job without --noBuild/--libDS since those options impose a tighter size limit"
        else:
            errStr += " Please remove redundant files from your workarea"
        tmpLog.error(errStr)
        tmpLog.debug("end")
        return errStr
    try:
        fileName = file.filename.split("/")[-1]
        fileFullPath = f"{panda_config.cache_dir}/{fileName}"

        # avoid overwriting
        if os.path.exists(fileFullPath) and file.filename.split(".")[-1] != "__ow__":
            # touch
            os.utime(fileFullPath, None)
            # send error message
            errStr = "ERROR : Cannot overwrite file"
            tmpLog.debug(f"cannot overwrite file {fileName}")
            tmpLog.debug("end")
            return errStr
        # write
        fo = open(fileFullPath, "wb")
        fileContent = file.file.read()
        if hasattr(panda_config, "compress_file_names") and [
            True for patt in panda_config.compress_file_names.split(",") if re.search(patt, fileName) is not None
        ]:
            fileContent = gzip.compress(fileContent)
        fo.write(fileContent)
        fo.close()
    except Exception:
        errStr = "ERROR : Cannot write file"
        tmpLog.error(errStr)
        tmpLog.debug("end")
        return errStr
    # checksum
    try:
        # decode Footer
        footer = fileContent[-8:]
        checkSum, isize = struct.unpack("II", footer)
        tmpLog.debug(f"CRC from gzip Footer {checkSum}")
    except Exception:
        # calculate on the fly
        """
        import zlib
        checkSum = zlib.adler32(fileContent) & 0xFFFFFFFF
        """
        # use None to avoid delay for now
        checkSum = None
        tmpLog.debug(f"CRC calculated {checkSum}")
    # file size
    fileSize = len(fileContent)
    tmpLog.debug(f"written dn={username} file={fileFullPath} size={fileSize} crc={checkSum}")
    # put file info to DB
    if panda_config.record_sandbox_info:
        to_insert = True
        for patt in IGNORED_SUFFIX:
            if file.filename.endswith(patt):
                to_insert = False
                break
        if not to_insert:
            tmpLog.debug("skipped to insert to DB")
        else:
            statClient, outClient = Client.insertSandboxFileInfo(username, file.filename, fileSize, checkSum)
            if statClient != 0 or outClient.startswith("ERROR"):
                errStr = f"ERROR : failed to register sandbox to DB with {statClient} {outClient}"
                tmpLog.error(errStr)
                tmpLog.debug("end")
                return errStr
            else:
                tmpLog.debug(f"inserted sandbox to DB with {outClient}")
    tmpLog.debug("end")
    return True


# get event picking request
def putEventPickingRequest(
    req,
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
):
    if not Protocol.isSecure(req):
        return "ERROR : no HTTPS"
    userName = req.subprocess_env["SSL_CLIENT_S_DN"]
    creationTime = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    _logger.debug(f"putEventPickingRequest : {userName} start")
    # size check
    sizeLimit = 10 * 1024 * 1024
    # get total size
    try:
        contentLength = int(req.headers_in["content-length"])
    except Exception:
        errStr = "cannot get content-length from HTTP request."
        _logger.error("putEventPickingRequest : " + errStr + " " + userName)
        _logger.debug(f"putEventPickingRequest : {userName} end")
        return "ERROR : " + errStr
    _logger.debug(f"size {contentLength}")
    if contentLength > sizeLimit:
        errStr = f"Too large run/event list. Exceeded size limit {contentLength}>{sizeLimit}."
        _logger.error("putEventPickingRequest : " + errStr + " " + userName)
        _logger.debug(f"putEventPickingRequest : {userName} end")
        return "ERROR : " + errStr
    if giveGUID == "True":
        giveGUID = True
    else:
        giveGUID = False
    try:
        # make filename
        evpFileName = f"{panda_config.cache_dir}/evp.{str(uuid.uuid4())}"
        _logger.debug(f"putEventPickingRequest : {userName} -> {evpFileName}")
        # write
        fo = open(evpFileName, "w")
        fo.write(f"userName={userName}\n")
        fo.write(f"creationTime={creationTime}\n")
        fo.write(f"eventPickDataType={eventPickDataType}\n")
        fo.write(f"eventPickStreamName={eventPickStreamName}\n")
        fo.write(f"eventPickDS={eventPickDS}\n")
        fo.write(f"eventPickAmiTag={eventPickAmiTag}\n")
        fo.write(f"eventPickNumSites={eventPickNumSites}\n")
        fo.write(f"userTaskName={userTaskName}\n")
        fo.write(f"userDatasetName={userDatasetName}\n")
        fo.write(f"lockedBy={lockedBy}\n")
        fo.write(f"params={params}\n")
        fo.write(f"inputFileList={inputFileList}\n")
        fo.write(f"ei_api={ei_api}\n")
        runEvtGuidMap = {}
        for tmpLine in runEventList.split("\n"):
            tmpItems = tmpLine.split()
            if (len(tmpItems) != 2 and not giveGUID) or (len(tmpItems) != 3 and giveGUID):
                continue
            fo.write("runEvent=%s,%s\n" % tuple(tmpItems[:2]))
            if giveGUID:
                runEvtGuidMap[tuple(tmpItems[:2])] = [tmpItems[2]]
        fo.write(f"runEvtGuidMap={str(runEvtGuidMap)}\n")
        fo.close()
    except Exception:
        errType, errValue = sys.exc_info()[:2]
        errStr = f"cannot put request due to {errType} {errValue}"
        _logger.error("putEventPickingRequest : " + errStr + " " + userName)
        return "ERROR : " + errStr
    _logger.debug(f"putEventPickingRequest : {userName} end")
    return True


# upload lost file recovery request
def put_file_recovery_request(req, jediTaskID, dryRun=None):
    if not Protocol.isSecure(req):
        return json.dumps((False, "ERROR : no HTTPS"))
    userName = req.subprocess_env["SSL_CLIENT_S_DN"]
    creationTime = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    tmpLog = LogWrapper(_logger, f"put_file_recovery_request < jediTaskID={jediTaskID}")
    tmpLog.debug(f"start user={userName}")
    # get total size
    try:
        jediTaskID = int(jediTaskID)
        # make filename
        evpFileName = f"{panda_config.cache_dir}/recov.{str(uuid.uuid4())}"
        tmpLog.debug(f"file={evpFileName}")
        # write
        with open(evpFileName, "w") as fo:
            data = {
                "userName": userName,
                "creationTime": creationTime,
                "jediTaskID": int(jediTaskID),
            }
            if dryRun:
                data["dryRun"] = True
            json.dump(data, fo)
    except Exception as e:
        errStr = f"cannot put request due to {str(e)} "
        tmpLog.error(errStr + traceback.format_exc())
        return json.dumps((False, errStr))
    tmpLog.debug("done")
    return json.dumps((True, "request was accepted and will be processed in a few minutes"))


# upload workflow request
def put_workflow_request(req, data, check=False):
    if not Protocol.isSecure(req):
        return json.dumps((False, "ERROR : no HTTPS"))
    userName = req.subprocess_env["SSL_CLIENT_S_DN"]
    creationTime = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    tmpLog = LogWrapper(_logger, "put_workflow_request")
    tmpLog.debug(f"start user={userName} check={check}")
    if check == "True" or check is True:
        check = True
    else:
        check = False
    # get total size
    try:
        # make filename
        evpFileName = f"{panda_config.cache_dir}/workflow.{str(uuid.uuid4())}"
        tmpLog.debug(f"file={evpFileName}")
        # write
        with open(evpFileName, "w") as fo:
            data = {
                "userName": userName,
                "creationTime": creationTime,
                "data": json.loads(data),
            }
            json.dump(data, fo)
        # check
        if check:
            tmpLog.debug("checking")
            from pandaserver.taskbuffer.workflow_processor import WorkflowProcessor

            processor = WorkflowProcessor(log_stream=_logger)
            ret = processor.process(evpFileName, True, True, True, True)
            if os.path.exists(evpFileName):
                try:
                    os.remove(evpFileName)
                except Exception:
                    pass
            tmpLog.debug("done")
            return json.dumps((True, ret))
    except Exception as e:
        errStr = f"cannot put request due to {str(e)} "
        tmpLog.error(errStr + traceback.format_exc())
        return json.dumps((False, errStr))
    tmpLog.debug("done")
    return json.dumps((True, "request was accepted and will be processed in a few minutes"))


# delete file
def deleteFile(req, file):
    if not Protocol.isSecure(req):
        return "False"
    try:
        # may be reused for rebrokreage
        # os.remove('%s/%s' % (panda_config.cache_dir,file.split('/')[-1]))
        return "True"
    except Exception:
        return "False"


# touch file
def touchFile(req, filename):
    if not Protocol.isSecure(req):
        return "False"
    try:
        os.utime(f"{panda_config.cache_dir}/{filename.split('/')[-1]}", None)
        return "True"
    except Exception:
        errtype, errvalue = sys.exc_info()[:2]
        _logger.error(f"touchFile : {errtype} {errvalue}")
        return "False"


# get server name:port for SSL
def getServer(req):
    return f"{panda_config.pserverhost}:{panda_config.pserverport}"


# get server name:port for HTTP
def getServerHTTP(req):
    return f"{panda_config.pserverhosthttp}:{panda_config.pserverporthttp}"


# update stdout
def updateLog(req, file):
    _logger.debug(f"updateLog : {file.filename} start")
    # write to file
    try:
        # expand
        extStr = zlib.decompress(file.file.read())
        # stdout name
        logName = f"{panda_config.cache_dir}/{file.filename.split('/')[-1]}"
        # append
        ft = open(logName, "a")
        ft.write(extStr)
        ft.close()
    except Exception:
        type, value, traceBack = sys.exc_info()
        _logger.error(f"updateLog : {type} {value}")
    _logger.debug(f"updateLog : {file.filename} end")
    return True


# fetch stdout
def fetchLog(req, logName, offset=0):
    _logger.debug(f"fetchLog : {logName} start offset={offset}")
    # put dummy char to avoid Internal Server Error
    retStr = " "
    try:
        # stdout name
        fullLogName = f"{panda_config.cache_dir}/{logName.split('/')[-1]}"
        # read
        ft = open(fullLogName, "r")
        ft.seek(int(offset))
        retStr += ft.read()
        ft.close()
    except Exception:
        type, value, traceBack = sys.exc_info()
        _logger.error(f"fetchLog : {type} {value}")
    _logger.debug(f"fetchLog : {logName} end read={len(retStr)}")
    return retStr


# get VOMS attributes
def getVomsAttr(req):
    vomsAttrs = []
    for tmpKey in req.subprocess_env:
        tmpVal = req.subprocess_env[tmpKey]
        # compact credentials
        if tmpKey.startswith("GRST_CRED_"):
            vomsAttrs.append(f"{tmpKey} : {tmpVal}\n")
    vomsAttrs.sort()
    retStr = ""
    for tmpStr in vomsAttrs:
        retStr += tmpStr
    return retStr


# get all attributes
def getAttr(req, **kv):
    allAttrs = []
    for tmpKey in req.subprocess_env:
        tmpVal = req.subprocess_env[tmpKey]
        allAttrs.append(f"{tmpKey} : {tmpVal}\n")
    allAttrs.sort()
    retStr = "===== param =====\n"
    kk = sorted(kv.keys())
    for k in kk:
        retStr += f"{k} = {kv[k]}\n"
    retStr += "\n====== env ======\n"
    for tmpStr in allAttrs:
        retStr += tmpStr
    return retStr


# upload log
def uploadLog(req, file):
    if not Protocol.isSecure(req):
        return False
    if "/CN=limited proxy" in req.subprocess_env["SSL_CLIENT_S_DN"]:
        return False
    tmpLog = LogWrapper(_logger, f"uploadLog <{file.filename}>")
    tmpLog.debug(f"start {req.subprocess_env['SSL_CLIENT_S_DN']}")
    # size check
    sizeLimit = 100 * 1024 * 1024
    # get file size
    contentLength = 0
    try:
        contentLength = int(req.headers_in["content-length"])
    except Exception:
        if "content-length" in req.headers_in:
            tmpLog.error(f"cannot get CL : {req.headers_in['content-length']}")
        else:
            tmpLog.error("no CL")
    tmpLog.debug(f"size {contentLength}")
    if contentLength > sizeLimit:
        errStr = "failed to upload log due to size limit"
        tmpLog.error(errStr)
        tmpLog.debug("end")
        return errStr
    jediLogDir = "/jedilog"
    retStr = ""
    try:
        fileBaseName = file.filename.split("/")[-1]
        fileFullPath = f"{panda_config.cache_dir}{jediLogDir}/{fileBaseName}"
        # delete old file
        if os.path.exists(fileFullPath):
            os.remove(fileFullPath)
        # write
        fo = open(fileFullPath, "wb")
        fileContent = file.file.read()
        fo.write(fileContent)
        fo.close()
        tmpLog.debug(f"written to {fileFullPath}")
        if panda_config.disableHTTP:
            retStr = f"https://{getServer(None)}/cache{jediLogDir}/{fileBaseName}"
        else:
            retStr = f"http://{getServerHTTP(None)}/cache{jediLogDir}/{fileBaseName}"
    except Exception:
        errtype, errvalue = sys.exc_info()[:2]
        errStr = f"failed to write log with {errtype.__name__}:{errvalue}"
        tmpLog.error(errStr)
        tmpLog.debug("end")
        return errStr
    tmpLog.debug("end")
    return retStr


# partitions input into shards of a given size for bulk operations in the DB
def create_shards(input_list, size):
    """
    Creates shards of size n from the input list.
    @author: Miguel Branco in DQ2 Site Services code
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


# checkpoint filename
def get_checkpoint_filename(task_id, sub_id):
    return f"hpo_cp_{task_id}_{sub_id}"


# upload checkpoint file
def put_checkpoint(req, file):
    tmpLog = LogWrapper(_logger, f"put_checkpoint <jediTaskID_subID={file.filename}>")
    status = False
    if not Protocol.isSecure(req):
        errStr = "insecure request"
        tmpLog.error(errStr)
        return json.dumps({"status": status, "message": errStr})
    tmpLog.debug(f"start {req.subprocess_env['SSL_CLIENT_S_DN']}")
    # extract taskID and subID
    try:
        task_id, sub_id = file.filename.split("/")[-1].split("_")
    except Exception:
        errStr = "failed to extract ID"
        tmpLog.error(errStr)
        return json.dumps({"status": status, "message": errStr})
    # size check
    sizeLimit = 500 * 1024 * 1024
    # get file size
    try:
        contentLength = int(req.headers_in["content-length"])
    except Exception as e:
        errStr = f"cannot get int(content-length) due to {str(e)}"
        tmpLog.error(errStr)
        return json.dumps({"status": status, "message": errStr})
    tmpLog.debug(f"size {contentLength}")
    if contentLength > sizeLimit:
        errStr = f"exceeded size limit {contentLength}>{sizeLimit}"
        tmpLog.error(errStr)
        return json.dumps({"status": status, "message": errStr})
    try:
        fileFullPath = os.path.join(panda_config.cache_dir, get_checkpoint_filename(task_id, sub_id))
        # write
        with open(fileFullPath, "wb") as fo:
            fo.write(file.file.read())
    except Exception as e:
        errStr = f"cannot write file due to {str(e)}"
        tmpLog.error(errStr)
        return json.dumps({"status": status, "message": errStr})
    status = True
    tmpMsg = f"successfully placed at {fileFullPath}"
    tmpLog.debug(tmpMsg)
    return json.dumps({"status": status, "message": tmpMsg})


# delete checkpoint file
def delete_checkpoint(req, task_id, sub_id):
    tmpLog = LogWrapper(_logger, f"delete_checkpoint <jediTaskID={task_id} ID={sub_id}>")
    status = True
    if not Protocol.isSecure(req):
        msg = "insecure request"
        tmpLog.error(msg)
        status = False
    else:
        tmpLog.debug(f"start {req.subprocess_env['SSL_CLIENT_S_DN']}")
        try:
            fileFullPath = os.path.join(panda_config.cache_dir, get_checkpoint_filename(task_id, sub_id))
            os.remove(fileFullPath)
            msg = "done"
            tmpLog.debug(msg)
        except Exception as e:
            msg = f"failed to delete file due to {str(e)}"
            tmpLog.error(msg)
            status = False
    return json.dumps({"status": status, "message": msg})
