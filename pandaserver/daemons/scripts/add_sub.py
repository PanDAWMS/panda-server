import datetime
import glob
import os
import re
import sys
import threading
import time
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

import pandaserver.taskbuffer.ErrorCode
import pandaserver.userinterface.Client as Client
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.srvcore.CoreUtils import commands_get_status_output

# logger
_logger = PandaLogger().getLogger("add_sub")


# main
def main(argv=tuple(), tbuf=None, **kwargs):
    tmp_log = LogWrapper(_logger, None)
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    tmp_log.debug("===================== start =====================")

    # current minute
    current_minute = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).minute

    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer

        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            useTimeout=True,
            requester=requester_id,
        )
    else:
        taskBuffer = tbuf

    # instantiate sitemapper
    aSiteMapper = SiteMapper(taskBuffer)

    # delete
    tmp_log.debug("Del session")
    status, retSel = taskBuffer.querySQLS("SELECT MAX(PandaID) FROM ATLAS_PANDA.jobsDefined4", {})
    if retSel is not None:
        try:
            maxID = retSel[0][0]
            tmp_log.debug(f"maxID : {maxID}")
            if maxID is not None:
                varMap = {}
                varMap[":maxID"] = maxID
                varMap[":jobStatus1"] = "activated"
                varMap[":jobStatus2"] = "waiting"
                varMap[":jobStatus3"] = "failed"
                varMap[":jobStatus4"] = "cancelled"
                status, retDel = taskBuffer.querySQLS(
                    "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID<:maxID AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4)",
                    varMap,
                )
        except Exception:
            pass

    # count # of getJob/updateJob in dispatcher's log
    try:
        # don't update when logrotate is running
        timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        logRotateTime = timeNow.replace(hour=3, minute=2, second=0, microsecond=0)
        if (timeNow > logRotateTime and (timeNow - logRotateTime) < datetime.timedelta(minutes=5)) or (
            logRotateTime > timeNow and (logRotateTime - timeNow) < datetime.timedelta(minutes=5)
        ):
            tmp_log.debug("skip pilotCounts session for logrotate")
        else:
            # log filename
            dispLogName = f"{panda_config.logdir}/panda-PilotRequests.log"
            # time limit
            timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=3)
            timeLimitS = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=1)
            # check if tgz is required
            com = f"head -1 {dispLogName}"
            lostat, loout = commands_get_status_output(com)
            useLogTgz = True
            if lostat == 0:
                match = re.search("^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", loout)
                if match is not None:
                    startTime = datetime.datetime(*time.strptime(match.group(0), "%Y-%m-%d %H:%M:%S")[:6])
                    # current log contains all info
                    if startTime < timeLimit:
                        useLogTgz = False
            # log files
            dispLogNameList = [dispLogName]
            if useLogTgz:
                today = datetime.date.today()
                dispLogNameList.append(f"{dispLogName}-{today.strftime('%Y%m%d')}.gz")
            # delete tmp
            commands_get_status_output(f"rm -f {dispLogName}.tmp-*")
            # tmp name
            tmp_logName = f"{dispLogName}.tmp-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d-%H-%M-%S')}"
            # loop over all files
            pilotCounts = {}
            pilotCountsS = {}
            for tmpDispLogName in dispLogNameList:
                # expand or copy
                if tmpDispLogName.endswith(".gz"):
                    com = f"gunzip -c {tmpDispLogName} > {tmp_logName}"
                else:
                    com = f"cp {tmpDispLogName} {tmp_logName}"
                lostat, loout = commands_get_status_output(com)
                if lostat != 0:
                    errMsg = f"failed to expand/copy {tmpDispLogName} with : {loout}"
                    raise RuntimeError(errMsg)
                # search string
                sStr = "^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*"
                sStr += "method=(.+),site=(.+),node=(.+),type=(.+)"
                # read
                logFH = open(tmp_logName)
                for line in logFH:
                    # check format
                    match = re.search(sStr, line)
                    if match is not None:
                        # check timerange
                        timeStamp = datetime.datetime(*time.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")[:6])
                        if timeStamp < timeLimit:
                            continue
                        tmpMethod = match.group(2)
                        tmpSite = match.group(3)
                        tmpNode = match.group(4)

                        # protection against corrupted entries from pilot,
                        # e.g. pilot reading site json from cvmfs while it was being updated
                        if tmpSite not in aSiteMapper.siteSpecList:
                            continue
                        # sum
                        pilotCounts.setdefault(tmpSite, {})
                        pilotCounts[tmpSite].setdefault(tmpMethod, {})
                        pilotCounts[tmpSite][tmpMethod].setdefault(tmpNode, 0)
                        pilotCounts[tmpSite][tmpMethod][tmpNode] += 1
                        # short
                        if timeStamp > timeLimitS:
                            if tmpSite not in pilotCountsS:
                                pilotCountsS[tmpSite] = dict()
                            if tmpMethod not in pilotCountsS[tmpSite]:
                                pilotCountsS[tmpSite][tmpMethod] = dict()
                            if tmpNode not in pilotCountsS[tmpSite][tmpMethod]:
                                pilotCountsS[tmpSite][tmpMethod][tmpNode] = 0
                            pilotCountsS[tmpSite][tmpMethod][tmpNode] += 1
                # close
                logFH.close()
            # delete tmp
            commands_get_status_output(f"rm {tmp_logName}")
            # update
            hostID = panda_config.pserverhost.split(".")[0]
            tmp_log.debug("pilotCounts session")
            retPC = taskBuffer.updateSiteData(hostID, pilotCounts, interval=3)
            tmp_log.debug(retPC)
            retPC = taskBuffer.updateSiteData(hostID, pilotCountsS, interval=1)
            tmp_log.debug(retPC)
    except Exception:
        errType, errValue = sys.exc_info()[:2]
        tmp_log.error(f"updateJob/getJob : {errType} {errValue}")

    # nRunning
    tmp_log.debug("nRunning session")
    try:
        if (current_minute / panda_config.nrun_interval) % panda_config.nrun_hosts == panda_config.nrun_snum:
            retNR = taskBuffer.insertnRunningInSiteData()
            tmp_log.debug(retNR)
    except Exception:
        errType, errValue = sys.exc_info()[:2]
        tmp_log.error(f"nRunning : {errType} {errValue}")

    # session for co-jumbo jobs
    tmp_log.debug("co-jumbo session")
    try:
        ret = taskBuffer.getCoJumboJobsToBeFinished(30, 0, 1000)
        if ret is None:
            tmp_log.debug("failed to get co-jumbo jobs to finish")
        else:
            coJumboA, coJumboD, coJumboW, coJumboTokill = ret
            tmp_log.debug(f"finish {len(coJumboA)} co-jumbo jobs in Active")
            if len(coJumboA) > 0:
                jobSpecs = taskBuffer.peekJobs(
                    coJumboA,
                    fromDefined=False,
                    fromActive=True,
                    fromArchived=False,
                    fromWaiting=False,
                )
                for jobSpec in jobSpecs:
                    fileCheckInJEDI = taskBuffer.checkInputFileStatusInJEDI(jobSpec)
                    if not fileCheckInJEDI:
                        jobSpec.jobStatus = "closed"
                        jobSpec.jobSubStatus = "cojumbo_wrong"
                        jobSpec.taskBufferErrorCode = pandaserver.taskbuffer.ErrorCode.EC_EventServiceInconsistentIn
                    taskBuffer.archiveJobs([jobSpec], False)
            tmp_log.debug(f"finish {len(coJumboD)} co-jumbo jobs in Defined")
            if len(coJumboD) > 0:
                jobSpecs = taskBuffer.peekJobs(
                    coJumboD,
                    fromDefined=True,
                    fromActive=False,
                    fromArchived=False,
                    fromWaiting=False,
                )
                for jobSpec in jobSpecs:
                    fileCheckInJEDI = taskBuffer.checkInputFileStatusInJEDI(jobSpec)
                    if not fileCheckInJEDI:
                        jobSpec.jobStatus = "closed"
                        jobSpec.jobSubStatus = "cojumbo_wrong"
                        jobSpec.taskBufferErrorCode = pandaserver.taskbuffer.ErrorCode.EC_EventServiceInconsistentIn
                    taskBuffer.archiveJobs([jobSpec], True)
            tmp_log.debug(f"finish {len(coJumboW)} co-jumbo jobs in Waiting")
            if len(coJumboW) > 0:
                jobSpecs = taskBuffer.peekJobs(
                    coJumboW,
                    fromDefined=False,
                    fromActive=False,
                    fromArchived=False,
                    fromWaiting=True,
                )
                for jobSpec in jobSpecs:
                    fileCheckInJEDI = taskBuffer.checkInputFileStatusInJEDI(jobSpec)
                    if not fileCheckInJEDI:
                        jobSpec.jobStatus = "closed"
                        jobSpec.jobSubStatus = "cojumbo_wrong"
                        jobSpec.taskBufferErrorCode = pandaserver.taskbuffer.ErrorCode.EC_EventServiceInconsistentIn
                    taskBuffer.archiveJobs([jobSpec], False, True)
            tmp_log.debug(f"kill {len(coJumboTokill)} co-jumbo jobs in Waiting")
            if len(coJumboTokill) > 0:
                jediJobs = list(coJumboTokill)
                nJob = 100
                iJob = 0
                while iJob < len(jediJobs):
                    tmp_log.debug(f" killing {str(jediJobs[iJob:iJob + nJob])}")
                    Client.killJobs(jediJobs[iJob : iJob + nJob], 51, keepUnmerged=True)
                    iJob += nJob
    except Exception:
        errStr = traceback.format_exc()
        tmp_log.error(errStr)

    # stop taskBuffer if created inside this script
    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)

    tmp_log.debug("===================== end =====================")


# run
if __name__ == "__main__":
    main(argv=sys.argv)
